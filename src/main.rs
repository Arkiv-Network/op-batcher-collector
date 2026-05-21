use std::collections::VecDeque;
use std::env;
use std::fmt::Write as FmtWrite;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_RPC_URL: &str = "http://host.docker.internal:8548";
const DEFAULT_HISTORY_SIZE: usize = 5000;
const DEFAULT_LISTEN_HOST: &str = "0.0.0.0";
const DEFAULT_LISTEN_PORT: u16 = 28881;
const RPC_TIMEOUT_MS: u64 = 900;
const POLL_INTERVAL_MS: u64 = 250;

#[derive(Clone, Debug)]
struct Config {
    rpc_url: String,
    history_size: usize,
    listen_host: String,
    listen_port: u16,
}

#[derive(Clone, Debug)]
struct ErrorInfo {
    message: String,
    code_json: Option<String>,
    status: Option<u16>,
    body_json: Option<String>,
}

#[derive(Clone, Debug)]
struct HistoryEntry {
    second: String,
    collected_at: String,
    rpc_url: String,
    duration_ms: u128,
    ok: bool,
    result_json: Option<String>,
    error: Option<ErrorInfo>,
}

#[derive(Debug)]
struct HistoryStore {
    limit: usize,
    entries: VecDeque<HistoryEntry>,
}

#[derive(Debug)]
struct CollectorState {
    history: HistoryStore,
    latest_epoch_second: Option<i64>,
    collecting: bool,
    started_at: String,
}

#[derive(Debug)]
struct SharedCollector {
    config: Config,
    state: Mutex<CollectorState>,
}

#[derive(Debug)]
struct HttpUrl {
    host: String,
    port: u16,
    path: String,
}

fn main() {
    install_process_panic_handler();

    let config = create_config();
    let shared = Arc::new(SharedCollector {
        state: Mutex::new(CollectorState {
            history: HistoryStore::new(config.history_size),
            latest_epoch_second: None,
            collecting: false,
            started_at: now_iso_second(),
        }),
        config,
    });

    let collector = Arc::clone(&shared);
    let collector_thread = thread::Builder::new()
        .name("collector-query".to_string())
        .spawn(move || poll_loop(collector))
        .expect("failed to spawn collector query thread");

    let server = Arc::clone(&shared);
    let server_thread = thread::Builder::new()
        .name("collector-web".to_string())
        .spawn(move || run_server(server))
        .expect("failed to spawn collector web thread");

    if let Err(error) = server_thread.join() {
        eprintln!("web server thread failed: {error:?}");
    }
    if let Err(error) = collector_thread.join() {
        eprintln!("collector query thread failed: {error:?}");
    }
}

fn install_process_panic_handler() {
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("uncaught panic: {panic_info}");
    }));
}

fn create_config() -> Config {
    let rpc_url = env::var("BATCHER_RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
    let history_size = parse_positive_usize(
        env::var("HISTORY_SIZE").ok().as_deref(),
        DEFAULT_HISTORY_SIZE,
    );
    let listen_host =
        env::var("COLLECTOR_LISTEN_HOST").unwrap_or_else(|_| DEFAULT_LISTEN_HOST.to_string());
    let listen_port = parse_positive_u16(
        env::var("COLLECTOR_LISTEN_PORT").ok().as_deref(),
        DEFAULT_LISTEN_PORT,
    );

    Config {
        rpc_url,
        history_size,
        listen_host,
        listen_port,
    }
}

fn parse_positive_usize(value: Option<&str>, fallback: usize) -> usize {
    value
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|parsed| *parsed > 0)
        .unwrap_or(fallback)
}

fn parse_positive_u16(value: Option<&str>, fallback: u16) -> u16 {
    value
        .and_then(|raw| raw.parse::<u16>().ok())
        .filter(|parsed| *parsed > 0)
        .unwrap_or(fallback)
}

fn poll_loop(shared: Arc<SharedCollector>) {
    loop {
        if let Err(error) = collect_due_seconds(&shared) {
            eprintln!(
                "collector tick failed {}",
                error_info_to_json(&ErrorInfo {
                    message: error,
                    code_json: None,
                    status: None,
                    body_json: None,
                })
            );
        }
        thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

fn collect_due_seconds(shared: &SharedCollector) -> Result<(), String> {
    let current_second = epoch_second_now();
    {
        let mut state = shared
            .state
            .lock()
            .map_err(|_| "collector state lock was poisoned".to_string())?;
        if state.collecting {
            return Ok(());
        }

        state.collecting = true;

        let first_due_second = state
            .latest_epoch_second
            .map(|second| second + 1)
            .unwrap_or(current_second);
        let earliest_retained_second = current_second - state.history.limit as i64 + 1;
        let start_second = first_due_second.max(earliest_retained_second);

        if start_second > current_second {
            state.collecting = false;
            return Ok(());
        }

        for epoch_second in start_second..current_second {
            record_locked(
                &shared.config.rpc_url,
                &mut state,
                epoch_second,
                false,
                None,
                Some(ErrorInfo {
                    message: "Collector fell behind before this second could be polled".to_string(),
                    code_json: Some(json_string("COLLECTOR_BEHIND")),
                    status: None,
                    body_json: None,
                }),
                0,
            );
        }
    }

    let started = Instant::now();
    let second_key = second_key(current_second);
    let rpc_result = call_throttle_controller(&shared.config.rpc_url, &second_key);
    let duration_ms = started.elapsed().as_millis();

    let mut state = shared
        .state
        .lock()
        .map_err(|_| "collector state lock was poisoned".to_string())?;
    match rpc_result {
        Ok(result_json) => {
            record_locked(
                &shared.config.rpc_url,
                &mut state,
                current_second,
                true,
                Some(result_json),
                None,
                duration_ms,
            );
            println!(
                "batcher response ok {}",
                object_json(&[
                    ("second", json_string(&second_key)),
                    ("durationMs", duration_ms.to_string()),
                    ("rpcUrl", json_string(&shared.config.rpc_url)),
                ])
            );
        }
        Err(error) => {
            record_locked(
                &shared.config.rpc_url,
                &mut state,
                current_second,
                false,
                None,
                Some(error),
                duration_ms,
            );
        }
    }
    state.collecting = false;

    Ok(())
}

fn record_locked(
    rpc_url: &str,
    state: &mut CollectorState,
    epoch_second: i64,
    ok: bool,
    result_json: Option<String>,
    error: Option<ErrorInfo>,
    duration_ms: u128,
) {
    let key = second_key(epoch_second);
    state.history.set(HistoryEntry {
        second: key,
        collected_at: now_iso_second(),
        rpc_url: rpc_url.to_string(),
        duration_ms,
        ok,
        result_json,
        error,
    });
    state.latest_epoch_second = Some(epoch_second);
}

impl HistoryStore {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            entries: VecDeque::new(),
        }
    }

    fn set(&mut self, entry: HistoryEntry) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|existing| existing.second == entry.second)
        {
            self.entries.remove(index);
        }

        self.entries.push_back(entry);
        while self.entries.len() > self.limit {
            self.entries.pop_front();
        }
    }

    fn get(&self, second: &str) -> Option<&HistoryEntry> {
        self.entries.iter().find(|entry| entry.second == second)
    }

    fn oldest_key(&self) -> Option<&str> {
        self.entries.front().map(|entry| entry.second.as_str())
    }

    fn latest_key(&self) -> Option<&str> {
        self.entries.back().map(|entry| entry.second.as_str())
    }
}

fn call_throttle_controller(rpc_url: &str, id: &str) -> Result<String, ErrorInfo> {
    let url = parse_http_url(rpc_url)?;
    let mut stream = connect_http(&url)?;
    let body = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":\"admin_getThrottleController\",\"params\":[]}}",
        json_string(id)
    );
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        url.path,
        url.host,
        body.len(),
        body
    );

    stream
        .write_all(request.as_bytes())
        .map_err(|error| ErrorInfo {
            message: format!("RPC HTTP request failed: {error}"),
            code_json: Some(json_string("RPC_HTTP_ERROR")),
            status: None,
            body_json: None,
        })?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| ErrorInfo {
            message: format!("RPC HTTP response read failed: {error}"),
            code_json: Some(json_string("RPC_HTTP_ERROR")),
            status: None,
            body_json: None,
        })?;

    let response = String::from_utf8_lossy(&response);
    let (head, mut body) = response.split_once("\r\n\r\n").ok_or_else(|| ErrorInfo {
        message: "RPC HTTP response was malformed".to_string(),
        code_json: Some(json_string("RPC_HTTP_ERROR")),
        status: None,
        body_json: Some(json_string(response.as_ref())),
    })?;
    let status = parse_http_status(head).unwrap_or(0);
    let decoded_chunked;
    if header_value(head, "transfer-encoding")
        .map(|value| value.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false)
    {
        decoded_chunked = decode_chunked_body(body).unwrap_or_else(|| body.to_string());
        body = &decoded_chunked;
    }

    if !(200..300).contains(&status) {
        return Err(ErrorInfo {
            message: format!("RPC HTTP request failed with {status}"),
            code_json: Some(json_string("RPC_HTTP_ERROR")),
            status: Some(status),
            body_json: Some(json_value_or_string(body)),
        });
    }

    if let Some(error_json) = find_json_key_value(body, "error") {
        if error_json.trim() != "null" && error_json.trim() != "false" {
            let message = find_json_key_value(error_json, "message")
                .and_then(json_string_value)
                .unwrap_or_else(|| "RPC returned an error".to_string());
            let code_json = find_json_key_value(error_json, "code")
                .map(str::trim)
                .map(ToOwned::to_owned)
                .or_else(|| Some(json_string("RPC_ERROR")));
            let data_json = find_json_key_value(error_json, "data")
                .map(str::trim)
                .map(ToOwned::to_owned);

            return Err(ErrorInfo {
                message,
                code_json,
                status: None,
                body_json: data_json,
            });
        }
    }

    if let Some(result_json) = find_json_key_value(body, "result") {
        Ok(result_json.trim().to_string())
    } else {
        Ok(json_value_or_string(body))
    }
}

fn parse_http_url(raw: &str) -> Result<HttpUrl, ErrorInfo> {
    let without_scheme = raw.strip_prefix("http://").ok_or_else(|| ErrorInfo {
        message: "Only plain http:// RPC URLs are supported by the standard-library client"
            .to_string(),
        code_json: Some(json_string("RPC_UNSUPPORTED_URL")),
        status: None,
        body_json: None,
    })?;
    let (authority, path) = without_scheme
        .split_once('/')
        .map(|(authority, path)| (authority, format!("/{path}")))
        .unwrap_or((without_scheme, "/".to_string()));
    let (host, port) = authority
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host.to_string(), port.parse::<u16>().ok()?)))
        .unwrap_or_else(|| (authority.to_string(), 80));

    if host.is_empty() {
        return Err(ErrorInfo {
            message: "RPC URL host is empty".to_string(),
            code_json: Some(json_string("RPC_INVALID_URL")),
            status: None,
            body_json: None,
        });
    }

    Ok(HttpUrl { host, port, path })
}

fn connect_http(url: &HttpUrl) -> Result<TcpStream, ErrorInfo> {
    let address = (url.host.as_str(), url.port)
        .to_socket_addrs()
        .map_err(|error| ErrorInfo {
            message: format!("RPC host resolution failed: {error}"),
            code_json: Some(json_string("RPC_HTTP_ERROR")),
            status: None,
            body_json: None,
        })?
        .next()
        .ok_or_else(|| ErrorInfo {
            message: "RPC host resolution returned no addresses".to_string(),
            code_json: Some(json_string("RPC_HTTP_ERROR")),
            status: None,
            body_json: None,
        })?;
    let stream = TcpStream::connect_timeout(&address, Duration::from_millis(RPC_TIMEOUT_MS))
        .map_err(|error| ErrorInfo {
            message: format!("RPC connection failed: {error}"),
            code_json: Some(json_string("RPC_HTTP_ERROR")),
            status: None,
            body_json: None,
        })?;
    let timeout = Some(Duration::from_millis(RPC_TIMEOUT_MS));
    let _ = stream.set_read_timeout(timeout);
    let _ = stream.set_write_timeout(timeout);
    Ok(stream)
}

fn run_server(shared: Arc<SharedCollector>) {
    let bind_address = format!(
        "{}:{}",
        shared.config.listen_host, shared.config.listen_port
    );
    let listener = match TcpListener::bind(&bind_address) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("failed to bind HTTP server on {bind_address}: {error}");
            return;
        }
    };

    println!(
        "{}",
        object_json(&[
            ("message", json_string("op-batcher collector listening")),
            ("url", json_string(&format!("http://{bind_address}/")),),
            ("rpcUrl", json_string(&shared.config.rpc_url)),
            ("historySize", shared.config.history_size.to_string()),
            ("listenHost", json_string(&shared.config.listen_host)),
            ("listenPort", shared.config.listen_port.to_string()),
        ])
    );

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let request_shared = Arc::clone(&shared);
                let _ = thread::Builder::new()
                    .name("collector-http-request".to_string())
                    .spawn(move || handle_connection(stream, request_shared));
            }
            Err(error) => eprintln!("HTTP accept failed: {error}"),
        }
    }
}

fn handle_connection(mut stream: TcpStream, shared: Arc<SharedCollector>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buffer = [0_u8; 8192];
    let bytes_read = match stream.read(&mut buffer) {
        Ok(bytes_read) => bytes_read,
        Err(error) => {
            let _ = write_http_response(
                &mut stream,
                400,
                &object_json(&[
                    ("ok", "false".to_string()),
                    (
                        "error",
                        object_json(&[("message", json_string(&format!("Bad request: {error}")))]),
                    ),
                ]),
            );
            return;
        }
    };

    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let response = handle_http_request(&request, &shared);
    let _ = write_http_response(&mut stream, response.status, &response.body);
}

struct HttpResponse {
    status: u16,
    body: String,
}

fn handle_http_request(request: &str, shared: &SharedCollector) -> HttpResponse {
    let request_line = match request.lines().next() {
        Some(line) => line,
        None => return json_http(400, &error_payload("Bad request")),
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or("/");

    if method != "GET" {
        return json_http(
            405,
            &object_json(&[
                ("ok", "false".to_string()),
                ("error", error_payload("Only GET requests are supported")),
            ]),
        );
    }

    let (path_raw, query_raw) = target.split_once('?').unwrap_or((target, ""));
    let path_decoded = percent_decode(path_raw, false).unwrap_or_else(|| path_raw.to_string());
    let pathname = trim_trailing_slashes(&path_decoded);

    match pathname.as_str() {
        "/" | "/health" | "/status" => json_http(200, &status_payload(shared)),
        "/latest" => json_http(200, &latest_payload(shared)),
        "/history" => history_payload(shared, query_raw),
        path if path.starts_with("/history/") => {
            let requested_raw = &path["/history/".len()..];
            match normalize_second(requested_raw) {
                Some(second) => history_lookup_payload(shared, &second),
                None => json_http(
                    400,
                    &object_json(&[
                        ("ok", "false".to_string()),
                        (
                            "error",
                            error_payload(
                                "Invalid second. Use an ISO datetime second or epoch second.",
                            ),
                        ),
                    ]),
                ),
            }
        }
        _ => json_http(
            404,
            &object_json(&[
                ("ok", "false".to_string()),
                ("error", error_payload("Not found")),
            ]),
        ),
    }
}

fn status_payload(shared: &SharedCollector) -> String {
    let state = shared.state.lock().expect("collector state lock poisoned");
    let current_second = epoch_second_now();
    let behind_seconds = state
        .latest_epoch_second
        .map(|latest| (current_second - latest).max(0))
        .unwrap_or(0);

    object_json(&[
        ("ok", "true".to_string()),
        ("rpcUrl", json_string(&shared.config.rpc_url)),
        ("historySize", state.history.limit.to_string()),
        ("retainedEntries", state.history.entries.len().to_string()),
        (
            "oldestSecond",
            option_json_string(state.history.oldest_key()),
        ),
        (
            "latestSecond",
            option_json_string(state.history.latest_key()),
        ),
        ("currentSecond", json_string(&second_key(current_second))),
        ("behindSeconds", behind_seconds.to_string()),
        ("collecting", state.collecting.to_string()),
        ("startedAt", json_string(&state.started_at)),
        (
            "endpoints",
            "[\"/health\",\"/latest\",\"/history\",\"/history?second=<datetime>\"]".to_string(),
        ),
    ])
}

fn latest_payload(shared: &SharedCollector) -> String {
    let state = shared.state.lock().expect("collector state lock poisoned");
    let latest_second = state.history.latest_key();
    let entry_json = latest_second
        .and_then(|second| state.history.get(second))
        .map(history_entry_to_json)
        .unwrap_or_else(|| "null".to_string());

    object_json(&[
        ("ok", "true".to_string()),
        ("second", option_json_string(latest_second)),
        ("entry", entry_json),
    ])
}

fn history_payload(shared: &SharedCollector, query: &str) -> HttpResponse {
    let second_param = query_param(query, "second");
    if let Some(raw_second) = second_param {
        return match normalize_second(&raw_second) {
            Some(second) => history_lookup_payload(shared, &second),
            None => json_http(
                400,
                &object_json(&[
                    ("ok", "false".to_string()),
                    (
                        "error",
                        error_payload(
                            "Invalid second. Use an ISO datetime second or epoch second.",
                        ),
                    ),
                ]),
            ),
        };
    }

    let state = shared.state.lock().expect("collector state lock poisoned");
    let mut history = String::from("{");
    for (index, entry) in state.history.entries.iter().enumerate() {
        if index > 0 {
            history.push(',');
        }
        history.push_str(&json_string(&entry.second));
        history.push(':');
        history.push_str(&history_entry_to_json(entry));
    }
    history.push('}');

    json_http(
        200,
        &object_json(&[
            ("ok", "true".to_string()),
            ("count", state.history.entries.len().to_string()),
            (
                "oldestSecond",
                option_json_string(state.history.oldest_key()),
            ),
            (
                "latestSecond",
                option_json_string(state.history.latest_key()),
            ),
            ("history", history),
        ]),
    )
}

fn history_lookup_payload(shared: &SharedCollector, second: &str) -> HttpResponse {
    let state = shared.state.lock().expect("collector state lock poisoned");
    let entry = state.history.get(second);
    let status = if entry.is_some() { 200 } else { 404 };
    json_http(
        status,
        &object_json(&[
            ("ok", entry.is_some().to_string()),
            ("second", json_string(second)),
            (
                "entry",
                entry
                    .map(history_entry_to_json)
                    .unwrap_or_else(|| "null".to_string()),
            ),
        ]),
    )
}

fn history_entry_to_json(entry: &HistoryEntry) -> String {
    let mut fields = vec![
        ("second", json_string(&entry.second)),
        ("collectedAt", json_string(&entry.collected_at)),
        ("rpcUrl", json_string(&entry.rpc_url)),
        ("durationMs", entry.duration_ms.to_string()),
        ("ok", entry.ok.to_string()),
    ];

    if entry.ok {
        fields.push((
            "result",
            entry
                .result_json
                .clone()
                .unwrap_or_else(|| "null".to_string()),
        ));
    } else {
        fields.push((
            "error",
            entry
                .error
                .as_ref()
                .map(error_info_to_json)
                .unwrap_or_else(|| error_payload("Unknown error")),
        ));
    }

    object_json(&fields)
}

fn error_info_to_json(error: &ErrorInfo) -> String {
    let mut fields = vec![("message", json_string(&error.message))];
    if let Some(code_json) = &error.code_json {
        fields.push(("code", code_json.clone()));
    }
    if let Some(status) = error.status {
        fields.push(("status", status.to_string()));
    }
    if let Some(body_json) = &error.body_json {
        fields.push(("body", body_json.clone()));
    }
    object_json(&fields)
}

fn error_payload(message: &str) -> String {
    object_json(&[("message", json_string(message))])
}

fn json_http(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        body: body.to_string(),
    }
}

fn write_http_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn object_json(fields: &[(&str, String)]) -> String {
    let mut json = String::from("{");
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&json_string(key));
        json.push(':');
        json.push_str(value);
    }
    json.push('}');
    json
}

fn option_json_string(value: Option<&str>) -> String {
    value.map(json_string).unwrap_or_else(|| "null".to_string())
}

fn json_string(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for character in value.chars() {
        match character {
            '"' => encoded.push_str("\\\""),
            '\\' => encoded.push_str("\\\\"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            '\u{08}' => encoded.push_str("\\b"),
            '\u{0c}' => encoded.push_str("\\f"),
            character if character < ' ' => {
                let _ = write!(encoded, "\\u{:04x}", character as u32);
            }
            character => encoded.push(character),
        }
    }
    encoded.push('"');
    encoded
}

fn json_value_or_string(value: &str) -> String {
    let trimmed = value.trim();
    if parse_json_value_end(trimmed.as_bytes(), 0)
        .map(|end| trimmed[end..].trim().is_empty())
        .unwrap_or(false)
    {
        trimmed.to_string()
    } else {
        json_string(value)
    }
}

fn epoch_second_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn now_iso_second() -> String {
    second_key(epoch_second_now())
}

fn second_key(epoch_second: i64) -> String {
    let days = epoch_second.div_euclid(86_400);
    let second_of_day = epoch_second.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3600;
    let minute = (second_of_day % 3600) / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn normalize_second(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    if raw
        .chars()
        .all(|character| character == '-' || character.is_ascii_digit())
        && raw.chars().filter(|character| *character == '-').count() <= 1
        && !raw[1.min(raw.len())..].contains('-')
    {
        return raw.parse::<i64>().ok().map(second_key);
    }
    parse_iso_epoch_second(raw).map(second_key)
}

fn parse_iso_epoch_second(raw: &str) -> Option<i64> {
    let bytes = raw.as_bytes();
    if bytes.len() < 19 {
        return None;
    }

    let year = parse_digits(bytes, 0, 4)? as i32;
    expect_byte(bytes, 4, b'-')?;
    let month = parse_digits(bytes, 5, 2)? as i32;
    expect_byte(bytes, 7, b'-')?;
    let day = parse_digits(bytes, 8, 2)? as i32;
    if bytes.get(10) != Some(&b'T') && bytes.get(10) != Some(&b' ') {
        return None;
    }
    let hour = parse_digits(bytes, 11, 2)?;
    expect_byte(bytes, 13, b':')?;
    let minute = parse_digits(bytes, 14, 2)?;
    expect_byte(bytes, 16, b':')?;
    let second = parse_digits(bytes, 17, 2)?;
    let mut index = 19;

    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes
            .get(index)
            .map(|byte| byte.is_ascii_digit())
            .unwrap_or(false)
        {
            index += 1;
        }
    }

    let offset_seconds = match bytes.get(index) {
        Some(b'Z') | Some(b'z') => {
            index += 1;
            0
        }
        Some(b'+') | Some(b'-') => {
            let sign = if bytes[index] == b'+' { 1 } else { -1 };
            let offset_hour = parse_digits(bytes, index + 1, 2)?;
            let next = index + 3;
            let (offset_minute, end) = if bytes.get(next) == Some(&b':') {
                (parse_digits(bytes, next + 1, 2)?, next + 3)
            } else {
                (parse_digits(bytes, next, 2)?, next + 2)
            };
            index = end;
            sign * ((offset_hour * 3600) + (offset_minute * 60))
        }
        None => 0,
        _ => return None,
    };

    if index != bytes.len() {
        return None;
    }
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds)
}

fn parse_digits(bytes: &[u8], start: usize, len: usize) -> Option<i64> {
    let slice = bytes.get(start..start + len)?;
    if !slice.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(slice).ok()?.parse::<i64>().ok()
}

fn expect_byte(bytes: &[u8], index: usize, expected: u8) -> Option<()> {
    (bytes.get(index) == Some(&expected)).then_some(())
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let year = year as i64 - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn parse_http_status(head: &str) -> Option<u16> {
    head.lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    for line in head.lines().skip(1) {
        let (header_name, value) = line.split_once(':')?;
        if header_name.eq_ignore_ascii_case(name) {
            return Some(value.trim());
        }
    }
    None
}

fn decode_chunked_body(body: &str) -> Option<String> {
    let mut rest = body;
    let mut decoded = String::new();

    loop {
        let (size_line, after_size) = rest.split_once("\r\n")?;
        let size_hex = size_line.split(';').next()?.trim();
        let size = usize::from_str_radix(size_hex, 16).ok()?;
        if size == 0 {
            return Some(decoded);
        }
        let chunk = after_size.get(..size)?;
        decoded.push_str(chunk);
        rest = after_size.get(size + 2..)?;
    }
}

fn find_json_key_value<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let bytes = json.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        index = skip_ws(bytes, index);
        if bytes.get(index) == Some(&b'"') {
            let key_end = parse_string_end(bytes, index)?;
            let raw_key = &json[index + 1..key_end - 1];
            let after_key = skip_ws(bytes, key_end);
            if raw_key == key && bytes.get(after_key) == Some(&b':') {
                let value_start = skip_ws(bytes, after_key + 1);
                let value_end = parse_json_value_end(bytes, value_start)?;
                return json.get(value_start..value_end);
            }
            index = key_end;
        } else {
            index += 1;
        }
    }
    None
}

fn parse_json_value_end(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start)? {
        b'"' => parse_string_end(bytes, start),
        b'{' => parse_balanced_end(bytes, start, b'{', b'}'),
        b'[' => parse_balanced_end(bytes, start, b'[', b']'),
        b'-' | b'0'..=b'9' => parse_number_end(bytes, start),
        b't' if bytes.get(start..start + 4) == Some(b"true") => Some(start + 4),
        b'f' if bytes.get(start..start + 5) == Some(b"false") => Some(start + 5),
        b'n' if bytes.get(start..start + 4) == Some(b"null") => Some(start + 4),
        _ => None,
    }
}

fn parse_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn parse_balanced_end(bytes: &[u8], start: usize, open: u8, close: u8) -> Option<usize> {
    if bytes.get(start) != Some(&open) {
        return None;
    }
    let mut depth = 0;
    let mut index = start;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => index = parse_string_end(bytes, index)?,
            byte if byte == open => {
                depth += 1;
                index += 1;
            }
            byte if byte == close => {
                depth -= 1;
                index += 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => index += 1,
        }
    }
    None
}

fn parse_number_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    if bytes.get(index) == Some(&b'-') {
        index += 1;
    }
    while bytes
        .get(index)
        .map(|byte| byte.is_ascii_digit())
        .unwrap_or(false)
    {
        index += 1;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes
            .get(index)
            .map(|byte| byte.is_ascii_digit())
            .unwrap_or(false)
        {
            index += 1;
        }
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
            index += 1;
        }
        while bytes
            .get(index)
            .map(|byte| byte.is_ascii_digit())
            .unwrap_or(false)
        {
            index += 1;
        }
    }
    (index > start).then_some(index)
}

fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .map(|byte| byte.is_ascii_whitespace())
        .unwrap_or(false)
    {
        index += 1;
    }
    index
}

fn json_string_value(json: &str) -> Option<String> {
    let trimmed = json.trim();
    let bytes = trimmed.as_bytes();
    if bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return None;
    }
    let mut decoded = String::new();
    let mut index = 1;
    while index + 1 < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index += 1;
                match bytes.get(index)? {
                    b'"' => decoded.push('"'),
                    b'\\' => decoded.push('\\'),
                    b'/' => decoded.push('/'),
                    b'b' => decoded.push('\u{08}'),
                    b'f' => decoded.push('\u{0c}'),
                    b'n' => decoded.push('\n'),
                    b'r' => decoded.push('\r'),
                    b't' => decoded.push('\t'),
                    b'u' => {
                        let hex = std::str::from_utf8(bytes.get(index + 1..index + 5)?).ok()?;
                        let codepoint = u16::from_str_radix(hex, 16).ok()? as u32;
                        decoded.push(char::from_u32(codepoint)?);
                        index += 4;
                    }
                    _ => return None,
                }
            }
            byte => decoded.push(byte as char),
        }
        index += 1;
    }
    Some(decoded)
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decoded_name = percent_decode(name, true)?;
        if decoded_name == key {
            return percent_decode(value, true);
        }
    }
    None
}

fn percent_decode(value: &str, plus_as_space: bool) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;
                decoded.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
            }
            b'+' if plus_as_space => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

fn trim_trailing_slashes(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_positive_values_use_fallbacks_for_invalid_input() {
        assert_eq!(parse_positive_usize(Some("42"), 1), 42);
        assert_eq!(parse_positive_usize(Some("0"), 7), 7);
        assert_eq!(parse_positive_usize(Some("abc"), 7), 7);
        assert_eq!(parse_positive_u16(Some("28881"), 1), 28881);
        assert_eq!(parse_positive_u16(Some("70000"), 9), 9);
    }

    #[test]
    fn second_keys_are_utc_iso_seconds() {
        assert_eq!(second_key(0), "1970-01-01T00:00:00Z");
        assert_eq!(second_key(100), "1970-01-01T00:01:40Z");
        assert_eq!(second_key(1_775_001_600), "2026-04-01T00:00:00Z");
    }

    #[test]
    fn normalize_second_supports_epoch_and_iso_values() {
        assert_eq!(
            normalize_second("1970-01-01T00:01:40.900Z").as_deref(),
            Some("1970-01-01T00:01:40Z")
        );
        assert_eq!(
            normalize_second("1970-01-01T02:01:40+02:00").as_deref(),
            Some("1970-01-01T00:01:40Z")
        );
        assert_eq!(
            normalize_second("100").as_deref(),
            Some("1970-01-01T00:01:40Z")
        );
        assert_eq!(normalize_second("not-a-date"), None);
    }

    #[test]
    fn history_store_keeps_newest_entries() {
        let mut history = HistoryStore::new(2);
        for second in 1..=3 {
            history.set(HistoryEntry {
                second: second_key(second),
                collected_at: second_key(second),
                rpc_url: "http://rpc.example".to_string(),
                duration_ms: 0,
                ok: true,
                result_json: Some("{}".to_string()),
                error: None,
            });
        }

        assert_eq!(history.entries.len(), 2);
        assert_eq!(history.oldest_key(), Some(second_key(2).as_str()));
        assert_eq!(history.latest_key(), Some(second_key(3).as_str()));
    }

    #[test]
    fn json_key_scanner_extracts_nested_values() {
        let payload = r#"{"jsonrpc":"2.0","result":{"nested":["value",{"ok":true}]}}"#;
        assert_eq!(
            find_json_key_value(payload, "result"),
            Some(r#"{"nested":["value",{"ok":true}]}"#)
        );

        let error = r#"{"error":{"code":-1,"message":"denied"}}"#;
        let error_value = find_json_key_value(error, "error").unwrap();
        assert_eq!(
            find_json_key_value(error_value, "message").and_then(json_string_value),
            Some("denied".to_string())
        );
    }

    #[test]
    fn http_handler_returns_history_lookup() {
        let shared = SharedCollector {
            config: Config {
                rpc_url: "http://rpc.example".to_string(),
                history_size: 10,
                listen_host: "127.0.0.1".to_string(),
                listen_port: 28881,
            },
            state: Mutex::new(CollectorState {
                history: HistoryStore::new(10),
                latest_epoch_second: Some(300),
                collecting: false,
                started_at: second_key(300),
            }),
        };
        {
            let mut state = shared.state.lock().unwrap();
            record_locked(
                "http://rpc.example",
                &mut state,
                300,
                true,
                Some(r#"{"value":"stored"}"#.to_string()),
                None,
                37,
            );
        }

        let response = handle_http_request(
            "GET /history?second=1970-01-01T00%3A05%3A00Z HTTP/1.1\r\n\r\n",
            &shared,
        );
        assert_eq!(response.status, 200);
        assert!(response.body.contains(r#""ok":true"#));
        assert!(response.body.contains(r#""value":"stored""#));
    }
}
