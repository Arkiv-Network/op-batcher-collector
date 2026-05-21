use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::model::{
    epoch_second_now, error_info_to_json, error_payload, json_string, normalize_second,
    object_json, option_json_string, second_key, HistoryEntry, SharedCollector,
};

const WEB_SERVER_WORKERS: usize = 4;

pub fn run_server(shared: Arc<SharedCollector>) {
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

    let (sender, receiver) = mpsc::channel::<TcpStream>();
    let receiver = Arc::new(Mutex::new(receiver));
    for worker_id in 0..WEB_SERVER_WORKERS {
        let worker_receiver = Arc::clone(&receiver);
        let worker_shared = Arc::clone(&shared);
        thread::Builder::new()
            .name(format!("collector-http-worker-{worker_id}"))
            .spawn(move || run_worker(worker_receiver, worker_shared))
            .expect("failed to spawn collector http worker thread");
    }

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                if let Err(error) = sender.send(stream) {
                    eprintln!("HTTP dispatch failed: {error}");
                }
            }
            Err(error) => eprintln!("HTTP accept failed: {error}"),
        }
    }
}

fn run_worker(receiver: Arc<Mutex<Receiver<TcpStream>>>, shared: Arc<SharedCollector>) {
    loop {
        let stream = {
            let lock = match receiver.lock() {
                Ok(lock) => lock,
                Err(_) => return,
            };
            match lock.recv() {
                Ok(stream) => stream,
                Err(_) => return,
            }
        };
        handle_connection(stream, Arc::clone(&shared));
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
    use crate::model::{record_locked, CollectorState, Config, HistoryStore};

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
