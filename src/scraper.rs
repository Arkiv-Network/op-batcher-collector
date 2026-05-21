use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::model::{
    epoch_second_now, error_info_to_json, find_json_key_value, json_string, json_string_value,
    json_value_or_string, object_json, record_locked, second_key, ErrorInfo, SharedCollector,
};

const RPC_TIMEOUT_MS: u64 = 900;
const POLL_INTERVAL_MS: u64 = 250;

#[derive(Debug)]
struct HttpUrl {
    host: String,
    port: u16,
    path: String,
}

pub fn poll_loop(shared: Arc<SharedCollector>) {
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
