use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, lookup_host};
use tokio::time::{sleep, timeout};

use crate::model::{
    ErrorInfo, SharedCollector, epoch_second_now, record_locked, second_key,
};

const RPC_TIMEOUT_MS: u64 = 900;
const POLL_INTERVAL_MS: u64 = 250;

#[derive(Debug)]
struct HttpUrl {
    host: String,
    port: u16,
    path: String,
}

pub async fn poll_loop(shared: Arc<SharedCollector>) {
    loop {
        if let Err(error) = collect_due_seconds(&shared).await {
            eprintln!(
                "collector tick failed {}",
                json!({ "message": error })
            );
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}

async fn collect_due_seconds(shared: &SharedCollector) -> Result<(), String> {
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
                    code: Some(Value::String("COLLECTOR_BEHIND".to_string())),
                    status: None,
                    body: None,
                }),
                0,
            );
        }
    }

    let started = Instant::now();
    let second_key = second_key(current_second);
    let rpc_result = call_throttle_controller(&shared.config.rpc_url, &second_key).await;
    let duration_ms = started.elapsed().as_millis();

    let mut state = shared
        .state
        .lock()
        .map_err(|_| "collector state lock was poisoned".to_string())?;
    match rpc_result {
        Ok(result) => {
            record_locked(
                &shared.config.rpc_url,
                &mut state,
                current_second,
                true,
                Some(result),
                None,
                duration_ms,
            );
            println!(
                "batcher response ok {}",
                json!({
                    "second": second_key,
                    "durationMs": duration_ms.to_string(),
                    "rpcUrl": shared.config.rpc_url,
                })
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

async fn call_throttle_controller(rpc_url: &str, id: &str) -> Result<Value, ErrorInfo> {
    let url = parse_http_url(rpc_url)?;
    let request_body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "admin_getThrottleController",
        "params": [],
    })
    .to_string();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        url.path,
        url.host,
        request_body.len(),
        request_body
    );

    let rpc_timeout = Duration::from_millis(RPC_TIMEOUT_MS);
    let raw_response = timeout(rpc_timeout, exchange(&url, request.as_bytes()))
        .await
        .map_err(|_| http_error(format!("RPC timed out after {RPC_TIMEOUT_MS}ms"), None, None))??;

    let response = String::from_utf8_lossy(&raw_response);
    let (head, mut body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| http_error(
            "RPC HTTP response was malformed".to_string(),
            None,
            Some(Value::String(response.to_string())),
        ))?;
    let status = parse_http_status(head).unwrap_or(0);
    let decoded_chunked;
    if header_value(head, "transfer-encoding")
        .map(|value| value.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false)
    {
        decoded_chunked = decode_chunked_body(body).unwrap_or_else(|| body.to_string());
        body = &decoded_chunked;
    }

    let parsed: Option<Value> = serde_json::from_str(body).ok();

    if !(200..300).contains(&status) {
        return Err(http_error(
            format!("RPC HTTP request failed with {status}"),
            Some(status),
            Some(body_as_value(parsed.as_ref(), body)),
        ));
    }

    if let Some(value) = &parsed {
        if let Some(error_value) = value.get("error")
            && !error_value.is_null()
            && error_value.as_bool() != Some(false)
        {
            let message = error_value
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| "RPC returned an error".to_string());
            let code = error_value
                .get("code")
                .cloned()
                .or_else(|| Some(Value::String("RPC_ERROR".to_string())));
            let data = error_value.get("data").cloned();

            return Err(ErrorInfo {
                message,
                code,
                status: None,
                body: data,
            });
        }

        if let Some(result_value) = value.get("result") {
            return Ok(result_value.clone());
        }
    }

    Ok(body_as_value(parsed.as_ref(), body))
}

async fn exchange(url: &HttpUrl, request: &[u8]) -> Result<Vec<u8>, ErrorInfo> {
    let addr = lookup_host((url.host.as_str(), url.port))
        .await
        .map_err(|error| http_error(format!("RPC host resolution failed: {error}"), None, None))?
        .next()
        .ok_or_else(|| {
            http_error(
                "RPC host resolution returned no addresses".to_string(),
                None,
                None,
            )
        })?;

    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|error| http_error(format!("RPC connection failed: {error}"), None, None))?;

    stream
        .write_all(request)
        .await
        .map_err(|error| http_error(format!("RPC HTTP request failed: {error}"), None, None))?;

    let mut buffer = Vec::new();
    stream
        .read_to_end(&mut buffer)
        .await
        .map_err(|error| {
            http_error(
                format!("RPC HTTP response read failed: {error}"),
                None,
                None,
            )
        })?;

    Ok(buffer)
}

fn http_error(message: String, status: Option<u16>, body: Option<Value>) -> ErrorInfo {
    ErrorInfo {
        message,
        code: Some(Value::String("RPC_HTTP_ERROR".to_string())),
        status,
        body,
    }
}

fn body_as_value(parsed: Option<&Value>, raw: &str) -> Value {
    parsed
        .cloned()
        .unwrap_or_else(|| Value::String(raw.to_string()))
}

fn parse_http_url(raw: &str) -> Result<HttpUrl, ErrorInfo> {
    let without_scheme = raw.strip_prefix("http://").ok_or_else(|| ErrorInfo {
        message: "Only plain http:// RPC URLs are supported by the collector".to_string(),
        code: Some(Value::String("RPC_UNSUPPORTED_URL".to_string())),
        status: None,
        body: None,
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
            code: Some(Value::String("RPC_INVALID_URL".to_string())),
            status: None,
            body: None,
        });
    }

    Ok(HttpUrl { host, port, path })
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
