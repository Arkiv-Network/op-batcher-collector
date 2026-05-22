use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::time::sleep;

use crate::model::{ErrorInfo, SharedCollector, epoch_second_now, record_locked, second_key};

pub const RPC_TIMEOUT_MS: u64 = 900;
const POLL_INTERVAL_MS: u64 = 250;

pub async fn poll_loop(shared: Arc<SharedCollector>) {
    loop {
        if let Err(error) = collect_due_seconds(&shared).await {
            eprintln!("collector tick failed {}", json!({ "message": error }));
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
    let rpc_result =
        call_throttle_controller(&shared.http_client, &shared.config.rpc_url, &second_key).await;
    let duration_ms = started.elapsed().as_millis();

    let mut state = shared
        .state
        .lock()
        .map_err(|_| "collector state lock was poisoned".to_string())?;
    match rpc_result {
        Ok(result) => {
            record_locked(
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

async fn call_throttle_controller(
    client: &reqwest::Client,
    rpc_url: &str,
    id: &str,
) -> Result<Value, ErrorInfo> {
    let request_body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "admin_getThrottleController",
        "params": [],
    });

    let response = client
        .post(rpc_url)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| http_error(format!("RPC request failed: {error}"), None, None))?;

    let status = response.status().as_u16();
    let body_bytes = response
        .bytes()
        .await
        .map_err(|error| http_error(format!("RPC body read failed: {error}"), None, None))?;
    let parsed: Option<Value> = serde_json::from_slice(&body_bytes).ok();

    if !(200..300).contains(&status) {
        return Err(http_error(
            format!("RPC HTTP request failed with {status}"),
            Some(status),
            Some(body_as_value(parsed.as_ref(), &body_bytes)),
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

    Ok(body_as_value(parsed.as_ref(), &body_bytes))
}

fn http_error(message: String, status: Option<u16>, body: Option<Value>) -> ErrorInfo {
    ErrorInfo {
        message,
        code: Some(Value::String("RPC_HTTP_ERROR".to_string())),
        status,
        body,
    }
}

fn body_as_value(parsed: Option<&Value>, raw: &[u8]) -> Value {
    parsed
        .cloned()
        .unwrap_or_else(|| Value::String(String::from_utf8_lossy(raw).into_owned()))
}
