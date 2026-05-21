use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::model::{
    SharedCollector, epoch_second_now, normalize_second, second_key,
};

pub async fn run_server(shared: Arc<SharedCollector>) {
    let bind_address = format!(
        "{}:{}",
        shared.config.listen_host, shared.config.listen_port
    );

    let listener = match TcpListener::bind(&bind_address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("failed to bind HTTP server on {bind_address}: {error}");
            return;
        }
    };

    println!(
        "{}",
        json!({
            "message": "op-batcher collector listening",
            "url": format!("http://{bind_address}/"),
            "rpcUrl": shared.config.rpc_url,
            "historySize": shared.config.history_size,
            "listenHost": shared.config.listen_host,
            "listenPort": shared.config.listen_port,
            "webWorkers": shared.config.web_workers,
        })
    );

    let app = Router::new()
        .route("/", get(status_handler))
        .route("/health", get(status_handler))
        .route("/status", get(status_handler))
        .route("/latest", get(latest_handler))
        .route("/history", get(history_handler))
        .route("/history/{second}", get(history_lookup_handler))
        .fallback(not_found_handler)
        .with_state(shared);

    if let Err(error) = axum::serve(listener, app).await {
        eprintln!("HTTP server failed: {error}");
    }
}

async fn status_handler(State(shared): State<Arc<SharedCollector>>) -> Json<Value> {
    let state = shared.state.lock().expect("collector state lock poisoned");
    let current_second = epoch_second_now();
    let behind_seconds = state
        .latest_epoch_second
        .map(|latest| (current_second - latest).max(0))
        .unwrap_or(0);

    Json(json!({
        "ok": true,
        "rpcUrl": shared.config.rpc_url,
        "historySize": state.history.limit,
        "retainedEntries": state.history.entries.len(),
        "oldestSecond": state.history.oldest_key(),
        "latestSecond": state.history.latest_key(),
        "currentSecond": second_key(current_second),
        "behindSeconds": behind_seconds,
        "collecting": state.collecting,
        "startedAt": state.started_at,
        "endpoints": ["/health", "/latest", "/history", "/history?second=<datetime>"],
    }))
}

async fn latest_handler(State(shared): State<Arc<SharedCollector>>) -> Json<Value> {
    let state = shared.state.lock().expect("collector state lock poisoned");
    let latest_second = state.history.latest_key();
    let entry = latest_second
        .and_then(|second| state.history.get(second))
        .map(|entry| entry.to_value())
        .unwrap_or(Value::Null);

    Json(json!({
        "ok": true,
        "second": latest_second,
        "entry": entry,
    }))
}

async fn history_handler(
    State(shared): State<Arc<SharedCollector>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(raw_second) = params.get("second") {
        return lookup_response(&shared, raw_second);
    }

    let state = shared.state.lock().expect("collector state lock poisoned");
    let history: serde_json::Map<String, Value> = state
        .history
        .entries
        .iter()
        .map(|entry| (entry.second.clone(), entry.to_value()))
        .collect();

    json_response(
        StatusCode::OK,
        json!({
            "ok": true,
            "count": state.history.entries.len(),
            "oldestSecond": state.history.oldest_key(),
            "latestSecond": state.history.latest_key(),
            "history": history,
        }),
    )
}

async fn history_lookup_handler(
    State(shared): State<Arc<SharedCollector>>,
    Path(raw_second): Path<String>,
) -> Response {
    lookup_response(&shared, &raw_second)
}

async fn not_found_handler() -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        json!({ "ok": false, "error": { "message": "Not found" } }),
    )
}

fn lookup_response(shared: &SharedCollector, raw_second: &str) -> Response {
    let Some(second) = normalize_second(raw_second) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "ok": false,
                "error": { "message": "Invalid second. Use an ISO datetime second or epoch second." },
            }),
        );
    };

    let state = shared.state.lock().expect("collector state lock poisoned");
    let entry = state.history.get(&second);
    let status = if entry.is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    };
    json_response(
        status,
        json!({
            "ok": entry.is_some(),
            "second": second,
            "entry": entry.map(|entry| entry.to_value()).unwrap_or(Value::Null),
        }),
    )
}

type Response = (StatusCode, Json<Value>);

fn json_response(status: StatusCode, body: Value) -> Response {
    (status, Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CollectorState, Config, HistoryStore, record_locked};
    use std::sync::Mutex;

    fn test_shared() -> Arc<SharedCollector> {
        Arc::new(SharedCollector {
            config: Config {
                rpc_url: "http://rpc.example".to_string(),
                history_size: 10,
                listen_host: "127.0.0.1".to_string(),
                listen_port: 28881,
                web_workers: 4,
            },
            state: Mutex::new(CollectorState {
                history: HistoryStore::new(10),
                latest_epoch_second: Some(300),
                collecting: false,
                started_at: second_key(300),
            }),
        })
    }

    #[tokio::test]
    async fn history_lookup_returns_stored_entry() {
        let shared = test_shared();
        {
            let mut state = shared.state.lock().unwrap();
            record_locked(
                "http://rpc.example",
                &mut state,
                300,
                true,
                Some(json!({"value": "stored"})),
                None,
                37,
            );
        }

        let (status, Json(body)) =
            history_lookup_handler(State(shared.clone()), Path("1970-01-01T00:05:00Z".to_string()))
                .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["entry"]["result"]["value"], json!("stored"));
    }

    #[tokio::test]
    async fn history_lookup_rejects_invalid_second() {
        let shared = test_shared();
        let (status, Json(body)) =
            history_lookup_handler(State(shared), Path("not-a-second".to_string())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["ok"], json!(false));
    }
}
