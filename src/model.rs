use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::config::Config;

#[derive(Clone, Debug)]
pub struct ErrorInfo {
    pub message: String,
    pub code: Option<Value>,
    pub status: Option<u16>,
    pub body: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub second: String,
    pub collected_at: String,
    pub rpc_url: String,
    pub duration_ms: u128,
    pub ok: bool,
    pub result: Option<Value>,
    pub error: Option<ErrorInfo>,
}

#[derive(Debug)]
pub struct HistoryStore {
    pub limit: usize,
    pub entries: VecDeque<HistoryEntry>,
}

#[derive(Debug)]
pub struct CollectorState {
    pub history: HistoryStore,
    pub latest_epoch_second: Option<i64>,
    pub collecting: bool,
    pub started_at: String,
}

#[derive(Debug)]
pub struct SharedCollector {
    pub config: Config,
    pub http_client: reqwest::Client,
    pub state: Mutex<CollectorState>,
}

impl HistoryStore {
    pub fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            entries: VecDeque::new(),
        }
    }

    pub fn set(&mut self, entry: HistoryEntry) {
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

    pub fn get(&self, second: &str) -> Option<&HistoryEntry> {
        self.entries.iter().find(|entry| entry.second == second)
    }

    pub fn oldest_key(&self) -> Option<&str> {
        self.entries.front().map(|entry| entry.second.as_str())
    }

    pub fn latest_key(&self) -> Option<&str> {
        self.entries.back().map(|entry| entry.second.as_str())
    }
}

impl ErrorInfo {
    pub fn to_value(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("message".to_string(), Value::String(self.message.clone()));
        if let Some(code) = &self.code {
            map.insert("code".to_string(), code.clone());
        }
        if let Some(status) = self.status {
            map.insert("status".to_string(), Value::from(status));
        }
        if let Some(body) = &self.body {
            map.insert("body".to_string(), body.clone());
        }
        Value::Object(map)
    }
}

impl HistoryEntry {
    pub fn to_value(&self) -> Value {
        let mut value = json!({
            "second": self.second,
            "collectedAt": self.collected_at,
            "rpcUrl": self.rpc_url,
            "durationMs": self.duration_ms.to_string(),
            "ok": self.ok,
        });
        let map = value
            .as_object_mut()
            .expect("history entry serializes as object");
        if self.ok {
            map.insert(
                "result".to_string(),
                self.result.clone().unwrap_or(Value::Null),
            );
        } else {
            map.insert(
                "error".to_string(),
                self.error
                    .as_ref()
                    .map(ErrorInfo::to_value)
                    .unwrap_or_else(|| json!({ "message": "Unknown error" })),
            );
        }
        value
    }
}

pub fn record_locked(
    rpc_url: &str,
    state: &mut CollectorState,
    epoch_second: i64,
    ok: bool,
    result: Option<Value>,
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
        result,
        error,
    });
    state.latest_epoch_second = Some(epoch_second);
}

pub fn epoch_second_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

pub fn now_iso_second() -> String {
    second_key(epoch_second_now())
}

pub fn second_key(epoch_second: i64) -> String {
    let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
    OffsetDateTime::from_unix_timestamp(epoch_second)
        .expect("epoch second within OffsetDateTime range")
        .format(&format)
        .expect("format infallible for fixed description")
}

pub fn normalize_second(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(epoch) = raw.parse::<i64>() {
        return Some(second_key(epoch));
    }
    OffsetDateTime::parse(raw, &Rfc3339)
        .ok()
        .map(|dt| second_key(dt.unix_timestamp()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
                result: Some(json!({})),
                error: None,
            });
        }

        assert_eq!(history.entries.len(), 2);
        assert_eq!(history.oldest_key(), Some(second_key(2).as_str()));
        assert_eq!(history.latest_key(), Some(second_key(3).as_str()));
    }
}
