use std::collections::VecDeque;
use std::env;
use std::fmt::Write as FmtWrite;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_RPC_URL: &str = "http://host.docker.internal:8548";
pub const DEFAULT_HISTORY_SIZE: usize = 5000;
pub const DEFAULT_LISTEN_HOST: &str = "0.0.0.0";
pub const DEFAULT_LISTEN_PORT: u16 = 28881;

#[derive(Clone, Debug)]
pub struct Config {
    pub rpc_url: String,
    pub history_size: usize,
    pub listen_host: String,
    pub listen_port: u16,
}

#[derive(Clone, Debug)]
pub struct ErrorInfo {
    pub message: String,
    pub code_json: Option<String>,
    pub status: Option<u16>,
    pub body_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub second: String,
    pub collected_at: String,
    pub rpc_url: String,
    pub duration_ms: u128,
    pub ok: bool,
    pub result_json: Option<String>,
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

pub fn create_config() -> Config {
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

pub fn record_locked(
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
    let days = epoch_second.div_euclid(86_400);
    let second_of_day = epoch_second.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3600;
    let minute = (second_of_day % 3600) / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

pub fn normalize_second(value: &str) -> Option<String> {
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

pub fn object_json(fields: &[(&str, String)]) -> String {
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

pub fn option_json_string(value: Option<&str>) -> String {
    value.map(json_string).unwrap_or_else(|| "null".to_string())
}

pub fn json_string(value: &str) -> String {
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

pub fn json_value_or_string(value: &str) -> String {
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

pub fn error_info_to_json(error: &ErrorInfo) -> String {
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

pub fn error_payload(message: &str) -> String {
    object_json(&[("message", json_string(message))])
}

pub fn find_json_key_value<'a>(json: &'a str, key: &str) -> Option<&'a str> {
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

pub fn parse_json_value_end(bytes: &[u8], start: usize) -> Option<usize> {
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

pub fn json_string_value(json: &str) -> Option<String> {
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
}
