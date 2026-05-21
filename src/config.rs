use std::env;

pub const DEFAULT_RPC_URL: &str = "http://host.docker.internal:8548";
pub const DEFAULT_HISTORY_SIZE: usize = 5000;
pub const DEFAULT_LISTEN_HOST: &str = "0.0.0.0";
pub const DEFAULT_LISTEN_PORT: u16 = 28881;
pub const DEFAULT_WEB_WORKERS: usize = 4;

#[derive(Clone, Debug)]
pub struct Config {
    pub rpc_url: String,
    pub history_size: usize,
    pub listen_host: String,
    pub listen_port: u16,
    pub web_workers: usize,
}

pub fn create_config() -> Config {
    let rpc_url = env::var("BATCHER_RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
    let history_size = parse_positive_usize_env("HISTORY_SIZE", DEFAULT_HISTORY_SIZE);
    let listen_host =
        env::var("COLLECTOR_LISTEN_HOST").unwrap_or_else(|_| DEFAULT_LISTEN_HOST.to_string());
    let listen_port = parse_positive_u16_env("COLLECTOR_LISTEN_PORT", DEFAULT_LISTEN_PORT);
    let web_workers = parse_positive_usize_env("COLLECTOR_WEB_WORKERS", DEFAULT_WEB_WORKERS);

    Config {
        rpc_url,
        history_size,
        listen_host,
        listen_port,
        web_workers,
    }
}

fn parse_positive_usize_env(name: &str, fallback: usize) -> usize {
    match env::var(name) {
        Ok(raw) => parse_positive_usize(name, &raw),
        Err(_) => fallback,
    }
}

fn parse_positive_u16_env(name: &str, fallback: u16) -> u16 {
    match env::var(name) {
        Ok(raw) => parse_positive_u16(name, &raw),
        Err(_) => fallback,
    }
}

fn parse_positive_usize(name: &str, raw: &str) -> usize {
    let parsed: usize = raw
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a positive integer, got {raw:?}"));
    if parsed == 0 {
        panic!("{name} must be greater than zero, got {raw:?}");
    }
    parsed
}

fn parse_positive_u16(name: &str, raw: &str) -> u16 {
    let parsed: u16 = raw.parse().unwrap_or_else(|_| {
        panic!("{name} must be an integer between 1 and 65535, got {raw:?}")
    });
    if parsed == 0 {
        panic!("{name} must be greater than zero, got {raw:?}");
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_positive_values_accept_positive_integers() {
        assert_eq!(parse_positive_usize("HISTORY_SIZE", "42"), 42);
        assert_eq!(parse_positive_u16("COLLECTOR_LISTEN_PORT", "28881"), 28881);
    }

    #[test]
    #[should_panic(expected = "HISTORY_SIZE must be a positive integer")]
    fn parse_positive_usize_panics_on_non_integer() {
        parse_positive_usize("HISTORY_SIZE", "abc");
    }

    #[test]
    #[should_panic(expected = "HISTORY_SIZE must be greater than zero")]
    fn parse_positive_usize_panics_on_zero() {
        parse_positive_usize("HISTORY_SIZE", "0");
    }

    #[test]
    #[should_panic(expected = "COLLECTOR_LISTEN_PORT must be an integer between 1 and 65535")]
    fn parse_positive_u16_panics_on_overflow() {
        parse_positive_u16("COLLECTOR_LISTEN_PORT", "70000");
    }
}
