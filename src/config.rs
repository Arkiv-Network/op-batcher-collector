use std::num::{NonZeroU16, NonZeroUsize};

use serde::Deserialize;

const DEFAULT_RPC_URL: &str = "http://host.docker.internal:8548";
const DEFAULT_LISTEN_HOST: &str = "0.0.0.0";
const DEFAULT_HISTORY_SIZE: NonZeroUsize = NonZeroUsize::new(5000).unwrap();
const DEFAULT_LISTEN_PORT: NonZeroU16 = NonZeroU16::new(28881).unwrap();
const DEFAULT_WEB_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(rename = "batcher_rpc_url", default = "default_rpc_url")]
    pub rpc_url: String,
    #[serde(default = "default_history_size")]
    pub history_size: NonZeroUsize,
    #[serde(rename = "collector_listen_host", default = "default_listen_host")]
    pub listen_host: String,
    #[serde(rename = "collector_listen_port", default = "default_listen_port")]
    pub listen_port: NonZeroU16,
    #[serde(rename = "collector_web_workers", default = "default_web_workers")]
    pub web_workers: NonZeroUsize,
}

pub fn create_config() -> Config {
    envy::from_env::<Config>().unwrap_or_else(|err| panic!("invalid config: {err}"))
}

fn default_rpc_url() -> String {
    DEFAULT_RPC_URL.to_string()
}

fn default_listen_host() -> String {
    DEFAULT_LISTEN_HOST.to_string()
}

fn default_history_size() -> NonZeroUsize {
    DEFAULT_HISTORY_SIZE
}

fn default_listen_port() -> NonZeroU16 {
    DEFAULT_LISTEN_PORT
}

fn default_web_workers() -> NonZeroUsize {
    DEFAULT_WEB_WORKERS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Result<Config, envy::Error> {
        envy::from_iter(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        )
    }

    #[test]
    fn defaults_apply_when_env_is_empty() {
        let config = from_pairs([]).unwrap();
        assert_eq!(config.rpc_url, DEFAULT_RPC_URL);
        assert_eq!(config.history_size, DEFAULT_HISTORY_SIZE);
        assert_eq!(config.listen_host, DEFAULT_LISTEN_HOST);
        assert_eq!(config.listen_port, DEFAULT_LISTEN_PORT);
        assert_eq!(config.web_workers, DEFAULT_WEB_WORKERS);
    }

    #[test]
    fn parses_valid_overrides() {
        let config =
            from_pairs([("HISTORY_SIZE", "42"), ("COLLECTOR_LISTEN_PORT", "9000")]).unwrap();
        assert_eq!(config.history_size.get(), 42);
        assert_eq!(config.listen_port.get(), 9000);
    }

    #[test]
    fn rejects_non_integer() {
        assert!(from_pairs([("HISTORY_SIZE", "abc")]).is_err());
    }

    #[test]
    fn rejects_zero_for_nonzero_field() {
        assert!(from_pairs([("HISTORY_SIZE", "0")]).is_err());
    }

    #[test]
    fn rejects_port_overflow() {
        assert!(from_pairs([("COLLECTOR_LISTEN_PORT", "70000")]).is_err());
    }
}
