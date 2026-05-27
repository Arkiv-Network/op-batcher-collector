mod config;
mod model;
mod scraper;
mod server;

use std::ffi::{OsStr, OsString};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use config::create_config;
use model::{CollectorState, HistoryStore, SharedCollector, now_iso_second};
use scraper::RPC_TIMEOUT_MS;

fn main() {
    install_process_panic_handler();

    match startup_action(std::env::args_os().skip(1)) {
        StartupAction::Run => {}
        StartupAction::PrintVersion => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            return;
        }
        StartupAction::Error(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    }

    let config = create_config();
    let worker_threads = config.web_workers.get();
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(RPC_TIMEOUT_MS))
        .build()
        .expect("failed to build reqwest client");
    let shared = Arc::new(SharedCollector {
        state: Mutex::new(CollectorState {
            history: HistoryStore::new(config.history_size.get()),
            latest_epoch_second: None,
            collecting: false,
            started_at: now_iso_second(),
        }),
        config,
        http_client,
    });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async move {
        let collector = Arc::clone(&shared);
        tokio::spawn(async move { scraper::poll_loop(collector).await });
        server::run_server(shared).await;
    });
}

fn install_process_panic_handler() {
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("uncaught panic: {panic_info}");
    }));
}

#[derive(Debug, PartialEq, Eq)]
enum StartupAction {
    Run,
    PrintVersion,
    Error(String),
}

fn startup_action<I>(args: I) -> StartupAction
where
    I: IntoIterator<Item = OsString>,
{
    let mut saw_version = false;

    for arg in args {
        if arg == OsStr::new("-v") || arg == OsStr::new("--version") {
            saw_version = true;
            continue;
        }

        return StartupAction::Error(format!(
            "unsupported command-line argument: {}. Use environment variables to configure op-batcher-collector; command-line arguments are not supported.",
            arg.to_string_lossy()
        ));
    }

    if saw_version {
        StartupAction::PrintVersion
    } else {
        StartupAction::Run
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn no_arguments_runs_service() {
        assert_eq!(startup_action(args(&[])), StartupAction::Run);
    }

    #[test]
    fn version_arguments_print_version() {
        assert_eq!(startup_action(args(&["-v"])), StartupAction::PrintVersion);
        assert_eq!(
            startup_action(args(&["--version"])),
            StartupAction::PrintVersion
        );
    }

    #[test]
    fn invalid_argument_returns_error() {
        match startup_action(args(&["--rpc-url", "http://localhost:8545"])) {
            StartupAction::Error(message) => {
                assert!(message.contains("--rpc-url"));
                assert!(message.contains("environment variables"));
            }
            action => panic!("expected error action, got {action:?}"),
        }
    }

    #[test]
    fn version_with_invalid_argument_returns_error() {
        assert!(matches!(
            startup_action(args(&["--version", "--rpc-url"])),
            StartupAction::Error(_)
        ));
    }
}
