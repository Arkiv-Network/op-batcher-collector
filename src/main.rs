mod config;
mod model;
mod scraper;
mod server;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use config::create_config;
use model::{CollectorState, HistoryStore, SharedCollector, now_iso_second};
use scraper::RPC_TIMEOUT_MS;

fn main() {
    install_process_panic_handler();

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
