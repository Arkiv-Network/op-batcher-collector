mod model;
mod scraper;
mod server;

use std::sync::{Arc, Mutex};
use std::thread;

use model::{create_config, now_iso_second, CollectorState, HistoryStore, SharedCollector};

fn main() {
    install_process_panic_handler();

    let config = create_config();
    let shared = Arc::new(SharedCollector {
        state: Mutex::new(CollectorState {
            history: HistoryStore::new(config.history_size),
            latest_epoch_second: None,
            collecting: false,
            started_at: now_iso_second(),
        }),
        config,
    });

    let collector = Arc::clone(&shared);
    let collector_thread = thread::Builder::new()
        .name("collector-query".to_string())
        .spawn(move || scraper::poll_loop(collector))
        .expect("failed to spawn collector query thread");

    let server = Arc::clone(&shared);
    let server_thread = thread::Builder::new()
        .name("collector-web".to_string())
        .spawn(move || server::run_server(server))
        .expect("failed to spawn collector web thread");

    if let Err(error) = server_thread.join() {
        eprintln!("web server thread failed: {error:?}");
    }
    if let Err(error) = collector_thread.join() {
        eprintln!("collector query thread failed: {error:?}");
    }
}

fn install_process_panic_handler() {
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("uncaught panic: {panic_info}");
    }));
}
