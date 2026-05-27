# op-batcher-collector

Async collector for the `admin_getThrottleController` JSON-RPC method.

Built on tokio + axum + reqwest. A single multi-threaded tokio runtime hosts
two long-running tasks that share the worker pool:

- a poll task that calls the configured JSON-RPC endpoint once per UTC
  datetime second
- an axum HTTP server that exposes the read API

Entries are retained by second:

```json
{
  "2026-05-21T10:00:00Z": {
    "second": "2026-05-21T10:00:00Z",
    "collectedAt": "2026-05-21T10:00:00Z",
    "durationMs": "12",
    "ok": true,
    "result": {}
  }
}
```

Failed seconds carry the same envelope with `"ok": false` and an `error` object
(`message`, optional `code`, `status`, `body`) instead of `result`.

If polling falls behind, the collector writes error entries for missed seconds
so the retained timeline has no empty seconds. RPC failures are stored as error
entries instead of crashing the process.

## Environment

| Variable | Default | Description |
| --- | --- | --- |
| `BATCHER_RPC_URL` | `http://host.docker.internal:8548` | Plain HTTP JSON-RPC endpoint to poll. |
| `HISTORY_SIZE` | `5000` | Number of datetime-second entries to retain. |
| `COLLECTOR_LISTEN_HOST` | `0.0.0.0` | HTTP API listen host. |
| `COLLECTOR_LISTEN_PORT` | `28881` | HTTP API listen port. |
| `COLLECTOR_WEB_WORKERS` | `4` | Worker threads in the tokio runtime. |

Integer variables must be positive; zero or out-of-range values cause the
process to panic at startup with a message naming the variable.

## Build and run locally

```sh
cargo build --release
./target/release/op-batcher-collector
```

Print the collector version with `-v` or `--version`:

```sh
./target/release/op-batcher-collector --version
```

Runtime configuration is environment-variable only. Any other command-line
argument exits with an error that points users back to environment variables.

`reqwest` is compiled without TLS features, so `BATCHER_RPC_URL` must use
`http://`. To target an `https://` endpoint, enable a TLS feature on the
`reqwest` dependency (e.g. `rustls-tls`) or front the RPC with a local
plain-HTTP proxy.

## Run with Docker

Pull the published image from GitHub Container Registry:

```sh
docker pull ghcr.io/arkiv-network/op-batcher-collector:latest
docker run --rm -p 28881:28881 \
  -e BATCHER_RPC_URL=http://host.docker.internal:8548 \
  -e HISTORY_SIZE=5000 \
  -e COLLECTOR_LISTEN_HOST=0.0.0.0 \
  -e COLLECTOR_LISTEN_PORT=28881 \
  ghcr.io/arkiv-network/op-batcher-collector:latest
```

Build the image locally for development:

```sh
docker build -t op-batcher-collector .
docker run --rm -p 28881:28881 \
  -e BATCHER_RPC_URL=http://host.docker.internal:8548 \
  -e HISTORY_SIZE=5000 \
  -e COLLECTOR_LISTEN_HOST=0.0.0.0 \
  -e COLLECTOR_LISTEN_PORT=28881 \
  op-batcher-collector
```

## HTTP API

| Endpoint | Description |
| --- | --- |
| `GET /` | Collector status and retained range. |
| `GET /health` | Alias for `/`. |
| `GET /status` | Alias for `/`. |
| `GET /latest` | Latest retained entry. |
| `GET /history` | Full retained history keyed by datetime second. |
| `GET /history?second=2026-05-21T10:00:00Z` | Lookup one retained second. |
| `GET /history/2026-05-21T10%3A00%3A00Z` | Path-style lookup for one retained second. |

The `second` lookup accepts ISO datetimes or epoch seconds and normalizes them
to UTC second precision.
