# op-batcher-collector

Standard-library Rust collector for the `admin_getThrottleController` JSON-RPC
method.

The collector uses two long-running threads:

- a query thread that polls the configured JSON-RPC endpoint once per UTC
  datetime second
- a web thread that serves the HTTP API and spawns short-lived request handlers

Entries are retained by second:

```json
{
  "2026-05-21T10:00:00Z": {
    "second": "2026-05-21T10:00:00Z",
    "ok": true,
    "result": {}
  }
}
```

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

## Build and run locally

```sh
cargo build --release
./target/release/op-batcher-collector
```

The RPC client intentionally uses only Rust's standard library, so
`BATCHER_RPC_URL` must use `http://`. TLS-backed `https://` RPC endpoints need a
local plain-HTTP proxy or a future implementation that permits TLS crates.

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
| `GET /health` | Collector status and retained range. |
| `GET /status` | Alias for `/health`. |
| `GET /latest` | Latest retained entry. |
| `GET /history` | Full retained history keyed by datetime second. |
| `GET /history?second=2026-05-21T10:00:00Z` | Lookup one retained second. |
| `GET /history/2026-05-21T10%3A00%3A00Z` | Path-style lookup for one retained second. |

The `second` lookup accepts ISO datetimes or epoch seconds and normalizes them
to UTC second precision.
