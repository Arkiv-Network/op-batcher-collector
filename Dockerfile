# Previous runtime reference: oven/bun:1

FROM rust:1-slim AS build

WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=build /app/target/release/op-batcher-collector /usr/local/bin/op-batcher-collector

ENV BATCHER_RPC_URL=http://host.docker.internal:8548
ENV HISTORY_SIZE=5000
ENV COLLECTOR_LISTEN_HOST=0.0.0.0
ENV COLLECTOR_LISTEN_PORT=28881

EXPOSE 28881

CMD ["op-batcher-collector"]
