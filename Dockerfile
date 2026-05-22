FROM rust:1-alpine3.23 AS build

WORKDIR /app

RUN apk add --no-cache musl-dev

COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked

FROM alpine:3.23

COPY --from=build /app/target/release/op-batcher-collector /usr/local/bin/op-batcher-collector

ENV BATCHER_RPC_URL=http://host.docker.internal:8548
ENV HISTORY_SIZE=5000
ENV COLLECTOR_LISTEN_HOST=0.0.0.0
ENV COLLECTOR_LISTEN_PORT=28881
ENV COLLECTOR_WEB_WORKERS=4

EXPOSE 28881

CMD ["op-batcher-collector"]
