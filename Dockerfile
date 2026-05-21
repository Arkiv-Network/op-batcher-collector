FROM oven/bun:1

WORKDIR /app

COPY package.json ./
COPY src ./src

ENV BATCHER_RPC_URL=http://host.docker.internal:8548
ENV HISTORY_SIZE=5000
ENV COLLECTOR_LISTEN_PORT=28881

EXPOSE 28881

CMD ["bun", "src/collector.js"]
