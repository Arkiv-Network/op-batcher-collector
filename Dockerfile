FROM node:22-slim AS build

WORKDIR /app

COPY package*.json tsconfig.json ./
RUN npm ci --include=dev
COPY src ./src
RUN npm run build

FROM oven/bun:1

WORKDIR /app

COPY --from=build /app/dist ./dist

ENV BATCHER_RPC_URL=http://host.docker.internal:8548
ENV HISTORY_SIZE=5000
ENV COLLECTOR_LISTEN_PORT=28881

EXPOSE 28881

CMD ["bun", "dist/collector.js"]
