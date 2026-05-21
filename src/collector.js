const DEFAULT_RPC_URL = "http://host.docker.internal:8548";
const DEFAULT_HISTORY_SIZE = 5000;
const DEFAULT_LISTEN_PORT = 28881;
const RPC_TIMEOUT_MS = 900;
const POLL_INTERVAL_MS = 250;

export function parsePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function createConfig(env = process.env) {
  return {
    rpcUrl: env.BATCHER_RPC_URL || DEFAULT_RPC_URL,
    historySize: parsePositiveInteger(env.HISTORY_SIZE, DEFAULT_HISTORY_SIZE),
    listenPort: parsePositiveInteger(
      env.COLLECTOR_LISTEN_PORT,
      DEFAULT_LISTEN_PORT,
    ),
  };
}

export function epochSecondNow() {
  return Math.floor(Date.now() / 1000);
}

export function secondKey(epochSecond) {
  return new Date(epochSecond * 1000).toISOString().replace(".000Z", "Z");
}

export function normalizeSecond(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const raw = String(value).trim();
  if (/^-?\d+$/.test(raw)) {
    return secondKey(Number.parseInt(raw, 10));
  }

  const parsed = Date.parse(raw);
  if (Number.isNaN(parsed)) {
    return null;
  }

  return secondKey(Math.floor(parsed / 1000));
}

export function serializeError(error) {
  if (!error || typeof error !== "object") {
    return {
      message: String(error),
    };
  }

  const serialized = {
    message: String(error.message || error),
  };

  if (error.name) {
    serialized.name = String(error.name);
  }

  if (error.code !== undefined) {
    serialized.code = error.code;
  }

  if (error.status !== undefined) {
    serialized.status = error.status;
  }

  if (error.data !== undefined) {
    serialized.data = error.data;
  }

  if (error.body !== undefined) {
    serialized.body = error.body;
  }

  if (error.cause !== undefined) {
    serialized.cause = serializeError(error.cause);
  }

  return serialized;
}

export class HistoryStore {
  constructor(limit) {
    this.limit = Math.max(1, parsePositiveInteger(limit, DEFAULT_HISTORY_SIZE));
    this.entries = new Map();
  }

  set(key, entry) {
    if (this.entries.has(key)) {
      this.entries.delete(key);
    }

    this.entries.set(key, entry);
    this.trim();
  }

  get(key) {
    return this.entries.get(key);
  }

  list() {
    return Array.from(this.entries.values());
  }

  object() {
    return Object.fromEntries(this.entries);
  }

  oldestKey() {
    return this.entries.keys().next().value ?? null;
  }

  latestKey() {
    let latest = null;
    for (const key of this.entries.keys()) {
      latest = key;
    }
    return latest;
  }

  trim() {
    while (this.entries.size > this.limit) {
      const oldest = this.entries.keys().next().value;
      this.entries.delete(oldest);
    }
  }
}

export async function callThrottleController({ rpcUrl, id, timeoutMs = RPC_TIMEOUT_MS }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(rpcUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id,
        method: "admin_getThrottleController",
        params: [],
      }),
      signal: controller.signal,
    });

    const text = await response.text();
    let payload = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch (error) {
        const parseError = new Error("RPC response was not valid JSON");
        parseError.cause = error;
        parseError.body = text.slice(0, 1000);
        throw parseError;
      }
    }

    if (!response.ok) {
      const httpError = new Error(`RPC HTTP request failed with ${response.status}`);
      httpError.code = "RPC_HTTP_ERROR";
      httpError.status = response.status;
      httpError.body = payload ?? text;
      throw httpError;
    }

    if (payload?.error) {
      const rpcError = new Error(payload.error.message || "RPC returned an error");
      rpcError.code = payload.error.code ?? "RPC_ERROR";
      rpcError.data = payload.error.data;
      throw rpcError;
    }

    return payload?.result ?? payload;
  } finally {
    clearTimeout(timeout);
  }
}

export class RpcThrottleCollector {
  constructor({
    rpcUrl,
    historySize,
    now = epochSecondNow,
    rpcCall = callThrottleController,
    logger = console,
  }) {
    this.rpcUrl = rpcUrl;
    this.history = new HistoryStore(historySize);
    this.now = now;
    this.rpcCall = rpcCall;
    this.logger = logger;
    this.latestEpochSecond = null;
    this.collecting = false;
    this.timer = null;
    this.startedAt = new Date().toISOString();
  }

  start() {
    if (this.timer) {
      return;
    }

    const run = () => {
      this.collectDueSeconds().catch((error) => {
        this.logger.error("collector tick failed", serializeError(error));
      });
    };

    run();
    this.timer = setInterval(run, POLL_INTERVAL_MS);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async collectDueSeconds() {
    if (this.collecting) {
      return;
    }

    this.collecting = true;
    try {
      const currentSecond = this.now();
      const firstDueSecond =
        this.latestEpochSecond === null ? currentSecond : this.latestEpochSecond + 1;
      const earliestRetainedSecond = currentSecond - this.history.limit + 1;
      const startSecond = Math.max(firstDueSecond, earliestRetainedSecond);

      if (startSecond > currentSecond) {
        return;
      }

      for (let epochSecond = startSecond; epochSecond < currentSecond; epochSecond += 1) {
        this.recordError(epochSecond, {
          code: "COLLECTOR_BEHIND",
          message: "Collector fell behind before this second could be polled",
        });
      }

      await this.recordRpcResult(currentSecond);
    } finally {
      this.collecting = false;
    }
  }

  async recordRpcResult(epochSecond) {
    const key = secondKey(epochSecond);
    const startedAtMs = Date.now();

    try {
      const result = await this.rpcCall({
        rpcUrl: this.rpcUrl,
        id: key,
        second: epochSecond,
        secondKey: key,
      });

      this.record(epochSecond, {
        ok: true,
        result,
        durationMs: Date.now() - startedAtMs,
      });
    } catch (error) {
      this.recordError(epochSecond, serializeError(error), Date.now() - startedAtMs);
    }
  }

  recordError(epochSecond, error, durationMs = 0) {
    this.record(epochSecond, {
      ok: false,
      error,
      durationMs,
    });
  }

  record(epochSecond, entry) {
    const key = secondKey(epochSecond);
    this.history.set(key, {
      second: key,
      collectedAt: new Date().toISOString(),
      rpcUrl: this.rpcUrl,
      ...entry,
    });
    this.latestEpochSecond = epochSecond;
  }

  status() {
    const currentSecond = this.now();
    return {
      ok: true,
      rpcUrl: this.rpcUrl,
      historySize: this.history.limit,
      retainedEntries: this.history.entries.size,
      oldestSecond: this.history.oldestKey(),
      latestSecond: this.history.latestKey(),
      currentSecond: secondKey(currentSecond),
      behindSeconds:
        this.latestEpochSecond === null
          ? 0
          : Math.max(0, currentSecond - this.latestEpochSecond),
      collecting: this.collecting,
      startedAt: this.startedAt,
    };
  }
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

export function createHttpHandler(collector) {
  return async function handleRequest(request) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname.replace(/\/+$/, "") || "/";

      if (request.method !== "GET") {
        return jsonResponse(
          {
            ok: false,
            error: {
              message: "Only GET requests are supported",
            },
          },
          405,
        );
      }

      if (pathname === "/" || pathname === "/health" || pathname === "/status") {
        return jsonResponse({
          ...collector.status(),
          endpoints: ["/health", "/latest", "/history", "/history?second=<datetime>"],
        });
      }

      if (pathname === "/latest") {
        const latestSecond = collector.history.latestKey();
        return jsonResponse({
          ok: true,
          second: latestSecond,
          entry: latestSecond ? collector.history.get(latestSecond) : null,
        });
      }

      if (pathname === "/history") {
        const requestedSecond = normalizeSecond(url.searchParams.get("second"));
        if (url.searchParams.has("second")) {
          if (!requestedSecond) {
            return jsonResponse(
              {
                ok: false,
                error: {
                  message: "Invalid second. Use an ISO datetime second or epoch second.",
                },
              },
              400,
            );
          }

          const entry = collector.history.get(requestedSecond) ?? null;
          return jsonResponse({
            ok: entry !== null,
            second: requestedSecond,
            entry,
          }, entry ? 200 : 404);
        }

        return jsonResponse({
          ok: true,
          count: collector.history.entries.size,
          oldestSecond: collector.history.oldestKey(),
          latestSecond: collector.history.latestKey(),
          history: collector.history.object(),
        });
      }

      if (pathname.startsWith("/history/")) {
        const requestedSecond = normalizeSecond(decodeURIComponent(pathname.slice(9)));
        if (!requestedSecond) {
          return jsonResponse(
            {
              ok: false,
              error: {
                message: "Invalid second. Use an ISO datetime second or epoch second.",
              },
            },
            400,
          );
        }

        const entry = collector.history.get(requestedSecond) ?? null;
        return jsonResponse({
          ok: entry !== null,
          second: requestedSecond,
          entry,
        }, entry ? 200 : 404);
      }

      return jsonResponse(
        {
          ok: false,
          error: {
            message: "Not found",
          },
        },
        404,
      );
    } catch (error) {
      return jsonResponse(
        {
          ok: false,
          error: serializeError(error),
        },
        500,
      );
    }
  };
}

function installProcessErrorHandlers(logger = console) {
  process.on("uncaughtException", (error) => {
    logger.error("uncaught exception", serializeError(error));
  });

  process.on("unhandledRejection", (reason) => {
    logger.error("unhandled rejection", serializeError(reason));
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function main() {
  installProcessErrorHandlers();

  const config = createConfig();
  const collector = new RpcThrottleCollector({
    rpcUrl: config.rpcUrl,
    historySize: config.historySize,
  });
  collector.start();

  while (true) {
    try {
      const server = Bun.serve({
        port: config.listenPort,
        fetch: createHttpHandler(collector),
      });

      console.log(
        JSON.stringify({
          message: "op-batcher collector listening",
          url: server.url.toString(),
          rpcUrl: config.rpcUrl,
          historySize: config.historySize,
        }),
      );
      return;
    } catch (error) {
      console.error("failed to start HTTP server; retrying", serializeError(error));
      await sleep(5000);
    }
  }
}

if (typeof Bun !== "undefined" && import.meta.main) {
  main().catch((error) => {
    console.error("collector main failed", serializeError(error));
  });
}
