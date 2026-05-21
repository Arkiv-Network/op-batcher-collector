const DEFAULT_RPC_URL = "http://host.docker.internal:8548";
const DEFAULT_HISTORY_SIZE = 5000;
const DEFAULT_LISTEN_PORT = 28881;
const RPC_TIMEOUT_MS = 900;
const POLL_INTERVAL_MS = 250;

type Env = Record<string, string | undefined>;

export interface CollectorConfig {
  rpcUrl: string;
  historySize: number;
  listenPort: number;
}

export interface SerializedError {
  message: string;
  name?: string;
  code?: unknown;
  status?: unknown;
  data?: unknown;
  body?: unknown;
  cause?: SerializedError;
}

interface HistoryEntryBase {
  second: string;
  collectedAt: string;
  rpcUrl: string;
  durationMs: number;
}

export interface SuccessfulHistoryEntry extends HistoryEntryBase {
  ok: true;
  result: unknown;
}

export interface FailedHistoryEntry extends HistoryEntryBase {
  ok: false;
  error: SerializedError;
}

export type HistoryEntry = SuccessfulHistoryEntry | FailedHistoryEntry;
type NewHistoryEntry =
  | Pick<SuccessfulHistoryEntry, "durationMs" | "ok" | "result">
  | Pick<FailedHistoryEntry, "durationMs" | "error" | "ok">;

export interface RpcCallRequest {
  rpcUrl: string;
  id: string;
  second?: number;
  secondKey?: string;
  timeoutMs?: number;
}

export type RpcCall = (request: RpcCallRequest) => Promise<unknown>;

export interface CollectorLogger {
  error: (...args: unknown[]) => void;
}

export interface CollectorStatus {
  ok: true;
  rpcUrl: string;
  historySize: number;
  retainedEntries: number;
  oldestSecond: string | null;
  latestSecond: string | null;
  currentSecond: string;
  behindSeconds: number;
  collecting: boolean;
  startedAt: string;
}

export type HttpHandler = (request: Request) => Response | Promise<Response>;

interface BunServer {
  url: URL;
}

interface BunRuntime {
  serve(options: { port: number; fetch: HttpHandler }): BunServer;
}

declare const Bun: BunRuntime | undefined;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function parsePositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function createConfig(env: Env = process.env): CollectorConfig {
  return {
    rpcUrl: env.BATCHER_RPC_URL || DEFAULT_RPC_URL,
    historySize: parsePositiveInteger(env.HISTORY_SIZE, DEFAULT_HISTORY_SIZE),
    listenPort: parsePositiveInteger(
      env.COLLECTOR_LISTEN_PORT,
      DEFAULT_LISTEN_PORT,
    ),
  };
}

export function epochSecondNow(): number {
  return Math.floor(Date.now() / 1000);
}

export function secondKey(epochSecond: number): string {
  return new Date(epochSecond * 1000).toISOString().replace(".000Z", "Z");
}

export function normalizeSecond(value: unknown): string | null {
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

export function serializeError(error: unknown): SerializedError {
  if (!error || typeof error !== "object") {
    return {
      message: String(error),
    };
  }

  const maybeError = error as Record<string, unknown>;
  const serialized: SerializedError = {
    message: String(maybeError.message || error),
  };

  if (maybeError.name) {
    serialized.name = String(maybeError.name);
  }

  if (maybeError.code !== undefined) {
    serialized.code = maybeError.code;
  }

  if (maybeError.status !== undefined) {
    serialized.status = maybeError.status;
  }

  if (maybeError.data !== undefined) {
    serialized.data = maybeError.data;
  }

  if (maybeError.body !== undefined) {
    serialized.body = maybeError.body;
  }

  if (maybeError.cause !== undefined) {
    serialized.cause = serializeError(maybeError.cause);
  }

  return serialized;
}

export class HistoryStore<TEntry = HistoryEntry> {
  readonly limit: number;
  readonly entries: Map<string, TEntry>;

  constructor(limit: unknown) {
    this.limit = Math.max(1, parsePositiveInteger(limit, DEFAULT_HISTORY_SIZE));
    this.entries = new Map();
  }

  set(key: string, entry: TEntry): void {
    if (this.entries.has(key)) {
      this.entries.delete(key);
    }

    this.entries.set(key, entry);
    this.trim();
  }

  get(key: string): TEntry | undefined {
    return this.entries.get(key);
  }

  list(): TEntry[] {
    return Array.from(this.entries.values());
  }

  object(): Record<string, TEntry> {
    return Object.fromEntries(this.entries);
  }

  oldestKey(): string | null {
    return this.entries.keys().next().value ?? null;
  }

  latestKey(): string | null {
    let latest: string | null = null;
    for (const key of this.entries.keys()) {
      latest = key;
    }
    return latest;
  }

  trim(): void {
    while (this.entries.size > this.limit) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) {
        return;
      }
      this.entries.delete(oldest);
    }
  }
}

export async function callThrottleController({
  rpcUrl,
  id,
  timeoutMs = RPC_TIMEOUT_MS,
}: RpcCallRequest): Promise<unknown> {
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
    let payload: unknown = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch (error) {
        const parseError = new Error("RPC response was not valid JSON") as Error & {
          body?: string;
          cause?: unknown;
        };
        parseError.cause = error;
        parseError.body = text.slice(0, 1000);
        throw parseError;
      }
    }

    if (!response.ok) {
      const httpError = new Error(`RPC HTTP request failed with ${response.status}`) as Error & {
        body?: unknown;
        code?: string;
        status?: number;
      };
      httpError.code = "RPC_HTTP_ERROR";
      httpError.status = response.status;
      httpError.body = payload ?? text;
      throw httpError;
    }

    const rpcErrorPayload = isRecord(payload) ? payload.error : undefined;
    if (isRecord(rpcErrorPayload)) {
      const message =
        typeof rpcErrorPayload.message === "string"
          ? rpcErrorPayload.message
          : "RPC returned an error";
      const rpcError = new Error(message) as Error & {
        code?: unknown;
        data?: unknown;
      };
      rpcError.code = rpcErrorPayload.code ?? "RPC_ERROR";
      rpcError.data = rpcErrorPayload.data;
      throw rpcError;
    }

    return isRecord(payload) && "result" in payload ? payload.result : payload;
  } finally {
    clearTimeout(timeout);
  }
}

export interface RpcThrottleCollectorOptions {
  rpcUrl: string;
  historySize: number;
  now?: () => number;
  rpcCall?: RpcCall;
  logger?: CollectorLogger;
}

export class RpcThrottleCollector {
  readonly rpcUrl: string;
  readonly history: HistoryStore<HistoryEntry>;
  readonly now: () => number;
  readonly rpcCall: RpcCall;
  readonly logger: CollectorLogger;
  latestEpochSecond: number | null;
  collecting: boolean;
  timer: ReturnType<typeof setInterval> | null;
  readonly startedAt: string;

  constructor({
    rpcUrl,
    historySize,
    now = epochSecondNow,
    rpcCall = callThrottleController,
    logger = console,
  }: RpcThrottleCollectorOptions) {
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

  start(): void {
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

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async collectDueSeconds(): Promise<void> {
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

  async recordRpcResult(epochSecond: number): Promise<void> {
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

  recordError(epochSecond: number, error: SerializedError, durationMs = 0): void {
    this.record(epochSecond, {
      ok: false,
      error,
      durationMs,
    });
  }

  record(epochSecond: number, entry: NewHistoryEntry): void {
    const key = secondKey(epochSecond);
    this.history.set(key, {
      second: key,
      collectedAt: new Date().toISOString(),
      rpcUrl: this.rpcUrl,
      ...entry,
    });
    this.latestEpochSecond = epochSecond;
  }

  status(): CollectorStatus {
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

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

export function createHttpHandler(collector: RpcThrottleCollector): HttpHandler {
  return async function handleRequest(request: Request): Promise<Response> {
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

function installProcessErrorHandlers(logger: CollectorLogger = console): void {
  process.on("uncaughtException", (error) => {
    logger.error("uncaught exception", serializeError(error));
  });

  process.on("unhandledRejection", (reason) => {
    logger.error("unhandled rejection", serializeError(reason));
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function main(): Promise<void> {
  installProcessErrorHandlers();

  const bun = typeof Bun === "undefined" ? undefined : Bun;
  if (!bun) {
    throw new Error("Bun runtime is required to start the HTTP server");
  }

  const config = createConfig();
  const collector = new RpcThrottleCollector({
    rpcUrl: config.rpcUrl,
    historySize: config.historySize,
  });
  collector.start();

  while (true) {
    try {
      const server = bun.serve({
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

const isBunMain =
  typeof Bun !== "undefined" && (import.meta as ImportMeta & { main?: boolean }).main === true;

if (isBunMain) {
  main().catch((error) => {
    console.error("collector main failed", serializeError(error));
  });
}
