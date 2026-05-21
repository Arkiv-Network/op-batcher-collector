import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";

import {
  HistoryStore,
  RpcThrottleCollector,
  callThrottleController,
  createConfig,
  createHttpHandler,
  normalizeSecond,
  parsePositiveInteger,
  secondKey,
} from "../dist/collector.js";

async function withJsonServer(handler) {
  const server = createServer(handler);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    url: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

test("parsePositiveInteger uses fallback for invalid values", () => {
  assert.equal(parsePositiveInteger("42", 1), 42);
  assert.equal(parsePositiveInteger("0", 7), 7);
  assert.equal(parsePositiveInteger("-1", 7), 7);
  assert.equal(parsePositiveInteger("abc", 7), 7);
});

test("createConfig applies requested defaults", () => {
  assert.deepEqual(createConfig({}), {
    rpcUrl: "http://host.docker.internal:8548",
    historySize: 5000,
    listenPort: 28881,
  });
});

test("normalizeSecond supports ISO datetime and epoch seconds", () => {
  assert.equal(normalizeSecond("1970-01-01T00:01:40.900Z"), "1970-01-01T00:01:40Z");
  assert.equal(normalizeSecond("100"), "1970-01-01T00:01:40Z");
  assert.equal(normalizeSecond("not-a-date"), null);
});

test("HistoryStore keeps only the newest entries", () => {
  const history = new HistoryStore(2);
  history.set(secondKey(1), { second: secondKey(1) });
  history.set(secondKey(2), { second: secondKey(2) });
  history.set(secondKey(3), { second: secondKey(3) });

  assert.equal(history.entries.size, 2);
  assert.equal(history.oldestKey(), secondKey(2));
  assert.equal(history.latestKey(), secondKey(3));
});

test("collector backfills missed seconds with errors", async () => {
  let now = 100;
  const collector = new RpcThrottleCollector({
    rpcUrl: "http://rpc.example",
    historySize: 10,
    now: () => now,
    rpcCall: async ({ second }) => ({ second }),
    logger: { error() {} },
  });

  await collector.collectDueSeconds();
  now = 103;
  await collector.collectDueSeconds();

  const entries = collector.history.object();
  assert.equal(entries[secondKey(100)].ok, true);
  assert.equal(entries[secondKey(101)].ok, false);
  assert.equal(entries[secondKey(101)].error.code, "COLLECTOR_BEHIND");
  assert.equal(entries[secondKey(102)].ok, false);
  assert.equal(entries[secondKey(103)].ok, true);
});

test("collector stores RPC failures as entries", async () => {
  const collector = new RpcThrottleCollector({
    rpcUrl: "http://rpc.example",
    historySize: 10,
    now: () => 200,
    rpcCall: async () => {
      throw new Error("rpc unavailable");
    },
    logger: { error() {} },
  });

  await collector.collectDueSeconds();

  const entry = collector.history.get(secondKey(200));
  assert.equal(entry.ok, false);
  assert.equal(entry.error.message, "rpc unavailable");
});

test("callThrottleController sends the expected JSON-RPC request", async () => {
  let rpcRequest = null;
  const server = await withJsonServer((request, response) => {
    let body = "";
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => {
      rpcRequest = JSON.parse(body);
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ jsonrpc: "2.0", id: rpcRequest.id, result: { ok: 1 } }));
    });
  });

  try {
    const result = await callThrottleController({
      rpcUrl: server.url,
      id: "test-second",
      timeoutMs: 500,
    });

    assert.deepEqual(result, { ok: 1 });
    assert.equal(rpcRequest.method, "admin_getThrottleController");
    assert.deepEqual(rpcRequest.params, []);
  } finally {
    await server.close();
  }
});

test("callThrottleController treats truthy JSON-RPC error values as failures", async () => {
  const server = await withJsonServer((request, response) => {
    request.resume();
    request.on("end", () => {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ jsonrpc: "2.0", id: "error-test", error: "denied" }));
    });
  });

  try {
    await assert.rejects(
      callThrottleController({
        rpcUrl: server.url,
        id: "error-test",
        timeoutMs: 500,
      }),
      {
        message: "RPC returned an error",
        code: "RPC_ERROR",
      },
    );
  } finally {
    await server.close();
  }
});

test("HTTP handler returns retained history lookups", async () => {
  const collector = new RpcThrottleCollector({
    rpcUrl: "http://rpc.example",
    historySize: 10,
    now: () => 300,
    rpcCall: async () => ({ value: "stored" }),
    logger: { error() {} },
  });
  await collector.collectDueSeconds();

  const handler = createHttpHandler(collector);
  const response = await handler(
    new Request(`http://collector.local/history?second=${secondKey(300)}`),
  );
  const payload = await response.json();

  assert.equal(response.status, 200);
  assert.equal(payload.ok, true);
  assert.equal(payload.second, secondKey(300));
  assert.deepEqual(payload.entry.result, { value: "stored" });
});
