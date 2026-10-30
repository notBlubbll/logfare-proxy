// Mock test suite for logfare-proxy.
//   bun test/run-tests.mjs
// Spawns a mock upstream (port 17999) and the proxy under test (port 17331),
// then drives 17 scenarios through the proxy. No real API is contacted and no
// credentials are used (the mock only checks for "Bearer test-key").
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const MOCK_PORT = Number(process.env.MOCK_PORT || 17999);
const PROXY_PORT = Number(process.env.PROXY_PORT || 17331);
const PLUGIN = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "index.js");
const RUNTIME = process.execPath; // bun or node, whichever runs this file

let hitCount = 0;
const hits = [];

function sse(res, events, opts = {}) {
  res.writeHead(200, { "content-type": "text/event-stream", ...opts.headers });
  let i = 0;
  const tick = () => {
    if (i >= events.length) { res.end(); return; }
    const e = events[i++];
    if (e === "CLOSE") { res.destroy(); return; }
    if (e === "STALL") { setTimeout(tick, 400); return; }
    res.write(typeof e === "string" ? `data: ${e}\n\n` : e);
    setTimeout(tick, opts.gap ?? 10);
  };
  tick();
}

const scenarios = {
  "t-precommit-retry": (req, res) => {
    if (hitCount === 1) {
      sse(res, ['{"error":"overloaded"}', '{"choices":[{"delta":{"content":"SHOULD-NOT-APPEAR"}}]}', "[DONE]"]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"role":"assistant"}}]}',
        '{"choices":[{"delta":{"content":"Hello"}}]}',
        '{"choices":[{"delta":{"content":" world"}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}',
        "[DONE]",
      ]);
    }
  },
  "t-postcommit-drop": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"role":"assistant"}}]}',
      '{"choices":[{"delta":{"content":"AB"}}]}',
      '{"choices":"nope"}',
      "oops not json",
      '{"choices":[{"delta":{"content":"C"}}]}',
      "[DONE]",
    ]);
  },
  "t-error-event-retry": (req, res) => {
    if (hitCount === 1) sse(res, ['{"error":{"message":"model overloaded","type":"server_error"}}', "[DONE]"]);
    else sse(res, ['{"choices":[{"delta":{"content":"ok-after-retry"}}]}', "[DONE]"]);
  },
  "t-giveup": (req, res) => {
    res.writeHead(500, { "content-type": "application/json" });
    res.end('{"error":{"message":"boom","type":"server_error"}}');
  },
  "t-repair": (req, res) => {
    sse(res, [
      '{"choices":[{"index":0,"delta":{"role":"assistant","content":null},"finish_reason":null}]}',
      '{"choices":[{"index":0,"delta":{"content":"R"}}]}',
      '{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  "t-midstream-death": (req, res) => {
    sse(res, ['{"choices":[{"delta":{"role":"assistant"}}]}', '{"choices":[{"delta":{"content":"partial"}}]}', "CLOSE"]);
  },
  "t-nonstream": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"id":"x","choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{}}');
  },
  "t-bulkjson": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"id":"b","choices":[{"index":0,"message":{"role":"assistant","content":"bulk"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}');
  },
  "t-stall-retry": (req, res) => {
    if (hitCount === 1) sse(res, ["STALL", "CLOSE"]);
    else sse(res, ['{"choices":[{"delta":{"content":"after-stall"}}]}', "[DONE]"]);
  },
  "t-200-error-retry": (req, res) => {
    if (hitCount === 1) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"error": {"errors": [{"message": "AiError: AiError: Invalid data for body - reason request body exceeds the maximum supported size (test-id)", "code": 8006}], "success": false, "result": {}, "messages": []}}');
    } else {
      sse(res, ['{"choices":[{"delta":{"content":"same-body-ok"}}]}', "[DONE]"]);
    }
  },
  "t-200-error-always": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"error": {"errors": [{"message": "AiError: AiError: Invalid data for body - reason request body exceeds the maximum supported size (always)", "code": 8006}], "success": false, "result": {}, "messages": []}}');
  },
  "t-inband-rl": (req, res) => {
    if (hitCount === 1) sse(res, ['{"error":{"message":"Serverless token-per-minute limit reached for this model","type":"server_error"}}', "[DONE]"]);
    else sse(res, ['{"choices":[{"delta":{"content":"rl-ok"}}]}', "[DONE]"]);
  },
};

const mock = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    hitCount++;
    let model = "?";
    try { model = JSON.parse(body).model; } catch {}
    hits.push(model);
    const fn = scenarios[model];
    if (!fn) { res.writeHead(404); res.end("no scenario"); return; }
    if (!/^Bearer test-key/.test(req.headers["authorization"] || "")) {
      res.writeHead(401); res.end("mock: missing auth"); return;
    }
    fn(req, res);
  });
});

async function readSSE(body) {
  const r = await fetch(`http://127.0.0.1:${PROXY_PORT}/v1/chat/completions`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer test-key" },
    body: JSON.stringify(body),
  });
  return { status: r.status, retries: r.headers.get("x-lf-retries"), text: await r.text() };
}

const dataLines = (t) => t.split("\n").filter((l) => l.startsWith("data: ")).map((l) => l.slice(6));
const textOf = (t) => dataLines(t).filter((d) => d !== "[DONE]").map((d) => {
  try { return JSON.parse(d)?.choices?.[0]?.delta?.content || ""; } catch { return "<BADJSON>"; }
}).join("");

let pass = 0, fail = 0;
function check(name, cond, extra = "") {
  if (cond) { pass++; console.log(`PASS ${name}`); }
  else { fail++; console.log(`FAIL ${name} ${extra}`); }
}

mock.listen(MOCK_PORT, "127.0.0.1", async () => {
  const proxy = spawn(RUNTIME, [PLUGIN], {
    env: {
      ...process.env,
      LOGFARE_PROXY_PORT: String(PROXY_PORT),
      LOGFARE_PROXY_UPSTREAM: `http://127.0.0.1:${MOCK_PORT}/v1`,
      LOGFARE_PROXY_RL_BUDGET_MS: "9000",
      LOGFARE_PROXY_RL_BACKOFF_MS: "200,300,500",
      LOGFARE_PROXY_BACKOFF_MS: "10,20,30",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  proxy.stdout.on("data", (d) => process.stdout.write(`[proxy] ${d}`));
  proxy.stderr.on("data", (d) => process.stdout.write(`[proxy!] ${d}`));

  for (let i = 0; i < 50; i++) {
    try { const h = await fetch(`http://127.0.0.1:${PROXY_PORT}/healthz`); if (h.ok) break; } catch {}
    await delay(200);
  }

  try {
    hitCount = 0;
    let r = await readSSE({ model: "t-precommit-retry", stream: true });
    check("A: retried once", r.retries === "1", `retries=${r.retries}`);
    check("A: clean content", textOf(r.text) === "Hello world", JSON.stringify(r.text));
    check("A: no leaked attempt-1 text", !r.text.includes("SHOULD-NOT-APPEAR"));
    check("A: upstream hit twice", hits.length === 2, `hits=${hits.length}`);

    hitCount = 0;
    r = await readSSE({ model: "t-postcommit-drop", stream: true });
    check("B: drops mid-stream, content intact", textOf(r.text) === "ABC", JSON.stringify(r.text));
    check("B: no retry after commit", r.retries === "0", `retries=${r.retries}`);

    hitCount = 0;
    r = await readSSE({ model: "t-error-event-retry", stream: true });
    check("C: in-band error pre-commit retried", r.retries === "1" && textOf(r.text) === "ok-after-retry", `retries=${r.retries} text=${textOf(r.text)}`);

    hitCount = 0;
    const hitsBeforeD = hits.length;
    r = await readSSE({ model: "t-giveup", stream: true });
    check("D: gives up after 10 retries (11 hits)", hits.length - hitsBeforeD === 11, `hits=${hits.length - hitsBeforeD}`);
    check("D: passes upstream 500 through", r.status === 500, `status=${r.status}`);

    hitCount = 0;
    r = await readSSE({ model: "t-repair", stream: true });
    const hasNullContent = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.delta?.content === null; } catch { return false; } });
    check("E: null content stripped", textOf(r.text) === "R" && !hasNullContent, JSON.stringify(r.text));

    hitCount = 0;
    r = await readSSE({ model: "t-midstream-death", stream: true });
    const hasValidErr = dataLines(r.text).some((d) => { try { const j = JSON.parse(d); return j?.error?.message && typeof j.error.message === "string"; } catch { return false; } });
    check("F: mid-stream death -> valid error chunk + DONE", textOf(r.text) === "partial" && hasValidErr && r.text.includes("[DONE]"), `FULL=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE({ model: "t-nonstream", stream: false });
    check("G: non-stream passthrough", r.status === 200 && r.text.includes('"hi"'), r.text.slice(0, 120));

    hitCount = 0;
    r = await readSSE({ model: "t-bulkjson", stream: true });
    check("H: bulk-JSON converted to chunk", textOf(r.text) === "bulk", JSON.stringify(r.text.slice(0, 300)));

    hitCount = 0;
    r = await readSSE({ model: "t-stall-retry", stream: true });
    check("I: stalled attempt retried", r.retries === "1" && textOf(r.text) === "after-stall", `retries=${r.retries} text=${textOf(r.text)}`);

    hitCount = 0;
    const hitsBeforeJ = hits.length;
    r = await readSSE({ model: "t-200-error-retry", stream: true });
    check("J: 200+8006 error body retried with SAME body", r.retries === "1" && textOf(r.text) === "same-body-ok" && hits.length - hitsBeforeJ === 2, `retries=${r.retries} hits=${hits.length - hitsBeforeJ}`);

    hitCount = 0;
    const hitsBeforeK = hits.length;
    r = await readSSE({ model: "t-200-error-always", stream: true });
    const kErr = dataLines(r.text).some((d) => d.includes("exceeds the maximum supported size"));
    const kNoOpencodeError = !r.text.includes("Invalid logfare");
    check("K: budget exhaustion -> REAL error as valid SSE event", hits.length - hitsBeforeK > 5 && kErr && kNoOpencodeError && r.text.includes("[DONE]"), `hits=${hits.length - hitsBeforeK}`);

    hitCount = 0;
    r = await readSSE({ model: "t-inband-rl", stream: true });
    check("M: in-band Serverless TPM retried patiently", r.retries === "1" && textOf(r.text) === "rl-ok", `retries=${r.retries} text=${textOf(r.text)}`);
  } catch (e) {
    fail++;
    console.log(`FAIL harness error: ${e?.stack || e}`);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  proxy.kill();
  mock.close();
  process.exit(fail ? 1 : 0);
});
