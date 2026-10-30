// Test harness for logfare-proxy. Spawns:
//   - a mock upstream on 127.0.0.1:17999 (scenarios keyed by request "model")
//   - the proxy (bun index.js) on 127.0.0.1:17331 with LOGFARE_PROXY_UPSTREAM
// Then drives HTTP requests through the proxy and asserts behavior.
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const MOCK_PORT = 17999;
const PROXY_PORT = 17331;
const PLUGIN = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "index.js");
const RUNTIME = process.execPath; // bun or node, whichever runs this file

let hits = []; // { model, n }
let hitCount = 0;

function sse(res, events, opts = {}) {
  res.writeHead(200, { "content-type": "text/event-stream", ...opts.headers });
  let i = 0;
  const tick = () => {
    if (i >= events.length) { res.end(); return; }
    const e = events[i++];
    if (e === "CLOSE") { res.destroy(); return; }
    if (e === "STALL") { setTimeout(tick, 400); return; } // just delay
    res.write(typeof e === "string" ? `data: ${e}\n\n` : e);
    setTimeout(tick, opts.gap ?? 10);
  };
  tick();
}

const scenarios = {
  // A: pre-commit invalid event on attempt 1 -> retry -> attempt 2 clean
  "t-precommit-retry": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"error":"overloaded"}', // invalid: error must be object w/ string message
        '{"choices":[{"delta":{"content":"SHOULD-NOT-APPEAR"}}]}',
        "[DONE]",
      ]);
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
  // B: post-commit drop â€” invalid event after content started
  "t-postcommit-drop": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"role":"assistant"}}]}',
      '{"choices":[{"delta":{"content":"AB"}}]}',
      '{"choices":"nope"}',          // invalid: choices not array -> DROP
      'oops not json',               // invalid: not-json -> DROP
      '{"choices":[{"delta":{"content":"C"}}]}',
      "[DONE]",
    ]);
  },
  // C: valid in-band error event pre-commit -> retry -> clean
  "t-error-event-retry": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"error":{"message":"model overloaded","type":"server_error"}}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"content":"ok-after-retry"}}]}',
        "[DONE]",
      ]);
    }
  },
  // D: always 500 -> give up after 10 retries (11 upstream hits), pass status
  "t-giveup": (req, res) => {
    res.writeHead(500, { "content-type": "application/json" });
    res.end('{"error":{"message":"boom","type":"server_error"}}');
  },
  // E: null-content repair
  "t-repair": (req, res) => {
    sse(res, [
      '{"choices":[{"index":0,"delta":{"role":"assistant","content":null},"finish_reason":null}]}',
      '{"choices":[{"index":0,"delta":{"content":"R"}}]}',
      '{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // F: upstream dies mid-stream after commit -> synth error + [DONE]
  "t-midstream-death": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"role":"assistant"}}]}',
      '{"choices":[{"delta":{"content":"partial"}}]}',
      "CLOSE",
    ]);
  },
  // G: non-stream passthrough
  "t-nonstream": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"id":"x","choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{}}');
  },
  // H: bulk-JSON answer to a stream request -> converted to one chunk
  "t-bulkjson": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"id":"b","choices":[{"index":0,"message":{"role":"assistant","content":"bulk"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}');
  },
  // I: first-byte stall then death pre-commit -> retry -> clean
  "t-stall-retry": (req, res) => {
    const n = hitCount;
    if (n === 1) { sse(res, ["STALL", "CLOSE"]); }
    else { sse(res, ['{"choices":[{"delta":{"content":"after-stall"}}]}', "[DONE]"]); }
  },
  // J: HTTP 200 + JSON error body (AiError 8006 "body exceeds maximum
  // supported size") on attempt 1 â€” patient retry with SAME body -> clean
  "t-200-error-retry": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"error": {"errors": [{"message": "AiError: AiError: Invalid data for body - reason request body exceeds the maximum supported size (test-id)", "code": 8006}], "success": false, "result": {}, "messages": []}}');
    } else {
      sse(res, ['{"choices":[{"delta":{"content":"same-body-ok"}}]}', "[DONE]"]);
    }
  },
  // K: 200 + 8006 error body ALWAYS -> patient retries exhaust the budget ->
  // give-up must surface the REAL error as a schema-VALID SSE event
  "t-200-error-always": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end('{"error": {"errors": [{"message": "AiError: AiError: Invalid data for body - reason request body exceeds the maximum supported size (always)", "code": 8006}], "success": false, "result": {}, "messages": []}}');
  },
  // M: in-band Serverless token-per-minute error event -> patient retry
  // N/O: vision handoff â€” text model receives a caption instead of the image
  "vision-flash": (req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ id: "v", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: "A login form with username and password fields." }, finish_reason: "stop" }], usage: {} }));
  },
  "vision-text": (req, res, bodyObj) => {
    const flat = JSON.stringify(bodyObj?.messages ?? []);
    const hasImage = flat.includes("image_url");
    const hasDesc = flat.includes("login form");
    sse(res, [`{"choices":[{"delta":{"content":"desc=${hasDesc ? "yes" : "no"} image=${hasImage ? "yes" : "no"}"}}]}`, "[DONE]"]);
  },
  "t-inband-rl": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, ['{"error":{"message":"Serverless token-per-minute limit reached for this model","type":"server_error"}}', "[DONE]"]);
    } else {
      sse(res, ['{"choices":[{"delta":{"content":"rl-ok"}}]}', "[DONE]"]);
    }
  },
};

const mock = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    hitCount++;
    let model = "?";
    let bodyObj = null;
    try { bodyObj = JSON.parse(body); model = bodyObj.model; } catch {}
    hits.push(model);
    const fn = scenarios[model];
    if (!fn) { res.writeHead(404); res.end("no scenario"); return; }
    // verify auth header forwarded
    if (!/^Bearer test-key/.test(req.headers["authorization"] || "")) {
      res.writeHead(401); res.end("mock: missing auth"); return;
    }
    fn(req, res, bodyObj);
  });
});

async function readSSE(path, body) {
  const r = await fetch(`http://127.0.0.1:${PROXY_PORT}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer test-key" },
    body: JSON.stringify(body),
  });
  const retries = r.headers.get("x-lf-retries");
  const text = await r.text();
  return { status: r.status, retries, text };
}

const dataLines = (t) => t.split("\n").filter((l) => l.startsWith("data: ")).map((l) => l.slice(6));
const textOf = (t) => dataLines(t).filter((d) => d !== "[DONE]").map((d) => { try { return JSON.parse(d)?.choices?.[0]?.delta?.content || ""; } catch { return "<BADJSON>"; } }).join("");

let pass = 0, fail = 0;
function check(name, cond, extra = "") {
  if (cond) { pass++; console.log(`PASS ${name}`); }
  else { fail++; console.log(`FAIL ${name} ${extra}`); }
}

mock.listen(MOCK_PORT, "127.0.0.1", async () => {
  const proxy = spawn(RUNTIME, [PLUGIN], {
    env: { ...process.env, LOGFARE_PROXY_PORT: String(PROXY_PORT), LOGFARE_PROXY_UPSTREAM: `http://127.0.0.1:${MOCK_PORT}/v1`, LOGFARE_PROXY_RL_BUDGET_MS: "9000", LOGFARE_PROXY_RL_BACKOFF_MS: "200,300,500", LOGFARE_PROXY_BACKOFF_MS: "10,20,30", LOGFARE_PROXY_VISION_TARGETS: "vision-text", LOGFARE_PROXY_VISION_MODEL: "vision-flash" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  proxy.stdout.on("data", (d) => process.stdout.write(`[proxy] ${d}`));
  proxy.stderr.on("data", (d) => process.stdout.write(`[proxy!] ${d}`));

  // wait for proxy
  for (let i = 0; i < 50; i++) {
    try { const h = await fetch(`http://127.0.0.1:${PROXY_PORT}/healthz`); if (h.ok) break; } catch {}
    await delay(200);
  }

  try {
    hitCount = 0;
    let r = await readSSE("/v1/chat/completions", { model: "t-precommit-retry", stream: true });
    check("A: retried once", r.retries === "1", `retries=${r.retries}`);
    check("A: clean content", textOf(r.text) === "Hello world", JSON.stringify(r.text));
    check("A: no leaked attempt-1 text", !r.text.includes("SHOULD-NOT-APPEAR"));
    check("A: upstream hit twice", hits.length === 2, `hits=${hits.length}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-postcommit-drop", stream: true });
    check("B: drops mid-stream, content intact", textOf(r.text) === "ABC", JSON.stringify(r.text));
    check("B: no retry after commit", r.retries === "0", `retries=${r.retries}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-error-event-retry", stream: true });
    check("C: in-band error pre-commit retried", r.retries === "1" && textOf(r.text) === "ok-after-retry", `retries=${r.retries} text=${textOf(r.text)}`);

    hitCount = 0;
    const hitsBeforeD = hits.length;
    r = await readSSE("/v1/chat/completions", { model: "t-giveup", stream: true });
    check("D: gives up after 10 retries (11 hits)", hits.length - hitsBeforeD === 11, `hits=${hits.length - hitsBeforeD}`);
    check("D: passes upstream 500 through", r.status === 500, `status=${r.status}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-repair", stream: true });
    const hasNullContent = dataLines(r.text).some((d) => { try { const j = JSON.parse(d); return j?.choices?.[0]?.delta?.content === null; } catch { return false; } });
    check("E: null content stripped", textOf(r.text) === "R" && !hasNullContent, JSON.stringify(r.text));

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-midstream-death", stream: true });
    const hasValidErr = dataLines(r.text).some((d) => { try { const j = JSON.parse(d); return j?.error?.message && typeof j.error.message === "string"; } catch { return false; } });
    check("F: mid-stream death -> valid error chunk + DONE", textOf(r.text) === "partial" && hasValidErr && r.text.includes("[DONE]"), `FULL=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-nonstream", stream: false });
    check("G: non-stream passthrough", r.status === 200 && r.text.includes('"hi"'), r.text.slice(0, 120));

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-bulkjson", stream: true });
    check("H: bulk-JSON converted to chunk", textOf(r.text) === "bulk", JSON.stringify(r.text.slice(0, 300)));

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-stall-retry", stream: true });
    check("I: stalled attempt retried", r.retries === "1" && textOf(r.text) === "after-stall", `retries=${r.retries} text=${textOf(r.text)}`);

    hitCount = 0;
    const hitsBeforeJ = hits.length;
    r = await readSSE("/v1/chat/completions", { model: "t-200-error-retry", stream: true });
    check("J: 200+8006 error body retried with SAME body", r.retries === "1" && textOf(r.text) === "same-body-ok" && hits.length - hitsBeforeJ === 2, `retries=${r.retries} hits=${hits.length - hitsBeforeJ} text=${textOf(r.text)}`);

    hitCount = 0;
    const hitsBeforeK = hits.length;
    r = await readSSE("/v1/chat/completions", { model: "t-200-error-always", stream: true });
    const kErr = dataLines(r.text).some((d) => d.includes("exceeds the maximum supported size"));
    const kNoOpencodeError = !r.text.includes("Invalid logfare"); // the opencode router error must never appear (upstream text may legitimately contain "Invalid data for body")
    check("K: budget exhaustion -> REAL error as valid SSE event", hits.length - hitsBeforeK > 5 && kErr && kNoOpencodeError && r.text.includes("[DONE]"), `hits=${hits.length - hitsBeforeK} kErr=${kErr} kNoOpencodeError=${kNoOpencodeError} text=${JSON.stringify(r.text.slice(0, 600))}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-inband-rl", stream: true });
    check("M: in-band Serverless RL retried patiently", r.retries === "1" && textOf(r.text) === "rl-ok", `retries=${r.retries} text=${textOf(r.text)}`);

    // N: text model + image -> vision model captions it, text model gets text only
    const imgBody = {
      model: "vision-text",
      stream: true,
      messages: [{
        role: "user",
        content: [
          { type: "text", text: "what is this?" },
          { type: "image_url", image_url: { url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg==" } },
        ],
      }],
    };
    hitCount = 0;
    const hitsBeforeN = hits.length;
    r = await readSSE("/v1/chat/completions", imgBody);
    const nHits = hits.slice(hitsBeforeN);
    check(
      "N: vision handoff (flash captions -> text model, no image)",
      textOf(r.text).includes("desc=yes") && textOf(r.text).includes("image=no") && nHits[0] === "vision-flash" && nHits[1] === "vision-text",
      `text=${textOf(r.text)} hits=${JSON.stringify(nHits)}`
    );

    // O: identical image again -> caption served from cache, no second vision call
    hitCount = 0;
    const hitsBeforeO = hits.length;
    r = await readSSE("/v1/chat/completions", imgBody);
    const oHits = hits.slice(hitsBeforeO);
    check(
      "O: identical image served from caption cache",
      oHits.length === 1 && oHits[0] === "vision-text" && textOf(r.text).includes("desc=yes"),
      `hits=${JSON.stringify(oHits)} text=${textOf(r.text)}`
    );
  } catch (e) {
    fail++;
    console.log(`FAIL harness error: ${e?.stack || e}`);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  proxy.kill();
  mock.close();
  process.exit(fail ? 1 : 0);
});

