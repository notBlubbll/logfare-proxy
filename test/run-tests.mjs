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

const ABRUPT_PREFIX = "Let me keep it simple and honest: the locale file blurbs are authored from the actual persona prompts, which I just";
const ABRUPT_FULL = ABRUPT_PREFIX + " realized should live in the persona's own data and completed cleanly.";
const DASH_PREFIX = "Internal #bot-status: 3 individual per-bot cards with everything per-bot (description, Age, Awake, Replies, Avg response, Tokens, In/out) —";
const DASH_FULL = DASH_PREFIX + " plus the remaining per-bot metrics and the final card layout.";
const COMPLETE_TEXT = "This answer is already complete and ends with a period, so it must not be continued by the proxy.";
const FINISH_PREFIX = "interface StatusCardRow { showProviderUsage: boolean; showProfile: boolean; showTokens: boolean; showTimings: boolean; showAvgResponse: boolean; showMeasuredLine: boolean; showAggregateModel: boolean; }\n\n";
const FINISH_TAIL = "interface StatusCardRow {";
const FINISH_FULL = FINISH_PREFIX + "interface StatusCardRow {\n  id: string;\n  showProviderUsage: boolean;\n}";
const CODE_PREFIX = "C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\modules\\status-card\\index.ts\n" + "const value = 1;\n".repeat(100) + "}); }";
const CODE_FULL = CODE_PREFIX + "\n// continued\nconst more = 2;";
const PLAIN_CODE_PREFIX = "function buildCard() {\n" + "  const value = 1;\n".repeat(100) + "  return value;\n}";
const PLAIN_CODE_FULL = PLAIN_CODE_PREFIX + "\n\nfunction finishCard() {\n  return true;\n}";

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
  // P: "Service temporarily unavailable" is transient -> patient retry, does
  // NOT consume the 10 hard retries (12 retries then success would give up
  // under the old hard-failure accounting).
  "t-transient-patient": (req, res) => {
    const n = hitCount;
    if (n <= 12) {
      sse(res, ['{"error":{"message":"Service temporarily unavailable","type":"server_error"}}']);
    } else {
      sse(res, ['{"choices":[{"delta":{"content":"transient-ok"}}]}', "[DONE]"]);
    }
  },
  // Q: upstream closes right after finish_reason without [DONE] -> the turn is
  // complete; proxy must synthesize [DONE] and must NOT emit an error chunk.
  "t-finish-no-done": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"content":"done-no-done"}}]}',
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
    ]);
  },
  // R: "Service temporarily unavailable" after content committed -> proxy must
  // suppress it (never forward) and close cleanly with [DONE], so the desktop
  // does not start its "Attempt N - retrying" turn loop.
  "t-never-forward-postcommit": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"content":"partial"}}]}',
      '{"error":{"message":"Service temporarily unavailable","type":"server_error"}}',
    ]);
  },
  // S: upstream sends [DONE] but never a finish_reason chunk. OpenCode rejects
  // that with "OpenAI Chat stream ended without finish_reason"; the proxy must
  // inject a terminal finish_reason chunk before [DONE].
  "t-done-no-finish": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"content":"done-no-finish"}}]}',
      "[DONE]",
    ]);
  },
  // T: upstream closes cleanly without finish_reason or [DONE].
  "t-close-no-finish": (req, res) => {
    sse(res, [
      '{"choices":[{"delta":{"content":"closed-no-finish"}}]}',
    ]);
  },
  // U: mid-answer transient error -> proxy retries, skips the sent prefix,
  // and streams only the continuation (no duplicate, no error).
  "t-postcommit-continue": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"choices":[{"delta":{"content":"Hello "}}]}',
        '{"error":{"message":"Service temporarily unavailable","type":"server_error"}}',
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"content":"Hello world!"}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // V: the retry diverges from the sent prefix -> end the partial answer
  // cleanly instead of duplicating/garbling it.
  "t-postcommit-diverge": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"choices":[{"delta":{"content":"Hello "}}]}',
        '{"error":{"message":"Service temporarily unavailable","type":"server_error"}}',
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"content":"Goodbye"}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // W: upstream cuts the stream with no error/finish/[DONE] -> proxy retries
  // and streams the continuation after the sent prefix.
  "t-truncate-continue": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"choices":[{"delta":{"content":"Part one "}}]}',
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"content":"Part one two!"}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // X: the model sends finish_reason=stop but the text ends mid-sentence ->
  // treat it as a cut and continue after the sent prefix.
  "t-abrupt-stop-continue": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(ABRUPT_PREFIX)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(ABRUPT_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Y: a complete stop (terminal punctuation) must not trigger continuation.
  "t-abrupt-stop-complete": (req, res) => {
    sse(res, [
      `{"choices":[{"delta":{"content":${JSON.stringify(COMPLETE_TEXT)}}}]}`,
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // Z2: a stop right after an em dash is mid-thought -> continue.
  "t-abrupt-dash-continue": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(DASH_PREFIX)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(DASH_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z3: the final chunk carries new content together with finish_reason; the
  // completeness check must include it (here it ends with "{").
  "t-abrupt-finish-content": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(FINISH_PREFIX)}}}]}`,
        `{"choices":[{"delta":{"content":${JSON.stringify(FINISH_TAIL)}},"finish_reason":"stop"}]}`,
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(FINISH_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z4: the continuation retry emits a tool call before reproducing the text.
  // The proxy must hand the tool call to the client instead of ending partial.
  "t-continuation-toolcall": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"choices":[{"delta":{"content":"I will update the file now: "}}]}',
        '{"error":{"message":"Service temporarily unavailable","type":"server_error"}}',
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"edit","arguments":"{}"}}]}}]}',
        '{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z5: a large file/code dump ending with "}" is treated as a cut.
  "t-code-dump-continue": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(CODE_PREFIX)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(CODE_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z6: DSML edit block streamed as text, finishing with tool_calls but no
  // structured tool call -> treat as a code dump and continue.
  "t-dsml-toolcall-continue": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(CODE_PREFIX)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(CODE_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z7: a prior tool call must not block a code-dump continuation.
  "t-code-dump-after-toolcall": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_0","type":"function","function":{"name":"read","arguments":"{}"}}]}}]}',
        `{"choices":[{"delta":{"content":${JSON.stringify(CODE_PREFIX)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(CODE_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z8: large code-like output without a file path/DSML marker still counts
  // as a possible code dump.
  "t-plain-code-dump": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(PLAIN_CODE_PREFIX)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(PLAIN_CODE_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z9: a text tool call ("edit C:\...") instead of structured tool_calls ->
  // retry; the retry emits a real tool call, which the proxy hands off.
  "t-text-toolcall-handoff": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify("edit C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\modules\\status-card\\index.ts const status = liveness.online ? localize(locale, \"status_card.bot_online\") : localize(locale, \"status_card.bot_offline\");")}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_edit","type":"function","function":{"name":"edit","arguments":"{\\"filePath\\":\\"src/modules/status-card/index.ts\\"}"}}]}}]}',
        '{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z10: DSML read block streamed as text -> convert to a real read tool call.
  "t-dsml-read-toolcall": (req, res) => {
    const dsml = '< | DSML | parameter name="limit" string="false">100< | DSML | parameter name="offset" string="false">140< | DSML | parameter name="path" string="true">C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\utils\\discord\\interactions\\botConfigRoutes.ts';
    sse(res, [
      `{"choices":[{"delta":{"content":${JSON.stringify(dsml)}}}]}`,
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // Z11: content + [DONE] with no finish_reason; buffer mode must not flush
  // an incomplete answer just because DONE arrived.
  "t-done-no-finish-abrupt": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(ABRUPT_PREFIX)}}}]}`,
        "[DONE]",
      ]);
    } else {
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(ABRUPT_FULL)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z12: DSML read with the bare path before the parameter tags.
  "t-dsml-bare-read": (req, res) => {
    const dsml = 'C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\utils\\discord\\interactions\\botConfigRoutes.ts</ | DSML | parameter > < | DSML | parameter name="offset" string="false">52< | DSML | parameter > < | DSML | parameter name="limit" string="false">28< | DSML | parameter > </ | DSML | invoke>';
    sse(res, [
      `{"choices":[{"delta":{"content":${JSON.stringify(dsml)}}}]}`,
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // Z13: DSML edit with path + oldString + newString -> real edit tool call.
  "t-dsml-edit-toolcall": (req, res) => {
    const dsml = 'edit C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\modules\\status-card\\index.ts</ | DSML | parameter > < | DSML | parameter name="oldString" string="true">const A = 1;</ | DSML | parameter > < | DSML | parameter name="newString" string="true">const A = 2;</ | DSML | parameter > </ | DSML | invoke>';
    sse(res, [
      `{"choices":[{"delta":{"content":${JSON.stringify(dsml)}}}]}`,
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // Z14: malformed DSML with a bare path followed by file content (no
  // oldString/newString) -> treat as a safe read of that path.
  "t-dsml-bare-read-content": (req, res) => {
    const dsml = 'C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\modules\\status-card\\index.ts if (config.botKey) { embeds.push(...botEmbeds(config.botKey, context)); } </ | DSML | parameter>';
    sse(res, [
      `{"choices":[{"delta":{"content":${JSON.stringify(dsml)}}}]}`,
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // Z15: bare path + file content after "edit didn't apply / add..." is a
  // malformed tool call; buffer mode must retry, not flush it as an answer.
  "t-bare-path-edit": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      const bad = "The edit didn't apply. Let me add the keys now.\nC:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\locales\\en-US\\commands\\config-bots.ts enabled_description: Whether this bot connects to Discord at all., model_description: Model ID from the catalog. Leave blank to keep the current model., enabled_description: Image to use as this bot's avatar., name_description: Display name this bot shows in Discord. Leave blank to keep the current one., avatar_applied: Avatar updated., avatar_failed: Could not set the avatar. Discord rejects images that are too large or not square., name_applied: Display name updated., avatar_current: Avatar, avatar_none: not set, model_description: Model ID from the catalog. Leave blank to keep the current model.,";
      sse(res, [
        `{"choices":[{"delta":{"content":${JSON.stringify(bad)}}}]}`,
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"content":"The keys were added correctly."}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
    }
  },
  // Z16: DSML grep (pattern + path + include) -> real grep tool call.
  "t-dsml-grep-toolcall": (req, res) => {
    const dsml = '< | DSML | parameter name="include" string="true">*.ts < | DSML | parameter name="limit" string="false">20 < | DSML | parameter name="path" string="true">C:\\Users\\User\\Desktop\\EXPERIMENTS\\DISCORD\\CHATBOT\\src\\utils\\db\\repositories\\PersonaRepository.ts < | DSML | parameter name="pattern" string="true">async setAvatar|webhook_avatar_url';
    sse(res, [
      `{"choices":[{"delta":{"content":${JSON.stringify(dsml)}}}]}`,
      '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
      "[DONE]",
    ]);
  },
  // Z: role-only chunk committed, then a 503 before any visible content ->
  // the proxy must retry instead of ending an empty turn.
  "t-empty-503-retry": (req, res) => {
    const n = hitCount;
    if (n === 1) {
      sse(res, [
        '{"choices":[{"delta":{"role":"assistant"}}]}',
        '{"error":{"message":"Service temporarily unavailable","type":"server_error"}}',
      ]);
    } else {
      sse(res, [
        '{"choices":[{"delta":{"content":"recovered-after-empty-503"}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}]}',
        "[DONE]",
      ]);
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

async function readSSE(path, body, port = PROXY_PORT) {
  const r = await fetch(`http://127.0.0.1:${port}${path}`, {
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
    env: { ...process.env, LOGFARE_PROXY_PORT: String(PROXY_PORT), LOGFARE_PROXY_UPSTREAM: `http://127.0.0.1:${MOCK_PORT}/v1`, LOGFARE_PROXY_BUFFER: "0", LOGFARE_PROXY_RL_BUDGET_MS: "9000", LOGFARE_PROXY_RL_BACKOFF_MS: "200,300,500", LOGFARE_PROXY_BACKOFF_MS: "10,20,30", LOGFARE_PROXY_VISION_TARGETS: "vision-text", LOGFARE_PROXY_VISION_MODEL: "vision-flash" },
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

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-transient-patient", stream: true });
    check("P: 'Service temporarily unavailable' patient (13 attempts, no give-up)", r.retries === "12" && textOf(r.text) === "transient-ok", `retries=${r.retries} text=${textOf(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-finish-no-done", stream: true });
    const qErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Q: finish_reason without [DONE] -> clean [DONE], no error chunk", textOf(r.text) === "done-no-done" && !qErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-never-forward-postcommit", stream: true });
    const rErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const rFinish = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.choices?.[0]?.finish_reason; } catch { return false; } });
    check("R: post-commit 'Service temporarily unavailable' suppressed", textOf(r.text) === "partial" && !rErr && rFinish && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-done-no-finish", stream: true });
    const sFinish = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.choices?.[0]?.finish_reason; } catch { return false; } });
    const sErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("S: [DONE] without finish_reason -> injected finish_reason", textOf(r.text) === "done-no-finish" && sFinish && !sErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-close-no-finish", stream: true });
    const tFinish = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.choices?.[0]?.finish_reason; } catch { return false; } });
    const tErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("T: clean close without finish_reason/[DONE] -> injected finish + DONE", textOf(r.text) === "closed-no-finish" && tFinish && !tErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-postcommit-continue", stream: true });
    const uErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("U: post-commit error continued without duplication", textOf(r.text) === "Hello world!" && !uErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-postcommit-diverge", stream: true });
    const vErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("V: divergent continuation ends partial cleanly", textOf(r.text) === "Hello " && !vErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-truncate-continue", stream: true });
    const wErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("W: truncated stream continued after sent prefix", textOf(r.text) === "Part one two!" && !wErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-abrupt-stop-continue", stream: true });
    const xErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("X: abrupt stop mid-answer continued", textOf(r.text) === ABRUPT_FULL && !xErr && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    const hitsBeforeY = hits.length;
    r = await readSSE("/v1/chat/completions", { model: "t-abrupt-stop-complete", stream: true });
    const yErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Y: complete stop not continued", textOf(r.text) === COMPLETE_TEXT && hits.length - hitsBeforeY === 1 && !yErr, `hits=${hits.length - hitsBeforeY} text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    const hitsBeforeZ = hits.length;
    r = await readSSE("/v1/chat/completions", { model: "t-empty-503-retry", stream: true });
    const zErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Z: empty 503 after role chunk retried", textOf(r.text) === "recovered-after-empty-503" && hits.length - hitsBeforeZ === 2 && !zErr && r.text.includes("[DONE]"), `hits=${hits.length - hitsBeforeZ} text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-abrupt-dash-continue", stream: true });
    const z2Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Z2: stop after em dash continued", textOf(r.text) === DASH_FULL && !z2Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-abrupt-finish-content", stream: true });
    const z3Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Z3: finish-chunk content included in incomplete check", textOf(r.text) === FINISH_FULL && !z3Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-continuation-toolcall", stream: true });
    const z4Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z4Tool = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.choices?.[0]?.delta?.tool_calls; } catch { return false; } });
    const z4Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("Z4: continuation hands tool calls to the client", textOf(r.text) === "I will update the file now: " && z4Tool && z4Finish && !z4Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-code-dump-continue", stream: true });
    const z5Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Z5: large code dump continued", textOf(r.text) === CODE_FULL && !z5Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-toolcall-continue", stream: true });
    const z6Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Z6: DSML text + tool_calls finish continued", textOf(r.text) === CODE_FULL && !z6Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-code-dump-after-toolcall", stream: true });
    const z7Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z7Tool = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.choices?.[0]?.delta?.tool_calls; } catch { return false; } });
    check("Z7: code dump continued after a tool call", textOf(r.text) === CODE_FULL && z7Tool && !z7Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-plain-code-dump", stream: true });
    const z8Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("Z8: large plain code dump continued", textOf(r.text) === PLAIN_CODE_FULL && !z8Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-text-toolcall-handoff", stream: true });
    const z9Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z9Tool = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.choices?.[0]?.delta?.tool_calls; } catch { return false; } });
    const z9Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("Z9: text tool call retried and handed off", z9Tool && z9Finish && !z9Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-read-toolcall", stream: true });
    const z10Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z10Tool = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; return tc?.function?.name === "read" && String(tc.function.arguments).includes("botConfigRoutes.ts"); } catch { return false; } });
    const z10Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    const z10NoDsml = !r.text.includes("< | DSML |");
    check("Z10: DSML read converted to tool call", z10Tool && z10Finish && z10NoDsml && !z10Err && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-bare-read", stream: true });
    const z12Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z12Tool = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; const a = JSON.parse(tc?.function?.arguments || "{}"); return tc?.function?.name === "read" && String(a.filePath).includes("botConfigRoutes.ts") && a.offset === 52 && a.limit === 28; } catch { return false; } });
    const z12Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("Z12: bare-path DSML read converted", z12Tool && z12Finish && !z12Err && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-edit-toolcall", stream: true });
    const z13Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z13Tool = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; const a = JSON.parse(tc?.function?.arguments || "{}"); return tc?.function?.name === "edit" && String(a.filePath).includes("status-card") && a.oldString === "const A = 1;" && a.newString === "const A = 2;"; } catch { return false; } });
    const z13Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("Z13: DSML edit converted to tool call", z13Tool && z13Finish && !z13Err && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-bare-read-content", stream: true });
    const z14Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z14Tool = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; return tc?.function?.name === "read" && String(tc.function.arguments).includes("status-card"); } catch { return false; } });
    const z14Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("Z14: malformed DSML path+content treated as read", z14Tool && z14Finish && !z14Err && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-grep-toolcall", stream: true });
    const z16Err = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    const z16Tool = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; const a = JSON.parse(tc?.function?.arguments || "{}"); return tc?.function?.name === "grep" && a.pattern === "async setAvatar|webhook_avatar_url" && String(a.path).includes("PersonaRepository.ts") && a.include === "*.ts"; } catch { return false; } });
    const z16Finish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("Z16: DSML grep converted to tool call", z16Tool && z16Finish && !z16Err && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

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

    // Buffer mode: same plugin with LOGFARE_PROXY_BUFFER=1.
    const BUFFER_PORT = 17332;
    const bufferProxy = spawn(RUNTIME, [PLUGIN], {
      env: { ...process.env, LOGFARE_PROXY_PORT: String(BUFFER_PORT), LOGFARE_PROXY_UPSTREAM: `http://127.0.0.1:${MOCK_PORT}/v1`, LOGFARE_PROXY_BUFFER: "1", LOGFARE_PROXY_RL_BUDGET_MS: "9000", LOGFARE_PROXY_RL_BACKOFF_MS: "200,300,500", LOGFARE_PROXY_BACKOFF_MS: "10,20,30" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    bufferProxy.stdout.on("data", (d) => process.stdout.write(`[buf] ${d}`));
    bufferProxy.stderr.on("data", (d) => process.stdout.write(`[buf!] ${d}`));
    for (let i = 0; i < 50; i++) {
      try { const h = await fetch(`http://127.0.0.1:${BUFFER_PORT}/healthz`); if (h.ok) break; } catch {}
      await delay(200);
    }

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-postcommit-continue", stream: true }, BUFFER_PORT);
    const bufErr = dataLines(r.text).some((d) => { try { return !!JSON.parse(d)?.error; } catch { return false; } });
    check("BUF: cut retried transparently, full answer once", r.retries === "1" && textOf(r.text) === "Hello world!" && !bufErr && r.text.includes("[DONE]"), `retries=${r.retries} text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-read-toolcall", stream: true }, BUFFER_PORT);
    const bufTool = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; return tc?.function?.name === "read"; } catch { return false; } });
    const bufFinish = dataLines(r.text).some((d) => { try { return JSON.parse(d)?.choices?.[0]?.finish_reason === "tool_calls"; } catch { return false; } });
    check("BUF: DSML read converted in buffer mode", bufTool && bufFinish && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-abrupt-stop-continue", stream: true }, BUFFER_PORT);
    check("BUF: abrupt stop retried transparently", textOf(r.text) === ABRUPT_FULL && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-done-no-finish-abrupt", stream: true }, BUFFER_PORT);
    check("BUF: [DONE] without finish_reason still retried when incomplete", textOf(r.text) === ABRUPT_FULL && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-bare-read", stream: true }, BUFFER_PORT);
    const bufBare = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; return tc?.function?.name === "read"; } catch { return false; } });
    check("BUF: bare-path DSML read converted", bufBare && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-edit-toolcall", stream: true }, BUFFER_PORT);
    const bufEdit = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; const a = JSON.parse(tc?.function?.arguments || "{}"); return tc?.function?.name === "edit" && a.oldString === "const A = 1;" && a.newString === "const A = 2;"; } catch { return false; } });
    check("BUF: DSML edit converted to tool call", bufEdit && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-bare-read-content", stream: true }, BUFFER_PORT);
    const bufBareContent = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; return tc?.function?.name === "read" && String(tc.function.arguments).includes("status-card"); } catch { return false; } });
    check("BUF: malformed DSML path+content treated as read", bufBareContent && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    hitCount = 0;
    r = await readSSE("/v1/chat/completions", { model: "t-dsml-grep-toolcall", stream: true }, BUFFER_PORT);
    const bufGrep = dataLines(r.text).some((d) => { try { const tc = JSON.parse(d)?.choices?.[0]?.delta?.tool_calls?.[0]; const a = JSON.parse(tc?.function?.arguments || "{}"); return tc?.function?.name === "grep" && a.pattern === "async setAvatar|webhook_avatar_url" && String(a.path).includes("PersonaRepository.ts"); } catch { return false; } });
    check("BUF: DSML grep converted to tool call", bufGrep && !r.text.includes("< | DSML |") && r.text.includes("[DONE]"), `text=${JSON.stringify(r.text)}`);

    bufferProxy.kill();
  } catch (e) {
    fail++;
    console.log(`FAIL harness error: ${e?.stack || e}`);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  proxy.kill();
  mock.close();
  process.exit(fail ? 1 : 0);
});

