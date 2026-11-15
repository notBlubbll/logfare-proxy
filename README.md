# logfare-proxy

An OpenCode V2 plugin that sits between OpenCode and a logfare-compatible
OpenAI endpoint and makes `Invalid <provider>/openai-compatible-chat stream
event` impossible to reach the client, while retrying every transient failure
class (rate limits, body-size flakes, 5xx, dropped streams) transparently.

No credentials are stored in this code. The proxy forwards whatever
`Authorization` header the client sends, and the request body (including the
model id) byte-exact.

## Why

The error above is thrown by OpenCode's route validator when an SSE `data:`
payload fails the openai-compatible-chat chunk schema. In practice the biggest
producer is the upstream answering **HTTP 200 with a JSON error body**, e.g.:

```json
{"error":{"errors":[{"message":"AiError: Invalid data for body - reason request body exceeds the maximum supported size","code":8006}],"success":false,"result":{},"messages":[]}}
```

That body is not a chunk. If it is forwarded as one, the decoder fails the
union and blames the route. This proxy treats it as a failed attempt and
retries — the same body often succeeds on a later attempt.

## Install

Copy the folder to your global plugin directory:

```
~/.config/opencode/plugins/logfare-proxy/index.js
```

OpenCode discovers plugins from that directory on server start — no config
entry is required. Then point your provider at the proxy:

```jsonc
{
  "providers": {
    "logfare": {
      "package": "@opencode-ai/ai/providers/openai-compatible",
      "settings": {
        "baseURL": "http://127.0.0.1:17330/v1",
        "apiKey": "<your logfare key>",
        "timeout": 1800000,
        "headerTimeout": 600000,
        "chunkTimeout": 600000
      },
      "models": { /* unchanged */ }
    }
  }
}
```

Keep the real key in `settings.apiKey`: OpenCode sends it as `Authorization`
and the proxy forwards it upstream unchanged.

## Behavior

**Pre-commit** (nothing has been sent to the client yet): every upstream event
is buffered and validated. Any failure — non-2xx, invalid event, in-band error
payload, network death, stall — makes the attempt fail and the whole request is
retried with the same buffered body.

**Post-commit**: every event is still validated before it is written; invalid
events are dropped inline (logged) so the client parser can never see one. If
the upstream dies mid-stream, a schema-valid error chunk + `[DONE]` is
synthesized, so the turn fails with a real message instead of a parse error.

**Repairs** (schema-safe, meaning-preserving): `delta.content: null` and other
null-valued typed fields are stripped; `{"error":"str"}` is reshaped to
`{"error":{"message":"str"}}`.

## Model-based vision handoff

A text-only model cannot see images. When a request targets a configured
text-only model **and** carries image parts, the proxy:

1. captions each image with a vision model (`glm-5.3-flash` by default),
2. replaces the image part with the caption text,
3. forwards the request to the **original** model with the same `stream` flag.

One request in, one stream out — invisible to the client. The original model
answers normally, using the caption. Captions are cached by image hash (1h TTL,
200 entries), so the same screenshot resent on later turns costs nothing —
measured: 5.3s cold vs 1.1s warm, identical answer.

Config:

| Env var | Default | Meaning |
|---|---|---|
| `LOGFARE_PROXY_VISION` | on | `0` disables the handoff |
| `LOGFARE_PROXY_VISION_TARGETS` | `glm-5.3` | comma list of text-only models to hand off (`*` wildcard supported) |
| `LOGFARE_PROXY_VISION_MODEL` | `glm-5.3-flash` | vision model used for captions |
| `LOGFARE_PROXY_VISION_PROMPT` | built-in | caption prompt override |

Accepted image shapes: `{"type":"image_url","image_url":{"url":...}}`,
`{"type":"image","source":{"media_type","data"}}`, `{"type":"input_image"}`.
If the vision call fails, the image is replaced with
`[Image attached by the user — vision analysis unavailable]` so the turn still
proceeds.

> **Requirement — the target model must advertise image input.** OpenCode
> filters attachments client-side: if the model's `capabilities.input` in
> `opencode.json` is `["text"]`, the image is stripped from the request and
> never reaches the proxy (nothing to hand off — the model just says it can't
> see the image). For a text-only target like `glm-5.3`, declare it as:
>
> ```jsonc
> "glm-5.3": {
>   "capabilities": { "tools": true, "input": ["text", "image"], "output": ["text"] }
> }
> ```
>
> The proxy then captions the image and removes it before the upstream call, so
> the real text-only model never receives image bytes. Capability changes need
> an OpenCode restart (provider snapshot is cached).
>
> Verify with: `opencode run -m logfare/glm-5.3 --file=shot.png "what text is in the image?"`
> The proxy log should show `non-text parts: image_url{type,image_url}` followed
> by `vision handoff model=glm-5.3 images=1 analyzed=1`.

### Retry classes

| Failure | Class | Behavior |
|---|---|---|
| `429`, Serverless token-per-minute limit (status or in-band) | patient | retry until it works: 5/7/10/15/20/30s ladder, `Retry-After` honored (cap 60s), 25-min budget |
| `AiError 8006` body-size, `413` | patient | same-body retry until it works (flaky per attempt) |
| other 5xx, network death pre-commit, invalid event pre-commit | hard | 10 retries, then give up |
| invalid event post-commit | — | dropped inline |
| upstream death post-commit | — | healed with valid error chunk + `[DONE]` |
| give-up | — | forwards the last status, or the real upstream error as a valid SSE event |

Patient retries never consume the 10 hard retries.

## Standalone

The same file runs outside OpenCode:

```sh
bun index.js            # or: node index.js
```

| Env var | Default | Meaning |
|---|---|---|
| `LOGFARE_PROXY_PORT` | `17330` | listen port (127.0.0.1 only) |
| `LOGFARE_PROXY_UPSTREAM` | `https://logfare.ai/v1` | upstream base URL |
| `LOGFARE_PROXY_KEY` | — | fallback bearer token when the request has none |
| `LOGFARE_PROXY_DEBUG` | off | `1` = log every upstream chunk and forwarded event |
| `LOGFARE_PROXY_VISION` | on | `0` disables the vision handoff |
| `LOGFARE_PROXY_VISION_TARGETS` | `glm-5.3` | text-only models that receive captions instead of images |
| `LOGFARE_PROXY_VISION_MODEL` | `glm-5.3-flash` | vision model used to caption images |
| `LOGFARE_PROXY_VISION_PROMPT` | built-in | caption prompt override |
| `LOGFARE_PROXY_RL_BUDGET_MS` | `1500000` | patient-retry wall-clock budget |
| `LOGFARE_PROXY_RL_BACKOFF_MS` | `5000,7000,10000,15000,20000,30000` | patient backoff ladder |
| `LOGFARE_PROXY_BACKOFF_MS` | `300,600,1000,2000,3000` | hard-failure backoff ladder |

Health check: `GET /healthz` → `logfare-proxy OK`.

Per-request overrides (stripped before forwarding): `x-lf-upstream`,
`x-lf-key`.

## Tests

```sh
bun test/run-tests.mjs
```

Spins a mock upstream plus the proxy and asserts 32 scenarios: pre-commit
retry, post-commit drop, in-band error retry, give-up after exactly 10 retries,
null-field repair, mid-stream healing, non-stream passthrough, bulk-JSON
conversion, stall retry, 200+8006 same-body retry, budget exhaustion surfacing
the real error, patient TPM retry, vision handoff (image → caption → text
model), and caption caching (no second vision call). Newer cases also cover
transient `Service temporarily unavailable` retries (no budget), post-commit
suppression of that error, `finish_reason` injection before `[DONE]`/close, and
clean closes without `[DONE]`, post-commit continuation (retry + prefix-skip
after a cut or transient error), abrupt-stop continuation
(`finish_reason=stop` mid-sentence, including Unicode dashes and content in the
finish chunk), and empty-turn recovery (retry when a role-only chunk precedes a
503).

## Desktop working timer (optional)

`desktop-working-timer/` is a small OpenCode Desktop GUI plugin (experimental
`gui-experimental.cjs` API) that shows `Working (20s)` as a composer pill while
the routed session is running. It needs no ASAR patching or app rebuild. Copy it
to `~/.config/opencode/plugins/working-timer/`, then run `opencode reload` and
restart the desktop window. See `desktop-working-timer/README.md`.

## Operational notes

- Listen address is fixed to `127.0.0.1` (never exposed).
- If the port is busy the plugin retries binding every 5s, so a standalone
  instance can hand over to the plugin (or vice versa) without manual steps.
- Logs: console (OpenCode server log) plus `.logfare-proxy.log` next to the
  plugin file, truncated at 2 MB.
- Request bodies are buffered in memory so retries can replay them; this is
  what makes same-body retries possible.
