# Reachability audit — 2026-11-11

Probed every configured model **direct** against `https://logfare.ai/v1` (bypassing the
proxy), then diffed `/v1/models` and probed each new chat-capable entry.

## Removed from the export (now DEAD — 404 "Model not found")

| model | status |
|---|---|
| `gpt-6.1-sol` | 404 (was live Oct 31 — removed upstream) |
| `noctis-auto` | 404 |
| `noctis-auto-paid` | 404 |
| `obi2` | 404 |

These 16 were already removed on Oct 31 and stayed dead (not re-added):
minimax-m3.1-flash, gpt-5-nano, deepseek-v4-pro-0813, gpt-5-chat-2, gpt-5.4-mini-kb-curate,
gpt-5.2_critique, gpt-4o-mini, gpt-5-chat, gpt-5-mini, gpt-chat-latest, gpt-5.2-chat,
gpt-5-kiara, gpt-4.1-nano, gpt-4.1, gpt-4o.

## Kept (verified 200 + real content)

| model | ctx | tools | vision | notes |
|---|---|---|---|---|
| `glm-5.3` | 750000* | yes | meta no / **declared image** for proxy handoff | 200, content OK, reasoning present |
| `glm-5.3-flash` | 1048576 | yes | yes | 200, content OK |
| `logfare/auto` | 262144 | no | yes (live) | 200 |
| `gemma-4-26b` | 256000 | yes | yes | 200 |
| `qwen-3.8-27b` | 262144 | yes | yes | 200 |
| `deepseek-v4.1-flash-1017` | 1048576 | yes | yes | 200; xhigh→max (upstream clamps literal xhigh) |

\* glm-5.3 context kept at your existing 750000 setting (live metadata says 262144).

## Added (NEW in /v1/models, all verified 200)

| model | ctx | tools | vision | reasoningField | notes |
|---|---|---|---|---|---|
| `glm-5.2` | 1048576 | yes | no | `reasoning` | variants added (low/high/xhigh/max) |
| `gemma-4-31b-it` | 262144 | yes | yes | — | no reasoning field |
| `kimi-k2.6` | 262144 | yes | yes | `reasoning` | back after being dead on Oct 31 |
| `kimi-k2.7-code` | 262144 | yes | yes | `reasoning` | new |
| `economy/deepseek-v4.1-flash` | 1048576 | yes | yes | `reasoning_content` | cheaper deepseek |
| `stealth/funfly` | 14336 | **no** | no | — | tiny ctx, no tools; kept for completeness |
| `stealth/autistic-parrot` | 24576 | **no** | no | `reasoning` | returned empty content on probe |

## Rejected by probe (in /v1/models but NOT chat-capable — not added)

The 24 non-chat models (embeddings, TTS/STT, image-gen, `clef*`, `8ball`,
`phoenix-1.0`, `lucid-origin`) — they 404 on `chat/completions`.

## Review file

`opencode.logfare.json` — 14 models, `apiKey: REPLACE_ME`, direct baseURL
(`https://logfare.ai/v1`, not the proxy). Review before committing to the repo.
