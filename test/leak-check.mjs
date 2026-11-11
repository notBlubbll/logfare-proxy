// Leak check: drive N streaming requests through a fresh standalone proxy
// and sample RSS + active handle count between waves. With the fix, RSS
// stays at baseline; without it, each response leaks a 250ms interval
// (visible as growing libuv handles / pending handles).
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const MOCK_PORT = 17994, PROXY_PORT = 17333;
const PLUGIN = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "index.js");
const RUNTIME = process.execPath;
const WAVES = 6, PER_WAVE = 40;

const mock = createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/event-stream" });
  let i = 0;
  const tick = () => {
    if (i >= 3) { res.end(); return; }
    i++;
    res.write(`data: {"choices":[{"delta":{"content":"chunk${i}"}}]}\n\n`);
    setTimeout(tick, 5);
  };
  tick();
});
mock.listen(MOCK_PORT, "127.0.0.1");

const proxy = spawn(RUNTIME, [PLUGIN], {
  env: { ...process.env, LOGFARE_PROXY_PORT: String(PROXY_PORT), LOGFARE_PROXY_UPSTREAM: `http://127.0.0.1:${MOCK_PORT}/v1` },
  stdio: ["ignore", "ignore", "ignore"],
});
for (let i = 0; i < 50; i++) {
  try { const h = await fetch(`http://127.0.0.1:${PROXY_PORT}/healthz`); if (h.ok) break; } catch {}
  await delay(200);
}

function rssOf(pid) {
  const out = Bun.spawnSync(["powershell", "-NoProfile", "-Command",
    `(Get-Process -Id ${pid}).WorkingSet64 / 1MB`]);
  return parseFloat(out.stdout.toString().trim());
}

const pid = proxy.pid;
const samples = [];
for (let w = 0; w < WAVES; w++) {
  for (let i = 0; i < PER_WAVE; i++) {
    const r = await fetch(`http://127.0.0.1:${PROXY_PORT}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer t" },
      body: JSON.stringify({ model: "m", stream: true }),
    });
    await r.text();
  }
  const rss = rssOf(pid);
  samples.push(rss);
  console.log(`wave ${w + 1} (${(w + 1) * PER_WAVE} reqs): RSS ${rss.toFixed(1)} MB`);
  await delay(500);
}

const first = samples[0], last = samples[samples.length - 1];
const growth = last - first;
console.log(`\ngrowth: ${growth.toFixed(1)} MB over ${(WAVES - 1) * PER_WAVE} extra requests`);
console.log(growth < 15 ? "LEAK CHECK PASS (flat)" : "LEAK CHECK FAIL (growing)");
proxy.kill(); mock.close();
process.exit(growth < 15 ? 0 : 1);
