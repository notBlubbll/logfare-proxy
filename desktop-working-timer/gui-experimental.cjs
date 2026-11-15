// working-timer — OpenCode Desktop GUI part.
//
// Shows "Working (20s)" as a composer pill while the routed session is running.
// It polls the logfare proxy's /status endpoint and appends the live retry
// count ("Working (20s · retry 2)") when the proxy is retrying.
// Uses only the experimental desktop plugin API (@opencode/plugin/gui): no ASAR
// patching, no app rebuild. The pill row sits above the composer beside the
// app's own background/todo pills.
//
// Loaded by the desktop from this prebuilt CommonJS bundle:
//   Function("require","module","exports", code)(sharedRequire, module, module.exports)
// `@opencode/plugin/gui` and `solid-js` are provided by the app at runtime.
const { Plugin, ComposerPill } = require("@opencode/plugin/gui");
const { createSignal } = require("solid-js");

// One shared 1s ticker per window; the host re-runs `show` when `tick()` is read.
const [tick, setTick] = createSignal(0);
// Live proxy attempt state, refreshed from http://127.0.0.1:17330/status.
// `sessions` is keyed by OpenCode session id (x-opencode-session-id), so the
// pill only reports failures from the session it belongs to.
const [status, setStatus] = createSignal(null);

function formatSeconds(total) {
  if (total < 60) return `${total}s`;
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return seconds ? `${minutes}m ${seconds}s` : `${minutes}m`;
}

function elapsedSeconds(session) {
  const data = session.server && session.server.data;
  if (!data) return null;
  const id = session.id;
  let running = false;
  try {
    running = typeof data.session.status === "function" && data.session.status(id) === "running";
  } catch {}
  const info = data.session.get(id);
  const busy = info && info.time && info.time.busy;
  const idle = info && info.time && info.time.idle;
  // `busy` is the timestamp the current execution started (see Session.Info.time).
  const byTime = typeof busy === "number" && (typeof idle !== "number" || busy > idle);
  if (!running && !byTime) return null;
  if (typeof busy !== "number") return 0;
  const elapsed = Math.floor((Date.now() - busy) / 1000);
  return elapsed >= 0 && elapsed < 24 * 60 * 60 ? elapsed : 0;
}

module.exports = {
  default: Plugin.define({
    i18n: { en: { working: "Working" } },
    setup(ctx) {
      const timer = setInterval(() => setTick((n) => n + 1), 1000);
      const poll = setInterval(async () => {
        try {
          const r = await fetch("http://127.0.0.1:17330/status", { cache: "no-store" });
          if (!r.ok) { setStatus(null); return; }
          setStatus(await r.json());
        } catch { setStatus(null); }
      }, 1000);
      ctx.add(ComposerPill, {
        id: "working-timer",
        show(input) {
          tick(); // reactive: re-run this pill once per second
          const seconds = elapsedSeconds(input.session);
          if (seconds === null) return undefined;
          const j = status();
          const sid = input.session && input.session.id;
          const own = j && j.sessions && sid ? j.sessions[sid] : null;
          const known = j && j.sessions ? Object.keys(j.sessions).length > 0 : false;
          // Strictly per-session when the proxy reports session ids; fall back
          // to the global count only if no request carried a session id.
          const failed = own ? (own.retries || 0) : (!known && j && j.streams > 0 ? (j.retries || 0) : 0);
          const suffix = failed > 0 ? ` · failed: ${failed}` : "";
          const label = `${ctx.t("working")} (${formatSeconds(seconds)}${suffix})`;
          return {
            id: "working",
            label,
            title: ctx.t("working"),
          };
        },
      });
      return () => { clearInterval(timer); clearInterval(poll); };
    },
  }),
};
