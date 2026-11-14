# working-timer (OpenCode Desktop GUI plugin)

Shows `Working (20s)` as a composer pill while the routed session is running.
Uses only the experimental desktop plugin API (`@opencode/plugin/gui`) — no
ASAR patching and no app rebuild.

## Install

Copy this folder to your global plugin directory as `working-timer`:

```
~/.config/opencode/plugins/working-timer/
  index.js
  gui-experimental.cjs
```

The server part is a no-op; it exists so the desktop discovers the plugin and
reads its `gui-experimental.cjs` bundle. After copying, run `opencode reload`
and restart the OpenCode Desktop app (or its window) so the GUI bundle loads.

## Behavior

- The pill appears in the row above the composer, beside the app's own
  background/todo pills.
- The label is `Working (<elapsed>)` while the session's execution is running;
  it disappears when the session is idle.
- Elapsed time comes from `Session.Info.time.busy` (the execution start
  timestamp), so it survives window reloads during a turn.

## Limitations

The desktop plugin API exposes composer pills and slots, not the timeline's
internal `Working` status lane. The pill is the supported way to show this
without modifying the app bundle.
