# Thinking control

`thinking.*` lets a program change the host Pi session's reasoning effort for a bounded scope. It is the adaptive-thinking primitive: a program can raise effort for a hard step and drop back to the baseline afterwards. Fabric applies every change through Pi's own `setThinkingLevel`, so Pi persists the level and clamps it to the active model as usual.

## Guest API

```ts
const before = await thinking.status();
// { level, available, bounds: { min, max }, baseline, override? }

await thinking.set({ level: "high", reason: "proof step" });              // this agent run
await thinking.set({ level: "low", scope: "turns", turns: 3 });           // the next 3 agent_end events
await thinking.set({ level: "xhigh", scope: "session" });                 // until reset
await thinking.reset();                                                   // back to the baseline
```

Python uses the same calls, for example `await thinking.set(level="high", scope="turn")`.

| Action | Risk | Result |
| --- | --- | --- |
| `thinking.status()` | read, no effect, speculation-eligible | `{level, available, bounds:{min,max}, baseline, override?:{level, scope, remainingTurns?, reason?, setAt}}` |
| `thinking.set({level, scope?, turns?, reason?})` | write | status plus `clamped: true` and `requested` when the applied level differs from the request |
| `thinking.reset()` | write | status after restoring the baseline |

- `available` lists the active model's supported levels inside the effective bounds. Without a model, every level is listed, matching Pi.
- `bounds` resolves unbounded ends to the model's lowest and highest supported levels.
- `baseline` is the level that was active when the first override began. A second `set` keeps that baseline.
- `scope: "turn"` (the default) reverts at the next `agent_end`. A `set` inside the running program therefore lasts for the rest of the current agent run. `scope: "turns"` requires `turns` (1 to 20) and counts `agent_end` events, including the current run. `scope: "session"` lasts until `thinking.reset()`. `turns` with any other scope is rejected.
- `reason` is optional, at most 256 characters, and appears in status.
- A requested level is first clamped into `thinking.bounds`, then to the nearest level the model supports inside those bounds (next higher first, then lower, as in pi-ai). When no supported level lies inside the bounds, `set` fails and changes nothing.

`set` and `reset` are control-plane writes. They follow `approvals.write` like `compact.request`, and Schema enforce mode blocks them while still allowing `thinking.status`. Managed hosts do not expose `thinking.*`.

## Persistence and ownership

Every override change appends a `pi-fabric-thinking` custom session entry `{version: 1, override}`. After a reload or restart, Fabric replays the latest entry on the active branch, so a pending revert still happens at the next `agent_end`. Tree navigation replays the entry on the new branch. A malformed latest entry ends the override and never revives an older one.

The `agent_end` revert runs even before the Fabric runtime activates. If the user or another extension changes the level while an override is active, Fabric ends the override without restoring the baseline. It never fights another owner for the setting.

## Bounds

`thinking.bounds` (`{min?, max?}`, default unbounded) caps every level Fabric selects: `thinking.set`, child runs, and inherited child sessions. Level names are validated, and an inverted or malformed value fails configuration loading. See [configuration](configuration.md#thinking-bounds).

## Child runs

`agents.run` and `agents.spawn` accept `thinkingBounds: {min?, max?}`. Explicit ends must lie inside the caller's effective bounds, and omitted ends inherit them. A wider request fails before launch. Each run's `thinking` level (explicit, alias, or `agents.thinking`) is clamped into the child's bounds. A clamped run reports the original level as `requestedThinking` on its handle and record. Without configured bounds, child runs keep the existing behavior.

Fabric passes the child's effective bounds to the worker, which exports them to the child process as `PI_FABRIC_THINKING_BOUNDS` (JSON `{min?, max?}`). A child Fabric intersects its own `thinking.bounds` with that value and never widens it: when the two ranges do not overlap, the parent range applies. At session start, the child moves a level outside its bounds back inside them, which covers a model clamp that would overshoot the parent's maximum. A malformed inherited value fails the child's configuration load. Durable `agents.spawn` sends the narrowed bounds with the request, so the resident host enforces the caller's bounds.
