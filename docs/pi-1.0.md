# Pi 1.0 compatibility

Fabric is developed against `@earendil-works/pi-{coding-agent,ai,agent-core,tui}` **1.0.0**. Host packages remain peers, not bundled runtime dependencies. The peer ranges remain `*`: existing runtime fallbacks are retained where cheap, but this migration's verified host is 1.0.0, not a new certification of 0.87 or every older host.

## Native Codemode and tool ownership

Pi 1.0 includes native `codemode`, `tool_search`, and MCP support. They do not replace Fabric: Fabric registers `fabric_exec` and `/fabric`, not those native names. Pi only replaces a built-in extension automatically when another extension registers the same tool, command, or flag name.

With Fabric's default `fullCodeMode: true`, or whenever `schema.mode` is `"enforce"`, **`fabric_exec` remains authoritative**. Fabric removes native `codemode` and `tool_search` from the model's active tools, independently of `capture.enabled`, `capture.hideFromModel`, or `capture.keepVisible`. Ownership is reasserted after registry refresh and before each model turn. A defensive tool-call gate also rejects native orchestrator execution if an asynchronous refresh or stale declaration races the loadout. Releasing exclusive mode restores previously active native tools. Fabric leaves their registrations intact for host diagnostics and never captures the two native orchestrators as `extensions.*` tools.

For a Fabric-only profile, explicitly disable the competing built-in extensions in **Pi's** `<agent-dir>/settings.json` (usually `~/.pi/agent/settings.json`):

```json
{
  "extensions": ["-builtin:codemode", "-builtin:tool-search", "-builtin:mcp"]
}
```

Merge these entries with existing resource settings; do not overwrite other settings. `--no-extensions` also disables Pi 1.0 built-in extensions, so isolated workers must explicitly load the Fabric entry with `-e /absolute/path/to/dist/index.js`.

To deliberately use native Codemode instead, relinquish Fabric's exclusive ownership in **Fabric's** `fabric.json`:

```json
{
  "fullCodeMode": false,
  "schema": { "mode": "off" },
  "capture": { "enabled": false },
  "mcp": { "enabled": false }
}
```

Then enable native Codemode through Pi's `defaultTools: ["+codemode"]`, or use native MCP auto-activation. This chooses native MCP and disables Fabric MCP; it is not a transparent migration of Fabric provider configuration. Keep `schema.mode` non-enforcing when relinquishing Fabric execution authority. Do not use `capture.keepVisible` as an override for native orchestrators in exclusive mode.

## MCP: one owner per server

Fabric uses its own MCP provider/catalog through `mcporter`, with scripts calling `mcp.<server>.<tool>`. Native Pi reads `<agent-dir>/mcp.json` and trusted project `.pi/mcp.json`, and registers tools named `mcp__<server>__<tool>` (sanitized, sometimes hash-suffixed). Registering the same server in both systems creates two connections and two tool surfaces; neither config silently wins over the other. Fabric's `/fabric` command does not replace native `/mcp`.

Recommended default: Fabric owns the server; disable `builtin:mcp` in Pi settings. If native MCP owns it, disable Fabric MCP and choose native `direct` tools or relinquish full-code mode as above. A native `mcp.json` can set `autoEnableCodemode: false` to prevent automatic native Codemode activation, but that does not by itself disable the native MCP connection or deferred tool search.

## Deferred and hidden tools

In Pi 1.0, `getActiveTools()` is the model declaration set, not the full callable catalog. Native MCP connects asynchronously; deferred/codemode tools may be callable before they are declared. `tool_search` can declare deferred tools, and Pi restores those declarations on reload/resume.

Fabric capture observes registered tools on registry refresh. Callable `direct`, `codemode`, and `deferred` tools remain eligible for capture and discovery. `hidden` and `model-only` tools are excluded from capture: keeping a tool registered for auditors must not turn it into a callable Fabric tool. With capture disabled, native deferred tools are not automatically bridged into Fabric; use a single MCP owner rather than assuming Fabric waits for native servers.

Captured tools on a capable host receive the live runner's call-bound tool context, including `tools` and `executeTool()`. Cwd overrides preserve the host's guarded getters and non-enumerable nested APIs. Older hosts retain plain contexts, and unsupported nested calls fail rather than implementing a second tool pipeline. Native nested-call execution still follows Pi's tool hooks. Calls carrying a native `parentToolCallId` do not inherit Fabric's already-authorized nested-ID exemption: schema top-level authorization and direct-call approvals apply, even if their ID starts with Fabric's prefix. In schema enforce, a captured extension cannot bypass policy by calling native `ctx.executeTool()`. Native `ctx.executeTool("fabric_exec", ...)` is refused before child execution, in every schema mode, even while the calling Fabric invocation is live and even with a caller-supplied signal. Native child promises are not joined by the outer Fabric runtime, so Pi-hook authorization revocation alone cannot fence their later non-Pi provider effects. Run the needed Fabric provider calls in the current program instead; supported agent APIs retain their separate lifetime contract. Ordinary top-level `fabric_exec`, Fabric-mediated Pi/captured calls, and native nested calls to other tools are unchanged. Follow-up qualification should cover bounded compaction records for custom extensions that use this API.

## Other host behavior changes

- TUI is fullscreen by default, with the system theme. Use Pi `tuiMode: "regular"` or `--tui-mode regular` for terminal scrollback. RPC tests do not qualify fullscreen, mouse/focus, overlays, or terminal-specific rendering.
- Pi 1.0 shell failures can return `isError` and structured exit status instead of throwing. Fabric classifies the unmodified execute result before middleware, preserving `settle: true`, explicit recovery, redaction, and native exit status without trusting status-looking stdout. Captured tools' returned `isError` is respected too.
- RPC prompt/steer/follow-up replies now include a per-input disposition (`started`, `queued`, `handled`). Acceptance is not completion or a durable receipt; wait for `agent_settled` for settled work.
- Reload/session replacement invalidates old contexts. Fabric retains its stale-context and ownership rearm behavior.
- Dynamic tools/prompt sections use mid-conversation system messages. Fabric preserves structured prompt sections; native virtual models and image/classifier models are not automatically substituted for Fabric routing/Jev.
- The new `mcp_servers_change` extension event is a native configuration/control event and is intentionally not fanned out to Fabric actors.

## Deployment owner gate

A dependency bump and green build are not authorization to replace Smarty's live Pi fork. Before shared-runtime promotion, the owner must reconcile exact fork commits for session JSONL RSS bounds (pi#89/#94), pi#105, and in-flight input admission (smarty-dev#3048). Upstream 1.0 TUI retention fixes and RPC dispositions do not establish equivalence to those patches. Preserve a known-compatible rollback.

Source comparison also identifies upstream gaps in `hostCapabilities.turnProvenance`, `ctx.isPromptPending()`, process-local `--no-auto-compaction`, and the fork's `agent_settled.outcome`. Fabric retains legacy/fail-closed handling; default full-history process agents work, but activation-window inference and verified requester attribution are not certified on vanilla upstream 1.0. RPC `agent_before_settle` has an outcome upstream; that is not evidence for the fork's final settle/compaction-stop outcome contract.

This lane does not redesign native Codemode/MCP integration. Follow-ups: native asynchronous MCP/deferred lifecycle matrix; virtual-model routing and context-size acceptance; nested-call schema/approval and bounded compaction-record qualification; Windows/macOS and fullscreen human smoke; production provider acceptance on the exact rebased fork candidate.
