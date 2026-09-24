# External connector components

Fabric is connector-agnostic. A browser, desktop, database, or application connector is an independently installed component package, not a special case in Fabric's runtime. Jev is one possible decision-maker; ordinary models and deterministic programs use the same discovered capabilities.

## Ownership boundary

| Fabric owns | Connector package owns |
| --- | --- |
| Component registration/discovery and configuration | Component definition and config schema |
| Provider catalog, validation, approvals, audit | Action vocabulary, arguments, and results |
| Exact capability commitments and generation pinning | Target identity, scope, freshness, and execution guards |
| Activation, replacement, draining, provider close | SDK loading, connections, subprocesses, and cleanup implementation |
| Generic model-guidance projection | Connector-specific usage guidance |

Fabric does not require every connector to implement `observe`, `act`, or a shared receipt schema. The two harnesses below choose that interface because it fits UI automation. Discover and describe the installed package's actual capabilities before use. Adding a third connector requires no Fabric runtime branch, export, or rebuild.

See [components](components.md) for the authoritative lifecycle, trust, and capability contracts. Extensions are trusted host code; neither a component label nor a model confidence score makes a connector a security sandbox.

## Load the harness-owned extension

Each harness repository supplies an optional `pi/` package:

- `browser-harness-js/pi/extension.ts`: registers `browser-harness`, providing `browser`.
- `macos-harness/pi/extension.ts`: registers `macos-harness`, providing `macos`.

From a sibling `pi-fabric` checkout, try them for one Pi invocation:

```sh
pi -e ../browser-harness-js/pi/extension.ts -e ../macos-harness/pi/extension.ts
```

Or install either local package through Pi's normal package manager:

```sh
pi install ../browser-harness-js/pi
pi install ../macos-harness/pi
```

These are instructions, not actions taken automatically by Fabric. Review/trust the extensions first. Follow each package's README for its host peers and standalone test setup. The adapters do not install the Browser Harness SDK or native Python harness for you.

The extension emits `FABRIC_COMPONENT_REGISTER_EVENT` and answers `FABRIC_COMPONENT_DISCOVER_EVENT`, using the existing v1 protocol. Registration publishes a cheap definition and config schema; it does not connect to an app/browser or start a subprocess. Fabric does not load an extension merely because its name appears in `fabric.json`. Configuration may precede discovery: the instance stays `waiting` until its definition is supplied.

## Configure through the generic component plane

Inspect the installed definition before constructing config:

```ts
return await components.describe({component:"browser-harness"});
```

Use the returned config schema. For example, the Browser Harness adapter accepts an explicit debug endpoint and separate raw/guarded grants:

```ts
const plan = await components.plan({
  entries: [{
    id: "browser",
    component: "browser-harness",
    config: {
      modulePath: "../browser-harness-js/skills/cdp/sdk/session.ts",
      interactionModulePath: "../browser-harness-js/skills/cdp/sdk/interaction.ts",
      wsUrl: "ws://127.0.0.1:9222/devtools/browser/REPLACE_WITH_AUTHORIZED_DEBUG_ID",
      allowedOrigins: ["https://example.com"],
      allowedMethods: ["Target.getTargets", "Target.attachToTarget"],
      callTimeoutMs: 10000,
    },
  }],
});
return plan; // Inspect the change; then apply the exact request/revision under normal approvals.
```

Apply with `components.apply({...plan.request,expectedRevision:plan.revision})`. The default session scope does not write files; use explicit global/project scope for persistence. Alternatively put the same component entry in trusted `fabric.json`. Neither path requests native permissions or implicitly calls `browser.connect`.

The macOS package accepts a trusted argv prefix and exact app grants:

```json
{
  "id": "desktop",
  "component": "macos-harness",
  "config": {
    "command": ["uv", "run", "--project", "../macos-harness", "macos-harness"],
    "allowedApps": ["com.apple.TextEdit"],
    "callTimeoutMs": 10000
  }
}
```

That adapter owns the persistent JSON-lines child process and appends its `serve --app` arguments without a shell. It starts only on explicit `macos.connect`. Its package, not Fabric core, defines subprocess bounds, cancellation behavior, receipts, and native limitations.

Use `tools.call({ref,args})` for newly discovered actions; do not assume a static guest proxy exists. The browser package requires an explicitly attached session ID and an authorized origin. It does not borrow the separate extension-relay daemon's process-local connection. Raw CDP method grants are separate, more powerful capabilities, not origin restrictions.

## Harness workflow, not a core protocol

These harness packages support:

1. **Known deterministic route:** use an exact supported API/shortcut, batch safe deterministic steps, and verify the goal.
2. **Unknown UI decision:** observe bounded state, select an observed target and advertised operation, call guarded `act`, then inspect fresh evidence.
3. **Unsupported mechanic:** use separately authorized raw CDP/AX/vision/script paths, then re-observe. Never use an escape hatch to bypass a denied action.

The browser scope is `{sessionId}`; native scope is `{app}`. Observations carry temporary handles. Read-only context rows may have no supported operations. The packages perform freshness validation inside `act`; they do not trust an earlier model-visible validation step. Their receipts distinguish `executed`, `stale`, `blocked`, and `outcome_unknown`:

- Executed means dispatched, not goal success.
- Stale means re-observe and select again.
- Blocked means resolve the prerequisite/approval or stop, not bypass.
- Unknown means inspect; never blindly replay.

Cancellation is not rollback, and GUI effects cannot be made completely atomic against user/application changes. A model's `DONE` is not independent verification. Exact postconditions belong in code where possible. See the harness-owned skills for supported controls, limits, privacy behavior, and operation names.

The browser controller currently uses bounded light-DOM observation and synthetic DOM input, not a complete accessibility implementation or trusted mouse/keyboard events. The native controller conservatively blocks background AX effects to preserve focus guarantees. These are connector capabilities, not assumptions built into Fabric.

## Jev composition

Jev programs declare only the exact installed refs they need. For these harnesses that may be `jev.evaluate`, `browser.observe`, and `browser.act`, or the corresponding native refs. Connect/attach explicitly before launching a loop; pass the authorized scope as input. Batch independent selection questions and execute only the branch selected by code. Keep the no-match/escalation path and finite budgets.

Other connectors may use entirely different verbs and result shapes. Read their descriptors and guidance. Do not infer a universal UI interface. Keep deterministic rules, permission decisions, and execution outside model confidence. Send only consented, bounded, redacted state to a model; generated prose requires Main or an authorized text helper, not Jev. See [Jev](jev.md) for program/auth contracts.

## Migration and verification

Existing `browser-harness` configuration now requires loading the Browser Harness-owned extension. The component name and raw `modulePath` / `wsUrl` / `allowedMethods` configuration are retained by that package. New guarded features belong there.

The former `BrowserHarnessProvider`, `browserHarnessComponent`, and browser adapter type exports have been removed from `pi-fabric/jev`, along with the core implementation. Code importing them must migrate to the Browser Harness-owned package. There is no compatibility shim or connector-specific `pi-fabric/harness` API; Fabric and Jev expose only generic integration contracts.

Fabric's `tests/fabric-runtime-components.test.ts` checks an arbitrary externally registered device, including configuration before discovery, dynamic invocation, and cleanup. Concrete browser/native adapter tests live with their owning `pi/` packages. Those tests and the native fixture use synthetic/offline backends; live model probes are separate from live-UI reliability tests. No personal browser/app control is required to verify the registration boundary.
