# Native Pi codemode parity

Fabric's TypeScript kernel exposes Pi-compatible non-chat models, small script state and output helpers in QuickJS and the trusted Node/Bun executors. The runtime stays lazy; it imports no native provider engine or credential resolver during registration.

## Models

`models === tools.models`. Calling `tools.models()` still returns Fabric's legacy authenticated chat catalog. Both aliases also expose:

- `getModelsOfType(type, provider?)`
- `getAvailableOfType(type, provider?)`
- `getModelOfType(type, provider, id)`
- `classify({provider, id}, {state, questions})`
- `generateImages({provider, id}, {input})`

Types are `chat`, `classifier`, and `image`. Chat execution is not exposed. Classifiers accept native `choice`, `score`, and `bool` questions; bool answers contain `probability`. Full catalog entries also work as selectors. Registry lookups resolve the authoritative model; guest-supplied configuration, headers and credentials are never used. Catalog metadata is allowlisted. Authentication and cancellation use the public host `ctx.modelRegistry` methods and their `{signal}` option.

Calls are ordinary registered `native.*` actions: schema validation, approval policies, Schema authorizers, committed capability views and host scopes apply. `native` is a reserved provider name. A pinned view must explicitly include the required native actions; no internal dispatch widens it. Native catalog/load actions have read risk, store has write risk, and model execution has network risk. Scoped hosts additionally need `native:models` read/execute grants or `native:store` read/write grants. At most four classifier/image calls run concurrently per invocation, including mixed calls. Waiting calls cancel with the script.

Native result `stopReason` and `errorMessage` are retained. Their reported usage is normalized and included in the outer tool result, even on native provider errors. This does **not** change direct `jev.evaluate` accounting. Classifier strings are capped at 262144 characters, question/criteria/input collections at 1000, and serialized model requests/responses at 16777216 characters. Provider-specific limits may be lower. Missing native registry methods produce an explicit upgrade/compatibility error.

## Branch-local script store

`store(key, value)` and `load(key)` are synchronous JSON helpers; aliases work too. `store(key, undefined)` deletes. One serialized value is limited to **262144 characters**, and the sum of serialized values to **1048576**. Loads return detached values. This store is not mesh state.

A mounted execution initializes its snapshot through authorized `native.load` before user code. If that read is denied, ordinary scripts can still run, but store/load throw the captured denial. Successful scripts stage through authorized `native.store`, then append a `fabric-codemode-store` custom entry via `pi.appendEntry`. Failed, cancelled, timed-out, and type-error scripts do not commit. Snapshot reconstruction uses only `sessionManager.getBranch()`, so resume and navigation follow the selected branch. Malformed historical snapshots are ignored.

Commit checks the captured session ID, leaf ID and runtime generation. Any intervening leaf/session switch rejects the transaction, preventing writes to another branch. Concurrent writers from one snapshot therefore have a single winner; retry the stale writer explicitly. The guard is intentionally conservative: even unrelated leaf changes invalidate a write. Runtime teardown invalidates outstanding transactions. Standalone execution-service users must mount `nativeCodemode.setPersistence((type, data) => pi.appendEntry(type, data))`; unmounted synchronous state helpers fail explicitly.

## Output and discovery helpers

- `text(value)` emits strings directly and other values as JSON.
- `image(value)` accepts PNG/JPEG/GIF/WebP image blocks, local base64 data URLs, and `{image_url: string | {url}}`. It never fetches remote URLs. Returned local image forms also pass through the media sanitizer.
- `exit()` ends successfully, retaining prior output and committing staged state. Like a thrown control signal it can be intercepted by a guest catch block.
- `console.log/info/warn/error/debug` and `print` remain available.

Explicit images are retained with partial text on errors and exit. Pending host calls are cancelled when a script ends; detached task ownership remains governed by Fabric's existing task contract. Output helpers enforce 100000 emissions and 16777216 characters of explicit text/image output in addition to Fabric's existing log and result limits.

`searchTools(query, {limit?, namespace?})`, `describeTool(name)`, `describeNamespace(name)` and `ALL_TOOLS` offer compatibility discovery. Names are **Fabric-qualified action refs**, usable with `tools.call({ref:name,args})`, not new `tools.<name>` methods. Search uses Fabric's lexical ranking (default limit 8), not a promise of native BM25-identical ordering. The snapshot uses Fabric's bounded discovery page (at most 1000 actions); use existing provider-specific catalog/list/search for larger installations. `ALL_TOOLS` is populated in mounted executions. Descriptions include bounded advisory TypeScript declarations; authoritative schemas remain enforced at dispatch. Namespace `instructions` comes only from actual Pi `namespace.instructions` on a visible tool, never inferred from `description`. Hidden/scoped-out actions cannot reveal namespace guidance.

## Per-call options

Tool arguments accept `maxOutputTokens` and its alias `max_output_tokens`; output is estimated at four characters per token and capped by configured `executor.maxOutputChars`. Truncated output retains Fabric's full-output artifact behavior. Conflicting aliases are rejected.

`timeout_ms` is a positive-integer hard script deadline, capped by `executor.maxTimeoutMs`. It can lower the configured deadline and cannot be extended by nested calls or paused for human waits. Existing `timeoutMs` retains raises-only behavior. If both are supplied, the hard deadline wins.

TypeScript source may begin with `// @options: {"max_output_tokens": 2000, "timeout_ms": 60000}`. Unsupported keys, nonpositive/noninteger values and conflicting explicit arguments fail explicitly.

## Deliberate boundaries

Shell return values/behavior and direct Jev accounting are unchanged. Python's existing kernel and generic `tools.call` surface remain intact; synchronous helpers and native namespace aliases above are TypeScript additions, not new Python syntax. Native store entries are separate from Pi's own `codemode-store` entries, avoiding incompatible cross-extension writes. Fabric's existing memory, nested-call, approval and capability policies remain authoritative.
