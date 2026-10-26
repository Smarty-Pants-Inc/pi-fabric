# #342 — portable implementation and evidence handoff

Snapshot for [smarty-inbox #342](https://github.com/Smarty-Pants-Inc/smarty-inbox/issues/342), actor `fs-342hub`. R3 documentation HEAD **`1567ef03a6fe2585bfa2e1388c981965c351e523`** has the source tree of09790 below. **Later integration/test/build receipts are DIRTY UNCOMMITTED WIP diagnostics, NOT reproducible from1567 or a new docs-only head; no source-commit claim.** Parent owns Git, checks, acceptance and installation; lead/CoS own R3 (0 blocking,5 non-blocking,2 questions), not a new challenge round. Read with [DESIGN](../../DESIGN.md), [authenticated rulings](rulings.md) and [timer budget](timer-budget.md).

This document records exact bounded facts read from Parent's handoffs and existing terminal/command output. **No command below was executed by the R2/R3 documentation writers; all validation is Parent-owned.** No native acceptance, release approval or production effect is inferred from source preparation, terminal task completion or a hash. Parent added the explicitly dated fresh integration receipts below; this snapshot does not anticipate later actual-path results.

## Immutable reference points

| Artifact | Full pin / meaning |
|---|---|
| Design inspection baseline | `7ac1f5dcfc2d1b624657bbc5fa0114a31f99f155`, not the installed runtime. |
| Installed pre-candidate hub/link | **`ca4b1482119748394df5125c2a8d2853676f7e04`**. Its actual ledger reader is required by N12, not substituted with candidate code. |
| Frozen pod/provider | **`f33937156f587f2479c651ae301de44ec0f0592a`**, unchanged; divergent from hub, not its ancestor. Shared ancestor `7c8358ff8afb6c16ba7d5acdb3bb584c12804389`. |
| Frozen helper / transactional fixture preparation | `f78625d137230b691ef837a484a89613d3d45a7f`; committed [materializer](../../tests/helpers/frozen-f3393715.ts). |
| R1 design | `fdcafd9aca7cb54bcf569b4caf41b2f80305a797`, DESIGN SHA-256 `01b5f0ea132e52fb6668dbc1f46d954150bc4163b38dd4c3915108a02b443aea`. |
| Settled API/ingress preparation | **`09790bc5f4e0e815a1e3018fbf490af807d49759`**, the 184-test/build receipt below. |
| Actual app inspected, still synchronous | smarty-inbox **`1dc71f70610bc5ec69a1b80df0bcd4170b12ab7f`**, archive SHA-256 **`fdae85c6dae9f9e152b0fc34447add713d00c4491277995947a15f943eea8f82`**. Native Git tar ID verified by Parent; handler/store not executed here. New all-synchronous-wait full pin/CI/REAL probe still required, including precheck/writer/gate and every relevant lock. |
| Pi release inspected / fixture SDK target | **`031c749962972b0cb966e443e921d64654627b20`**. Source-supported no-model SDK/task-entry path, not an executed N7 receipt. |

Frozen helper `src` tree: `98c0b004c04662de15b3d4e1440125d67892fcce`. It archives actual unchanged source/assets, fails if history/lock pins are absent/different, and never falls back to a current sender. Pinned SHA-256 values:

| File | SHA-256 |
|---|---|
| `src/providers/agents-message-router.ts` | `544971ab607c1ad6d5f0ced43bdc540bea124e3e97c6eaa8e291d4eee1bdd7c8` |
| `src/topology/control-plane.ts` | `4101689148db8ba6df87e7a719574d9c6f8397c377b3e8f21a144f4ba711c8fe` |
| `src/providers/agents-provider.ts` | `7bf93ff6f412a94c87159ef58e837a94836d2299a8e78dd545b8166a3fabbed3` |
| `src/fabric-runtime-state.ts` | `6682fce72c33f4bd3e8caafe732ffae3cff8cd50b0094e94e42b906feea649d8` |
| `bun.lock` (old/current equality prerequisite) | `7b871eab624822df21b6d4628f420ffd09e5d7ac4b62eca5436a129fb4b63ebb` |

## Actual preparation receipt at 09790 — PASS within this scope only

Parent's canonical receipt ID **`tmp.nZKIBfEFQu`** is recorded here as historical job identity, not an external filesystem authority link. R2 writer read its command script, terminal JSON and stdout. Terminal facts:

- status **`exited`**, **exit code 0**; PID 951766;
- start **2026-10-25T16:58:26.990479+00:00**;
- end **2026-10-25T16:59:57.629321+00:00**;
- elapsed **90.6388427913189 s**; command deadline 600 s;
- wrapper: `systemd-run --user --scope -p CPUQuota=400% nice -n 10 /home/paul/smarty/smarty-pants/bin/smarty-test-env -- bash <owned-script>`;
- script changed cwd to the lane, selected a staged **nats-server v2.14.7 Linux amd64** through `NATS_BIN`, enabled `set -euo pipefail`, and ran the following exact check commands in order:

```sh
bun run typecheck
bunx vitest run tests/native-actor-handler-registration.test.ts tests/native-actor-global-registry.test.ts tests/global-registry.test.ts tests/bridge-nats-native-ingress.test.ts tests/bridge-nats-carrier.test.ts tests/bridge-nats-ledger.test.ts tests/bridge-nats-link.test.ts tests/nats-auth-path-b-bridge-ask.test.ts tests/mesh-control-idempotency.test.ts --retry=0
bun run assert:lazy-graph
bun run build
```

The absolute temporary script/server location is not required to understand this result. Reproduction requires an authorized pinned NATS executable, matching source/lock/host prerequisites and the same bounded check selection; it is not an instruction to execute in this prep task.

Observed Vitest 4.1.10 output:

| Suite | Passed |
|---|---:|
| `bridge-nats-native-ingress.test.ts` | 23 |
| `bridge-nats-carrier.test.ts` | 20 |
| `bridge-nats-ledger.test.ts` | 24 |
| `global-registry.test.ts` | 27 |
| `bridge-nats-link.test.ts` | 8 |
| `native-actor-handler-registration.test.ts` | 26 |
| `native-actor-global-registry.test.ts` | 19 |
| `mesh-control-idempotency.test.ts` | 34 |
| `nats-auth-path-b-bridge-ask.test.ts` | 3 |
| **Total / files** | **184 / 9** |

Output also records host-package-free lazy UI graph **51 files**, verified-kernel artifact matching source/bridge, fresh build artifact/startup closure **53 startup files / 1,260,413 bytes / 1,155 bytes headroom / 82 stable lazy entries / 200 chunks**. No budget was raised. Parent notes native compiler **UNPINNED local**: this is not release-qualified binary provenance. Nonfatal listener/lock/quarantine diagnostics do not change the terminal result, but are not erased from the original evidence.

**Limits:** the 3 broker tests are the **ordinary bridge ask/cancel path**, not actual native N7/N8, actual frozen-provider→native-owner acceptance or real app effects. The source at this preparation commit supplies host API/registration, global template guards, private ingress receipt/ledger/carrier/link machinery, final semantic binding including MeshStore sender stamp, and preserved generic data-only asks. It does **not** contain the later integrated Manager/control/resident feature. No subsequent dirty integration inherits this PASS merely by sharing HEAD.

Earlier Parent-reported bounded prerequisites: two actual-old-runtime smoke passes; 11 transactional-fixture passes with typecheck/fresh build; affected-test selector pass; 46 global/template guard passes (19+27), which overlap the later 184 selection. These are not added up as independent native coverage. Earlier type/semantic-stamp failures were corrected before the final 184 receipt; final stamp is integrity-bound but never treated as remote identity authentication.

## C2 component receipt — 14 PASS, before five typing fixes only

Canonical job identity **`tmp.X1gVL7tMms`**. R2 writer read command/terminal/stdout:

```sh
bunx vitest run tests/native-actor-control-admission.test.ts --retry=0
```

Same bounded CPUQuota400%/nice/test-env wrapper. Terminal **exited / exit 0**, PID 1329268, deadline 180 s; start **2026-10-25T17:44:45.480020+00:00**, end **2026-10-25T17:44:50.902235+00:00**, elapsed **5.422214994207025 s**. Vitest reports **1 file / 14 tests passed**.

This ran the frozen C2 component bytes before Parent's **five small typing corrections**. The diagnostic typecheck then had five typing issues; Parent corrected them at the same committed HEAD. This initial receipt does not cover those corrections; later evidence is separately recorded below. There is no immutable phase-B source/artifact commit attached to those dirty bytes here. Thus **this 14 PASS receipt is not itself a typecheck/build or integrated-candidate PASS**. Its presence/test doubles and actor adapter make it component proof only, not real credentials/broker/ActorManager/resident N8 proof.

## Later Parent integration — DIRTY UNCOMMITTED WIP, not reproducible from docs HEAD

**All receipts in this section cover uncommitted integration, not source bytes in1567 or a subsequent docs-only head.** The source must be frozen/pinned and required checks repeated on the final candidate. These diagnostics are not release/native acceptance.

Parent supplied the fresh update after confirming C3's bounded native completion at **17:54Z**: all three writer results consumed/native completed exit 0; five typing errors fixed; product HEAD still **09790bc5f4e0e815a1e3018fbf490af807d49759**. R2 writer read the following existing command/terminal/stdout files; it ran no checks.

First job **`tmp.KErzEsvGpE`**: same CPUQuota400%/nice/test-env wrapper, terminal **exited / exit 1**, PID 1555636, start **2026-10-25T18:11:13.597627+00:00**, end **2026-10-25T18:12:03.848687+00:00**, elapsed **50.251052069012076 s**. Its `set -euo pipefail` script ran:

```sh
bun run typecheck
bunx vitest run tests/native-actor.test.ts tests/native-actor-registry.test.ts tests/native-actor-custody.test.ts tests/native-actor-sources.test.ts tests/native-actor-control-admission.test.ts tests/actor-registry-store.test.ts tests/actor-registry-payloads.test.ts tests/actor-registry-writes.test.ts tests/agents-message-router.test.ts tests/agents-provider.test.ts tests/mesh-control-idempotency.test.ts --retry=0
```

Whole-program **typecheck PASS** after the five fixes. Vitest ran **10 files / 229 tests: 216 PASS, 13 FAIL**; hosted-only `agents-provider.test.ts` was excluded, not counted as passed. All 13 failures were in ordinary-actor counterfixtures using policy-unapproved Claude/default Opus; Parent reports **no worker started**. Native core **6/6**, custody **7/7** and control **14/14** passed as components. The script listed lazy/build after Vitest, but **set-e stopped on failure: neither ran**.

Parent changed **only those ordinary counterfixtures** to policy-allowed **Pi/Sol 6.1**, with **no product-code change and no policy bypass**, then ran:

```sh
bunx vitest run tests/native-actor-registry.test.ts tests/native-actor-sources.test.ts --retry=0
```

Second job **`tmp.YcbQ0E70ow`**, same wrapper: terminal **exited / exit 0**, PID 1603989, start **2026-10-25T18:18:27.514770+00:00**, end **2026-10-25T18:18:36.067842+00:00**, elapsed **8.553061287850142 s**. Registry **18/18** and sources **5/5**, **23/23 PASS**.

Therefore the **229 selected tests are covered PASS in aggregate on unchanged product code** after test-fixture-only correction, **not a single all-green first run** and not 216+23 unique tests (the rerun overlaps earlier passing cases). This first-run snapshot stopped before lazy/build; the subsequent fresh integration receipts follow. Actual SDK N7 and broker-native N8 remain **UNRUN**; component passes and native task terminal completion do not prove either. No unreported future gate outcome is inferred.

### Fresh startup/lazy/build receipts after integration

Parent-owned **`tmp.GM3Lc7fbi8`**, same capped wrapper, PID1645997, limit600 s, started2026-10-25T18:25:02.989947Z, exited1 at18:26:40.375025Z (97.38507593004033 s). **Fresh `bun run typecheck` PASS; `node --check` PASS for all five native-owner/resident fixture `.mjs` files; `actor-manager-startup.test.ts`15/15 plus `optional-durable-startup.test.ts`1/1 PASS; `bun run assert:lazy-graph` PASS (51 UI files).** Build itself FAILED the stable dynamic-import guard: ActorManager's first-use `bridge-nats-ingress` imports were emitted under a hashed path. No false overall PASS.

Parent fixed **only the build inventory**: added `src/mesh/bridge-nats-ingress.ts` to `scripts/build.mjs` stable lazy entries, no graph-budget raise. Fresh **`bun run build` PASS** in **`tmp.Xyu3BmnN3J`** under the same CPUQuota400%/nice/test-env wrapper. Canonical receipt validated: PID1784560, limit300 s, started **2026-10-25T18:46:51.287921Z**, exited0 **2026-10-25T18:47:25.743962Z**, elapsed34.45603171503171 s. Artifact assertion: **53 startup files /1,260,413 bytes /1,155 bytes headroom;82 stable lazy entries /204 chunks**. Native helper build remains **UNPINNED local host compiler**, not release-qualified binary evidence. The 229 selected cases plus16 additional startup cases give **245 covered component/startup cases across12 executed files in aggregate**, not one all-green run or actual SDK/broker-native proof. At that historical point only the missing build entry changed after those test receipts. Later Q-A/SDK/source preparation is additional dirty work; those earlier receipts do not qualify its final bytes.

## R3 prepared source and latest Parent diagnostics — not native acceptance

These are **DIRTY UNCOMMITTED WIP**, not reproducible from1567/docs-only head. Preparation is not execution:

- **F-1/SDK prep:** actual Main/standalone provision+verify acquire nonblocking adjacent session flock before SDK/session touch; Main retains fd to kernel death, resident does not inherit. Prepared second-Main/provision/verify contention checks require nonzero/no writes while first Main/resident live. Actual launcher-death/live-old-Main remains a separate service integration requirement. Routine same-build restart preserves resident; different-build upgrade/re-canary requires resident drain/confirmed terminal under custody before artifacts change.
- **SDK19:42 exception:** only approved `031c749962972b0cb966e443e921d64654627b20` SDK-owned UNCONFIGURED full shape: `{id:'unknown',name:'unknown',api:'unknown',provider:'unknown',baseUrl:'',reasoning:false,input:[],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:0,maxTokens:0}`. All own keys/extras (hidden/symbol too), data descriptors, prototypes and nested fields pinned, not a generic unknown-looking object. Deny-all `modelPolicy.allow:[]`, empty auth, zero before_agent_start/provider/tool events retained. Actual sentinel positive, actual catalog-model negative, every-field/container/extra/prototype mutation negative and actual would-be turn refused/zero provider calls **PASS the standalone SDK contract** in Parent's20:38–20:41Z diagnostic below; no fake provider. This is not resident readiness or full N7. SDK pin/source change fails closed pending re-review/rerun. Trusted host assumption, not malicious same-UID protection.
- **Q-A prep:** ledger append uses one encoded complete-line write, full-count check, fsync and fail-stop/rollback; **not disk/power atomicity**. Prepared candidate crash-injection/old SOURCE tail-recovery, short/zero/write/fsync fault and ordinary ask/cancel/receipt cases are UNRUN. Actual installed-ca4 compiled-artifact leg and N12 RUN/PASS remain mandatory. Corrupt-middle STOP/recovery is lead-owned, with preserved evidence, disclosed dedupe risk and post-recovery opKey dedupe check.
- **Residual interface/status:** app consumed local optional `onResidual` ABI20:24:29Z, preserving `crossRead`; metadata-only principal-output grant active and four app codewriters working, **no new full candidate supplied**. **Lead21:09Z accepts DESIGN ONLY/no code yet:** order(A), N7/N8 + doc delta first, then once-per-resident `createHandler(host)`, optional `nativeResiduals` actorStatus subview persisted in existing `actors.json`, committed unknowns restored/coverage UNKNOWN not clean zero. Read-only checker in the hub supervised-unit environment on a transient user timer reads existing status; existing `PI_FABRIC_MESH_ALARM_TO` / `smarty-notify` alarms target **owning lead fabric-store `session:01a118ab-50cf-75ea-ae00-721c27d6d6cc`**, never service Main. Threshold>3 outstanding or age>600s lives in **checker, not core**. No collector/new wire/RPC. Implementation/real watch-alarm/runbook/coverage receipts remain **OPEN pre-canary**, no counter delivered. App21:06 producer N1 repair/re-freeze pending for escaped-child-survives-pipe-close; separate unknown ID must survive other resource closures. [Planned semantics/owner gate](../../DESIGN.md#planned-residual-hoststatusrunbook-gate--open-pre-canary).

Ten **approved** SDK SHA-256 pins prepared in `tests/helpers/native-sdk-no-model.ts` (paths relative to coding-agent `dist/`; approval is not learned from whatever is installed):

| Approved source/package | SHA-256 |
|---|---|
| `../package.json` | `a87892a2985221d6c80a921a5016341117ebf8223f5f49eac12c79b3ac43719c` |
| `../../pi-agent-core/package.json` | `541f131cd43832f75a9c64ee886c367c219c0772da7047c66053d7cc80db98b9` |
| `../../pi-agent-core/dist/agent.js` | `0e73cc09ae5563b42e34272d5f3257766923b4086461944785c24701f7d0d04d` |
| `index.js` | `b2285dc6a9c15d8b577a6dbc287ec6babec2703cf8637afb733ad824156a8d6a` |
| `core/sdk.js` | `3f4859912cb8465239924468690fb8bf1f3d3a843e79273aa234c2923b6ecede` |
| `core/model-runtime.js` | `58a8afd627708c0e43fde668234c70be883134b4c8428846691e503928cb067f` |
| `core/model-policy.js` | `fdb5dc9c090e4b77e084e2bb3a40022eedd537d356767357b15301d9907de88f` |
| `core/agent-session.js` | `8dbef0a2e0f00b99ba176808442840201f29e1b4e80ffa1fd2bf7cc332a65f28` |
| `core/settings-manager.js` | `3ce7458e9c41ccb2bb83f11249148e8d5a97e9bbb6fd68f51dda4665827ffcbe` |
| `core/session-manager.js` | `ca138e988f07b62d403218007a95cf9e9a70d0f194c1eaeb7a4b796f5311d5a9` |

**Latest independently verified Parent WIP receipt supplied to this writer:** `tmp.kKlXumGIjT`, **2026-10-25T20:32:17.945Z–20:33:26.207Z**, terminal **exit0**: fresh whole-program types, **six fixture syntax checks**, fresh build PASS; **53 startup files /1,260,413 bytes /1,155 bytes headroom /82 stable lazy entries /204 chunks**. Native helper compiler **UNPINNED**, not release-qualified. Actual SDK selection **`tmp.IK4xbkVS5x`**, PID2659414, limit600s, **2026-10-25T20:38:47.533775Z–20:41:58.680823Z**, terminal **exit1**, ran only `native-sdk-no-model.test.ts` + `native-actor-residency.test.ts` with retry0: **12/15 PASS, three actual-service cases FAIL** resident-readiness deadlines. Standalone actual-SDK sentinel/registered-model/mutation/would-be-turn contract PASS with zero provider/turn/tool events; actual Main model/session/input-admission readback passes. No service READY, restart/stall/second-Main success is inferred. Existing live launcher custody retained the whole root before cleanup; all **98 archived regular files and4 native manifests verified**, archive SHA-256 **`cfc828ba6252354345668311151a4d12f647f3dada7b0dad3d9f1c70f18d10ed`**. The earlier strict undefined-model diagnostic rejected the SDK's own placeholder; it was not real-model execution. No N7 PASS. Earlier245 component/startup aggregate is not one-shot green or native acceptance. N8/frozen broker, #344/new client, installed rollback and live canary remain open; no new READY/design approval or promised install.

## Consumed source preparations — lifecycle completion, not gate PASS

Parent reports all three phase-B writers consumed and **native terminal exit 0**, at unchanged full HEAD 09790 above. Documentation writer received fresh exclusive doc custody only; source/test/package ownership belongs to Parent. These writer terminals mean their processes exited and files may be integrated, not that prepared tests were run.

### Actor/core

Prepared actor types/manager/directory/registry-payload/store, new internal native-execution helper, participant record and new actor/registry/custody/sources suites plus genuine-ingress helper. Main requirements:

- exact-ID FILE/session/durable native ensure, marker/reserved-ID quarantine before model defaults; no random-ID birth, ordinary row conversion or orphan adoption;
- private admission claim/consume around ephemeral direct asks, exact input snapshot, serialized queue branch before all model/session/AgentManager work;
- await **actual callback promise**; abort/deadline observer failure does not clear drain/inFlight/release custody; `FABRIC_NATIVE_ACTOR_BUSY` on unjoined close;
- metadata-only native actor history/archive, no input/output body broadcasts; direct Fabric reply data unchanged;
- participant remains `kind: actor` + ask/stop and publishes the reserved host-native **`runner: native`** string, matching local actor info/registry; strict FILE/KV validation is unchanged, with no registered model runner, model-binding/steer/followUp/Fabric-recursion capabilities.

`canMaterializeNative` is only consulted in **ensureNativeActor preflight and under registry locks**, not ask/drain/callback/control/READY. It supplements birth readiness only under host's actual-OWNER/confirmed-lease, paused-queues/control-unopened try/finally phase. No general `canConsumeMesh` bootstrap bypass.

**S1 source correction after actual OLD proof (2026-10-25):** [smarty-dev #16896](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/16896) is lead-owned and tracks a Phase-B-only source regression, **not a production defect**. The feature dependency **`656e218f2385e050502d6eeaf8195614ea611d2c`** deliberately retains the omission with **DO NOT LAND ALONE**; lead reviews it together with the separate focused producer-restoration delta. That delta restores `runner: actor.runner`, not validators or either bridge. Origin/main and installed-ca4 source already publish runner unconditionally; sandbox1's root cause remains **UNKNOWN pending installed readback**, cleanup stopped. This does not prove installed artifact/ledger compatibility; N12 stays open.

Parent's canonical **`tmp.N3e6n7nYxJ`** exited0 **2026-10-25T23:37:55.869212Z**, elapsed **180.57979621598497 s**, **3/3 SHAPE tests PASS** in [native-participant-presence.test.ts](../../tests/native-participant-presence.test.ts). Whole-tree archive **`bdf818fe5a172541080013b0b89bce46aae79eb65f83d4a63304c4a0e2a446f8`**, **5,430 regular files /1 native manifest**, was Parent-verified before cleanup. Exact Git-archived f339 FILE/public list + old/current KV reject missing runner/null/number/bad token and accept literal native. Real committed broker rows through the actual old projector retain literal native/drop omitted, preserving root/ordinary controls. Genuine source-pinned031c SDK context + frozen runtime public registry first routed one authenticated ordinary control frame, then one literal-native frame machine-b→real owner machine-a with exact SDK session/owner epoch/message/data and no binding/overrides; omitted yielded `Unknown Fabric actor`/+0 frames. No Fabric ACK/reply/callback was manufactured; routed asks reached the actual40s abort observation, zero callbacks/model/provider/tool events, unchanged24h provider default. **Shape/routing proof only, not actual N7/N8 callback acceptance.**

Measured directory `start→publish` with malformed native FIRST/MIDDLE/LAST produced respectively **host lease only /host lease+root /host lease+root+ordinary**, then failed; root+ordinary succeeded. Actual bridge writer instead refused only the malformed actor and retained ordinary. The S1 test delta keeps these counterexamples using **explicit `form(undefined)`**, adds valid generated-native/root/ordinary publication+mirror and actual FILE parser/epoch coverage, and prevents the fixed producer refresh from masking the old omitted-public-route negative. Prepared first N7 happy-path convergence uses only existing event-driven `owner.until` (bounded60s) to observe one real parsed committed native participant and matching FILE-view epoch after READY; no fake row/epoch, new poller or admission/timer change. **The preparation writer ran no checks; Parent subsequently verified the S1 delta as follows.** Fresh whole-program types, 88 targeted tests across topology/FILE/presence/native control/registry/custody, lazy graph and fresh build PASS (`tmp.3tYiGJV0KZ`, canonical exit0, 2026-10-26T00:09:29.728629Z). The live-custody three-file run (`tmp.gxvfel51SL`, canonical exit1, 2026-10-26T00:13:54.191248Z) finished **16/18 PASS**: actual frozen SHAPE3/3 and SDK no-real-model1/1 PASS; residency12/14 includes the first genuine SDK/native-resident **FILE-ingress component callback**, root input refusal, configured idle, Main-down refusal and same-file/actor Main restart. Source NATS delivery is substituted in that N7 component: it is not the full authenticated frozen-provider/broker→native/public-reply N8 path. Whole-root archive **`53530b63b7d42101dfad7e77ae00ba8072355b8443a908878800826f347f1a13`**, **5,670 regular files /5 native manifests**, was byte-verified before launcher cleanup; failing evidence and raw SDK traces are retained.

**Two N7 failures remain OPEN for the next fix**, explicitly accepted as open by owning lead's 2026-10-26T00:30:12.980Z ordering (A), not waived acceptance: (1) `tests/native-actor-residency.test.ts:398` compares bytes after an **allowed Main restart**, where the pinned SDK appends genuine `thinking_level_change/off` metadata; all refused second-Main/provision/verify pre-crash no-write assertions passed. (2) The stall case reaches its existing **60 s callback-entry observation timeout**, without observed callback entry; initial publication convergence is suspected, not yet a proven cause. No SDK/product/timer/gate relaxation is included in S1. Commit the dependency plus focused S1 on its proven scope first, then start the bounded existing-TLS-helper prep. **Full N7/N8, installed reader, helper N1 and pre-canary holds remain unchanged.**

### Control/admission

Prepared FILE control pre-claim/cache/replay/cancel/stale-ACK authentication, private fifth handler argument across provider/router, native exact-message/no-model preflight, marker rejection before template reconstruction. Actual wire binding (including explicit `{}`)/overrides refuse; only helper-synthesized empty binding/overrides are omitted for legitimate native asks while validated provenance/sender/ownership policy remains.

Positive component fixture uses real carrier/ledger projection/StoreBridgeSide/MeshStore/private verifier/control outcomes, but fake presence, supplied fixed MeshBridge stamp and actor adapter. It is **not** N8. Frozen f339 source read shows wire binding emitted only when model/thinking requested; actual old-provider publication without native binding remains N8 assertion, not a source-read PASS.

### Resident/bootstrap

Prepared runtime/protocol/client/host/native-loader and N7 fixture scaffolding:

- serializable binding declarations only in Main; resident alone imports callback after actual OWNER and confirmed FILE lease, before native readiness;
- immutable desired config compared to **actual loaded owner declarations**, mismatch requires quiescent restart;
- exact fifth admission argument through Main and both resident control-start paths;
- narrow `canMaterializeNative` try/finally, no general admission before READY;
- actual actor teardown before OWNER release, `NATIVE_RESIDENT_CUSTODY_BUSY` retains fence on unsettled promise;
- actual pinned SDK task-entry/native-session helper, held-open RPC stdin, positive-allowlist credential-free **test** environment, synthetic resident module and real-process observer/stall modes.

Actual fixture entry requires fresh built candidate plus explicit `FABRIC_NATIVE_TEST_PI_SDK_DIST` and `FABRIC_NATIVE_TEST_NODE`. Missing prerequisites are **N7 BLOCKED**, not a substitute runtime or successful skip. Synthetic test-home/userInfo isolation is not real-handler production env proof. Linux process/fence coverage skips elsewhere are missing platform coverage, not PASS.

Prepared N7 substitutes the NATS delivery/KV source boundary; it is not real-broker/f339 N8. Real abort-ignoring callback process close/handover, fixed-app all-synchronous-wait heartbeat continuity and full N7 execution were not proved by the writer's host adapter/scaffolding.

## Remaining gates — explicitly UNRUN / not established in this snapshot

1. Post-five-fix fresh whole-program types, aggregate229 selected component tests,16 startup tests, five fixture syntax checks, lazy graph and **fresh integrated build PASS** have the bounded receipts above. Actual resident/SDK coverage and a same-machine fresh-process startup comparison remain separate; no full suite. Earlier184-prep build alone was insufficient for integrated code.
2. N7 actual credential-free SDK Main + detached native resident, real session persistence/restart/input refusal/readiness/custody/idle; N5 real process close/handover and default-timer stalls. Source fixtures are not execution receipts.
3. N8 actual unchanged **f339 public provider** in separate process, authenticated broker, candidate link/file control/real Main/native resident/same-sender reply. Include Main crash under **KillMode=process** with resident surviving→observed root-down notRun/+0; both Main/resident down before ask→actual client opKey error **≤60 s/+0**, stale-route/no-reply as well as missing discovery; same-file/root/actor restart and idle >configured 10 min default.
4. **External application CLIENT** ≤60 s end-to-end including grace/retries, opKey-naming final error, caller-supplied add opKey, hub-down→error→same-key-after-recovery records once, honest possible-entry/commit uncertainty. **inbox342 owns client package delivery**; fixed full installed SHA/readback in dedicated canary caller pods before canary START and relevant production seat pods before production START. Frozen Fabric f339 provider unchanged: not a Fabric pod re-pin. No new client pin/delivery/both-down result asserted.
5. **Hard pre-canary hold on #344:** new immutable full async app SHA **with CI and measured probe**, covers **all synchronous waits on add/refusal**, including precheck/org-search, writer-context/protected-writer, gate/protected-recipient and replay-write/read-audit/repeated lookup/refusal-audit/commit/all3–4 locks; external inbox342 measured10.2s precheck/10.1s writer/20.1s writer+refusal audit; awaited/AbortSignal-aware, unchanged Fabric timers, **no heartbeat gap ≥10 s on any add/refusal path**, <15 s TTL, plus definite caller-visible outcome≤60 s (honest opKey-naming unknown outcome after possible commit). Old1dc pin may stall≥30 s and is not qualified. No replacement pin/proof supplied as done.
6. N9/N10 actual-handler authorized/unauthorized seats, protected writer, stable opKey result across new commands, counted response-envelope loss, whole-process mid-transaction/after-commit kill and crash-atomic dedupe. Generic SQLite fixture success is not real app/data policy acceptance.
7. **N12 actual installed ca4 reader** opens isolated candidate-written ledger (optional toFile body fields + ignored receipt files), mutation/compaction/reopen, and unparseable-middle-line refusal/recovery operating gate. **Candidate bytes readable and torn FINAL recovery are mandatory**: prepared candidate crash-injection + exact old SOURCE recovery and separate actual installed-ca4 **compiled-artifact** leg remain UNRUN. One full-line syscall/full count/fsync/fail-stop is not disk/power atomicity. Source parser similarity is not backward-compatibility PASS. Sealed originals, stopped ingress/native owner/link, no bad-line editing/fresh auto-ledger/unknown replay; corrupt MIDDLE accepted loud STOP with sealed evidence; owning lead chooses backup/fresh with explicit **DEDUPE RISK**, reconciliation/authorization and post-recovery opKey dedupe check.
8. **Before canary:** dedicated CANARY seat pod(s)/TEST machine credential(s)/TEST seats, exact test bootstrap SID/target/allow/Secret mount/readback and hosts-lead/auth-owner mint/revocation authorization/owner. Two-seat machine-boundary test uses two independent TEST credentials plus dedicated unauthorized negative identity, never production org-kate/org-paul seats/credentials or same credential/two sessions. Lane has no mint/install authority. Canary is not real production-seat proof; existing N9 independent app-policy gates remain. Separately inventory installed item-18 **factory/current/setup/org.json** current Kate/Paul SIDs, distinct org-kate/org-paul machines/Secrets/no sandbox/no reuse; real HOME=passwd home/XDG unset-or-real `.config`/no SMARTY_INBOX_*/no impersonating role; exact Main mesh root = bridge **config.file.mesh** + resident readback before READY; live allow/stream/security/data approvals.
9. Exact native candidate full Git build SHA + immutable artifact digests for **Main/resident/bridge**, SDK/bootstrap/adapter/app pins, distinct 32-hex canary ID/**own stable Main root**, authorized side-by-side **same-build native-code canary after Proof A**. A non-native #1572 build cannot qualify. Production actor **5027cfd59b1247b6bb4666970c481cc4 unused**, service **not STARTED or BOOT ENABLED before pass**. Only then production-pin smoke/real-seat acceptance.
10. Lead/CoS-owned **R3 documentation response** (0 blocking,5 non-blocking,2 questions), not a new challenge/approval; code/review/security/CI/install decisions remain owner gates. No child self-approval. Parent commits the docs and provides new full commit/SHA by **2026-10-26 01:30Z** (principal-lead 02:00Z).
11. **OPEN residual host/status/runbook gate:** lead21:09 accepts the design seam/route above only, no code yet; after N7/N8 + this delta, implement/test constructor/status/persistence/freshness/watch/owning-lead delivery. Threshold belongs in checker, not core; restart/never-clear-unknown coverage owner gate remains distinct. `outstanding=alive+unconfirmed`, strict>3 or age>600000ms, stable first-host age, last64 separate active IDs, no unknown clearance on restart, no post2s probing/signalling or late inbox mutation. Counter/injection/status/watch not claimed implemented. F-1 actual second-Main and launcher-death/live-old-Main integration tests likewise not passed by fixture preparation.

**Latest authenticated planning override:** fabric-store Main delivered2026-10-25T18:37:25.488Z, relaying principal-lead18:33Z: native hub/resident rollout HOLDS on #344's proper async fix/new immutable SHA+CI+measured all-add/refusal<10s heartbeat/≤60s outcome probe. **Hard pre-canary gate; app-owner ETA follows its design revision;06:00Z is no longer forcing. No confirmed install ETA.** Historical R2~10-26 22Z/before10-27 06Z targets are conditional, never a reason to cut proof. R1 only if all gates green/approved by10-26 15Z. Design commit+SHA deadline10-26 01:30Z unchanged; implementation continues.
