# B4EffectRecoveryProviderV1 — contract freeze v2 (round-2 Phase 1)

**Revision:** K1–K4 corrections to v1 (`43a693524111e4915137c2ff2bafbe0be1d8447ad79da303be492d2b12d07f69`); co-versioned with the interface/fake/suite. This file's SHA-256 is in sibling `B4-CONTRACT.sha256`. Builder evidence is in round-2 `PHASE1.md`; independent acceptance remains CoS/judge-owned. K5–K9 are scheduled for v3, due 2026-10-30 12:00Z. The K4 PR-body update is CoS-owned: the task forbids builder GitHub writes other than branch pushes.

**Scope:** a source-pinned, test-only ABI for S4 development, not a claim that the real provider conforms or that a receipt is genuine. The independent challenge belongs to fv2-l7-main/CoS, 2026-10-29 17:30–18:00Z. No production behavior is changed. The brief and CoS's 14:22Z direction require one file set at `tests/contracts/l7-b4-effect-recovery-provider-v1/`; CUTS.md:15–21 and CHALLENGE-CUTS.md:129–155 require shared conformance and producer-owned fakes, not fixture authority in production.

## 1. Source pins and citation notation

Every repository path below is relative to its named repository at the exact pin, not a moving branch. `T` means the new test-only directory above, whose exact committed bytes and SHA are in RESULT.md. `R3`, `CUTS`, and `CHALLENGE-CUTS` are the supplied files in `/srv/scratch/paul/cos-verdicts-16188/l7-b2-revised-r3-in/`; input digests are in `evidence/source-manifest.json`.

| Name | Pin/source |
|---|---|
| F, actual integration/base | pi-fabric `7a43579021dce430c2b3045c53b5cf9e033f582e`, `fv2/l7-integration-1400`; selected explicitly by CoS 14:22Z. The exact `fv2/l7-integration` branch was absent. |
| B4 dispatch | pi-fabric `00575e015543bb056744c90c1cfe60b2d802a97f`; F's `src/effects/` is byte-identical to this pin (`git diff` evidence). |
| B4 reachable records | pi-fabric `9d265ea84a819ed7a250b90475081bcbd6305989`, dispatch's parent, “persist and reconcile immutable effect records.” The brief's `fa378a77` object was absent after origin fetch. This is the reachable implementation used, **not a claim that an unread absent commit is identical**. |
| B3 stable | pi-fabric `f163d18b474a3d82dde063651990e9f6a53bc8c3`, [draft #1690](https://github.com/Smarty-Pants-Inc/pi-fabric/pull/1690). F includes later changes to its shared interface; F, not the older B3 pin, governs the consumed seam below. |
| P | pi `cf3cf677c07c2ad3a65d3cad43343e88a95c0e73`, paths prefixed `packages/coding-agent/`. |
| M | pi-fabric `6993174850bfd1460f2d3be2ca3f40376c63d1c5`, `fv2/l7-b5-maintenance-admission`; not silently assumed integrated in F. |
| Caller | smarty-dev `815a1c3e05ae33c7623982c3056e9b8bfe04e31c`, [draft #17265](https://github.com/Smarty-Pants-Inc/smarty-dev/pull/17265), `tests/soak/l7-line5/README.md`. |

Source links: [F](https://github.com/Smarty-Pants-Inc/pi-fabric/tree/7a43579021dce430c2b3045c53b5cf9e033f582e), [dispatch](https://github.com/Smarty-Pants-Inc/pi-fabric/commit/00575e015543bb056744c90c1cfe60b2d802a97f), [records](https://github.com/Smarty-Pants-Inc/pi-fabric/commit/9d265ea84a819ed7a250b90475081bcbd6305989), [P](https://github.com/Smarty-Pants-Inc/pi/tree/cf3cf677c07c2ad3a65d3cad43343e88a95c0e73), [M](https://github.com/Smarty-Pants-Inc/pi-fabric/tree/6993174850bfd1460f2d3be2ca3f40376c63d1c5).

## 2. Exact provider interface and method ownership

```ts
interface B4EffectRecoveryProviderV1 {
  settlePendingIntents(sessionId: string, recoveryGeneration: string): Promise<RecoveryEffectsSettlement>;
  readToolResult(effect: SettledRecoveryEffect): Promise<RecoveryToolResultRead>;
  repairMaintenance(input: Readonly<AcceptedRecoveryInput>, executionLeafId: string | null): Promise<RepairRef>;
  resolveRepair(ref: RepairRef): Readonly<RepairReceiptView>;
}
```

This is exactly `T/contract.ts:B4EffectRecoveryProviderV1`. It is a **composition contract**, not the inferred return type of the existing low-level reconciler:

1. **Existing B3 consumer methods:** F `src/recovery/l7-interfaces.ts:243–246`. Settled rows are `{toolCallId,key,outcome,resultBytesHash?}`, outcome is `committed | not-committed | uncertain`; the result is `{settled: readonly SettledRecoveryEffect[], uncertain:number}` (`:186–201`). Reader result is `{message: RecoveryToolResult, source: "native-entry" | "adapter-original", postHook:boolean}` (`:203–212`), with a full native `toolResult` message. Native-entry must be post-hook; adapter-original is strictly transformed regardless of its flag (`:234–242`; `coordinator.ts:264–281`). No timestamp, parent result or missing metadata may be invented.
2. **Existing Pi host producer/resolution seam, not a new producer name:** P `src/core/session-recovery-control.ts:330–331` has `repairMaintenance?(input: Readonly<AcceptedRecoveryInput>, executionLeafId: string | null): Promise<RepairRef>`, described as read-only native/B5 resolution, not input execution permission. The optional marker belongs to the general Pi host; this repair-capable profile requires it. P `agent-session.ts:1741–1753` calls it after actual join and debt drainage. `AcceptedRecoveryInput` is mirrored field-for-field from P `session-recovery-control.ts:26–37`: sessionId, originalInputId, admissionId, deliveryId, kind (`initial|recovery`), text, payloadSha256, deadlineMs, consumption (`{kind:"round"}|{kind:"effect-read",effectKey:string}`). There is **no `produceRepairReceipt` method**.
3. **Only new method:** synchronous `resolveRepair(ref)` is R3 §7.1 (`R3:144`)'s mandatory replacement for validation-only `validateRepair?`. It resolves already produced, registered and barriered evidence; no provider/effect work or asynchronous I/O inside B5's transaction. If evidence is not already available and authenticated, it refuses; it must not return a Promise or caller JSON as authority. CoS's 14:39Z direction identifies this as Main's MC-3 gap.

The real B4 code exposes three *different* lower-level APIs: `createEffectReconciler` returns `reconcilePending(g)`, `settlePendingIntents(sessionId,g)`, and bare-native `readToolResult(effect)` (F `effects/reconcile.ts:247–257`); the module-level settlement binding refuses unbound sessions (`:260–274`); `createCertifiedEffectRecordPort` adds execute/sinks and returns a labelled reader without `postHook` (F `effects/extension.ts:42–60,372–395,401–471`). None is silently cast into a conformant production implementation by this change. The test harness's explicit cast lets the same runtime assertions reveal those gaps (`T/real.ts`).

## 3. Issued-intent identity, ordering and idempotency

| Requirement/current behavior | Exact source |
|---|---|
| K = SHA256(canonicalEncode([1,orgId,logicalSessionId,acceptedInputId,effectSlotId,effectKind])). Host/Pod/boot/attempt/physical aliases and recovery generation are not K. | F `effects/contracts.ts:6–7`; `slots.ts:184–197,349–370`; Caller README:65–78. |
| Settle only stored issued intents; never replay a nested program, ask a model to regenerate arguments, prepare a new effect, or admit input. Same K with changed semantic bytes conflicts; terminal results remain immutable. | F `reconcile.ts:26–28,129–140`; `store.ts:487–526`; `extension.ts:483–495`; R3:97–110. |
| Session-wide serialization surrounds list/attempt/sink/result, including awaits. B3 owns cross-process exclusion; the in-process lock is not a lease or B1 proof. Authority is checked after queue entry and before sink work. | F `store.ts:14–19,35–39,152–175`; `reconcile.ts:45–47,132–140`. |
| Roots are preprovisioned, never discovered from cwd/home. An invalid/missing root refuses; a corrupt ledger cannot appear clean. Missing effects subdirectory versus missing entire REQUIRED history is not an interchangeable case. | F `store.ts:134–146,238–248`; `reconcile.ts:53–57`; R3:78–85. |
| Initial traversal sorts ordinal then key; recursively settles predecessors of the same accepted input first. Only committed predecessors permit a pending successor; missing/not-committed/uncertain predecessors hold it. Cycles hold. Already durable RESULT is projected before dependency checks, not re-executed. | F `reconcile.ts:59–63,104–128,175–187`. |
| Persist ATTEMPT before sink; append RESULT or UNCERTAIN after it. Committed requires original result. Invalid sink outcome/hash and thrown sink errors become uncertainty, not negative proof. Failed result durability stops the pass; no immediate re-execution. | F `reconcile.ts:91–102,131–172,175–185`. |
| Durable committed **and not-committed** RESULT are terminal lookup results. Exact retry/reopen returns retained result without another sink call or generation-dependent write. UNCERTAIN may be reconciled again. | F `reconcile.ts:15–16,110–118,91–102`; `store.ts:524–526`. |
| Output has one row per physical alias, with logical K. `uncertain` counts unresolved logical keys, not aliases; unreadable whole ledger produces uncertain=1, no fake empty success. Nested parent IDs are join references, not synthetic fabric_exec return values. | F `reconcile.ts:57,66–87,187`; `contracts.ts:173–201`. |
| Original peer body, logical recipient, envelope identity, first-offered time and absolute deadline are immutable. Sink receives saved bytes; route/incarnation must not alter business identity. Durable receiver dedupe beyond transport cache lifetime is **required, not implemented by a generic B4 peer sink here**. | F `contracts.ts:95–103`; `slots.ts:331–345,349–370`; `extension.ts:315–317,440`; R3:99–110. |

**File profile:** actual retained-volume write/edit, exact stored pre/post state, mode and payload. Post-state establishes durability and returns original result without replacing inode; pre-state permits the original mutation; neither state is uncertain. Temp file fsync, rename, directory barriers and readback remain real adapter behavior (F `file.ts:307–381`). This contract does not extend certification to arbitrary shell or HTTP.

**Shell profile:** only host-bound `read-sha256-v1`, exact `read-sha256 <JSON path> <SHA256>` command. It uses `/usr/bin/python3 -I -B`, fixed source, controlled environment, no shell, 5-second timeout, 4096-byte output bound, and a 4 MiB checked-file bound. Changed file/hash refuses; a read-only retry is not proof of globally once execution (F `shell.ts:6–16,29–33,79–107,109–150`; `file.ts:14`). Caller README:80–104 agrees; its manifest requires an explicit B5 mapping, not direct deserialization into B4's `EffectManifest`.

## 4. Numbered caller rule — G4 generation ownership

**CoS accepted these rules at 14:37Z as the proposed freeze, not as solved implementation.** R3 §7.1 requires B5's durable original-input generation before settlement (`R3:148–154`); CUTS:19 calls clean/terminal behavior a contract hole. F `coordinator.ts:234–248` currently creates a random UUID even for non-interrupted cases and calls session-wide settlement. The following is the caller contract:

1. B4 never mints a generation. For an open original, caller obtains the B5 retained pin before any generation-dependent settlement or repair. Empty/invalid g is refused, never replaced by randomness (existing B4 check: `reconcile.ts:19–24`).
2. A second crash, lost pin-write ACK, terminal result lookup, changed boot or missing volatile state never authorizes g2. Reuse the retained original pin; **B5/Main**, not B4, refuses missing pin with prior evidence (`RECOVERY_HISTORY_INCOMPLETE`) or different g (`RECOVERY_CONFLICT`) (R3:150–154). B4 has no pin lookup port and the fake performs no hidden pin validation. **C21/C22 now live in `tests/contracts/l7-b5-main-generation-v1/conformance.ts:runGenerationPinConformance`**, for the B5RecoveryAdmissionV1/Main composition driver. That suite is not yet bound/run against real B5 in this delivery. Binding/real execution owner: fv2-l7-main with fv2-l7-b5fix, due 2026-10-30 18:00Z. The configured fake receipt generation is only test evidence input, never proof that B5 checked a pin.
3. Completed/cancelled originals can still have issued obligations. Settle only those obligations under **their retained original pin**; do not start unissued slots, change deadline, create continuation, mint a receipt for clean history, or revive an input (F `coordinator.ts:241–248`; `slots.ts:488`; R3:150–152).
4. A genuinely clean session with no issued intents has no generation-dependent work. Caller with no original pin skips that operation rather than generating a random g. A direct call with a valid supplied g and empty ledger returns `{settled:[],uncertain:0}` without minting or writing (`reconcile.ts:48–57,175–187`; C01/C03). The empty-argument call is still invalid; clean is not permission to weaken the signature.
5. **K10 OPEN — owner fv2-l7-main.** One session-wide `(sessionId,g)` cannot represent several retained originals with different pins. Main owns the per-original hold versus integration decision (challenge due 2026-10-29 17:30Z). No decision file was supplied in round-2 `in/` at this freeze; per the explicit CoS ruling, this item remains OPEN, not a builder decision. Do not stamp unrelated pending intents with one selected original's g or silently claim this suite resolves that case (F `reconcile.ts:54–63,91–102,175`; R3:148–154).

## 5. Repair reference and genuine receipt shape needed by S4

The four-field reference is unchanged P `session-recovery-control.ts:111–116` and M `maintenance-admission.ts:15–18`:

```ts
type RepairRef = Readonly<{
  originalInputId: string;
  recoveryGeneration: string;
  executionBranchEntryId: string;
  repairReceiptSha256: string;
}>;
```

The following **new frozen view encoding** implements R3's named bindings, not a discovered existing receipt implementation. Exact types are in `T/contract.ts:B4RepairRequestV1`, `B4RepairReceiptV1` and `RepairReceiptView`:

```ts
interface RepairReceiptView {
  readonly version: 1;
  readonly sessionId: string;
  readonly originalInputId: string;
  readonly recoveryGeneration: string;
  readonly executionBranchEntryId: string; // A, immutable original anchor
  readonly joinedLeafEntryId: string;       // J, actual joined leaf; J may differ from A
  readonly joinedHistorySha256: string;
  readonly protectedRangeSha256: string;
  readonly protectedEntryIds: readonly string[];
  readonly settlements: readonly SettledRecoveryEffect[];
  readonly nativeEntries: readonly {
    readonly entryId: string;
    readonly deliveryId: string;
    readonly recordKind: "carrier";
    readonly file: string;
    readonly offset: number;
    readonly length: number; // native JSON byte length excluding LF
    readonly bytesSha256: string; // exact serializer bytes including LF
    readonly b6Sequence: number;
  }[];
}
```

- **A/g/session/original/J/history/range/settlement bindings:** R3:136–146. A remains original delivery.executionBranchEntryId; do not substitute J, rewind to A, or drop late protected entries. **K1 removes `viewSha256` from the receipt.** Pi's maintenance-request `viewSha256` hashes an object containing `repair`, whose ref already hashes this receipt (P `agent-session.ts:1769–1777`). Putting it inside the receipt would be circular. Pi still owns that separate, post-repair request digest; removing the invented receipt field requires no Pi change.
- **K2 exact joined-history encoding:** `joinedHistorySha256` equals Pi P's `baseHistorySha256` at J (`agent-session.ts:1757–1768`): `H(JSON.stringify(branch.map(entry => [entry.id, H(JSON.stringify(entry) + "\n")])))`, where `H(text)` is SHA256 of UTF-8 text as lower-case hex. Preserve native branch order and each entry's JSON.stringify property order. Each inner digest includes exactly one LF; the outer array has no trailing LF. Do not canonical-sort, hash only IDs, hash a fixture label, or substitute the receipt hash. B5 compares this with maintenance's actual `baseHistorySha256` (M `maintenance-admission.ts:19–24`; R3:144). C23 derives the expected bytes independently from the harness's observed joined branch and rejects no-LF and canonical-sorted variants.
- **Protected-range encoding is OPEN**, owned by the native/B4 receipt producer with **fv2-l7-main**, due **2026-10-30 12:00Z**, before G2 real-provider judging. The current fixture's range label/hash is not the production encoding. The owner must bind actual protected call/result entries and exact native bytes; this delivery does not invent that format.
- **Native/B6 reference fields:** F `l7-interfaces.ts:111–147,151–179`. `length` excludes LF; bytes hash includes LF. Real native provenance must cover the actual flush receipt and the B6 registered+durable barrier; a sequence number alone is not evidence. The view is a reference projection, not a replacement writer or new B6 API.
- **Hash:** `repairReceiptSha256 = SHA256(UTF8(canonicalEncode(RepairReceiptView)))`, lower-case hex, no newline. This follows CoS/fabric-v2's 14:39Z ruling and the existing B3/B4 canonical encoder (`effects/slots.ts:29–168,171–179`). Native message `resultBytesHash` is separately exact JSON.stringify bytes, not this receipt encoding (`l7-interfaces.ts:190–196`). Joined-history encoding is pinned above. Protected-range encoding remains the dated owner item above; Pi's maintenance-request view digest remains separate from the receipt.
- **Authority and durability:** the existing host repairMaintenance seam must return the ref only after native/B4 production and unchanged B6 registration/barrier. Resolver authenticates retained evidence and matches reference/session/original/g/A; consumer separately checks actual J/history and Pi's separate maintenance-request view. Frozen recursively immutable views, no provider work inside the synchronous lookup (R3:142–146; P `session-recovery-control.ts:330–331`). No plain JSON, arbitrary callback hash or test fake is a genuine receipt.
- **One receipt per original/g:** exact retries resolve identical receipt; after J advances to J2 a new repair-bearing operation refuses `RECOVERY_MAINTENANCE_REPAIR_REQUIRED`, and replacement of original receipt conflicts. No remint/retry-slot rearm (R3:166). This is a requirement exercised by the fake, not delivered real behavior.

## 6. Error contract

`EffectContractError` has `code:string`, `message`, optional `original:{key,state,result?}`; it is **not a closed TypeScript enum today** (F `effects/contracts.ts:204–212`). `T/contract.ts:B4ProviderErrorCodeV1` lists principal recovery-surface codes, not all OS/owner failures. Owner exceptions propagate. A caught sink error commonly becomes `{outcome:"uncertain"}` rather than a rejected promise; consumers must check `uncertain` (F `reconcile.ts:45–57,141–185`).

| Codes | Trigger/source |
|---|---|
| EFFECT_RECOVERY_GENERATION | Blank/non-string g; `reconcile.ts:19–24`. |
| EFFECT_SESSION_MISMATCH, EFFECT_SESSION_UNBOUND, EFFECT_SESSION_ALREADY_BOUND | Wrong logical session, absent/duplicate binding; `reconcile.ts:250–274`, `extension.ts:466–469`. |
| EFFECT_OWNER_MISMATCH, EFFECT_OWNER_STALE | Different store capability or changed owner; `reconcile.ts:36–37,197`; `store.ts:223–229`. |
| EFFECT_SINK_AMBIGUOUS, EFFECT_LEDGER_CORRUPT | Duplicate sink kind/version or cyclic dependencies; `reconcile.ts:39–43,108`; corrupt store may instead become uncertainty. |
| EFFECT_SINK_RESULT, EFFECT_RESULT_HASH | Invalid outcome, committed without result, wrong hash or hash without bytes; converted to uncertainty inside sink processing; `reconcile.ts:141–162`. |
| EFFECT_CALL_ALIAS_MISMATCH, EFFECT_RESULT_UNAVAILABLE | Unknown alias, absent/hash-mismatched durable original; `reconcile.ts:199–205,220–221`; `extension.ts:373`. |
| EFFECT_RESULT_OUTCOME_MISMATCH | Low-level reader checks saved outcome versus descriptor; `reconcile.ts:224`. The certified reader lacks that same explicit check (`extension.ts:372–395`); do not assume every layer emits it. |
| EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR, EFFECT_NATIVE_RESULT_INVALID, EFFECT_ADAPTER_RESULT_INVALID | Parent-result fabrication or incomplete native/adapter result; `reconcile.ts:225–241`; `extension.ts:377–394`. |
| EFFECT_STORE_UNCERTAIN | Namespace/read/write/barrier failure; `store.ts:27,59–62,238–248`. |
| EFFECT_INVALID_OWNER, EFFECT_PERMISSION_DENIED, EFFECT_SESSION_REENTRANT, EFFECT_TRANSACTION_CLOSED, EFFECT_ADMISSION_CLOSED, EFFECT_UNSAFE_PATH | Store construction/permission/lifetime/admission/key checks; `store.ts:134–185,232–255`. |
| EFFECT_INVALID_TRANSITION, EFFECT_INTENT_IDENTITY, EFFECT_INTENT_PAYLOAD, EFFECT_RESULT_IMMUTABLE | Transition/intent bytes/terminal immutability checks; `store.ts:64–113,369–374,526`. |
| EFFECT_CANONICAL_INVALID, EFFECT_MANIFEST_INVALID, EFFECT_INTENT_INVALID, EFFECT_SLOT_INVALID, EFFECT_KEY_INVALID, EFFECT_MANIFEST_VERSION_UNSUPPORTED | Strict canonical/schema/tuple/version validation; `slots.ts:18–26,184–218,277–346`. |
| EFFECT_SLOT_CONFLICT, EFFECT_SLOT_AMBIGUOUS, EFFECT_SLOT_NOT_FOUND, EFFECT_SLOT_PREDECESSOR, EFFECT_INPUT_CLOSED | Changed immutable request, ambiguous/unapproved slot, predecessor missing, closed original; `slots.ts:385,467–493`; `store.ts:493`. |
| EFFECT_ADAPTER_VERSION_UNSUPPORTED, EFFECT_NATIVE_ASSISTANT_UNCONFIRMED, EFFECT_SETTLEMENT_REQUIRED, EFFECT_PENDING_INTENT, EFFECT_CALL_ALIAS_CONFLICT, EFFECT_NOT_COMMITTED, EFFECT_UNCERTAIN, EFFECT_ADAPTER_DATA_INVALID | Dispatch/retained-intent bridge; `extension.ts:327,454,474–503,543`. |
| EFFECT_UNSUPPORTED; EFFECT_FILE_UNSAFE; EFFECT_SHELL_UNSUPPORTED, EFFECT_SHELL_TIMEOUT, EFFECT_SHELL_OUTPUT_LIMIT, EFFECT_SHELL_FAILED | **Message prefixes, not guaranteed `.code` fields**: `extension.ts:100–200,272`; `file.ts:62`; `shell.ts:76,130–142`. |
| RECOVERY_HISTORY_INCOMPLETE, RECOVERY_CONFLICT | B5/Main pin/history refusals, not B4 settlement enforcement; R3:148–154; `../l7-b5-main-generation-v1/conformance.ts`. Receipt identity conflicts remain B4 repair errors. |
| RECOVERY_MAINTENANCE_REPAIR_REQUIRED | Missing/wrong receipt/base evidence, uncertain repair or later J2; P `agent-session.ts:1746–1753`; R3:166. New synchronous resolver uses this same refusal, not a new success fallback. |

The outer certified runtime additionally throws ordinary Errors for closed runtime, wrong session and invalid g (`extension.ts:119,218–226,279`); the conformance target is its real **record port**, whose typed session/generation refusal is testable. No wrapper translates those outer errors in this PR.

## 7. Conflicts and explicit choices

| Conflict | Frozen choice / why | Remaining owner/work |
|---|---|---|
| B4 canonical sorted result hash vs B3 exact native JSON.stringify hash; canonical copying changes original byte order. | Preserve existing B3 consumer contract; do not relabel adapter bytes native. F `contracts.ts:173–187`, `reconcile.ts:149–159`; B3 `l7-interfaces.ts:190–196`; `extension.ts:376`. | B4/Main native result join; C16 RED. |
| B4 labelled result lacks `postHook`; adapter-original omits timestamp and keeps original physical ID. | Keep readToolResult seam unchanged, document incompatibility; no fabricated timestamp/postHook. F `extension.ts:372–395`; B3 `l7-interfaces.ts:208–212,234–242`. | B4/Main; C17 RED, adapter-original integration also outstanding. |
| B4 projects one hash across aliases; B3 requires each alias's full native hash and consistent semantic result. | Require B3 per-alias identity/hash, no blanket alias normalization. F `reconcile.ts:66–83`; B3 `l7-interfaces.ts:190–196,234–242`. | B4/Main; C18 RED. |
| Random g at coordinator and no B5-pin awareness in B4. | Numbered caller rules §4; never add g to K or mint in fake. F `coordinator.ts:240–245`; R3:148–154. | B5/Main; C21/C22 moved to their composition suite, not counted as B4 failures. K10 OPEN, owner fv2-l7-main. |
| No real repair producer/resolver attached to B4 record port. | Reuse Pi `repairMaintenance`, add only synchronous R3 `resolveRepair`; new view encoding is explicit. P `session-recovery-control.ts:331`; F `extension.ts:463–471`. | MC-3/Main/B4 native binding; C23–27/C33–34 RED. |
| Pi currently requires A == current leaf J; B5's original anchor remains A. | R3's two-part A/J/history validation wins. Never rewrite original or rewind history. P `agent-session.ts:1749–1753`; R3:136–146. | Main/P/B5 integration; no production change here. |
| No generic durable peer/control sink in F; additionalSinks is only an extension seam. | Preserve original peer bytes/deadline but explicitly withhold real late-request/cache-expiry qualification. F `extension.ts:315–317,440`; R3:99–110. | B5/transport; C12 is only boundary-probe coverage. |
| Clean/terminal generation unspecified in original API. | CoS-approved numbered caller rule; clean/no-issued work is inert, terminal obligations retain original pin. CUTS:19; F `coordinator.ts:241–248`. | Main caller integration; multi-original decision at challenge. |
| Caller README ends with physical-power-off final scope, while R3 and current brief say live-host Pod-stop. | Current brief/R3 governs this K1 delivery; do not inherit an older qualification promise. Caller README:212–216; R3:50. | Qualification owners; no power-off/RTO claim. |

## 8. Shared suite, linkage, evidence and S4 start

`T/B4-CONTRACT.md`, `B4-CONTRACT.sha256`, `contract.ts`, `fixtures.ts`, `fake.ts`, `real.ts`, `conformance.ts`, and `provider.test.ts` are the **one co-versioned B4 source set**. The sibling B5/Main suite owns the moved C21/C22 assertions. Consumers import the type/fake/suite; `runConformance(provider)` receives an isolated fixture factory, not alternate copied assertions. Test-only controls are not added to the provider ABI. `T/README.md` has exact commands. `tsconfig.build.json:10–11` excludes tests; no production export/registration is added.

**Historical v1 validation (not a v2 re-run claim):** verification used the existing shared dependency installation (Pi SDK package metadata reports `0.87.0`, not P's built declarations; exact package hash/path in `evidence/source-manifest.json`). Runtime recovery imports used by this suite are type-only SDK edges; tests execute the exact F B4 source, not a rebuilt Pi executable. Added-file semantic checks are not co-versioned F/P typecheck proof. The full typecheck timed out at 600 seconds without diagnostics; the no-large-build brief precludes the whole-tree production build. These limits remain explicit, not GREEN.

The shared cases cover no-mint/empty behavior, wrong session/g, retained K/bytes, terminal replay, changed bytes, dependency order/holds, not-committed, lost ACK, probe peer deadline/dedupe, serialization, corruption, aliases, native bytes/provenance, A≠J receipt/ref resolution/forgery/J2, and actual file pre/post/neither and fixed shell behavior (`T/conformance.ts:C01–C34`, excluding C21/C22 now assigned to B5/Main). V2 re-run results and every test line are retained in round-2 `PHASE1.md` and `evidence/phase1/`. Fake GREEN is COMPONENT ONLY. Real B4 failures are ordinary RED tests, not skipped/expected-success conversions.

**S4 can start** by importing this exact test-only type and fake, binding its own durable B5 generation fixture, and consuming `repairMaintenance -> RepairRef -> resolveRepair` with A distinct from J. Main must retain the G4 multi-original OPEN item, resolve MC-3 and B3 result-format gaps with real native/B6 provenance, and rerun the **same** suite with the real composed provider before claiming G2. Real peer/control late requests, B6 barriers, process-death/second-host recovery, default-executable proof and S3/S7/S8 remain outside these component checks (R3:156–186; CHALLENGE-CUTS:129–155). No default REQUIRED activation, engine-delivery or ≤120-second claim follows from this artifact.
