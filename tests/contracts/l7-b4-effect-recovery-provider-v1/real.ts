import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCertifiedEffectRecordPort } from "../../../src/effects/extension.js";
import type { EffectIntent, EffectSink, RecoveryOwner } from "../../../src/effects/contracts.js";
import { EffectStore } from "../../../src/effects/store.js";
import { canonicalEncode, mintEffectKey, manifestDigest } from "../../../src/effects/slots.js";
import { prepareFileEffect } from "../../../src/effects/file.js";
import { prepareShellEffect, shellEffectCommand } from "../../../src/effects/shell.js";
import { adapterOriginalResult, canonicalCopy, copy, createAcceptedInput, createSinkGate, fail, GENERATION, hash, immutable, intent, joinedBranch, originalResult, resolveScenario, type RecoveryIdentity, type ScenarioSource, type ConformanceHarness } from "./fixtures.js";

/** Unmodified real B4 store/reconciler/record port at the selected integration
 * head. Sinks below are controlled boundary probes, NOT real peer/native/B6
 * qualification. Real file/shell behavior is exercised separately by the suite.
 * Missing repair methods and incompatible read results are intentionally exposed. */
export interface RealOptions {
  readonly now?: () => number;
  /** Supply actual B5 observations in a composed harness; no fabricated pin writes.
   * The old B4-only base has no B5 allocator, so its default is explicitly a probe. */
  readonly recoveryIdentity?: () => RecoveryIdentity;
}
export const createReal = async (source: ScenarioSource, options: RealOptions = {}): Promise<ConformanceHarness> => {
  const probeIdentity = immutable({ acceptedInput: createAcceptedInput(options.now), executionBranchEntryId: "A", recoveryGeneration: GENERATION });
  const recoveryIdentity = options.recoveryIdentity ?? (() => probeIdentity);
  const identity = recoveryIdentity();
  const scenario = resolveScenario(source, { recoveryIdentity });
  const sessionId = identity.acceptedInput.sessionId;
  const root = mkdtempSync(join(tmpdir(), "b4-contract-"));
  const sessionRoot = join(root, "session");
  const workspaceRoot = join(root, "workspace");
  mkdirSync(sessionRoot, { mode: 0o700 });
  mkdirSync(workspaceRoot, { mode: 0o700 });
  const destination = new Set(scenario.committedBeforeAck ?? []);
  const observed: EffectIntent[] = [];
  const generations: string[] = [];
  const sinkGate = createSinkGate(scenario.stallSink);
  let lostAck = scenario.lostAck === true;
  let incarnation = 0;
  const sink = (kind: string): EffectSink => ({ kind, adapterVersion: "1",
    async reconcile(value, context) {
      observed.push(copy(value)); generations.push(context.recoveryGeneration);
      await sinkGate.enter();
      const outcome = scenario.outcomes?.[value.slot.id] ?? "committed";
      if (outcome === "committed") destination.add(value.key);
      if (lostAck) { lostAck = false; throw new Error("destination committed, reply lost"); }
      return { outcome, ...(outcome === "committed" ? { result: scenario.resultSource === "adapter-original" ? adapterOriginalResult(value) : originalResult(value) } : {}) };
    },
  });
  const open = () => {
    const owner: RecoveryOwner = {
      orgId: "contract-org", logicalSessionId: sessionId, sessionRoot, workspaceRoot, sessionFile: join(root, "session.jsonl"),
      pvcUID: "fixture-pvc", exclusionReceipt: "TEST-ONLY-local-boundary", signal: new AbortController().signal,
      attempt: { podUID: `pod-${incarnation}`, bootId: `boot-${incarnation}`, ownerIncarnation: `owner-${incarnation}`, authorityRevision: `${incarnation}` },
      assertLive() {},
    };
    const store = new EffectStore(owner);
    const port = createCertifiedEffectRecordPort({ owner, store,
      admissionForCall() { throw new Error("conformance must not admit unissued work"); },
      async confirmNativeAssistant() { throw new Error("conformance must not invent a native barrier"); },
      assertManifestConstraints() { throw new Error("conformance must not prepare unissued work"); },
      additionalSinks: [sink("contract-file"), sink("contract-peer")],
      // Simulated native boundary returns exact native-shaped bytes; this does
      // not fix B4's hash or missing postHook. B6 provenance is not tested here.
      async readNativeToolResult(effect) {
        if (scenario.resultSource === "adapter-original") return undefined;
        const value = await store.withSession("settle", tx => tx.read(effect.key).intent);
        return { ...originalResult(value) as object, toolCallId: effect.toolCallId } as Awaited<ReturnType<NonNullable<Parameters<typeof createCertifiedEffectRecordPort>[0]["readNativeToolResult"]>>>;
      },
    });
    return { store, port };
  };
  let runtime = open();
  const issue = async (value: EffectIntent) => { await runtime.store.withSession("new-intent", tx => tx.bind(canonicalCopy(value))); };
  for (const value of scenario.intents ?? []) await issue(value);
  const target = join(workspaceRoot, "file.txt");
  if (scenario.fileState || scenario.shellState) {
    writeFileSync(target, "before", { mode: 0o644 });
    const shell = scenario.shellState !== undefined;
    const args = shell ? { command: shellEffectCommand("file.txt", hash("before")) } : { path: "file.txt", content: "after" };
    const prepared = shell
      ? await prepareShellEffect(workspaceRoot, args, { kind: "read-sha256-v1", path: "file.txt", sha256: hash("before") })
      : await prepareFileEffect(workspaceRoot, "write", args);
    const base = intent("file", 0, [], false, identity);
    const slot = { ...base.slot, kind: shell ? "shell-check" : "file-write", canonicalRef: shell ? "pi.bash" : "pi.write", canonicalTarget: target, canonicalArguments: args };
    const tuple = [1, "contract-org", sessionId, base.acceptedInput.id, slot.id, slot.kind] as const;
    const text = prepared.format === "fabric-file-effect-v1" ? Buffer.from(prepared.desiredBase64, "base64") : Buffer.from(canonicalEncode(prepared));
    const payload = { encoding: "base64" as const, bytes: text.toString("base64"), digest: hash(text) };
    await issue({ ...base, key: mintEffectKey(tuple), keyTuple: tuple, slot, canonicalArguments: args,
      canonicalTarget: target, manifestDigest: manifestDigest({ version: 1, slots: [slot] }),
      tool: { ...base.tool, canonicalRef: slot.canonicalRef }, payload, adapterData: canonicalCopy(prepared) as unknown as EffectIntent["canonicalArguments"],
      ...(prepared.format !== "fabric-file-effect-v1" ? {} : { file: { preExists: prepared.pre.exists, preHash: prepared.pre.sha256,
        preMode: prepared.pre.mode, postHash: prepared.post.sha256!, postMode: prepared.post.mode!, desiredBytes: payload,
        preparedResult: canonicalCopy(prepared.preparedResult) as unknown as EffectIntent["canonicalArguments"] } }) });
    if (scenario.fileState === "post") writeFileSync(target, "after");
    if (scenario.fileState === "neither" || scenario.shellState === "changed") writeFileSync(target, "foreign");
  }
  if (scenario.alias) await runtime.store.withSession("settle", tx => tx.append(scenario.alias!.key, {
    state: "ALIAS", toolCallId: scenario.alias!.toolCallId, attempt: runtime.store.owner.attempt, at: "2026-10-29T00:00:00.000Z",
  }));
  if (scenario.corrupt) {
    const key = scenario.intents?.[0]?.key;
    if (!key) fail("fixture requires an intent to corrupt");
    writeFileSync(join(sessionRoot, "effects", key, "intent.json"), "corrupt");
  }
  const provider = runtime.port as unknown as ConformanceHarness["provider"];
  const harness: ConformanceHarness = { provider, recoveryIdentity, issue,
    async reopen() { incarnation++; runtime = open(); harness.provider = runtime.port as unknown as ConformanceHarness["provider"]; },
    destination: () => [...destination], observedIntents: () => copy(observed), generations: () => [...generations],
    async ledgerDigest() {
      const files: Array<[string, string]> = [];
      const walk = (directory: string) => {
        for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
          const file = join(directory, entry.name);
          if (entry.isDirectory()) walk(file); else files.push([file.slice(root.length), hash(readFileSync(file))]);
        }
      };
      walk(sessionRoot); return hash(canonicalEncode(files));
    },
    joinedBranch: () => copy(joinedBranch("J", recoveryIdentity())), sinkGate, // Native boundary probe, not a production reader.
    advanceJoinedLeaf() { /* No producer at this head: suite reports absent method. */ },
    fileSnapshot() { const stat = statSync(target); return { bytes: readFileSync(target, "utf8"), identity: `${stat.dev}:${stat.ino}` }; },
    async dispose() { sinkGate.release(); rmSync(root, { recursive: true, force: true }); },
  };
  return harness;
};
