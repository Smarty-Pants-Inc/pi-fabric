import { lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import {
  EffectContractError, type AcceptedInput, type EffectIntent, type EffectSlot, type JsonValue,
  type RecoveryOwner, type SettlePendingIntentsResult,
} from "../src/effects/contracts.js";
import { createCertifiedEffectRecordPort } from "../src/effects/extension.js";
import { prepareFileEffect } from "../src/effects/file.js";
import { canonicalEncode, manifestDigest, mintEffectKey, sha256Bytes } from "../src/effects/slots.js";
import { EffectStore } from "../src/effects/store.js";

// Keep reason: data safety at the original-selection boundary. This NEW shared
// B4 contract runs the SAME assertions against a test-only FAKE effect-selection
// model and the REAL B4 store/record port/file sink. Neither is a B5 admission
// driver. The fake is not a runtime authority, pin validator or B5 imitation.
// Existing frozen contracts/fakes/C21/C22 are not modified or replicated here.
interface Spec {
  original: string;
  slot: string;
  ordinal?: number;
  predecessors?: readonly string[];
  issued?: boolean;
  terminal?: boolean;
  aliases?: readonly string[];
}
interface Summary {
  key: string;
  original: string;
  slot: string;
  transitions: Array<{ state: string; generation?: string; outcome?: string }>;
}
interface ScopedHarness {
  key(original: string, slot: string): string;
  settle(original: string, generation: string): Promise<SettlePendingIntentsResult>;
  legacy(generation: string): Promise<SettlePendingIntentsResult>;
  inventory(original?: string): Promise<unknown>;
  summaries(): Promise<Summary[]>;
  destination(original: string, slot: string): string | undefined;
  sinkCalls: Array<[string, string, string]>;
  projections: string[];
  failProjection(original?: string): void;
  attemptNew(): Promise<void>;
}
type Factory = (specs: readonly Spec[]) => Promise<ScopedHarness>;
const gA = "component-only-opaque-A";
const gB = "component-only-opaque-B";
const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});
const name = (original: string, slot: string) => `${original}/${slot}`;
const call = (original: string, slot: string) => `component-call:${name(original, slot)}`;
const contents = (original: string, slot: string) => `immutable ${original}/${slot}: 雪\n`;
const refusal = (code: string): never => { throw new EffectContractError(code, `test-only FAKE B4 refusal: ${code}`); };

/** Independent, deliberately bounded test-only FAKE B4 effect model. No input
 * admission, lifecycle, deadline, receipt producer or recovery-pin allocation. */
const fakeFactory: Factory = async specs => {
  type Row = { spec: Spec; key: string; transitions: Summary["transitions"]; terminal: boolean };
  const rows: Row[] = specs.filter(spec => spec.issued !== false).map(spec => ({ spec: { ...spec },
    key: sha256Bytes(`test-only-fake-key:${name(spec.original, spec.slot)}`),
    transitions: spec.terminal ? [{ state: "RESULT", generation: "historical-component-pin", outcome: "committed" }] : [],
    terminal: spec.terminal === true,
  }));
  const destinations = new Map<string, string>();
  const sinkCalls: ScopedHarness["sinkCalls"] = [];
  const projections: string[] = [];
  const failedOriginals = new Set<string>();
  let projectionFailure: string | undefined;
  const execute = async (selected: Row[], generation: string, original?: string): Promise<SettlePendingIntentsResult> => {
    const settled: SettlePendingIntentsResult["settled"] = [];
    const outcomes = new Map<string, "committed" | "uncertain">();
    const visit = (row: Row): "committed" | "uncertain" => {
      const previous = outcomes.get(row.key);
      if (previous) return previous;
      let outcome: "committed" | "uncertain" = "committed";
      if (!row.terminal) {
        for (const predecessor of row.spec.predecessors ?? []) {
          const dependency = selected.find(candidate => candidate.spec.original === row.spec.original && candidate.spec.slot === predecessor);
          if (!dependency || visit(dependency) !== "committed") { outcome = "uncertain"; break; }
        }
        if (outcome === "committed") {
          row.transitions.push({ state: "ATTEMPT", generation });
          sinkCalls.push([row.spec.original, row.spec.slot, generation]);
          destinations.set(name(row.spec.original, row.spec.slot), contents(row.spec.original, row.spec.slot));
          row.terminal = true;
        }
        row.transitions.push({ state: outcome === "committed" ? "RESULT" : "UNCERTAIN", generation, outcome });
      }
      outcomes.set(row.key, outcome);
      for (const toolCallId of [call(row.spec.original, row.spec.slot), ...(row.spec.aliases ?? [])]) {
        settled.push({ key: row.key, toolCallId, outcome,
          ...(outcome === "committed" ? { resultBytesHash: sha256Bytes(`fake-result:${row.key}`) } : {}) });
      }
      return outcome;
    };
    for (const row of [...selected].sort((a, b) => (a.spec.ordinal ?? 0) - (b.spec.ordinal ?? 0))) visit(row);
    const uncertain = [...outcomes.values()].filter(outcome => outcome === "uncertain").length;
    for (const effect of settled) {
      if (effect.outcome !== "uncertain") {
        projections.push(effect.toolCallId);
        const row = selected.find(candidate => candidate.key === effect.key)!;
        if (projectionFailure === row.spec.original) {
          failedOriginals.add(original ?? "legacy");
          throw new Error("probe:native-Pi: component projection unavailable");
        }
      }
    }
    if (uncertain) failedOriginals.add(original ?? "legacy");
    else if (original === undefined) failedOriginals.clear();
    else failedOriginals.delete(original);
    return { settled, uncertain };
  };
  return {
    key(original, slot) { return rows.find(row => name(row.spec.original, row.spec.slot) === name(original, slot))!.key; },
    settle(original, generation) { return execute(rows.filter(row => row.spec.original === original), generation, original); },
    async legacy(generation) {
      if (new Set(rows.filter(row => !row.terminal).map(row => row.spec.original)).size > 1) refusal("EFFECT_ORIGINAL_SCOPE_REQUIRED");
      return execute(rows, generation);
    },
    async inventory(original) {
      return JSON.stringify({ records: rows.filter(row => original === undefined || row.spec.original === original),
        destinations: [...destinations].filter(([id]) => original === undefined || id.startsWith(`${original}/`)) });
    },
    async summaries() { return rows.map(row => ({ key: row.key, original: row.spec.original, slot: row.spec.slot,
      transitions: row.transitions.map(value => ({ ...value })) })); },
    destination(original, slot) { return destinations.get(name(original, slot)); }, sinkCalls, projections,
    failProjection(original) { projectionFailure = original; },
    async attemptNew() {
      if (failedOriginals.size) refusal("EFFECT_SETTLEMENT_REQUIRED");
      if (rows.some(row => !row.terminal)) refusal("EFFECT_PENDING_INTENT");
      destinations.set(name("C", "new"), contents("C", "new"));
    },
  };
};

// Complete recursive inventory: includes directories, modes, hard-link counts,
// publication stages and witnesses, and every byte (not just canonical tails).
// Reads do not use mtime/atime as evidence; ordinary verified reads may touch atime.
function diskInventory(directory: string): unknown {
  const entries: Array<{ path: string; mode: number; links: number; bytes?: string }> = [];
  const visit = (file: string, relative: string) => {
    const stat = lstatSync(file);
    if (stat.isSymbolicLink() || (!stat.isFile() && !stat.isDirectory())) throw new Error("Unexpected test-owned inventory entry");
    entries.push({ path: relative, mode: stat.mode & 0o7777, links: stat.nlink,
      ...(stat.isFile() ? { bytes: readFileSync(file).toString("base64") } : {}) });
    if (stat.isDirectory()) for (const child of readdirSync(file).sort()) visit(join(file, child), `${relative}/${child}`);
  };
  visit(directory, ".");
  return entries;
}

const realFactory: Factory = async given => {
  const root = mkdtempSync(join(tmpdir(), "fabric-scoped-contract-"));
  roots.push(root);
  mkdirSync(join(root, "session"), { mode: 0o700 });
  mkdirSync(join(root, "workspace"), { mode: 0o700 });
  const owner: RecoveryOwner = {
    orgId: "component-real-B4", logicalSessionId: `component:${root}`,
    sessionRoot: realpathSync(join(root, "session")), workspaceRoot: realpathSync(join(root, "workspace")),
    sessionFile: join(root, "probe-native.jsonl"), pvcUID: "probe:native-Pi:volume", exclusionReceipt: "probe:native-Pi:excluded",
    attempt: { podUID: "probe:native-Pi:pod", bootId: "probe:native-Pi:boot", ownerIncarnation: "probe:native-Pi:owner", authorityRevision: "probe:native-Pi:revision" },
    signal: new AbortController().signal,
    assertLive(permission) { if (permission !== "settle" && permission !== "new-intent") throw new Error("Invalid component permission"); },
  };
  const store = new EffectStore(owner);
  const specs: Spec[] = [...given, { original: "C", slot: "new", issued: false }];
  const slots = new Map<Spec, EffectSlot>();
  const accepted = new Map<string, AcceptedInput>();
  for (const original of new Set(specs.map(spec => spec.original))) {
    const group = specs.filter(spec => spec.original === original);
    for (const [index, spec] of group.entries()) slots.set(spec, {
      id: spec.slot, ordinal: spec.ordinal ?? index, predecessors: spec.predecessors ?? [], kind: "file-write",
      canonicalRef: "pi.write", adapterVersion: "1", canonicalTarget: join(owner.workspaceRoot, `${original}-${spec.slot}.txt`),
      canonicalArguments: { path: `${original}-${spec.slot}.txt`, content: contents(original, spec.slot) }, constraints: {},
    });
    const manifest = { version: 1 as const, slots: group.map(spec => slots.get(spec)!) };
    accepted.set(original, { id: original, digest: sha256Bytes(`component-original:${original}`), nativeEntryId: `probe:native-Pi:carrier:${original}`,
      manifest, manifestDigest: manifestDigest(manifest) });
  }
  const intents = new Map<string, EffectIntent>();
  for (const spec of specs.filter(spec => spec.issued !== false)) {
    const input = accepted.get(spec.original)!;
    const slot = slots.get(spec)!;
    const prepared = await prepareFileEffect(owner.workspaceRoot, "write", slot.canonicalArguments as Record<string, unknown>);
    const keyTuple = [1, owner.orgId, owner.logicalSessionId, input.id, slot.id, slot.kind] as const;
    const payload = { encoding: "base64" as const, bytes: prepared.desiredBase64, digest: prepared.post.sha256! };
    const intent: EffectIntent = { format: "fabric.effect-intent", version: 1, key: mintEffectKey(keyTuple), keyTuple,
      manifestDigest: input.manifestDigest, acceptedInput: { id: input.id, digest: input.digest, nativeEntryId: input.nativeEntryId }, slot,
      tool: { canonicalRef: "pi.write", adapterVersion: "1", originalToolCallId: call(input.id, slot.id), nativeAssistantEntryId: `probe:native-Pi:assistant:${input.id}:${slot.id}` },
      canonicalArguments: slot.canonicalArguments, canonicalTarget: slot.canonicalTarget, payload,
      adapterData: prepared as unknown as JsonValue,
      file: { preExists: prepared.pre.exists, preHash: prepared.pre.sha256, preMode: prepared.pre.mode,
        postHash: prepared.post.sha256!, postMode: prepared.post.mode!, desiredBytes: payload, preparedResult: prepared.preparedResult as unknown as JsonValue },
      originAttempt: owner.attempt };
    intents.set(name(input.id, slot.id), intent);
    // Historical B4 fixture setup through its real validated store API. This is
    // not a claim that ordinary live dispatch admits a new intent past pending K.
    await store.withSession("new-intent", tx => tx.bind(intent));
    await store.withSession("settle", tx => {
      for (const alias of spec.aliases ?? []) tx.append(intent.key, { state: "ALIAS", toolCallId: alias, attempt: owner.attempt, at: "2026-01-01T00:00:00.000Z" });
      if (spec.terminal) tx.append(intent.key, { state: "RESULT", toolCallId: intent.tool.originalToolCallId, attempt: owner.attempt,
        at: "2026-01-01T00:00:00.000Z", recoveryGeneration: "historical-component-pin", outcome: "committed",
        result: { role: "toolResult", toolCallId: intent.tool.originalToolCallId, toolName: "write", isError: false,
          content: [{ type: "text", text: "retained component result" }], details: {} } });
    });
  }
  const sinkCalls: ScopedHarness["sinkCalls"] = [];
  const projections: string[] = [];
  let projectionFailure: string | undefined;
  const port = createCertifiedEffectRecordPort({ owner, store,
    // Only C/new uses live dispatch, with an explicit fixed call association.
    // No active-context identity or accepted original is remapped here.
    admissionForCall(request) {
      if (request.toolCallId !== call("C", "new")) throw new Error("Unknown component call");
      return { acceptedInput: accepted.get("C")!, nativeAssistantEntryId: "probe:native-Pi:assistant:C:new" };
    },
    async confirmNativeAssistant(request) {
      // Component acknowledgement only, not a native/B6 durability receipt.
      if (request.nativeAssistantEntryId !== "probe:native-Pi:assistant:C:new") throw new Error("Unknown component assistant");
      await owner.assertLive("new-intent");
    },
    assertManifestConstraints(selected, prepared) {
      expect(selected).toEqual(accepted.get("C")!.manifest.slots[0]);
      expect(prepared.workspaceRoot).toBe(owner.workspaceRoot);
      expect(prepared.path).toBe("C-new.txt");
    },
    async failpoint(point, details) {
      if (point === "destination-committed") {
        const key = String(details?.key);
        const record = [...intents.values()].find(intent => intent.key === key);
        if (record) {
          // This hook observes a REAL file commit. Its generation is read from
          // the durable ATTEMPT after lock release below, not copied from the
          // requested argument or invented as native/sink-context evidence.
          sinkCalls.push([record.acceptedInput.id, record.slot.id, "unread-attempt-generation"]);
        }
      }
    },
    async readNativeToolResult(effect) {
      projections.push(effect.toolCallId);
      if ([...intents.values()].some(intent => intent.key === effect.key && intent.acceptedInput.id === projectionFailure)) {
        throw new Error("probe:native-Pi: component projection unavailable");
      }
      return undefined; // Explicit adapter-original; no fabricated native join.
    },
  });
  const settle = async (original: string, generation: string) => {
    const start = sinkCalls.length;
    const result = await port.settleOriginalPendingIntents(owner.logicalSessionId, original, generation);
    for (const row of sinkCalls.slice(start)) {
      const intent = intents.get(name(row[0], row[1]))!;
      const record = await store.withSession("settle", tx => tx.read(intent.key));
      row[2] = [...record.transitions].reverse().find(transition => transition.state === "ATTEMPT")?.recoveryGeneration ?? "missing-attempt-generation";
    }
    return result;
  };
  const destination: ScopedHarness["destination"] = (original, slot) => {
    try { return readFileSync(join(owner.workspaceRoot, `${original}-${slot}.txt`), "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  };
  return {
    key(original, slot) { return intents.get(name(original, slot))!.key; }, settle,
    legacy(generation) { return port.settlePendingIntents(owner.logicalSessionId, generation); },
    async inventory(original) {
      if (original === undefined) return diskInventory(root);
      return [...intents.values()].filter(intent => intent.acceptedInput.id === original).map(intent => ({ key: intent.key,
        ledger: diskInventory(join(owner.sessionRoot, "effects", intent.key)),
        destination: destination(original, intent.slot.id) ?? null }));
    },
    async summaries() {
      return store.withSession("settle", tx => tx.list().map(record => ({ key: record.intent.key, original: record.intent.acceptedInput.id, slot: record.intent.slot.id,
        transitions: record.transitions.map(row => ({ state: row.state,
          ...(row.recoveryGeneration === undefined ? {} : { generation: row.recoveryGeneration }), ...(row.outcome === undefined ? {} : { outcome: row.outcome }) })) })));
    },
    destination, sinkCalls, projections,
    failProjection(original) { projectionFailure = original; },
    async attemptNew() {
      const slot = accepted.get("C")!.manifest.slots[0]!;
      const args = slot.canonicalArguments as Record<string, unknown>;
      await port.execute({ canonicalRef: "pi.write", adapterVersion: "1", kind: "file-write", canonicalTarget: slot.canonicalTarget,
        canonicalArguments: args, toolCallId: call("C", "new"), context: { cwd: owner.workspaceRoot } as ExtensionContext,
        prepare: () => prepareFileEffect(owner.workspaceRoot, "write", args) });
    },
  };
};

// Author once, bind twice. These cases must not acquire B5/Main-policy names or
// be cited as moved C21/C22, S3, B6/native qualification, or B5 conformance.
function scopedContract(label: string, factory: Factory) {
  describe(label, () => {
    it("A/gA settlement preserves every B byte, then B/gB legitimately settles", async () => {
      const h = await factory([{ original: "A", slot: "same" }, { original: "B", slot: "same" }]);
      const beforeB = await h.inventory("B");
      const a = await h.settle("A", gA);
      expect(a).toEqual({ uncertain: 0, settled: [{ key: h.key("A", "same"), toolCallId: call("A", "same"), outcome: "committed", resultBytesHash: expect.any(String) }] });
      expect(await h.inventory("B")).toEqual(beforeB);
      expect(h.destination("B", "same")).toBeUndefined();
      expect(h.projections).toEqual([call("A", "same")]);
      const afterA = await h.inventory("A");
      expect(await h.settle("B", gB)).toMatchObject({ uncertain: 0, settled: [{ key: h.key("B", "same"), outcome: "committed" }] });
      expect(await h.inventory("A")).toEqual(afterA);
      expect(h.key("A", "same")).not.toBe(h.key("B", "same"));
      expect(h.sinkCalls).toEqual([["A", "same", gA], ["B", "same", gB]]);
      for (const row of await h.summaries()) expect(row.transitions.filter(t => t.state !== "ALIAS").map(t => t.generation)).toEqual(row.original === "A" ? [gA, gA] : [gB, gB]);
      expect(h.destination("A", "same")).toBe(contents("A", "same"));
      expect(h.destination("B", "same")).toBe(contents("B", "same"));
    });

    it("identical slot IDs do not lend a terminal predecessor to another original", async () => {
      const h = await factory([{ original: "A", slot: "root", issued: false, ordinal: 9 },
        { original: "A", slot: "child", predecessors: ["root"], ordinal: 0 }, { original: "B", slot: "root", terminal: true }]);
      const b = await h.inventory("B");
      expect(await h.settle("A", gA)).toMatchObject({ uncertain: 1, settled: [{ key: h.key("A", "child"), outcome: "uncertain" }] });
      expect(h.sinkCalls).toEqual([]);
      expect(h.projections).toEqual([]);
      expect(h.destination("A", "child")).toBeUndefined();
      expect(await h.inventory("B")).toEqual(b);
      expect((await h.summaries()).find(row => row.original === "A")!.transitions).toEqual([{ state: "UNCERTAIN", generation: gA, outcome: "uncertain" }]);
    });

    it("out-of-ordinal dependencies traverse only the selected original namespace", async () => {
      const h = await factory(["A", "B"].flatMap(original => [
        { original, slot: "root", ordinal: 10 }, { original, slot: "child", ordinal: 0, predecessors: ["root"] },
      ]));
      const before = await h.inventory("B");
      expect((await h.settle("A", gA)).settled.map(row => row.key)).toEqual([h.key("A", "root"), h.key("A", "child")]);
      expect(h.sinkCalls).toEqual([["A", "root", gA], ["A", "child", gA]]);
      expect(await h.inventory("B")).toEqual(before);
      expect((await h.settle("B", gB)).settled.map(row => row.key)).toEqual([h.key("B", "root"), h.key("B", "child")]);
      expect(h.sinkCalls.slice(2)).toEqual([["B", "root", gB], ["B", "child", gB]]);
    });

    it("legacy ambiguity refuses before any append, sink or terminal-alias projection", async () => {
      const h = await factory([{ original: "H", slot: "history", ordinal: 0, terminal: true, aliases: ["history-alias"] },
        { original: "A", slot: "pending", ordinal: 1 }, { original: "B", slot: "pending", ordinal: 2 }]);
      const before = await h.inventory();
      await expect(h.legacy(gA)).rejects.toMatchObject({ code: "EFFECT_ORIGINAL_SCOPE_REQUIRED" });
      expect(await h.inventory()).toEqual(before);
      expect(h.sinkCalls).toEqual([]); expect(h.projections).toEqual([]);
      // The no-write refusal must not create a session-wide failure latch that
      // scoped A+B cannot discharge.
      await h.settle("A", gA); await h.settle("B", gB);
      await h.attemptNew();
      expect(h.destination("C", "new")).toBe(contents("C", "new"));
    });

    it("terminal aliases project idempotently without a new attempt or generation stamp", async () => {
      const h = await factory([{ original: "A", slot: "root", issued: false, ordinal: 9 },
        { original: "A", slot: "terminal", ordinal: 0, predecessors: ["root"], terminal: true, aliases: ["saved-alias"] },
        { original: "B", slot: "pending" }]);
      const before = await h.inventory();
      const result = await h.settle("A", gA);
      expect(result.uncertain).toBe(0);
      expect(result.settled.map(row => [row.toolCallId, row.outcome])).toEqual([[call("A", "terminal"), "committed"], ["saved-alias", "committed"]]);
      expect(await h.settle("A", "read-only-opaque-not-written")).toEqual(result);
      expect(await h.inventory()).toEqual(before);
      expect(h.sinkCalls).toEqual([]);
    });

    it("clean empty scope is a zero-settlement result, not a receipt or pin allocation", async () => {
      const h = await factory([]);
      const before = await h.inventory();
      expect(await h.settle("absent", gA)).toEqual({ settled: [], uncertain: 0 });
      expect(await h.legacy(gB)).toEqual({ settled: [], uncertain: 0 });
      expect(await h.inventory()).toEqual(before);
      expect(h.sinkCalls).toEqual([]); expect(h.projections).toEqual([]);
    });

    it("B4 pending-only history uses its retained input association, never an ambient current input", async () => {
      const h = await factory([{ original: "B", slot: "pending" }]);
      const before = await h.inventory();
      expect(await h.settle("not-B", gA)).toEqual({ settled: [], uncertain: 0 });
      expect(await h.inventory()).toEqual(before);
      expect(await h.settle("B", gB)).toMatchObject({ uncertain: 0, settled: [{ key: h.key("B", "pending"), outcome: "committed" }] });
      expect(h.sinkCalls).toEqual([["B", "pending", gB]]);
    });

    it("selected success does not open the session-wide new-intent gate over unrelated pending K", async () => {
      const h = await factory([{ original: "A", slot: "done" }, { original: "B", slot: "pending" }]);
      await h.settle("A", gA);
      const before = await h.inventory();
      await expect(h.attemptNew()).rejects.toMatchObject({ code: "EFFECT_PENDING_INTENT" });
      expect(await h.inventory()).toEqual(before);
      expect(h.destination("C", "new")).toBeUndefined();
      await h.settle("B", gB); await h.attemptNew();
      expect(h.destination("C", "new")).toBe(contents("C", "new"));
    });

    it("A success cannot discharge B's failed native projection even when all K are terminal", async () => {
      const h = await factory([{ original: "A", slot: "done", terminal: true }, { original: "B", slot: "done", terminal: true }]);
      const before = await h.inventory();
      h.failProjection("B");
      await expect(h.settle("B", gB)).rejects.toThrow("probe:native-Pi");
      await h.settle("A", gA);
      await expect(h.attemptNew()).rejects.toMatchObject({ code: "EFFECT_SETTLEMENT_REQUIRED" });
      expect(await h.inventory()).toEqual(before);
      h.failProjection(); await h.settle("B", gB); await h.attemptNew();
      expect(h.destination("C", "new")).toBe(contents("C", "new"));
    });
  });
}

scopedContract("COMPONENT ONLY — test-only FAKE B4 scoped effect-selection model", fakeFactory);
describe.skipIf(process.platform === "win32")("retained POSIX port binding", () => {
  scopedContract("COMPONENT — REAL B4 EffectStore + record port + file sink; probe:native-Pi only", realFactory);
});
