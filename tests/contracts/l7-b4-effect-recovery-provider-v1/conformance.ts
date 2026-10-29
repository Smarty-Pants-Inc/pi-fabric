import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { canonicalEncode } from "../../../src/effects/slots.js";
import type { B4EffectRecoveryProviderV1 } from "./contract.js";
import { accepted, copy, GENERATION, hash, intent, originalResult, SESSION, type ConformanceFactory, type ConformanceHarness, type Scenario } from "./fixtures.js";

/** ONE suite for consumer stand-ins and the actual B4 adapter. A factory gives
 * each case an isolated retained-state fixture; it is not a provider API.
 * Keep reason: data safety/contract boundary; no result is L7 qualification. */
export function runConformance(provider: ConformanceFactory): void {
  const test = (name: string, scenario: Scenario, check: (h: ConformanceHarness) => Promise<void>) => {
    it(name, async () => { const h = await provider(scenario); try { await check(h); } finally { await h.dispose(); } });
  };
  const settle = (h: ConformanceHarness, generation = GENERATION) => h.provider.settlePendingIntents!(SESSION, generation);
  const read = (h: ConformanceHarness, effect: Parameters<B4EffectRecoveryProviderV1["readToolResult"]>[0]) => h.provider.readToolResult!(effect);
  const repair = (h: ConformanceHarness) => {
    expect(h.provider.repairMaintenance, "real host repairMaintenance is required; no fake shim").toBeTypeOf("function");
    return h.provider.repairMaintenance!(accepted, "J");
  };
  const value = intent();
  describe("B4EffectRecoveryProviderV1", () => {
    test("C01 empty/clean returns empty without minting a generation or receipt", {}, async h => {
      const before = await h.ledgerDigest();
      expect(await settle(h)).toEqual({ settled: [], uncertain: 0 });
      expect(h.generations()).toEqual([]); expect(h.destination()).toEqual([]);
      expect(await h.ledgerDigest()).toBe(before);
    });
    test("C02 wrong session refuses before settlement", { intents: [value] }, async h => {
      await expect(h.provider.settlePendingIntents!("foreign", GENERATION)).rejects.toMatchObject({ code: "EFFECT_SESSION_MISMATCH" });
      expect(h.destination()).toEqual([]);
    });
    test("C03 empty generation is not replaced with a random UUID", { intents: [value] }, async h => {
      await expect(settle(h, " ")).rejects.toMatchObject({ code: "EFFECT_RECOVERY_GENERATION" });
      expect(h.destination()).toEqual([]); expect(h.generations()).toEqual([]);
    });
    test("C04 settle uses original K/bytes and caller's durable generation", { intents: [value] }, async h => {
      const result = await settle(h);
      expect(result.uncertain).toBe(0); expect(result.settled).toHaveLength(1);
      expect(result.settled[0]).toMatchObject({ key: value.key, toolCallId: value.tool.originalToolCallId, outcome: "committed" });
      expect(h.observedIntents()).toEqual([value]); expect(h.generations()).toEqual([GENERATION]);
    });
    test("C05 terminal result survives reopen with no second sink effect or ledger write", { intents: [value] }, async h => {
      const first = await settle(h); const before = await h.ledgerDigest();
      await h.reopen(); expect(await settle(h)).toEqual(first);
      expect(await h.ledgerDigest()).toBe(before); expect(h.observedIntents()).toHaveLength(1);
      expect(h.destination()).toEqual([value.key]);
    });
    test("C06 same K with changed bytes conflicts without overwrite", { intents: [value] }, async h => {
      const before = await h.ledgerDigest(); const changed = copy(value);
      const bytes = "changed";
      await expect(h.issue({ ...changed, payload: { encoding: "base64", bytes: Buffer.from(bytes).toString("base64"), digest: hash(bytes) } }))
        .rejects.toMatchObject({ code: "EFFECT_SLOT_CONFLICT" });
      expect(await h.ledgerDigest()).toBe(before); expect(h.destination()).toEqual([]);
    });
    const predecessor = intent("predecessor", 9);
    const successor = intent("successor", 0, ["predecessor"]);
    test("C07 predecessors settle before lower-ordinal successors", { intents: [successor, predecessor] }, async h => {
      expect((await settle(h)).uncertain).toBe(0);
      expect(h.observedIntents().map(item => item.slot.id)).toEqual(["predecessor", "successor"]);
    });
    test("C08 missing predecessor holds successor without issuing it", { intents: [successor] }, async h => {
      expect((await settle(h)).uncertain).toBe(1); expect(h.destination()).toEqual([]); expect(h.observedIntents()).toEqual([]);
    });
    test("C09 uncertain predecessor never runs its successor", { intents: [predecessor, successor], outcomes: { predecessor: "uncertain" } }, async h => {
      expect((await settle(h)).uncertain).toBe(2); expect(h.destination()).toEqual([]);
    });
    test("C10 definitive not-committed is retained, never silently retried", { intents: [value], outcomes: { write: "not-committed" } }, async h => {
      expect((await settle(h)).settled[0]?.outcome).toBe("not-committed");
      const count = h.observedIntents().length; await h.reopen(); await settle(h);
      expect(h.observedIntents()).toHaveLength(count); expect(h.destination()).toEqual([]);
    });
    test("C11 lost sink ACK reconciles original key once on the destination", { intents: [value], lostAck: true }, async h => {
      expect((await settle(h)).uncertain).toBe(1); expect(h.destination()).toEqual([value.key]);
      await h.reopen(); expect((await settle(h)).uncertain).toBe(0);
      expect(h.destination()).toEqual([value.key]); expect(h.observedIntents()).toEqual([value, value]);
    });
    test("C12 controlled peer probe retains original body/deadline on replay", { intents: [intent("peer", 0, [], true)], committedBeforeAck: [intent("peer", 0, [], true).key] }, async h => {
      const peer = intent("peer", 0, [], true); await settle(h); await h.reopen(); await settle(h);
      // Only a controlled sink boundary, not a real late network request or cache-expiry proof.
      expect(h.destination()).toEqual([peer.key]); expect(h.observedIntents()).toEqual([peer]);
      expect(h.observedIntents()[0]?.message?.originalDeadlineAt).toBe(peer.message!.originalDeadlineAt);
    });
    test("C13 concurrent settlement serializes across an awaited stalled sink", { intents: [value], stallSink: true }, async h => {
      const first = settle(h);
      let second: ReturnType<typeof settle> | undefined;
      try {
        // An event, not a sleep: first holds the list/attempt/sink/result critical section.
        await Promise.race([h.sinkGate.entered, first.then(() => { throw new Error("settlement bypassed stalled sink"); })]);
        second = settle(h);
        // Let a lock-free fake enter its second sink before releasing the first.
        await new Promise<void>(resolve => setImmediate(resolve));
        expect(h.observedIntents()).toHaveLength(1);
      } finally {
        h.sinkGate.release();
        await Promise.allSettled([first, ...(second ? [second] : [])]);
      }
      expect(second).toBeDefined();
      const [a, b] = await Promise.all([first, second!]);
      expect(a).toEqual(b); expect(h.observedIntents()).toHaveLength(1);
      expect(h.destination()).toEqual([value.key]);
    });
    test("C14 corrupt retained ledger cannot appear clean", { intents: [value], corrupt: true }, async h => {
      expect((await settle(h)).uncertain).toBeGreaterThan(0); expect(h.destination()).toEqual([]);
    });
    test("C15 uncertainty counts logical keys, not physical aliases", { intents: [value], outcomes: { write: "uncertain" }, alias: { key: value.key, toolCallId: "alias" } }, async h => {
      const result = await settle(h); expect(result.uncertain).toBe(1); expect(result.settled).toHaveLength(2);
    });
    test("C16 result hash is full native JSON.stringify bytes, not canonical sorted JSON", { intents: [value] }, async h => {
      const effect = (await settle(h)).settled[0]!; const result = await read(h, effect);
      expect(JSON.stringify(result.message)).toBe(JSON.stringify(originalResult(value)));
      expect(effect.resultBytesHash).toBe(hash(JSON.stringify(originalResult(value))));
    });
    test("C17 native result has explicit postHook=true provenance", { intents: [value] }, async h => {
      const effect = (await settle(h)).settled[0]!;
      expect(await read(h, effect)).toMatchObject({ source: "native-entry", postHook: true, message: { timestamp: 12345 } });
    });
    test("C18 physical aliases have their own exact result hash and identity", { intents: [value], alias: { key: value.key, toolCallId: "alias" } }, async h => {
      const effects = (await settle(h)).settled; expect(effects).toHaveLength(2);
      for (const effect of effects) {
        const result = await read(h, effect);
        expect(result.message.toolCallId).toBe(effect.toolCallId);
        expect(effect.resultBytesHash).toBe(hash(JSON.stringify(result.message)));
      }
      expect(effects[0]!.resultBytesHash).not.toBe(effects[1]!.resultBytesHash);
    });
    test("C19 forged alias cannot read an original result", { intents: [value] }, async h => {
      const effect = (await settle(h)).settled[0]!;
      await expect(read(h, { ...effect, toolCallId: "forged" })).rejects.toMatchObject({ code: "EFFECT_CALL_ALIAS_MISMATCH" });
    });
    test("C20 forged result hash cannot read a result", { intents: [value] }, async h => {
      const effect = (await settle(h)).settled[0]!;
      await expect(read(h, { ...effect, resultBytesHash: hash("forged") })).rejects.toMatchObject({ code: "EFFECT_RESULT_UNAVAILABLE" });
    });
    // C21/C22 moved to ../l7-b5-main-generation-v1/conformance.ts.
    // The B4 ABI has no B5 pin port; B5/Main owns these refusals (R3:150-152).
    test("C23 J differs from A: host repair returns bound view and canonical receipt hash", { intents: [value] }, async h => {
      await settle(h); const ref = await repair(h);
      expect(ref).toMatchObject({ originalInputId: accepted.originalInputId, recoveryGeneration: GENERATION, executionBranchEntryId: "A" });
      expect(h.provider.resolveRepair).toBeTypeOf("function"); const view = h.provider.resolveRepair!(ref);
      expect(view).not.toBeInstanceOf(Promise);
      expect(view).toMatchObject({ sessionId: SESSION, joinedLeafEntryId: "J", executionBranchEntryId: "A", recoveryGeneration: GENERATION });
      expect(view.protectedEntryIds).not.toHaveLength(0); expect(view.nativeEntries).not.toHaveLength(0);
      expect(view.settlements).toHaveLength(1);
      // Compute from the observed branch, not joinedEvidence or the receipt itself.
      const sha256 = (text: string) => createHash("sha256").update(text, "utf8").digest("hex");
      const branch = h.joinedBranch();
      const exactHistory = JSON.stringify(branch.map(entry => [entry.id, sha256(JSON.stringify(entry) + "\n")]));
      expect(branch.at(-1)?.id).toBe(view.joinedLeafEntryId);
      expect(view.joinedHistorySha256).toBe(sha256(exactHistory));
      expect(view.joinedHistorySha256).not.toBe(sha256(JSON.stringify(branch.map(entry => [entry.id, sha256(JSON.stringify(entry))]))));
      expect(view.joinedHistorySha256).not.toBe(sha256(JSON.stringify(branch.map(entry => [entry.id, sha256(canonicalEncode(entry) + "\n")]))));
      expect(view).not.toHaveProperty("viewSha256");
      expect(view.protectedRangeSha256).toMatch(/^[a-f0-9]{64}$/);
      expect(ref.repairReceiptSha256).toBe(hash(canonicalEncode(view)));
    });
    test("C24 second recovery resolves identical immutable receipt without reminting", { intents: [value] }, async h => {
      await settle(h); const ref = await repair(h); const view = h.provider.resolveRepair!(ref);
      await h.reopen(); expect(await repair(h)).toEqual(ref); expect(h.provider.resolveRepair!(ref)).toEqual(view);
      expect(Object.isFrozen(view)).toBe(true); expect(Object.isFrozen(view.nativeEntries)).toBe(true);
    });
    test("C25 forged receipt hash/anchor/generation is refused synchronously", { intents: [value] }, async h => {
      await settle(h); const ref = await repair(h);
      for (const bad of [{ ...ref, repairReceiptSha256: hash("forged") }, { ...ref, executionBranchEntryId: "J" }, { ...ref, recoveryGeneration: "g2" }]) {
        expect(() => h.provider.resolveRepair!(bad)).toThrowError(expect.objectContaining({ code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" }));
      }
    });
    test("C26 later joined J2 refuses new repair rather than replacing receipt", { intents: [value] }, async h => {
      await settle(h); const ref = await repair(h); h.advanceJoinedLeaf();
      await expect(h.provider.repairMaintenance!(accepted, "J2")).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" });
      expect(h.provider.resolveRepair!(ref).joinedLeafEntryId).toBe("J");
    });
    test("C27 uncertain effects cannot yield a repair receipt", { intents: [value], outcomes: { write: "uncertain" } }, async h => {
      await settle(h); expect(h.provider.repairMaintenance).toBeTypeOf("function");
      await expect(h.provider.repairMaintenance!(accepted, "J")).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" });
    });
    test("C28 file pre-state commits original bytes once; reopen preserves inode", { fileState: "pre" }, async h => {
      expect(h.fileSnapshot().bytes).toBe("before"); expect((await settle(h)).uncertain).toBe(0);
      const post = h.fileSnapshot(); expect(post.bytes).toBe("after");
      await h.reopen(); await settle(h); expect(h.fileSnapshot()).toEqual(post);
    });
    test("C29 file post-state lost ACK returns result without replacing inode", { fileState: "post" }, async h => {
      const before = h.fileSnapshot(); expect((await settle(h)).uncertain).toBe(0);
      expect(h.fileSnapshot()).toEqual(before);
    });
    test("C30 file neither pre nor post holds without mutation", { fileState: "neither" }, async h => {
      const before = h.fileSnapshot(); expect((await settle(h)).uncertain).toBe(1);
      expect(h.fileSnapshot()).toEqual(before);
    });
    test("C31 fixed read/hash shell settles without mutating checked file", { shellState: "matching" }, async h => {
      const before = h.fileSnapshot(); expect((await settle(h)).uncertain).toBe(0);
      expect(h.fileSnapshot()).toEqual(before);
    });
    test("C32 changed shell input holds, never labels a new observation original", { shellState: "changed" }, async h => {
      const before = h.fileSnapshot(); expect((await settle(h)).uncertain).toBe(1);
      expect(h.fileSnapshot()).toEqual(before);
    });
    test("C33 missing receipt cannot be resolved from caller JSON", {}, async h => {
      expect(h.provider.resolveRepair).toBeTypeOf("function");
      expect(() => h.provider.resolveRepair!({ originalInputId: accepted.originalInputId, recoveryGeneration: GENERATION,
        executionBranchEntryId: "A", repairReceiptSha256: hash("invented") })).toThrowError(expect.objectContaining({ code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" }));
    });
    test("C34 host repair rejects unrelated original input", { intents: [value] }, async h => {
      await settle(h); expect(h.provider.repairMaintenance).toBeTypeOf("function");
      await expect(h.provider.repairMaintenance!({ ...accepted, originalInputId: "unrelated" }, "J")).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
    });
    test("C35 adapter-original read labels provenance without inventing a timestamp", { intents: [value], resultSource: "adapter-original" }, async h => {
      const effect = (await settle(h)).settled[0]!;
      const result = await read(h, effect);
      expect(result.source).toBe("adapter-original");
      expect(result.message).toEqual({ role: "toolResult", toolCallId: value.tool.originalToolCallId, toolName: "write",
        content: [{ type: "text", text: "original result" }], isError: false });
      expect(result.message).not.toHaveProperty("timestamp");
      // B3 requires a boolean, and re-runs strict hooks regardless of its value.
      expect(result.postHook).toBeTypeOf("boolean");
    });
  });
}
