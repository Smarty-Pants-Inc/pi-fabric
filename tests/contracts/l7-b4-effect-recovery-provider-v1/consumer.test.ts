import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { AcceptedRecoveryInput, B4EffectRecoveryProviderV1 } from "./contract.js";
import { createFake, createFakeProvider } from "./fake.js";
import { accepted, GENERATION, INPUT_LIFETIME_MS, intent, type FixtureIdentity } from "./fixtures.js";

const ownInput: AcceptedRecoveryInput = {
  sessionId: "s4-consumer-session", originalInputId: "s4-original", admissionId: "s4-admission",
  deliveryId: "s4-admission", kind: "recovery", text: "consumer text λ",
  payloadSha256: createHash("sha256").update("consumer text λ").digest("hex"),
  deadlineMs: Date.now() + INPUT_LIFETIME_MS, consumption: { kind: "effect-read", effectKey: "consumer-effect" },
};
const identity: FixtureIdentity = { acceptedInput: ownInput, executionBranchEntryId: "s4-anchor" };
const options = { sessionId: ownInput.sessionId, ...identity, joinedLeafEntryId: "s4-joined", pin: "s4-retained-generation" };

describe("B4 fake consumer configuration / COMPONENT ONLY", () => {
  it("P1 consumer identities complete settle/read/repair/resolve without a Partial cast", async () => {
    const value = intent("consumer-write", 0, [], false, identity);
    const h = await createFake({ intents: [value] }, options);
    try {
      const provider: B4EffectRecoveryProviderV1 = h.provider;
      const settled = await provider.settlePendingIntents(ownInput.sessionId, options.pin);
      expect(settled.uncertain).toBe(0);
      const read = await provider.readToolResult(settled.settled[0]!);
      expect(read.message.toolCallId).toBe("call-consumer-write");
      expect(read.postHook).toBe(true);
      const ref = await provider.repairMaintenance(ownInput, "s4-joined");
      const view = provider.resolveRepair(ref);
      expect(view).not.toBeInstanceOf(Promise);
      expect(ref).toMatchObject({ originalInputId: "s4-original", executionBranchEntryId: "s4-anchor", recoveryGeneration: "s4-retained-generation" });
      expect(view).toMatchObject({ sessionId: "s4-consumer-session", originalInputId: "s4-original",
        executionBranchEntryId: "s4-anchor", joinedLeafEntryId: "s4-joined", recoveryGeneration: "s4-retained-generation" });
      const sha = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
      expect(view.joinedHistorySha256).toBe(sha(JSON.stringify(h.joinedBranch().map(entry => [entry.id, sha(JSON.stringify(entry) + "\n")]))));
      expect(view.nativeEntries[0]?.entryId).toBe("s4-joined");
      expect(view.protectedEntryIds).toContain("s4-joined");
      await h.reopen(); expect(await provider.repairMaintenance(ownInput, "s4-joined")).toEqual(ref);
      h.advanceJoinedLeaf("consumer-later-leaf");
      await expect(provider.repairMaintenance(ownInput, "consumer-later-leaf")).rejects.toMatchObject({ code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" });
      expect(provider.resolveRepair(ref).joinedLeafEntryId).toBe("s4-joined");
    } finally { await h.dispose(); }
  });

  it("P2 full-provider accessor accepts a consumer deadline rather than module constants", async () => {
    // A clock beyond the retired 2026 fixture deadline still produces live input.
    const now = () => Date.UTC(2031, 0, 1);
    const clockHarness = await createFake({}, { now });
    const clockInput = clockHarness.recoveryIdentity().acceptedInput;
    expect(clockInput.deadlineMs).toBe(now() + INPUT_LIFETIME_MS);
    expect(clockInput.deliveryId).toBe(clockInput.admissionId);
    await clockHarness.dispose();
    const input = { ...accepted, deadlineMs: accepted.deadlineMs + 1234 };
    const value = intent("deadline-write", 0, [], false, { acceptedInput: input, executionBranchEntryId: "A" });
    const provider: B4EffectRecoveryProviderV1 = await createFakeProvider({ intents: [value] }, { acceptedInput: input });
    await provider.settlePendingIntents(input.sessionId, GENERATION);
    const ref = await provider.repairMaintenance(input, "J");
    expect(provider.resolveRepair(ref).originalInputId).toBe(input.originalInputId);
    await expect(provider.repairMaintenance(accepted, "J")).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
  });

  it("P3 separate consumers cannot resolve each other's receipts or use each other's session", async () => {
    const left = await createFakeProvider({ intents: [intent("left", 0, [], false, identity)] }, options);
    const rightInput = { ...ownInput, sessionId: "another-session", originalInputId: "another-original" };
    const rightIdentity = { ...identity, acceptedInput: rightInput };
    const right = await createFakeProvider({ intents: [intent("right", 0, [], false, rightIdentity)] },
      { ...options, sessionId: rightInput.sessionId, acceptedInput: rightInput });
    await left.settlePendingIntents(ownInput.sessionId, options.pin);
    await right.settlePendingIntents(rightInput.sessionId, options.pin);
    const ref = await left.repairMaintenance(ownInput, "s4-joined");
    await right.repairMaintenance(rightInput, "s4-joined");
    expect(() => right.resolveRepair(ref)).toThrowError(expect.objectContaining({ code: "RECOVERY_MAINTENANCE_REPAIR_REQUIRED" }));
    await expect(right.settlePendingIntents(ownInput.sessionId, options.pin)).rejects.toMatchObject({ code: "EFFECT_SESSION_MISMATCH" });
  });

  it("P4 B4 does not secretly enforce the consumer B5 fixture pin", async () => {
    const value = intent("consumer-write", 0, [], false, identity);
    const h = await createFake({ intents: [value] }, options);
    try {
      // A Main test must enforce the pin before calling B4. This probe proves
      // the stand-in cannot accidentally make that missing caller check GREEN.
      expect((await h.provider.settlePendingIntents(ownInput.sessionId, "valid-but-not-the-B5-pin")).uncertain).toBe(0);
      expect(h.generations()).toEqual(["valid-but-not-the-B5-pin"]);
    } finally { await h.dispose(); }
  });
});
