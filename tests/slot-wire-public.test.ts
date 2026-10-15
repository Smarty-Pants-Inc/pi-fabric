// Keep: trust boundary (#7991). The public schema/normalizer must not strip or
// coerce the exact claim revision before native admission checks it.
import { describe, expect, it } from "vitest";
import type { FabricActorCreateRequest } from "../src/actors/types.js";
import { normalizeActorSlotClaim } from "../src/actors/slot-claim.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { actionArgNormalizer } from "../src/providers/arg-normalization.js";

const claim = { key: "factory/review-slots/v1/example/repo/1/base", ifVersion: 7 };
const normalize = actionArgNormalizer(() => AGENTS_ACTION_DESCRIPTORS);

describe("slot claim public contract", () => {
  it.each(["create", "createActor"])("%s preserves the exact claim and retry identity", action => {
    const request: FabricActorCreateRequest = {
      name: "review", instructions: "Review.", topics: ["pull.example.repo"],
      slotClaim: claim, idempotencyKey: "claim-7-review",
    };
    const normalized = normalize(action, request as unknown as Record<string, unknown>);
    expect(normalized.slotClaim).toEqual(claim);
    expect(normalized.idempotencyKey).toBe("claim-7-review");
    expect(normalizeActorSlotClaim(normalized.slotClaim)).toEqual(claim);
    const descriptor = AGENTS_ACTION_DESCRIPTORS.find(entry => entry.name === action)!;
    const schema = descriptor.inputSchema as { properties: Record<string, unknown> };
    expect(schema.properties.slotClaim).toMatchObject({
      type: "object", required: ["key", "ifVersion"], additionalProperties: false,
      properties: { ifVersion: { type: "integer", minimum: 1, maximum: Number.MAX_SAFE_INTEGER } },
    });
  });

  it("copies valid input without changing the key or revision", () => {
    expect(normalizeActorSlotClaim(undefined)).toBeUndefined();
    const result = normalizeActorSlotClaim(claim);
    expect(result).toEqual(claim);
    expect(result).not.toBe(claim);
    expect(normalizeActorSlotClaim({ ...claim, ifVersion: Number.MAX_SAFE_INTEGER })?.ifVersion)
      .toBe(Number.MAX_SAFE_INTEGER);
  });

  it.each([null, [], "token", {}, { key: "", ifVersion: 1 },
    { key: claim.key, ifVersion: "7" }, { key: claim.key, ifVersion: 0 },
    { key: claim.key, ifVersion: -1 }, { key: claim.key, ifVersion: 1.5 },
    { key: claim.key, ifVersion: Number.MAX_SAFE_INTEGER + 1 },
    { key: claim.key, ifVersion: Infinity }, { key: claim.key, ifVersion: NaN },
    { ...claim, claimedBy: "forged-main" }, { ...claim, authorityGeneration: "forged" },
  ])("rejects invalid/authority-bearing input %#", value => {
    expect(() => normalizeActorSlotClaim(value)).toThrow(/slotClaim/);
    try { normalizeActorSlotClaim(value); } catch (error) {
      expect(error).toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "invalid" });
    }
  });

  it("import accepts a per-materialization token, not template authority", () => {
    const normalized = normalize("import", { id: "template", as: "review", slotClaim: claim,
      idempotencyKey: "claim-7-review" });
    expect(normalized).toMatchObject({ slotClaim: claim, idempotencyKey: "claim-7-review" });
  });

  it("exposes only a read-only slotStatus target selection", () => {
    const action = AGENTS_ACTION_DESCRIPTORS.find(entry => entry.name === "slotStatus")!;
    expect(action.risk).toBe("read");
    expect(action.inputSchema).toMatchObject({ type: "object", additionalProperties: false,
      properties: { residency: { enum: ["session", "durable"] } } });
    expect(Object.keys((action.inputSchema as { properties: object }).properties)).toEqual(["residency"]);
    expect(normalize("slotStatus", { residency: "durable" })).toEqual({ residency: "durable" });
  });
});
