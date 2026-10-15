import { describe, expect, it } from "vitest";
import { activationBlockClass, classifyActivationFailure } from "../src/actors/activation-failure.js";
import { ActorRegistryUpdateVetoedError } from "../src/actors/registry-store.js";

// Keep: smarty-dev#11998's real catch. Registry vetoes removed review/security actors
// from routing permanently; near matches and permanent causes must still fail closed.
const veto = new ActorRegistryUpdateVetoedError().message;

describe("registry veto activation classification (smarty-dev#11998)", () => {
  it.each([
    veto,
    `Error: ${veto}`,
    `ActorRegistryUpdateVetoedError: ${veto}`,
    `Actor abc-123 preparation (registry): ${veto}`,
    `ActorPreparationError: Actor abc-123 preparation (registry): ${veto}`,
  ])("recognizes the exact boundary error only in persisted legacy mode: %s", lastError => {
    for (const preparation of [false, true]) {
      const live = classifyActivationFailure({ lastError, preparation });
      expect(live.transient).toBe(false);
      expect(activationBlockClass(live.code)).toBe("hard");
      const result = classifyActivationFailure({ lastError, preparation, persistedRegistryVeto: "legacy" });
      expect(result).toMatchObject({ code: "registry-update-vetoed", transient: true });
      expect(activationBlockClass(result.code)).toBe("transient");
    }
  });

  it.each([
    ["Actor private-id preparation (registry): " + veto, "caller-validation"],
    ["actor registry validation veto (ownership)", "ownership"],
  ])("normalizes a stored known veto reason: %s", (lastError, reason) => {
    const result = classifyActivationFailure({ lastError, persistedRegistryVeto: "known-code" });
    expect(result).toMatchObject({ code: "registry-update-vetoed", transient: true,
      reason: `actor registry validation veto (${reason})` });
    expect(JSON.stringify(result)).not.toContain("private-id");
  });

  it.each(["401 Unauthorized", "security policy violation"])("keeps independent stored permanent evidence hard: %s", error => {
    expect(classifyActivationFailure({ lastError: veto, error, persistedRegistryVeto: "known-code" }).transient).toBe(false);
    expect(classifyActivationFailure({ lastError: veto, error, persistedRegistryVeto: "legacy" }).transient).toBe(false);
  });

  it("accepts a typed veto cause independently of its wrapper text", () => {
    expect(classifyActivationFailure({ lastError: "Activation preparation failed", preparation: true, registryVeto: true }))
      .toMatchObject({ code: "registry-update-vetoed", transient: true });
  });

  it.each(["ownership", "revision", "cancelled", "actor-state"] as const)("records the concrete safe reason %s without wrapper identity", registryVeto => {
    const result = classifyActivationFailure({ lastError: "Actor private-id preparation (registry): failure", registryVeto });
    expect(result).toMatchObject({ code: "registry-update-vetoed", transient: true,
      reason: `actor registry validation veto (${registryVeto})` });
    expect(result.classification).toContain(`(${registryVeto})`);
    expect(result.reason).not.toContain("private-id");
  });

  it("does not include an untyped identity in a safe reason", () => {
    const result = classifyActivationFailure({ lastError: veto, registryVeto: "actor:private /secret" as "ownership" });
    expect(result.reason).toBe("actor registry validation veto (caller-validation)");
  });

  it.each([
    `unrelated failure mentioning ${veto}`,
    `${veto}; something else happened`,
    `${veto} (output: quoted model prose)`,
    `${veto}\nError: another unknown failure`,
    "Actor registry update was vetoed",
    "FABRIC_ACTOR_REGISTRY_UPDATE_VETOED",
    `Directive reply missing (output: ${veto})`,
    `Directive reply missing (final text: ${veto})`,
  ])("does not infer a veto from a mention: %s", lastError => {
    const result = classifyActivationFailure({ lastError });
    expect(result.transient).toBe(false);
    expect(activationBlockClass(result.code)).toBe("hard");
  });

  it.each([
    { lastError: "Error: 401 Unauthorized", registryVeto: "ownership" as const },
    { lastError: veto, registryVeto: "revision" as const, evidence: ["security policy violation"] },
    { lastError: veto, error: `Error: 401 Unauthorized\n${veto}` },
    { lastError: veto, evidence: ["Error: 403 Forbidden"] },
    { lastError: veto, evidence: ["security policy violation"] },
    { lastError: veto, evidence: ["insufficient_quota"] },
    { lastError: veto, evidence: ["Error: 400 bad request"] },
    { lastError: `Actor abc preparation (registry): 401 Unauthorized: ${veto}`, preparation: true, registryVeto: true },
  ])("keeps permanent evidence hard: %j", input => {
    const result = classifyActivationFailure(input);
    expect(result.transient).toBe(false);
    expect(activationBlockClass(result.code)).toBe("hard");
  });
});
