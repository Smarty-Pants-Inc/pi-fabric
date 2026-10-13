import { describe, expect, it } from "vitest";
import * as jev from "../src/jev.js";

describe("public Jev API", () => {
  it("exports Jev host APIs without concrete connector implementations", () => {
    expect(jev.JevClient).toBeTypeOf("function");
    expect(jev.JevDecisions).toBeTypeOf("function");
    expect(jev.normalizeDecisionProfiles).toBeTypeOf("function");
    expect(jev.decisionRequestSchema).toHaveProperty("properties.profile");
    expect(jev.JEV_ACTION_DESCRIPTORS.map(action => action.name)).toEqual(expect.arrayContaining(["decide", "resolveDecision", "decisionProviders", "models"]));
    const profiles: jev.DecisionProfiles = { version: 1, profiles: { safe: { provider: "openai" } } };
    expect(jev.normalizeDecisionProfiles(profiles)).toEqual(profiles);
    expect(jev.JevProvider).toBeTypeOf("function");
    expect(jev.JevProgramManager).toBeTypeOf("function");
    expect(jev.JevObservationHost).toBeTypeOf("function");
    expect(Object.keys(jev).filter(name => /browser|harness/i.test(name))).toEqual([]);
  });
});
