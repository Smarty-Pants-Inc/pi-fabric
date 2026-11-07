import { describe, expect, it } from "vitest";
import { canonicalResidentWakeConfig, residentWakeConfigMatches } from "../src/residency/wake-index.js";

describe("whole restart-config canonical equality", () => {
  it("ignores object ordering at every depth, but preserves array ordering", () => {
    const config = { actorRoot: "/actors", mesh: { actorScope: "project", grants: ["read", "write"] }, optional: undefined };
    const reordered = { mesh: { grants: ["read", "write"], actorScope: "project" }, actorRoot: "/actors" };
    expect(canonicalResidentWakeConfig(config)).toBe(canonicalResidentWakeConfig(reordered));
    expect(residentWakeConfigMatches(config, reordered)).toBe(true);
    expect(residentWakeConfigMatches(config, { ...reordered, mesh: { ...reordered.mesh, grants: ["write", "read"] } })).toBe(false);
  });
  it.each(["actorRoot", "sessionActorRoot", "mesh", "role", "piBinary", "futureAuthority"])("includes the entire %s field without an allowlist", field => {
    const config = { actorRoot: "/actors", sessionActorRoot: "/sessions", mesh: { actorScope: "project" }, role: "worker", piBinary: "/pi", futureAuthority: { grants: ["read"] } };
    expect(residentWakeConfigMatches(config, { ...config, [field]: "different" })).toBe(false);
    const removed: Record<string, unknown> = { ...config };
    delete removed[field];
    expect(residentWakeConfigMatches(config, removed)).toBe(false);
  });
  it("fails closed on missing, non-object, and non-JSON config", () => {
    const circular: Record<string, unknown> = {}; circular.self = circular;
    for (const value of [undefined, null, [], "config", circular]) expect(residentWakeConfigMatches(value, {})).toBe(false);
  });
});
