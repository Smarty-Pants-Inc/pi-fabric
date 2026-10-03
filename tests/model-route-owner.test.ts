import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_JEV_CONFIG } from "../src/jev/config.js";

const probe = vi.hoisted(() => ({ loads: vi.fn(), evaluate: vi.fn(), drain: vi.fn(async () => {}) }));
vi.mock("../src/jev/client.js", () => {
  probe.loads();
  return { JevClient: class { evaluate = probe.evaluate; drainCredentials = probe.drain; } };
});
beforeEach(() => { vi.resetModules(); probe.loads.mockClear(); probe.evaluate.mockReset(); probe.drain.mockClear(); });
const request = { state: {}, questions: {} };
const policy = () => ({ jev: { ...DEFAULT_JEV_CONFIG }, networkAllowed: true, schemaEnforced: false });

describe("shadow resident Jev owner lifecycle", () => {
  it("does not import the optional client at cold import, registration, idle or unused close", async () => {
    const { ShadowRouteOwner } = await import("../src/agents/model-route-owner.js");
    const owner = new ShadowRouteOwner(policy);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(probe.loads).not.toHaveBeenCalled();
    await owner.close();
    expect(probe.loads).not.toHaveBeenCalled();
    expect(probe.evaluate).not.toHaveBeenCalled();
  });
  it.each(["disabled", "network", "schema", "old-snapshot"])("refuses %s before optional client load", async blocked => {
    const { ShadowRouteOwner } = await import("../src/agents/model-route-owner.js");
    const gates = policy();
    if (blocked === "disabled") gates.jev.enabled = false;
    if (blocked === "network") gates.networkAllowed = false;
    if (blocked === "schema") gates.schemaEnforced = true;
    const owner = new ShadowRouteOwner(() => blocked === "old-snapshot" ? undefined : gates);
    expect(() => owner.evaluate(request, new AbortController().signal)).toThrow("Jev routing unavailable");
    expect(probe.loads).not.toHaveBeenCalled();
    await owner.close();
  });
  it("loads on first eligible use once and rechecks policy on every activation", async () => {
    const { ShadowRouteOwner } = await import("../src/agents/model-route-owner.js");
    const gates = policy();
    const owner = new ShadowRouteOwner(() => gates);
    probe.evaluate.mockResolvedValue({ answers: {} });
    await owner.evaluate(request, new AbortController().signal);
    await owner.evaluate(request, new AbortController().signal);
    expect(probe.loads).toHaveBeenCalledTimes(1);
    expect(probe.evaluate).toHaveBeenCalledTimes(2);
    gates.jev.enabled = false;
    expect(() => owner.evaluate(request, new AbortController().signal)).toThrow("Jev routing unavailable");
    expect(probe.evaluate).toHaveBeenCalledTimes(2);
    await owner.close();
    expect(probe.drain).toHaveBeenCalledTimes(1);
  });
  it("revokes in-flight evaluation at retirement and joins credential drain", async () => {
    const { ShadowRouteOwner } = await import("../src/agents/model-route-owner.js");
    let entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    probe.evaluate.mockImplementation((_request, signal: AbortSignal) => new Promise((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(new Error("retired")), { once: true }); entered();
    }));
    const owner = new ShadowRouteOwner(policy);
    const pending = owner.evaluate(request, new AbortController().signal).catch(error => error);
    await ready;
    await owner.close();
    expect(await pending).toMatchObject({ message: "retired" });
    expect(probe.drain).toHaveBeenCalledTimes(1);
    expect(() => owner.evaluate(request, new AbortController().signal)).toThrow("Jev routing unavailable");
  });
});
