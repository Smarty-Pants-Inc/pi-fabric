import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore, ActorRegistryUpdateVetoedError, type ActorRegistryMutation } from "../src/actors/registry-store.js";

// Recorded-call probe: run the real bounded update protocol, but never execute file
// publication/disposal or lock cleanup (#4664). Hosted registry tests cover real disk custody.
afterEach(() => vi.restoreAllMocks());
const fixture = (valid = true, vetoReason: "generation" | "payload-generation" = "generation") => {
  let now = 0;
  const store = new ActorRegistryStore(path.join(os.tmpdir(), "unused-veto-probe"), { now: () => now });
  vi.spyOn(store, "snapshot").mockReturnValue({ generation: "initial", bytes: undefined, actors: [] });
  const commit = vi.fn(), dispose = vi.fn(), barrier = vi.fn(async () => {});
  vi.spyOn(store, "prepare").mockReturnValue({ valid: () => valid, vetoReason: () => vetoReason, commit, dispose, barrier });
  vi.spyOn(store, "withLock").mockImplementation(async operation => operation());
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  return { store, commit, dispose, warning, expire: () => { now = 5_000; } };
};

describe("registry veto reason at expiry (smarty-dev#11998)", () => {
  it.each([false, "cancelled", "revision", "ownership", "actor-state"] as const)("logs caller validation %s once, without committing", async reason => {
    const f = fixture();
    const validate = vi.fn(() => { f.expire(); return reason; });
    const result = f.store.update(() => ({ actors: [], value: "must not publish", validate }), "manager-update");
    await expect(result).rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    expect(f.commit).not.toHaveBeenCalled();
    expect(f.dispose).toHaveBeenCalledOnce();
    expect(f.warning.mock.calls).toEqual([[`[pi-fabric] actor registry update vetoed: operation=manager-update; validations=${reason === false ? "caller-validation" : reason}; budgetMs=5000`]]);
  });

  it.each(["generation", "payload-generation"] as const)("logs the prepared %s validation rather than blaming ownership", async reason => {
    const f = fixture(false, reason);
    const select = () => ({ actors: [], value: "must not publish" });
    // Let the first attempt acquire custody, then consume the budget at validation.
    vi.mocked(f.store.prepare).mockImplementation(() => ({
      valid: () => { f.expire(); return false; }, vetoReason: () => reason,
      commit: f.commit, dispose: f.dispose, barrier: vi.fn(async () => {}),
    }));
    await expect(f.store.update(select)).rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    expect(f.commit).not.toHaveBeenCalled();
    expect(f.warning.mock.calls).toEqual([[`[pi-fabric] actor registry update vetoed: operation=update; validations=${reason}; budgetMs=5000`]]);
  });

  it("logs accumulated fixed reasons once across repeated vetoes, never caller text", async () => {
    const f = fixture();
    let attempts = 0;
    const validate = (() => {
      attempts++;
      if (attempts === 3) { f.expire(); return "not-a-fixed-reason\nprivate caller text"; }
      return attempts === 1 ? "ownership" : "revision";
    }) as NonNullable<ActorRegistryMutation<string>["validate"]>;
    await expect(f.store.update(() => ({ actors: [], value: "must not publish", validate })))
      .rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    expect(attempts).toBe(3);
    expect(f.commit).not.toHaveBeenCalled();
    expect(f.warning.mock.calls).toEqual([["[pi-fabric] actor registry update vetoed: operation=update; validations=ownership,revision,caller-validation; budgetMs=5000"]]);
  });

  it("does not log or reject a temporary veto that clears on the fresh selection", async () => {
    const f = fixture();
    let selections = 0;
    const select = (): ActorRegistryMutation<string> => ({ actors: [], value: "committed", validate: () => ++selections > 2 });
    await expect(f.store.update(select)).resolves.toBe("committed");
    expect(selections).toBe(3);
    expect(f.commit).toHaveBeenCalledOnce();
    expect(f.warning).not.toHaveBeenCalled();
  });
});
