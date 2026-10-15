import fs from "node:fs";
import os from "node:os";
import { ActorManager, ActorPreparationError, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { AgentManager } from "../src/agents/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
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
  // Recorded-call trust-boundary probe: real manager admission/catch/status; no child
  // process or deletion. Registry publication is mocked by fixture(), mesh writes recorded.
  it("manager exposes safe veto reason, closed alarm code, cooldown and successful retry", async () => {
    vi.spyOn(fs, "rmSync").mockImplementation(() => {});
    vi.spyOn(fs, "unlinkSync").mockImplementation(() => {});
    vi.spyOn(fs, "rmdirSync").mockImplementation(() => {});
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "reason-manager-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    vi.spyOn(mesh, "put").mockResolvedValue(undefined as never);
    vi.spyOn(mesh, "delete").mockResolvedValue(undefined as never);
    const publish = vi.spyOn(mesh, "publish").mockImplementation(async input => ({ ...input, id: "event", createdAt: Date.now() }) as never);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
    let veto = true;
    const run = vi.spyOn(agents, "run").mockImplementation(async () => {
      if (veto) {
        const f = fixture();
        await f.store.update(() => ({ actors: [], value: "never", validate: () => { f.expire(); return "ownership"; } }))
          .catch(error => { throw new ActorPreparationError("private-id", "registry", error); });
      }
      return { id: "probe", status: "completed", text: "ok", toolCalls: 0, warnings: [] } as never;
    });
    const manager = new ActorManager("reason", { id: "session:private", name: "main", kind: "main" }, mesh,
      DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, { actorRoot: path.join(root, "actors"),
        persistent: false, transientBlockCooldownMs: 2_000, meshRetentionSweepPath: false });
    const realNow = Date.now;
    let skew = 0;
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    try {
      const actor = await manager.create({ name: "private-name", instructions: "Respond.", responseMode: "text" });
      for (let i = 0; i < 3; i++) {
        await expect(manager.ask(actor.id, "activate")).rejects.toThrow("Actor registry update was vetoed");
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      const blocked = manager.status(actor.id).activationBlocked!;
      expect(blocked).toMatchObject({ reason: "actor registry validation veto (ownership)",
        code: "registry-update-vetoed", class: "transient", classification: expect.stringContaining("(ownership)"), backoffMs: 2_000 });
      expect(blocked.reason).not.toContain(actor.id);
      expect(publish.mock.calls.some(([input]) => input.topic === FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC &&
        (input.data as { code?: string }).code === "registry-update-vetoed")).toBe(true);
      skew = blocked.retryAt! - realNow() - 1;
      expect(manager.status(actor.id).activationBlocked).toBeDefined();
      skew += 2;
      expect(manager.status(actor.id).activationBlockExpired?.code).toBe("registry-update-vetoed");
      veto = false;
      await manager.ask(actor.id, "probe");
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(run).toHaveBeenCalledTimes(4);
      expect(manager.status(actor.id).activationBlocked).toBeUndefined();
      run.mockRejectedValueOnce(new Error("401 Unauthorized: invalid api key"));
      await expect(manager.ask(actor.id, "hard failure")).rejects.toThrow("401");
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(manager.status(actor.id).activationBlocked).toMatchObject({ code: "auth", class: "hard" });
      expect(manager.status(actor.id).activationBlocked).not.toHaveProperty("retryAt");
      skew += 86_400_000;
      expect(manager.status(actor.id).activationBlocked?.code).toBe("auth");
    } finally {
      await manager.close();
      await agents.close();
    }
  });

  it.each([false, "cancelled", "revision", "ownership", "actor-state"] as const)("logs caller validation %s once, without committing", async reason => {
    const f = fixture();
    const validate = vi.fn(() => { f.expire(); return reason; });
    const result = f.store.update(() => ({ actors: [], value: "must not publish", validate }), "manager-update");
    await expect(result).rejects.toMatchObject({ code: "FABRIC_ACTOR_REGISTRY_UPDATE_VETOED", reason: reason === false ? "caller-validation" : reason });
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

  it.each(["ownership", "revision", "actor-state", "cancelled"] as const)("preserves typed-thrown validation reason %s on both paths", async reason => {
    const f = fixture();
    let calls = 0;
    await expect(f.store.update(() => ({ actors: [], value: "never", validate: () => {
      if (++calls === 3) f.expire();
      throw new ActorRegistryUpdateVetoedError(reason);
    } }))).rejects.toMatchObject({ reason });
    expect(calls).toBe(3);
    expect(f.commit).not.toHaveBeenCalled();
  });

  it.each([new Error("private actor identity"), "ownership", { reason: "ownership", retryable: true }])("does not convert an arbitrary validation throw into a safe veto: %j", async error => {
    const f = fixture();
    await expect(f.store.update(() => ({ actors: [], value: "never", validate: () => { throw error; } }))).rejects.toBe(error);
    expect(f.commit).not.toHaveBeenCalled();
    expect(f.warning).not.toHaveBeenCalled();
  });

  it("sanitizes untyped constructor and returned identity strings", async () => {
    const identity = "actor:private /secret/path" as "ownership";
    expect(new ActorRegistryUpdateVetoedError(identity).reason).toBe("caller-validation");
    const f = fixture();
    await expect(f.store.update(() => ({ actors: [], value: "never", validate: () => { f.expire(); return identity; } })))
      .rejects.toMatchObject({ reason: "caller-validation" });
    expect(JSON.stringify(f.warning.mock.calls)).not.toContain("private");
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
