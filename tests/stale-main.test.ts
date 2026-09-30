import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const actorManagers: ActorManager[] = [];
afterEach(async () => {
  await Promise.all(actorManagers.splice(0).map(manager => manager.close()));
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true }));
  vi.restoreAllMocks();
});

const setup = (loaded: string, active = "active", extra: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stale-main-"));
  roots.push(root);
  const base = path.join(root, "fabric");
  const releases = path.join(base, "releases");
  const sequence = ["B65", "B66", "B68", "after-critical", "active", "next"];
  for (const [index, release] of sequence.entries()) {
    const directory = path.join(releases, release);
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({ name: "pi-fabric" }));
    fs.writeFileSync(path.join(base, `${release}.receipt.json`), JSON.stringify({
      commit: release, installedAt: new Date(Date.UTC(2026, 8, 30, index)).toISOString(),
    }));
  }
  fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({
    releases: { B66: { safetyCritical: true }, B68: { safetyCritical: true } },
  }));
  const settingsPath = path.join(root, "settings.json");
  const activate = (name: string) => fs.writeFileSync(settingsPath, JSON.stringify({
    packages: [{ source: path.join(releases, name) }],
  }));
  activate(active);
  const publishStaleMain = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 2 }, {
    runRoot: path.join(root, "runs"),
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.join(releases, loaded, "dist/index.js"),
    fullCodeMode: false,
    mainAgentId: randomUUID(),
    releaseSettingsPath: settingsPath,
    publishStaleMain,
    ...extra,
  });
  managers.push(manager);
  return { manager, publishStaleMain, activate, base, releases, settingsPath };
};

const notice = "This Main runs after-critical; the fleet runs active; it self-reloads at its next safe settle";

describe("stale Main task admission", () => {
  it.each(["spawn", "run"] as const)("refuses %s from B65 across B66 before any worker launch", async method => {
    const { manager, publishStaleMain } = setup("B65");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(manager[method]({ task: "Do not launch", transport: "process" }))
      .rejects.toThrow(/safetyCritical.*B66.*B68.*\/fabric-release-reload/);
    expect(launch).not.toHaveBeenCalled();
    expect(publishStaleMain).toHaveBeenCalledTimes(1);
  });

  it("refuses actor task activations through the same boundary", async () => {
    const { manager } = setup("B65");
    await expect(manager.run({ task: "Actor activation", actorId: "actor-test", actorName: "Test", transport: "process" }))
      .rejects.toThrow(/safetyCritical/);
  });

  it.each(["B65", "after-critical"])("real actor ask on %s obeys admission and preserves its notice", async loaded => {
    const { manager, base } = setup(loaded);
    const actors = new ActorManager("stale-session", {
      id: "main:stale-session", name: "Main", kind: "main", sessionId: "stale-session",
    }, new MeshStore(path.join(base, "mesh"), 64 * 1024, 100), {
      ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20,
    }, manager, () => {}, { actorRoot: path.join(base, "actors") });
    actorManagers.push(actors);
    const actor = await actors.create({ name: "Guarded actor", instructions: "Reply", responseMode: "text" });
    const result = actors.ask(actor.id, "Test activation");
    if (loaded === "B65") await expect(result).rejects.toThrow(/safetyCritical/);
    else expect(await result).toHaveProperty("notice", notice);
  });
  it("adds a notice, not a refusal, after the last critical release; reports once per active", async () => {
    const { manager, publishStaleMain, activate } = setup("after-critical");
    const handle = await manager.spawn({ task: "Allowed task", transport: "process" });
    expect(handle).toHaveProperty("notice", notice);
    const result = await manager.wait(handle.id);
    expect(result).toHaveProperty("notice", notice);
    expect(manager.status(handle.id)).toHaveProperty("notice", notice);
    expect(await manager.run({ task: "Allowed again", transport: "process" })).toHaveProperty("notice", notice);
    expect(publishStaleMain).toHaveBeenCalledTimes(1);
    activate("next");
    expect(await manager.run({ task: "New activation", transport: "process" }))
      .toHaveProperty("notice", expect.stringContaining("fleet runs next"));
    expect(publishStaleMain).toHaveBeenCalledTimes(2);
  });

  it("rechecks a safety activation while awaited model preparation is in flight", async () => {
    let enter!: () => void;
    let resume!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; });
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const { manager, activate, base } = setup("after-critical", "active", {
      preparePiModel: async () => { enter(); await gate; },
    });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const spawning = manager.spawn({ task: "Queued at activation", transport: "process" });
    const refused = expect(spawning).rejects.toThrow(/safetyCritical.*next/);
    await entered;
    fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({ releases: { next: { safetyCritical: true } } }));
    activate("next");
    resume();
    await refused;
    expect(launch).not.toHaveBeenCalled();
    expect(manager.runningCount()).toBe(0);
  });
  it("loaded equals active: no notice or stale event", async () => {
    const { manager, publishStaleMain } = setup("active");
    const handle = await manager.spawn({ task: "Current task", transport: "process" });
    expect(handle).not.toHaveProperty("notice");
    expect(await manager.wait(handle.id)).not.toHaveProperty("notice");
    expect(publishStaleMain).not.toHaveBeenCalled();
  });

  it("the loaded release is exclusive (B68 itself is allowed after the last critical release)", async () => {
    const { manager } = setup("B68");
    expect(await manager.run({ task: "Already has B68", transport: "process" }))
      .toHaveProperty("notice", expect.stringContaining("Main runs B68"));
  });

  it("the active release is inclusive even if intermediate release trees were pruned", async () => {
    const { manager, releases } = setup("B65", "B66");
    fs.rmSync(path.join(releases, "B68"), { recursive: true });
    await expect(manager.spawn({ task: "Stop at B66", transport: "process" }))
      .rejects.toThrow(/safetyCritical.*B66/);
  });
});
