import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore, ActorRegistryUpdateVetoedError } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";

// pi-fabric#577 / smarty-dev#816: an ActorRegistryUpdateVetoedError from a
// background registry save escaped as an unhandled rejection and killed the
// resident host (the RC3.1.3.1 canary rollback). Vitest also fails on unhandled
// rejections; the explicit listener names the leak.
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
let unhandled: unknown[] = [];
const onUnhandled = (reason: unknown): void => { unhandled.push(reason); };
beforeEach(() => {
  unhandled = [];
  process.on("unhandledRejection", onUnhandled);
  vi.spyOn(console, "warn").mockImplementation(() => {});
});
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  process.off("unhandledRejection", onUnhandled);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "registry-veto-bg-")); roots.push(dir); return dir; };
const settleTicks = () => new Promise(resolve => setTimeout(resolve, 50));
const until = async (check: () => boolean, ms = 10_000): Promise<void> => {
  const end = Date.now() + ms;
  while (!check()) {
    if (Date.now() > end) throw new Error("condition not reached");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const registryFiles = (dir: string): string[] => fs.readdirSync(dir, { recursive: true, encoding: "utf8" })
  .filter(file => path.basename(file) === "actors.json").map(file => path.join(dir, file));
const committedRow = (dir: string, id: string): Record<string, unknown> | undefined => {
  for (const file of registryFiles(dir)) {
    const row = new ActorRegistryStore(path.dirname(file)).records().find(record => record.id === id);
    if (row) return row;
  }
  return undefined;
};
const realUpdate = ActorRegistryStore.prototype.update;
/** Veto the next `count` registry updates, then let the real store commit. */
const vetoNext = (count: number) => {
  const real = realUpdate;
  let vetoes = 0;
  const spy = vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(function (this: ActorRegistryStore, ...args) {
    if (vetoes < count) { vetoes++; return Promise.reject(new ActorRegistryUpdateVetoedError()); }
    return real.apply(this, args as Parameters<typeof real>);
  });
  return { spy, vetoes: () => vetoes };
};

const manager = (dir: string) => {
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
  const actors = new ActorManager("veto", { id: "session:veto", name: "main", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, { actorRoot: path.join(dir, "actors"), persistent: true });
  closers.push(() => agents.close(), () => actors.close());
  return actors;
};

describe("pi-fabric#577 a vetoed background registry save never crashes the host", () => {
  it("retries a background save vetoed 3 times with backoff until it commits, with no unhandled rejection", async () => {
    const dir = tempRoot();
    const actors = manager(dir);
    const actor = await actors.create({ name: "veto", instructions: "Reply" });
    // 1 foreground veto (the awaited setter rejects) + 3 background vetoes, then success.
    const veto = vetoNext(4);
    await expect(actors.setNice(actor.id, 7)).rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    await until(() => committedRow(dir, actor.id)?.nice === 7);
    expect(veto.vetoes()).toBe(4);
    expect(veto.spy.mock.calls.length).toBeGreaterThanOrEqual(5);
    // Logged once per outage plus one recovery line, not once per attempt.
    const warnings = vi.mocked(console.warn).mock.calls.map(call => String(call[0]));
    expect(warnings.filter(line => line.includes("actor registry save failed"))).toHaveLength(1);
    expect(warnings.filter(line => line.includes("committed after 4 failed attempt"))).toHaveLength(1);
    await settleTicks();
    expect(unhandled).toEqual([]);
  }, 20_000);

  it("a close whose final save is vetoed retries it, resolves and commits", async () => {
    const dir = tempRoot();
    const actors = manager(dir);
    const actor = await actors.create({ name: "veto-close", instructions: "Reply" });
    await actors.setNice(actor.id, 3);
    const veto = vetoNext(3);
    await expect(actors.close()).resolves.toBeUndefined();
    expect(veto.vetoes()).toBe(3);
    expect(committedRow(dir, actor.id)?.nice).toBe(3);
    await settleTicks();
    expect(unhandled).toEqual([]);
  }, 20_000);

  const residentHost = async (dir: string) => {
    const hostConfig: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:resident", sessionId: "resident", cwd: process.cwd(), projectRoot: process.cwd(),
      meshRoot: path.join(dir, "mesh"), actorRoot: path.join(dir, "actors"), residencyRoot: path.join(dir, "resident"), fullCodeMode: false,
      agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda", piModels: { available: [], aliases: {} },
    };
    const host = new ResidentHost(hostConfig, () => {});
    closers.push(() => host.close());
    await host.start();
    const actor = await host.actors.create({ name: "resident-veto", instructions: "Reply", residency: "durable" });
    return { host, actor };
  };

  it("a resident host whose registry save is vetoed N times stays alive and commits once the veto clears", async () => {
    const dir = tempRoot();
    const { host, actor } = await residentHost(dir);
    const veto = vetoNext(5);
    await expect(host.actors.setNice(actor.id, 9)).rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    // The host keeps serving while the background save retries, and the save commits once the veto clears.
    await until(() => committedRow(dir, actor.id)?.nice === 9);
    expect(veto.vetoes()).toBe(5);
    expect(host.actors.status(actor.id).nice).toBe(9);
    // Shutdown with a vetoed final save neither rejects nor leaks a rejection.
    const closing = vetoNext(2);
    await expect(host.close()).resolves.toBeUndefined();
    expect(closing.vetoes()).toBe(2);
    await settleTicks();
    expect(unhandled).toEqual([]);
  }, 30_000);

  it("a resident host shutting down while every final save is vetoed leaks no rejection (the RC3.1.3.1 crash)", async () => {
    const dir = tempRoot();
    const { host } = await residentHost(dir);
    // Before the fix ActorManager.close() rejected while host.close() was still
    // awaiting participant quiesce (slow under registry contention), and nothing
    // observed that promise yet: an unhandled rejection, exit 1.
    const quiesce = host.participants.quiesce.bind(host.participants);
    vi.spyOn(host.participants, "quiesce").mockImplementation(async () => {
      await new Promise(resolve => setTimeout(resolve, 300));
      return quiesce();
    });
    const veto = vetoNext(3);
    await expect(host.close()).resolves.toBeUndefined();
    expect(veto.vetoes()).toBe(3);
    await settleTicks();
    expect(unhandled).toEqual([]);
  }, 30_000);
});
