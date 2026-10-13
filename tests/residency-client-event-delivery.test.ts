import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { MeshLockTimeoutError } from "../src/mesh.js";
import { CompletionJournal } from "../src/agents/completion-journal.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const realSetTimeout = globalThis.setTimeout; // captured before any fake timers
const realSleep = (ms: number) => new Promise<void>((resolve) => realSetTimeout(resolve, ms));
const until = async (check: () => boolean, ms = 3_000): Promise<void> => {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() > end) throw new Error("condition not reached"); await realSleep(10); }
};

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-client-events-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:events", sessionId: "events", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(path.join(config.meshRoot, "agent-completions"), { recursive: true });
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
    mainAgent: { local: true } as FabricMainAgentTarget });
  const read = vi.spyOn(mesh, "listAll");
  const drains = () => read.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/").length;
  // A journal envelope lands by atomic rename (one notification), as a resident completion does.
  const landCompletion = (name: string) => {
    const temporary = path.join(root, `${name}.tmp`);
    fs.writeFileSync(temporary, "{}");
    fs.renameSync(temporary, path.join(config.meshRoot, "agent-completions", `${name}.json`));
  };
  return { root, config, mesh, client, read, drains, landCompletion,
    cleanup: () => fs.rmSync(root, { recursive: true, force: true }) };
};

describe("resident client event-driven delivery", () => {
  it("idle: one startup catch-up drain, then no timers and no recurring drains", async () => {
    const { client, drains, cleanup } = fixture();
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(drains()).toBe(1);
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(drains()).toBe(1);
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("a new journal completion (external resident writer) drains once", async () => {
    const { client, drains, landCompletion, cleanup } = fixture();
    try {
      client.start();
      await until(() => drains() === 1);
      await realSleep(50);
      landCompletion("external");
      await until(() => drains() === 2);
      await realSleep(200);
      expect(drains()).toBe(2);
    } finally { await client.close(); cleanup(); }
  });

  it("a peer mesh write wakes delivery and the wake converges", async () => {
    const { client, mesh, drains, cleanup } = fixture();
    try {
      client.start();
      await until(() => drains() === 1);
      await realSleep(50);
      await mesh.put({ key: "peer/unrelated", value: { at: 1 }, identity: { id: "peer", name: "peer", kind: "agent" } });
      await until(() => drains() >= 2);
      await realSleep(300);
      const settled = drains();
      expect(settled).toBeLessThanOrEqual(4);
      await realSleep(300);
      expect(drains()).toBe(settled);
    } finally { await client.close(); cleanup(); }
  });

  it("failed delivery retries once on its own backoff without another event", async () => {
    const { client, read, drains, cleanup } = fixture();
    read.mockImplementationOnce(() => { throw new MeshLockTimeoutError("fixture outage", 1, 100); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(drains()).toBe(1);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(drains()).toBe(2);
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(drains()).toBe(2);
    } finally { await client.close(); vi.useRealTimers(); warn.mockRestore(); cleanup(); }
  });

  it("an event during a slow in-flight drain is not lost", async () => {
    const { client, drains, landCompletion, cleanup } = fixture();
    let release!: () => void;
    const original = CompletionJournal.prototype.drain;
    const drain = vi.spyOn(CompletionJournal.prototype, "drain").mockImplementationOnce(function (this: CompletionJournal) {
      return new Promise<void>((resolve) => { release = resolve; });
    }).mockImplementation(function (this: CompletionJournal, deliver?: boolean) { return original.call(this, deliver); });
    try {
      client.start();
      await until(() => drain.mock.calls.length === 1);
      landCompletion("during-drain");
      await realSleep(200);
      expect(drains()).toBe(1); // coalesced behind the in-flight pass
      release();
      await until(() => drains() === 2);
      await realSleep(200);
      expect(drains()).toBe(2);
    } finally { await client.close(); drain.mockRestore(); cleanup(); }
  });

  it("watcher error rescans, warns once, and re-arms on a bounded one-shot", async () => {
    const { client, drains, cleanup } = fixture();
    const watchers: fs.FSWatcher[] = [];
    const realWatch = fs.watch;
    const watch = vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      const watcher = realWatch(...args);
      watchers.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      await realSleep(100); // let the startup pass's real file I/O settle
      expect(drains()).toBe(1);
      const armed = watch.mock.calls.length;
      expect(armed).toBeGreaterThan(0);
      watchers[0]!.emit("error", new Error("fixture watcher failure"));
      await vi.advanceTimersByTimeAsync(0);
      await realSleep(100);
      expect(drains()).toBe(2); // rescan
      expect(warn).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(1); // one re-arm, not a poll
      await vi.advanceTimersByTimeAsync(1_000);
      await realSleep(100);
      expect(watch.mock.calls.length).toBe(armed * 2);
      expect(drains()).toBe(3); // post-rearm rescan
      expect(vi.getTimerCount()).toBe(0);
      watchers.at(-1)!.emit("error", new Error("fixture watcher failure"));
      await vi.advanceTimersByTimeAsync(0);
      expect(warn).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(1);
      await client.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); watch.mockRestore(); warn.mockRestore(); cleanup(); }
  });
});
