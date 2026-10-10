import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal } from "../src/agents/completion-journal.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = (metadata: Pick<ResidentHostConfig, "mainName" | "mainStartedAt">, mainName?: () => string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recipient-idle-"));
  roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const rootId = "session:recipient-idle";
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "recipient-idle", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(meshRoot, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(root, "unused-worker.js"), fabricExtensionPath: path.join(root, "unused-extension.js"),
    piBinary: "unused-pi", claudeBinary: "unused-claude", vedaBinary: "unused-veda", ...metadata,
  };
  const lastKnown = vi.fn(() => ({ participant: { name: "legacy lane", startedAt: 123 } as FabricParticipantInfo, lapsedMs: 1 }));
  const participants: FabricParticipantSource = {
    lastKnown, list: () => [], get: () => undefined, peers: () => [],
    self: () => { throw new Error("idle metadata lookup must not request self"); },
    refresh: async () => {}, scheduleRefresh: () => {},
  };
  const mainAgent = { id: rootId, local: true, deliverAgent: vi.fn() } as unknown as FabricMainAgentTarget;
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100, { readCacheMs: 2_000 });
  const recipient = vi.spyOn(CompletionJournal.prototype, "recipient", "get");
  // Drive native notifications explicitly: real FS callbacks must not race the fake clock.
  const watches: Array<{ dir: string; callback: (event: string, filename: string | null) => void; watcher: EventEmitter & { close: ReturnType<typeof vi.fn> } }> = [];
  vi.spyOn(fs, "watch").mockImplementation(((dir: fs.PathLike, callback: (event: string, filename: string | null) => void) => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() });
    watches.push({ dir: String(dir), callback, watcher });
    return watcher;
  }) as unknown as typeof fs.watch);
  const client = new ResidencyClient({ platform: "linux", config, mesh, participants, mainAgent, ...(mainName ? { mainName } : {}) });
  return { client, lastKnown, recipient, meshRoot, watches };
};

// A live-name journal derives its recipient even when pending() finds no results.
// That metadata must not invoke lastKnown's fresh full-fleet scan every idle poll.
describe("ResidencyClient completion recipient metadata", () => {
  it("keeps empty delivery scans idle through non-waking safety checks until a native event, and closes observation resources", async () => {
    vi.useFakeTimers();
    const { client, meshRoot, watches } = setup({ mainName: "fixed lane", mainStartedAt: 456 });
    const reads = vi.spyOn(client.options.mesh, "listAllShared");
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    const polls = () => reads.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/").length;
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      await drain.mock.results[0]!.value;
      expect(polls()).toBe(1);
      await vi.advanceTimersByTimeAsync(59_999);
      expect(polls()).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      await drain.mock.results.at(-1)!.value;
      expect(polls()).toBe(1);
      const native = watches.find(watch => watch.dir === meshRoot && !watch.watcher.close.mock.calls.length);
      expect(native).toBeDefined();
      // A generic state.json commit (heartbeat/lease) is not a delivery change: delivery
      // keys arrive only through their own residency notification, so no selection runs.
      native!.callback("change", "state.json");
      await vi.advanceTimersByTimeAsync(0);
      expect(polls()).toBe(1);
      expect(vi.getTimerCount()).toBe(0);
      // The residency notification directory appearing is a real native event: one
      // bounded namespace discovery.
      native!.callback("rename", "residency-notifications");
      await vi.advanceTimersByTimeAsync(0);
      expect(polls()).toBe(2);
      await client.close();
      expect(watches.length).toBeGreaterThan(0);
      expect(watches.every(watch => watch.watcher.close.mock.calls.length > 0)).toBe(true);
      native!.callback("change", "state.json");
      await vi.advanceTimersByTimeAsync(120_000);
      expect(polls()).toBe(2);
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); }
  });

  it("does not scan the directory during empty idle polls, and keeps live Main renames", async () => {
    vi.useFakeTimers();
    let name = "initial lane";
    const { client, lastKnown, recipient } = setup({ mainName: "startup lane", mainStartedAt: 456 }, () => name);
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(100);
      expect(recipient).toHaveBeenCalled();
      expect(lastKnown).not.toHaveBeenCalled();
      name = "renamed lane";
      expect(client.hasAgent("a".repeat(32))).toBe(false);
      expect(recipient.mock.results.at(-1)?.value).toMatchObject({ name: "renamed lane", startedAt: 456 });
      expect(lastKnown).not.toHaveBeenCalled();
    } finally { await client.close(); }
  });

  it("joins an in-flight empty journal scan with a stopped clock instead of polling a timer", async () => {
    const { client, lastKnown, meshRoot } = setup({ mainName: "fixed lane", mainStartedAt: 456 });
    fs.mkdirSync(path.join(meshRoot, "agent-completions"), { recursive: true });
    let entered!: () => void, release!: () => void;
    const reading = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(fs.promises, "realpath").mockImplementation(async target => {
      entered(); await held; return String(target);
    });
    vi.useFakeTimers();
    try {
      client.start(); await reading;
      const closing = client.close();
      release(); await closing;
      expect(lastKnown).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally { release(); await client.close(); }
  });

  it("does not scan for a fixed recipient when both config fields are supplied, including zero", async () => {
    const { client, lastKnown, recipient } = setup({ mainName: "fixed lane", mainStartedAt: 0 });
    try {
      expect(client.hasAgent("b".repeat(32))).toBe(false);
      expect(recipient.mock.results.at(-1)?.value).toMatchObject({ name: "fixed lane", startedAt: 0 });
      expect(lastKnown).not.toHaveBeenCalled();
    } finally { await client.close(); }
  });

  it.each([
    { metadata: {}, expected: { name: "legacy lane", startedAt: 123 } },
    { metadata: { mainName: "configured lane" }, expected: { name: "configured lane", startedAt: 123 } },
    { metadata: { mainStartedAt: 456 }, expected: { name: "legacy lane", startedAt: 456 } },
  ])("retains the fresh directory fallback for incomplete legacy metadata: $metadata", async ({ metadata, expected }) => {
    const { client, lastKnown, recipient } = setup(metadata);
    try {
      expect(client.hasAgent("c".repeat(32))).toBe(false);
      expect(lastKnown).toHaveBeenCalledWith("session:recipient-idle");
      expect(recipient.mock.results.at(-1)?.value).toMatchObject(expected);
    } finally { await client.close(); }
  });
});
