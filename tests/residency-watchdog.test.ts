import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { kernelFenceAvailable } from "../src/residency/file-lock.js";

vi.mock("../src/residency/file-lock.js", async (original) => ({
  ...await original<typeof import("../src/residency/file-lock.js")>(), kernelFenceAvailable: vi.fn(() => true),
}));
afterEach(() => vi.mocked(kernelFenceAvailable).mockReturnValue(true));
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { MeshLockTimeoutError } from "../src/mesh.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, type ResidentDeliveryRecord, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:watchdog", sessionId: "watchdog", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
    mainAgent: { local: true } as FabricMainAgentTarget });
  return { root, config, client };
};

describe("resident watchdog", () => {
  it("F6 pending completion outbox alone owns automatic recovery with unchanged-work backoff", async () => {
    const { root, config, client } = fixture();
    const outbox = path.join(config.residencyRoot, "delivery-outbox");
    fs.mkdirSync(outbox, { recursive: true });
    const record: ResidentDeliveryRecord = { format: RESIDENT_HOST_FORMAT, id: "completion-envelope", rootId: config.rootId,
      agentCompletionId: "a".repeat(32), from: { id: "a".repeat(32), name: "sole completed task", kind: "agent" },
      message: "completed", delivery: "followUp", triggerTurn: true, createdAt: Date.now() };
    const file = path.join(outbox, `${record.id}.json`);
    fs.writeFileSync(file, JSON.stringify(record));
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(50);
      expect(start).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(5_000);
      expect(start).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(start).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(start).toHaveBeenCalledTimes(3);
      fs.rmSync(file);
      await vi.advanceTimersByTimeAsync(120_000);
      expect(start).toHaveBeenCalledTimes(3);
    } finally { await client.close(); vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("ignores malformed, mismatched and temporary outbox files as recovery work", async () => {
    const { root, config, client } = fixture();
    const outbox = path.join(config.residencyRoot, "delivery-outbox");
    fs.mkdirSync(outbox, { recursive: true });
    const valid = { format: RESIDENT_HOST_FORMAT, id: "pending", rootId: config.rootId,
      from: { id: "a", kind: "agent" }, message: "completed", delivery: "followUp", triggerTurn: true };
    for (const [file, content] of Object.entries({
      "invalid.json": "{", "null.json": "null", "array.json": "[]",
      "mismatch.json": JSON.stringify(valid), "pending.tmp": JSON.stringify(valid),
      "format.json": JSON.stringify({ ...valid, id: "format", format: 999 }),
      "message.json": JSON.stringify({ ...valid, id: "message", message: null }),
    })) fs.writeFileSync(path.join(outbox, file), content);
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start(); await vi.advanceTimersByTimeAsync(180_000);
      expect(start).not.toHaveBeenCalled();
    } finally { await client.close(); vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("keeps watchdog restart independent of mesh delivery outage backoff", async () => {
    const { root, config, client } = fixture();
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { id: "a", rootId: config.rootId, residency: "durable", status: "idle" },
    ] }));
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    const read = vi.spyOn(client.options.mesh, "listAll").mockImplementation(() => {
      throw new MeshLockTimeoutError("fixture delivery outage", 1, 100);
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(50);
      expect(start).toHaveBeenCalledOnce();
      expect(read).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(250);
      expect(read).toHaveBeenCalledTimes(3);
      expect(warn).toHaveBeenCalledOnce();
      read.mockReturnValue([]);
      await vi.advanceTimersByTimeAsync(400);
      // Successor completion reconciliation also scans claims after a successful delivery pass.
      // Count delivery polls, not that separate scan, to keep the backoff assertion unchanged.
      expect(read.mock.calls.filter(([prefix]) => prefix?.startsWith("residency/deliveries/"))).toHaveLength(4);
      expect(warn).toHaveBeenCalledOnce();
      await client.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      await client.close();
      read.mockRestore(); start.mockRestore(); warn.mockRestore();
      vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it.each(["actor", "outbox"])("F4 never relaunches durable %s work without a kernel fence", async (kind) => {
    const { root, config, client } = fixture();
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { id: "a", rootId: config.rootId, residency: "durable", status: "idle" },
    ] }));
    if (kind === "outbox") {
      fs.rmSync(path.join(config.actorRoot, "actors.json"));
      const outbox = path.join(config.residencyRoot, "delivery-outbox");
      fs.mkdirSync(outbox, { recursive: true });
      fs.writeFileSync(path.join(outbox, "pending.json"), JSON.stringify({ format: RESIDENT_HOST_FORMAT, id: "pending",
        rootId: config.rootId, from: { id: "a", kind: "agent" }, message: "completed", delivery: "followUp", triggerTurn: true }));
    }
    vi.mocked(kernelFenceAvailable).mockReturnValue(false);
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start(); await vi.advanceTimersByTimeAsync(180_000);
      expect(start).not.toHaveBeenCalled();
    } finally { await client.close(); vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["actor", "session actor"])("restarts for this root's durable %s and backs failures off to 60 s", async (kind) => {
    const { root, config, client } = fixture();
    const directory = kind === "actor" ? config.actorRoot : config.sessionActorRoot!;
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "actors.json"), JSON.stringify({ actors: [
      { rootId: config.rootId, residency: "durable", status: "idle" },
    ] }));
    const start = vi.spyOn(client, "ensureHost").mockRejectedValue(new Error("fixture start failure"));
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(50);
      expect(start).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(9_950);
      expect(start).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(50);
      expect(start).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(20_000);
      expect(start).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(40_000);
      expect(start).toHaveBeenCalledTimes(4);
      await vi.advanceTimersByTimeAsync(59_950);
      expect(start).toHaveBeenCalledTimes(4);
      await vi.advanceTimersByTimeAsync(50);
      expect(start).toHaveBeenCalledTimes(5);
    } finally {
      await client.close();
      vi.useRealTimers();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("R2 nonterminal durable agent metadata alone never launches a host", async () => {
    const { root, config, client } = fixture();
    const directory = path.join(config.residencyRoot, "agents");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "a.json"), JSON.stringify({ rootId: config.rootId, id: "a",
      handle: { status: "running", transport: "process", sessionId: "-1" }, runDirectory: path.join(root, "run") }));
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start(); await vi.advanceTimersByTimeAsync(180_000);
      expect(start).not.toHaveBeenCalled();
    } finally { await client.close(); vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("P2-2 backs off successful idle exits with unchanged durable work", async () => {
    const { root, config, client } = fixture();
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { id: "a", rootId: config.rootId, residency: "durable", status: "idle" },
    ] }));
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(5_050);
      expect(start).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(start).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(start).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(19_950);
      expect(start).toHaveBeenCalledTimes(3);
    } finally { await client.close(); vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does not launch for another root's actors or terminal agents", async () => {
    const { root, config, client } = fixture();
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { rootId: "session:other", residency: "durable", status: "idle" },
    ] }));
    const directory = path.join(config.residencyRoot, "agents");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "a.json"), JSON.stringify({ rootId: config.rootId,
      id: "a", handle: { status: "completed" }, runDirectory: path.join(root, "run") }));
    const start = vi.spyOn(client, "ensureHost");
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(10_100);
      expect(start).not.toHaveBeenCalled();
    } finally {
      await client.close(); vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
