import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { MeshStore } from "../src/mesh/store.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { markUnresolvedWorker, claimTempRunSweep, markRunRootActive } from "../src/storage/retention.js";
import { commitResidentRequest, abandonResidentRequest, type ResidentCommand } from "../src/residency/protocol.js";
import { writeRunRecord, emptyUsage } from "../src/worker/run-record.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { RemoteRecords } from "../src/records/client.js";

const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-callers-")); roots.push(directory); return directory; };
const identity = { id: "session:audit", name: "main", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });

// Observe real filesystem calls, including the namespace barrier after publication.
const observe = () => {
  const events: string[] = [];
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs), link = fs.linkSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { events.push(`sync:${descriptors.get(fd)}`); sync(fd); });
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { events.push(`rename:${to}`); rename(from, to); });
  vi.spyOn(fs, "linkSync").mockImplementation((from, to) => { events.push(`link:${to}`); link(from, to); });
  return events;
};
const expectPublished = (events: string[], file: string, kind = "rename") => {
  const publication = events.indexOf(`${kind}:${file}`);
  expect(publication).toBeGreaterThan(0);
  expect(events.slice(0, publication).some(event => event.startsWith(`sync:${file}.`))).toBe(true);
  if (process.platform !== "win32") expect(events.slice(publication + 1)).toContain(`sync:${path.dirname(file)}`);
};

describe("#2479 durable caller classes", () => {
  it("persists ordinary actor definitions and lineage claims", () => {
    const directory = root(), events = observe();
    const store = new ActorRegistryStore(directory);
    store.write([{ id: "actor", rootId: "old" }]);
    expectPublished(events, path.join(directory, "actors.json"));
    events.length = 0;
    store.write([{ id: "actor", rootId: "new" }]);
    expectPublished(events, path.join(directory, "actors.json"));
  });

  it("requests durability for global templates and session bindings", async () => {
    const directory = root(), write = vi.spyOn(atomic, "writeJsonAtomic");
    new GlobalActorRegistry(directory, 64 * 1024).create({ name: "audit", instructions: "Audit.", events: [], topics: [] });
    await new ActorBindingStore("session:audit", directory).setModel("actor", "test/model");
    expect(write.mock.calls).toHaveLength(2);
    for (const [, , options] of write.mock.calls) expect(options?.durable).toBe(true);
  });

  it("syncs authoritative mesh revision state, not reconstructible reservations/read signals", async () => {
    const directory = root(), write = vi.spyOn(atomic, "writeFileAtomic");
    const mesh = new MeshStore(directory, 64 * 1024, 100);
    await mesh.put({ key: "resource/grant", value: { accepted: true }, identity });
    await mesh.publish({ topic: "audit", from: identity, text: "one" });
    const calls = write.mock.calls;
    expect(calls.find(([file]) => file === path.join(directory, "state.json"))?.[2]?.durable).toBe(true);
    expect(calls.find(([file]) => file === path.join(directory, "sequence"))?.[2]?.durable).not.toBe(true);
    for (const [file, , options] of calls.filter(([file]) => file.includes("signal"))) expect(options?.durable, file).not.toBe(true);
  });

  it("syncs a dependent live bridge publication and its restart cursor", async () => {
    const directory = root(), mesh = new MeshStore(path.join(directory, "mesh"), 64 * 1024, 100);
    const side = new StoreBridgeSide(mesh, "peer");
    const events = observe();
    const publish = vi.spyOn(mesh, "publish");
    await side.publish({ topic: "fleet.work.audit", kind: "message", from: identity, to: "session:target", data: { bridge: { id: "source", from: "peer" } } });
    expect(publish.mock.calls[0]?.[0].durable).toBe(true);
    const write = vi.spyOn(atomic, "writeJsonAtomic");
    const bridge = new MeshBridge({ localName: "local", remoteName: "peer", local: side, remote: side, cursorPath: path.join(directory, "cursor.json") });
    await bridge.start();
    expect(write.mock.calls.find(([file]) => file.endsWith("cursor.json"))?.[2]?.durable).toBe(true);
    const liveSync = events.indexOf(`sync:${path.join(directory, "mesh", "events.jsonl")}`);
    expect(liveSync).toBeGreaterThanOrEqual(0);
    expect(liveSync).toBeLessThan(events.indexOf(`rename:${path.join(directory, "cursor.json")}`));
    if (process.platform !== "win32") expect(events.slice(liveSync + 1)).toContain(`sync:${path.join(directory, "mesh")}`);
  });

  it.each(["commit", "abandon"])("persists the immutable resident %s fence before acknowledging it", (decision) => {
    const directory = root(), events = observe();
    const command = { format: 1, requestId: "request", operation: "spawn", rootId: identity.id, createdAt: 1, request: { task: "audit" } } as ResidentCommand;
    if (decision === "commit") commitResidentRequest(directory, command, "agent", "host");
    else abandonResidentRequest(path.join(directory, "requests"), path.join(directory, "responses"), command.requestId);
    expectPublished(events, path.join(directory, "decisions", "request.json"), "link");
  });

  it("persists unresolved-worker deletion vetoes while leaving sweep/heartbeat hints fast", () => {
    const directory = root(), write = vi.spyOn(atomic, "writeJsonAtomic");
    markUnresolvedWorker(path.join(directory, "run"), "lost worker");
    expect(write.mock.calls[0]?.[2]?.durable).toBe(true);
    write.mockClear();
    markRunRootActive(path.join(directory, "root"));
    claimTempRunSweep(directory, 1);
    expect(write.mock.calls).toHaveLength(2);
    for (const [, , options] of write.mock.calls) expect(options?.durable).not.toBe(true);
  });

  it.each(["completed", "failed", "stopped", "timed_out"] as const)("persists worker %s results but leaves progress fast", (status) => {
    const directory = root(), file = path.join(directory, "status.json"), events = observe();
    const record: AgentRunRecord = { id: "agent", name: "agent", task: "audit", status: "running", runner: "pi", transport: "process", cwd: directory, startedAt: 1, updatedAt: 1, turns: 0, toolCalls: 0, text: "", usage: emptyUsage(), logFile: path.join(directory, "events.jsonl") };
    writeRunRecord(file, record);
    expect(events.filter(event => event.startsWith("sync:"))).toEqual([]);
    events.length = 0;
    writeRunRecord(file, { ...record, status });
    expectPublished(events, file);
  });

  it("syncs a compacted event log before publishing its durable generation", async () => {
    const directory = root(), events = observe();
    const mesh = new MeshStore(directory, 1024, 100, { maxEventLogBytes: 1100, retainedEventLogBytes: 300 });
    for (let n = 0; n < 6; n++) await mesh.publish({ topic: "audit", from: identity, text: "x".repeat(400) });
    const log = path.join(directory, "events.jsonl"), generation = path.join(directory, "generation");
    expectPublished(events, log);
    expectPublished(events, generation);
    const logRename = events.indexOf(`rename:${log}`), generationRename = events.indexOf(`rename:${generation}`);
    expect(logRename).toBeLessThan(generationRename);
    if (process.platform !== "win32") expect(events.slice(logRename + 1, generationRename)).toContain(`sync:${directory}`);
  });

  it("keeps expiring host/participant presence writes non-durable", () => {
    const directory = root(), write = vi.spyOn(atomic, "writeJsonAtomic");
    writeHostLease(directory, { id: identity.id, rootId: identity.id, identityId: identity.id, updatedAt: 1, expiresAt: 2 });
    writeParticipantFile(directory, { key: `topology/participants/${"a".repeat(64)}`, value: {}, version: 1, updatedAt: 1, updatedBy: identity });
    for (const [, , options] of write.mock.calls) expect(options?.durable).not.toBe(true);
  });

  it("persists enrollment nonce before register and credential before open returns (fake local server)", async () => {
    const directory = root(), socketPath = path.join(directory, "records.sock"), write = vi.spyOn(atomic, "writeJsonAtomic");
    let nonceDurableAtRegister = false;
    const server = net.createServer(socket => {
      let input = "";
      socket.on("data", chunk => {
        input += chunk.toString();
        let boundary: number;
        while ((boundary = input.indexOf("\n")) >= 0) {
          const request = JSON.parse(input.slice(0, boundary)); input = input.slice(boundary + 1);
          if (request.method === "register") {
            nonceDurableAtRegister = write.mock.calls.at(-1)?.[2]?.durable === true;
          }
          const result = request.method === "hello" ? { org: "fake", origin: "fake" } : { id: identity.id, token: "synthetic-test-only" };
          socket.write(`${JSON.stringify({ id: request.id, ok: true, result })}\n`);
        }
      });
    });
    const address = process.platform === "win32" ? `\\\\.\\pipe\\atomic-callers-${process.pid}-${Date.now()}` : socketPath;
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(address, resolve); });
    const client = new RemoteRecords({ socket: address, credentialDir: path.join(directory, "credentials"), identity });
    try {
      await client.open();
      expect(nonceDurableAtRegister).toBe(true);
      expect(write.mock.calls).toHaveLength(2);
      for (const [, , options] of write.mock.calls) expect(options?.durable).toBe(true);
    } finally { await client.close(); await new Promise<void>(resolve => server.close(() => resolve())); }
  });
});
