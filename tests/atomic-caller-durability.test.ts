import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { markUnresolvedWorker, claimTempRunSweep, markRunRootActive } from "../src/storage/retention.js";
import { commitResidentRequest, abandonResidentRequest, type ResidentCommand } from "../src/residency/protocol.js";
import { RemoteRecords } from "../src/records/client.js";

const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-callers-")); roots.push(directory); return directory; };
const identity = { id: "session:audit", name: "main", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });

// Observe real filesystem calls, including the namespace barrier after publication.
const observe = () => {
  const events: string[] = [];
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), asyncSync = fs.fsync.bind(fs), rename = fs.renameSync.bind(fs), link = fs.linkSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { events.push(`sync:${descriptors.get(fd)}`); sync(fd); });
  vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => { events.push(`sync:${descriptors.get(fd)}`); asyncSync(fd, callback); });
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
  it.each(["native", "win32"] as const)("#2479 R3 F5 confirms the winning abandoned inode after a failed link barrier (%s)", (platformMode) => {
    const directory = root(), requests = path.join(directory, "requests"), responses = path.join(directory, "responses");
    fs.mkdirSync(requests); fs.mkdirSync(responses);
    const exchange = path.join(requests, "request.json"), decision = path.join(directory, "decisions", "request.json");
    fs.writeFileSync(exchange, "retained until confirmed");
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const actualNamespace = atomic.syncPathNamespace;
    let unavailable = true, confirmations = 0;
    const confirmation = vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
      if (file === decision) { confirmations++; if (unavailable) throw new Error("decision barrier unavailable"); }
      actualNamespace(file, inode);
    });
    const opened = vi.spyOn(fs, "openSync"), synced = vi.spyOn(fs, "fsyncSync");
    try {
      if (platformMode === "win32") Object.defineProperty(process, "platform", { value: "win32" });
      expect(() => abandonResidentRequest(requests, responses, "request")).toThrow("decision barrier unavailable");
      expect(fs.existsSync(decision)).toBe(true);
      expect(fs.existsSync(exchange)).toBe(true);
      // Existence alone is not a receipt, including while another publisher is confirming.
      expect(() => abandonResidentRequest(requests, responses, "request")).toThrow("decision barrier unavailable");
      expect(fs.existsSync(exchange)).toBe(true);
      unavailable = false;
      opened.mockClear(); synced.mockClear();
      expect(abandonResidentRequest(requests, responses, "request").state).toBe("abandoned");
      expect(confirmations).toBe(3);
      const handle = opened.mock.calls.findIndex(([file]) => String(file) === decision);
      expect(handle).toBeGreaterThanOrEqual(0);
      expect(opened.mock.calls[handle]![1]).toBe(process.platform === "win32" ? "r+" : "r");
      expect(synced.mock.calls.some(([fd]) => fd === opened.mock.results[handle]!.value)).toBe(true);
      expect(fs.existsSync(exchange)).toBe(false);
      expect(fs.readdirSync(path.join(directory, "decisions"))).toEqual(["request.json"]);
    } finally { Object.defineProperty(process, "platform", platform); opened.mockRestore(); synced.mockRestore(); confirmation.mockRestore(); }
  });
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

  it.each(["commit", "abandon"])("persists the immutable resident %s fence before acknowledging it", (decision) => {
    const directory = root(), events = observe();
    const command = {
      format: 1, requestId: "request", operation: "spawnBound", rootId: identity.id, createdAt: 1, request: { task: "audit" },
      caller: {
        id: identity.id, rootId: identity.id, sessionId: "audit", ownerHostId: "host", ownerIdentityId: identity.id, kind: "root",
        returnAddress: { spawnerId: identity.id, spawnerSessionId: "audit", ancestors: [identity.id], escalationTargets: [] },
      },
    } satisfies ResidentCommand;
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
