import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const rootDirectory = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-session-namespace-"));
  roots.push(root);
  return root;
};
const actor = { id: "actor:reply", name: "reply", kind: "actor" as const };
const directoryChain = (directory: string): string[] => {
  const chain: string[] = [];
  for (let current = path.resolve(directory); ; current = path.dirname(current)) {
    chain.push(current);
    if (path.dirname(current) === current) return chain;
  }
};
const persist = (manager: SessionManager, details: unknown) => {
  manager.appendCustomMessageEntry("pi-fabric-agent-message", "reply", true, details);
  // Real Pi defers a new session's first write until the first assistant message.
  manager.appendMessage(fauxAssistantMessage("persisted"));
};
const replace = (root: string, manager: SessionManager, details: unknown) => {
  // The test dependency's _rewriteFile is in-place; newer Pi installs a replacement by
  // rename. Have a real SessionManager persist that new inode, then install it identically.
  const replacement = SessionManager.create(root, manager.getSessionDir());
  persist(replacement, details);
  fs.renameSync(replacement.getSessionFile()!, manager.getSessionFile()!);
};
const controller = (root: string, manager: SessionManager) => {
  const handlers = new Map<string, Array<(event: unknown, context: ExtensionContext) => void>>();
  const sent: Array<{ details: Record<string, unknown> }> = [];
  const pi = {
    on: (name: string, handler: (event: unknown, context: ExtensionContext) => void) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
    },
    sendMessage: (message: { details: Record<string, unknown> }) => sent.push(message),
  } as unknown as ExtensionAPI;
  const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: manager } as unknown as ExtensionContext;
  const journal = path.join(root, "mesh", "main-followups", "root.json");
  const main = new MainAgentController(pi, "session:root", true, root, "root");
  main.attachFollowUpDrain(context, 60_000, journal);
  const settle = () => { for (const handler of handlers.get("agent_settled") ?? []) handler({ outcome: "completed" }, context); };
  return { main, sent, journal, settle };
};
const barrierProbe = (file: string, failingPath: string) => {
  const descriptors = new Map<number, string>();
  const events: string[] = [];
  const open = fs.openSync.bind(fs);
  const sync = fs.fsyncSync.bind(fs);
  let failing = true;
  const opened = vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
    const fd = open(target, flags, mode);
    descriptors.set(fd, String(target));
    return fd;
  });
  const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
    const target = descriptors.get(fd)!;
    events.push(target === file ? "session-file" : target);
    if (failing && target === failingPath) throw new Error("session directory barrier unavailable");
    sync(fd); // Session-file sync succeeds; ONLY the required directory barrier fails.
  });
  return { events, recover: () => { failing = false; events.length = 0; }, restore: () => { synced.mockRestore(); opened.mockRestore(); } };
};
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("session durability test timed out");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const cases = ["creation", "replacement"].flatMap((operation) => ["leaf", "ancestor"].map((barrier) => ({ operation, barrier })));

describe("#180 S4 persisted session namespace durability", () => {
  it.skipIf(process.platform === "win32").each(cases)("retains the journal payload on persistent $operation $barrier barrier failure, then retires once", ({ operation, barrier }) => {
    const root = rootDirectory();
    const directory = path.join(root, "separate", "sessions");
    const manager = SessionManager.create(root, directory);
    const state = controller(root, manager);
    state.main.deliverAgent({ from: actor, message: "reply", delivery: "steer", deliveryId: "retire-once" });
    const details = state.sent[0]!.details;
    persist(manager, details);
    if (operation === "replacement") replace(root, manager, details);
    const probe = barrierProbe(manager.getSessionFile()!, barrier === "leaf" ? directory : path.dirname(directory));
    const retire = fs.rmSync.bind(fs);
    const retired = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) === state.journal) probe.events.push("payload-retirement");
      retire(target, options);
    });
    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        state.settle();
        expect(fs.readFileSync(state.journal, "utf8")).toContain("retire-once");
        expect(fs.existsSync(`${state.journal}.delivered`)).toBe(false);
      }
      expect(probe.events.filter((event) => event === "session-file")).toHaveLength(3);
      expect(probe.events).not.toContain("payload-retirement");
      probe.recover();
      state.settle();
      const chain = directoryChain(directory);
      expect(probe.events.slice(0, chain.length + 1)).toEqual(["session-file", ...chain]);
      expect(probe.events.indexOf("payload-retirement")).toBeGreaterThan(chain.length);
      expect(fs.existsSync(state.journal)).toBe(false);
      expect(JSON.parse(fs.readFileSync(`${state.journal}.delivered`, "utf8")).ids).toEqual(["retire-once"]);
      state.settle();
      expect(probe.events.filter((event) => event === "payload-retirement")).toHaveLength(1);
      expect(state.sent).toHaveLength(1);
    } finally { retired.mockRestore(); probe.restore(); state.main.closeFollowUpDrain(); }
  });

  it.skipIf(process.platform === "win32").each(cases)("retains the resident source and acknowledges nothing on persistent session-only $operation $barrier failure, then acknowledges once", async ({ operation, barrier }) => {
    const root = rootDirectory();
    const directory = path.join(root, "separate", "sessions");
    const manager = SessionManager.create(root, directory);
    const id = "reply";
    const deliveryId = `resident:session:root:${id}`;
    const details = { id: "session-message", deliveryId };
    persist(manager, details);
    const state = controller(root, manager);
    const request = { from: actor, message: "reply", delivery: "steer" as const, deliveryId };
    if (operation === "replacement") {
      // Prime cached receipts, then replace at the SAME SIZE. Size-only caches must not
      // bypass namespace barriers or retain the old inode's receipt after replacement.
      expect(state.main.deliverAgent(request)).toMatchObject({ duplicate: true });
      const before = fs.statSync(manager.getSessionFile()!);
      replace(root, manager, details);
      const after = fs.statSync(manager.getSessionFile()!);
      expect(after.size).toBe(before.size);
      expect(after.ino).not.toBe(before.ino);
    }
    const mesh = new MeshStore(path.join(root, "mesh"), DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents);
    const key = `${residentDeliveryPrefix("session:root")}${id}`;
    await mesh.put({ key, identity: { id: residentHostId("session:root"), name: "resident", kind: "main" }, value: {
      format: RESIDENT_HOST_FORMAT, id, rootId: "session:root", from: actor,
      message: "reply", delivery: "steer", triggerTurn: true, createdAt: 1,
    } });
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:root", sessionId: "root", cwd: root, projectRoot: root,
      meshRoot: mesh.root, actorRoot: path.join(root, "mesh", "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const probe = barrierProbe(manager.getSessionFile()!, barrier === "leaf" ? directory : path.dirname(directory));
    const deliver = state.main.deliverAgent.bind(state.main);
    const delivered = vi.spyOn(state.main, "deliverAgent").mockImplementation((value) => {
      const result = deliver(value); probe.events.push("acknowledgment"); return result;
    });
    const remove = mesh.delete.bind(mesh);
    const removed = vi.spyOn(mesh, "delete").mockImplementation(async (value) => {
      if (value.key === key) probe.events.push("source-delete");
      return remove(value);
    });
    const client = new ResidencyClient({ config, mesh, participants: {} as never, mainAgent: state.main });
    try {
      client.start();
      await waitFor(() => delivered.mock.results.length >= 3 || delivered.mock.results.some((result) => result.type === "return"));
      expect(delivered.mock.results.every((result) => result.type === "throw")).toBe(true);
      expect(mesh.get(key)).toBeDefined();
      expect(probe.events).not.toContain("acknowledgment");
      expect(probe.events).not.toContain("source-delete");
      expect(state.sent).toHaveLength(0);
      expect(fs.existsSync(state.journal)).toBe(false);
      probe.recover();
      await waitFor(() => mesh.get(key) === undefined);
      const chain = directoryChain(directory);
      expect(probe.events.slice(0, chain.length + 1)).toEqual(["session-file", ...chain]);
      expect(probe.events.slice(chain.length + 1)).toEqual(["acknowledgment", "source-delete"]);
      expect(delivered.mock.results.filter((result) => result.type === "return")).toHaveLength(1);
      expect(delivered.mock.results.at(-1)).toMatchObject({ value: { duplicate: true } });
      expect(removed.mock.calls.filter(([value]) => value.key === key)).toHaveLength(1);
      expect(state.sent).toHaveLength(0);
    } finally {
      await client.close(); removed.mockRestore(); delivered.mockRestore(); probe.restore(); state.main.closeFollowUpDrain();
    }
  });

  it("keeps Windows receipt handles writable but noncreating/nontruncating, and skips unsupported directory fsync", () => {
    const root = rootDirectory();
    const manager = SessionManager.create(root, path.join(root, "separate", "sessions"));
    persist(manager, { id: "entry", deliveryId: "windows-receipt" });
    const state = controller(root, manager);
    const file = manager.getSessionFile()!;
    const before = fs.readFileSync(file, "utf8");
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const opened = vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      expect(String(target)).toBe(file);
      expect(flags).toBe("r+");
      return open(target, flags, mode);
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      expect(fs.fstatSync(fd).isDirectory()).toBe(false);
      sync(fd);
    });
    try {
      Object.defineProperty(process, "platform", { value: "win32" });
      expect(state.main.deliverAgent({ from: actor, message: "reply", delivery: "steer", deliveryId: "windows-receipt" }))
        .toMatchObject({ duplicate: true });
      expect(opened).toHaveBeenCalledTimes(1);
      expect(synced).toHaveBeenCalledTimes(1);
    } finally {
      Object.defineProperty(process, "platform", platform);
      synced.mockRestore(); opened.mockRestore(); state.main.closeFollowUpDrain();
    }
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it("invalidates cached receipts when the persisted session file disappears", () => {
    const root = rootDirectory();
    const manager = SessionManager.create(root, path.join(root, "separate", "sessions"));
    persist(manager, { id: "entry", deliveryId: "removed-receipt" });
    const state = controller(root, manager);
    const request = { from: actor, message: "reply", delivery: "steer" as const, deliveryId: "removed-receipt" };
    try {
      expect(state.main.deliverAgent(request)).toMatchObject({ duplicate: true });
      fs.rmSync(manager.getSessionFile()!);
      expect(state.main.deliverAgent(request).duplicate).toBeUndefined();
      expect(state.sent).toHaveLength(1);
      expect(fs.readFileSync(state.journal, "utf8")).toContain("removed-receipt");
    } finally { state.main.closeFollowUpDrain(); }
  });

  it("rescans a same-size replacement instead of trusting the previous inode's cached receipts", () => {
    const root = rootDirectory();
    const manager = SessionManager.create(root, path.join(root, "separate", "sessions"));
    persist(manager, { id: "entry-a", deliveryId: "receipt-a" });
    const state = controller(root, manager);
    const request = (deliveryId: string) => ({ from: actor, message: "reply", delivery: "steer" as const, deliveryId });
    try {
      expect(state.main.deliverAgent(request("receipt-a"))).toMatchObject({ duplicate: true });
      const before = fs.statSync(manager.getSessionFile()!).size;
      replace(root, manager, { id: "entry-b", deliveryId: "receipt-b" });
      expect(fs.statSync(manager.getSessionFile()!).size).toBe(before);
      expect(state.main.deliverAgent(request("receipt-b"))).toMatchObject({ duplicate: true });
      expect(state.main.deliverAgent(request("receipt-a")).duplicate).toBeUndefined();
      expect(state.sent).toHaveLength(1);
      expect(fs.readFileSync(state.journal, "utf8")).toContain("receipt-a");
    } finally { state.main.closeFollowUpDrain(); }
  });
});
