import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MeshStore,
  type MeshIdentity,
  type MeshStoreOptions,
} from "../src/mesh/store.js";
import { CONTROL_CLAIMS_POLICY_KEY, FabricControlPlane } from "../src/topology/control-plane.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];

const identity = (id: string): MeshIdentity => ({
  id,
  name: id,
  kind: "main",
  sessionId: id,
});

const plane = (
  meshRoot: string,
  id: string,
  storeOptions: MeshStoreOptions = {},
  controlOptions: { pollMs?: number; acknowledgementTimeoutMs?: number } = {},
): FabricControlPlane => {
  const value = new FabricControlPlane(
    new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions),
    identity(id),
    {
      enabled: true,
      hostId: id,
      pollMs: 20,
      acknowledgementTimeoutMs: 1_000,
      ...controlOptions,
    },
  );
  planes.push(value);
  return value;
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map((value) => value.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("FabricControlPlane", () => {
  // smarty-dev#816: the deadline was stamped before the sender waited for the mesh lock (2-3 s
  // under load), so the owner got only what was left of the 5 s, or nothing. review/astra F1 on
  // #70: the sender's own wait must start at commit too.
  describe("with a publish that waits for the mesh lock", () => {
    const run = async (lockWaitMs: number, handlerMs: number) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s timeout
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, handlerMs));
        return { accepted: true, messageId: "delivered" };
      });
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      const original = MeshStore.prototype.publish;
      vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
        if (input.topic === "fabric.control.command" && input.kind === "steer") {
          await new Promise((resolve) => setTimeout(resolve, lockWaitMs));
        }
        return original.call(this, input);
      });
      const result = await sender.request("host:receiver", "agent:target", "steer", { message: "late" });
      const event = new MeshStore(meshRoot, 64 * 1024, 1_000).read({ topic: "fabric.control.command", limit: 10 })
        .find((candidate) => candidate.kind === "steer")!;
      return { result, receive, event };
    };

    it("stamps the command's deadline at commit, and waits for a slow owner from there", async () => {
      // 2.5 s lock wait + 750 ms handler: the old sender timer (1 s + 2 s grace from the start) fired first.
      const { result, receive, event } = await run(2_500, 750);
      expect(result.messageId).toBe("delivered");
      expect(receive).toHaveBeenCalledTimes(1);
      expect((event.data as { requestedAt: number }).requestedAt).toBe(event.createdAt);
    }, 15_000);

    it("does not time out before its publish commits, however long the lock wait", async () => {
      const { result, receive } = await run(3_500, 0);                    // longer than the old 3 s timer
      expect(result.messageId).toBe("delivered");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });

  // smarty-dev#424: a lock timeout while the owner claimed, recorded or acknowledged a command
  // dropped it without an acknowledgement or a retry (dev-lead: 30 of 61 commands in 6 h).
  describe("after a lock timeout", () => {
    const lockTimeout = () => Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), {
      code: "FABRIC_MESH_LOCK_TIMEOUT",
    });
    const failOnce = <K extends "put" | "publish">(method: K, when: (store: MeshStore, input: never) => boolean, afterMs = 0) => {
      const original = MeshStore.prototype[method] as (...args: unknown[]) => unknown;
      let failed = false;
      return vi.spyOn(MeshStore.prototype, method).mockImplementation(function (this: MeshStore, input: never) {
        if (!failed && when(this, input)) {
          failed = true;
          return new Promise((_resolve, reject) => setTimeout(() => reject(lockTimeout()), afterMs));
        }
        return original.call(this, input);
      } as never);
    };
    const run = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn((received: { commandId: string }) => ({ accepted: true, messageId: "local:" + received.commandId }));
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 100));
      return { meshRoot, receive, request: () => sender.request("host:receiver", "agent:target", "steer", { message: "once" }) };
    };
    const shared = (store: MeshStore) => !store.root.includes(`${path.sep}control-seen${path.sep}`);
    const isClaim = (input: { key?: string }) => input.key?.startsWith("topology/control-seen/") === true;

    it("on the shared claim, the command is claimed on the next poll and runs once", async () => {
      const { receive, request } = await run();
      const spy = failOnce("put", (store, input: { key?: string }) => shared(store) && isClaim(input));
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
      expect(spy.mock.results.some((entry) => entry.type === "return")).toBe(true);
    });

    it("between the shared and the own claim, the retry keeps its shared claim and runs once", async () => {
      const { receive, request } = await run();
      failOnce("put", (store, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(store) && isClaim(input) && input.value?.acceptance === undefined);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);                           // not "indeterminate"
      expect(receive).toHaveBeenCalledTimes(1);
    });

    it("while recording the outcome, the sender still gets it", async () => {
      const { receive, request } = await run();
      failOnce("put", (store, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(store) && isClaim(input) && input.value?.acceptance !== undefined);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
    });

    // review/astra F1 on #67: a detached ask is consumed before it runs, so nothing retries its
    // acknowledgement; keeping its outcome would grow memory without bound under contention.
    it("of a detached ask, no outcome is kept: a replay is answered from the store alone", async () => {
      const { meshRoot, receive } = await run();
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const ask = {
        topic: "fabric.control.command", kind: "ask", from: identity("host:sender"), to: "host:receiver",
        data: { version: 1, commandId: "command:ask", targetId: "agent:target", operation: "ask", replyTo: "host:sender",
          message: "inspect", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
      };
      const acks = () => store.read({ topic: "fabric.control.ack", limit: 100 })
        .filter((event) => (event.data as { commandId?: string }).commandId === "command:ask");
      failOnce("put", (owner, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(owner) && isClaim(input) && input.value?.acceptance !== undefined);
      failOnce("publish", (_owner, input: { topic?: string }) => input.topic === "fabric.control.ack");
      await store.publish(ask);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(acks()).toHaveLength(0);                                       // not retried: detached
      await store.publish(ask);                                             // a replay of the same command
      await vi.waitFor(() => expect(acks()).toHaveLength(1), { timeout: 3_000, interval: 20 });
      expect(acks()[0]!.data).toMatchObject({ accepted: false, error: expect.stringContaining("indeterminate") });
      expect(receive).toHaveBeenCalledTimes(1);
    });

    it("on the acknowledgement, the next poll publishes the real outcome, even past the deadline", async () => {
      const { receive, request } = await run();
      // A real lock wait (10 s) outlasts the deadline (1 s here): the retry must not answer "expired".
      failOnce("publish", (_store, input: { topic?: string }) => input.topic === "fabric.control.ack", 1_200);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
    });
  });

  // smarty-dev#643: dedupe records lived in the shared state (26% of it on the fleet), and each
  // received command rewrote that whole file twice.
  describe("dedupe records", () => {
    const seenKey = (hostId: string, commandId: string) =>
      "topology/control-seen/" + createHash("sha256").update(`${hostId}\0${commandId}`).digest("hex");
    const ownStore = (meshRoot: string, hostId: string) =>
      new MeshStore(path.join(meshRoot, "control-seen", createHash("sha256").update(hostId).digest("hex").slice(0, 32)), 64 * 1024, 1_000);
    const command = (commandId: string, to: string) => ({
      topic: "fabric.control.command", kind: "steer", from: identity("host:sender"), to,
      data: { version: 1, commandId, targetId: "agent:target", operation: "steer", replyTo: "host:sender", message: "m", requestedAt: Date.now() },
    });
    const ackFor = (store: MeshStore, commandId: string) =>
      store.read({ topic: "fabric.control.ack", limit: 100 }).find((event) => (event.data as { commandId?: string }).commandId === commandId);

    it("keep the outcome in the owner's own store; the shared state holds only the claim", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const sender = plane(meshRoot, "host:sender");
      const receiver = plane(meshRoot, "host:receiver");
      sender.start(() => ({ accepted: false }));
      receiver.start((received) => ({ accepted: true, messageId: "local:" + received.commandId }));
      await new Promise((resolve) => setTimeout(resolve, 100));             // past its one-time legacy move
      const result = await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
      const commandEvent = store.read({ topic: "fabric.control.command", limit: 10 }).at(-1)!;
      const commandId = (commandEvent.data as { commandId: string }).commandId;
      expect(result.messageId).toBe("local:" + commandId);
      const shared = store.listAll("topology/control-seen/");
      expect(shared.map((entry) => entry.key)).toEqual([seenKey("host:receiver", commandId)]);
      expect(shared[0]!.value).not.toHaveProperty("acceptance");            // one shared write, no outcome
      expect(shared[0]!.version).toBe(store.get(shared[0]!.key)!.version);
      expect(ownStore(meshRoot, "host:receiver").get(seenKey("host:receiver", commandId))?.value).toMatchObject({
        hostId: "host:receiver", commandId, sequence: commandEvent.sequence, acceptance: { accepted: true },
      });
    });

    it("left in the shared state by older runtimes still answer a replay, and stay there", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:old", "host:receiver"));          // replayed at startup
      await store.put({
        key: seenKey("host:receiver", "command:old"), identity: identity("host:receiver"), ifVersion: 0, value: {
          format: 1, hostId: "host:receiver", commandId: "command:old", targetId: "agent:target", expiresAt: Date.now() + 60_000,
          acceptance: { accepted: true, messageId: "earlier:command:old" },
        },
      });
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:old")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(ackFor(store, "command:old")?.data).toMatchObject({ accepted: true, messageId: "earlier:command:old" });
      await store.publish(command("command:old", "host:receiver"));          // republished
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(receive).not.toHaveBeenCalled();
      expect(store.get(seenKey("host:receiver", "command:old"))).toBeDefined(); // older runtimes still rely on it
    });

    // review/astra on #58, F2: a runtime before this change claims only the shared key and never
    // reads the owner's store. Its admission is get(key) then put(key, ifVersion 0).
    const olderRuntimeAdmits = (store: MeshStore, commandId: string) => ({
      checked: store.get(seenKey("host:receiver", commandId), { fresh: true }) === undefined,
      claim: () => store.put({
        key: seenKey("host:receiver", commandId), identity: identity("host:receiver"), ifVersion: 0,
        value: { format: 1, hostId: "host:receiver", commandId, targetId: "agent:target", expiresAt: Date.now() + 60_000 },
      }).then(() => true, () => false),
    });

    it("run a command once when an older runtime of the same host checked it first and claims it later", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:race", "host:receiver"));
      const older = olderRuntimeAdmits(store, "command:race");               // paused after its check
      expect(older.checked).toBe(true);
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      const olderRuns = (await older.claim()) ? 1 : 0;                       // it resumes and claims
      expect(receive.mock.calls.length + olderRuns).toBe(1);
    });

    it("run a command once when an older runtime of the same host claimed it first", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:taken", "host:receiver"));
      const older = olderRuntimeAdmits(store, "command:taken");
      expect(older.checked && await older.claim()).toBe(true);               // it runs the command
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:taken")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:taken")?.data).toMatchObject({ accepted: false });
    });

    // review/astra on #58, F1: the deadline can pass while the claim waits for a lock, or after it
    // commits and before its promise resumes; the handler must not run then.
    const expiringCommand = async (meshRoot: string, commandId: string, deadlineInMs: number) => {
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const requestedAt = Date.now();
      await store.publish({ ...command(commandId, "host:receiver"),
        data: { ...command(commandId, "host:receiver").data, requestedAt, deadlineAt: requestedAt + deadlineInMs } });
      return store;
    };

    it("do not run a command whose claim returned after its deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = await expiringCommand(meshRoot, "command:late", 400);
      const put = MeshStore.prototype.put;
      vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
        const result = await put.call(this, input);                           // the claim commits ...
        if (this.root.includes("control-seen") && input.ifVersion === 0) await new Promise((resolve) => setTimeout(resolve, 600));
        return result;                                                        // ... and returns late
      });
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:late")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:late")?.data).toMatchObject({ accepted: false, error: "Fabric control command expired" });
      expect(ownStore(meshRoot, "host:receiver").get(seenKey("host:receiver", "command:late"))?.value)
        .toMatchObject({ acceptance: { accepted: false, error: "Fabric control command expired" } });
    });

    it("do not run a command whose claim waited for the owner's store lock past its deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = await expiringCommand(meshRoot, "command:locked", 400);
      const own = ownStore(meshRoot, "host:receiver");
      await own.put({ key: "warm", value: 1, identity: identity("host:receiver") });   // the store exists
      const lock = path.join(own.root, ".lock");
      fs.mkdirSync(lock);                                                     // another writer holds it
      fs.writeFileSync(path.join(lock, "owner"), `held\n${process.pid}\n${Date.now()}\n`);   // a live owner
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 700));              // past the deadline
      fs.rmSync(lock, { recursive: true, force: true });                     // the lock is released
      await vi.waitFor(() => expect(ackFor(store, "command:locked")).toBeDefined(), { timeout: 5_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:locked")?.data).toMatchObject({ accepted: false, error: "Fabric control command expired" });
    });

    // smarty-dev#816: shared claims were kept until their command left the event log (compacted
    // only at 64 MiB); 2,858 expired claims made up 46% of the shared state and saturated the lock.
    it("in the shared state go once expired plus a grace when the fleet owner allows it and the command had its own deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const start = Date.now();
      const MINUTE = 60_000;
      const later = start + 16 * MINUTE;                                   // past the 15-min sweep interval
      const claim = (commandId: string, value: Record<string, unknown>) => store.put({
        key: seenKey("host:other", commandId), identity: identity("host:other"), ifVersion: 0,
        value: { format: 1, hostId: "host:other", commandId, targetId: "agent:target", ...value },
      });
      for (const id of ["command:flagged", "command:grace", "command:legacy"]) {
        await store.publish(command(id, "host:other"));                    // all still in the log
      }
      await claim("command:flagged", { expiresAt: start, explicitDeadline: true });
      await claim("command:grace", { expiresAt: later - 5 * MINUTE, explicitDeadline: true });   // inside the grace
      await claim("command:legacy", { expiresAt: start });                // an older writer: no flag
      await store.put({ key: CONTROL_CLAIMS_POLICY_KEY, value: { version: 1, sharedClaims: "expiry" }, identity: identity("host:owner") });
      const receiver = plane(meshRoot, "host:receiver");
      receiver.start(() => ({ accepted: true }));
      const sender = plane(meshRoot, "host:sender");
      sender.start(() => ({ accepted: false }));
      await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
      const fresh = store.listAll("topology/control-seen/").find((entry) =>
        (entry.value as { hostId?: string }).hostId === "host:receiver");
      expect(fresh?.value).toMatchObject({ explicitDeadline: true });    // new claims carry the flag
      const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
      const now = Date.now;
      vi.spyOn(Date, "now").mockImplementation(() => now() - start + later);
      await vi.waitFor(() => expect(store.get(seenKey("host:other", "command:flagged"))).toBeUndefined(),
        { timeout: 3_000, interval: 20 });
      expect(store.get(seenKey("host:other", "command:grace"))).toBeDefined();
      expect(store.get(seenKey("host:other", "command:legacy"))).toBeDefined();   // still in the log
      const sweeps = batches.mock.calls.filter(([input]) => input.ops.every((op) => op.kind === "delete")
        && input.ops.some((op) => op.key.startsWith("topology/control-seen/")));
      expect(sweeps.length).toBeGreaterThanOrEqual(1);
      expect(sweeps[0]![0].ops.every((op) => op.ifVersion !== undefined)).toBe(true);   // one fenced batch
    });

    // review/astra on #65: a runtime before phase 1 checks the deadline only at admission and knows
    // only the shared claim. Without the fleet owner's policy, the shared claim must outlive expiry.
    it("keep the shared claim for a paused older runtime through the sweep and tombstone eviction", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const requestedAt = Date.now();
      await store.publish({ ...command("command:paused", "host:receiver"),
        data: { ...command("command:paused", "host:receiver").data, requestedAt, deadlineAt: requestedAt + 5_000 } });
      const older = olderRuntimeAdmits(store, "command:paused");            // passed admission, then pauses
      expect(older.checked).toBe(true);
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      expect(store.get(seenKey("host:receiver", "command:paused"))?.value).toMatchObject({ explicitDeadline: true });
      const now = Date.now;
      vi.spyOn(Date, "now").mockImplementation(() => now() + 16 * 60_000);   // past expiry, grace and sweep
      await new Promise((resolve) => setTimeout(resolve, 300));             // sweeps run
      for (let index = 0; index < 1_010; index++) {                         // evict every older tombstone
        await store.writeBatch({ identity: identity("host:noise"), ops: [
          { kind: "put", key: `noise/${index}`, value: index }, { kind: "delete", key: `noise/${index}` },
        ] });
      }
      const olderRuns = (await older.claim()) ? 1 : 0;                      // the older runtime resumes
      expect(receive.mock.calls.length + olderRuns).toBe(1);
      expect(store.get(seenKey("host:receiver", "command:paused"))).toBeDefined();
    }, 60_000);

    it("are deleted only once expired and their command has left the log", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const storeOptions: MeshStoreOptions = { maxEventLogBytes: 80_000, retainedEventLogBytes: 40_000 };
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
      const sender = plane(meshRoot, "host:sender", storeOptions, { acknowledgementTimeoutMs: 200 });
      const receiver = plane(meshRoot, "host:receiver", storeOptions, { acknowledgementTimeoutMs: 200 });
      const receive = vi.fn(() => ({ accepted: true }));
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
      const commandId = (store.read({ topic: "fabric.control.command", limit: 10 }).at(-1)!.data as { commandId: string }).commandId;
      const own = ownStore(meshRoot, "host:receiver");
      await new Promise((resolve) => setTimeout(resolve, 900));             // expired, still in the log
      expect(own.get(seenKey("host:receiver", commandId))).toBeDefined();
      for (let index = 0; index < 100; index++) {                          // rotate it out of the log
        await store.publish({ topic: "compact", from: identity("host:publisher"), text: "x".repeat(900) });
      }
      expect(store.oldestSequence()).toBeGreaterThan(1);
      await vi.waitFor(() => expect(own.get(seenKey("host:receiver", commandId))).toBeUndefined(), { timeout: 3_000, interval: 20 });
      expect(receive).toHaveBeenCalledTimes(1);
    });
  });

  it("routes to one execution owner and returns its acknowledgement", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const bystander = plane(meshRoot, "host:bystander");
    const receive = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: "local:" + command.commandId,
    }));
    const observe = vi.fn(() => ({ accepted: true }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);
    bystander.start(observe);

    await expect(
      sender.request("host:receiver", "agent:target", "steer", {
        message: "focus",
        triggerTurn: false,
      }),
    ).resolves.toMatchObject({
      queued: true,
      routed: "mesh",
      acknowledged: true,
      messageId: expect.stringMatching(/^local:/),
    });
    expect(receive).toHaveBeenCalledWith(
      expect.objectContaining({
        targetId: "agent:target",
        operation: "steer",
        message: "focus",
        triggerTurn: false,
        replyTo: "host:sender",
      }),
      expect.objectContaining({ id: "host:sender" }),
      expect.any(AbortSignal),
    );
    expect(observe).not.toHaveBeenCalled();
  });

  it("returns an authenticated result with the caller's actor binding", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const response = { id: "message:1", text: "done" };
    const receive = vi.fn((command: { binding?: unknown }) => ({
      accepted: true,
      messageId: "message:1",
      result: response,
    }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);

    await expect(
      sender.requestResult(
        "host:receiver",
        "actor:target",
        "ask",
        {
          message: "inspect",
          binding: { model: "provider/session-b", thinking: "high" },
        },
      ),
    ).resolves.toEqual(response);
    expect(receive).toHaveBeenCalledWith(
      expect.objectContaining({
        targetId: "actor:target",
        operation: "ask",
        binding: { model: "provider/session-b", thinking: "high" },
        deadlineAt: expect.any(Number),
      }),
      expect.objectContaining({ id: "host:sender" }),
      expect.any(AbortSignal),
    );
  });
  it("ignores an acknowledgement forged by a different mesh identity", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    sender.start(() => ({ accepted: false }));
    receiver.start(async (command) => {
      await new Promise((resolve) => setTimeout(resolve, 120));
      return { accepted: true, messageId: "real:" + command.commandId };
    });

    const request = sender.request("host:receiver", "agent:target", "steer", {
      message: "focus",
    });
    await new Promise((resolve) => setTimeout(resolve, 35));
    const command = store.read({ topic: "fabric.control.command", limit: 1 })[0];
    const commandId = (command?.data as { commandId?: string } | undefined)?.commandId;
    expect(commandId).toBeTypeOf("string");
    await store.publish({
      topic: "fabric.control.ack",
      kind: "accepted",
      from: identity("host:bystander"),
      to: "host:sender",
      data: {
        version: 1,
        commandId,
        targetId: "agent:target",
        accepted: true,
        messageId: "forged",
      },
    });

    await expect(request).resolves.toMatchObject({
      acknowledged: true,
      messageId: expect.stringMatching(/^real:/),
    });
  });

  it("recovers an unexpired command published before owner startup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:before-start",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "recover",
        requestedAt: Date.now(),
      },
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true, messageId: "recovered" }));
    receiver.start(receive);
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).toHaveBeenCalledTimes(1);
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:before-start",
          accepted: true,
        }),
      }),
    );
  });

  it("rejects an interrupted durable claim as indeterminate after restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const commandId = "command:interrupted";
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId,
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "unknown outcome",
        requestedAt: Date.now(),
      },
    });
    const seenKey =
      "topology/control-seen/" +
      createHash("sha256").update(`host:receiver\0${commandId}`).digest("hex");
    await store.put({
      key: seenKey,
      value: {
        format: 1,
        hostId: "host:receiver",
        commandId,
        targetId: "agent:target",
        expiresAt: Date.now() + 1_000,
      },
      identity: identity("host:receiver"),
      ifVersion: 0,
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId,
          accepted: false,
          error: "Fabric control outcome is indeterminate after owner restart",
        }),
      }),
    );
  });

  it("does not re-execute a command republished after owner restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const firstOwner = plane(meshRoot, "host:receiver");
    const firstHandler = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    firstOwner.start(firstHandler);
    await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
    expect(firstHandler).toHaveBeenCalledTimes(1);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const original = store.read({ topic: "fabric.control.command", limit: 10 })[0];
    expect(original).toBeDefined();
    await firstOwner.close();

    const restartedOwner = plane(meshRoot, "host:receiver");
    const restartedHandler = vi.fn(() => ({ accepted: true }));
    restartedOwner.start(restartedHandler);
    await store.publish({
      topic: "fabric.control.command",
      kind: original!.kind,
      from: original!.from,
      ...(original!.to ? { to: original!.to } : {}),
      ...(original!.data === undefined ? {} : { data: original!.data }),
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(restartedHandler).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 }).length).toBeGreaterThan(1);
  });

  it("rejects a replay outside the acknowledgement lifetime", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:replayed",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "stale",
        requestedAt: Date.now() - 5_000,
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:replayed",
          accepted: false,
          error: "Fabric control command expired",
        }),
      }),
    );
  });

  // smarty-dev#367: every restarting owner replays the retained log from its start; it
  // answered each long-expired command again, 21,479 acks nobody waited for.
  it("does not answer a command whose sender stopped waiting long ago", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:history",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "old",
        requestedAt: Date.now() - 10 * 60_000,
      },
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);                                     // a restart replays the log
    await new Promise((resolve) => setTimeout(resolve, 150));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toEqual([]);
  });

  it("final-drains a command published immediately before close", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true, messageId: "drained" }));
    receiver.start(receive);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:before-close",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "finish",
        requestedAt: Date.now(),
      },
    });

    await receiver.close();

    expect(receive).toHaveBeenCalledTimes(1);
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        to: "host:sender",
        data: expect.objectContaining({
          commandId: "command:before-close",
          accepted: true,
        }),
      }),
    );
  });

  it("does not re-execute a retained command after event-log compaction", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const storeOptions: MeshStoreOptions = {
      maxEventLogBytes: 80_000,
      retainedEventLogBytes: 75_000,
    };
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
    for (let index = 0; index < 8; index++) {
      await store.publish({
        topic: "prefill",
        from: identity("host:prefill"),
        text: "p".repeat(900),
      });
    }
    const sender = plane(meshRoot, "host:sender", storeOptions);
    const receiver = plane(meshRoot, "host:receiver", storeOptions);
    const receive = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);

    await sender.request("host:receiver", "agent:target", "steer", { message: "once" });
    expect(receive).toHaveBeenCalledTimes(1);
    for (let index = 0; index < 62; index++) {
      await store.publish({
        topic: "compact",
        from: identity("host:publisher"),
        text: "x".repeat(900),
      });
    }
    await new Promise((resolve) => setTimeout(resolve, 160));

    expect(receive).toHaveBeenCalledTimes(1);
  });

  it("keeps control traffic responsive while an ask is running and cancels the owner", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const controller = new AbortController();
    let askStarted = false;
    let askAborted = false;
    receiver.start((command, _from, signal) => {
      if (command.operation !== "ask") {
        return { accepted: true, messageId: `accepted:${command.operation}` };
      }
      askStarted = true;
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          askAborted = true;
          resolve({ accepted: false, error: "actor request cancelled" });
        }, { once: true });
      });
    });
    sender.start(() => ({ accepted: false }));

    const ask = sender.requestResult(
      "host:receiver",
      "actor:target",
      "ask",
      { message: "inspect" },
      "host:receiver",
      { timeoutMs: 2_000, signal: controller.signal },
    );
    await vi.waitFor(() => expect(askStarted).toBe(true));
    await expect(
      sender.request("host:receiver", "actor:target", "stop"),
    ).resolves.toMatchObject({ acknowledged: true });

    controller.abort();
    await expect(ask).rejects.toThrow("Remote Fabric request cancelled");
    await vi.waitFor(() => expect(askAborted).toBe(true));
  });

  it("publishes an immediately cancelled command before its cancellation", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const publish = sender.mesh.publish.bind(sender.mesh);
    vi.spyOn(sender.mesh, "publish").mockImplementation(async (input) => {
      if (input.topic === "fabric.control.command" && input.kind === "ask") {
        await new Promise((resolve) => setTimeout(resolve, 60));
      }
      return publish(input);
    });
    let ownerAborted = false;
    receiver.start((command, _from, signal) => {
      if (command.operation !== "ask") return { accepted: true };
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          ownerAborted = true;
          resolve({ accepted: false, error: "cancelled" });
        }, { once: true });
      });
    });
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();

    const request = sender.requestResult(
      "host:receiver",
      "actor:target",
      "ask",
      { message: "inspect" },
      "host:receiver",
      { timeoutMs: 1_000, signal: controller.signal },
    );
    controller.abort();

    await expect(request).rejects.toThrow("Remote Fabric request cancelled");
    await vi.waitFor(() => expect(ownerAborted).toBe(true));
    const kinds = new MeshStore(meshRoot, 64 * 1024, 1_000)
      .tail(0, 10)
      .events.filter((event) => event.topic === "fabric.control.command")
      .map((event) => event.kind);
    expect(kinds.indexOf("ask")).toBeLessThan(kinds.indexOf("cancel"));
  });

  it("retains a completed ask outcome through its request deadline", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(
      meshRoot,
      "host:receiver",
      {},
      { pollMs: 20, acknowledgementTimeoutMs: 80 },
    );
    const receive = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 120));
      return { accepted: true, result: { id: "message:1", text: "done" } };
    });
    receiver.start(receive);
    sender.start(() => ({ accepted: false }));

    await expect(
      sender.requestResult(
        "host:receiver",
        "actor:target",
        "ask",
        { message: "inspect" },
        "host:receiver",
        { timeoutMs: 500 },
      ),
    ).resolves.toMatchObject({ text: "done" });
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const original = store.read({ topic: "fabric.control.command", limit: 10 })
      .find((event) => event.kind === "ask");
    expect(original).toBeDefined();
    await store.publish({
      topic: "fabric.control.command",
      kind: original!.kind,
      from: original!.from,
      ...(original!.to ? { to: original!.to } : {}),
      ...(original!.data === undefined ? {} : { data: original!.data }),
    });
    await new Promise((resolve) => setTimeout(resolve, 180));

    expect(receive).toHaveBeenCalledTimes(1);
  });

  // smarty-dev#367: a retry after an acknowledgement timeout delivered the message twice.
  it("waits past the deadline for the acknowledgement of a command the owner admitted in time", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // A 400 ms deadline leaves a slow runner (windows-latest) time to admit the command;
    // the handler then acknowledges after the deadline but inside the 2 x 400 ms grace.
    const sender = plane(meshRoot, "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 400 });
    const receiver = plane(meshRoot, "host:receiver", {}, { pollMs: 20, acknowledgementTimeoutMs: 400 });
    // Admitted within the deadline, acknowledged after it (a slow handler or a contended
    // mesh lock): the sender must report the delivery, not a timeout.
    receiver.start(async () => {
      await new Promise((resolve) => setTimeout(resolve, 500));
      return { accepted: true, messageId: "late-but-delivered" };
    });
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "slow ack" }))
      .resolves.toMatchObject({ acknowledged: true, messageId: "late-but-delivered" });
  });

  it("still times out, after the deadline plus a bounded grace, when no owner answers", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 100 });
    sender.start(() => ({ accepted: false }));
    const started = Date.now();
    // No receiver runs. The outcome is unknown, so the error must not promise a safe retry.
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "nobody home" }))
      .rejects.toThrow("the outcome is unknown and it may still be delivered, so a retry can deliver it twice");
    const waited = Date.now() - started;
    expect(waited).toBeGreaterThanOrEqual(100 + 200 - 20);   // the deadline plus 2 x 100 ms of grace
    expect(waited).toBeLessThan(2_000);
  });

  it("surfaces owner rejection instead of reporting an unverified queue", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    sender.start(() => ({ accepted: false }));
    receiver.start(() => ({ accepted: false, error: "target already settled" }));

    await expect(
      sender.request("host:receiver", "agent:missing", "stop"),
    ).rejects.toThrow("target already settled");
  });
});
