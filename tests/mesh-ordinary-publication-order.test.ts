import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import type { MeshEvent } from "../src/mesh/event-log.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { withInitialMeshLockAcquisition, withInitialMeshLockAdmission } from "../src/mesh/mesh-lock.js";
import { MeshStore } from "../src/mesh/store.js";

// Ordinary (non-retained) publication keeps the lock's promise transparent. On an idle, uncontended
// protocol-1 root the canonical critical section runs synchronously inside publish(), so the lock's
// own promise is already settled when EventLog awaits it. EventLog then resumes in the next microtask
// and registers the live-log group barrier before any microtask its caller queued after the call.
// An extra adopting promise on the ordinary branch moves that registration behind the caller's
// microtask. This is an ordering witness for ordinary publication only, not an elapsed-time test.

const from = { id: "session:publication-order", name: "publication-order", kind: "main" as const };
const topic = "mesh.publication-order";
const TICKET = /^\d{24}-\d+-[a-f0-9-]+$/;
// EventLog's live-log group barrier is the only setImmediate on the ordinary publication path.
const EVENT_LOG_FRAME = /[\\/]mesh[\\/]event-log\.[cm]?[jt]s\b/;

const scratchStore = () => {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-publication-order-"));
  // The FIFO queue lives beside the mesh root: a child root keeps it inside this scratch directory.
  const root = path.join(scratch, "mesh");
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
  return {
    root, store,
    lines: (): string[] => {
      const file = path.join(root, "events.jsonl");
      return fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean) : [];
    },
    tickets: (): string[] => {
      const directory = meshLockQueueDirectory(root);
      return fs.existsSync(directory) ? fs.readdirSync(directory).filter(name => TICKET.test(name)) : [];
    },
    close: (): void => {
      store.closeState();
      fs.rmSync(scratch, { recursive: true, force: true });
    },
  };
};

describe("ordinary mesh publication promise order", () => {
  it("registers the durable group barrier before a caller microtask queued right after publish", async () => {
    const f = scratchStore();
    const order: string[] = [];
    const original = globalThis.setImmediate;
    // Pass-through observer: the real barrier is registered at the same moment, with the same callback.
    const observed = function (this: unknown, ...args: unknown[]) {
      if (EVENT_LOG_FRAME.test((new Error().stack ?? "").split("\n")[2] ?? "")) order.push("barrier");
      return Reflect.apply(original, this, args);
    } as unknown as typeof setImmediate;
    let publication: Promise<MeshEvent> | undefined;
    try {
      expect(f.store.lockProtocol).toBe(1);
      globalThis.setImmediate = observed;
      publication = f.store.publish({ topic, from, text: "ordered", durable: true });
      queueMicrotask(() => { order.push("caller microtask"); });
      // Resolves only after the real barrier has fsynced the live log.
      const event = await publication;
      globalThis.setImmediate = original;
      expect(order).toEqual(["barrier", "caller microtask"]);
      expect(event).toMatchObject({ sequence: 1, topic, text: "ordered" });
      expect(f.lines().map(line => (JSON.parse(line) as MeshEvent).id)).toEqual([event.id]);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      expect(f.tickets()).toEqual([]);
    } finally {
      if (globalThis.setImmediate === observed) globalThis.setImmediate = original;
      await publication?.catch(() => undefined);
      f.close();
    }
  });

  it("rejects a synchronous preparation refusal with its own reason and appends nothing", async () => {
    const f = scratchStore();
    try {
      const reason = new Error("caller aborted before preparation");
      const controller = new AbortController();
      controller.abort(reason);
      await expect(f.store.publish({ topic, from, text: "refused", durable: true, signal: controller.signal }))
        .rejects.toBe(reason);
      expect(f.lines()).toEqual([]);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
    } finally {
      f.close();
    }
  });

  it("hands an ordinary acquisition its own promise and error", async () => {
    const value = { acquired: true };
    const fulfilled = Promise.resolve(value);
    const returned = withInitialMeshLockAcquisition("ordinary-root", () => fulfilled);
    expect(returned).toBe(fulfilled);
    await expect(returned).resolves.toBe(value);

    const failure = new Error("ordinary refusal");
    const rejected = Promise.reject(failure);
    const refused = withInitialMeshLockAcquisition("ordinary-root", () => rejected);
    const outcome = await refused.then(() => undefined, (error: unknown) => error);
    expect(refused).toBe(rejected);
    expect(outcome).toBe(failure);

    // A synchronous throw or a rejection are both compatible; the error object must stay the same.
    const thrown = new Error("synchronous preparation refusal");
    await expect((async () => withInitialMeshLockAcquisition("ordinary-root", () => { throw thrown; }))())
      .rejects.toBe(thrown);

    // A held lifetime for another root is not an eligible scope for this acquisition.
    const held = Promise.resolve("other root");
    let heldReturn: Promise<string> | undefined;
    await withInitialMeshLockAdmission("held-root", async () => {
      heldReturn = withInitialMeshLockAcquisition("ordinary-root", () => held);
      return heldReturn;
    }, { isPreAppendRefusal: () => false });
    expect(heldReturn).toBe(held);
  });
});
