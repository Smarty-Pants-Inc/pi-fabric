import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { importMeshStateToNats, type MeshNatsMigrationOptions } from "../src/mesh/backend-migration-nats.js";
import { MeshBackendRefusedError, meshSnapshotDigest, readMeshStateNatsMarker } from "../src/mesh/backend-migration.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";

const identity: MeshIdentity = { id: "retry-test", name: "retry-test", kind: "agent" };
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-nats-publish-retry-")); roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "retry/key", value: { exact: "bytes" }, identity }); store.closeState();
  const server = fakeJetStream();
  return { root, server, options: { port: server.port() } satisfies MeshNatsMigrationOptions };
};
const transient = () => Object.assign(new Error("lost acknowledgement"), { transient: true });
// These port-boundary errors are already classified; real SDK cause-chain controls live elsewhere.
const noResponders = () => Object.assign(new Error("no responders for migration publish"), { name: "NoResponders", transient: true });
const interceptPublishBackoff = (onBackoff: (resume: () => void) => void) => {
  const actualSetTimeout = globalThis.setTimeout;
  return vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
    if (delay !== 100) return actualSetTimeout(callback, delay, ...args);
    onBackoff(() => callback(...args));
    // The migration's awaited backoff never inspects or cancels its timer handle.
    return {} as ReturnType<typeof setTimeout>;
  }) as typeof setTimeout);
};

describe("migration-only NATS publish retries", () => {
  it("reconciles a commit whose acknowledgement was lost and commits exactly once", async () => {
    const { root, server, options } = await setup();
    let lost = false;
    server.beforePublish = () => { if (!lost) { lost = true; return "lose-ack"; } };
    const result = await importMeshStateToNats(root, options);
    expect(result.converged).toBe(false);
    expect(server.messages).toHaveLength(3); // begin, one batch, commit; no duplicate commit
    expect(server.messages[0]?.msgID).toBe(`${result.stream}-import-begin-1`);
  });

  it("retries an unknown but uncommitted publish with identical bytes, id and CAS", async () => {
    const { root, server, options } = await setup();
    const underlying = options.port!;
    const attempts: { bytes: Buffer; id: string; cas: number }[] = [];
    let failOnce = true;
    const port = { ...underlying, publish: async (subject: string, data: Uint8Array, args: { expectLastSequence: number; msgID: string }) => {
      attempts.push({ bytes: Buffer.from(data), id: args.msgID, cas: args.expectLastSequence });
      if (failOnce) { failOnce = false; throw transient(); }
      return underlying.publish(subject, data, args);
    } };
    await importMeshStateToNats(root, { port });
    expect(attempts.length).toBeGreaterThan(1);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(server.messages).toHaveLength(3);
  });

  it.each(["id", "bytes"] as const)("refuses ambiguous stored publish with mismatched %s", async mismatch => {
    const { root, server, options } = await setup();
    let corrupted = false;
    server.beforePublish = () => { if (!corrupted) { corrupted = true; return "lose-ack"; } };
    server.afterStore = seq => {
      const message = server.messages[seq - 1]!;
      if (corrupted && seq === 1) {
        if (mismatch === "id") message.msgID = "foreign-id";
        else message.data = Buffer.from("foreign-bytes");
      }
    };
    await expect(importMeshStateToNats(root, options)).rejects.toBeInstanceOf(MeshBackendRefusedError);
    expect(server.messages).toHaveLength(1);
  });

  it("bounds transient publish attempts at three", async () => {
    const { root, options } = await setup();
    const underlying = options.port!;
    let attempts = 0;
    const port = { ...underlying, publish: async () => { attempts++; throw transient(); } };
    await expect(importMeshStateToNats(root, { port })).rejects.toThrow("lost acknowledgement");
    expect(attempts).toBe(3);
  });

  // Keep: smarty-dev#13276's real FIRST-import NoResponders catch (not its seq-1 waiter).
  // This guards data integrity and bounded retry, not a performance threshold or cluster claim.
  it("paces the first import until delayed JetStream interest arrives (#13276)", async () => {
    const { root, server, options } = await setup(); // Complete setup before spying on timers.
    const statePath = path.join(root, "state.json");
    const source = fs.readFileSync(statePath);
    const digest = meshSnapshotDigest(decodeMeshStateFile(statePath, 64 * 1024, false));
    const underlying = options.port!;
    const attempts: { subject: string; bytes: Buffer; id: string; cas: number }[] = [];
    let interested = false;
    const port = { ...underlying, publish: async (subject: string, data: Uint8Array, args: { expectLastSequence: number; msgID: string }) => {
      attempts.push({ subject, bytes: Buffer.from(data), id: args.msgID, cas: args.expectLastSequence });
      if (!interested) throw noResponders();
      return underlying.publish(subject, data, args);
    } };
    let enterBackoff!: (value: "backoff-entered") => void;
    const backoffEntered = new Promise<"backoff-entered">(resolve => { enterBackoff = resolve; });
    let release = () => {};
    let cleaningUp = false;
    let backoffs = 0;
    interceptPublishBackoff(resume => {
      backoffs++;
      if (cleaningUp) queueMicrotask(resume);
      else { release = resume; enterBackoff("backoff-entered"); }
    });
    const importing = importMeshStateToNats(root, { port });
    // Attach both handlers immediately: the expected RED rejection must not be unhandled.
    const settledImport = importing.then(() => "import-settled" as const, () => "import-settled" as const);
    try {
      expect(await Promise.race([backoffEntered, settledImport])).toBe("backoff-entered");
      expect(backoffs).toBe(1);
      expect(attempts).toHaveLength(1);
      expect(fs.readFileSync(statePath)).toEqual(source);
      expect(readMeshStateNatsMarker(root)).toBeUndefined();
      expect(server.messages).toHaveLength(0);
      interested = true;
      release();
      const result = await importing; // The SAME import, not an operator rerun.
      expect(result).toMatchObject({ digest, entries: 1, tombstones: 0, converged: false });
      expect(backoffs).toBe(1);
      expect(attempts).toHaveLength(4); // refused begin, identical begin, batch, commit
      expect(attempts[1]).toEqual(attempts[0]);
      expect(server.messages.map(message => JSON.parse(Buffer.from(message.data).toString("utf8")).type))
        .toEqual(["import-begin", "batch", "import-commit"]);
      expect(server.messages.map(message => message.seq)).toEqual([1, 2, 3]);
      expect(readMeshStateNatsMarker(root)).toMatchObject({ natsStream: result.stream, epoch: 1 });
      const copy = path.join(root, "state.json.nats-cutover-1");
      expect(fs.readFileSync(copy)).toEqual(source);
      expect(meshSnapshotDigest(decodeMeshStateFile(copy, 64 * 1024, false))).toBe(digest);
    } finally {
      cleaningUp = true;
      interested = true;
      release();
      await settledImport; // Release even on RED/assertion failure before restoring mocks/removing root.
    }
  });

  it("bounds perpetual NoResponders at three identical attempts without changing source data", async () => {
    const { root, server, options } = await setup();
    const statePath = path.join(root, "state.json");
    const source = fs.readFileSync(statePath);
    const error = noResponders();
    const underlying = options.port!;
    const attempts: { bytes: Buffer; id: string; cas: number }[] = [];
    let backoffs = 0;
    interceptPublishBackoff(resume => { backoffs++; queueMicrotask(resume); });
    const port = { ...underlying, publish: async (_subject: string, data: Uint8Array, args: { expectLastSequence: number; msgID: string }) => {
      attempts.push({ bytes: Buffer.from(data), id: args.msgID, cas: args.expectLastSequence });
      throw error;
    } };
    await expect(importMeshStateToNats(root, { port })).rejects.toBe(error);
    expect(attempts).toHaveLength(3);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(attempts[2]).toEqual(attempts[0]);
    expect(backoffs).toBe(2); // Never delay after the final refusal.
    expect(server.messages).toHaveLength(0);
    expect(fs.readFileSync(statePath)).toEqual(source);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fs.existsSync(path.join(root, "state.json.nats-cutover-1"))).toBe(false);
  });

  it.each([
    ["disabled JetStream (10039)", Object.assign(new Error("JetStream not enabled for account"), { code: 503, err_code: 10039 })],
    ["permanent untagged refusal", new Error("publish permission denied")],
  ])("does not retry or delay %s", async (_label, error) => {
    const { root, server, options } = await setup();
    const statePath = path.join(root, "state.json");
    const source = fs.readFileSync(statePath);
    let attempts = 0;
    let backoffs = 0;
    interceptPublishBackoff(resume => { backoffs++; queueMicrotask(resume); });
    const port = { ...options.port!, publish: async () => { attempts++; throw error; } };
    await expect(importMeshStateToNats(root, { port })).rejects.toBe(error);
    expect(attempts).toBe(1);
    expect(backoffs).toBe(0);
    expect(server.messages).toHaveLength(0);
    expect(fs.readFileSync(statePath)).toEqual(source);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fs.existsSync(path.join(root, "state.json.nats-cutover-1"))).toBe(false);
  });

  it("continues to refuse an unrelated CAS conflict", async () => {
    const { root, server, options } = await setup();
    let raced = false;
    server.beforePublish = () => { if (!raced) { raced = true; server.append(Buffer.from("foreign")); } };
    await expect(importMeshStateToNats(root, options)).rejects.toBeInstanceOf(MeshBackendRefusedError);
    expect(server.messages).toHaveLength(1);
  });
});
