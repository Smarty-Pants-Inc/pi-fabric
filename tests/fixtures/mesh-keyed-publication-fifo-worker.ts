import fs from "node:fs";
import path from "node:path";
import { createInterface } from "node:readline";
import { MeshStore } from "../../src/mesh/store.js";
import { MeshLock } from "../../src/mesh/mesh-lock.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../../src/topology/types.js";

const [role, root, identityId, countText, maxReceiptsText] = process.argv.slice(2);
if (!role || !root) throw new Error("worker requires role and mesh root");
const count = Number(countText ?? 0);
const maxReceipts = Number(maxReceiptsText ?? 100_000);
const lockTimeoutMs = 30_000;
const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
const nextCommand = (): Promise<string> => new Promise(resolve => input.once("line", resolve));
const emit = (value: Record<string, unknown>): void => { fs.writeSync(1, `${JSON.stringify({ pid: process.pid, ...value })}\n`); };
const receiptDirectory = path.join(root, "event-receipts");
const receiptReads: number[] = [];
let receiptReadsUnderOwnLock = 0;

// The keyed path's names-only capacity observation must stay outside this process's canonical
// lock. Keep the real filesystem call and record the live owner at each receipt-directory read.
const runtimeFs = fs as unknown as { readdirSync: (...args: unknown[]) => unknown };
const originalReaddir = fs.readdirSync;
runtimeFs.readdirSync = (target: unknown, ...args: unknown[]): unknown => {
  const result = Reflect.apply(originalReaddir, fs, [target, ...args]) as unknown;
  if (typeof target === "string" && path.resolve(target) === path.resolve(receiptDirectory)) {
    const entries = Array.isArray(result) ? result.length : -1;
    receiptReads.push(entries);
    try {
      const owner = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1];
      if (owner === String(process.pid)) receiptReadsUnderOwnLock += 1;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  return result;
};

const main = async (): Promise<void> => {
  if (role === "holder") {
    const store = new MeshLock(root, { lockTimeoutMs }, () => undefined);
    emit({ ready: true });
    const start = await nextCommand();
    if (start !== "hold") throw new Error(`holder expected hold, received ${start}`);
    let release!: () => void;
    const held = store.withLockAcrossAwait(() => new Promise<void>(resolve => {
      release = resolve;
      emit({ entered: true });
    }));
    const command = await nextCommand();
    if (command !== "release") throw new Error(`holder expected release, received ${command}`);
    release();
    await held;
    emit({ released: true });
    return;
  }

  const mesh = new MeshStore(root, 65_536, 1_000, { lockTimeoutMs, maxDedupeReceipts: maxReceipts });
  if (role === "publisher") {
    const writer = identityId!;
    emit({ ready: true });
    const command = await nextCommand();
    if (command !== "go") throw new Error(`publisher expected go, received ${command}`);
    const first = [];
    for (let index = 0; index < count; index += 1) {
      first.push(await mesh.publish({ topic: "mesh.keyed-publication-fifo", kind: "message",
        from: { id: `session:${writer}`, name: writer, kind: "main" },
        dedupeKey: `fifo:${writer}:${index}`, text: `${writer}:${index}`, data: { writer, index } }));
    }
    const replays = [];
    for (let index = 0; index < count; index += 1) {
      replays.push(await mesh.publish({ topic: "mesh.keyed-publication-fifo", kind: "message",
        from: { id: `session:${writer}`, name: writer, kind: "main" },
        dedupeKey: `fifo:${writer}:${index}`, text: `${writer}:${index}`, data: { writer, index } }));
    }
    emit({ done: true, writer, first, replays, receiptReads, receiptReadsUnderOwnLock });
    mesh.closeState();
    return;
  }

  if (role === "participant") {
    const id = identityId!;
    const identity = { id, name: "fifo participant", kind: "main" as const, sessionId: id.slice("session:".length) };
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: id, rootId: id, identity,
      heartbeatMs: 1_000, leaseMs: 5_000, random: () => 0.5, reapDeadHosts: false,
    });
    let sourceCalls = 0;
    const startedAt = Date.now();
    directory.registerSource((): FabricParticipantRecord[] => {
      sourceCalls += 1;
      const now = Date.now();
      return [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
        ownerIncarnation: directory.ownerIncarnation, name: identity.name, status: "idle",
        runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
        sessionId: identity.sessionId, cwd: process.cwd(), startedAt, updatedAt: now, controlProtocol: "v1" }];
    });
    emit({ ready: true });
    const command = await nextCommand();
    if (command !== "start") throw new Error(`participant expected start, received ${command}`);
    await directory.start();
    emit({ registered: true, sourceCalls });
    const stop = await nextCommand();
    if (stop !== "stop") throw new Error(`participant expected stop, received ${stop}`);
    await directory.close();
    mesh.closeState();
    emit({ stopped: true, sourceCalls, receiptReads, receiptReadsUnderOwnLock });
    return;
  }

  mesh.closeState();
  throw new Error(`unknown worker role: ${role}`);
};

main().catch(error => {
  emit({ fatal: error instanceof Error ? error.stack ?? error.message : String(error) });
  process.exitCode = 1;
}).finally(() => {
  runtimeFs.readdirSync = originalReaddir as unknown as (...args: unknown[]) => unknown;
  input.close();
});
