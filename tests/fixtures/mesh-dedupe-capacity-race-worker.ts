import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { MeshStore, type MeshEvent, type MeshPublishInput } from "../../src/mesh/store.js";

// Bundle this narrow fixture, not the extension entrypoint, and execute it in native Node.
const [role, root, control, writer, countText, capText, timeoutText, variantFlag, variant] = process.argv.slice(2);
assert(role && root && control && writer, "worker requires role, root, control directory and writer");
assert.equal(variantFlag, "--variant");
assert(variant === "offlock" || variant === "base-late", "unknown scheduling variant");
assert.equal(process.release.name, "node");
assert.equal(process.versions.bun, undefined, "capacity race must use native Node, not Bun");
const count = Number(countText);
const cap = Number(capText);
const lockTimeoutMs = Number(timeoutText);
assert(Number.isSafeInteger(count) && count > 0);
assert(Number.isSafeInteger(cap) && cap > count);
assert(Number.isSafeInteger(lockTimeoutMs) && lockTimeoutMs > 0);
const directory = path.join(root, "event-receipts");
const checkpoint = path.join(control, `${writer}.prefixes.jsonl`);
const ready = path.join(control, `${writer}.ready.json`);
const release = path.join(control, role === "fill" ? "fill-go" : "stale-release");
const deadline = Date.now() + 300_000;
const sleeper = new Int32Array(new SharedArrayBuffer(4));
const writeCompleted = (file: string, value: unknown): void => {
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(value), { mode: 0o600 });
  fs.renameSync(temporary, file);
};
// The off-lock callback is synchronous: IPC callbacks cannot run until it returns. The parent
// watches completed files; this bounded file check is only the worker's release safety net.
const waitForRelease = (): void => {
  while (!fs.existsSync(release)) {
    if (Date.now() >= deadline) throw new Error(`${writer}: release barrier timed out`);
    Atomics.wait(sleeper, 0, 0, 10);
  }
};
const owner = (): string | null => {
  try { return fs.readFileSync(path.join(root, ".lock", "owner"), "utf8"); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
};
const announce = (extra: Record<string, unknown>): void => writeCompleted(ready, {
  pid: process.pid, node: process.versions.node, bun: process.versions.bun ?? null,
  role, writer, count, cap, lockTimeoutMs, variant, ...extra,
});
const packet = (index: number): MeshPublishInput => ({
  topic: "mesh.dedupe-capacity-race", kind: "message",
  from: { id: `session:capacity-${writer}`, name: writer, kind: "main" },
  dedupeKey: `capacity:${writer}:${index}`, text: `${writer}:${index}`,
});

const mesh = new MeshStore(root, 4_096, 200_000, {
  stateBackend: "file", maxDedupeReceipts: cap, lockTimeoutMs, lockHeldEventMs: 0,
  maxEventLogBytes: 512 * 1024 * 1024, retainedEventLogBytes: 128 * 1024 * 1024,
});
const runtimeFs = fs as unknown as { readdirSync: (...args: unknown[]) => unknown };
const originalReaddir = fs.readdirSync;
let paused = false;
if (role !== "fill" && variant === "offlock") {
  runtimeFs.readdirSync = (target: unknown, ...args: unknown[]): unknown => {
    // Preserve the real call and the exact returned names. Stop AFTER enumeration, BEFORE
    // production can receive the observation; neither a count nor a receipt is invented.
    const names: unknown = Reflect.apply(originalReaddir, fs, [target, ...args]);
    if (!paused && typeof target === "string" && path.resolve(target) === path.resolve(directory)) {
      const canonicalOwner = owner();
      const heldByThisProcess = canonicalOwner?.split("\n")[1] === String(process.pid);
      if (!heldByThisProcess) {
        assert(Array.isArray(names));
        paused = true;
        announce({ barrier: "after-real-offlock-readdir", names: names.length,
          reserve: count, canonicalOwner });
        waitForRelease();
      }
    }
    return names;
  };
}

const recordPrefix = (events: MeshEvent[]): void => {
  // Checkpoint ONLY successfully returned events before submitting the uncommitted suffix.
  fs.appendFileSync(checkpoint, `${JSON.stringify(events)}\n`, { mode: 0o600 });
};
const recordCapacity = (committed: number): void => {
  if (role === "fill") return;
  // Bypass our scheduling wrapper for test-owned inspection: only a PRODUCTION read may
  // announce the off-lock barrier (BASE must not accidentally trap on this assertion).
  const names = originalReaddir(directory);
  assert(names.every(name => /^[a-f0-9]{64}(?:\.pending)?\.json$/.test(name)), "unexpected capacity entry");
  const sample = { committed, receiptKeys: new Set(names.map(name => name.slice(0, 64))).size,
    pending: names.filter(name => name.endsWith(".pending.json")).length };
  fs.appendFileSync(path.join(control, `${writer}.capacity.jsonl`), `${JSON.stringify(sample)}\n`, { mode: 0o600 });
};
const publishAll = async (): Promise<MeshEvent[]> => {
  const events: MeshEvent[] = [];
  while (events.length < count) {
    if (Date.now() >= deadline) throw new Error(`${writer}: publication deadline exceeded`);
    const inputs = Array.from({ length: Math.min(32, count - events.length) }, (_, index) => packet(events.length + index));
    const prefix = await mesh.publishBatch(inputs);
    assert(prefix.length > 0 && prefix.length <= inputs.length, "batch must return a nonempty bounded prefix");
    prefix.forEach((event, index) => assert.equal(event.dedupeKey, inputs[index]!.dedupeKey, "returned prefix reordered"));
    recordPrefix(prefix);
    events.push(...prefix);
    // A later suffix may repair an earlier overshoot by pruning. Preserve the actual count
    // after EVERY returned prefix, so slow disks/short prefixes cannot hide the original F1.
    recordCapacity(events.length);
  }
  return events;
};

const main = async (): Promise<void> => {
  if (role === "fill" || variant === "base-late") {
    // BASE has no off-lock enumeration. Explicit late mode does not wait for a nonexistent
    // callback: it announces immediately and invokes the SAME publication API after the fill.
    announce({ barrier: role === "fill" ? "fill-ready" : "base-late-before-publish" });
    waitForRelease();
  }
  assert(role === "fill" || role === "stale-batch" || role === "stale-single");
  let events: MeshEvent[];
  if (role === "stale-single") {
    assert.equal(count, 1);
    events = [await mesh.publish(packet(0))];
    recordPrefix(events);
    recordCapacity(events.length);
  } else events = await publishAll();
  if (role !== "fill" && variant === "offlock") assert(paused, "stale publication never reached the off-lock barrier; use explicit BASE control only on BASE");
  if (role !== "fill") {
    const replays: MeshEvent[] = [];
    for (let index = 0; index < events.length; index++) {
      const replay = await mesh.publish(packet(index));
      assert.deepEqual(replay, events[index], "exact stale-key replay changed the published event");
      replays.push(replay);
    }
    writeCompleted(path.join(control, `${writer}.replays.json`), replays);
  }
  writeCompleted(path.join(control, `${writer}.done.json`), {
    pid: process.pid, committed: events.length, paused, lockTimeoutMs, variant,
  });
};

main().catch(error => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
}).finally(() => {
  runtimeFs.readdirSync = originalReaddir as unknown as (...args: unknown[]) => unknown;
  mesh.closeState();
});
