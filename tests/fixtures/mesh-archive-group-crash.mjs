// smarty-dev#8305 option D (scope cut): the archived publish keeps the strict v1 protocol under
// `.lock` and makes exactly one segment fdatasync before the live append; unkeyed index,
// PENDING, HEAD and digest are plain advisory writes. Keyed index fences stay durable. The file name is historical (the off-lock group commit
// went to a follow-up issue). This publisher stops at one boundary that a removed metadata sync
// used to guard, so the parent can SIGKILL it there or release it. No production seam: the
// fixture intercepts fs calls only.
//   after-pending:  PENDING.json is in place; before the first write of the event's archive line.
//   after-datasync: the archive segment's data sync returned; before the index, the live append
//                   and the commit (HEAD).
//   after-live:     the live append returned; before the archive commit (PENDING removal, HEAD).
// One IPC receipt per stop: a `{"paused":{...}}` JSON line on stdout. The process then blocks in a
// read of stdin until the parent writes a byte (release) or kills it. A final `{"done":{...}}`
// line reports the result.
// argv: <mesh root> <archive dir> <packet json, an array means publishBatch> <phase>
import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";

const [root, archiveDir, packetJson, phase] = process.argv.slice(2);
if (!["after-pending", "after-datasync", "after-live"].includes(phase)) throw new Error(`unknown pause phase: ${phase}`);
const archiveReal = fs.realpathSync(archiveDir);
const liveReal = path.join(fs.realpathSync(root), "events.jsonl");
const fdPath = (fd) => { try { return fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return ""; } };
const lockOwnerPid = () => {
  try { return Number(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1]); } catch { return undefined; }
};
const isSegment = (target) => target.startsWith(`${archiveReal}/`) && target.endsWith(".jsonl");
const readText = (file) => { try { return fs.readFileSync(file, "utf8"); } catch { return undefined; } };
const emit = (record) => fs.writeSync(1, `${JSON.stringify(record)}\n`);

let paused = false;
const pause = (call, target) => {
  paused = true;
  emit({ paused: {
    pid: process.pid, phase, call, target: path.relative(archiveReal, target) || target,
    lockHeldBySelf: lockOwnerPid() === process.pid,
    pending: readText(path.join(archiveReal, "PENDING.json")) ?? null,
    head: readText(path.join(archiveReal, "HEAD.json")) ?? null,
    segmentLines: isSegment(target) ? (readText(target) ?? "").split("\n").filter(Boolean) : [],
  } });
  const byte = Buffer.alloc(1);
  for (;;) {
    try { if (fs.readSync(0, byte, 0, 1, null) === 0) process.exit(3); break; }
    catch (error) { if (error?.code !== "EAGAIN") throw error; Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5); }
  }
};

for (const name of ["fsyncSync", "fdatasyncSync"]) {
  const original = fs[name].bind(fs);
  fs[name] = (fd) => {
    const result = original(fd);
    const target = fdPath(fd);
    if (!paused && phase === "after-datasync" && isSegment(target)) pause(name, target);
    return result;
  };
}

const writeSync = fs.writeSync.bind(fs);
fs.writeSync = (fd, ...rest) => {
  if (fd > 2 && !paused && phase === "after-pending") {
    const target = fdPath(fd);
    if (isSegment(target) && fs.existsSync(path.join(archiveReal, "PENDING.json"))) pause("writeSync", target);
  }
  const result = writeSync(fd, ...rest);
  if (fd > 2 && !paused && phase === "after-live" && fdPath(fd) === liveReal) pause("writeSync", liveReal);
  return result;
};
const appendFileSync = fs.appendFileSync.bind(fs);
fs.appendFileSync = (file, ...rest) => {
  const result = appendFileSync(file, ...rest);
  const target = typeof file === "number" ? fdPath(file) : typeof file === "string" && path.resolve(file) === path.resolve(root, "events.jsonl") ? liveReal : "";
  if (!paused && phase === "after-live" && target === liveReal) pause("appendFileSync", liveReal);
  return result;
};

const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const store = new MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 10_000, staleLockMs: 100 });
try {
  const packet = JSON.parse(packetJson);
  const result = Array.isArray(packet) ? await store.publishBatch(packet) : await store.publish(packet);
  emit({ done: { paused, result } });
} catch (error) {
  emit({ done: { paused, error: String(error?.stack ?? error) } });
  process.exitCode = 2;
}
