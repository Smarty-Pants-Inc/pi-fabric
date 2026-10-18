// Real isolated process. Loaded from source, never dist; no network or live mesh.
import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
const [mode, root] = process.argv.slice(2);
if (!root || !path.isAbsolute(root) || !["crash", "restart"].includes(mode ?? "")) throw new Error("invalid fixture arguments");
const jiti = createJiti(import.meta.url, { tryNative: false });
const { writeFileAtomic } = await jiti.import<typeof import("../../src/core/atomic-write.js")>(path.resolve("src/core/atomic-write.ts"));
const leaf = path.join(root, "unprepared", "leaf"), target = path.join(leaf, "entry");
const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), mkdir = fs.mkdirSync.bind(fs), rename = fs.renameSync.bind(fs);
const descriptors = new Map<number, string>();
let events: string[] = [], barriers = 0, fileUses = 0;
const processGroup = process.platform === "linux" ? (() => {
  const stat = fs.readFileSync("/proc/self/stat", "utf8");
  return Number(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[2]);
})() : undefined;
fs.openSync = ((file, flags, permissions) => {
  const fd = open(file, flags, permissions);
  descriptors.set(fd, String(file));
  if (!fs.fstatSync(fd).isDirectory()) { fileUses++; events.push(`open:${String(file)}`); }
  return fd;
}) as typeof fs.openSync;
let fail = mode === "restart";
fs.fsyncSync = fd => {
  const file = descriptors.get(fd)!;
  if (fail && file === root) { events.push(`fault:${file}`); throw new Error("owed-external-parent"); }
  sync(fd); barriers++; events.push(`fsync:${file}`);
};
fs.renameSync = (from, to) => { rename(from, to); events.push(`rename:${String(to)}`); };
if (mode === "crash") {
  fs.mkdirSync = ((directory, options) => {
    const result = mkdir(directory, options as fs.MakeDirectoryOptions & { recursive: true });
    if (String(directory) === leaf) {
      // Actual recursive mkdir returned. No production link barrier or file open has happened.
      // Trace writes are fixture evidence only; do not fsync the unprepared chain accidentally.
      fs.writeFileSync(path.join(root, "crash-cut.json"), JSON.stringify({ created: fs.statSync(leaf).isDirectory(),
        barriers, fileUses, processGroup, cut: "after-mkdir-before-link-barrier" }));
      process.kill(process.pid, "SIGKILL");
      throw new Error("SIGKILL unexpectedly returned");
    }
    return result;
  }) as typeof fs.mkdirSync;
  writeFileAtomic(target, "must not be used", { durable: true });
  throw new Error("missed crash cut");
} else {
  // Fresh module/process sees only an existing chain. First prove a real parent fsync failure
  // refuses acknowledgment; then forward ALL barriers and prove readback + before-ack ordering.
  const attempts = [];
  for (const contents of ["retry", "accepted"]) {
    events = [];
    try {
      writeFileAtomic(target, contents, { durable: true }); events.push("ack");
      attempts.push({ acknowledged: true, events });
    } catch (error) {
      attempts.push({ acknowledged: false, error: (error as Error).message, events });
    }
    fail = false;
  }
  process.stdout.write(JSON.stringify({ failed: attempts[0], accepted: attempts[1], processGroup }));
}
