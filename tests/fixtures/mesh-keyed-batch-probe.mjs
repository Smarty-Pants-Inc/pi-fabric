import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";

// Real files and real OS barriers, not a mocked durability model. Keep the recorder enabled
// before importing MeshLock so its acquired() receipt includes acquisition through release.
process.env.PI_FABRIC_LOCK_STATS = "1";
const jiti = createJiti(import.meta.url);
const { MeshStore } = await jiti.import("../../src/mesh/store.ts");
const { MeshArchive, MESH_ARCHIVE_CONFIG } = await jiti.import("../../src/mesh/archive.ts");
const { lockStats } = await jiti.import("../../src/mesh/mesh-lock.ts");
const [root, mode = "archive", size = "12"] = process.argv.slice(2);
const count = Number(size);
if (!root || !path.isAbsolute(root) || !Number.isSafeInteger(count) || count < 1 || count > 100) throw new Error("Invalid probe arguments");
fs.mkdirSync(root, { recursive: true, mode: 0o700 });
const archive = path.join(path.dirname(root), "archive");
if (mode === "archive") {
  fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
}
const store = new MeshStore(root, 4096, 1000, { maxEventLogBytes: 230 * 1024 * 1024 });
const from = { id: "session:keyed-batch-probe", name: "keyed-batch-probe", kind: "main" };
const packet = key => ({ topic: "mesh.keyed-batch-probe", from, text: key, dedupeKey: key });
await store.publishBatch([packet("warm")]);

const native = { fsyncSync: fs.fsyncSync, fdatasyncSync: fs.fdatasyncSync, readdirSync: fs.readdirSync };
const acquired = lockStats.acquired;
const samples = [], listings = new Map();
let sample;
const held = () => fs.existsSync(path.join(root, ".lock"));
for (const method of ["fsyncSync", "fdatasyncSync"]) {
  fs[method] = fd => {
    const isHeld = held();
    if (sample && isHeld) {
      sample[method]++;
      const target = fs.readlinkSync(`/proc/self/fd/${fd}`);
      if (fs.fstatSync(fd).isDirectory()) sample.directories.push(target);
      else sample.files.push(target);
    }
    return native[method](fd);
  };
}
fs.readdirSync = (...args) => {
  if (sample && held()) {
    const directory = String(args[0]);
    listings.set(directory, (listings.get(directory) ?? 0) + 1);
  }
  return native.readdirSync(...args);
};
lockStats.acquired = (...args) => {
  if (sample && args[0] === root && args[1] === "bridge") sample.holdMs = args[3];
  return acquired(...args);
};
const published = [];
try {
  for (let i = 0; i < count; i++) {
    sample = { fsyncSync: 0, fdatasyncSync: 0, holdMs: null, directories: [], files: [] };
    const [event] = await store.publishBatch([packet(`key-${i}`)]);
    published.push(event);
    samples.push(sample);
  }
} finally {
  sample = undefined;
  Object.assign(fs, native);
  lockStats.acquired = acquired;
}
const receiptDirectory = path.join(root, "event-receipts");
const receiptsMatch = published.every(event => JSON.stringify(event) === fs.readFileSync(path.join(receiptDirectory,
  createHash("sha256").update(event.dedupeKey).digest("hex") + ".json"), "utf8"));
const retries = [];
for (const event of published) retries.push((await store.publishBatch([packet(event.dedupeKey)]))[0]);
const events = store.read({ limit: 1000 });
const archiveEntries = mode === "archive" ? new MeshArchive(archive, root).readAfter(0, store.latestSequence(), () => true, 1000) : undefined;
console.log(JSON.stringify({ mode, root, archive, receiptDirectory, samples, listings: [...listings], receiptsMatch,
  retriesMatch: JSON.stringify(retries) === JSON.stringify(published), liveCount: events.length,
  uniqueIds: new Set(events.map(event => event.id)).size,
  archivedIdsMatch: archiveEntries ? JSON.stringify(archiveEntries.map(event => event.id)) === JSON.stringify(events.map(event => event.id)) : null,
  lastIndex: mode === "archive" ? JSON.parse(fs.readFileSync(path.join(archive, "sequence-index", "0", `${published.at(-1).sequence}.json`), "utf8")) : null }));
store.closeState();
