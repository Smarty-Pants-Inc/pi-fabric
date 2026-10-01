// A genuinely new process: no imported module state or remembered directory receipts.
import fs from "node:fs";
import { buildSync } from "esbuild";
const [modulePath, target, failingPath] = process.argv.slice(2);
// Native Node strips types but cannot execute parameter properties/enums. Compile
// the source graph inside this new process, rather than depending on erasable TS
// or importing an old dist bundle. No product module state survives from the parent.
const { outputFiles } = buildSync({
  entryPoints: [modulePath], bundle: true, platform: "node", format: "esm", write: false,
});
const source = Buffer.from(outputFiles[0].contents).toString("base64");
const { writeFileAtomic } = await import(`data:text/javascript;base64,${source}`);
const open = fs.openSync.bind(fs);
const sync = fs.fsyncSync.bind(fs);
const descriptors = new Map();
let events = [];
let fail = true;
fs.openSync = (file, flags, mode) => {
  const fd = open(file, flags, mode);
  descriptors.set(fd, String(file));
  return fd;
};
fs.fsyncSync = (fd) => {
  const file = descriptors.get(fd);
  events.push(file);
  if (fail && file === failingPath) throw new Error("owed directory barrier unavailable");
  sync(fd);
};
const attempts = [];
for (const contents of ["retry", "accepted"]) {
  events = [];
  try {
    writeFileAtomic(target, contents, { durable: true });
    events.push("acknowledged");
    attempts.push({ acknowledged: true, events });
  } catch (error) {
    attempts.push({ acknowledged: false, events, error: error.message });
  }
  fail = false;
}
process.stdout.write(JSON.stringify(attempts));
