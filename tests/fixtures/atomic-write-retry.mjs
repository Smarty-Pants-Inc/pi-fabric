// A genuinely new process: no imported module state or remembered directory receipts.
import fs from "node:fs";
import { pathToFileURL } from "node:url";
const [modulePath, target, failingPath] = process.argv.slice(2);
const { writeFileAtomic } = await import(pathToFileURL(modulePath).href);
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
