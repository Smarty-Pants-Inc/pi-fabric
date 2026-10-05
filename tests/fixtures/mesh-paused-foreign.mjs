import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
const { MeshStore } = await createJiti(import.meta.url).import("../../src/mesh/store.ts");
const [root, protocol, phase] = process.argv.slice(2);
const lock = path.join(root, ".lock");
const nativeReadlink = fs.readlinkSync.bind(fs);
const actualNamespace = nativeReadlink("/proc/self/ns/pid");
// Explicit synthetic identity regression, NOT a real PID-namespace proof.
fs.readlinkSync = (file, ...args) => String(file) === "/proc/self/ns/pid"
  ? (actualNamespace === "pid:[1]" ? "pid:[2]" : "pid:[1]") : nativeReadlink(file, ...args);
const now = Date.now.bind(Date);
Date.now = () => now() - 121_000;
const write = fs.writeFileSync.bind(fs);
const rename = fs.renameSync.bind(fs);
const pause = (name) => {
  write(path.join(root, `${name}.ready`), "");
  const deadline = now() + 15_000;
  while (!fs.existsSync(path.join(root, `${name}.go`))) {
    if (now() >= deadline) throw new Error(`Timed out awaiting ${name}`);
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
};
let armed = true;
fs.renameSync = (from, to) => {
  if (armed && phase === "staged" && String(to) === lock) {
    armed = false;
    const owner = path.join(String(from), "owner");
    const old = new Date(now() - 121_000);
    fs.utimesSync(owner, old, old);
    pause("before");
    const result = rename(from, to);
    pause("after"); // stop on the actual publication boundary, before validation
    return result;
  }
  if (armed && phase === "state" && String(to) === path.join(root, "state.json")) {
    armed = false;
    const old = new Date(now() - 121_000);
    fs.utimesSync(path.join(lock, "owner"), old, old);
    pause("after"); // snapshot encoded, immediately before its canonical commit
  }
  return rename(from, to);
};
if (phase === "event") {
  const append = fs.appendFileSync.bind(fs);
  fs.appendFileSync = (file, ...args) => {
    if (armed && String(file) === path.join(root, "events.jsonl")) {
      armed = false;
      const old = new Date(now() - 121_000);
      fs.utimesSync(path.join(lock, "owner"), old, old);
      pause("after"); // sequence allocated and archive staged, before live append
    }
    return append(file, ...args);
  };
}
const store = new MeshStore(root, 65536, 100, { lockProtocol: Number(protocol), lockTimeoutMs: 1000 });
const identity = { id: "holder", name: "holder", kind: "main" };
if (phase === "event") await store.publish({ topic: "probe.resume", kind: "probe", from: identity, data: "resumed" });
else await store.put({ key: "state/holder", value: "resumed", identity });
console.log(JSON.stringify({ committed: true, phase, protocol }));
