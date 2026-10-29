// Test-only process boundary. Imports the REAL candidate; never implements a guard.
// No history/journal/identity/generation/reclaim operation is imported or authorized.
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { createJiti } from "jiti";

type Guard = { assertCurrent(): void; close(): void };
type Acquire = (file: string) => Promise<Guard>;
const send = (value: unknown): void => { process.stdout.write(`${JSON.stringify(value)}\n`); };
const file = process.argv[2]!;
const sandbox = process.argv[3] === "deny-ofd";
const target = fileURLToPath(new URL("../../src/recovery/exclude-writer.ts", import.meta.url));
let acquire: Acquire;
try {
  const module = await createJiti(import.meta.url, { fsCache: false }).import(target) as Record<string, unknown>;
  if (typeof module.acquirePortableVolumeGuard !== "function") throw new Error("required acquirePortableVolumeGuard export absent");
  acquire = module.acquirePortableVolumeGuard as Acquire;
} catch (error) {
  send({ event: "IMPORT_RED", target, error: String(error) });
  process.exit(1);
}

let sandboxProof: unknown;
if (sandbox) {
  const result = spawnSync("/usr/bin/python3", ["-I", "-B",
    fileURLToPath(new URL("./portable-ofd-probe.py", import.meta.url)), "try-ofd", file],
  { encoding: "utf8", timeout: 5_000, env: process.env });
  if (result.error || result.status !== 0) {
    send({ event: "FIXTURE_ERROR", error: result.error?.message ?? result.stderr });
    process.exit(2);
  }
  sandboxProof = JSON.parse(result.stdout) as unknown;
}

let guard: Guard;
try {
  guard = await acquire(file);
  guard.assertCurrent();
} catch (error) {
  send({ event: "REFUSED", error: String(error), sandboxProof,
    pid: process.pid, pidNamespace: fs.readlinkSync("/proc/self/ns/pid") });
  process.exit(3);
}
const stat = fs.statSync(file, { bigint: true });
// Normal readiness/control is pipe-driven. This single deadline is only an orphan
// safety net if the test or supervisor disappears, not a polling/renewal loop.
const deadline = setTimeout(() => { guard.close(); process.exit(4); }, 25_000);
// Kernel graceful-exit fixture ONLY. This does not run Main, extension cleanup,
// cancel/settlement persistence, a transport lease, or the A5 stop path (A12a).
process.once("SIGTERM", () => { clearTimeout(deadline); guard.close(); process.exit(0); });
const input = createInterface({ input: process.stdin });
input.on("line", command => {
  if (command !== "CLOSE") { send({ event: "FIXTURE_ERROR", error: "unknown worker control" }); return; }
  guard.close();
  let assertRefused = false;
  try { guard.assertCurrent(); } catch { assertRefused = true; }
  send({ event: "CLOSED", assertRefused });
});
input.on("close", () => { clearTimeout(deadline); guard.close(); });
// READY follows installation of every control/signal handler: no timing sleep.
send({ event: "READY", pid: process.pid, dev: String(stat.dev), ino: String(stat.ino),
  pidNamespace: fs.readlinkSync("/proc/self/ns/pid"), sandboxProof });
