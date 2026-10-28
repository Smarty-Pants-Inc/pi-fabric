// Real-path probe: a real ParticipantDirectory heartbeat loop, real mesh lock files held
// by this process, and real timers. Reads private persisted diagnostics with their wall times
// and prints one RATE line for tests/mesh-background-retry-log-rate.test.ts to judge.
import fs from "node:fs";
import path from "node:path";
import { isMeshLockTimeout, MeshBackgroundRetry } from "../../src/core/atomic-write.js";
import { flushFabricDiagnostics } from "../../src/core/diagnostics.js";
import { MeshStore } from "../../src/mesh/store.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import type { FabricParticipantInfo } from "../../src/topology/types.js";

const root = process.argv[2]!;
process.env.PI_CODING_AGENT_DIR = path.join(root, "agent");
process.env.SMARTY_ROLE = "worktree-agent";
delete process.env.PI_FABRIC_SESSION_ID;
process.env.PI_FABRIC_PROJECT_ROOT = root;
process.env.PI_FABRIC_MESH_ROOT = path.join(root, "mesh");
process.env.PI_FABRIC_RUN_ROOT = path.join(root, "runs");
const windowMs = Number(process.env.PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS);
const label = "participant heartbeat/change refresh";
const startedAt = Date.now();

type DiagnosticRecord = { at: string; source: string; level: string; message: string };
const lines = () => {
  const directory = path.join(root, "agent", "fabric", "diagnostics");
  const records: DiagnosticRecord[] = fs.existsSync(directory) ? fs.readdirSync(directory).filter(file => file.endsWith(".jsonl"))
    .flatMap(file => fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line))) : [];
  return records.map(record => ({ at: Date.parse(record.at) - startedAt, text: record.message,
    source: record.source, level: record.level }));
};
// Observation only: count this path's typed lock timeouts and confirmed acquisitions.
let timeouts = 0, acquisitions = 0;
const failure = MeshBackgroundRetry.prototype.failure;
MeshBackgroundRetry.prototype.failure = function(this: MeshBackgroundRetry, error: unknown) {
  if (this.label === label && isMeshLockTimeout(error)) timeouts++;
  return failure.call(this, error);
};
const success = MeshBackgroundRetry.prototype.success;
MeshBackgroundRetry.prototype.success = function(this: MeshBackgroundRetry) {
  if (this.label === label) acquisitions++;
  return success.call(this);
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const until = async (what: string, check: () => boolean, ms = 15_000) => {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}: ${JSON.stringify({ timeouts, acquisitions, lines: lines() })}`);
    await sleep(10);
  }
};
// A short acquisition deadline; the lock itself is the real directory lock file.
const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100, { lockTimeoutMs: 40 });
const lockPath = path.join(mesh.root, ".lock");
const hold = () => {
  fs.mkdirSync(lockPath);
  fs.writeFileSync(path.join(lockPath, "owner"), `probe\n${process.pid}\n${Date.now()}\n`);
};
const release = () => fs.rmSync(lockPath, { recursive: true, force: true });
const identity = { id: "session:rate-probe", name: "main", kind: "main" as const, sessionId: "rate-probe" };
const record: FabricParticipantInfo = { format: 1, id: identity.id, name: identity.id, kind: "root", rootId: identity.id,
  ownerHostId: identity.id, ownerIdentityId: identity.id, status: "idle", runner: "pi", transport: "host",
  capabilities: ["followUp", "steer", "fabric"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: true, stale: false };
const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
  heartbeatMs: 100, leaseMs: 180_000, reapDeadHosts: false });
directory.registerSource(() => [record]);
await directory.start();

const retryLines = () => lines().filter(line => line.text.startsWith(`[pi-fabric] ${label}: mesh lock timeout; retrying`));
const suppressed = () => lines().reduce((sum, line) => sum + Number(/\((\d+) similar retries suppressed/.exec(line.text)?.[1] ?? 0), 0);
let outages = 0;
// One short outage: the heartbeat times out on the held lock, then recovers with a real acquisition.
const shortOutage = async () => {
  const before = timeouts;
  hold();
  outages++;
  try { await until("a lock timeout", () => timeouts > before); }
  finally { release(); }
  const recovered = acquisitions;
  await until("a real acquisition", () => acquisitions > recovered);
};

// Phase 1: one continuous outage held across a whole window.
hold();
outages++;
try { await until("the second warning window", () => retryLines().length >= 2, 20_000); }
finally { release(); }
const afterHeld = acquisitions;
await until("recovery from the held outage", () => acquisitions > afterHeld);
// Phase 2: a flapping lock, several short outages. The old code printed one warning per outage.
for (let index = 0; index < 4; index++) await shortOutage();
// A slow host may cross a window boundary on the last outage; then that outage printed its
// own line and owes no summary. Add outages until the latest one was suppressed.
for (let extra = 0; extra < 3 && timeouts === retryLines().length + suppressed(); extra++) await shortOutage();
// The final summary arrives with the first real acquisition after the window.
await until("the final summary", () => timeouts === retryLines().length + suppressed(), windowMs + 15_000);
await directory.close();
await flushFabricDiagnostics();
console.log(`RATE ${JSON.stringify({ windowMs, pid: process.pid, outages, timeouts, acquisitions, lines: lines() })}`);
