import { spawn, spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainDrainControl, type MainDrainActors, type MainDrainHost } from "../src/main-drain/control.js";
import { clearBinding, procStartTime } from "../src/main-drain/binding.js";
import { assertMainInboxOpen, reopenMainInbox } from "../src/main-drain/gate.js";
import { releaseNativeInput } from "../src/main-drain/native-port.js";
import { MainDrainServer } from "../src/main-drain/server.js";

/** D3 drain seam fixtures (smarty-dev#15310): a synthetic sealed RUN, a synthetic mesh and a live drain channel. */
export const SID = "00000000-0000-4000-8000-00000000d3d1";
export const ROOT = `session:${SID}`;
const sha = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");

export interface FakeNative {
  fenceApi: boolean;
  fenced: boolean;
  busy: boolean;
  queued: number;
  admissions: number;
  idle: boolean;
  releases: number;
  shutdown: ReturnType<typeof vi.fn>;
}
export const fakeNative = (fenceApi: boolean): { pi: ExtensionAPI; ctx: () => ExtensionContext; state: FakeNative } => {
  const state: FakeNative = { fenceApi, fenced: false, busy: false, queued: 0, admissions: 0, idle: true, releases: 0, shutdown: vi.fn() };
  const pi = { hostCapabilities: { inputAdmission: 1, ...(fenceApi ? { extensionInputFence: 1 } : {}) } } as unknown as ExtensionAPI;
  // A fresh ctx object per call, as Pi hands each handler; the native session state is shared (a reload keeps the fence).
  const ctx = (): ExtensionContext => ({
    isIdle: () => state.idle,
    isPromptPending: () => state.admissions > 0,
    hasPendingMessages: () => state.queued + state.admissions > 0,
    shutdown: state.shutdown,
    ...(fenceApi ? {
      fenceInputs: async () => {
        if (state.fenced) throw Object.assign(new Error("fenced"), { code: "INPUT_ADMISSION_FENCED" });
        if (state.busy || state.queued || state.admissions) throw Object.assign(new Error("busy"), { code: "INPUT_ADMISSION_BUSY" });
        state.fenced = true;
        let released = false;
        return { release: () => { if (!released) { released = true; state.fenced = false; state.releases++; } } };
      },
      getInputState: () => ({ inputsFenced: state.fenced, inputAdmissionCount: state.admissions, queued: state.queued,
        drained: !state.busy && state.queued === 0 && state.admissions === 0 }),
    } : {}),
  }) as unknown as ExtensionContext;
  return { pi, ctx, state };
};

export interface FakeActor { id: string; name: string; status: string; residency?: string; quiescence: string[]; worker: null | undefined | { runId: string; pid: number; startTime: string | undefined } }
export const fakeActors = (rows: FakeActor[]): MainDrainActors & { stops: string[] } => {
  const stops: string[] = [];
  return {
    stops,
    listOwned: () => rows.map(row => ({ id: row.id, name: row.name, status: row.status, rootId: ROOT, ...(row.residency ? { residency: row.residency } : {}) })),
    drainQuiescence: id => rows.find(row => row.id === id)!.quiescence,
    drainWorker: id => rows.find(row => row.id === id)!.worker,
    stop: async id => { stops.push(id); const row = rows.find(entry => entry.id === id)!; row.status = "stopped"; return {}; },
  };
};

export interface Seat {
  base: string; runDir: string; meshRoot: string; run: string; runSha256: string; operationId: string;
  request: (phase: string, inputs?: Record<string, unknown>) => string;
  host: MainDrainHost; native: ReturnType<typeof fakeNative>; fabric: { held: number; handed: number; halted: boolean };
  actors: ReturnType<typeof fakeActors>; agents: Array<{ id: string; status: string }>;
  server: MainDrainServer; control: MainDrainControl; meshSnapshot: () => string;
  writeJournal: (items: Array<{ id: string; handed?: true }>) => void; writeActors: (rows: Array<Record<string, unknown>>, scope?: string) => void;
  lockFd: () => number; close: () => Promise<void>;
}

const writeSealed = (file: string, value: unknown): Buffer => {
  const bytes = Buffer.from(JSON.stringify(value));
  fs.writeFileSync(file, bytes, { mode: 0o400 });
  fs.chmodSync(file, 0o400);
  return bytes;
};

export const createSeat = async (options: { fenceApi?: boolean; start?: boolean;
  kube?: { kubeconfig: string; server: string; caSha256: string } } = {}): Promise<Seat> => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "seat-drain-")));
  fs.chmodSync(base, 0o700);
  const runDir = path.join(base, "run");
  fs.mkdirSync(runDir, { mode: 0o700 });
  const project = path.join(base, "project");
  const meshRoot = path.join(project, ".pi", "fabric", "mesh");
  fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
  const journal = path.join(base, "agent", "sessions", "--group--", `2026-10-25T00-00-00-000Z_${SID}.jsonl`);
  fs.mkdirSync(path.dirname(journal), { recursive: true, mode: 0o700 });
  fs.writeFileSync(journal, `${JSON.stringify({ type: "session", version: 3, id: SID, timestamp: "2026-10-25T00:00:00.000Z", cwd: project })}\n`, { mode: 0o600 });
  const run = randomBytes(16).toString("hex");
  const operationId = randomBytes(16).toString("hex");
  const runBytes = writeSealed(path.join(runDir, "seat-run.json"), {
    schema: "d3-seat-run-v1", run, org: "smarty-pants-inc", principal: "d3synth", instance: "org-d3synth", sessionId: SID,
    plannedSeats: ["alpha", "bravo", "charlie"], context: "synthetic", namespace: "org-synthetic", namespaceUID: "00000000-0000-4000-8000-000000000001",
    natsSecret: "org-d3synth-nats", source: { journal, journalSha256: "0".repeat(64), journalBytes: 1, leafId: "leaf", handoffs: null },
    target: { claim: "synthetic-claim", node: "synthetic-node" },
    seams: { drain: { owner: "fabric-v2", path: "/synthetic/drain", sha256: "1".repeat(64) },
      fence: { owner: "synthetic-fence-owner", path: "/synthetic/fence", sha256: "2".repeat(64) } },
    recoveryRoot: "/synthetic/recovery", createdAt: "2026-10-25T00:00:00Z",
  });
  const runSha256 = sha(runBytes);
  const inputBytes = writeSealed(path.join(runDir, "seat-inputs.json"), {
    schema: "d3-seat-inputs-v1", run, runSha256, feed: null, restartGoRef: "synthetic", routeTool: null, relaunchTools: null, inputs: null,
    profile: "rehearsal", seat: { uid: process.getuid!(), home: os.userInfo().homedir }, rehearsal: null, kube: options.kube ?? null,
  });
  fs.writeFileSync(path.join(runDir, "seat-journal.jsonl"), [
    { event: "begin", run, runSha256 }, { event: "inputs", sha256: sha(inputBytes) }, { event: "source", sha256: "2".repeat(64) },
  ].map(record => `${JSON.stringify(record)}\n`).join(""), { mode: 0o600 });
  fs.writeFileSync(path.join(runDir, "seat.lock"), "", { mode: 0o600 });
  fs.chmodSync(path.join(runDir, "seat.lock"), 0o600);

  // The live Main's inbox owner record: the seam checks the server's SO_PEERCRED pid and start time against it.
  fs.mkdirSync(path.join(meshRoot, "main-followups"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(meshRoot, "main-followups", `${SID}.owner.json`), JSON.stringify({ rootId: ROOT, sessionId: SID,
    ownerIdentityId: ROOT, pid: process.pid, host: os.hostname(), processStartedAt: procStartTime(process.pid) }), { mode: 0o600 });
  const native = fakeNative(options.fenceApi ?? true);
  const fabric = { held: 0, handed: 0, halted: false };
  const actors = fakeActors([]);
  const agents: Array<{ id: string; status: string }> = [];
  const completionsPending = { count: 0 };
  const host: MainDrainHost = {
    rootId: ROOT, sessionId: SID, meshRoot, pi: native.pi, context: native.ctx,
    main: () => ({
      drainSnapshot: () => ({ ...fabric, heldIds: Array.from({ length: fabric.held }, (_, index) => `mem-held-${index}`),
        handedIds: Array.from({ length: fabric.handed }, (_, index) => `mem-handed-${index}`), journalOpen: true, reloading: false }),
      probeAdmission: () => { try { assertMainInboxOpen(ROOT); return true; } catch { return false; } },
    }),
    actors: () => actors, agents: () => ({ list: () => agents }), completions: () => completionsPending.count,
  };
  const control = new MainDrainControl(host);
  const server = new MainDrainServer(control);
  if (options.start !== false) {
    const started = await server.start();
    if (started !== "listening") throw new Error(`drain channel did not start: ${started}`);
  }
  let heldLock: number | undefined;
  const followUps = path.join(meshRoot, "main-followups");
  return {
    base, runDir, meshRoot, run, runSha256, operationId, host, native, fabric, actors, agents, server, control,
    // A's shared request schema; contract rev 4 puts the run's one operationId in inputs.
    request: (phase, inputs = {}) => JSON.stringify({ schema: "d3-seam-request-v1", run, runSha256, phase, principal: "d3synth", sessionId: SID,
      inputs: { operationId, ...inputs } }),
    meshSnapshot: () => snapshot(meshRoot),
    writeJournal: items => {
      fs.mkdirSync(followUps, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(followUps, `${SID}.json`), JSON.stringify({ version: 1, items: items.map((item, index) => ({
        id: item.id, from: { id: "session:00000000-0000-4000-8000-0000000000aa", kind: "main", name: "peer" }, message: "synthetic",
        sentAt: 1_000 + index, ...(item.handed ? { handed: true } : {}) })) }), { mode: 0o600 });
    },
    writeActors: (rows, scope = SID) => {
      const directory = path.join(meshRoot, "actors", scope);
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(directory, "actors.json"), JSON.stringify({ format: 1, actors: rows }), { mode: 0o600 });
    },
    /** A real exclusive flock held by THIS process's open file description (a child takes it on the shared OFD). */
    lockFd: () => {
      if (heldLock !== undefined) return heldLock;
      heldLock = fs.openSync(path.join(runDir, "seat.lock"), fs.constants.O_RDWR);
      const taken = spawnSync("python3", ["-c", "import fcntl; fcntl.flock(3, fcntl.LOCK_EX | fcntl.LOCK_NB)"], { stdio: ["ignore", "ignore", "pipe", heldLock] });
      if (taken.status !== 0) throw new Error("could not take the synthetic seat lock");
      return heldLock;
    },
    close: async () => {
      await server.close();
      if (heldLock !== undefined) fs.closeSync(heldLock);
      releaseNativeInput(ROOT); reopenMainInbox(ROOT); clearBinding(ROOT);
      // The drain slot is process-global by design (it outlives a reload): drop it between tests.
      ((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.main.drain-slot.v1")] as Map<string, unknown> | undefined)?.delete(ROOT);
      fs.chmodSync(path.join(runDir, "seat-run.json"), 0o600);
      fs.rmSync(base, { recursive: true, force: true });
    },
  };
};

/** Every path, type, size and content hash under the mesh root (the drain channel lives outside it). */
const snapshot = (root: string): string => {
  const lines: string[] = [];
  const walk = (directory: string): void => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      if (stat.isDirectory()) { lines.push(`d ${path.relative(root, file)}`); walk(file); }
      else lines.push(`f ${path.relative(root, file)} ${stat.size} ${sha(fs.readFileSync(file))}`);
    }
  };
  walk(root);
  return lines.join("\n");
};

let bundled: string | undefined;
/** The REAL seam artifact: the same esbuild bundle scripts/build.mjs makes, installed with its interpreter's shebang. */
export const installedSeam = async (directory: string): Promise<string> => {
  if (!bundled) {
    const { build } = await import("esbuild");
    bundled = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "seat-drain-bundle-")), "fabric-seat-drain.cjs");
    await build({ entryPoints: ["src/seat-drain/entry.ts"], outfile: bundled, bundle: true, platform: "node", format: "cjs", target: "node24", logLevel: "silent" });
  }
  const out = path.join(directory, "fabric-seat-drain");
  const node = spawnSync("node", [bundled, "--install", "--out", out], { encoding: "utf8" });
  if (node.status !== 0) throw new Error(`install failed: ${node.stderr}`);
  return out;
};

/** Run the installed seam the way A does (tests/fixtures/seat-drain-harness.py), without blocking this event loop. */
export const runSeam = (seat: Seat, exe: string, phase: string, request: string, nolock = false):
  Promise<{ code: number | null; stdout: string; stderr: string }> => new Promise((resolve, reject) => {
  const child = spawn("python3", [path.join("tests", "fixtures", "seat-drain-harness.py"), seat.runDir, exe, phase, ...(nolock ? ["nolock"] : [])],
    { stdio: ["pipe", "pipe", "pipe"] });
  let out = "";
  let err = "";
  child.stdout.on("data", chunk => { out += chunk; });
  child.stderr.on("data", chunk => { err += chunk; });
  child.on("error", reject);
  child.on("close", () => {
    try { resolve(JSON.parse(out) as { code: number | null; stdout: string; stderr: string }); }
    catch { reject(new Error(`harness failed: ${err}`)); }
  });
  child.stdin.end(request);
});
