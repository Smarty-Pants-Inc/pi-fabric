import { spawn, spawnSync } from "node:child_process";
import type net from "node:net";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/**
 * D3 seat drain seam (smarty-dev#15310): the RUN directory, the sealed packet, the seat-lock proof,
 * the mesh root and the write-once evidence. Every failure is a fixed code (SeamGap), never a path,
 * a payload or an OS message.
 */
export class SeamGap extends Error {
  constructor(readonly code: string) { super(code); this.name = "SeamGap"; }
}
const gap = (code: string): never => { throw new SeamGap(code); };

const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_SEALED = 1024 * 1024;
const MAX_JOURNAL = 64 * 1024 * 1024;
const MAX_SOURCE_HEADER = 65536;
const PYTHON = "/usr/bin/python3";
const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
export const sha256 = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");

/** No-follow read of a regular file owned by `uid`; `exactMode` and nlink 1 when sealed. */
export const readOwned = (file: string, uid: number, limit: number, sealed: { mode: number } | undefined, code: string): Buffer => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | (fs.constants.O_NONBLOCK ?? 0));
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid || stat.size > limit) gap(code);
    if (sealed && (stat.nlink !== 1 || (stat.mode & 0o7777) !== sealed.mode)) gap(code);
    return fs.readFileSync(fd);
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    return gap(code);
  } finally { if (fd !== undefined) fs.closeSync(fd); }
};
const parse = (bytes: Buffer, code: string): Record<string, unknown> => {
  try {
    const value: unknown = JSON.parse(bytes.toString("utf8"));
    return object(value) ? value : gap(code);
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    return gap(code);
  }
};

/** RUN: absolute, canonical (no symlink component), a directory owned by the executor, mode 0700. */
export const verifyRunDir = (runDir: string, uid: number): void => {
  if (!path.isAbsolute(runDir) || path.normalize(runDir) !== runDir) gap("run-dir-invalid");
  try {
    if (fs.realpathSync(runDir) !== runDir) gap("run-dir-not-canonical");
    const stat = fs.lstatSync(runDir);
    if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o700) gap("run-dir-unsafe");
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    gap("run-dir-unreadable");
  }
};

export interface SeamRequest {
  schema: "d3-seam-request-v1"; run: string; runSha256: string; phase: string; principal: string; sessionId: string;
  /** DRAIN-PHASE-CONTRACT rev 4: `inputs.operationId`, ONE per run, minted and journaled by A, shared by every drain phase. */
  operationId: string;
  inputs: Record<string, unknown>;
}
/**
 * A's shared request schema (SEAT.md §3, closed keys), the phase from argv, and the contract's
 * `inputs.operationId` (32 lowercase hex). A missing or malformed operationId is a binding failure.
 */
export const parseRequest = (bytes: Buffer, phase: string, options: { operationId?: boolean } = {}): SeamRequest => {
  const value = parse(bytes, "request-invalid");
  const inputs = object(value.inputs) ? value.inputs : undefined;
  // The drain phases carry A's operationId (contract rev 4); the bridge phases do not.
  const needsOperationId = options.operationId !== false;
  if (needsOperationId && (typeof inputs?.operationId !== "string" || !HEX32.test(inputs.operationId))) gap("operation-id-invalid");
  if (Object.keys(value).sort().join(",") !== "inputs,phase,principal,run,runSha256,schema,sessionId" ||
    value.schema !== "d3-seam-request-v1" || value.phase !== phase || typeof value.run !== "string" || !HEX32.test(value.run) ||
    typeof value.runSha256 !== "string" || !HEX64.test(value.runSha256) || typeof value.sessionId !== "string" ||
    !UUID.test(value.sessionId) || typeof value.principal !== "string" || !/^[a-z0-9]{1,32}$/.test(value.principal) ||
    !object(value.inputs)) gap("request-invalid");
  return { ...(value as unknown as SeamRequest), operationId: needsOperationId ? inputs!.operationId as string : "" };
};

export interface SealedRun {
  runBytes: Buffer;
  run: Record<string, unknown>;
  inputs: Record<string, unknown>;
  inputsSha256: string;
  seat: { uid: number; home: string };
  journal: string;
}

/**
 * `seat-run.json` hashes to the request's runSha256 and names the same run, session and principal;
 * `seat-inputs.json` hashes to the `inputs` pin journaled right after `begin`; the executor is the
 * seat's own user, whose passwd home is `seat.home`; the drain seam is pinned to fabric-v2.
 */
export const readSealedRun = (runDir: string, request: SeamRequest, uid: number, seamName: "drain" | "bridge" = "drain"): SealedRun => {
  const runBytes = readOwned(path.join(runDir, "seat-run.json"), uid, MAX_SEALED, { mode: 0o400 }, "seat-run-unreadable");
  if (sha256(runBytes) !== request.runSha256) gap("seat-run-sha256-mismatch");
  const run = parse(runBytes, "seat-run-invalid");
  const seams = object(run.seams) ? run.seams : undefined;
  const drain = seams && object(seams[seamName]) ? seams[seamName] : undefined;
  const source = object(run.source) ? run.source : undefined;
  if (run.schema !== "d3-seat-run-v1" || run.run !== request.run || run.sessionId !== request.sessionId ||
    run.principal !== request.principal || drain?.owner !== "fabric-v2" || typeof source?.journal !== "string" ||
    !path.isAbsolute(source.journal) || typeof run.namespace !== "string" || typeof run.context !== "string") gap("seat-run-mismatch");
  const journalLines = readOwned(path.join(runDir, "seat-journal.jsonl"), uid, MAX_JOURNAL, undefined, "seat-journal-unreadable")
    .toString("utf8").split("\n");
  let begin: Record<string, unknown>, pinned: Record<string, unknown>;
  try { begin = JSON.parse(journalLines[0]!) as Record<string, unknown>; pinned = JSON.parse(journalLines[1]!) as Record<string, unknown>; }
  catch { return gap("seat-journal-invalid"); }
  if (begin?.event !== "begin" || begin.run !== request.run || begin.runSha256 !== request.runSha256 ||
    pinned?.event !== "inputs" || typeof pinned.sha256 !== "string" || !HEX64.test(pinned.sha256)) gap("seat-journal-invalid");
  const inputBytes = readOwned(path.join(runDir, "seat-inputs.json"), uid, MAX_SEALED, { mode: 0o400 }, "seat-inputs-unreadable");
  if (sha256(inputBytes) !== pinned.sha256) gap("seat-inputs-sha256-mismatch");
  const inputs = parse(inputBytes, "seat-inputs-invalid");
  const seat = object(inputs.seat) ? inputs.seat : undefined;
  if (inputs.schema !== "d3-seat-inputs-v1" || inputs.run !== request.run || inputs.runSha256 !== request.runSha256 ||
    !Number.isSafeInteger(seat?.uid) || typeof seat?.home !== "string" || !path.isAbsolute(seat.home)) gap("seat-inputs-mismatch");
  if (seat!.uid !== uid) gap("executor-not-seat-user");
  let home: string;
  try { home = os.userInfo().homedir; } catch { return gap("executor-home-unknown"); }
  if (home !== seat!.home) gap("executor-home-mismatch");
  return { runBytes, run, inputs, inputsSha256: pinned.sha256 as string, seat: { uid: seat!.uid as number, home: seat!.home as string },
    journal: source!.journal as string };
};

/**
 * SEAT.md §2: D3_SEAT_LOCK_FD is the holder of RUN/seat.lock. Same inode; a fresh open cannot take
 * the lock; a reentrant flock on the inherited open file description succeeds. Node has no flock,
 * so a fixed `/usr/bin/python3 -I` helper receives the inherited descriptor as its fd 3.
 */
const LOCK_PROOF = [
  "import fcntl, os, sys",
  "fresh = os.open(sys.argv[1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)",
  "try:",
  "    try:",
  "        fcntl.flock(fresh, fcntl.LOCK_EX | fcntl.LOCK_NB)",
  "        first = 'acquired'",
  "    except BlockingIOError:",
  "        first = 'blocked'",
  "finally:",
  "    os.close(fresh)",
  "try:",
  "    fcntl.flock(3, fcntl.LOCK_EX | fcntl.LOCK_NB)",
  "    second = 'held'",
  "except OSError:",
  "    second = 'lost'",
  "sys.stdout.write(first + ' ' + second)",
].join("\n");

export const proveSeatLock = (runDir: string, fdText: string | undefined, uid: number, python = PYTHON): number => {
  if (!fdText || !/^[0-9]{1,7}$/.test(fdText)) gap("lock-fd-missing");
  const fd = Number(fdText);
  const lockFile = path.join(runDir, "seat.lock");
  try {
    const held = fs.fstatSync(fd, { bigint: true });
    const named = fs.lstatSync(lockFile, { bigint: true });
    if (!named.isFile() || named.uid !== BigInt(uid) || named.nlink !== 1n || (named.mode & 0o7777n) !== 0o600n) gap("lock-file-unsafe");
    if (held.dev !== named.dev || held.ino !== named.ino) gap("lock-fd-other-file");
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    gap("lock-fd-unreadable");
  }
  const result = spawnSync(python, ["-I", "-B", "-c", LOCK_PROOF, lockFile], {
    stdio: ["ignore", "pipe", "pipe", fd], env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C" }, timeout: 10_000, encoding: "utf8",
  });
  if (result.status !== 0 || result.stdout !== "blocked held") gap("lock-not-held");
  return fd;
};

// As records/peer.ts: getsockopt through ctypes on fd 3, which leaves the shared descriptor non-blocking.
const PEERCRED = "import ctypes, struct, sys; l = ctypes.CDLL(None, use_errno=True); b = ctypes.create_string_buffer(12); " +
  "n = ctypes.c_uint32(12); r = l.getsockopt(3, 1, 17, b, ctypes.byref(n)); " +
  "sys.stdout.write('%d %d %d' % struct.unpack('3i', b.raw)) if r == 0 else sys.exit(1)";

/** The connected peer's {pid, uid} from SO_PEERCRED, via the fixed python helper (Node exposes none). */
export const socketPeer = (socket: net.Socket, python = PYTHON): Promise<{ pid: number; uid: number } | undefined> =>
  new Promise(resolve => {
    const fd = (socket as unknown as { _handle?: { fd?: unknown } })._handle?.fd;
    if (typeof fd !== "number" || fd < 0) { resolve(undefined); return; }
    let out = "";
    const child = spawn(python, ["-I", "-B", "-c", PEERCRED], { stdio: ["ignore", "pipe", "ignore", fd],
      env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C" }, timeout: 5_000 });
    child.stdout!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => { out += chunk; });
    child.on("error", () => resolve(undefined));
    child.on("close", code => {
      const [pid, uid] = out.split(" ").map(Number);
      resolve(code === 0 && Number.isSafeInteger(pid) && pid! > 0 && Number.isSafeInteger(uid) ? { pid: pid!, uid: uid! } : undefined);
    });
  });

/** A source journal's bounded head, not its whole (potentially very large) session body. */
const readSourceHead = (file: string, uid: number): Buffer => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | (fs.constants.O_NONBLOCK ?? 0));
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid) gap("source-journal-unreadable");
    const head = Buffer.alloc(MAX_SOURCE_HEADER);
    let offset = 0;
    while (offset < head.length) {
      const read = fs.readSync(fd, head, offset, head.length - offset, offset);
      if (read === 0) break;
      offset += read;
    }
    return head.subarray(0, offset);
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    return gap("source-journal-unreadable");
  } finally { if (fd !== undefined) fs.closeSync(fd); }
};

/**
 * The mesh root the seat's Main uses, from the sealed packet and the seat's Fabric config only
 * (never this process's environment): the cwd recorded in the sealed native journal's header,
 * then mesh.root from `<cwd>/.pi/fabric.json`, else from the agent dir's fabric.json, relative to
 * that cwd, else `<cwd>/.pi/fabric/mesh`. The Main's own reply must name the same real path.
 */
export const resolveSeatMeshRoot = (journal: string, sessionId: string, uid: number): string => {
  const head = readSourceHead(journal, uid);
  const newline = head.indexOf(0x0a);
  if (newline < 0) gap("source-journal-header-invalid");
  const header = parse(head.subarray(0, newline), "source-journal-header-invalid");
  if (header.type !== "session" || header.id !== sessionId || typeof header.cwd !== "string" || !path.isAbsolute(header.cwd)) {
    gap("source-journal-header-invalid");
  }
  const sessions = path.dirname(path.dirname(journal));
  if (path.basename(sessions) !== "sessions") gap("source-journal-outside-agent-dir");
  const agentDir = path.dirname(sessions);
  const cwd = header.cwd as string;
  const configured = (file: string): string | undefined => {
    try {
      const stat = fs.lstatSync(file, { throwIfNoEntry: false });
      if (!stat) return undefined;
    } catch { return gap("fabric-config-unreadable"); }
    const value = parse(readOwned(file, uid, MAX_SEALED, undefined, "fabric-config-unreadable"), "fabric-config-invalid");
    const mesh = object(value.mesh) ? value.mesh : undefined;
    return typeof mesh?.root === "string" && mesh.root.trim() ? mesh.root.trim() : undefined;
  };
  const root = configured(path.join(cwd, ".pi", "fabric.json")) ?? configured(path.join(agentDir, "fabric.json"));
  const meshRoot = root ? path.resolve(cwd, root) : path.join(cwd, ".pi", "fabric", "mesh");
  try { return fs.realpathSync(meshRoot); } catch { return gap("mesh-root-unreadable"); }
};

/**
 * Write-once evidence under RUN (SEAT.md §0): O_CREAT|O_EXCL|O_NOFOLLOW, 0600, fully written, the
 * file and RUN fsynced, then read back byte-for-byte. Nothing under RUN is ever rewritten.
 */
export const writeOnce = (runDir: string, name: string, bytes: Buffer): string => {
  if (!/^fabric-v2-[a-z0-9-]+\.json$/.test(name)) gap("evidence-name-invalid");
  const file = path.join(runDir, name);
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
    let offset = 0;
    while (offset < bytes.length) offset += fs.writeSync(fd, bytes, offset, bytes.length - offset);
    fs.fsyncSync(fd);
  } catch { return gap("evidence-write-failed"); } finally { if (fd !== undefined) fs.closeSync(fd); }
  let directory: number | undefined;
  try { directory = fs.openSync(runDir, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY); fs.fsyncSync(directory); }
  catch { return gap("evidence-sync-failed"); } finally { if (directory !== undefined) fs.closeSync(directory); }
  const back = readOwned(file, os.userInfo().uid, bytes.length, { mode: 0o600 }, "evidence-readback-failed");
  if (!back.equals(bytes)) gap("evidence-readback-failed");
  return sha256(bytes);
};

/** The interpreter running this seam, measured (rule-d3drain-dev 2): path, sha256, owner. */
export const interpreterIdentity = (): { execPath: string; sha256: string | null; uid: number | null; rootOwned: boolean } => {
  try {
    const execPath = fs.realpathSync(process.execPath);
    const stat = fs.statSync(execPath);
    return { execPath, sha256: sha256(fs.readFileSync(execPath)), uid: stat.uid, rootOwned: stat.uid === 0 && (stat.mode & 0o022) === 0 };
  } catch { return { execPath: process.execPath, sha256: null, uid: null, rootOwned: false }; }
};
