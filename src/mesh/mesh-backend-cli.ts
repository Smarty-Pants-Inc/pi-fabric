// fabric-mesh-backend: import, cutover, rollback and status of a mesh root's state backend
// (smarty-dev#6477 L4b). The fence and the commit order live in backend-migration.ts; see
// docs/mesh-lock-plan.md section 5.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { censusSync as writerCensusSync, type CensusWriter } from "./writer-census.js";
import { abortMeshRollback, cutoverMeshState, describeCensusAdvisory, importMeshState, meshBackendStatus, MeshBackendFenceError,
  MeshBackendRefusedError, rollbackMeshState, type MeshBackendAlarm, type MeshBackendOptions, type MeshBackendStatus,
  type MeshCensusAdvisory, type MeshCensusWriter, type MeshWriterCensus } from "./backend-migration.js";
import { BUILTIN_READERS, installReaderProof, probeStateDbLeases, scanStateDbHolders, watchStateDbLeases, type StateDbLeaseWatch, type LeaseProbeOptions, type ProcScanOptions, type StateDbHolder, isReaderName, proveFabricReader, readerReadiness, recordSwitch, requiredReaders,
  type InstalledReader, type ReaderBackend, type ReaderReadiness, type RequiredReader } from "./reader-proof.js";

const COMMANDS = ["census", "import", "status", "cutover", "rollback", "abort-rollback", "reader-proof"] as const;
type Command = typeof COMMANDS[number];

const USAGE = `Usage: fabric-mesh-backend <census|import|status|cutover|rollback|abort-rollback> --root DIR [--json]
                          [--lock-protocol 1|2] [--lock-timeout-ms N]
                          [--accept-unready NAME,... --alarm-owner ROOT --alarm-cc ROOT]
                          [--require-reader NAME=INSTALL_ROOT]...
       fabric-mesh-backend import --scratch-proof-from STATE_JSON [--scratch-parent DIR] [--json]
       fabric-mesh-backend reader-proof --root DIR --backend file|sqlite [--name fabric...] [--reader-version V] [--json]
       fabric-mesh-backend reader-proof --root DIR --name NAME --proof-file FILE [--json]

  census          the ADVISORY writer census (writer-census.ts): the processes it attributed (mode,
                  release) and the evidence it could not. Informational; always exit 0 when it ran.
  status          backend flag, epochs, digests, the reader decision; the census as advisory.
  import          the cutover section (also the roll forward): state.json -> state.db under the
                  fence at backend=importing E+1, digest verified both ways, state.json replaced
                  by the moved marker, then backend=sqlite. Refuses a diverged sqlite root.
  cutover         import (the same fenced section).
  rollback        the R1 fence: flag exporting (E+1), export to a verified temp, switch to file,
                  then replace the marker (last). A rerun converges from the stored flag.
  abort-rollback  under the fence: restore the marker, then exporting -> sqlite, keeping epoch E+1.
  reader-proof    --backend: THIS Fabric release reads the backend with MeshStore (sqlite: a migrated
                  scratch copy of state.json), then writes readers/fabric-<release>.json.
                  --proof-file: installs the proof a non-Fabric reader made with its own read code.
                  Both write under the migration fence (custody.lock, then .lock).

Readiness gate (smarty-dev#7815, docs/mesh-backend.md): import and cutover refuse (exit 3) unless
every REQUIRED reader holds a valid proof of sqlite for this switch, made within 24 h by its installed
release. Required: each distinct release of a live Fabric writer (host leases and process records),
the built-in installed readers (factory) and each --require-reader. --accept-unready NAME,... is the
only override. It needs --alarm-owner ROOT and --alarm-cc ROOT (the chief-of-staff copy; one root each, never a
principal instance org or org-*): every listed reader is alarmed to both (mesh ops.owner, addressed, never a
broadcast) before the fence, else it refuses; under the fence an unalarmed accepted reader refuses the commit
(smarty-dev#9436). The intent is written to <root>/backend-switches.jsonl before the switch; the check
runs again under the fence right before each flag commit (importing, sqlite), crash reruns included.

--scratch-proof-from STATE_JSON (import only, smarty-dev#9404): a reader's proof step. The CLI itself
creates a fresh 0700 scratch directory (mkdtemp in --scratch-parent, default the OS temp directory; its
ancestors must be ours or root's, no symlink, and not group/other-writable unless root-owned and sticky),
copies STATE_JSON into it (read through an O_NOFOLLOW fd; a regular file of at most 64 MiB; the live
state.json is fine) and imports there without the readiness gate: the root is provably a copy this
process just made. It prints "scratchProof": true and "root"; the caller reads it, then removes it. A
refused or failed import removes the whole directory; a cleanup failure warns (also "cleanupWarnings").
No caller-named root ever skips the gate.

The fence is the mesh .lock plus custody.lock, held for the whole section of import, cutover,
rollback and abort-rollback. Stop every writer (Pi sessions, actors, mesh-bridge, the projector) first.
The writer census is advisory only (smarty-dev#6982): each of those commands prints
"advisory: N writers, M unknown" and never blocks, permits or changes the operation on it.
Exit status: 0 done, 1 error, 2 usage, 3 refused or fence violation (nothing unsafe was done).`;

/**
 * smarty-dev#6477 W1: L4a's writer census as the advisory census provider (smarty-dev#6982). An
 * attributed writer keeps its mode (file, shadow, sqlite; "foreign host H" when another host's);
 * evidence the census could not attribute is listed as unknown, with its source and reason.
 * Reported only: nothing accepts or refuses on it.
 */
export const meshWriterCensus = (root: string): MeshWriterCensus => async () => meshWriterCensusSync(root)();

/** The same census, synchronous: the readiness gate recomputes its inventory under the fence. */
export type MeshWriterCensusSync = () => { writers: MeshCensusWriter[]; unknown?: MeshCensusWriter[] };
export const meshWriterCensusSync = (root: string): MeshWriterCensusSync => () => {
  const result = writerCensusSync(root);
  const host = os.hostname();
  const unknownSet = new Set<CensusWriter>(result.unknown);
  const where = (writer: CensusWriter): string => writer.name ? ` ${writer.source} ${writer.name}` : ` ${writer.source}`;
  const writers: MeshCensusWriter[] = result.writers.filter(writer => !unknownSet.has(writer)).map(writer => {
    const foreign = writer.host !== undefined && writer.host !== host;
    return { pid: writer.pid ?? 0, release: writer.releaseSha ?? "unknown",
      mode: foreign ? `foreign host ${writer.host} ${writer.stateBackend ?? "?"}${where(writer)}` : writer.stateBackend ?? "?" };
  });
  const unknown: MeshCensusWriter[] = result.unknown.map(writer => ({ pid: writer.pid ?? 0, release: writer.releaseSha ?? "unknown",
    mode: `unknown${where(writer)}${writer.reason ? `: ${writer.reason}` : ""}` }));
  return { writers, unknown };
};

interface Options {
  command: Command; root: string; json: boolean; lockProtocol: 1 | 2; lockTimeoutMs?: number;
  acceptUnready: string[]; requireReaders: InstalledReader[]; name?: string; backend?: ReaderBackend; readerVersion?: string; proofFile?: string;
  /** smarty-dev#9404: import a fresh scratch copy of this state.json, gate skipped; `root` is then unset until made. */
  scratchProofFrom?: string; scratchParent?: string;
  /** smarty-dev#9436: the roots an accepted override alarms: its owner and the chief-of-staff copy. */
  alarmOwner?: string; alarmCc?: string;
}

class UsageError extends Error {}

const parseArgs = (argv: string[]): Options | "help" => {
  if (argv.length === 0 || argv.includes("--help") || argv.includes("-h")) return "help";
  const [command, ...rest] = argv;
  if (!COMMANDS.includes(command as Command)) throw new UsageError(`Unknown command ${String(command)}`);
  let root: string | undefined;
  let json = false;
  let lockProtocol: 1 | 2 = 1;
  let lockTimeoutMs: number | undefined;
  let acceptUnready: string[] = [];
  let name: string | undefined;
  let backend: ReaderBackend | undefined;
  let readerVersion: string | undefined;
  let proofFile: string | undefined;
  let scratchProofFrom: string | undefined;
  let scratchParent: string | undefined;
  let alarmOwner: string | undefined;
  let alarmCc: string | undefined;
  const alarmRoot = (flag: string, value: string): string => {
    // ponytail: Fabric keeps no list of principal instances (FabricPrincipal is input authority, RecordsPrincipal a
    // records caller), so a fixed deny list: `org` and every `org-*` (org-kate) are principal instances, never alarm
    // targets (R-org-delegate; pi-fabric#919 review). Checked on the normalized name, so an alias ("Org", " org") is too.
    if (/^org(-.*)?$/.test(value.trim().toLowerCase())) {
      throw new UsageError(`Bad ${flag} ${value}: a principal instance (org, org-*) is never an alarm target; name the owning lead`);
    }
    // One root name (a participant id or a session root): no wildcard, list or broadcast form.
    if (!/^[A-Za-z0-9][A-Za-z0-9._:@-]{0,255}$/.test(value)) {
      throw new UsageError(`Bad ${flag} ${value} (one participant or root id: letters, digits, . _ : @ -; no wildcard or list)`);
    }
    return value;
  };
  const requireReaders: InstalledReader[] = [];
  for (let index = 0; index < rest.length; index++) {
    const flag = rest[index]!;
    if (flag === "--json") { json = true; continue; }
    const value = rest[++index];
    if (value === undefined || value.startsWith("--")) throw new UsageError(`Missing value for ${flag}`);
    if (flag === "--root" || flag === "--mesh") root = value;
    else if (flag === "--lock-protocol") {
      if (value !== "1" && value !== "2") throw new UsageError("--lock-protocol must be 1 or 2");
      lockProtocol = value === "2" ? 2 : 1;
    } else if (flag === "--lock-timeout-ms") {
      const number = Number(value);
      if (!Number.isFinite(number) || number < 100) throw new UsageError(`Bad --lock-timeout-ms ${value}`);
      lockTimeoutMs = Math.floor(number);
    } else if (flag === "--accept-unready") acceptUnready = value.split(",").map(item => item.trim()).filter(Boolean);
    else if (flag === "--name") {
      if (!isReaderName(value)) throw new UsageError(`Bad --name ${value} (letters, digits, . _ -; at most 64)`);
      name = value;
    } else if (flag === "--backend") {
      if (value !== "file" && value !== "sqlite") throw new UsageError("--backend must be file or sqlite");
      backend = value;
    } else if (flag === "--reader-version") readerVersion = value;
    else if (flag === "--proof-file") proofFile = value;
    else if (flag === "--scratch-proof-from") scratchProofFrom = value;
    else if (flag === "--scratch-parent") scratchParent = value;
    else if (flag === "--alarm-owner") alarmOwner = alarmRoot(flag, value);
    else if (flag === "--alarm-cc") alarmCc = alarmRoot(flag, value);
    else if (flag === "--require-reader") {
      const [readerName, installRoot] = [value.slice(0, value.indexOf("=")), value.slice(value.indexOf("=") + 1)];
      // Flags only ADD readers: a built-in's trusted root can never be replaced from the command line.
      if (readerName.startsWith("fabric") || BUILTIN_READERS.some(reader => reader.name === readerName)) {
        throw new UsageError(`Bad --require-reader ${value}: reserved reader name ${readerName}`);
      }
      if (!value.includes("=") || !isReaderName(readerName) || !path.isAbsolute(installRoot)) {
        throw new UsageError(`Bad --require-reader ${value} (NAME=/absolute/release/root)`);
      }
      requireReaders.push({ name: readerName, installRoot });
    }
    else throw new UsageError(`Bad argument: ${flag}`);
  }
  if (scratchProofFrom !== undefined || scratchParent !== undefined) {
    if (command !== "import" || scratchProofFrom === undefined || root !== undefined || acceptUnready.length > 0 || requireReaders.length > 0) {
      throw new UsageError("--scratch-proof-from STATE_JSON [--scratch-parent DIR] is for import only, without --root, --accept-unready or --require-reader");
    }
    return { command: "import", root: "", json, lockProtocol, acceptUnready, requireReaders, scratchProofFrom: path.resolve(scratchProofFrom),
      ...(scratchParent === undefined ? {} : { scratchParent: path.resolve(scratchParent) }),
      ...(lockTimeoutMs === undefined ? {} : { lockTimeoutMs }) };
  }
  if (!root) throw new UsageError("--root DIR is required");
  // smarty-dev#9436: an override is never silent: it names who is alarmed when it is accepted.
  if (acceptUnready.length > 0 && (alarmOwner === undefined || alarmCc === undefined)) {
    throw new UsageError("--accept-unready needs --alarm-owner ROOT and --alarm-cc ROOT (the chief-of-staff copy): an accepted override alarms both");
  }
  if (command === "reader-proof" && (proofFile ? !name || backend : !backend)) {
    throw new UsageError("reader-proof needs --backend file|sqlite, or --name NAME --proof-file FILE");
  }
  return { command: command as Command, root: path.resolve(root), json, lockProtocol, acceptUnready, requireReaders,
    ...(proofFile === undefined ? {} : { proofFile }),
    ...(alarmOwner === undefined ? {} : { alarmOwner }), ...(alarmCc === undefined ? {} : { alarmCc }),
    ...(lockTimeoutMs === undefined ? {} : { lockTimeoutMs }), ...(name === undefined ? {} : { name }),
    ...(backend === undefined ? {} : { backend }), ...(readerVersion === undefined ? {} : { readerVersion }) };
};

const formatStatus = (status: MeshBackendStatus): string => {
  const lines = [
    `root          ${status.root}`,
    `backend       ${status.backend}  epoch ${status.epoch}${status.commit === undefined ? "" : `  commit ${status.commit}`}`,
    `state.json    ${status.fileMoved ? `moved marker (backend=sqlite at epoch ${status.fileEpoch})` : status.fileError ? `unreadable: ${status.fileError}` : `epoch ${status.fileEpoch}  generation ${status.fileGeneration ?? "-"}`}`,
    `db digest     ${status.dbDigest ?? "-"}`,
    `file digest   ${status.fileDigest ?? "-"}`,
    `readers use   ${"source" in status.reader ? status.reader.source : `NOTHING (fails closed: ${status.reader.error})`}`,
    `fence         ${status.fenceHolds ? "holds" : "VIOLATED"}`,
  ];
  if (status.importDigest) lines.push(`last import   ${status.importDigest}`);
  if (status.exportDigest) lines.push(`last export   ${status.exportDigest} (${status.exportGeneration ?? "-"})`);
  if (status.censusError !== undefined || status.writers) {
    lines.push(...formatAdvisory({ writers: status.writers ?? [], unknown: status.unknownWriters ?? [],
      ...(status.censusError === undefined ? {} : { error: status.censusError }) }));
  } else lines.push("census        none (no census provider)");
  return lines.join("\n");
};

/** The advisory census, never a verdict: "advisory: N writers, M unknown" and the entries. */
const formatAdvisory = (advisory: MeshCensusAdvisory): string[] => [
  `census        ${describeCensusAdvisory(advisory)}`,
  ...advisory.writers.map(writer => `  pid ${writer.pid}  ${writer.mode}  ${writer.release}`),
  ...advisory.unknown.map(writer => `  pid ${writer.pid}  ${writer.mode}  ${writer.release}`),
];

const formatResult = (command: Command, result: Record<string, unknown>): string => {
  const done = result.converged ? "already done (verified)" : "done";
  if (command === "import" || command === "cutover") {
    return `${command} ${done}: backend=sqlite epoch ${String(result.epoch)} (from ${String(result.previousEpoch)}), ` +
      `${String(result.entries)} entries, ${String(result.tombstones)} tombstones, digest ${String(result.digest)}`;
  }
  if (command === "rollback") {
    return `rollback ${done}: backend=file epoch ${String(result.epoch)}, steps ${(result.steps as number[]).join(",")}, digest ${String(result.digest)}`;
  }
  return `abort-rollback ${done}: backend=sqlite epoch ${String(result.epoch)}`;
};

/** A directory other uids cannot change: ours or root's, never a symlink (pi-fabric#884 round 2: not even root's, whose
 * target chain would go unchecked), and not group/other-writable unless root-owned and sticky (/tmp). Throws why. */
const assertSafeChain = (directory: string): void => {
  const uid = process.getuid?.();
  for (let current = path.resolve(directory); ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    const owner = stat.uid === uid || stat.uid === 0;
    if (stat.isSymbolicLink()) throw new MeshBackendRefusedError(`--scratch-parent chain: ${current} is a symbolic link`);
    else if (!stat.isDirectory() || !owner) throw new MeshBackendRefusedError(`--scratch-parent chain: ${current} is not a directory owned by us or root`);
    else if ((stat.mode & 0o022) !== 0 && !(stat.uid === 0 && (stat.mode & 0o1000) !== 0)) {
      throw new MeshBackendRefusedError(`--scratch-parent chain: ${current} is writable by group or others (mode ${(stat.mode & 0o7777).toString(8)})`);
    }
    if (path.dirname(current) === current) return;
  }
};

/**
 * smarty-dev#9404 (pi-fabric#884 round 1): the scratch copy a reader's proof imports. Created here, so it is provably a
 * copy and not a live mesh: a fresh mkdtemp (0700) in a parent whose whole chain no other uid can change, held by a
 * no-follow fd. Everything after mkdtemp goes through that fd (`/proc/self/fd/<fd>`, pi-fabric#884 round 2): the
 * copy, the import and the cleanup, so a swap of the path cannot redirect them; the path is re-checked against the
 * fd before the import and before it is reported.
 */
const MAX_SCRATCH_SOURCE_BYTES = 64 * 1024 * 1024;
interface Scratch { root: string; fd: number; pinned: string }
/**
 * The fd directory through which a held directory fd can be traversed (`/proc/self/fd`, else `/dev/fd`), proven on
 * an fd of `base` itself: `<fdBase>/<fd>/.` must name that directory. Refuses when neither works. Creates nothing.
 */
const scratchFdBase = (base: string): string => {
  const probe = fs.openSync(base, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
  try {
    const held = fs.fstatSync(probe);
    for (const fdBase of ["/proc/self/fd", "/dev/fd"]) {
      const through = (() => { try { return fs.statSync(path.join(fdBase, String(probe), ".")); } catch { return undefined; } })();
      if (through?.isDirectory() && through.dev === held.dev && through.ino === held.ino) return fdBase;
    }
  } finally { fs.closeSync(probe); }
  throw new MeshBackendRefusedError("--scratch-proof-from needs /proc/self/fd or /dev/fd to pin its scratch directory");
};
const makeScratchCopy = (stateJson: string, parent: string | undefined): Scratch => {
  if (process.platform === "win32" || typeof process.getuid !== "function") throw new MeshBackendRefusedError("--scratch-proof-from needs POSIX ownership (not on this platform)");
  const base = parent ?? os.tmpdir();
  assertSafeChain(base);
  const source = fs.openSync(stateJson, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  let bytes: Buffer;
  try {
    const stat = fs.fstatSync(source);
    if (!stat.isFile()) throw new MeshBackendRefusedError(`--scratch-proof-from ${stateJson} is not a regular file`);
    if (stat.size > MAX_SCRATCH_SOURCE_BYTES) throw new MeshBackendRefusedError(`--scratch-proof-from ${stateJson} is larger than 64 MiB (${stat.size} bytes)`);
    // smarty-dev#9526: read at most cap+1 bytes from this fd; a source that grew after the fstat is refused unread.
    const chunks: Buffer[] = [];
    let total = 0;
    const chunk = Buffer.alloc(1024 * 1024);
    for (let read; (read = fs.readSync(source, chunk, 0, Math.min(chunk.length, MAX_SCRATCH_SOURCE_BYTES + 1 - total), null)) > 0;) {
      chunks.push(Buffer.from(chunk.subarray(0, read)));
      total += read;
      if (total > MAX_SCRATCH_SOURCE_BYTES) throw new MeshBackendRefusedError(`--scratch-proof-from ${stateJson} grew past 64 MiB while it was read`);
    }
    bytes = Buffer.concat(chunks, total);
  } finally { fs.closeSync(source); }
  // smarty-dev#9526: the copy, the import and the cleanup all go through the pinned fd, so prove the platform can
  // traverse a directory fd BEFORE anything is created: otherwise a refused pin also left its directory behind.
  const fdBase = scratchFdBase(base);
  const root = fs.mkdtempSync(path.join(base, "fabric-scratch-proof-"));
  const fd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
  const pinned = `${fdBase}/${fd}`;
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || stat.uid !== process.getuid() || (stat.mode & 0o777) !== 0o700) throw new MeshBackendRefusedError(`scratch directory ${root} is not our private directory`);
    const through = fs.statSync(pinned, { throwIfNoEntry: false });
    if (!through || through.dev !== stat.dev || through.ino !== stat.ino) throw new MeshBackendRefusedError(`--scratch-proof-from needs ${fdBase} to pin its scratch directory`);
    const copy = fs.openSync(path.join(pinned, "state.json"), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
    try { fs.writeFileSync(copy, bytes); fs.fsyncSync(copy); } finally { fs.closeSync(copy); }
  } catch (error) {
    const warnings: string[] = [];
    removeScratch({ root, fd, pinned }, warnings);
    throw Object.assign(error as Error, { cleanupWarnings: warnings });
  }
  return { root, fd, pinned };
};
/**
 * Remove the scratch directory through its held fd (pi-fabric#884 round 2): each entry under the pinned directory, then
 * the directory itself only while its path still names the one we hold. Every failure is a warning; closes the fd.
 */
const removeScratch = (scratch: Scratch, warnings: string[]): boolean => {
  try {
    for (const name of fs.readdirSync(scratch.pinned)) {
      try { fs.rmSync(path.join(scratch.pinned, name), { recursive: true, force: true }); }
      catch (error) { warnings.push(`could not remove ${scratch.root}/${name}: ${(error as Error).message}`); }
    }
    if (!sameScratch(scratch.root, scratch.fd)) warnings.push(`did not remove ${scratch.root}: the path no longer names the scratch directory this process made`);
    else {
      try { fs.rmdirSync(scratch.root); }
      catch (error) { warnings.push(`could not remove the scratch directory ${scratch.root}: ${(error as Error).message}`); }
    }
  } catch (error) { warnings.push(`could not clean the scratch directory ${scratch.root}: ${(error as Error).message}`); }
  finally { try { fs.closeSync(scratch.fd); } catch { /* closed */ } }
  return warnings.length === 0;
};
/** The scratch path still names the directory this process made and holds. */
const sameScratch = (root: string, fd: number): boolean => {
  const named = fs.lstatSync(root, { throwIfNoEntry: false }), held = fs.fstatSync(fd);
  return !!named?.isDirectory() && named.dev === held.dev && named.ino === held.ino;
};

/** `import --scratch-proof-from` (smarty-dev#9404): the gate is skipped only on the copy this process made. */
const scratchProofImport = async (options: Options, io: { census?: MeshWriterCensus; options?: Partial<MeshBackendOptions>;
  afterScratch?: (root: string) => void },
  write: (text: string) => void, warn: (text: string) => void): Promise<number> => {
  const alarms: MeshBackendAlarm[] = [];
  let scratch: Scratch | undefined;
  let cleanupWarnings: string[] = [];
  try {
    try { scratch = makeScratchCopy(options.scratchProofFrom!, options.scratchParent); }
    catch (error) { cleanupWarnings = (error as { cleanupWarnings?: string[] }).cleanupWarnings ?? []; throw error; }
    io.afterScratch?.(scratch.root);
    // Before the gate-skipped import: the path must still name the directory we hold (the import uses the fd anyway).
    if (!sameScratch(scratch.root, scratch.fd)) throw new MeshBackendRefusedError(`the scratch directory ${scratch.root} changed before the import`);
    const library: MeshBackendOptions = {
      ...io.options, lockProtocol: options.lockProtocol,
      onAlarm: (alarm) => { alarms.push(alarm); warn(`fabric-mesh-backend: ALARM ${alarm.code}: ${alarm.message}\n`); },
      census: io.census ?? meshWriterCensus(scratch.root),
      onAdvisory: (advisory) => { warn(`fabric-mesh-backend: ${describeCensusAdvisory(advisory)}\n`); io.options?.onAdvisory?.(advisory); },
      ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }),
    };
    // ponytail: the import takes the PATH: the SQLite state refuses a symlink root, and /proc/self/fd/<fd> is one.
    // Safe from other uids: no symlink anywhere in the chain, every parent ours or root's and not writable by others
    // (a sticky /tmp lets only the owner rename our entry), and the directory is our 0700 one, checked against
    // the held fd just above and again just below. The copy and the cleanup go through the fd.
    const result: Record<string, unknown> = { ...(await importMeshState(scratch.root, library)) };
    if (!sameScratch(scratch.root, scratch.fd)) throw new MeshBackendRefusedError(`the scratch directory ${scratch.root} changed during the import`);
    fs.closeSync(scratch.fd);
    write(`${options.json ? JSON.stringify({ command: "import", ok: true, scratchProof: true, root: scratch.root, ...result, alarms }, null, 2)
      : `scratch-proof ${formatResult("import", result)}: ${scratch.root}`}\n`);
    return 0;
  } catch (error) {
    const refused = error instanceof MeshBackendRefusedError || error instanceof MeshBackendFenceError;
    const message = error instanceof Error ? error.message : String(error);
    // Ours and fresh: nothing else is in it. Removed through the held fd; every failure warns (never silently).
    const removed = scratch ? removeScratch(scratch, cleanupWarnings) : false;
    for (const text of cleanupWarnings) warn(`fabric-mesh-backend: ${text}\n`);
    if (options.json) {
      write(`${JSON.stringify({ command: "import", ok: false, refused, scratchProof: false, code: (error as { code?: unknown }).code, error: message,
        ...(scratch ? { root: scratch.root, removed } : {}), ...(cleanupWarnings.length ? { cleanupWarnings } : {}), alarms }, null, 2)}\n`);
    }
    warn(`fabric-mesh-backend: import ${refused ? "refused" : "failed"}: ${message}\n`);
    return refused ? 3 : 1;
  }
};

interface Gate { stop?: () => void; ownStateDbFds?: string[]; switchId: string; required: RequiredReader[]; readiness: ReaderReadiness; final?: ReaderReadiness; checked: string[];
  overrideAlarm?: OverrideAlarm }

/** smarty-dev#9436: what an override alarmed: the roots, every listed reader (armed), those accepted, the event ids. */
interface OverrideAlarm { owner: string; cc: string; armed: string[]; accepted: string[]; events: string[] }

export const OVERRIDE_ALARM_KIND = "mesh-backend.accepted-unready";

/**
 * Raises the accepted-override alarm (smarty-dev#9436): one ops.owner event addressed to the owner and one to the
 * chief-of-staff copy, never a broadcast. Event log only, outside the fence; the store is closed at once, so this
 * process's census record never joins the gate's inventory.
 */
/** One recipient's outcome: sent (the event id), failed (why), or not sent (an earlier send failed). */
type AlarmDelivery = { to: string; role: "owner" | "cc"; state: "sent"; id: string } | { to: string; role: "owner" | "cc"; state: "failed"; reason: string }
  | { to: string; role: "owner" | "cc"; state: "not sent" };

/** Some recipient was not alarmed: carries every recipient's state (pi-fabric#919 SEC P2: a sent id is never lost). */
class OverrideAlarmError extends Error {
  constructor(readonly deliveries: AlarmDelivery[]) {
    super(deliveries.map(delivery => `${delivery.role} ${delivery.to} ${delivery.state === "sent" ? `sent (event ${delivery.id})`
      : delivery.state === "failed" ? `failed (${delivery.reason})` : "not sent"}`).join("; "));
  }
}

const raiseOverrideAlarm = async (options: Options, accepted: ReaderReadiness["unready"], phase: string, switchId: string): Promise<string[]> => {
  const { MeshStore } = await import("./store.js");
  const store = new MeshStore(options.root, 256 * 1024, 500, { lockProtocol: options.lockProtocol });
  try {
    const text = `fabric-mesh-backend ${options.command} on ${options.root}: --accept-unready override of reader(s) (${phase}): `
      + `${accepted.map(reader => `${reader.name} (${reader.reason})`).join("; ")} (smarty-dev#9436)`;
    const deliveries: AlarmDelivery[] = [];
    for (const [role, to] of [["owner", options.alarmOwner!], ["cc", options.alarmCc!]] as const) {
      if (deliveries.some(delivery => delivery.state !== "sent")) { deliveries.push({ to, role, state: "not sent" }); continue; }
      try {
        const event = await store.publish({ topic: "ops.owner", kind: OVERRIDE_ALARM_KIND, to, text,
          from: { id: "fabric-mesh-backend", name: "fabric-mesh-backend", kind: "agent" },
          // switchId links the alarm to its intent and outcome lines in <root>/backend-switches.jsonl (pi-fabric#919 P2).
          data: { root: options.root, command: options.command, phase, switchId, accepted: accepted.map(reader => ({ name: reader.name, reason: reader.reason })),
            owner: options.alarmOwner, cc: options.alarmCc } });
        deliveries.push({ to, role, state: "sent", id: event.id });
      } catch (error) { deliveries.push({ to, role, state: "failed", reason: error instanceof Error ? error.message : String(error) }); }
    }
    if (deliveries.some(delivery => delivery.state !== "sent")) throw new OverrideAlarmError(deliveries);
    return deliveries.map(delivery => (delivery as { id: string }).id);
  } finally { store.closeState(); }
};

/**
 * The readiness gate of a switch to sqlite (smarty-dev#7815). The required readers come from the
 * inventory (live writers' releases from the census, built-in installed readers, --require-reader),
 * never from the registry. Scans (refuse early), writes the intent (refuse if it cannot), then installs
 * `beforeCommit`: the whole check again under the fence right before each flag commit, crash reruns too.
 */
const openGate = async (options: Options, library: MeshBackendOptions, builtins: readonly InstalledReader[],
  gateCensus: MeshWriterCensusSync, procScan: ProcScanOptions, leaseProbe: LeaseProbeOptions,
  warn: (text: string) => void): Promise<Gate | undefined> => {
  // No mesh root: the switch itself refuses with nothing changed.
  if (!fs.statSync(options.root, { throwIfNoEntry: false })?.isDirectory()) return undefined;
  const { backend, epoch } = await meshBackendStatus(options.root);
  // sqlite: switched already, a rerun only verifies (no commit); exporting/file+marker: the switch refuses.
  if (backend !== "file" && backend !== "none" && backend !== "importing") return undefined;
  // The whole inventory, recomputed for every check (pre-fence and under the fence before each commit).
  // Evidence the census cannot attribute, and a failed census, are required as fabric@unknown.
  let ownFds: string[] | undefined;
  // smarty-dev#7936: the write-lease probe runs while this process holds no fd on state.db: before the
  // fence (the preflight) and under it before the switch opens state.db (beforeOpen). Under the fence it
  // runs as a watch (smarty-dev#8116) whose final report comes BEFORE this process's first own open
  // (pi-fabric#881): its verdict stands for the checks before each commit.
  let fencedWatch: StateDbLeaseWatch | undefined;
  let fencedLeases: StateDbHolder[] | undefined;
  let underFence = false;
  const leaseHolders = (): StateDbHolder[] => !underFence ? probeStateDbLeases(options.root, leaseProbe)
    : fencedLeases ?? [{ pid: 0, name: "holder@lease-probe", reason: "the state.db lease probe did not run under the fence (smarty-dev#7936)" }];
  const inventory = (): RequiredReader[] => {
    let writers: MeshCensusWriter[];
    let unattributed: MeshCensusWriter[] = [];
    try {
      const report = gateCensus();
      writers = (report.writers ?? []).filter(writer => writer.pid !== process.pid);
      unattributed = (report.unknown ?? []).filter(writer => writer.pid !== process.pid);
    } catch (error) {
      writers = [];
      unattributed = [{ pid: 0, release: "unknown", mode: `census failed: ${(error as Error).message}` }];
    }
    // smarty-dev#7936: on Linux, SQLite-file evidence is resolved to its holders. The gate's own fds are
    // ignored; any other pid (or an unreadable fd dir) is required as holder@<pid>. Off Linux it stays
    // fabric@unknown (fail closed).
    const scan = scanStateDbHolders(options.root, procScan);
    if (scan) ownFds = scan.own;
    if (scan) unattributed = unattributed.filter(writer => !writer.mode.startsWith("unknown state-database "));
    return requiredReaders(writers, unattributed, builtins, options.requireReaders, [...scan?.holders ?? [], ...leaseHolders()]);
  };
  const evaluate = (window: { fromEpoch: number; toEpoch: number }, when: string, required = inventory()): ReaderReadiness => {
    let readiness: ReaderReadiness;
    try { readiness = readerReadiness(options.root, "sqlite", required, window); }
    catch (error) { throw new MeshBackendRefusedError(`${(error as Error).message}${when}`); }
    const blocking = readiness.unready.filter(reader => !options.acceptUnready.includes(reader.name));
    if (blocking.length > 0) {
      throw new MeshBackendRefusedError(`readers not ready for sqlite${when}: ${blocking.map(reader => `${reader.name} (${reader.reason})`).join("; ")}; `
        + "each reader proves a real read (docs/mesh-backend.md), or pass --accept-unready NAME,...");
    }
    return readiness;
  };
  const window = backend === "importing" ? { fromEpoch: epoch - 1, toEpoch: epoch } : { fromEpoch: epoch, toEpoch: epoch + 1 };
  const required = inventory();
  const preview = (() => { try { return readerReadiness(options.root, "sqlite", required, window); } catch { return undefined; } })();
  for (const reader of preview?.unready ?? []) {
    warn(`fabric-mesh-backend: reader ${reader.name} not ready: ${reader.reason}${options.acceptUnready.includes(reader.name) ? " (accepted by --accept-unready)" : ""}\n`);
  }
  const readiness = evaluate(window, "", required);
  const gate: Gate = { switchId: randomUUID(), required, readiness, checked: [], stop: () => fencedWatch?.stop() };
  // smarty-dev#9436: every unready reader that passed `evaluate` was accepted by --accept-unready: alarm its owner and
  // the chief-of-staff copy BEFORE the switch, or refuse it (an accepted override is never silent).
  // pi-fabric#919 review P1: a listed reader may first turn unready UNDER the fence, where no alarm can be sent (the
  // commit hook is synchronous inside the transaction; the event log takes the mesh lock the fence holds). So every
  // name in --accept-unready is alarmed here, before the fence, with both sends confirmed: unready ones as accepted,
  // ready ones as accepted if they turn unready. Under the fence, an accepted reader outside this set refuses.
  if (options.acceptUnready.length > 0) {
    const armed = options.acceptUnready.map(name => readiness.unready.find(reader => reader.name === name)
      ?? { name, reason: "ready now; accepted if it turns unready under the fence" });
    try {
      gate.overrideAlarm = { owner: options.alarmOwner!, cc: options.alarmCc!, armed: options.acceptUnready,
        accepted: readiness.unready.map(reader => reader.name), events: await raiseOverrideAlarm(options, armed, "before the switch", gate.switchId) };
    } catch (error) {
      const deliveries = error instanceof OverrideAlarmError ? error.deliveries : undefined;
      for (const delivery of deliveries ?? []) {
        warn(`fabric-mesh-backend: override alarm ${delivery.role} ${delivery.to}: ${delivery.state === "sent" ? `sent, event ${delivery.id}`
          : delivery.state === "failed" ? `FAILED: ${delivery.reason}` : "not sent (an earlier send failed)"}\n`);
      }
      throw Object.assign(new MeshBackendRefusedError(`the accepted-override alarm to ${options.alarmOwner} and ${options.alarmCc} could not be raised: `
        + `${(error as Error).message} (an accepted --accept-unready is never silent; smarty-dev#9436)`),
        deliveries ? { overrideAlarm: { owner: options.alarmOwner, cc: options.alarmCc, deliveries } } : {});
    }
  }
  try {
    recordSwitch(options.root, { switchId: gate.switchId, phase: "intent", command: options.command, backend: "sqlite", from: backend, ...window,
      required, requireReader: options.requireReaders, acceptUnready: options.acceptUnready, readersReady: readiness.ready, acceptedUnready: readiness.unready,
      ...(gate.overrideAlarm ? { overrideAlarm: gate.overrideAlarm } : {}) });
  } catch (error) {
    throw new MeshBackendRefusedError(`cannot write the switch record ${options.root}/backend-switches.jsonl: ${(error as Error).message}`);
  }
  const outerOpen = library.beforeOpen;
  library.beforeOpen = async () => {
    await outerOpen?.();
    // smarty-dev#8116, pi-fabric#881: the watch runs from before the lease probe to its final report, which is taken
    // here, BEFORE this process's first own open of the files (FenceDb.open follows). Every open in that window is
    // foreign but the probe's own, so the rule is exact (no own open to subtract, none to merge with). From the
    // report to each commit is the accepted residual r0 of smarty-dev#8116.
    fencedWatch?.stop();
    fencedWatch = await watchStateDbLeases(options.root, leaseProbe);
    leaseProbe.beforeReport?.();
    fencedLeases = [...fencedWatch.holders, ...fencedWatch.check()];
    fencedWatch.stop();
    leaseProbe.afterReport?.();
    underFence = true;
  };
  const outer = library.beforeCommit;
  library.beforeCommit = (commit) => {
    underFence = true;
    outer?.(commit);
    gate.final = evaluate(commit, ` (under the fence, before the ${commit.phase} commit)`);
    // Never commit with an unsent override alarm: an accepted reader must be one alarmed before the fence. A throw
    // here rolls the commit's transaction back.
    const unsent = gate.final.unready.filter(reader => !(gate.overrideAlarm?.armed ?? []).includes(reader.name));
    if (unsent.length > 0) {
      throw new MeshBackendRefusedError(`accepted reader(s) ${unsent.map(reader => reader.name).join(", ")} were not alarmed before the fence `
        + `(under the fence, before the ${commit.phase} commit; smarty-dev#9436)`);
    }
    gate.checked.push(commit.phase);
    if (ownFds) gate.ownStateDbFds = ownFds;
  };
  return gate;
};

/** The outcome line of a switch, with the intent's switchId. Best effort: the intent already records the override. */
const closeGate = (root: string, gate: Gate, warn: (text: string) => void, outcome: Record<string, unknown>): string | undefined => {
  try { return recordSwitch(root, { switchId: gate.switchId, phase: "outcome", ...outcome }); }
  catch (error) {
    warn(`fabric-mesh-backend: could not append the switch outcome ${gate.switchId}: ${(error as Error).message}\n`);
    return undefined;
  }
};

export const main = async (argv: string[], io: {
  stdout?: (text: string) => void; stderr?: (text: string) => void;
  /** The writer census; default `meshWriterCensus` (writer-census.ts) of --root. */
  census?: MeshWriterCensus;
  /** The readiness gate's synchronous census; default `meshWriterCensusSync` of --root. */
  gateCensus?: MeshWriterCensusSync;
  /** Injection points of the state.db holder scan (tests). */
  procScan?: ProcScanOptions;
  /** Injection points of the state.db lease probe (tests). */
  leaseProbe?: LeaseProbeOptions;
  /** The built-in installed readers; default BUILTIN_READERS (tests replace them). */
  builtinReaders?: readonly InstalledReader[];
  /** Extra library options (tests: onStep, open). */
  options?: Partial<MeshBackendOptions>;
  /** Tests: runs between the scratch mkdtemp and the gate-skipped import (pi-fabric#884 round 2). */
  afterScratch?: (root: string) => void;
} = {}): Promise<number> => {
  const write = io.stdout ?? ((text: string) => void process.stdout.write(text));
  const warn = io.stderr ?? ((text: string) => void process.stderr.write(text));
  let options: Options | "help";
  try { options = parseArgs(argv); }
  catch (error) {
    if (!(error instanceof UsageError)) throw error;
    warn(`fabric-mesh-backend: ${error.message}\n${USAGE}\n`);
    return 2;
  }
  if (options === "help") { write(`${USAGE}\n`); return 0; }
  if (options.scratchProofFrom !== undefined) return scratchProofImport(options, io, write, warn);
  const alarms: MeshBackendAlarm[] = [];
  let gate: Gate | undefined;
  const library: MeshBackendOptions = {
    ...io.options,
    lockProtocol: options.lockProtocol,
    onAlarm: (alarm) => { alarms.push(alarm); warn(`fabric-mesh-backend: ALARM ${alarm.code}: ${alarm.message}\n`); },
    census: io.census ?? meshWriterCensus(options.root),
    ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }),
  };
  try {
    if (options.command === "census") {
      // Advisory (smarty-dev#6982): exit 0 whatever it shows; an empty report proves nothing.
      const report = await library.census!();
      const others = (list: MeshCensusWriter[] | undefined): MeshCensusWriter[] => (list ?? []).filter(writer => writer.pid !== process.pid);
      const advisory: MeshCensusAdvisory = { writers: others(report.writers), unknown: others(report.unknown) };
      if (options.json) write(`${JSON.stringify({ command: "census", root: options.root, advisory: true, ...advisory }, null, 2)}\n`);
      else write(`${formatAdvisory(advisory).join("\n")}\n`);
      return 0;
    }
    // Mutating commands log the census report as advisory on stderr; it never decides anything.
    library.onAdvisory = (advisory) => {
      warn(`fabric-mesh-backend: ${describeCensusAdvisory(advisory)}\n`);
      io.options?.onAdvisory?.(advisory);
    };
    if (options.command === "reader-proof") {
      const proved = options.proofFile !== undefined
        ? { ...(await installReaderProof(options.root, options.name!, fs.readFileSync(options.proofFile, "utf8"))), entries: undefined }
        : await proveFabricReader(options.root, { backend: options.backend!, ...(options.name === undefined ? {} : { name: options.name }),
          ...(options.readerVersion === undefined ? {} : { version: options.readerVersion }) });
      write(`${options.json ? JSON.stringify({ command: "reader-proof", ok: true, ...proved }, null, 2)
        : `reader-proof done: ${proved.proof.name} ${proved.entries === undefined ? "installed" : `read ${options.backend} (${proved.entries} entries)`}, `
          + `backends ${proved.proof.backends.join(",")} at epoch ${proved.proof.provedEpoch}, release ${proved.proof.release}: ${proved.file}`}\n`);
      return 0;
    }
    if (options.command === "status") {
      const status = await meshBackendStatus(options.root, library);
      write(`${options.json ? JSON.stringify(status, null, 2) : formatStatus(status)}\n`);
      return status.fenceHolds && "source" in status.reader ? 0 : 3;
    }
    // The readiness gate (smarty-dev#7815): every registered reader proved the target backend.
    if (options.command === "import" || options.command === "cutover") gate = await openGate(options, library, io.builtinReaders ?? BUILTIN_READERS,
      io.gateCensus ?? meshWriterCensusSync(options.root), io.procScan ?? {}, io.leaseProbe ?? {}, warn);
    const result: Record<string, unknown> = { ...(await (options.command === "import" ? importMeshState(options.root, library)
      : options.command === "cutover" ? cutoverMeshState(options.root, library)
        : options.command === "rollback" ? rollbackMeshState(options.root, library)
          : abortMeshRollback(options.root, library))) };
    if (gate) {
      const final = gate.final ?? gate.readiness;
      // Accepted under the fence too: every one was alarmed before the fence (the commit hook refused otherwise).
      if (gate.overrideAlarm) gate.overrideAlarm.accepted = final.unready.map(reader => reader.name);
      result.readers = { switchId: gate.switchId, ready: final.ready, acceptedUnready: final.unready, checkedUnderFence: gate.checked };
      if (gate.overrideAlarm) result.overrideAlarm = gate.overrideAlarm;
      result.switchRecord = closeGate(options.root, gate, warn, { ok: true, epoch: result.epoch, converged: result.converged,
        checkedUnderFence: gate.checked, ownStateDbFds: gate.ownStateDbFds, required: gate.final?.required ?? gate.required, readersReady: final.ready, acceptedUnready: final.unready,
        ...(gate.overrideAlarm ? { overrideAlarm: gate.overrideAlarm } : {}) });
    }
    write(`${options.json ? JSON.stringify({ command: options.command, ok: true, ...result, alarms }, null, 2) : formatResult(options.command, result)}\n`);
    return 0;
  } catch (error) {
    const refused = error instanceof MeshBackendRefusedError || error instanceof MeshBackendFenceError;
    const message = error instanceof Error ? error.message : String(error);
    if (gate) closeGate(options.root, gate, warn, { ok: false, refused, error: message });
    if (options.json) {
      const overrideAlarm = (error as { overrideAlarm?: unknown }).overrideAlarm;
      write(`${JSON.stringify({ command: options.command, ok: false, refused, code: (error as { code?: unknown }).code, error: message,
        ...(overrideAlarm ? { overrideAlarm } : {}), alarms }, null, 2)}\n`);
    }
    warn(`fabric-mesh-backend: ${options.command} ${refused ? "refused" : "failed"}: ${message}\n`);
    return refused ? 3 : 1;
  } finally { gate?.stop?.(); }
};
