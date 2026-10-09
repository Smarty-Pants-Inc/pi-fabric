// fabric-mesh-backend: import, cutover, rollback and status of a mesh root's state backend
// (smarty-dev#6477 L4b). The fence and the commit order live in backend-migration.ts; see
// docs/mesh-lock-plan.md section 5.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { census as writerCensus, type CensusWriter } from "./writer-census.js";
import { abortMeshRollback, cutoverMeshState, describeCensusAdvisory, importMeshState, meshBackendStatus, MeshBackendFenceError,
  MeshBackendRefusedError, rollbackMeshState, type MeshBackendAlarm, type MeshBackendOptions, type MeshBackendStatus,
  type MeshCensusAdvisory, type MeshCensusWriter, type MeshWriterCensus } from "./backend-migration.js";
import { BUILTIN_READERS, installReaderProof, isReaderName, proveFabricReader, readerReadiness, recordSwitch, requiredReaders,
  type InstalledReader, type ReaderBackend, type ReaderReadiness, type RequiredReader } from "./reader-proof.js";

const COMMANDS = ["census", "import", "status", "cutover", "rollback", "abort-rollback", "reader-proof"] as const;
type Command = typeof COMMANDS[number];

const USAGE = `Usage: fabric-mesh-backend <census|import|status|cutover|rollback|abort-rollback> --root DIR [--json]
                          [--lock-protocol 1|2] [--lock-timeout-ms N] [--accept-unready NAME,...]
                          [--require-reader NAME=INSTALL_ROOT]...
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
only override. The intent is written to <root>/backend-switches.jsonl before the switch; the check
runs again under the fence right before each flag commit (importing, sqlite), crash reruns included.

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
export const meshWriterCensus = (root: string): MeshWriterCensus => async () => {
  const result = await writerCensus(root);
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
    else if (flag === "--require-reader") {
      const [readerName, installRoot] = [value.slice(0, value.indexOf("=")), value.slice(value.indexOf("=") + 1)];
      if (!value.includes("=") || !isReaderName(readerName) || readerName.startsWith("fabric") || !path.isAbsolute(installRoot)) {
        throw new UsageError(`Bad --require-reader ${value} (NAME=/absolute/release/root; NAME not fabric*)`);
      }
      requireReaders.push({ name: readerName, installRoot });
    }
    else throw new UsageError(`Bad argument: ${flag}`);
  }
  if (!root) throw new UsageError("--root DIR is required");
  if (command === "reader-proof" && (proofFile ? !name || backend : !backend)) {
    throw new UsageError("reader-proof needs --backend file|sqlite, or --name NAME --proof-file FILE");
  }
  return { command: command as Command, root: path.resolve(root), json, lockProtocol, acceptUnready, requireReaders,
    ...(proofFile === undefined ? {} : { proofFile }),
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

interface Gate { switchId: string; required: RequiredReader[]; readiness: ReaderReadiness; final?: ReaderReadiness; checked: string[] }

/**
 * The readiness gate of a switch to sqlite (smarty-dev#7815). The required readers come from the
 * inventory (live writers' releases from the census, built-in installed readers, --require-reader),
 * never from the registry. Scans (refuse early), writes the intent (refuse if it cannot), then installs
 * `beforeCommit`: the whole check again under the fence right before each flag commit, crash reruns too.
 */
const openGate = async (options: Options, library: MeshBackendOptions, builtins: readonly InstalledReader[],
  warn: (text: string) => void): Promise<Gate | undefined> => {
  // No mesh root: the switch itself refuses with nothing changed.
  if (!fs.statSync(options.root, { throwIfNoEntry: false })?.isDirectory()) return undefined;
  const { backend, epoch } = await meshBackendStatus(options.root);
  // sqlite: switched already, a rerun only verifies (no commit); exporting/file+marker: the switch refuses.
  if (backend !== "file" && backend !== "none" && backend !== "importing") return undefined;
  // Attributed writers only: evidence the census cannot attribute (a pid-only lock owner, a torn record)
  // never gates (smarty-dev#6982); it is recorded in the intent. A census failure blocks as fabric@unknown.
  let writers: MeshCensusWriter[];
  let unattributed: MeshCensusWriter[] = [];
  try {
    const report = await library.census?.();
    writers = (report?.writers ?? []).filter(writer => writer.pid !== process.pid);
    unattributed = (report?.unknown ?? []).filter(writer => writer.pid !== process.pid);
  } catch (error) {
    writers = [{ pid: 0, release: "unknown", mode: `census failed: ${(error as Error).message}` }];
  }
  const required = requiredReaders(writers, builtins, options.requireReaders);
  const evaluate = (window: { fromEpoch: number; toEpoch: number }, when: string): ReaderReadiness => {
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
  const preview = (() => { try { return readerReadiness(options.root, "sqlite", required, window); } catch { return undefined; } })();
  for (const reader of preview?.unready ?? []) {
    warn(`fabric-mesh-backend: reader ${reader.name} not ready: ${reader.reason}${options.acceptUnready.includes(reader.name) ? " (accepted by --accept-unready)" : ""}\n`);
  }
  const readiness = evaluate(window, "");
  const gate: Gate = { switchId: randomUUID(), required, readiness, checked: [] };
  try {
    recordSwitch(options.root, { switchId: gate.switchId, phase: "intent", command: options.command, backend: "sqlite", from: backend, ...window,
      required, requireReader: options.requireReaders, unattributedEvidence: unattributed, readersReady: readiness.ready, acceptedUnready: readiness.unready });
  } catch (error) {
    throw new MeshBackendRefusedError(`cannot write the switch record ${options.root}/backend-switches.jsonl: ${(error as Error).message}`);
  }
  const outer = library.beforeCommit;
  library.beforeCommit = (commit) => {
    outer?.(commit);
    gate.final = evaluate(commit, ` (under the fence, before the ${commit.phase} commit)`);
    gate.checked.push(commit.phase);
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
  /** The built-in installed readers; default BUILTIN_READERS (tests replace them). */
  builtinReaders?: readonly InstalledReader[];
  /** Extra library options (tests: onStep, open). */
  options?: Partial<MeshBackendOptions>;
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
    if (options.command === "import" || options.command === "cutover") gate = await openGate(options, library, io.builtinReaders ?? BUILTIN_READERS, warn);
    const result: Record<string, unknown> = { ...(await (options.command === "import" ? importMeshState(options.root, library)
      : options.command === "cutover" ? cutoverMeshState(options.root, library)
        : options.command === "rollback" ? rollbackMeshState(options.root, library)
          : abortMeshRollback(options.root, library))) };
    if (gate) {
      const final = gate.final ?? gate.readiness;
      result.readers = { switchId: gate.switchId, ready: final.ready, acceptedUnready: final.unready, checkedUnderFence: gate.checked };
      result.switchRecord = closeGate(options.root, gate, warn, { ok: true, epoch: result.epoch, converged: result.converged,
        checkedUnderFence: gate.checked, readersReady: final.ready, acceptedUnready: final.unready });
    }
    write(`${options.json ? JSON.stringify({ command: options.command, ok: true, ...result, alarms }, null, 2) : formatResult(options.command, result)}\n`);
    return 0;
  } catch (error) {
    const refused = error instanceof MeshBackendRefusedError || error instanceof MeshBackendFenceError;
    const message = error instanceof Error ? error.message : String(error);
    if (gate) closeGate(options.root, gate, warn, { ok: false, refused, error: message });
    if (options.json) {
      write(`${JSON.stringify({ command: options.command, ok: false, refused, code: (error as { code?: unknown }).code, error: message, alarms }, null, 2)}\n`);
    }
    warn(`fabric-mesh-backend: ${options.command} ${refused ? "refused" : "failed"}: ${message}\n`);
    return refused ? 3 : 1;
  }
};
