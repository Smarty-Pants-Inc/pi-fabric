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
import { blockingReaders, isReaderName, proveReader, readerReadiness, recordSwitch, type ReaderBackend, type ReaderReadiness } from "./reader-proof.js";

const COMMANDS = ["census", "import", "status", "cutover", "rollback", "abort-rollback", "reader-proof"] as const;
type Command = typeof COMMANDS[number];

const USAGE = `Usage: fabric-mesh-backend <census|import|status|cutover|rollback|abort-rollback> --root DIR [--json]
                          [--lock-protocol 1|2] [--lock-timeout-ms N] [--accept-unready NAME,...] [--accept-empty-registry]
       fabric-mesh-backend reader-proof --root DIR --name NAME --backend file|sqlite [--install-root DIR]
                          [--reader-version V] [--json]

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
  reader-proof    a real read of the backend with Fabric's MeshStore (sqlite: a migrated scratch copy
                  of state.json), then the proof in <root>/readers/NAME.json, bound to the release
                  of --install-root (or the entry's installRoot). Only for NAME fabric or an entry
                  with implementation fabric-meshstore; other readers write their own proof.

Readiness gate (smarty-dev#7815, docs/mesh-backend.md): import and cutover refuse (exit 3) unless
every reader in <root>/readers/ holds a valid proof of sqlite at the current epoch, made within 24 h
by the release installed now. --accept-unready NAME,... and --accept-empty-registry override; the
intent (with the overrides) is written to <root>/backend-switches.jsonl before the switch, and the
check is repeated under the fence right before the commit.

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
  acceptUnready: string[]; acceptEmptyRegistry: boolean; name?: string; backend?: ReaderBackend; readerVersion?: string; installRoot?: string;
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
  let installRoot: string | undefined;
  let acceptEmptyRegistry = false;
  for (let index = 0; index < rest.length; index++) {
    const flag = rest[index]!;
    if (flag === "--json") { json = true; continue; }
    if (flag === "--accept-empty-registry") { acceptEmptyRegistry = true; continue; }
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
    else if (flag === "--install-root") installRoot = path.resolve(value);
    else throw new UsageError(`Bad argument: ${flag}`);
  }
  if (!root) throw new UsageError("--root DIR is required");
  if (command === "reader-proof" && (!name || !backend)) throw new UsageError("reader-proof needs --name NAME and --backend file|sqlite");
  return { command: command as Command, root: path.resolve(root), json, lockProtocol, acceptUnready, acceptEmptyRegistry,
    ...(installRoot === undefined ? {} : { installRoot }),
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

interface Gate { switchId: string; readiness: ReaderReadiness; final?: ReaderReadiness }

/**
 * The readiness gate of a switch to sqlite (smarty-dev#7815). Only a switch that starts from file is
 * gated; a rerun after a crash (importing, sqlite) converges. Scans the registry (refuse early), writes
 * the intent with its overrides (refuse if it cannot), then installs `beforeCommit`, which repeats the
 * whole check under the fence inside the import transaction, so a proof changed since the scan counts.
 */
const openGate = async (options: Options, library: MeshBackendOptions, warn: (text: string) => void): Promise<Gate | undefined> => {
  // No mesh root: the switch itself refuses with nothing changed.
  if (!fs.statSync(options.root, { throwIfNoEntry: false })?.isDirectory()) return undefined;
  const { backend, epoch } = await meshBackendStatus(options.root);
  if (backend !== "file" && backend !== "none") return undefined;
  const accept = { unready: options.acceptUnready, emptyRegistry: options.acceptEmptyRegistry };
  const check = (readiness: ReaderReadiness, when: string): void => {
    const blocking = blockingReaders(readiness, accept);
    if (blocking.length > 0) {
      throw new MeshBackendRefusedError(`readers not ready for sqlite${when}: ${blocking.join("; ")}; `
        + "each reader proves a real read (docs/mesh-backend.md), or pass --accept-unready NAME,...");
    }
  };
  const readiness = readerReadiness(options.root, "sqlite", epoch);
  for (const reader of readiness.unready) {
    warn(`fabric-mesh-backend: reader ${reader.name} not ready: ${reader.reason}${accept.unready.includes(reader.name) ? " (accepted by --accept-unready)" : ""}\n`);
  }
  check(readiness, "");
  const empty = readiness.ready.length + readiness.unready.length === 0;
  if (empty) warn(`fabric-mesh-backend: no reader registered in ${options.root}/readers (accepted by --accept-empty-registry)\n`);
  const gate: Gate = { switchId: randomUUID(), readiness };
  try {
    recordSwitch(options.root, { switchId: gate.switchId, phase: "intent", command: options.command, backend: "sqlite", fromEpoch: epoch,
      readersReady: readiness.ready, acceptedUnready: readiness.unready, acceptedEmptyRegistry: empty });
  } catch (error) {
    throw new MeshBackendRefusedError(`cannot write the switch record ${options.root}/backend-switches.jsonl: ${(error as Error).message}`);
  }
  const outer = library.beforeCommit;
  library.beforeCommit = (before) => {
    outer?.(before);
    if (before.backend !== "file") return;
    const now = readerReadiness(options.root, "sqlite", before.epoch);
    check(now, " (under the fence, before the commit)");
    gate.final = now;
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
      const proved = await proveReader(options.root, { name: options.name!, backend: options.backend!,
        ...(options.readerVersion === undefined ? {} : { version: options.readerVersion }),
        ...(options.installRoot === undefined ? {} : { installRoot: options.installRoot }) });
      write(`${options.json ? JSON.stringify({ command: "reader-proof", ok: true, ...proved }, null, 2)
        : `reader-proof done: ${proved.proof.name} read ${options.backend} (${proved.entries} entries), backends ${proved.proof.backends.join(",")} at epoch ${proved.proof.provedEpoch}, release ${proved.proof.release}: ${proved.file}`}\n`);
      return 0;
    }
    if (options.command === "status") {
      const status = await meshBackendStatus(options.root, library);
      write(`${options.json ? JSON.stringify(status, null, 2) : formatStatus(status)}\n`);
      return status.fenceHolds && "source" in status.reader ? 0 : 3;
    }
    // The readiness gate (smarty-dev#7815): every registered reader proved the target backend.
    if (options.command === "import" || options.command === "cutover") gate = await openGate(options, library, warn);
    const result: Record<string, unknown> = { ...(await (options.command === "import" ? importMeshState(options.root, library)
      : options.command === "cutover" ? cutoverMeshState(options.root, library)
        : options.command === "rollback" ? rollbackMeshState(options.root, library)
          : abortMeshRollback(options.root, library))) };
    if (gate) {
      const final = gate.final ?? gate.readiness;
      result.readers = { switchId: gate.switchId, ready: final.ready, acceptedUnready: final.unready,
        acceptedEmptyRegistry: final.ready.length + final.unready.length === 0 };
      result.switchRecord = closeGate(options.root, gate, warn, { ok: true, epoch: result.epoch, converged: result.converged,
        underFence: gate.final !== undefined, readersReady: final.ready, acceptedUnready: final.unready });
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
