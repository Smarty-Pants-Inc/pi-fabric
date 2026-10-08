// fabric-mesh-backend: import, cutover, rollback and status of a mesh root's state backend
// (smarty-dev#6477 L4b). The fence and the commit order live in backend-migration.ts; see
// docs/mesh-lock-plan.md section 5.
import os from "node:os";
import path from "node:path";
import { census as writerCensus } from "./writer-census.js";
import { abortMeshRollback, cutoverMeshState, importMeshState, meshBackendStatus, MeshBackendFenceError, MeshBackendRefusedError,
  rollbackMeshState, type MeshBackendAlarm, type MeshBackendOptions, type MeshBackendStatus, type MeshCensusWriter,
  type MeshWriterCensus } from "./backend-migration.js";

const COMMANDS = ["census", "import", "status", "cutover", "rollback", "abort-rollback"] as const;
type Command = typeof COMMANDS[number];

const USAGE = `Usage: fabric-mesh-backend <census|import|status|cutover|rollback|abort-rollback> --root DIR [--json]
                          [--assume-no-writers] [--lock-protocol 1|2] [--lock-timeout-ms N]

  census          the writer census (writer-census.ts): every process that may write the root, its
                  mode and release. Exit 0 when it shows none, 3 otherwise.
  status          backend flag, epochs, digests, the reader decision and the census writers.
  import          the cutover section (also the roll forward): state.json -> state.db under the
                  mesh .lock at backend=importing E+1, digest verified both ways, state.json replaced
                  by the moved marker, then backend=sqlite. Refuses a diverged sqlite root.
  cutover         import, refused unless the writer census shows no file-mode writer.
  rollback        the R1 fence: flag exporting (E+1), export to a verified temp, switch to file,
                  then replace the marker (last). A rerun converges from the stored flag.
                  Refused while the census shows writers.
  abort-rollback  under .lock: restore the marker, then exporting -> sqlite, keeping epoch E+1.

import, cutover and rollback run the writer census. A writer this host cannot verify (unknown: no valid
metadata, no build SHA, or only lock evidence) or one on another host refuses them unless
--assume-no-writers: the operator attests that every such writer (Pi sessions, actors, mesh-bridge, the
projector) is stopped. A verified writer on this host still refuses (file or shadow mode for cutover and
import, any mode for rollback), with or without --assume-no-writers.
Exit status: 0 done, 1 error, 2 usage, 3 refused or fence violation (nothing unsafe was done).`;

/**
 * smarty-dev#6477 W1: L4a's writer census as the L4b census provider. A verified local writer keeps
 * its mode (file, shadow, sqlite). An unknown writer or one on another host (this host cannot see
 * its process) gets a mode no operation accepts, unless the operator attests it is stopped.
 */
export const meshWriterCensus = (root: string, assumeNoWriters = false): MeshWriterCensus => async () => {
  const result = await writerCensus(root);
  const host = os.hostname();
  const unknown = new Set(result.unknown);
  const writers: MeshCensusWriter[] = [];
  for (const writer of result.writers) {
    const foreign = writer.host !== undefined && writer.host !== host;
    if (!unknown.has(writer) && !foreign) {
      writers.push({ pid: writer.pid!, release: writer.releaseSha!, mode: writer.stateBackend! });
      continue;
    }
    if (assumeNoWriters) continue;
    const where = writer.name ? ` ${writer.source} ${writer.name}` : ` ${writer.source}`;
    writers.push({ pid: writer.pid ?? 0, release: writer.releaseSha ?? "unknown",
      mode: unknown.has(writer) ? `unknown${where}` : `foreign host ${writer.host} ${writer.stateBackend ?? "?"}${where}` });
  }
  return { writers };
};

interface Options { command: Command; root: string; json: boolean; assumeNoWriters: boolean; lockProtocol: 1 | 2; lockTimeoutMs?: number }

class UsageError extends Error {}

const parseArgs = (argv: string[]): Options | "help" => {
  if (argv.length === 0 || argv.includes("--help") || argv.includes("-h")) return "help";
  const [command, ...rest] = argv;
  if (!COMMANDS.includes(command as Command)) throw new UsageError(`Unknown command ${String(command)}`);
  let root: string | undefined;
  let json = false;
  let assumeNoWriters = false;
  let lockProtocol: 1 | 2 = 1;
  let lockTimeoutMs: number | undefined;
  for (let index = 0; index < rest.length; index++) {
    const flag = rest[index]!;
    if (flag === "--json") { json = true; continue; }
    if (flag === "--assume-no-writers") { assumeNoWriters = true; continue; }
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
    } else throw new UsageError(`Bad argument: ${flag}`);
  }
  if (!root) throw new UsageError("--root DIR is required");
  return { command: command as Command, root: path.resolve(root), json, assumeNoWriters, lockProtocol,
    ...(lockTimeoutMs === undefined ? {} : { lockTimeoutMs }) };
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
  if (status.censusError) lines.push(`census        failed: ${status.censusError}`);
  else if (status.writers) {
    lines.push(`writers       ${status.writers.length}`);
    for (const writer of status.writers) lines.push(`  pid ${writer.pid}  ${writer.mode}  ${writer.release}`);
  } else lines.push("writers       unknown (no census provider)");
  return lines.join("\n");
};

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
  const library: MeshBackendOptions = {
    ...io.options,
    lockProtocol: options.lockProtocol,
    assumeNoWriters: options.assumeNoWriters,
    onAlarm: (alarm) => { alarms.push(alarm); warn(`fabric-mesh-backend: ALARM ${alarm.code}: ${alarm.message}\n`); },
    census: io.census ?? meshWriterCensus(options.root, options.assumeNoWriters),
    ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }),
  };
  try {
    if (options.command === "census") {
      const writers = (await library.census!()).writers.filter(writer => writer.pid !== process.pid);
      if (options.json) write(`${JSON.stringify({ command: "census", root: options.root, writers }, null, 2)}\n`);
      else {
        write(`writers       ${writers.length}\n`);
        for (const writer of writers) write(`  pid ${writer.pid}  ${writer.mode}  ${writer.release}\n`);
      }
      return writers.length === 0 ? 0 : 3;
    }
    if (options.command === "status") {
      const status = await meshBackendStatus(options.root, library);
      write(`${options.json ? JSON.stringify(status, null, 2) : formatStatus(status)}\n`);
      return status.fenceHolds && "source" in status.reader ? 0 : 3;
    }
    const result: Record<string, unknown> = { ...(await (options.command === "import" ? importMeshState(options.root, library)
      : options.command === "cutover" ? cutoverMeshState(options.root, library)
        : options.command === "rollback" ? rollbackMeshState(options.root, library)
          : abortMeshRollback(options.root, library))) };
    write(`${options.json ? JSON.stringify({ command: options.command, ok: true, ...result, alarms }, null, 2) : formatResult(options.command, result)}\n`);
    return 0;
  } catch (error) {
    const refused = error instanceof MeshBackendRefusedError || error instanceof MeshBackendFenceError;
    const message = error instanceof Error ? error.message : String(error);
    if (options.json) {
      write(`${JSON.stringify({ command: options.command, ok: false, refused, code: (error as { code?: unknown }).code, error: message, alarms }, null, 2)}\n`);
    }
    warn(`fabric-mesh-backend: ${options.command} ${refused ? "refused" : "failed"}: ${message}\n`);
    if (refused && /census shows .*(unknown|foreign host)/.test(message)) {
      warn("fabric-mesh-backend: this host cannot verify those writers; stop them, then pass --assume-no-writers\n");
    }
    return refused ? 3 : 1;
  }
};
