import fs from "node:fs";
import { randomBytes } from "node:crypto";
import { basename, join, resolve } from "node:path";
import {
  RunLogCodecError, RUN_LOG_BYTE_CAP, assertRunLogDirectory, assertRunLogFile,
  closeRunLogFile, createRunLogFile, decodeOwnedRunLog, hashRunLogBytes, hashRunLogFile,
  holdRunLogDirectory, openRunLogFile, sealRunLogFile, syncRunLogDirectory,
  type HeldRunLogDirectory, type HeldRunLogFile,
} from "./run-log-compression.js";

/** Retention may recognize this name ONLY under its existing ownership/link/exit/age/reference fences. */
export const RUN_LOG_RESTORE_TEMP = /^\.events\.jsonl\.restore-[1-9][0-9]*-[0-9a-f]{32}\.tmp$/;
export interface RunLogRestore {
  kind: "dry-run" | "restored" | "already-restored";
  path: string;
  originalBytes: number;
}

/** Additive per-artifact rollback. No scan, no compressed-source deletion and no overwrite. */
export async function restoreRunLog(encodedPath: string, options: { apply?: boolean } = {}): Promise<RunLogRestore> {
  const path = resolve(encodedPath);
  if (basename(path) !== "events.jsonl.zst") throw new RunLogCodecError("invalid-path", path);
  const plainPath = path.slice(0, -4);
  let directory: HeldRunLogDirectory | undefined;
  let source: HeldRunLogFile | undefined;
  let plain: HeldRunLogFile | undefined;
  let temporary: HeldRunLogFile | undefined;
  let committed = false;
  let phase = "restore-custody";
  try {
    directory = holdRunLogDirectory(path);
    source = openRunLogFile(path);
    if (!source) throw new RunLogCodecError("source-missing", path);
    const decoded = await decodeOwnedRunLog(source);
    const expectedHash = hashRunLogBytes(decoded);
    plain = openRunLogFile(plainPath);
    if (plain) {
      if (plain.stat.size > BigInt(RUN_LOG_BYTE_CAP) || hashRunLogFile(plain) !== expectedHash) {
        throw new RunLogCodecError("content-mismatch", plainPath);
      }
      assertRunLogDirectory(directory);
      assertRunLogFile(source);
      assertRunLogFile(plain);
      return { kind: "already-restored", path: plainPath, originalBytes: decoded.length };
    }
    assertRunLogDirectory(directory);
    assertRunLogFile(source);
    if (options.apply !== true) return { kind: "dry-run", path: plainPath, originalBytes: decoded.length };
    phase = "restore-temp-create";
    const name = `.events.jsonl.restore-${process.pid}-${randomBytes(16).toString("hex")}.tmp`;
    if (!RUN_LOG_RESTORE_TEMP.test(name)) throw new RunLogCodecError("invalid-temp-name", path);
    temporary = createRunLogFile(join(directory.path, name));
    phase = "restore-temp-write";
    let offset = 0;
    while (offset < decoded.length) {
      const count = fs.writeSync(temporary.fd!, decoded, offset, decoded.length - offset, offset);
      if (count <= 0) throw new RunLogCodecError("short-write", temporary.path);
      offset += count;
    }
    fs.fsyncSync(temporary.fd!);
    sealRunLogFile(temporary);
    if (hashRunLogFile(temporary) !== expectedHash) throw new RunLogCodecError("restore-hash-mismatch", temporary.path);
    assertRunLogFile(source);
    syncRunLogDirectory(directory);
    // Windows cannot publish/retire files with our handles still open.
    closeRunLogFile(source);
    closeRunLogFile(temporary);
    assertRunLogDirectory(directory);
    assertRunLogFile(source);
    assertRunLogFile(temporary);
    phase = "restore-publish";
    // Atomic no-replace: EEXIST is a refusal, never permission to delete or rename over plain.
    fs.linkSync(temporary.path, plainPath);
    committed = true;
    phase = "restore-temp-cleanup";
    assertRunLogDirectory(directory);
    // link() deliberately changes nlink/ctime. Verify the two names against the held temp's
    // physical identity, size and mtime before accepting that one expected metadata transition.
    const alias = fs.lstatSync(plainPath, { bigint: true });
    const temp = fs.lstatSync(temporary.path, { bigint: true });
    for (const stat of [alias, temp]) {
      if (!stat.isFile() || stat.isSymbolicLink() || stat.dev !== temporary.stat.dev || stat.ino !== temporary.stat.ino
        || stat.nlink !== 2n || stat.uid !== temporary.stat.uid || stat.mode !== temporary.stat.mode
        || stat.size !== temporary.stat.size || stat.mtimeNs !== temporary.stat.mtimeNs) {
        throw new RunLogCodecError("restore-alias-changed", temporary.path, true);
      }
    }
    temporary.stat = temp;
    assertRunLogFile(temporary, 2n);
    phase = "restore-publish-sync";
    syncRunLogDirectory(directory);
    phase = "restore-temp-cleanup";
    assertRunLogFile(temporary, 2n);
    fs.unlinkSync(temporary.path); // Only this invocation's exact exclusively-created temporary.
    temporary = undefined;
    phase = "post-commit-sync";
    syncRunLogDirectory(directory);
    return { kind: "restored", path: plainPath, originalBytes: decoded.length };
  } catch (error) {
    // Preserve exclusively-created failed temporaries as evidence; never delete compressed source.
    if (error instanceof RunLogCodecError && error.committed === committed) throw error;
    throw new RunLogCodecError(phase, path, committed, error);
  } finally {
    for (const file of [source, plain, temporary, directory]) { try { closeRunLogFile(file); } catch { /* preserve primary error */ } }
  }
}

const usage = "Usage: fabric-restore-run-log [--apply] /path/to/events.jsonl.zst [...]\nDefault is dry-run. Keep this release installed until every required artifact is restored before downgrade.";
interface RestoreOutput { out(line: string): void; err(line: string): void }
export async function main(args: string[], io: RestoreOutput = {
  out: line => process.stdout.write(`${line}\n`),
  err: line => process.stderr.write(`${line}\n`),
}): Promise<number> {
  if (args.length === 1 && args[0] === "--help") { io.out(usage); return 0; }
  let apply = false;
  const paths: string[] = [];
  for (const argument of args) {
    if (argument === "--apply" && !apply) apply = true;
    else if (argument.startsWith("-")) { io.err(usage); return 2; }
    else paths.push(argument);
  }
  if (!paths.length) { io.err(usage); return 2; }
  let exit = 0;
  for (const path of paths) {
    try { io.out(JSON.stringify(await restoreRunLog(path, { apply }))); }
    catch (error) {
      exit = 1;
      io.err(JSON.stringify(error instanceof RunLogCodecError
        ? { code: error.code, reason: error.reason, path: error.path, committed: error.committed, message: error.message }
        : { path, message: error instanceof Error ? error.message : String(error) }));
    }
  }
  return exit;
}
