import fs from "node:fs";
import path from "node:path";
import {
  createNatsAccount,
  createNatsOperator,
  type NatsMintArtifact,
  NatsMintError,
  type NatsMintOutput,
  mintNatsBreakGlassUser,
  mintNatsSystemUser,
  mintNatsUser,
} from "./mint.js";
import { NatsServerConfigError, renderNatsServerConfig, validateNatsServerConfigInput } from "./server-config.js";

/**
 * `pi-fabric nats` (smarty-dev#7504 slice 1): explicit operator/self-hosted setup and per-call user mint.
 * Input is one JSON object on stdin only (never argv, environment or a credential file). Output goes to a new 0700
 * directory of exclusive, no-follow 0600 files; stdout carries public metadata only. Diagnostics never echo argv
 * values, input values, JWTs or seeds. Exit 0 success, 1 refusal, 2 usage.
 */

const USAGE = [
  "Usage: pi-fabric nats <command>",
  "",
  "  mint operator --out-dir <dir>                 operator identity, operator signing key, SYS account (no SYS user)",
  "  mint account --out-dir <dir>                  one org account with agent/principal/maintainer/break-glass scoped keys",
  "  mint user --out-dir <dir>                     one fresh agent/principal/maintainer user.creds",
  "  mint user --break-glass --out-dir <dir>       1 h break-glass user; approval and stop-first runbook are the caller's duty",
  "  mint user --sys --out-dir <dir>               1 h SYS-only user (resolver claims update and own inbox only)",
  "  config --input - --out <file>                 render a public nats-server config",
  "  send --to <id> --kind <followUp|steer> (--text <text>|--text-file <file>) ...",
  "       [--name <host-name>] [--await-reply-seconds <seconds>] [--org <org>] [--path-b-machines <csv>]",
  "",
  "send uses org NATS policy from PI_FABRIC_MESH_NATS or the PI_FABRIC_NATS_* policy variables.",
  "Input: one JSON object on stdin for mint/config. <dir> is an absolute path to a new directory; <file> an absolute path to a new file.",
].join("\n");

const MAX_INPUT_BYTES = 1 << 20;
const MAX_DRAIN_BYTES = 64 << 20;
const CONFIG_FIELDS = new Set(["bus", "serverName", "listen", "monitor", "jetstream", "operatorJwt", "systemAccount", "resolver", "preload", "cluster"]);

type Command =
  | { kind: "operator" | "account" | "user" | "break-glass" | "sys"; outDir: string }
  | { kind: "config"; out: string };

class CliRefusal extends Error {}

const absolute = (value: string | undefined): value is string => value !== undefined && path.isAbsolute(value) && !value.includes("\0");

/** Strict argv grammar: every flag at most once, no `--flag=value`, absolute paths only. Undefined means usage. */
function parse(argv: readonly string[]): Command | undefined {
  const [group, ...rest] = argv;
  const values = new Map<string, string>();
  const flags = new Set<string>();
  const valued = group === "config" ? ["--input", "--out"] : ["--out-dir"];
  const kind = group === "mint" ? rest.shift() : undefined;
  const booleans = kind === "user" ? ["--break-glass", "--sys"] : [];
  for (let index = 0; index < rest.length; index++) {
    const token = rest[index]!;
    if (valued.includes(token) && !values.has(token) && index + 1 < rest.length) values.set(token, rest[++index]!);
    else if (booleans.includes(token) && !flags.has(token)) flags.add(token);
    else return undefined;
  }
  if (group === "config") {
    const out = values.get("--out");
    return values.get("--input") === "-" && absolute(out) ? { kind: "config", out } : undefined;
  }
  const outDir = values.get("--out-dir");
  if (group !== "mint" || !absolute(outDir) || (kind !== "operator" && kind !== "account" && kind !== "user")) return undefined;
  if (flags.size > 1) return undefined;
  return { kind: flags.has("--break-glass") ? "break-glass" : flags.has("--sys") ? "sys" : kind, outDir };
}

async function readStdinJson(): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  try {
    for await (const chunk of process.stdin as AsyncIterable<Buffer>) {
      size += chunk.length;
      if (size <= MAX_INPUT_BYTES) chunks.push(chunk);
      else chunk.fill(0);
      if (size > MAX_DRAIN_BYTES) break;
    }
    if (size > MAX_INPUT_BYTES) throw new CliRefusal("stdin is larger than 1 MiB");
    try {
      return JSON.parse(Buffer.concat(chunks).toString("utf8"));
    } catch {
      throw new CliRefusal("stdin is not one JSON object");
    }
  } finally {
    for (const chunk of chunks) chunk.fill(0);
  }
}

function refuseExisting(target: string) {
  let exists = true;
  try {
    fs.lstatSync(target);
  } catch {
    exists = false;
  }
  if (exists) throw new CliRefusal("output path already exists");
  if (!fs.existsSync(path.dirname(target))) throw new CliRefusal("output parent directory does not exist");
}

function privateOutputUid(): number {
  const uid = process.getuid?.();
  if (uid === undefined) throw new CliRefusal("private output verification failed");
  return uid;
}

function privateStatMatches(stat: fs.Stats, uid: number, mode: number, kind: "directory" | "file"): boolean {
  return (
    stat.uid === uid &&
    (stat.mode & 0o7777) === mode &&
    !stat.isSymbolicLink() &&
    (kind === "directory" ? stat.isDirectory() : stat.isFile())
  );
}

type Identity = { dev: number; ino: number };

const CLEANUP_FAILED = "artifact cleanup failed: private output may remain";

function verifyPrivateDirectory(outDir: string, uid: number): Identity {
  try {
    const stat = fs.lstatSync(outDir);
    if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== uid) {
      throw new CliRefusal("private output verification failed");
    }
    fs.chmodSync(outDir, 0o700);
    const verified = fs.lstatSync(outDir);
    if (!privateStatMatches(verified, uid, 0o700, "directory")) {
      throw new CliRefusal("private output verification failed");
    }
    return { dev: verified.dev, ino: verified.ino };
  } catch {
    throw new CliRefusal("private output verification failed");
  }
}

function verifyPrivateFile(file: string, fd: number, uid: number): Identity {
  try {
    const descriptor = fs.fstatSync(fd);
    const entry = fs.lstatSync(file);
    if (
      !privateStatMatches(descriptor, uid, 0o600, "file") ||
      !privateStatMatches(entry, uid, 0o600, "file") ||
      descriptor.dev !== entry.dev ||
      descriptor.ino !== entry.ino
    ) {
      throw new CliRefusal("private output verification failed");
    }
    return { dev: descriptor.dev, ino: descriptor.ino };
  } catch {
    throw new CliRefusal("private output verification failed");
  }
}

/** Returns the verified inode identity when `ownerUid` is given (mint staging); config output passes none. */
function writePrivateFile(file: string, bytes: Uint8Array, ownerUid?: number): Identity | undefined {
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0);
  const fd = fs.openSync(file, flags, 0o600);
  try {
    fs.fchmodSync(fd, 0o600);
    const identity = ownerUid === undefined ? undefined : verifyPrivateFile(file, fd, ownerUid);
    for (let offset = 0; offset < bytes.length; ) offset += fs.writeSync(fd, bytes, offset);
    fs.fsyncSync(fd);
    return identity;
  } finally {
    fs.closeSync(fd);
  }
}

/** A renamed artifact must still be the exact private inode that was staged. */
function verifyPublishedFile(file: string, staged: Identity, uid: number) {
  try {
    const entry = fs.lstatSync(file);
    if (!privateStatMatches(entry, uid, 0o600, "file") || entry.dev !== staged.dev || entry.ino !== staged.ino) {
      throw new CliRefusal("private output verification failed");
    }
  } catch {
    throw new CliRefusal("private output verification failed");
  }
}

function syncDirectory(dir: string) {
  const flags = (fs.constants.O_RDONLY ?? 0) | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0);
  const fd = fs.openSync(dir, flags);
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Unlinks only target paths that still hold the inode we staged and renamed there. The match is dev/ino only (not
 * link/mode), so a path whose metadata failed post-rename verification is still removed if it is ours. A different
 * inode is a foreign replacement and is preserved; an already-missing path is already gone. Any other error is
 * recorded and the remaining paths are still attempted. Returns false if any of our paths may remain.
 */
function rollbackPublished(published: ReadonlyArray<Identity & { file: string }>): boolean {
  let complete = true;
  for (const entry of published) {
    try {
      const current = fs.lstatSync(entry.file);
      if (current.dev === entry.dev && current.ino === entry.ino) fs.unlinkSync(entry.file);
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code !== "ENOENT") complete = false;
    }
  }
  return complete;
}

/** Recursive removal only of the staging directory we created, re-checked against its captured identity first. */
function removeStage(stage: string, identity: Identity): boolean {
  try {
    const current = fs.lstatSync(stage);
    if (!current.isDirectory() || current.isSymbolicLink() || current.dev !== identity.dev || current.ino !== identity.ino) {
      return false;
    }
    fs.rmSync(stage, { recursive: true });
    return true;
  } catch {
    return false;
  }
}

/**
 * Per-file staged publication (smarty-dev#11682). Ordering is the trust argument:
 * 1. a private sibling mkdtemp directory (0700, this UID, not a link) is verified before any secret byte is written;
 * 2. every artifact is staged there exclusively (O_EXCL|O_NOFOLLOW, 0600, fd/path owner+mode+inode checks), fully
 *    written, fsynced and closed, and its staged dev/ino recorded;
 * 3. only after all succeed is the target created non-recursively at 0700 (an existing path fails) and verified; each
 *    file is then renamed individually, recorded, and verified at its final path as the same private inode; the target
 *    directory is fsynced; the empty stage is removed.
 * On any failure only recorded, still-identical target inodes are unlinked and only our verified stage is removed; a
 * pre-existing target is never touched, and an empty target created here may remain. If cleanup cannot be completed
 * the refusal says so instead of reporting the original error. Same-UID trust boundary is unchanged (#11465).
 */
function writeArtifacts(outDir: string, artifacts: readonly NatsMintArtifact[]) {
  const uid = privateOutputUid();
  const stage = fs.mkdtempSync(path.join(path.dirname(outDir), ".pi-fabric-nats-stage-"));
  // If the fresh stage cannot be verified as ours it is not removed; nothing secret has been written into it yet.
  const stageIdentity = verifyPrivateDirectory(stage, uid);
  const published: Array<Identity & { file: string }> = [];
  try {
    const staged = artifacts.map((artifact) => {
      const file = path.join(stage, artifact.name);
      const identity = writePrivateFile(file, artifact.bytes, uid);
      if (!identity) throw new CliRefusal("private output verification failed");
      return { name: artifact.name, file, identity };
    });
    fs.mkdirSync(outDir, { mode: 0o700 });
    verifyPrivateDirectory(outDir, uid);
    for (const entry of staged) {
      const file = path.join(outDir, entry.name);
      fs.renameSync(entry.file, file);
      published.push({ file, ...entry.identity });
      verifyPublishedFile(file, entry.identity, uid);
    }
    syncDirectory(outDir);
    if (!removeStage(stage, stageIdentity)) throw new CliRefusal(CLEANUP_FAILED);
  } catch (error) {
    const rolledBack = rollbackPublished(published);
    const stageRemoved = removeStage(stage, stageIdentity);
    if (!rolledBack || !stageRemoved) throw new CliRefusal(CLEANUP_FAILED);
    throw error;
  }
}

function mint(kind: Exclude<Command["kind"], "config">, input: unknown): NatsMintOutput<object> {
  switch (kind) {
    case "operator":
      return createNatsOperator(input);
    case "account":
      return createNatsAccount(input);
    case "user":
      return mintNatsUser(input);
    case "break-glass":
      return mintNatsBreakGlassUser(input);
    case "sys":
      return mintNatsSystemUser(input);
  }
}

/** Fixed, value-free reason text. Config field paths are named only at their known top-level segment. */
function reason(error: unknown): string {
  if (error instanceof CliRefusal) return error.message;
  if (error instanceof NatsMintError) return error.message;
  if (error instanceof NatsServerConfigError) {
    const top = error.field.split(".")[0] ?? "";
    return CONFIG_FIELDS.has(top) ? `invalid config field ${top}` : "invalid config input";
  }
  const code = (error as { code?: unknown } | null)?.code;
  if (code === "EEXIST") return "output path already exists";
  if (code === "ENOENT") return "output parent directory does not exist";
  if (code === "ELOOP") return "output path is a symbolic link";
  return "internal error";
}

export async function runNatsCli(argv: string[]): Promise<number> {
  if (argv[0] === "send") return (await import("./send-cli.js")).runNatsSendCli(argv.slice(1));
  if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h" || argv[0] === "help")) {
    process.stdout.write(`${USAGE}\n`);
    return 0;
  }
  const command = parse(argv);
  if (!command) {
    process.stderr.write(`pi-fabric nats: usage error\n${USAGE}\n`);
    return 2;
  }
  try {
    if (command.kind !== "config" && process.platform === "win32") {
      throw new CliRefusal("Windows nats mint is refused (smarty-dev#11482): private ACL verification is unavailable");
    }
    const target = command.kind === "config" ? command.out : command.outDir;
    refuseExisting(target);
    const input = await readStdinJson();
    if (command.kind === "config") {
      const rendered = renderNatsServerConfig(validateNatsServerConfigInput(input));
      writePrivateFile(command.out, new TextEncoder().encode(rendered));
      process.stdout.write(`${JSON.stringify({ ok: true, kind: "config", out: command.out })}\n`);
      return 0;
    }
    const output = mint(command.kind, input);
    try {
      writeArtifacts(command.outDir, output.artifacts);
    } finally {
      for (const artifact of output.artifacts) artifact.bytes.fill(0);
    }
    const files = output.artifacts.map((artifact) => artifact.name);
    process.stdout.write(`${JSON.stringify({ ok: true, ...output.metadata, outDir: command.outDir, files })}\n`);
    return 0;
  } catch (error) {
    process.stderr.write(`pi-fabric nats: refused: ${reason(error)}\n`);
    return 1;
  }
}
