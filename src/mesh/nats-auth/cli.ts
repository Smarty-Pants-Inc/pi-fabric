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
  "",
  "Input: one JSON object on stdin. <dir> is an absolute path to a new directory; <file> an absolute path to a new file.",
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

function verifyPrivateDirectory(outDir: string, uid: number) {
  try {
    const stat = fs.lstatSync(outDir);
    if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== uid) {
      throw new CliRefusal("private output verification failed");
    }
    fs.chmodSync(outDir, 0o700);
    if (!privateStatMatches(fs.lstatSync(outDir), uid, 0o700, "directory")) {
      throw new CliRefusal("private output verification failed");
    }
  } catch {
    throw new CliRefusal("private output verification failed");
  }
}

function verifyPrivateFile(file: string, fd: number, uid: number) {
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
  } catch {
    throw new CliRefusal("private output verification failed");
  }
}

function writePrivateFile(file: string, bytes: Uint8Array, ownerUid?: number) {
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0);
  const fd = fs.openSync(file, flags, 0o600);
  try {
    fs.fchmodSync(fd, 0o600);
    if (ownerUid !== undefined) verifyPrivateFile(file, fd, ownerUid);
    for (let offset = 0; offset < bytes.length; ) offset += fs.writeSync(fd, bytes, offset);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

/** New 0700 directory (non-recursive, so an existing path, link or missing parent fails), then exclusive files. */
function writeArtifacts(outDir: string, artifacts: readonly NatsMintArtifact[]) {
  const uid = privateOutputUid();
  fs.mkdirSync(outDir, { mode: 0o700 });
  verifyPrivateDirectory(outDir, uid);
  // A failure part-way keeps what was written: nothing here deletes product artifacts.
  for (const artifact of artifacts) writePrivateFile(path.join(outDir, artifact.name), artifact.bytes, uid);
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
