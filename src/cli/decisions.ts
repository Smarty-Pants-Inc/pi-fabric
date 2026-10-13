import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshStore } from "../mesh/store.js";
import { decisionEscalation, DecisionStore, type DecisionRecord, type DecisionStatus } from "../decisions/store.js";

// `pi-fabric decisions list|answer`: answer durable Fabric decisions from
// outside any Pi session. Authority is local file access to the mesh root
// (one OS user); answers record `via: "cli"` and the OS user name.

const USAGE = `Usage:
  pi-fabric decisions list --root <meshRoot> [--status open|answered|expired|cancelled|all] [--holder <h>] [--limit <n>] [--json]
  pi-fabric decisions answer --root <meshRoot> <id> (--option <optionId> | --text <text>) [--json]

--root defaults to $PI_FABRIC_MESH_ROOT.`;

// Mirrors the default mesh.maxEventBytes / mesh.maxReadEvents.
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;
const STATUSES = new Set(["open", "answered", "expired", "cancelled", "all"]);
const VALUE_FLAGS = new Set(["root", "status", "holder", "limit", "option", "text"]);

interface CliIo {
  stdout: Pick<NodeJS.WritableStream, "write">;
  stderr: Pick<NodeJS.WritableStream, "write">;
  env?: NodeJS.ProcessEnv;
}

class UsageError extends Error {}

const parseArgs = (argv: readonly string[]): { positionals: string[]; flags: Map<string, string | true> } => {
  const positionals: string[] = [];
  const flags = new Map<string, string | true>();
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!;
    if (!arg.startsWith("--")) {
      positionals.push(arg);
      continue;
    }
    const name = arg.slice(2);
    if (name === "json" || name === "help") {
      flags.set(name, true);
      continue;
    }
    if (!VALUE_FLAGS.has(name)) throw new UsageError(`Unknown option: ${arg}`);
    const value = argv[index + 1];
    if (value === undefined) throw new UsageError(`Missing value for ${arg}`);
    flags.set(name, value);
    index += 1;
  }
  return { positionals, flags };
};

const operator = (): string => {
  try {
    return os.userInfo().username || "user";
  } catch {
    return "user";
  }
};

const line = (record: DecisionRecord): string => {
  const options = record.options ? ` [${record.options.map((option) => `${option.id}=${option.label}`).join(", ")}]` : "";
  const deadline = record.deadline ? ` deadline=${new Date(record.deadline).toISOString()}` : "";
  const escalation = decisionEscalation(record);
  const chain = escalation
    ? ` hop ${escalation.hop + 1}/${escalation.chain.length} chain=${escalation.chain.join(">")}`
    : "";
  return `${record.id}  ${record.status}  ${record.kind}  holder=${record.holder}${chain}${deadline}  ${record.title}${options}`;
};

export const runDecisionsCli = async (
  argv: string[],
  io: CliIo = { stdout: process.stdout, stderr: process.stderr },
): Promise<number> => {
  try {
    const [command, ...rest] = argv;
    const { positionals, flags } = parseArgs(rest);
    if (!command || command === "help" || command === "--help" || flags.has("help")) {
      io.stdout.write(`${USAGE}\n`);
      return command ? 0 : 2;
    }
    const rootFlag = flags.get("root");
    const root = typeof rootFlag === "string" ? rootFlag : (io.env ?? process.env).PI_FABRIC_MESH_ROOT;
    if (!root) throw new UsageError("Missing --root <meshRoot>");
    const meshRoot = path.resolve(root);
    if (!fs.statSync(meshRoot, { throwIfNoEntry: false })?.isDirectory()) throw new Error(`No Fabric mesh root at ${meshRoot}`);
    const user = operator();
    const store = new DecisionStore(
      new MeshStore(meshRoot, MAX_EVENT_BYTES, MAX_READ_EVENTS),
      { id: `user:${user}`, name: user, kind: "main" },
    );
    const json = flags.get("json") === true;
    if (command === "list") {
      if (positionals.length > 0) throw new UsageError(`Unexpected argument: ${positionals[0]}`);
      const status = flags.get("status") ?? "open";
      if (typeof status !== "string" || !STATUSES.has(status)) throw new UsageError(`Invalid --status: ${String(status)}`);
      const limitFlag = flags.get("limit");
      const limit = limitFlag === undefined ? 50 : Number(limitFlag);
      if (!Number.isInteger(limit) || limit < 1 || limit > 200) throw new UsageError("--limit must be 1..200");
      const holder = flags.get("holder");
      const records = await store.list({
        ...(status !== "all" ? { status: status as DecisionStatus } : {}),
        ...(typeof holder === "string" ? { holder } : {}),
        limit,
      });
      io.stdout.write(json
        ? `${JSON.stringify(records, null, 2)}\n`
        : records.length > 0 ? `${records.map(line).join("\n")}\n` : "No decisions\n");
      return 0;
    }
    if (command === "answer") {
      const [id, extra] = positionals;
      if (!id || extra !== undefined) throw new UsageError("answer takes exactly one decision id");
      const option = flags.get("option");
      const text = flags.get("text");
      if (option === undefined && text === undefined) throw new UsageError("answer needs --option or --text");
      const record = await store.answer(id, {
        ...(typeof option === "string" ? { optionId: option } : {}),
        ...(typeof text === "string" ? { text } : {}),
      }, { answeredBy: user, via: "cli" });
      io.stdout.write(json ? `${JSON.stringify(record, null, 2)}\n` : `${line(record)}\n`);
      return 0;
    }
    throw new UsageError(`Unknown decisions command: ${command}`);
  } catch (error) {
    io.stderr.write(`pi-fabric decisions: ${error instanceof Error ? error.message : String(error)}\n`);
    if (error instanceof UsageError) {
      io.stderr.write(`${USAGE}\n`);
      return 2;
    }
    return 1;
  }
};
