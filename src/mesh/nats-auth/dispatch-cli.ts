import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { openAndSend, type NatsSendOptions, type NatsSendReply } from "./send-cli.js";
import { bindDispatchRunContent } from "./dispatch-binding.js";
import { pathBMachines } from "./mint.js";

const USAGE = [
  "Usage: pi-fabric nats dispatch --run-id <runId> --task-file <f> --out <burst.jsonl>",
  "  --creds <absolute-file> --ca <absolute-file> --servers <tls://host:port[,tls://host:port...]> --machine <machine> --org <org>",
  "  [--path-b-machines <csv>] [--find-timeout-seconds 120] [--await-reply-seconds 300]",
  "",
  "Finds one live root presence by placementRunId, sends the task as followUp, and appends one JSONL result.",
].join("\n");

const MAX_TASK_BYTES = 512 * 1024;
const MAX_FIND_SECONDS = 60 * 60;
const MAX_AWAIT_SECONDS = 24 * 60 * 60;
const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/;

class DispatchCliError extends Error {}

type DispatchOptions = {
  runId: string;
  taskFile: string;
  out: string;
  creds: string;
  ca: string;
  servers: string[];
  machine: string;
  org: string;
  pathBMachines?: string[];
  findTimeoutSeconds: number;
  awaitReplySeconds: number;
};

type DispatchRecord = {
  runId: string;
  childSession: string | null;
  model: string | null;
  t_task_dispatched: string | null;
  t_ack: string | null;
  t_answer_received: string | null;
  answer_ok: boolean;
  answer_text_sha256: string | null;
  error?: string;
};

const absolute = (value: string | undefined, flag: string): string => {
  if (!value || !path.isAbsolute(value) || value.includes("\0")) throw new DispatchCliError(`${flag} must be an absolute path`);
  return value;
};

function parse(argv: readonly string[]): DispatchOptions | "help" {
  if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h" || argv[0] === "help")) return "help";
  const valued = new Set([
    "--run-id", "--task-file", "--out", "--creds", "--ca", "--servers", "--machine", "--org",
    "--path-b-machines", "--find-timeout-seconds", "--await-reply-seconds",
  ]);
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index]!;
    if (!valued.has(flag) || values.has(flag) || index + 1 >= argv.length) throw new DispatchCliError("invalid arguments; see usage");
    values.set(flag, argv[++index]!);
  }
  const required = (flag: string): string => {
    const value = values.get(flag);
    if (value === undefined || value.length === 0) throw new DispatchCliError(`${flag} is required`);
    return value;
  };
  const runId = required("--run-id");
  if (!RUN_ID.test(runId)) throw new DispatchCliError("--run-id must be 1-100 letters, digits, dots, underscores, or hyphens");
  const taskFileValue = required("--task-file");
  if (taskFileValue.includes("\0")) throw new DispatchCliError("--task-file must be a valid path");
  const outValue = required("--out");
  if (outValue.includes("\0")) throw new DispatchCliError("--out must be a valid path");
  const creds = absolute(values.get("--creds"), "--creds");
  const ca = absolute(values.get("--ca"), "--ca");
  const rawServers = required("--servers");
  const servers = rawServers.split(",").map((server) => server.trim());
  if (!servers.length || servers.some((server) => !server)) throw new DispatchCliError("--servers must be a comma-separated list of NATS servers");
  const machine = required("--machine");
  if (!/^[a-z0-9][a-z0-9-]{0,31}$/.test(machine)) throw new DispatchCliError("--machine must be a Path B machine name");
  const org = required("--org");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(org) || org === "sys") throw new DispatchCliError("--org must be a non-system org name");
  let pathBMachinesOverride: string[] | undefined;
  const machinesText = values.get("--path-b-machines");
  if (machinesText !== undefined) {
    try { pathBMachinesOverride = pathBMachines(machinesText.split(",").map((value) => value.trim())); }
    catch { throw new DispatchCliError("--path-b-machines must be 1-64 unique Path B machine names"); }
  }
  const seconds = (flag: string, fallback: number, max: number): number => {
    const raw = values.get(flag) ?? String(fallback);
    if (!/^[1-9][0-9]*$/.test(raw) || !Number.isSafeInteger(Number(raw)) || Number(raw) > max) {
      throw new DispatchCliError(`${flag} must be an integer from 1 to ${max}`);
    }
    return Number(raw);
  };
  return {
    runId,
    taskFile: path.resolve(taskFileValue),
    out: path.resolve(outValue),
    creds,
    ca,
    servers,
    machine,
    org,
    ...(pathBMachinesOverride === undefined ? {} : { pathBMachines: pathBMachinesOverride }),
    findTimeoutSeconds: seconds("--find-timeout-seconds", 120, MAX_FIND_SECONDS),
    awaitReplySeconds: seconds("--await-reply-seconds", 300, MAX_AWAIT_SECONDS),
  };
}

function readTask(file: string): string {
  let bytes: Buffer;
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile() || stat.size > MAX_TASK_BYTES) throw new DispatchCliError("--task-file must be a regular file no larger than 512 KiB");
    bytes = fs.readFileSync(file);
  } catch (error) {
    if (error instanceof DispatchCliError) throw error;
    throw new DispatchCliError("could not read --task-file");
  }
  if (!bytes.length || bytes.byteLength > MAX_TASK_BYTES) throw new DispatchCliError("--task-file must be nonempty and no larger than 512 KiB");
  try { return new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
  catch { throw new DispatchCliError("--task-file must contain valid UTF-8"); }
}

const safeError = (error: unknown): string => {
  const message = error instanceof Error ? error.message : "NATS dispatch failed";
  return message.replace(/[\r\n\u0000-\u001f\u007f]/g, " ").slice(0, 500) || "NATS dispatch failed";
};

const wallBaseNs = BigInt(Date.now()) * 1_000_000n;
const monoBaseNs = process.hrtime.bigint();
function isoNs(): string {
  const epochNs = wallBaseNs + (process.hrtime.bigint() - monoBaseNs);
  const milliseconds = epochNs / 1_000_000n;
  const fraction = (epochNs % 1_000_000_000n).toString().padStart(9, "0");
  const second = new Date(Number(milliseconds)).toISOString().replace(/\.\d{3}Z$/, "");
  return `${second}.${fraction}Z`;
}

function appendRecord(file: string, record: DispatchRecord): void {
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW ?? 0);
  const fd = fs.openSync(file, flags, 0o600);
  try {
    if (!fs.fstatSync(fd).isFile()) throw new DispatchCliError("--out must be a regular JSONL file");
    const bytes = Buffer.from(`${JSON.stringify(record)}\n`, "utf8");
    const written = fs.writeSync(fd, bytes, 0, bytes.length);
    if (written !== bytes.length) throw new DispatchCliError("could not append a complete JSONL record");
  } finally {
    fs.closeSync(fd);
  }
}

function answerFields(reply: NatsSendReply | null | undefined, runId: string): Pick<DispatchRecord, "answer_ok" | "answer_text_sha256"> {
  if (!reply) return { answer_ok: false, answer_text_sha256: null };
  return {
    answer_ok: reply.text.startsWith(`FABRIC-RESULT ${runId}\n`),
    answer_text_sha256: createHash("sha256").update(reply.text, "utf8").digest("hex"),
  };
}

export async function runNatsDispatchCli(argv: string[]): Promise<number> {
  let options: DispatchOptions | "help";
  try { options = parse(argv); }
  catch (error) {
    process.stdout.write(`${JSON.stringify({ error: safeError(error) })}\n`);
    return 1;
  }
  if (options === "help") {
    process.stdout.write(`${USAGE}\n`);
    return 0;
  }
  const record: DispatchRecord = {
    runId: options.runId,
    childSession: null,
    model: null,
    t_task_dispatched: null,
    t_ack: null,
    t_answer_received: null,
    answer_ok: false,
    answer_text_sha256: null,
  };
  let exitCode = 1;
  let reply: NatsSendReply | null | undefined;
  try {
    const task = readTask(options.taskFile);
    const sendOptions: NatsSendOptions = {
      to: "",
      kind: "followUp",
      text: task,
      creds: options.creds,
      ca: options.ca,
      servers: options.servers,
      machine: options.machine,
      name: `dispatch-${options.runId}`,
      // FabricControlPlane additionally scopes this key to sender, owner, target, and operation.
      idempotencyKey: `pi-fabric:nats:dispatch:v1:${options.runId}`,
      timeoutMs: options.findTimeoutSeconds * 1_000 + 30_000,
      awaitReplySeconds: options.awaitReplySeconds,
      org: options.org,
      ...(options.pathBMachines === undefined ? {} : { pathBMachines: options.pathBMachines }),
      placementRunId: options.runId,
      findTimeoutMs: options.findTimeoutSeconds * 1_000,
      messageForSender: (senderId, targetId, replyCorrelationId) => {
        if (!replyCorrelationId) throw new DispatchCliError("dispatch reply correlation is unavailable");
        bindDispatchRunContent(options.runId, task, targetId, senderId);
        const answer = `FABRIC-RESULT ${options.runId}\n<answer>`;
        return `${task}\n\nWhen done, call agents.followUp({ id: ${JSON.stringify(senderId)}, message: ${JSON.stringify(answer)}, data: { inReplyTo: ${JSON.stringify(replyCorrelationId)} } }).`;
      },
      onTaskDispatched: () => { record.t_task_dispatched = isoNs(); },
      onReply: () => { record.t_answer_received = isoNs(); },
    };
    const outcome = await openAndSend(sendOptions, () => { record.t_ack = isoNs(); });
    reply = outcome.reply;
    record.childSession = outcome.childSession;
    record.model = outcome.model;
    Object.assign(record, answerFields(reply, options.runId));
    if (!reply) {
      record.error = "timed out waiting for the child reply";
      exitCode = 2;
    } else if (!record.answer_ok) {
      record.error = "reply did not start with the expected FABRIC-RESULT marker";
      exitCode = 1;
    } else exitCode = 0;
  } catch (error) {
    record.error = safeError(error);
    exitCode = 1;
  }
  try {
    appendRecord(options.out, record);
  } catch (error) {
    process.stderr.write(`pi-fabric nats dispatch: ${safeError(error)}\n`);
    return 1;
  }
  process.stdout.write(`${JSON.stringify(record)}\n`);
  return exitCode;
}
