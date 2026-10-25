import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { MAIN_DRAIN_DIRECTORY, MAIN_DRAIN_MAX_REPLY_BYTES, MAIN_DRAIN_MAX_SOCKET_PATH, mainDrainSocketName,
  type MainDrainOp, type MainDrainReply } from "../main-drain/protocol.js";

const USAGE = `Usage:
  pi-fabric session drain <session-uuid | session:uuid> --mesh <meshRoot> [--status | --exit | --reopen] [--deadline-s <1-600>]

Owner-only D3 seat drain of THIS host's live Main (smarty-dev#15310), through its local socket
<meshRoot>/main-drain/ (mode 0700, the Main's own OS user). It is never reachable from the mesh.
  (default)  close the Main's Fabric inbox to new deliveries, owner-stop its session actors, take
             the native input fence once Fabric holds nothing for Pi, and wait (bounded) until drained
  --status   observe only
  --exit     drain as above, then ask the Main for a clean exit (raw Pod resume-drain)
  --reopen   release this drain's fence and reopen the inbox (a failed drain, same process)
Nothing is deleted or acknowledged. Output is one JSON line. Exit 0 only when the op completed:
drained, exiting or reopened.`;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DEFAULT_DEADLINE_S = 540;
const POLL_MS = 1_000;
class UsageError extends Error {}

interface CliIo {
  stdout: Pick<NodeJS.WritableStream, "write">;
  stderr: Pick<NodeJS.WritableStream, "write">;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  uid?: number;
}

/** The socket directory must be the Main's: a real, uid-owned 0700 directory with safe ancestors. */
export const mainDrainSocket = (meshRoot: string, sessionId: string, uid: number): string => {
  const directory = path.join(fs.realpathSync(meshRoot), MAIN_DRAIN_DIRECTORY);
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o700) throw new Error("MAIN_DRAIN_DIRECTORY_UNSAFE");
  for (let current = path.dirname(directory); ; current = path.dirname(current)) {
    const parent = fs.lstatSync(current);
    if (!parent.isDirectory() || (parent.uid !== 0 && parent.uid !== uid) || ((parent.mode & 0o022) !== 0 && (parent.mode & 0o1000) === 0)) {
      throw new Error("MAIN_DRAIN_DIRECTORY_UNSAFE");
    }
    if (path.dirname(current) === current) break;
  }
  const socket = path.join(directory, mainDrainSocketName(sessionId));
  const entry = fs.lstatSync(socket);
  if (!entry.isSocket() || entry.uid !== uid) throw new Error("MAIN_DRAIN_SOCKET_UNSAFE");
  if (Buffer.byteLength(socket) > MAIN_DRAIN_MAX_SOCKET_PATH) throw new Error("MAIN_DRAIN_SOCKET_PATH_TOO_LONG");
  return socket;
};

/** One request, one reply line. Throws a fixed code on any transport or framing failure. */
export const callMainDrain = (socket: string, sessionId: string, op: MainDrainOp, timeoutMs = 30_000): Promise<MainDrainReply> =>
  new Promise((resolve, reject) => {
    const client = net.connect(socket);
    let buffer = Buffer.alloc(0);
    let done = false;
    const finish = (error?: Error, reply?: MainDrainReply): void => {
      if (done) return;
      done = true;
      client.destroy();
      if (error) reject(error); else resolve(reply!);
    };
    client.setTimeout(timeoutMs, () => finish(new Error("MAIN_DRAIN_TIMEOUT")));
    client.once("error", () => finish(new Error("MAIN_DRAIN_UNREACHABLE")));
    client.once("connect", () => client.write(`${JSON.stringify({ v: 1, op, sessionId })}\n`));
    client.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length > MAIN_DRAIN_MAX_REPLY_BYTES) { finish(new Error("MAIN_DRAIN_REPLY_TOO_LARGE")); return; }
      const newline = buffer.indexOf(0x0a);
      if (newline < 0) return;
      let reply: unknown;
      try { reply = JSON.parse(buffer.subarray(0, newline).toString("utf8")); } catch { finish(new Error("MAIN_DRAIN_REPLY_INVALID")); return; }
      const value = reply as Partial<MainDrainReply> | null;
      if (typeof value !== "object" || value === null || value.v !== 1) { finish(new Error("MAIN_DRAIN_REPLY_INVALID")); return; }
      if (value.ok !== true) { finish(new Error(typeof value.error === "string" && /^[A-Z_]{1,64}$/.test(value.error) ? `MAIN_DRAIN_REFUSED_${value.error}` : "MAIN_DRAIN_REFUSED")); return; }
      if (value.sessionId !== sessionId || value.op !== op) { finish(new Error("MAIN_DRAIN_REPLY_MISMATCH")); return; }
      finish(undefined, value as MainDrainReply);
    });
    client.once("end", () => finish(new Error("MAIN_DRAIN_REPLY_INVALID")));
  });

const TERMINAL_ACTOR = new Set(["stopped", "failed"]);
/** Drained as the Main itself observes it. Unknown (null) is never drained. Durable actors are reported, not waited for. */
export const mainDrainReady = (reply: MainDrainReply): boolean =>
  reply.inbox.closed && reply.inbox.held === 0 && reply.inbox.handed === 0 && reply.native.inputsFenced &&
  reply.native.drained === true && reply.native.queued === 0 && reply.native.inputAdmissionCount === 0 && reply.native.idle === true &&
  reply.actors.known && reply.actors.owned.every(actor => actor.residency === "durable" || TERMINAL_ACTOR.has(actor.status)) &&
  reply.agents.known && reply.agents.running.length === 0;

export const runSessionDrainCli = async (
  argv: string[], io: CliIo = { stdout: process.stdout, stderr: process.stderr },
): Promise<number> => {
  try {
    const positionals: string[] = [];
    const flags = new Set<string>();
    let mesh: string | undefined;
    let deadlineS = DEFAULT_DEADLINE_S;
    for (let index = 0; index < argv.length; index++) {
      const arg = argv[index]!;
      if (!arg.startsWith("-")) { positionals.push(arg); continue; }
      if (!["--mesh", "--status", "--exit", "--reopen", "--deadline-s", "--help"].includes(arg) || flags.has(arg)) throw new UsageError("Unknown or repeated option");
      flags.add(arg);
      if (arg === "--mesh") {
        mesh = argv[++index];
        if (!mesh || !path.isAbsolute(mesh)) throw new UsageError("--mesh requires an absolute path");
      } else if (arg === "--deadline-s") {
        const value = argv[++index];
        deadlineS = Number(value);
        if (!value || !/^[0-9]{1,3}$/.test(value) || deadlineS < 1 || deadlineS > 600) throw new UsageError("--deadline-s must be 1-600");
      }
    }
    if (flags.has("--help")) { io.stdout.write(`${USAGE}\n`); return 0; }
    const modes = ["--status", "--exit", "--reopen"].filter(flag => flags.has(flag));
    const sessionId = positionals[0]?.replace(/^session:/, "");
    if (positionals.length !== 1 || !sessionId || !UUID.test(sessionId)) throw new UsageError("Expected exactly one canonical session UUID or session:UUID");
    if (!mesh) throw new UsageError("--mesh is required; there is no environment default");
    if (modes.length > 1) throw new UsageError("--status, --exit and --reopen are exclusive");
    const uid = io.uid ?? process.getuid?.();
    if (uid === undefined) throw new Error("MAIN_DRAIN_UNSUPPORTED_PLATFORM");
    const socket = mainDrainSocket(mesh, sessionId, uid);
    const now = io.now ?? Date.now;
    const sleep = io.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
    const emit = (value: Record<string, unknown>, ok: boolean): number => {
      io.stdout.write(`${JSON.stringify({ format: 1, sessionId, ...value })}\n`);
      return ok ? 0 : 1;
    };
    const mode = modes[0];
    if (mode === "--status" || mode === "--reopen") {
      const op = mode === "--status" ? "status" : "reopen";
      const reply = await callMainDrain(socket, sessionId, op);
      return emit({ op, ready: mainDrainReady(reply), reply }, true);
    }
    const deadline = now() + deadlineS * 1000;
    let reply = await callMainDrain(socket, sessionId, "drain");
    // A halted Main never flushes held follow-ups: report at once instead of waiting out the bound.
    while (!mainDrainReady(reply) && !(reply.inbox.halted === true && (reply.inbox.held ?? 1) > 0) && now() + POLL_MS < deadline) {
      await sleep(POLL_MS);
      reply = await callMainDrain(socket, sessionId, "drain");
    }
    const ready = mainDrainReady(reply);
    if (mode !== "--exit") return emit({ op: "drain", ready, inputsFenced: reply.native.inputsFenced, reply }, ready);
    if (!ready) return emit({ op: "exit", ready, exiting: false, inputsFenced: reply.native.inputsFenced, exitRefused: ["NOT_DRAINED"], reply }, false);
    const exit = await callMainDrain(socket, sessionId, "exit");
    return emit({ op: "exit", ready, exiting: exit.exiting, inputsFenced: exit.native.inputsFenced, exitRefused: exit.exitRefused, reply: exit },
      exit.exiting);
  } catch (error) {
    const usage = error instanceof UsageError;
    const code = !usage && error instanceof Error && /^MAIN_DRAIN_[A-Z_]+$/.test(error.message) ? error.message : "MAIN_DRAIN_UNAVAILABLE";
    io[usage ? "stderr" : "stdout"].write(`${JSON.stringify({ format: 1, error: usage ? "usage" : code,
      ...(usage ? { message: (error as Error).message, usage: USAGE } : {}) })}\n`);
    return usage ? 2 : 1;
  }
};
