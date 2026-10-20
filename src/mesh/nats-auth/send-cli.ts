import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { DEFAULT_FABRIC_CONFIG, type FabricMeshConfig } from "../../config.js";
import { MeshStore, type MeshIdentity } from "../../mesh/store.js";
import { openPrivateDir } from "../../storage/private-dir.js";
import { openFabricTransport } from "../../transport/index.js";
import type { FabricTransport } from "../../transport/types.js";
import { FabricControlPlane, type FabricControlResult } from "../../topology/control-plane.js";
import { ParticipantDirectory } from "../../topology/participant-directory.js";
import type { FabricParticipantRecord } from "../../topology/types.js";
import { pathBMachines } from "./mint.js";
import { PATH_B_OPERATOR_MACHINE } from "./permissions.js";

const SEND_USAGE = [
  "Usage: pi-fabric nats send --to <participant-id> --kind <followUp|steer>",
  "  (--text <text> | --text-file <absolute-file> | --text -) --creds <absolute-file> --ca <absolute-file>",
  "  --text <text> is kept for compatibility but is less private because content appears in process argv; prefer --text-file or --text -.",
  "  --text-file reads up to 512 KiB of UTF-8 from a file; --text - reads up to 512 KiB of UTF-8 from stdin.",
  "  --servers <tls://host:port[,tls://host:port...]> --machine <machine>",
  "  [--name <host-name>] [--idempotency-key <key>] [--timeout-ms <ms>]",
  "  [--await-reply-seconds <seconds>] [--org <org>] [--path-b-machines <csv>]",
  "",
  "A stable private host receipt is kept under XDG_STATE_HOME (or ~/.local/state).",
  "Keep --machine, --creds path, --name, and state home stable to reuse host identity and idempotency.",
  "Policy comes from PI_FABRIC_MESH_NATS, or PI_FABRIC_NATS_CUSTODY_STREAM,",
  "PI_FABRIC_NATS_LIVENESS_STREAM, and PI_FABRIC_NATS_MACHINES.",
  "",
  "Exit 0 only after a Fabric acknowledgement; JSON is written to stdout.",
].join("\n");

const MAX_TEXT_BYTES = 512 * 1024;
const MAX_TIMEOUT_MS = 24 * 60 * 60 * 1_000;
const MAX_AWAIT_REPLY_SECONDS = 24 * 60 * 60;
// Match the participant peer-settle quiet window before asking a receiver to authenticate this sender.
const PRESENCE_SETTLE_MS = 3_000;
const PRESENCE_SETTLE_POLL_MS = 100;

type SendOptions = {
  to: string;
  kind: "followUp" | "steer";
  text: string;
  creds: string;
  ca: string;
  servers: string[];
  machine: string;
  name: string;
  idempotencyKey: string;
  timeoutMs: number;
  awaitReplySeconds?: number;
  org?: string;
  pathBMachines?: string[];
};

type SendReply = {
  from: { id: string; name: string; kind: MeshIdentity["kind"]; sessionId?: string };
  text: string;
  at: number;
};

type SendOutcome = { result: FabricControlResult; senderId: string; reply?: SendReply | null };
type Acknowledged = (result: FabricControlResult, senderId: string) => void;

class SendCliError extends Error {}
type DirectoryIdentity = { dev: number; ino: number; uid: number };

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const absolutePath = (value: string | undefined, flag: string): string => {
  if (!value || !path.isAbsolute(value) || value.includes("\0")) throw new SendCliError(`${flag} must be an absolute path`);
  return value;
};

const parseOptions = (argv: readonly string[]): SendOptions | "help" => {
  if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h" || argv[0] === "help")) return "help";
  const valuedFlags = new Set([
    "--to", "--kind", "--text", "--text-file", "--creds", "--ca", "--servers", "--machine",
    "--name", "--idempotency-key", "--timeout-ms", "--await-reply-seconds", "--org", "--path-b-machines",
  ]);
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index]!;
    if (!valuedFlags.has(flag) || values.has(flag) || index + 1 >= argv.length) {
      throw new SendCliError(`invalid arguments; see usage`);
    }
    values.set(flag, argv[++index]!);
  }
  const required = (flag: string): string => {
    const value = values.get(flag);
    if (value === undefined || value.length === 0) throw new SendCliError(`${flag} is required`);
    return value;
  };
  const to = required("--to");
  if (to !== to.trim() || to.length > 256 || /[\u0000-\u001f\u007f]/.test(to)) throw new SendCliError("--to must be a participant id");
  const kind = required("--kind");
  if (kind !== "followUp" && kind !== "steer") throw new SendCliError("--kind must be followUp or steer");
  const hasText = values.has("--text"), hasTextFile = values.has("--text-file");
  if (hasText === hasTextFile) throw new SendCliError("provide exactly one of --text, --text-file, or --text -");
  const text = hasText
    ? required("--text") === "-" ? readTextStream(0) : required("--text")
    : readTextFile(absolutePath(values.get("--text-file"), "--text-file"));
  if (!text || Buffer.byteLength(text, "utf8") > MAX_TEXT_BYTES) throw new SendCliError("message text must be nonempty and at most 512 KiB");
  const rawServers = required("--servers");
  const servers = rawServers.split(",").map((server) => server.trim());
  if (!servers.length || servers.some((server) => !server)) throw new SendCliError("--servers must be a comma-separated list of NATS servers");
  const timeoutText = values.get("--timeout-ms") ?? "30000";
  if (!/^[1-9][0-9]*$/.test(timeoutText) || !Number.isSafeInteger(Number(timeoutText)) || Number(timeoutText) > MAX_TIMEOUT_MS) {
    throw new SendCliError(`--timeout-ms must be an integer from 1 to ${MAX_TIMEOUT_MS}`);
  }
  const awaitReplyText = values.get("--await-reply-seconds");
  if (awaitReplyText !== undefined && (!/^[1-9][0-9]*$/.test(awaitReplyText) || !Number.isSafeInteger(Number(awaitReplyText)) || Number(awaitReplyText) > MAX_AWAIT_REPLY_SECONDS)) {
    throw new SendCliError(`--await-reply-seconds must be an integer from 1 to ${MAX_AWAIT_REPLY_SECONDS}`);
  }
  const org = values.get("--org");
  if (org !== undefined && (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(org) || org === "sys")) throw new SendCliError("--org must be a non-system org name");
  const machinesText = values.get("--path-b-machines");
  let pathBMachinesOverride: string[] | undefined;
  if (machinesText !== undefined) {
    try { pathBMachinesOverride = pathBMachines(machinesText.split(",").map((machine) => machine.trim())); }
    catch { throw new SendCliError("--path-b-machines must be 1-64 unique Path B machine names"); }
  }
  const name = values.get("--name")?.trim() || "Fabric CLI";
  if (name.length > 128 || /[\u0000-\u001f\u007f]/.test(name)) throw new SendCliError("--name must be at most 128 characters");
  const idempotencyKey = values.get("--idempotency-key") ?? randomUUID();
  if (!idempotencyKey || idempotencyKey !== idempotencyKey.trim() || idempotencyKey.length > 200 || /[\u0000-\u001f\u007f]/.test(idempotencyKey)) {
    throw new SendCliError("--idempotency-key must be 1-200 printable characters");
  }
  return {
    to,
    kind,
    text,
    creds: absolutePath(required("--creds"), "--creds"),
    ca: absolutePath(required("--ca"), "--ca"),
    servers,
    machine: required("--machine"),
    name,
    idempotencyKey,
    timeoutMs: Number(timeoutText),
    ...(awaitReplyText === undefined ? {} : { awaitReplySeconds: Number(awaitReplyText) }),
    ...(org === undefined ? {} : { org }),
    ...(pathBMachinesOverride === undefined ? {} : { pathBMachines: pathBMachinesOverride }),
  };
};

function decodeTextInput(bytes: Buffer, source: "--text-file" | "stdin"): string {
  if (bytes.byteLength > MAX_TEXT_BYTES) {
    throw new SendCliError(source === "--text-file"
      ? "--text-file must be a regular file no larger than 512 KiB"
      : "stdin message must be no larger than 512 KiB");
  }
  try { return new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
  catch { throw new SendCliError(source === "--text-file" ? "--text-file must contain valid UTF-8" : "stdin message must contain valid UTF-8"); }
}

function readTextFile(file: string): string {
  let bytes: Buffer;
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile() || stat.size > MAX_TEXT_BYTES) throw new SendCliError("--text-file must be a regular file no larger than 512 KiB");
    bytes = fs.readFileSync(file);
  } catch (error) {
    if (error instanceof SendCliError) throw error;
    throw new SendCliError("could not read --text-file");
  }
  return decodeTextInput(bytes, "--text-file");
}

function readTextStream(fd: number): string {
  const chunks: Buffer[] = [];
  const buffer = Buffer.alloc(64 * 1024);
  let total = 0;
  try {
    for (;;) {
      const count = fs.readSync(fd, buffer, 0, Math.min(buffer.byteLength, MAX_TEXT_BYTES + 1 - total), null);
      if (count === 0) break;
      total += count;
      if (total > MAX_TEXT_BYTES) throw new SendCliError("stdin message must be no larger than 512 KiB");
      chunks.push(Buffer.from(buffer.subarray(0, count)));
    }
  } catch (error) {
    if (error instanceof SendCliError) throw error;
    throw new SendCliError("could not read stdin");
  }
  return decodeTextInput(Buffer.concat(chunks, total), "stdin");
}

function policyConfig(): { custodyStream: string; livenessStream: string; pathBMachines: string[]; operatorMachine: string } {
  const raw = process.env.PI_FABRIC_MESH_NATS;
  let source: Record<string, unknown> | undefined;
  if (raw !== undefined) {
    try {
      const parsed: unknown = JSON.parse(raw);
      if (!isPlainObject(parsed)) throw new Error("not an object");
      source = parsed;
    } catch {
      throw new SendCliError("PI_FABRIC_MESH_NATS must be a JSON object with the NATS policy fields");
    }
  }
  const policyValue = (key: string, env: string): unknown => source ? source[key] : process.env[env]?.trim();
  const custodyStream = policyValue("custodyStream", "PI_FABRIC_NATS_CUSTODY_STREAM");
  const livenessStream = policyValue("livenessStream", "PI_FABRIC_NATS_LIVENESS_STREAM");
  const rawMachines = policyValue("pathBMachines", "PI_FABRIC_NATS_MACHINES");
  const pathBMachines = Array.isArray(rawMachines)
    ? rawMachines
    : typeof rawMachines === "string" ? rawMachines.split(",").map((machine) => machine.trim()) : undefined;
  const operatorMachine = source?.operatorMachine ?? PATH_B_OPERATOR_MACHINE;
  if (typeof custodyStream !== "string" || !custodyStream || typeof livenessStream !== "string" || !livenessStream ||
    !pathBMachines || pathBMachines.length === 0 || !pathBMachines.every((machine) => typeof machine === "string" && machine.length > 0) ||
    typeof operatorMachine !== "string" || !operatorMachine) {
    throw new SendCliError("NATS policy requires custodyStream, livenessStream, pathBMachines, and operatorMachine");
  }
  return { custodyStream, livenessStream, pathBMachines: [...pathBMachines], operatorMachine };
}

function stableSenderIdentity(options: SendOptions, stateHome: string): { token: string; sessionId: string } {
  const digest = createHash("sha256").update(options.machine).update("\0").update(path.resolve(options.creds)).update("\0").update(options.name).update("\0").update(stateHome).digest();
  const bytes = Buffer.from(digest.subarray(0, 16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const token = bytes.toString("hex");
  const sessionId = `${token.slice(0, 8)}-${token.slice(8, 12)}-${token.slice(12, 16)}-${token.slice(16, 20)}-${token.slice(20)}`;
  return { token, sessionId };
}

function createPrivateRoot(): { path: string; identity: DirectoryIdentity } {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-nats-send-"));
  const stat = fs.lstatSync(root);
  const uid = process.getuid?.();
  const identity = { dev: stat.dev, ino: stat.ino, uid: stat.uid };
  try {
    if (!stat.isDirectory() || stat.isSymbolicLink() || uid === undefined || stat.uid !== uid || (stat.mode & 0o7777) !== 0o700) {
      throw new SendCliError("private temporary state verification failed");
    }
    return { path: root, identity };
  } catch (error) {
    try { removePrivateRoot(root, identity); } catch { /* Preserve the original refusal; never remove a replacement. */ }
    throw error;
  }
}

function removePrivateRoot(root: string, identity: DirectoryIdentity): void {
  const current = fs.lstatSync(root);
  if (!current.isDirectory() || current.isSymbolicLink() || current.uid !== identity.uid ||
    current.dev !== identity.dev || current.ino !== identity.ino) throw new SendCliError("private temporary state changed before cleanup");
  fs.rmSync(root, { recursive: true });
}

function presenceToken(id: string): string { return createHash("sha256").update(id).digest("hex").slice(0, 32); }

function replyFor(command: { operation: string; targetId: string; message?: string }, from: MeshIdentity, targetId: string): SendReply | undefined {
  if (command.operation !== "followUp" || command.targetId !== targetId || typeof command.message !== "string" ||
    !command.message || Buffer.byteLength(command.message, "utf8") > MAX_TEXT_BYTES ||
    !from.id || from.id.length > 256 || /[\u0000-\u001f\u007f]/.test(from.id) ||
    typeof from.name !== "string" || from.name.length > 128 || /[\u0000-\u001f\u007f]/.test(from.name) ||
    (from.sessionId !== undefined && (from.sessionId.length > 128 || /[\u0000-\u001f\u007f]/.test(from.sessionId)))) return undefined;
  return {
    from: { id: from.id, name: from.name, kind: from.kind, ...(from.sessionId ? { sessionId: from.sessionId } : {}) },
    text: command.message,
    at: Date.now(),
  };
}

function waitForReply(reply: Promise<SendReply>, timeoutMs: number, signal: AbortSignal): Promise<SendReply | null> {
  return new Promise((resolve, reject) => {
    const finish = (result: SendReply | null, error?: unknown): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      if (error !== undefined) reject(error); else resolve(result);
    };
    const timer = setTimeout(() => finish(null), timeoutMs);
    const onAbort = (): void => finish(null, signal.reason instanceof Error ? signal.reason : new Error("NATS reply wait interrupted"));
    signal.addEventListener("abort", onAbort, { once: true });
    void reply.then(value => finish(value), error => finish(null, error));
    if (signal.aborted) onAbort();
  });
}

const routeFingerprint = (route: { machine: string; ownerHostId: string; ownerIdentityId: string; ownerIncarnation: string }): string =>
  JSON.stringify([route.machine, route.ownerHostId, route.ownerIdentityId, route.ownerIncarnation]);

async function waitForLiveRoute(
  directory: NonNullable<ParticipantDirectory["participantView"]>, id: string, signal: AbortSignal,
): Promise<NonNullable<ReturnType<typeof directory.resolve>>> {
  for (;;) {
    signal.throwIfAborted();
    const route = directory.resolve(id);
    if (route?.live) return route;
    await directory.whenChanged(id, signal);
  }
}

async function waitForSettledPresence(
  view: NonNullable<ParticipantDirectory["participantView"]>,
  directory: ParticipantDirectory,
  id: string,
  signal: AbortSignal,
): Promise<NonNullable<ReturnType<typeof view.resolve>>> {
  let fingerprint: string | undefined;
  let stableSince = 0;
  for (;;) {
    signal.throwIfAborted();
    const route = view.resolve(id);
    const current = route?.live === true && directory.canConsumeMesh() ? routeFingerprint(route) : undefined;
    if (!current) {
      fingerprint = undefined;
      stableSince = 0;
    } else if (fingerprint !== current) {
      fingerprint = current;
      stableSince = Date.now();
    } else if (Date.now() - stableSince >= PRESENCE_SETTLE_MS && route) {
      return route;
    }
    await sleep(PRESENCE_SETTLE_POLL_MS, undefined, { signal });
  }
}

async function openAndSend(options: SendOptions, onAcknowledged: Acknowledged): Promise<SendOutcome> {
  const policy = policyConfig();
  const configuredStateHome = process.env.XDG_STATE_HOME?.trim() || path.join(os.homedir(), ".local", "state");
  if (!path.isAbsolute(configuredStateHome) || configuredStateHome.includes("\0")) throw new SendCliError("XDG_STATE_HOME must be an absolute path");
  const stateHome = path.resolve(configuredStateHome);
  const stable = stableSenderIdentity(options, stateHome);
  const transportStateDir = path.join(stateHome, "pi-fabric", "nats-send", stable.token);
  const localState = openPrivateDir(transportStateDir);
  let root: ReturnType<typeof createPrivateRoot>;
  try { root = createPrivateRoot(); }
  catch (error) { localState.close(); throw error; }
  const meshRoot = path.join(root.path, "mesh");
  const sessionId = stable.sessionId;
  const hostId = `runtime:${stable.token}`;
  const rootId = `session:${sessionId}`;
  const identity: MeshIdentity = { id: `provider:cli:${stable.token}`, name: options.name, kind: "agent", sessionId };
  let meshConfig!: FabricMeshConfig;
  let mesh!: MeshStore;
  try {
    fs.mkdirSync(meshRoot, { mode: 0o700 });
    meshConfig = {
      ...structuredClone(DEFAULT_FABRIC_CONFIG.mesh),
      enabled: true,
      transport: "nats",
      hostTransport: "nats",
      stateBackend: "file",
      root: meshRoot,
      nats: {
        ...DEFAULT_FABRIC_CONFIG.mesh.nats,
        servers: options.servers,
        machine: options.machine,
        credentials: options.creds,
        custodyStream: policy.custodyStream,
        livenessStream: policy.livenessStream,
        pathBMachines: options.pathBMachines ?? policy.pathBMachines,
        ...(options.org === undefined ? {} : { org: options.org }),
        tls: { caFile: options.ca },
        operatorMachine: policy.operatorMachine,
      },
    };
    mesh = new MeshStore(meshRoot, meshConfig.maxEventBytes, meshConfig.maxReadEvents, {
      lockProtocol: meshConfig.lockProtocol,
      stateBackend: "file",
    });
  } catch (error) {
    try { removePrivateRoot(root.path, root.identity); } catch { /* Keep the original setup failure. */ }
    localState.close();
    throw error;
  }
  const lifetime = new AbortController();
  const timeout = AbortSignal.timeout(options.timeoutMs);
  const signal = AbortSignal.any([lifetime.signal, timeout]);
  const onSignal = (): void => lifetime.abort(new Error("NATS send interrupted"));
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  let transport: FabricTransport | undefined;
  let directory: ParticipantDirectory | undefined;
  let plane: FabricControlPlane | undefined;
  let result: FabricControlResult | undefined;
  let reply: SendReply | null | undefined;
  let resolveReply: ((value: SendReply) => void) | undefined;
  let awaitingReply = false;
  const replyPromise = options.awaitReplySeconds === undefined ? undefined : new Promise<SendReply>((resolve) => { resolveReply = resolve; });
  let failure: unknown;
  try {
    transport = await openFabricTransport({ config: meshConfig, hostId, mesh, signal });
    if (transport.kind !== "nats" || transport.self.hostId !== hostId) throw new SendCliError("NATS transport owner mismatch");
    const transportView = new ParticipantDirectory(mesh, {
      enabled: true,
      transport,
      transportStateDir,
      hostId,
      rootId,
      identity,
    });
    directory = transportView;
    await directory.start();
    signal.throwIfAborted();
    const view = directory.participantView;
    if (!view) throw new SendCliError("NATS participant view is unavailable");
    const target = await waitForLiveRoute(view, options.to, signal);
    if (!target.capabilities.includes(options.kind)) throw new SendCliError(`target does not advertise ${options.kind}`);
    plane = new FabricControlPlane(mesh, identity, {
      enabled: true,
      hostId,
      transport,
      participants: view,
      ownerIncarnation: directory.ownerIncarnation,
      controlIncarnationFence: "enforce",
      expectedNatsInboxStream: `FABRIC_IN_${options.machine}`,
      relayOperatorMachine: policy.operatorMachine,
      inboxLanes: meshConfig.nats.inboxLanes ?? 8,
      canConsumeMesh: () => directory?.canConsumeMesh() === true,
    });
    plane.start((command, from) => {
      const received = awaitingReply && replyPromise ? replyFor(command, from, identity.id) : undefined;
      if (!received || !resolveReply) return { accepted: false, error: "CLI accepts only its first awaited followUp reply" };
      reply = received;
      awaitingReply = false;
      resolveReply(received);
      resolveReply = undefined;
      return { accepted: true, messageId: command.commandId };
    });
    const now = Date.now();
    const presence: FabricParticipantRecord = {
      format: 1,
      id: identity.id,
      kind: "provider",
      rootId,
      ownerHostId: hostId,
      ownerIdentityId: identity.id,
      parentId: rootId,
      name: options.name,
      status: "running",
      residency: "session",
      provider: "cli",
      sessionId,
      transport: "host",
      capabilities: options.awaitReplySeconds === undefined ? [] : ["followUp"],
      startedAt: now,
      updatedAt: now,
      controlProtocol: "v1",
    };
    await directory.publishBeforeLaunch(presence);
    const settled = await waitForSettledPresence(view, directory, identity.id, signal);
    if (!settled.live || !directory.canConsumeMesh()) throw new SendCliError("sender presence is not live");
    signal.throwIfAborted();
    if (options.awaitReplySeconds !== undefined) awaitingReply = true;
    try {
      result = await plane.request(target.ownerHostId, options.to, options.kind, {
        message: options.text,
        ownerIncarnation: target.ownerIncarnation,
      }, target.ownerIdentityId, {
        idempotencyKey: options.idempotencyKey,
        timeoutMs: options.timeoutMs,
        signal,
      });
      if (result.acknowledged !== true) throw new SendCliError("Fabric request was not acknowledged");
    } catch (error) {
      awaitingReply = false;
      resolveReply = undefined;
      throw error;
    }
    onAcknowledged(result, identity.id);
    if (options.awaitReplySeconds !== undefined) {
      reply = await waitForReply(replyPromise!, options.awaitReplySeconds * 1_000, lifetime.signal);
      awaitingReply = false;
      if (reply) await plane.settleNatsInbox();
    }
  } catch (error) {
    failure = error;
  } finally {
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
    try { await plane?.close(); } catch { failure ??= new SendCliError("could not close Fabric control plane"); }
    try { await directory?.close(); } catch { failure ??= new SendCliError("could not withdraw CLI presence"); }
    try { await transport?.close(); } catch { failure ??= new SendCliError("could not close NATS transport"); }
    lifetime.abort();
    try { removePrivateRoot(root.path, root.identity); }
    catch { failure ??= new SendCliError("could not remove private temporary state"); }
    try { localState.close(); }
    catch { failure ??= new SendCliError("could not close private host state"); }
  }
  if (failure !== undefined) throw failure;
  if (!result) throw new SendCliError("Fabric send did not produce an acknowledgement");
  return { result, senderId: identity.id, ...(options.awaitReplySeconds === undefined ? {} : { reply: reply ?? null }) };
}

const errorText = (error: unknown): string => {
  const message = error instanceof Error ? error.message : "NATS send failed";
  return message.replace(/[\r\n\u0000-\u001f\u007f]/g, " ").slice(0, 500) || "NATS send failed";
};

export async function runNatsSendCli(argv: string[]): Promise<number> {
  try {
    const parsed = parseOptions(argv);
    if (parsed === "help") {
      process.stdout.write(`${SEND_USAGE}\n`);
      return 0;
    }
    const outcome = await openAndSend(parsed, (result, senderId) => {
      process.stdout.write(`${JSON.stringify({
        queued: result.queued,
        acknowledged: result.acknowledged,
        routed: result.routed,
        messageId: result.messageId,
        senderId,
      })}\n`);
    });
    if (parsed.awaitReplySeconds === undefined) return outcome.result.acknowledged === true ? 0 : 1;
    process.stdout.write(`${JSON.stringify({ reply: outcome.reply ?? null })}\n`);
    return outcome.reply === null ? 2 : 0;
  } catch (error) {
    process.stdout.write(`${JSON.stringify({ error: errorText(error) })}\n`);
    return 1;
  }
}
