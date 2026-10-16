import { existsSync, openSync, closeSync, fsyncSync, readFileSync, writeSync } from "node:fs";
import { isAbsolute } from "node:path";
import { openBootstrapNatsConnection, openNatsConnection, type NatsPermissionPolicy } from "./connection.js";
import { consumeDeadLetters, ensureDeadLetterConsumer, type DeadLetterEnvelope, type DeadLetterSink } from "./dead-letter.js";
import { openNatsKv } from "./kv.js";
import { ensureTransportStream, type TransportStreamPolicy } from "./provision.js";
import {
  ADVISORY_CAPTURE_STREAM,
  transportAdvisoryStreamSubject,
  transportDurableName,
  transportInboxStream,
  transportInboxStreamSubject,
  validateMachine,
} from "./subjects.js";

const MAX_BYTES = 1_073_741_824;
const MAX_MESSAGE_BYTES = 1_048_576;
const DUPLICATE_WINDOW_MS = 10 * 60_000;
const requiredEnv = (name: string): string => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
};

const streamPolicy = (name: string, subjects: readonly string[], maxAgeMs: number, maxMessageBytes = MAX_MESSAGE_BYTES): TransportStreamPolicy => ({
  name,
  subjects,
  maxBytes: MAX_BYTES,
  maxAgeMs,
  maxMessageBytes,
  duplicateWindowMs: DUPLICATE_WINDOW_MS,
  replicas: 1,
});

const machinesArg = (args: readonly string[]): string[] => {
  const index = args.indexOf("--machines");
  const value = index < 0 ? process.env.PI_FABRIC_NATS_MACHINES : args[index + 1];
  if (!value) throw new Error("--machines or PI_FABRIC_NATS_MACHINES is required");
  const machines = value.split(",").map((machine) => validateMachine(machine.trim()));
  if (machines.length === 0 || new Set(machines).size !== machines.length) throw new Error("machines must be unique");
  return machines;
};

/** Expected #1276 policy for verified admin connections. Values are validated by openNatsConnection (mint validators). */
const policyFromEnv = (args: readonly string[]): NatsPermissionPolicy => ({
  custodyStream: requiredEnv("PI_FABRIC_NATS_CUSTODY_STREAM"),
  livenessStream: requiredEnv("PI_FABRIC_NATS_LIVENESS_STREAM"),
  pathBMachines: machinesArg(args),
});

/** Optional explicit CA path; connection.ts owns TLS validation and transport policy. */
const caFileArg = (args: readonly string[]): { readonly tls?: { readonly caFile: string } } => {
  const index = args.indexOf("--ca-file");
  if (index < 0) return {};
  if (args.lastIndexOf("--ca-file") !== index) throw new Error("--ca-file must not be specified more than once");
  const caFile = args[index + 1];
  if (!caFile || caFile.startsWith("--")) throw new Error("--ca-file requires an absolute path");
  if (!isAbsolute(caFile)) throw new Error("--ca-file must be an absolute path");
  return { tls: { caFile } };
};

const operatorMachineArg = (args: readonly string[]): string => {
  const index = args.indexOf("--operator-machine");
  const machine = index < 0 ? "fabric-ops" : args[index + 1];
  if (!machine || !/^fabric-ops(-[a-z0-9]+)?$/.test(machine)) {
    throw new Error("--operator-machine must be in the fabric-ops namespace");
  }
  return validateMachine(machine);
};

// Dedupe is an internal stable tuple, never a persisted ID or advisory-supplied field.
const captureKey = (record: DeadLetterEnvelope): string => JSON.stringify([record.stream, record.consumer, record.stream_seq]);
const jsonLineSink = (file: string): DeadLetterSink => {
  const seen = new Set<string>();
  if (existsSync(file)) {
    for (const line of readFileSync(file, "utf8").split("\n")) {
      if (!line) continue;
      try {
        const record = JSON.parse(line) as DeadLetterEnvelope | null;
        if (record && typeof record.stream === "string" && typeof record.consumer === "string" &&
          Number.isSafeInteger(record.stream_seq) && record.stream_seq > 0 &&
          Number.isSafeInteger(record.deliveries) && record.deliveries >= 5) seen.add(captureKey(record));
      } catch { /* retain the append-only log */ }
    }
  }
  return {
    async put(envelope: DeadLetterEnvelope): Promise<void> {
      const key = captureKey(envelope);
      if (seen.has(key)) return;
      // Whitelist at the persistence boundary, even if a caller later grows the in-memory type.
      const record = JSON.stringify({ stream: envelope.stream, consumer: envelope.consumer,
        stream_seq: envelope.stream_seq, deliveries: envelope.deliveries });
      const fd = openSync(file, "a", 0o600);
      try { writeSync(fd, `${record}\n`); fsyncSync(fd); }
      finally { closeSync(fd); }
      seen.add(key);
    },
  };
};

async function provision(args: readonly string[]): Promise<void> {
  const machines = machinesArg(args);
  // Separate bootstrap authority is intentionally broader than machine runtime credentials.
  const connection = await openBootstrapNatsConnection({
    servers: requiredEnv("PI_FABRIC_NATS_SERVERS"),
    credentials: requiredEnv("PI_FABRIC_NATS_CREDENTIALS"),
    name: "fabric-transport-admin",
    ...caFileArg(args),
  });
  try {
    // This is deliberately first: advisories are otherwise lost before the operator consumer exists.
    await ensureTransportStream(connection.manager, streamPolicy(ADVISORY_CAPTURE_STREAM, [transportAdvisoryStreamSubject], 30 * 24 * 60 * 60_000, 64 * 1024));
    await ensureDeadLetterConsumer(connection.manager);
    await openNatsKv(connection.js, { bucket: "fabric_leases", ttlMs: 15_000, markerTtlMs: 5 * 60_000 });
    await openNatsKv(connection.js, { bucket: "fabric_presence", ttlMs: 10 * 60_000, markerTtlMs: 5 * 60_000 });
    for (const target of machines) {
      await ensureTransportStream(connection.manager, streamPolicy(transportInboxStream(target), [transportInboxStreamSubject(target),], 0));
    }
    process.stdout.write(`${JSON.stringify({ ok: true, advisory: ADVISORY_CAPTURE_STREAM, machines })}\n`);
  } finally {
    await connection.close();
  }
}

async function drain(args: readonly string[]): Promise<void> {
  const machine = validateMachine(requiredEnv("PI_FABRIC_MACHINE"));
  const hostId = process.env.PI_FABRIC_HOST_ID?.trim();
  if (!hostId) throw new Error("PI_FABRIC_HOST_ID is required");
  const connection = await openNatsConnection({
    servers: requiredEnv("PI_FABRIC_NATS_SERVERS"),
    credentials: requiredEnv("PI_FABRIC_NATS_CREDENTIALS"),
    machine,
    ...policyFromEnv(args),
    name: "fabric-transport-admin-drain",
    ...caFileArg(args),
  });
  try {
    const info = await connection.manager.consumers.info(transportInboxStream(machine), transportDurableName(hostId));
    process.stdout.write(`${JSON.stringify({ machine, hostId, pending: info.num_pending, ackPending: info.num_ack_pending, ackFloor: info.ack_floor })}\n`);
  } finally {
    await connection.close();
  }
}

async function orphan(args: readonly string[]): Promise<void> {
  const valueArg = (flag: string): string | undefined => {
    const index = args.indexOf(flag);
    if (index < 0) return undefined;
    const value = args[index + 1]?.trim();
    if (!value || value.startsWith("--")) throw new Error(`${flag} requires a value`);
    return value;
  };
  const target = valueArg("--machine");
  if (!target) throw new Error("orphan requires --machine <inbox-machine>");
  const targetMachine = validateMachine(target);
  if (!machinesArg(args).includes(targetMachine)) throw new Error("orphan inbox machine is outside the configured inventory");
  const hostId = valueArg("--host-id") ?? process.env.PI_FABRIC_HOST_ID?.trim();
  if (!hostId) throw new Error("orphan requires --host-id or PI_FABRIC_HOST_ID");
  const machine = operatorMachineArg(args);
  const connection = await openNatsConnection({
    servers: requiredEnv("PI_FABRIC_NATS_SERVERS"),
    credentials: requiredEnv("PI_FABRIC_NATS_CREDENTIALS"),
    machine,
    role: "operator",
    ...policyFromEnv(args),
    name: "fabric-transport-admin-orphan-report",
    ...caFileArg(args),
  });
  try {
    const stream = transportInboxStream(targetMachine);
    const consumer = transportDurableName(hostId);
    // E15: report one explicitly named inbox only. These counters/floors are NOT an exact pending
    // sequence set or proof of lease absence. No disposition, pull, ACK, delete or purge is attempted.
    const info = await connection.manager.consumers.info(stream, consumer);
    process.stdout.write(`${JSON.stringify({
      ok: true, mode: "report-only", disposition: "none", machine: targetMachine, hostId, stream, consumer,
      pending: info.num_pending, ackPending: info.num_ack_pending, ackFloor: info.ack_floor,
      num_pending: info.num_pending, num_ack_pending: info.num_ack_pending, ack_floor: info.ack_floor,
    })}\n`);
  } finally {
    await connection.close();
  }
}

async function dlq(args: readonly string[]): Promise<void> {
  const machine = operatorMachineArg(args);
  const machines = machinesArg(args);
  const connection = await openNatsConnection({
    servers: requiredEnv("PI_FABRIC_NATS_SERVERS"),
    credentials: requiredEnv("PI_FABRIC_NATS_CREDENTIALS"),
    machine,
    role: "operator",
    ...policyFromEnv(args),
    name: "fabric-transport-admin-dlq",
    ...caFileArg(args),
  });
  const controller = new AbortController();
  const stop = (): void => controller.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    const audit = jsonLineSink(process.env.PI_FABRIC_DLQ_PATH?.trim() || "fabric-dead-letters.jsonl");
    const count = await consumeDeadLetters({
      manager: connection.manager,
      js: connection.js,
      signal: controller.signal,
      once: args.includes("--once"),
      sink: audit,
      machines,
    });
    process.stdout.write(`${JSON.stringify({ ok: true, captured: count })}\n`);
  } finally {
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    await connection.close();
  }
}

export async function runTransportAdmin(args: readonly string[]): Promise<number> {
  if (args.includes("--allow-insecure-loopback")) throw new Error("--allow-insecure-loopback is not supported");
  const [command] = args;
  if (command === "provision") await provision(args);
  else if (command === "drain") await drain(args);
  else if (command === "orphan") await orphan(args);
  else if (command === "dlq") await dlq(args);
  else throw new Error("usage: fabric-transport-admin provision|dlq|drain|orphan [--ca-file <absolute path>]");
  return 0;
}
