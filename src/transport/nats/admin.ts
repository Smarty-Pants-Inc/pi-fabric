import { existsSync, openSync, closeSync, fsyncSync, readFileSync, writeSync } from "node:fs";
import { openNatsConnection } from "./connection.js";
import { consumeDeadLetters, type DeadLetterEnvelope, type DeadLetterSink } from "./dead-letter.js";
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

const jsonLineSink = (file: string): DeadLetterSink => {
  const seen = new Set<string>();
  if (existsSync(file)) {
    for (const line of readFileSync(file, "utf8").split("\n")) {
      if (!line) continue;
      try { const id = (JSON.parse(line) as { id?: unknown }).id; if (typeof id === "string") seen.add(id); } catch { /* retain the append-only log */ }
    }
  }
  return {
    async put(envelope: DeadLetterEnvelope): Promise<void> {
      if (seen.has(envelope.id)) return;
      const record = JSON.stringify({ ...envelope, original: envelope.original ? Buffer.from(envelope.original).toString("base64") : undefined });
      const fd = openSync(file, "a", 0o600);
      try { writeSync(fd, `${record}\n`); fsyncSync(fd); }
      finally { closeSync(fd); }
      seen.add(envelope.id);
    },
  };
};

async function provision(args: readonly string[]): Promise<void> {
  const machine = validateMachine(requiredEnv("PI_FABRIC_MACHINE"));
  const machines = machinesArg(args);
  const connection = await openNatsConnection({
    servers: requiredEnv("PI_FABRIC_NATS_SERVERS"),
    credentials: requiredEnv("PI_FABRIC_NATS_CREDENTIALS"),
    machine,
    name: "fabric-transport-admin",
    // Separate bootstrap authority is intentionally broader than machine runtime credentials.
    verifyMachine: false,
  });
  try {
    // This is deliberately first: advisories are otherwise lost before the operator consumer exists.
    await ensureTransportStream(connection.manager, streamPolicy(ADVISORY_CAPTURE_STREAM, [transportAdvisoryStreamSubject], 30 * 24 * 60 * 60_000, 64 * 1024));
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
    name: "fabric-transport-admin-drain",
  });
  try {
    const info = await connection.manager.consumers.info(transportInboxStream(machine), transportDurableName(hostId));
    process.stdout.write(`${JSON.stringify({ machine, hostId, pending: info.num_pending, ackPending: info.num_ack_pending })}\n`);
  } finally {
    await connection.close();
  }
}

async function dlq(args: readonly string[]): Promise<void> {
  const machine = validateMachine(requiredEnv("PI_FABRIC_MACHINE"));
  const connection = await openNatsConnection({
    servers: requiredEnv("PI_FABRIC_NATS_SERVERS"),
    credentials: requiredEnv("PI_FABRIC_NATS_CREDENTIALS"),
    machine,
    name: "fabric-transport-admin-dlq",
  });
  const controller = new AbortController();
  const stop = (): void => controller.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    const count = await consumeDeadLetters({
      manager: connection.manager,
      js: connection.js,
      signal: controller.signal,
      once: args.includes("--once"),
      sink: jsonLineSink(process.env.PI_FABRIC_DLQ_PATH?.trim() || "fabric-dead-letters.jsonl"),
      machines: machinesArg(args),
    });
    process.stdout.write(`${JSON.stringify({ ok: true, captured: count })}\n`);
  } finally {
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    await connection.close();
  }
}

export async function runTransportAdmin(args: readonly string[]): Promise<number> {
  const [command] = args;
  if (command === "provision") await provision(args);
  else if (command === "drain") await drain(args);
  else if (command === "orphan") throw new Error("orphan disposition is not implemented; no messages were acknowledged or removed");
  else if (command === "dlq") await dlq(args);
  else throw new Error("usage: fabric-transport-admin provision|dlq|drain|orphan");
  return 0;
}
