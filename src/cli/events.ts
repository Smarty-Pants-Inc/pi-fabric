import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { openBootstrapNatsConnection } from "../transport/nats/connection.js";
import { openNatsEventsPublisher } from "../transport/nats/events-publish.js";

const USAGE = [
  "Usage:",
  "  pi-fabric events publish --topic <t> (--data <json> | --data-file <path>) --creds <user.creds> --ca <ca.pem>",
  "                         --servers <url> --org <org> [--id <event-id>]",
  "",
  "Publishes one untrusted mesh topic event to the existing org NATS events stream.",
  "The stream must already be provisioned; --id makes retries by the caller idempotent.",
].join("\n");

const VALUE_FLAGS = new Set(["--topic", "--data", "--data-file", "--creds", "--ca", "--servers", "--org", "--id"]);
const MAX_DATA_BYTES = 1_048_576;

class UsageError extends Error {}

const parseFlags = (argv: readonly string[]): Map<string, string> => {
  const flags = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index]!;
    const equals = argument.indexOf("=");
    const name = argument.startsWith("--") && equals > 0 ? argument.slice(0, equals) : argument;
    if (!VALUE_FLAGS.has(name)) throw new UsageError(`unknown option ${argument}`);
    const value = equals > 0 && name !== argument ? argument.slice(equals + 1) : argv[++index];
    if (value === undefined) throw new UsageError(`${name} requires a value`);
    if (flags.has(name)) throw new UsageError(`${name} given more than once`);
    flags.set(name, value);
  }
  return flags;
};

const readBounded = async (file: string): Promise<string> => {
  const chunks: Buffer[] = [];
  let bytes = 0;
  const stream = fs.createReadStream(file);
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    bytes += buffer.length;
    if (bytes > MAX_DATA_BYTES) {
      stream.destroy();
      throw new Error(`NATS event data exceeds ${MAX_DATA_BYTES} bytes`);
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
};

const publish = async (argv: readonly string[]): Promise<number> => {
  const flags = parseFlags(argv);
  for (const required of ["--topic", "--creds", "--ca", "--servers", "--org"]) {
    if (!flags.has(required) || flags.get(required) === "") throw new UsageError(`${required} is required`);
  }
  if (flags.has("--data") === flags.has("--data-file")) {
    throw new UsageError("provide exactly one of --data or --data-file");
  }
  const raw = flags.has("--data-file") ? await readBounded(flags.get("--data-file")!) : flags.get("--data");
  if (raw === undefined) throw new UsageError("--data or --data-file is required");
  if (Buffer.byteLength(raw, "utf8") > MAX_DATA_BYTES) throw new Error(`NATS event data exceeds ${MAX_DATA_BYTES} bytes`);
  let data: unknown;
  try { data = JSON.parse(raw); }
  catch { throw new UsageError("event data must be valid JSON"); }

  const id = flags.get("--id") ?? randomUUID();
  const connection = await openBootstrapNatsConnection({
    servers: flags.get("--servers")!,
    credentials: flags.get("--creds")!,
    tls: { caFile: flags.get("--ca")! },
    name: "pi-fabric-events-publish",
  });
  try {
    const publisher = await openNatsEventsPublisher(connection, { org: flags.get("--org")! });
    const { event, duplicate } = await publisher.publishWithAck({
      id,
      topic: flags.get("--topic")!,
      kind: "message",
      from: { id: "mail-service", name: "mail-service", kind: "agent" },
      data,
      origin: "external",
      untrusted: true,
      createdAt: Date.now(),
    });
    process.stdout.write(`${JSON.stringify({ stream: publisher.stream, seq: event.sequence, duplicate })}\n`);
  } finally {
    await connection.close();
  }
  return 0;
};

/** `pi-fabric events publish`: one plain-Node producer event, without a Fabric host identity. */
export const runEventsCli = async (argv: string[]): Promise<number> => {
  const [command, ...rest] = argv;
  if (!command || command === "--help" || command === "-h" || rest.includes("--help")) {
    process.stdout.write(`${USAGE}\n`);
    return command ? 0 : 2;
  }
  try {
    if (command !== "publish") throw new UsageError(`unknown events command ${JSON.stringify(command)}`);
    return await publish(rest);
  } catch (error) {
    process.stderr.write(`pi-fabric events: ${error instanceof Error ? error.message : String(error)}\n`);
    if (error instanceof UsageError) {
      process.stderr.write(`${USAGE}\n`);
      return 2;
    }
    return 1;
  }
};
