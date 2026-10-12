import fs from "node:fs";
import path from "node:path";
import { MESH_EXTERNAL_DATA_MAX_BYTES, MESH_GRANT_TOKEN_ENV, postWithMeshGrant } from "../mesh/grants.js";
import { MeshStore } from "../mesh/store.js";

const USAGE = [
  "Usage: pi-fabric mesh post --root <meshRoot> [--token <token>] [--topic <topic>] [--kind <kind>]",
  "                          [--data '<json>' | --data-file <file> | --data-file -]",
  "",
  `Appends one untrusted event through a scoped grant from mesh.grant. Prefer the`,
  `${MESH_GRANT_TOKEN_ENV} environment variable over --token so the token stays out of`,
  "process listings. --topic, when given, must match the grant. --data-file - reads stdin.",
  `Data is JSON of at most ${MESH_EXTERNAL_DATA_MAX_BYTES} bytes.`,
].join("\n");

// Generous store ceilings: the grant carries the minting mesh's event limit.
const CLI_MAX_EVENT_BYTES = 256 * 1024;
const CLI_MAX_READ_EVENTS = 100;
const VALUE_FLAGS = new Set(["--root", "--token", "--topic", "--kind", "--data", "--data-file"]);

class UsageError extends Error {}

const parseFlags = (argv: string[]): Map<string, string> => {
  const flags = new Map<string, string>();
  for (let index = 0; index < argv.length; index++) {
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

const readBounded = async (source: string): Promise<string> => {
  const chunks: Buffer[] = [];
  let bytes = 0;
  const stream = source === "-" ? process.stdin : fs.createReadStream(source);
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    bytes += buffer.length;
    if (bytes > MESH_EXTERNAL_DATA_MAX_BYTES) {
      if (stream !== process.stdin) (stream as fs.ReadStream).destroy();
      throw new Error(`External mesh data exceeds ${MESH_EXTERNAL_DATA_MAX_BYTES} bytes`);
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
};

async function post(argv: string[]): Promise<number> {
  const flags = parseFlags(argv);
  const root = flags.get("--root");
  if (!root) throw new UsageError("--root is required");
  const token = flags.get("--token") ?? process.env[MESH_GRANT_TOKEN_ENV];
  if (!token) throw new UsageError(`--token or ${MESH_GRANT_TOKEN_ENV} is required`);
  if (flags.has("--data") && flags.has("--data-file")) throw new UsageError("use --data or --data-file, not both");
  const resolvedRoot = path.resolve(root);
  // Never create a mesh where none exists: a typo must not mint an empty root.
  if (!fs.statSync(resolvedRoot, { throwIfNoEntry: false })?.isDirectory()) {
    throw new Error(`Fabric mesh root does not exist: ${resolvedRoot}`);
  }
  const raw = flags.has("--data-file") ? await readBounded(flags.get("--data-file")!) : flags.get("--data");
  let data: unknown;
  if (raw !== undefined) {
    if (Buffer.byteLength(raw, "utf8") > MESH_EXTERNAL_DATA_MAX_BYTES) {
      throw new Error(`External mesh data exceeds ${MESH_EXTERNAL_DATA_MAX_BYTES} bytes`);
    }
    try {
      data = JSON.parse(raw);
    } catch {
      throw new UsageError("--data must be valid JSON");
    }
  }
  const store = new MeshStore(resolvedRoot, CLI_MAX_EVENT_BYTES, CLI_MAX_READ_EVENTS);
  const { event, grant } = await postWithMeshGrant(store, {
    token,
    ...(flags.has("--topic") ? { topic: flags.get("--topic")! } : {}),
    ...(flags.has("--kind") ? { kind: flags.get("--kind")! } : {}),
    ...(data !== undefined ? { data } : {}),
  });
  process.stdout.write(`${JSON.stringify({
    ok: true, id: event.id, sequence: event.sequence, topic: event.topic, kind: event.kind,
    grantId: grant.grantId, usesLeft: grant.uses,
  })}\n`);
  return 0;
}

/** `pi-fabric mesh <subcommand>`; exit 0 on success, 1 on refusal, 2 on usage errors. */
export async function runMeshCli(argv: string[]): Promise<number> {
  const [subcommand, ...rest] = argv;
  if (!subcommand || subcommand === "--help" || subcommand === "-h" || rest.includes("--help")) {
    process.stdout.write(`${USAGE}\n`);
    return subcommand ? 0 : 2;
  }
  try {
    if (subcommand === "post") return await post(rest);
    throw new UsageError(`unknown mesh subcommand ${JSON.stringify(subcommand)}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`pi-fabric mesh: ${message}\n`);
    if (error instanceof UsageError) {
      process.stderr.write(`${USAGE}\n`);
      return 2;
    }
    return 1;
  }
}
