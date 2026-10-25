#!/usr/bin/env bun
/** Standalone rollback for https://github.com/Smarty-Pants-Inc/smarty-dev/issues/16868. */
import { isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { AckPolicy, DeliverPolicy, ReplayPolicy, type ConsumerInfo } from "@nats-io/jetstream";
import { policyConfig } from "../src/mesh/nats-auth/send-cli.js";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { natsEventsStreamName } from "../src/transport/nats/provision.js";
import {
  hashToken,
  transportDurableName,
  transportInboxFilter,
  transportInboxStream,
  transportInboxStreamSubject,
  validateMachine,
} from "../src/transport/nats/subjects.js";

const USAGE = "Usage: bun tools/nats-inbox-downgrade.ts --host-id HOST [--kind rt|br] --machine MACHINE --creds ABS_PATH --ca ABS_PATH --servers tls://host:port[,tls://host:port...] [--org ORG] [--apply]";
const ACK_WAIT_NS = 30_000_000_000;
const UNCERTAIN = "NATS inbox apply outcome is uncertain; inspect with a dry-run before another apply";
class DowngradeError extends Error {}

export interface NatsInboxDowngradeOptions {
  machine: string;
  hostId: string;
  kind?: "rt" | "br";
  apply?: boolean;
}

export interface NatsInboxDowngradeReceipt {
  format: 1;
  mode: "dry-run" | "applied" | "unchanged";
  stream: string;
  durable: string;
  beforeInactiveThresholdNs: number;
  inactiveThresholdNs: number;
  targetInactiveThresholdNs: 0;
}

type InboxTarget = { stream: string; durable: string; filter: string; description: string };

function inboxTarget(options: NatsInboxDowngradeOptions): InboxTarget {
  const { hostId, machine, kind = "rt", apply } = options;
  if (typeof hostId !== "string" || !hostId || hostId !== hostId.trim() || hostId.length > 256 ||
    /[\u0000-\u001f\u007f]/.test(hostId) || typeof machine !== "string" || machine !== machine.trim() ||
    (kind !== "rt" && kind !== "br") || (apply !== undefined && typeof apply !== "boolean")) {
    throw new DowngradeError("invalid inbox downgrade options");
  }
  try { validateMachine(machine); }
  catch { throw new DowngradeError("invalid inbox downgrade options"); }
  const stream = transportInboxStream(machine);
  if (kind === "rt") {
    return { stream, durable: transportDurableName(hostId), filter: transportInboxFilter(machine, hostId),
      description: JSON.stringify({ fabricHostId: hostId, machine }) };
  }
  const link = hostId.startsWith("bridge:") ? hostId.slice("bridge:".length) : "";
  if (!link || link !== link.trim()) throw new DowngradeError("bridge host-id must be bridge:LINK with a nonempty link");
  // The native bridge hashes bridgeNatsLinkIdentity(link).id, not the bare link name.
  return { stream, durable: `br_${hashToken(hostId)}`, filter: transportInboxStreamSubject(machine),
    description: JSON.stringify({ fabricBridgeLink: link, machine }) };
}

function requireVerified(connection: Pick<NatsConnectionHandle, "isVerified">): void {
  if (connection.isVerified() !== true) throw new DowngradeError("NATS connection is not verified");
}

/** Match every shared transport consumer invariant except the deliberately changed lifetime. */
function validateInbox(info: ConsumerInfo, target: InboxTarget): number {
  const actual = info.config;
  if (actual.durable_name !== target.durable || actual.description !== target.description ||
    actual.filter_subject !== target.filter ||
    (actual.filter_subjects !== undefined && (!Array.isArray(actual.filter_subjects) || actual.filter_subjects.length !== 0)) ||
    (actual.deliver_subject !== undefined && actual.deliver_subject !== "") ||
    (actual.deliver_group !== undefined && actual.deliver_group !== "") ||
    (actual.headers_only !== undefined && actual.headers_only !== false) ||
    actual.ack_policy !== AckPolicy.Explicit || actual.deliver_policy !== DeliverPolicy.All ||
    actual.replay_policy !== ReplayPolicy.Instant || actual.ack_wait !== ACK_WAIT_NS || actual.max_deliver !== 5 ||
    !Array.isArray(actual.backoff) || actual.backoff.length !== 5 || Array.from(actual.backoff).some(wait => wait !== ACK_WAIT_NS) ||
    actual.max_ack_pending !== 128 || actual.max_waiting !== 1) {
    throw new DowngradeError("NATS inbox consumer configuration or owner mismatch");
  }
  // JetStream omits the zero-valued field in INFO responses for an unbounded durable.
  const threshold = actual.inactive_threshold === undefined ? 0 : actual.inactive_threshold;
  if (!Number.isSafeInteger(threshold) || threshold < 0) throw new DowngradeError("invalid NATS inbox inactive threshold");
  return threshold;
}

/**
 * Read one known inbox; only explicit apply can update its lifetime. Never creates, deletes,
 * pulls, ACKs, resets, or recreates a consumer. The caller retains ownership of the connection.
 */
export async function downgradeNatsInbox(
  connection: Pick<NatsConnectionHandle, "manager" | "isVerified">,
  options: NatsInboxDowngradeOptions,
): Promise<NatsInboxDowngradeReceipt> {
  try {
    const target = inboxTarget(options);
    const apply = options.apply === true;
    const api = connection.manager.consumers;
    requireVerified(connection);
    const before = await api.info(target.stream, target.durable).catch(() => {
      throw new DowngradeError("could not read NATS inbox consumer");
    });
    requireVerified(connection);
    const beforeInactiveThresholdNs = validateInbox(before, target);
    const receipt = (mode: NatsInboxDowngradeReceipt["mode"], inactiveThresholdNs: number): NatsInboxDowngradeReceipt => ({
      format: 1, mode, stream: target.stream, durable: target.durable,
      beforeInactiveThresholdNs, inactiveThresholdNs, targetInactiveThresholdNs: 0,
    });
    if (!apply) return receipt("dry-run", beforeInactiveThresholdNs);
    if (beforeInactiveThresholdNs === 0) return receipt("unchanged", 0);

    requireVerified(connection);
    // The official SDK reads/merges existing config and sends action=update. Never use add,
    // delete, or a whole-config replacement here; do not retry an ambiguous apply.
    await api.update(target.stream, target.durable, { inactive_threshold: 0 }).catch(() => {
      throw new DowngradeError(UNCERTAIN);
    });
    requireVerified(connection);
    const after = await api.info(target.stream, target.durable).catch(() => {
      throw new DowngradeError(UNCERTAIN);
    });
    requireVerified(connection);
    if (validateInbox(after, target) !== 0) throw new DowngradeError("NATS inbox inactive threshold was not cleared; inspect with a dry-run");
    return receipt("applied", 0);
  } catch (error) {
    // No broker cause, credential path/content, owner description, or config escapes this API.
    if (error instanceof DowngradeError) throw error;
    throw new DowngradeError("NATS inbox downgrade failed; inspect with a dry-run before another apply");
  }
}

type CliOptions = NatsInboxDowngradeOptions & { creds: string; ca: string; servers: string[]; org?: string };

function parseOptions(argv: readonly string[]): CliOptions {
  const valuedFlags = new Set(["--host-id", "--kind", "--machine", "--creds", "--ca", "--servers", "--org"]);
  const seen = new Set<string>();
  const values = new Map<string, string>();
  const invalid: () => never = () => { throw new DowngradeError(`invalid arguments; ${USAGE}`); };
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index]!;
    if (seen.has(flag) || (flag !== "--apply" && !valuedFlags.has(flag))) invalid();
    seen.add(flag);
    if (flag === "--apply") continue;
    const value = argv[++index];
    if (!value || value.startsWith("-") || value !== value.trim() || /[\u0000-\u001f\u007f]/.test(value)) invalid();
    values.set(flag, value);
  }
  const required = (flag: string): string => values.get(flag) ?? invalid();
  const kind = values.get("--kind") ?? "rt";
  if (kind !== "rt" && kind !== "br") invalid();
  const creds = required("--creds"), ca = required("--ca");
  if (!isAbsolute(creds) || !isAbsolute(ca)) invalid();
  const servers = required("--servers").split(",");
  for (const server of servers) {
    const match = /^tls:\/\/([A-Za-z0-9.-]+):([0-9]{1,5})$/.exec(server);
    if (!match || match[0] !== server || Number(match[2]) < 1 || Number(match[2]) > 65_535) invalid();
  }
  const org = values.get("--org");
  if (org !== undefined) {
    // Reuse the existing non-system org validator; no stream name is caller-selectable.
    try { natsEventsStreamName(org); } catch { invalid(); }
  }
  const parsed: CliOptions = {
    hostId: required("--host-id"), machine: required("--machine"), kind,
    creds, ca, servers, apply: seen.has("--apply"), ...(org === undefined ? {} : { org }),
  };
  inboxTarget(parsed);
  return parsed;
}

/** JSON receipt on stdout, fixed/redacted diagnostic on stderr, exit 0 on success or 1 on refusal. */
export async function runNatsInboxDowngrade(argv: readonly string[]): Promise<number> {
  let connection: NatsConnectionHandle | undefined;
  let receipt: NatsInboxDowngradeReceipt | undefined;
  let failure: unknown;
  try {
    const parsed = parseOptions(argv);
    // Exactly the normal Path B runtime/bridge agent route: no operator, bootstrap or grant fallback.
    const policy = policyConfig();
    connection = await openNatsConnection({
      machine: parsed.machine, credentials: parsed.creds, servers: parsed.servers, tls: { caFile: parsed.ca },
      custodyStream: policy.custodyStream, livenessStream: policy.livenessStream, pathBMachines: policy.pathBMachines,
      ...(parsed.org === undefined ? {} : { org: parsed.org }), role: "agent", name: "fabric-nats-inbox-downgrade",
    });
    receipt = await downgradeNatsInbox(connection, parsed);
  } catch (error) {
    failure = error;
  } finally {
    if (connection) {
      try { await connection.close(); }
      catch { failure ??= new DowngradeError("could not close NATS connection; inspect with a dry-run before another apply"); }
    }
  }
  if (failure !== undefined || receipt === undefined) {
    const message = failure instanceof DowngradeError ? failure.message : "NATS inbox downgrade connection or policy check failed";
    process.stderr.write(`${message}\n`);
    return 1;
  }
  process.stdout.write(`${JSON.stringify(receipt)}\n`);
  return 0;
}

// Works as a full Bun subprocess while an import from tests never invokes the CLI.
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  void runNatsInboxDowngrade(process.argv.slice(2)).then(code => { process.exitCode = code; });
}
