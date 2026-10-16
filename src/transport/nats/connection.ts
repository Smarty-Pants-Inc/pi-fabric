import { readFileSync } from "node:fs";
import { Buffer } from "node:buffer";
import { credsAuthenticator, type NatsConnection } from "@nats-io/nats-core";
import { jetstream, jetstreamManager, type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import { connect, type Status } from "@nats-io/transport-node";
import { PATH_B_ADVISORY_DURABLE } from "../../mesh/nats-auth/permissions.js";

export type NatsStatusListener = (status: Status) => void;
export type SubscribeNatsStatus = (listener: NatsStatusListener) => () => void;

export interface NatsConnectionOptions {
  readonly servers: string | readonly string[];
  readonly credentials?: string;
  readonly name?: string;
  readonly timeoutMs?: number;
  readonly inboxPrefix?: string;
  /** Verify the authenticated user and its machine-bound, server-expanded source grant. */
  readonly machine?: string;
  readonly verifyMachine?: boolean;
}

export interface NatsConnectionHandle {
  readonly nc: NatsConnection;
  readonly js: JetStreamClient;
  readonly manager: JetStreamManager;
  readonly userInfo?: unknown;
  onStatus: SubscribeNatsStatus;
  close(): Promise<void>;
}

const USER_NKEY = /^U[A-Z2-7]{55}$/;
const OPERATOR_MACHINE = /^fabric-ops(?:-[a-z0-9]+)?$/;

/** Read only the public user identity from a NATS creds file; the full bytes stay in memory for auth. */
export const readNatsCredentials = (file: string): { readonly bytes: Uint8Array; readonly subject: string; readonly tags: readonly string[] } => {
  const bytes = Uint8Array.from(readFileSync(file));
  const text = Buffer.from(bytes).toString("utf8");
  const match = text.match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2})\s*-{3,}END NATS USER JWT-{3,}/);
  if (!match?.[1]) throw new Error("NATS credentials do not contain a user JWT");
  const segments = match[1].split(".");
  let payload: unknown;
  try {
    payload = JSON.parse(Buffer.from(segments[1]!, "base64url").toString("utf8"));
  } catch {
    throw new Error("NATS user JWT is invalid");
  }
  const subject = payload && typeof payload === "object" && typeof (payload as Record<string, unknown>).sub === "string"
    ? (payload as Record<string, unknown>).sub as string
    : "";
  if (!USER_NKEY.test(subject)) throw new Error("NATS user JWT subject is invalid");
  const nats = (payload as Record<string, unknown>).nats as Record<string, unknown> | undefined;
  const tags = Array.isArray(nats?.tags) && nats.tags.every((tag) => typeof tag === "string") ? nats.tags as string[] : [];
  return { bytes, subject, tags };
};

const OPERATOR_API_GRANTS = new Set([
  "$JS.API.INFO", "$SYS.REQ.USER.INFO", "$JS.API.STREAM.INFO.FABRIC_ADV",
  `$JS.API.CONSUMER.INFO.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
  `$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
]);

const isOperatorMetadataGrant = (subject: string): boolean => {
  if (OPERATOR_API_GRANTS.has(subject)) return true;
  const match = subject.match(/^\$JS\.API\.(STREAM\.INFO|CONSUMER\.INFO)\.FABRIC_IN_([a-z0-9][a-z0-9-]{0,31})(\.>)?$/);
  return !!match && !OPERATOR_MACHINE.test(match[2]!) &&
    (match[1] === "CONSUMER.INFO" ? match[3] === ".>" : match[3] === undefined);
};

const hasForeignSourceGrant = (subject: string, machine: string): boolean => {
  const tokens = subject.split(".");
  const tail = tokens.at(-1) === ">";
  if ((!tail && tokens.length !== 7) || (tail && tokens.length > 7)) return false;
  const fixed = tail ? tokens.slice(0, -1) : tokens;
  const family = ["fabric", "in"];
  if (family.some((token, index) => fixed[index] !== undefined && fixed[index] !== "*" && fixed[index] !== token)) return false;
  if (fixed[4] !== undefined && fixed[4] !== "*" && fixed[4] !== "cmd" && fixed[4] !== "ack") return false;
  return tail || fixed[6] !== machine;
};

const verifyMachineIdentity = async (nc: NatsConnection, machine: string, timeoutMs: number, auth: ReturnType<typeof readNatsCredentials> | undefined): Promise<unknown> => {
  const response = await nc.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: timeoutMs });
  let value: unknown;
  try { value = JSON.parse(new TextDecoder().decode(response.data)); } catch { throw new Error("NATS USER.INFO response is invalid"); }
  // USER.INFO 2.11.17 returns the authenticated user and expanded permissions, not JWT tags.
  // Tie the locally decoded tag to the server-authenticated subject, then require its exact
  // expanded own-source grant. E19's reserved operator has no source publication at all:
  // require the fixed metadata-only API set and explicit all-ACK denial instead.
  // A decoded JWT alone is never an identity proof.
  const data = value && typeof value === "object" ? (value as { data?: { user?: unknown; permissions?: { publish?: { allow?: unknown; deny?: unknown } } } }).data : undefined;
  const allow = data?.permissions?.publish?.allow;
  const deny = data?.permissions?.publish?.deny;
  const operator = auth?.tags.includes("role:maintainer") ?? false;
  const ownSourceGrant = `fabric.in.*.*.*.*.${machine}`;
  if (!auth || auth.tags.filter((tag) => tag.startsWith("machine:")).length !== 1 || !auth.tags.includes(`machine:${machine}`) || data?.user !== auth.subject ||
    !Array.isArray(allow) || !(operator
      ? OPERATOR_MACHINE.test(machine) && [...OPERATOR_API_GRANTS].every((subject) => allow.includes(subject)) &&
        Array.isArray(deny) && deny.includes("$JS.ACK.>") &&
        allow.every((subject) => typeof subject === "string" && isOperatorMetadataGrant(subject))
      : allow.includes(ownSourceGrant) && allow.every((subject) => typeof subject === "string" && !hasForeignSourceGrant(subject, machine)))) {
    throw new Error("NATS USER.INFO machine tag mismatch");
  }
  return value;
};

export const openNatsConnection = async (options: NatsConnectionOptions): Promise<NatsConnectionHandle> => {
  const timeoutMs = options.timeoutMs ?? 5_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new Error("timeoutMs must be positive");
  const auth = options.credentials ? readNatsCredentials(options.credentials) : undefined;
  const nc = await connect({
    servers: typeof options.servers === "string" ? options.servers : [...options.servers],
    ...(options.name ? { name: options.name } : {}),
    timeout: timeoutMs,
    reconnect: true,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 100,
    ...(auth ? {
      authenticator: credsAuthenticator(auth.bytes),
      inboxPrefix: options.inboxPrefix ?? `_INBOX.${auth.subject}`,
    } : options.inboxPrefix ? { inboxPrefix: options.inboxPrefix } : {}),
  });
  try {
    const manager = await jetstreamManager(nc, { timeout: timeoutMs });
    const js = jetstream(nc, { timeout: timeoutMs });
    const userInfo = options.machine && options.verifyMachine !== false
      ? await verifyMachineIdentity(nc, options.machine, timeoutMs, auth)
      : undefined;
    const listeners = new Set<NatsStatusListener>();
    const statusTask = (async () => {
      for await (const status of nc.status()) {
        for (const listener of listeners) listener(status);
      }
    })();
    return {
      nc,
      js,
      manager,
      ...(userInfo === undefined ? {} : { userInfo }),
      onStatus: (listener) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
      close: async () => {
        await nc.drain().catch(() => undefined);
        await statusTask;
        listeners.clear();
      },
    };
  } catch (error) {
    await nc.close();
    throw error;
  }
};
