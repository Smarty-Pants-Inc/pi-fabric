import { Buffer } from "node:buffer";
import {
  AckPolicy,
  DeliverPolicy,
  JetStreamApiError,
  ReplayPolicy,
  type ConsumerInfo,
  type ConsumerMessages,
  type JsMsg,
} from "@nats-io/jetstream";
import type { MeshEvent } from "../../mesh/event-log.js";
import type { NatsConnectionHandle } from "./connection.js";
import { ensureFabricEventsStream, natsEventsStreamName, natsGithubStreamName } from "./provision.js";
import { hashToken } from "./subjects.js";

const CONSUMER_NOT_FOUND = 10014;
const ACK_WAIT_NS = 60_000 * 1_000_000;
const MAX_ACK_PENDING = 128;
type StreamKind = "ev" | "gh";

export interface NatsActorEventsConsumerOptions {
  readonly org: string;
  readonly actorId: string;
  /** Exact Fabric topics, or exact github.<org>.* projection subjects (no wildcards). */
  readonly topics: readonly string[];
  /** Epoch milliseconds, an ISO date string, or a Date. Applied only when creating each durable. */
  readonly eventsStartAt?: number | string | Date;
  /** Explicit admin-only stream creation; runtime event mode always passes false. */
  readonly provision?: boolean;
  /** Persist the actor queue and message-ID receipt, then confirm ACK before starting the actor drain. */
  readonly onMessage: (event: MeshEvent, messageId: string, ack: () => Promise<void>) => Promise<void>;
}

export interface NatsActorEventsConsumer {
  updateTopics(topics: readonly string[], active?: boolean): Promise<void>;
  /** Stop and drain local delivery; remove=true also deletes every durable owned by this actor. */
  close(remove?: boolean): Promise<void>;
}

const apiCode = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};

const validTopic = (topic: string): boolean => typeof topic === "string" &&
  /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/.exec(topic)?.[0] === topic &&
  !topic.split(".").some(token => token.length === 0);

const exactSubject = (org: string, topic: string): { kind: StreamKind; subject: string } => {
  if (!validTopic(topic)) throw new Error("Invalid exact NATS actor event topic");
  if (topic.startsWith("github.")) {
    if (!topic.startsWith(`github.${org}.`)) throw new Error("NATS actor GitHub topic belongs to another org");
    return { kind: "gh", subject: topic };
  }
  if (topic.startsWith("fabric.ev.")) {
    if (!topic.startsWith(`fabric.ev.${org}.`)) throw new Error("NATS actor Fabric subject belongs to another org");
    return { kind: "ev", subject: topic };
  }
  return { kind: "ev", subject: `fabric.ev.${org}.${topic}` };
};

const topicPlans = (org: string, actorId: string, topics: readonly string[]): Record<StreamKind, string[]> => {
  if (!Array.isArray(topics)) throw new Error("NATS actor event topics must be an array");
  if (!/^[a-zA-Z0-9:_/-]{1,128}$/.test(actorId)) throw new Error("NATS event actorId must fit one subject token");
  const fabric = new Set<string>([`fabric.ev.${org}.wake.${actorId}`]);
  const github = new Set<string>();
  for (const topic of topics) {
    const route = exactSubject(org, topic);
    (route.kind === "ev" ? fabric : github).add(route.subject);
  }
  return { ev: [...fabric].sort(), gh: [...github].sort() };
};

const subjectForEventTopic = (org: string, topic: string): string => {
  const route = exactSubject(org, topic);
  return route.subject;
};

const decodeEvent = (message: JsMsg, org: string): { event: MeshEvent; messageId: string } => {
  const value: unknown = JSON.parse(Buffer.from(message.data).toString("utf8"));
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid NATS MeshEvent");
  const event = value as MeshEvent;
  if (typeof event.id !== "string" || event.id.length === 0 || typeof event.topic !== "string" ||
    typeof event.kind !== "string" || event.from === null || typeof event.from !== "object" || Array.isArray(event.from) ||
    typeof event.from.id !== "string" || typeof event.from.name !== "string" ||
    !["main", "actor", "agent"].includes(event.from.kind) || !Number.isFinite(event.createdAt) ||
    subjectForEventTopic(org, event.topic) !== message.subject) throw new Error("Invalid NATS MeshEvent envelope or subject");
  const sequence = message.info.streamSequence;
  if (!Number.isSafeInteger(sequence) || sequence <= 0) throw new Error("Invalid NATS event stream sequence");
  // The stream owns ordering, including projections whose upstream sequence may differ or be absent.
  event.sequence = sequence;
  const headerId = message.headers?.get("Nats-Msg-Id");
  const messageId = typeof headerId === "string" && headerId.trim().length > 0 ? headerId : String(sequence);
  return { event, messageId };
};

interface DeliveryRun { messages: ConsumerMessages; done: Promise<void>; stopping: boolean }
interface ManagedGroup {
  update(subjects: string[]): Promise<void>;
  close(remove: boolean): Promise<void>;
}

const startGroup = async (
  connection: NatsConnectionHandle,
  options: NatsActorEventsConsumerOptions,
  kind: StreamKind,
  subjects: string[],
): Promise<ManagedGroup | undefined> => {
  const stream = kind === "ev" ? natsEventsStreamName(options.org) : natsGithubStreamName(options.org);
  const durable = `${kind}_${hashToken(options.actorId)}`;
  const api = connection.manager.consumers;
  const mutableConfig = (filters: string[]) => ({
    filter_subject: "",
    filter_subjects: filters,
    ack_wait: ACK_WAIT_NS,
    max_deliver: 5,
    max_ack_pending: MAX_ACK_PENDING,
    backoff: [],
    inactive_threshold: 0,
  });
  const assertCompatible = (info: ConsumerInfo): void => {
    const config = info.config;
    if (config.durable_name !== durable || config.deliver_subject || config.headers_only ||
      config.ack_policy !== AckPolicy.Explicit || config.replay_policy !== ReplayPolicy.Instant) {
      throw new Error(`Incompatible NATS actor events consumer: ${durable}`);
    }
  };

  let info: ConsumerInfo;
  let created = false;
  try { info = await api.info(stream, durable); }
  catch (error) {
    const code = apiCode(error);
    if (subjects.length === 0 && (code === CONSUMER_NOT_FOUND || code === 10059)) return undefined;
    if (code !== CONSUMER_NOT_FOUND) throw error;
    const startTime = options.eventsStartAt === undefined ? undefined : new Date(options.eventsStartAt).toISOString();
    try {
      info = await api.add(stream, {
        ...mutableConfig(subjects),
        durable_name: durable,
        ack_policy: AckPolicy.Explicit,
        replay_policy: ReplayPolicy.Instant,
        deliver_policy: startTime === undefined ? DeliverPolicy.All : DeliverPolicy.StartTime,
        ...(startTime === undefined ? {} : { opt_start_time: startTime }),
      });
      created = true;
    } catch (createError) {
      // A concurrent opener may have created this actor's durable. Never recreate/reset its cursor.
      try { info = await api.info(stream, durable); }
      catch { throw createError; }
    }
  }
  assertCompatible(info);
  if (!created && subjects.length > 0) await api.update(stream, durable, mutableConfig(subjects));

  let active: DeliveryRun | undefined;
  let failure: Error | undefined;
  let closing = false;
  let operations = Promise.resolve();
  let closeTask: Promise<void> | undefined;
  let removeTask: Promise<void> | undefined;
  const serialize = (operation: () => Promise<void>): Promise<void> => {
    const result = operations.then(operation);
    operations = result.catch(() => undefined);
    return result;
  };
  const startDelivery = async (): Promise<void> => {
    if (subjects.length === 0 || closing) return;
    const consumer = await connection.js.consumers.get(stream, durable);
    // The connection's inboxPrefix (`_INBOX.<user pubkey>`) is honored by consumer.consume's request/reply path.
    const messages = await consumer.consume({ max_messages: 1, abort_on_missing_resource: true });
    const allowed = new Set(subjects);
    const run: DeliveryRun = { messages, stopping: false, done: Promise.resolve() };
    active = run;
    run.done = (async () => {
      try {
        for await (const message of messages) {
          if (run.stopping || closing || !allowed.has(message.subject)) {
            message.nak();
            if (run.stopping || closing) break;
            continue;
          }
          let acknowledged = false;
          const ack = async (): Promise<void> => {
            if (acknowledged) return;
            const confirmed = await message.ackAck();
            if (!confirmed) throw new Error("NATS actor event ACK was not confirmed");
            acknowledged = true;
          };
          try {
            const { event, messageId } = decodeEvent(message, options.org);
            await options.onMessage(event, messageId, ack);
            await ack();
          } catch {
            if (!acknowledged) message.nak();
            continue;
          }
        }
        const error = await messages.closed();
        if (error) throw error;
      } catch (error) {
        if (!run.stopping && !closing) failure = error instanceof Error ? error : new Error("NATS actor events delivery failed", { cause: error });
      }
    })();
  };
  const stopDelivery = async (): Promise<void> => {
    const run = active;
    if (!run) return;
    run.stopping = true;
    try { await run.messages.close(); }
    finally { await run.done; active = undefined; }
  };
  const removeDurable = (): Promise<void> => {
    if (!removeTask) {
      removeTask = api.delete(stream, durable).then(() => undefined).catch((error: unknown) => {
        removeTask = undefined;
        if (apiCode(error) !== CONSUMER_NOT_FOUND) throw error;
      });
    }
    return removeTask;
  };

  await startDelivery();
  return {
    async update(next) {
      // Empty means suspend this local pull without replacing its durable or resetting the cursor.
      const captured = [...next];
      if (closing) throw new Error("NATS actor events consumer is closed");
      await serialize(async () => {
        if (closing) throw new Error("NATS actor events consumer is closed");
        if (failure) throw failure;
        await stopDelivery();
        if (captured.length > 0) await api.update(stream, durable, mutableConfig(captured));
        subjects = captured;
        await startDelivery();
      });
    },
    close(remove) {
      closing = true;
      if (!closeTask) closeTask = serialize(stopDelivery);
      return serialize(async () => {
        await closeTask;
        if (remove) await removeDurable();
        if (failure) throw failure;
      });
    },
  };
};

/**
 * Per actor, maintain an EV durable for Fabric events and stall-wakes; add a GH durable only when subscribed.
 * Each stream has an independent cursor; callbacks persist queue/receipt state before the broker ACK.
 */
export const startNatsActorEventsConsumer = async (
  connection: NatsConnectionHandle,
  options: NatsActorEventsConsumerOptions,
): Promise<NatsActorEventsConsumer> => {
  if (typeof options.org !== "string" || options.org.length === 0) throw new Error("NATS events org is required");
  if (typeof options.actorId !== "string" || options.actorId.length === 0) throw new Error("NATS event actorId is required");
  if (typeof options.onMessage !== "function") throw new Error("NATS event onMessage callback is required");
  if (options.provision === true) await ensureFabricEventsStream(connection.manager, options.org, true);
  const planned = topicPlans(options.org, options.actorId, options.topics);
  const groups = new Map<StreamKind, ManagedGroup>();
  try {
    const eventGroup = await startGroup(connection, options, "ev", planned.ev);
    if (!eventGroup) throw new Error("NATS Fabric events durable could not be opened");
    groups.set("ev", eventGroup);
    const githubGroup = await startGroup(connection, options, "gh", planned.gh);
    if (githubGroup) groups.set("gh", githubGroup);
  } catch (error) {
    await Promise.all([...groups.values()].map(group => group.close(false).catch(() => undefined)));
    throw error;
  }
  let closing = false;
  let operations = Promise.resolve();
  const serialize = (operation: () => Promise<void>): Promise<void> => {
    const result = operations.then(operation);
    operations = result.catch(() => undefined);
    return result;
  };
  let closeTask: Promise<void> | undefined;
  return {
    updateTopics(topics, active = true) {
      let next: Record<StreamKind, string[]>;
      try { next = topicPlans(options.org, options.actorId, topics); }
      catch (error) { return Promise.reject(error); }
      if (closing) return Promise.reject(new Error("NATS actor events consumer is closed"));
      if (!active) next = { ev: [], gh: [] };
      return serialize(async () => {
        if (closing) throw new Error("NATS actor events consumer is closed");
        await groups.get("ev")!.update(next.ev);
        let github = groups.get("gh");
        if (next.gh.length > 0 && !github) {
          github = await startGroup(connection, options, "gh", next.gh);
          if (github) groups.set("gh", github);
        } else if (github) {
          await github.update(next.gh);
        }
      });
    },
    close(remove = false) {
      closing = true;
      if (!closeTask) closeTask = serialize(async () => {
        await Promise.all([...groups.values()].map(group => group.close(remove)));
      });
      return closeTask;
    },
  };
};
