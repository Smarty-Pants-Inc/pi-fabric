import {
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  JetStreamApiError,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  type ConsumerInfo,
  type ConsumerAPI,
  type JetStreamManager,
  type StreamConfig,
  type StreamInfo,
} from "@nats-io/jetstream";

const STREAM_NOT_FOUND = 10059;
const STREAM_EXISTS = 10058;
const CONSUMER_NOT_FOUND = 10014;
const nanos = (ms: number): number => ms * 1_000_000;
const code = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};

export interface TransportStreamPolicy {
  readonly name: string;
  readonly subjects: readonly string[];
  readonly maxBytes: number;
  readonly maxAgeMs: number;
  readonly maxMessageBytes: number;
  readonly duplicateWindowMs: number;
  readonly replicas?: number;
  readonly discard?: DiscardPolicy;
  /** Existing Fabric event streams may be server-unbounded; publishers still enforce the application cap. */
  readonly allowUnboundedMaxMessageBytes?: boolean;
  /** Explicit legacy compatibility for an existing stream with max_age: 0; never updates it. */
  readonly allowUnboundedMaxAge?: boolean;
}

export const transportStreamConfig = (policy: TransportStreamPolicy): Partial<StreamConfig> => ({
  name: policy.name,
  subjects: [...policy.subjects],
  storage: StorageType.File,
  retention: RetentionPolicy.Limits,
  discard: policy.discard ?? DiscardPolicy.New,
  max_bytes: policy.maxBytes,
  max_age: nanos(policy.maxAgeMs),
  max_msg_size: policy.maxMessageBytes,
  max_msgs: -1,
  max_msgs_per_subject: -1,
  duplicate_window: nanos(policy.duplicateWindowMs),
  num_replicas: policy.replicas ?? 1,
  no_ack: false,
  allow_direct: false,
});

export const assertTransportStream = (info: StreamInfo, wanted: Partial<StreamConfig>, allowUnboundedMaxMessageBytes = false,
  allowUnboundedMaxAge = false): void => {
  const maxMessageBytesMatch = info.config.max_msg_size === wanted.max_msg_size ||
    (allowUnboundedMaxMessageBytes && info.config.max_msg_size === -1);
  const maxAgeMatch = info.config.max_age === wanted.max_age ||
    (allowUnboundedMaxAge && info.config.max_age === 0);
  const same = info.config.storage === wanted.storage &&
    info.config.retention === wanted.retention &&
    info.config.discard === wanted.discard &&
    info.config.max_bytes === wanted.max_bytes &&
    maxAgeMatch &&
    maxMessageBytesMatch &&
    info.config.max_msgs === wanted.max_msgs &&
    info.config.max_msgs_per_subject === wanted.max_msgs_per_subject &&
    info.config.duplicate_window === wanted.duplicate_window &&
    info.config.num_replicas === wanted.num_replicas &&
    (info.config.no_ack ?? false) === (wanted.no_ack ?? false) &&
    (info.config.allow_direct ?? false) === (wanted.allow_direct ?? false) &&
    info.config.subjects.join("\n") === (wanted.subjects as string[]).join("\n");
  if (!same) throw new Error(`NATS stream configuration mismatch: ${info.config.name}`);
};

export const ensureTransportStream = async (manager: JetStreamManager, policy: TransportStreamPolicy, provision = true): Promise<StreamInfo> => {
  const wanted = transportStreamConfig(policy);
  let info: StreamInfo;
  try { info = await manager.streams.info(policy.name); }
  catch (error) {
    if (code(error) !== STREAM_NOT_FOUND) throw error;
    if (!provision) throw new Error(`required NATS stream ${policy.name} is not provisioned`);
    try { info = await manager.streams.add(wanted as StreamConfig & { name: string }); }
    catch (createError) {
      if (code(createError) !== STREAM_EXISTS) throw createError;
      info = await manager.streams.info(policy.name);
    }
  }
  assertTransportStream(info, wanted, policy.allowUnboundedMaxMessageBytes, policy.allowUnboundedMaxAge);
  return info;
};

/** Org-wide Fabric event history. Fabric-store provisions this; runtime opens only verify it. */
export interface NatsEventsStreamOptions {
  readonly org: string;
  /** Pass mesh.nats.provision. Only literal true permits creation; existing streams are never updated. */
  readonly provision?: boolean;
  readonly maxBytes?: number;
  readonly maxMessageBytes?: number;
  readonly duplicateWindowMs?: number;
  readonly replicas?: number;
}

const normalizedNatsOrg = (org: string): string => {
  if (typeof org !== "string" || /^[a-z0-9][a-z0-9-]{0,62}$/.exec(org)?.[0] !== org || org === "sys") {
    throw new Error("NATS events org must be a GitHub-login slug");
  }
  return org.toUpperCase().replaceAll("-", "_");
};

export const natsEventsStreamName = (org: string): string => `FABRIC_EV_${normalizedNatsOrg(org)}`;
export const natsGithubStreamName = (org: string): string => `GITHUB_${normalizedNatsOrg(org)}`;

export const ensureNatsEventsStream = async (manager: JetStreamManager, options: NatsEventsStreamOptions): Promise<StreamInfo> => {
  const name = natsEventsStreamName(options.org);
  const maxBytes = options.maxBytes ?? 2_147_483_648;
  const maxMessageBytes = options.maxMessageBytes ?? 1_048_576;
  const duplicateWindowMs = options.duplicateWindowMs ?? 2 * 60 * 1_000;
  const replicas = options.replicas ?? 1;
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw new Error("NATS events maxBytes must be a positive finite safe integer");
  if (!Number.isSafeInteger(maxMessageBytes) || maxMessageBytes <= 0 || maxMessageBytes > maxBytes) {
    throw new Error("NATS events maxMessageBytes must be a positive safe integer no greater than maxBytes");
  }
  if (!Number.isSafeInteger(duplicateWindowMs) || duplicateWindowMs <= 0 || !Number.isSafeInteger(nanos(duplicateWindowMs))) {
    throw new Error("NATS events duplicateWindowMs must be positive and representable in nanoseconds");
  }
  if (!Number.isSafeInteger(replicas) || replicas <= 0) throw new Error("NATS events replicas must be a positive safe integer");
  return ensureTransportStream(manager, {
    name,
    subjects: [`fabric.ev.${options.org}.>`],
    maxBytes,
    maxAgeMs: 0,
    maxMessageBytes,
    duplicateWindowMs,
    replicas,
    discard: DiscardPolicy.New,
    allowUnboundedMaxMessageBytes: true,
  }, options.provision === true);
};

/** BUILD NOW provisioning entry: existing compatible streams are accepted; only an explicit admin path may create. */
export const ensureFabricEventsStream = (manager: JetStreamManager, org: string, provision: boolean): Promise<StreamInfo> =>
  ensureNatsEventsStream(manager, { org, provision });

export interface TransportConsumerPolicy {
  readonly stream: string;
  readonly durable: string;
  readonly filterSubject: string;
  readonly description: string;
  readonly maxAckPending: number;
  /** JetStream nanoseconds; omitted preserves the normal-host inactive_threshold: 0 policy. */
  readonly inactiveThresholdNs?: number;
}

export const ensureTransportConsumer = async (api: ConsumerAPI, policy: TransportConsumerPolicy): Promise<ConsumerInfo> => {
  const backoff = Array.from({ length: 5 }, () => nanos(30_000));
  let info: ConsumerInfo;
  try { info = await api.info(policy.stream, policy.durable); }
  catch (error) {
    if (code(error) !== CONSUMER_NOT_FOUND) throw error;
    try {
      info = await api.add(policy.stream, {
        durable_name: policy.durable,
        description: policy.description,
        filter_subject: policy.filterSubject,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        replay_policy: ReplayPolicy.Instant,
        ack_wait: nanos(30_000),
        max_deliver: 5,
        backoff,
        max_ack_pending: policy.maxAckPending,
        max_waiting: 1,
        inactive_threshold: policy.inactiveThresholdNs ?? 0,
      });
    } catch (createError) {
      // A concurrent opener may have won the create. Read it back only if the winner exists;
      // preserve permission and validation failures instead of masking them as a race.
      try { info = await api.info(policy.stream, policy.durable); }
      catch { throw createError; }
    }
  }
  const actual = info.config;
  const inactiveThresholdMismatch = policy.inactiveThresholdNs === undefined
    ? actual.inactive_threshold !== 0 && actual.inactive_threshold !== undefined
    : actual.inactive_threshold !== policy.inactiveThresholdNs;
  if (actual.durable_name !== policy.durable || actual.filter_subject !== policy.filterSubject || actual.filter_subjects?.length ||
    actual.deliver_subject || actual.headers_only || actual.ack_policy !== AckPolicy.Explicit || actual.replay_policy !== ReplayPolicy.Instant ||
    actual.max_waiting !== 1 || actual.deliver_policy !== DeliverPolicy.All || actual.ack_wait !== nanos(30_000) || actual.max_deliver !== 5 ||
    actual.max_ack_pending !== policy.maxAckPending || inactiveThresholdMismatch ||
    JSON.stringify(actual.backoff) !== JSON.stringify(backoff)) {
    throw new Error(`NATS consumer configuration mismatch: ${policy.durable}`);
  }
  return info;
};
