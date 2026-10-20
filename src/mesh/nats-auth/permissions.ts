/**
 * smarty-dev#7504 slice 1 (design sections 1.4, 9.2, 9.3, 10.1): permission templates of the four scoped signing keys.
 *
 * Pure: no keys, files or environment. `roleTemplate` returns a fresh object on every call, so mutating one result
 * can never widen the next. The custody (`<CUS>`) and liveness (`<LIVE>`) stream names are REQUIRED caller
 * configuration (open item O-2: the engine mapping is not invented here); every deny row is written with those literal
 * names, and a missing or invalid mapping is refused instead of silently omitting deny rows.
 *
 * The server expands `{{tag(mesh)}}` and `{{subject()}}` per user. Deny entries are always literal, because a missing
 * tag in a deny entry fails authentication. No role holds debug-attach subjects (9.3).
 *
 * Decision C: these lists are a model of nats-server authorization. Server acceptance and the real allow/refuse matrix
 * are the hosted E1 T2/T8 checks on nats-server 2.14.7 (owner fabric-store).
 */

export type NatsRole = "agent" | "principal" | "maintainer" | "break-glass";

export interface RoleTemplate {
  pub: { allow: string[]; deny: string[] };
  sub: { allow: string[] };
  allow_responses?: { readonly max: number };
}

export interface RoleStreams {
  custodyStream: string;
  livenessStream: string;
}

export class NatsPermissionsError extends Error {
  readonly field: string;
  constructor(field: string) {
    // Never carries the offending value: stream names, roles and orgs come from caller configuration.
    super(`invalid ${field}`);
    this.name = "NatsPermissionsError";
    this.field = field;
  }
}

export const NATS_ROLES: readonly NatsRole[] = ["agent", "principal", "maintainer", "break-glass"];

/** `<B>`: the user's bound mesh-root stream names, one per validated `mesh:<32 lowercase hex>` user tag. */
const BOUND = "FABRIC_MESH_{{tag(mesh)}}";
const CONSUMER_VERBS = ["CREATE", "MSG.NEXT", "DELETE", "INFO"] as const;
const OWN_INBOX = "_INBOX.{{subject()}}.>";
const USER_INFO = "$SYS.REQ.USER.INFO";
const JS_INFO = "$JS.API.INFO";

// One literal token: letters, digits, `_` and `-` only. JS regexes without the m flag anchor at the whole input, so a
// trailing LF, CR, U+2028 or U+2029 never matches; `.`, `*`, `>`, `$`, `{`, whitespace and path separators are excluded.
const STREAM_NAME = /^[A-Za-z0-9_-]{1,255}$/;
const RESERVED_PREFIXES = ["FABRIC_MESH_", "_INBOX"];

function fail(field: string): never {
  throw new NatsPermissionsError(field);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

function streamName(value: unknown, field: string): string {
  if (typeof value !== "string" || !STREAM_NAME.test(value)) fail(field);
  const upper = value.toUpperCase();
  // A custody or liveness stream is never a mesh root `<B>` and never an inbox.
  if (RESERVED_PREFIXES.some((prefix) => upper.startsWith(prefix))) fail(field);
  return value;
}

function streams(input: unknown): RoleStreams {
  if (!isPlainObject(input)) fail("streams");
  for (const key of Object.keys(input)) if (key !== "custodyStream" && key !== "livenessStream") fail("streams (unsupported field)");
  const custodyStream = streamName(input.custodyStream, "custodyStream");
  const livenessStream = streamName(input.livenessStream, "livenessStream");
  // Distinct streams; compared without case so they never share a store directory on a case-insensitive filesystem.
  if (custodyStream.toUpperCase() === livenessStream.toUpperCase()) fail("custodyStream/livenessStream");
  return { custodyStream, livenessStream };
}

function allowFor(role: NatsRole, cus: string, live: string): string[] {
  switch (role) {
    case "agent":
      return [
        `${BOUND}.state`,
        `${BOUND}.snapshot`,
        JS_INFO,
        `$JS.API.STREAM.INFO.${BOUND}`,
        `$JS.API.STREAM.MSG.GET.${BOUND}`,
        ...CONSUMER_VERBS.map((verb) => `$JS.API.CONSUMER.${verb}.${BOUND}.>`),
        USER_INFO,
      ];
    case "principal":
      return [
        JS_INFO,
        "$JS.API.STREAM.INFO.*",
        "$JS.API.STREAM.MSG.GET.*",
        ...CONSUMER_VERBS.map((verb) => `$JS.API.CONSUMER.${verb}.*.>`),
        "$JS.API.STREAM.NAMES",
        USER_INFO,
      ];
    case "maintainer":
      return [
        "*.state",
        "*.snapshot",
        JS_INFO,
        ...["INFO", "MSG.GET", "CREATE", "UPDATE", "DELETE", "PURGE"].map((verb) => `$JS.API.STREAM.${verb}.*`),
        "$JS.API.STREAM.NAMES",
        ...CONSUMER_VERBS.map((verb) => `$JS.API.CONSUMER.${verb}.*.>`),
        USER_INFO,
      ];
    case "break-glass":
      // 10.1: only stream INFO/CREATE/UPDATE on the literal custody and liveness streams; no MSG, mesh root or state.
      return [JS_INFO, ...[cus, live].flatMap((stream) => ["INFO", "CREATE", "UPDATE"].map((verb) => `$JS.API.STREAM.${verb}.${stream}`)), USER_INFO];
  }
}

function denyFor(role: NatsRole, cus: string, live: string): string[] {
  // Every role, break-glass included (9.2): custody direct get in both forms, custody/liveness purge and delete.
  const deny = [
    `$JS.API.DIRECT.GET.${cus}`,
    `$JS.API.DIRECT.GET.${cus}.>`,
    `$JS.API.STREAM.PURGE.${cus}`,
    `$JS.API.STREAM.PURGE.${live}`,
    `$JS.API.STREAM.DELETE.${cus}`,
    `$JS.API.STREAM.DELETE.${live}`,
  ];
  // Every normal role (10.1, Astra P1): no create or update of custody/liveness configuration.
  if (role !== "break-glass") {
    deny.push(
      `$JS.API.STREAM.CREATE.${cus}`,
      `$JS.API.STREAM.CREATE.${live}`,
      `$JS.API.STREAM.UPDATE.${cus}`,
      `$JS.API.STREAM.UPDATE.${live}`,
    );
  }
  return deny;
}

/** The scoped signing key template of one role (section 1.4 as amended). Throws on an unknown role or stream mapping. */
export function roleTemplate(role: NatsRole, input: RoleStreams): RoleTemplate {
  // Array membership, not an object lookup, so `constructor`/`__proto__` and case variants are unknown roles.
  if (typeof role !== "string" || !NATS_ROLES.includes(role)) fail("role");
  const { custodyStream, livenessStream } = streams(input);
  return {
    pub: { allow: allowFor(role, custodyStream, livenessStream), deny: denyFor(role, custodyStream, livenessStream) },
    sub: { allow: [OWN_INBOX] },
  };
}

/** Path B machine grants. This stays separate from #7504 so old roleTemplate callers remain byte-for-byte stable. */
export type PathBRole = "agent" | "operator";
export interface PathBRoleTemplate extends RoleTemplate {}

const PATH_B_MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;
const PATH_B_MACHINE_TAG = "{{tag(machine)}}";
const SERVICE_ORG = /^[a-z0-9][a-z0-9-]{0,62}$/;
export const SESSION_CHANNEL_OPERATIONS = ["snapshot", "history", "input", "model", "abort"] as const;

function serviceOrg(value: unknown): string {
  if (typeof value !== "string" || !SERVICE_ORG.test(value)) fail("org");
  return value;
}

function pathBMachine(value: string): string {
  if (value !== PATH_B_MACHINE_TAG && !PATH_B_MACHINE.test(value)) fail("machine");
  return value;
}

function pathBUnique(values: string[]): string[] {
  return [...new Set(values)];
}

/** Reserved operator principal, deliberately not a host in the runtime inventory (E14). */
export const PATH_B_OPERATOR_MACHINE = "fabric-ops";
/** Bootstrap provisions this durable; the messaging operator can bind, never create or delete it. */
export const PATH_B_ADVISORY_DURABLE = "transport_dlq_operator";
export const PATH_B_OPERATOR_MACHINE_PATTERN = /^fabric-ops(-[a-z0-9]+)?$/;

// Administrative mutations inventoried against both pinned upstream API constant tables:
// https://raw.githubusercontent.com/nats-io/nats-server/v2.11.17/server/jetstream_api.go
// https://raw.githubusercontent.com/nats-io/nats-server/v2.14.7/server/jetstream_api.go
// CREATE (including bare config.name), DELETE, PAUSE/UNPIN/RESET, leader/peer changes,
// message deletion and RESTORE change advisory state; RESET is absent in 2.11.17 but reserved.
// SNAPSHOT is read-like but mutates store snapshot-in-progress state, so reserve it too.
// System-account MOVE/CANCEL_MOVE carry a stream token; account-wide admin/templates do not
// and are not granted. INFO/list/GET remain reads. MSG.NEXT advances AckNone consumer state:
// denied separately to nonoperators, while the operator retains its fixed durable pull.
// Schedules/counters use data-publish headers, not additional API forms (v2.14.7 stream.go).
const PATH_B_RESERVED_MUTATIONS = [
  "$JS.API.CONSUMER.CREATE.FABRIC_ADV",
  `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
  `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}.>`,
  "$JS.API.CONSUMER.CREATE.FABRIC_ADV.>",
  "$JS.API.CONSUMER.DURABLE.CREATE.FABRIC_ADV.>",
  `$JS.API.CONSUMER.DELETE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
  "$JS.API.CONSUMER.DELETE.FABRIC_ADV.>",
  "$JS.API.CONSUMER.PAUSE.FABRIC_ADV.>",
  "$JS.API.CONSUMER.UNPIN.FABRIC_ADV.>",
  "$JS.API.CONSUMER.RESET.FABRIC_ADV.>",
  "$JS.API.CONSUMER.LEADER.STEPDOWN.FABRIC_ADV.>",
  "$JS.API.STREAM.CREATE.FABRIC_ADV",
  "$JS.API.STREAM.UPDATE.FABRIC_ADV",
  "$JS.API.STREAM.DELETE.FABRIC_ADV",
  "$JS.API.STREAM.PURGE.FABRIC_ADV",
  "$JS.API.STREAM.MSG.DELETE.FABRIC_ADV",
  "$JS.API.STREAM.SNAPSHOT.FABRIC_ADV",
  "$JS.API.STREAM.RESTORE.FABRIC_ADV",
  "$JS.API.STREAM.LEADER.STEPDOWN.FABRIC_ADV",
  "$JS.API.STREAM.PEER.REMOVE.FABRIC_ADV",
  "$JS.API.ACCOUNT.STREAM.MOVE.*.FABRIC_ADV",
  "$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.*.FABRIC_ADV",
];

function pathBRuntime(roleMachine: string, machines: readonly string[] = [], org?: string): PathBRoleTemplate {
  if (PATH_B_OPERATOR_MACHINE_PATTERN.test(roleMachine)) fail("machine");
  const inbox = `FABRIC_IN_${roleMachine}`;
  const privateInboxes = roleMachine === PATH_B_MACHINE_TAG ? machines.map((machine) => `FABRIC_IN_${pathBMachine(machine)}`) : [inbox];
  const allow = [
    `fabric.in.*.*.*.*.${roleMachine}`,
    `$JS.API.STREAM.INFO.${inbox}`,
    ...["CREATE", "INFO", "MSG.NEXT", "DELETE"].map((verb) => `$JS.API.CONSUMER.${verb}.${inbox}.>`),
    `$JS.ACK.${inbox}.>`,
    `$KV.fabric_presence.${roleMachine}.>`,
    `$KV.fabric_leases.${roleMachine}.>`,
    "$JS.FC.KV_fabric_presence.>",
    "$JS.FC.KV_fabric_leases.>",
    "$JS.API.STREAM.INFO.KV_fabric_presence",
    "$JS.API.STREAM.INFO.KV_fabric_leases",
    "$JS.API.STREAM.MSG.GET.KV_fabric_presence",
    "$JS.API.STREAM.MSG.GET.KV_fabric_leases",
    ...["CREATE", "INFO", "DELETE"].flatMap((verb) => [
      `$JS.API.CONSUMER.${verb}.KV_fabric_presence.>`,
      `$JS.API.CONSUMER.${verb}.KV_fabric_leases.>`,
    ]),
    JS_INFO,
    USER_INFO,
  ];
  const deny = [
    "$JS.API.STREAM.CREATE.>",
    "$JS.API.STREAM.UPDATE.>",
    "$JS.API.STREAM.DELETE.>",
    "$JS.API.STREAM.PURGE.>",
    ...privateInboxes.map((stream) => `$JS.API.STREAM.MSG.GET.${stream}`),
    "$JS.API.STREAM.MSG.DELETE.>",
    "$JS.API.DIRECT.GET.>",
    "$JS.API.CONSUMER.*.FABRIC_ADV.>",
    "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.>",
    ...PATH_B_RESERVED_MUTATIONS,
  ];
  if (org !== undefined) {
    const scopedOrg = serviceOrg(org);
    // Alpha deliberately shares one org runtime user across agents and callers. That user can read
    // service requests and race replies; its subject wildcard is the accepted same-org trust boundary.
    const serviceSubject = `fabric.svc.${scopedOrg}.>`;
    allow.push(serviceSubject);
    return {
      pub: { allow: pathBUnique(allow), deny },
      sub: { allow: [OWN_INBOX, serviceSubject] },
      allow_responses: { max: 16 },
    };
  }
  return { pub: { allow: pathBUnique(allow), deny }, sub: { allow: [OWN_INBOX] } };
}

function pathBOperator(roleMachine: string, machines: readonly string[]): PathBRoleTemplate {
  if (roleMachine !== PATH_B_MACHINE_TAG && !PATH_B_OPERATOR_MACHINE_PATTERN.test(roleMachine)) fail("machine");
  if (machines.length === 0) fail("machines");
  const inboxes = machines.map((machine) => {
    if (!PATH_B_MACHINE.test(machine) || PATH_B_OPERATOR_MACHINE_PATTERN.test(machine)) fail("machines");
    return `FABRIC_IN_${machine}`;
  });
  const allow = [
    "$JS.API.STREAM.INFO.FABRIC_ADV",
    ...inboxes.map((stream) => `$JS.API.STREAM.INFO.${stream}`),
    ...["INFO", "MSG.NEXT"].map((verb) => `$JS.API.CONSUMER.${verb}.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`),
    ...inboxes.flatMap((stream) => [`$JS.API.CONSUMER.INFO.${stream}.>`]),

    JS_INFO,
    USER_INFO,
  ];
  const deny = [
    "$JS.API.STREAM.CREATE.>",
    "$JS.API.STREAM.UPDATE.>",
    "$JS.API.STREAM.DELETE.>",
    "$JS.API.STREAM.PURGE.>",
    "$JS.API.CONSUMER.CREATE.>",
    "$JS.API.CONSUMER.DELETE.>",
    "$JS.ACK.>",
    "$JS.API.STREAM.MSG.GET.>",
    "$JS.API.DIRECT.GET.>",
    "fabric.in.>",
    "fabric.ev.>",
    ...PATH_B_RESERVED_MUTATIONS,
    ...inboxes.flatMap((stream) => [
      `$JS.API.STREAM.MSG.DELETE.${stream}`,
      `$JS.API.DIRECT.GET.${stream}`,
      `$JS.API.DIRECT.GET.${stream}.>`,
    ]),
  ];
  return { pub: { allow: pathBUnique(allow), deny }, sub: { allow: [OWN_INBOX] } };
}

/** Compose Path B with the existing role only when the mint input explicitly opts in. */
export function roleTemplateWithPathB(role: NatsRole | "operator", input: RoleStreams, machines: readonly string[] = [], org?: string): RoleTemplate {
  // Supplying an org never bypasses validation, even for roles that receive no service grants.
  if (org !== undefined) serviceOrg(org);
  if (role === "operator") {
    const path = pathBOperator(PATH_B_MACHINE_TAG, machines);
    const base = roleTemplate("maintainer", input);
    return {
      pub: { allow: path.pub.allow, deny: pathBUnique([...base.pub.deny, ...path.pub.deny]) },
      sub: { allow: path.sub.allow },
    };
  }
  const base = roleTemplate(role, input);
  if (role === "agent") {
    const path = pathBRuntime(PATH_B_MACHINE_TAG, machines, org);
    return {
      pub: { allow: pathBUnique([...base.pub.allow, ...path.pub.allow]), deny: pathBUnique([...base.pub.deny, ...path.pub.deny]) },
      sub: { allow: path.sub.allow },
      ...(path.allow_responses ? { allow_responses: path.allow_responses } : {}),
    };
  }
  if (role === "break-glass") return base;
  // Principal and normal maintainer retain their exact #7504 allows; only the Path B
  // advisory reservation adds denies. Emergency custody/liveness issuance is unchanged.
  return {
    pub: { allow: base.pub.allow, deny: pathBUnique([...base.pub.deny, ...PATH_B_RESERVED_MUTATIONS, "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.>"]) },
    sub: base.sub,
  };
}

export function pathBRoleTemplate(role: PathBRole, machine = PATH_B_MACHINE_TAG, machines: readonly string[] = [], org?: string): PathBRoleTemplate {
  if (org !== undefined) serviceOrg(org);
  if (role === "agent") return pathBRuntime(pathBMachine(machine), machines, org);
  if (role === "operator") return pathBOperator(pathBMachine(machine), machines);
  throw new Error("invalid Path B role");
}

/**
 * Frozen d231bd15 runtime scope for rollout verification. Do not derive this by removing the current flow-control
 * grants: later runtime lanes may add session/event subjects to the current template, and the installed account's
 * old scope must remain accepted only when it matches this exact pre-change set.
 */
const legacyPathBRuntime = (roleMachine: string, machines: readonly string[] = []): PathBRoleTemplate => {
  if (PATH_B_OPERATOR_MACHINE_PATTERN.test(roleMachine)) fail("machine");
  const inbox = `FABRIC_IN_${roleMachine}`;
  const privateInboxes = roleMachine === PATH_B_MACHINE_TAG ? machines.map((machine) => `FABRIC_IN_${pathBMachine(machine)}`) : [inbox];
  const allow = [
    `fabric.in.*.*.*.*.${roleMachine}`,
    `$JS.API.STREAM.INFO.${inbox}`,
    ...["CREATE", "INFO", "MSG.NEXT", "DELETE"].map((verb) => `$JS.API.CONSUMER.${verb}.${inbox}.>`),
    `$JS.ACK.${inbox}.>`,
    `$KV.fabric_presence.${roleMachine}.>`,
    `$KV.fabric_leases.${roleMachine}.>`,
    "$JS.API.STREAM.INFO.KV_fabric_presence",
    "$JS.API.STREAM.INFO.KV_fabric_leases",
    "$JS.API.STREAM.MSG.GET.KV_fabric_presence",
    "$JS.API.STREAM.MSG.GET.KV_fabric_leases",
    ...["CREATE", "INFO", "DELETE"].flatMap((verb) => [
      `$JS.API.CONSUMER.${verb}.KV_fabric_presence.>`,
      `$JS.API.CONSUMER.${verb}.KV_fabric_leases.>`,
    ]),
    JS_INFO,
    USER_INFO,
  ];
  const deny = [
    "$JS.API.STREAM.CREATE.>",
    "$JS.API.STREAM.UPDATE.>",
    "$JS.API.STREAM.DELETE.>",
    "$JS.API.STREAM.PURGE.>",
    ...privateInboxes.map((stream) => `$JS.API.STREAM.MSG.GET.${stream}`),
    "$JS.API.STREAM.MSG.DELETE.>",
    "$JS.API.DIRECT.GET.>",
    "$JS.API.CONSUMER.*.FABRIC_ADV.>",
    "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.>",
    ...PATH_B_RESERVED_MUTATIONS,
  ];
  return { pub: { allow: pathBUnique(allow), deny }, sub: { allow: [OWN_INBOX] } };
};

/** Frozen d231bd15 agent scope, including its old role grants and no later runtime additions. */
export function legacyRoleTemplateWithPathB(role: "agent", input: RoleStreams, machines: readonly string[] = []): RoleTemplate {
  const base = roleTemplate(role, input);
  const path = legacyPathBRuntime(PATH_B_MACHINE_TAG, machines);
  return {
    pub: { allow: pathBUnique([...base.pub.allow, ...path.pub.allow]), deny: pathBUnique([...base.pub.deny, ...path.pub.deny]) },
    sub: { allow: path.sub.allow },
  };
}
