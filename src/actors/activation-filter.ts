/**
 * Declarative skip-only activation filter for actors (smarty-dev#1579 item 4).
 *
 * A filter is a list of SKIP rules. ActorManager checks a queued event against them just before
 * it would run the actor's model; a match drops the activation with no model call. A rule only
 * ever skips: it never acts, replies or changes the event. When a rule is unsure (a field it reads
 * is missing), it does not match, so the event is delivered.
 */

export type ActorActivationFilterScalar = string | number | boolean | null;

/** One test on a field of the queued item's payload. Exactly one of equals, in, exists. */
export interface ActorActivationFilterPredicate {
  /**
   * A dotted path into the payload (a mesh event: `data.payload.action`). Arrays on the way fan
   * out, so `data.payload.issue.labels.name` reads every label's name. A list gives alternatives:
   * the first path with a value is used ("whichever the event carries").
   */
  path: string | string[];
  /** Some value at the path is exactly this scalar. */
  equals?: ActorActivationFilterScalar;
  /** Some value at the path is one of these scalars. */
  in?: ActorActivationFilterScalar[];
  /** The path has a value. Only `true`: a skip on a missing field would skip when unsure. */
  exists?: true;
}

export interface ActorActivationSkipRule {
  /** Recorded in the skip's log record as `filtered: <id>`. */
  id: string;
  /** Item sources, such as `mesh:ops.owner` or `host:tool_error`; a trailing `*` matches a prefix. */
  source?: string[];
  /** Mesh event topics, such as `github.*`; a host event has no topic and never matches. */
  topic?: string[];
  /** Mesh event kinds, such as `actions.minutes`. */
  kind?: string[];
  /** Every predicate must match. */
  where?: ActorActivationFilterPredicate[];
  /** When every one of these predicates matches too, the event is delivered after all. */
  unless?: ActorActivationFilterPredicate[];
}

/**
 * The ready-made rule sets. Each has zero false skips in 24 h of supervisor runs
 * (smarty-dev#1579 comment 5861556336).
 */
export const FABRIC_ACTOR_ACTIVATION_PRESETS: Readonly<Record<"hold" | "never-message-events", readonly ActorActivationSkipRule[]>> = {
  /**
   * R1: a GitHub event on an issue or PR labelled `hold`. The event that removes `hold` is always
   * delivered, and an event without labels is delivered.
   */
  hold: [
    {
      id: "hold",
      topic: ["github.*"],
      where: [{ path: ["data.payload.issue.labels.name", "data.payload.pull_request.labels.name"], equals: "hold" }],
      unless: [
        { path: "data.payload.action", equals: "unlabeled" },
        { path: "data.payload.label.name", equals: "hold" },
      ],
    },
  ],
  /** R7: event types that led to no supervisor message in 24 h. */
  "never-message-events": [
    ...["issues:field_added", "issues:typed", "issue_comment:deleted"].map((type): ActorActivationSkipRule => {
      const [event, action] = type.split(":") as [string, string];
      return {
        id: `never-message-events/${event}.${action}`,
        topic: ["github.*"],
        where: [{ path: "data.event", equals: event }, { path: "data.payload.action", equals: action }],
      };
    }),
    { id: "never-message-events/host:tool_error", source: ["host:tool_error"] },
    { id: "never-message-events/ops.owner:actions.minutes", topic: ["ops.owner"], kind: ["actions.minutes"] },
  ],
};

export type ActorActivationFilterPreset = keyof typeof FABRIC_ACTOR_ACTIVATION_PRESETS;

/** Preset names and custom rules; presets expand to their rules. */
export type FabricActorActivationFilter = (ActorActivationFilterPreset | ActorActivationSkipRule)[];

const RULE_ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,79}$/;
const PATH = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$/;
const PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\*?$|^\*$/;
const MAX_RULES = 32;
const MAX_PREDICATES = 16;
const MAX_LIST = 64;
const RULE_KEYS = new Set(["id", "source", "topic", "kind", "where", "unless"]);
const PREDICATE_KEYS = new Set(["path", "equals", "in", "exists"]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isScalar = (value: unknown): value is ActorActivationFilterScalar =>
  value === null || typeof value === "boolean" || (typeof value === "number" && Number.isFinite(value)) ||
  (typeof value === "string" && value.length <= 256);
const isPreset = (value: unknown): value is ActorActivationFilterPreset =>
  typeof value === "string" && Object.hasOwn(FABRIC_ACTOR_ACTIVATION_PRESETS, value);

function patterns(value: unknown, where: string): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_LIST ||
    !value.every((entry) => typeof entry === "string" && PATTERN.test(entry))) {
    throw new Error(`Invalid activationFilter ${where}: use a non-empty list of names, a trailing * matches a prefix`);
  }
  return [...value] as string[];
}

function predicates(value: unknown, where: string): ActorActivationFilterPredicate[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_PREDICATES) {
    throw new Error(`Invalid activationFilter ${where}: use a list of 1 to ${MAX_PREDICATES} predicates`);
  }
  return value.map((entry, index) => {
    const at = `${where}[${index}]`;
    if (!isRecord(entry)) throw new Error(`Invalid activationFilter ${at}: not an object`);
    for (const key of Object.keys(entry)) {
      if (!PREDICATE_KEYS.has(key)) throw new Error(`Invalid activationFilter ${at}: unknown field ${key}`);
    }
    const paths = typeof entry.path === "string" ? [entry.path] : entry.path;
    if (!Array.isArray(paths) || paths.length === 0 || paths.length > 8 ||
      !paths.every((p) => typeof p === "string" && p.length <= 200 && PATH.test(p))) {
      throw new Error(`Invalid activationFilter ${at}.path: use a dotted path such as data.payload.action, or a list of them`);
    }
    const operators = ["equals", "in", "exists"].filter((key) => Object.hasOwn(entry, key));
    if (operators.length !== 1) throw new Error(`Invalid activationFilter ${at}: use exactly one of equals, in, exists`);
    const path = typeof entry.path === "string" ? entry.path : [...paths] as string[];
    if (operators[0] === "equals") {
      if (!isScalar(entry.equals)) throw new Error(`Invalid activationFilter ${at}.equals: use a string, number, boolean or null`);
      return { path, equals: entry.equals };
    }
    if (operators[0] === "in") {
      if (!Array.isArray(entry.in) || entry.in.length === 0 || entry.in.length > MAX_LIST || !entry.in.every(isScalar)) {
        throw new Error(`Invalid activationFilter ${at}.in: use a non-empty list of strings, numbers, booleans or null`);
      }
      return { path, in: [...entry.in] };
    }
    // ponytail: no exists:false. A skip on a missing field would skip exactly when unsure.
    if (entry.exists !== true) throw new Error(`Invalid activationFilter ${at}.exists: only true is allowed (unsure means deliver)`);
    return { path, exists: true as const };
  });
}

function normalizeRule(value: unknown, index: number): ActorActivationSkipRule {
  const at = `rule ${index}`;
  if (!isRecord(value)) throw new Error(`Invalid activationFilter ${at}: use a preset name (${Object.keys(FABRIC_ACTOR_ACTIVATION_PRESETS).join(", ")}) or a rule object`);
  for (const key of Object.keys(value)) {
    if (!RULE_KEYS.has(key)) throw new Error(`Invalid activationFilter ${at}: unknown field ${key}`);
  }
  if (typeof value.id !== "string" || !RULE_ID.test(value.id)) throw new Error(`Invalid activationFilter ${at}.id: ${String(value.id)}`);
  const source = patterns(value.source, `${value.id}.source`);
  const topic = patterns(value.topic, `${value.id}.topic`);
  const kind = patterns(value.kind, `${value.id}.kind`);
  const where = predicates(value.where, `${value.id}.where`);
  const unless = predicates(value.unless, `${value.id}.unless`);
  // A rule that names nothing would skip every event.
  if (!source?.some((p) => p !== "*") && !topic?.some((p) => p !== "*") && !kind?.some((p) => p !== "*") && !where) {
    throw new Error(`Invalid activationFilter rule ${value.id}: name a source, topic, kind or where predicate`);
  }
  return {
    id: value.id,
    ...(source ? { source } : {}),
    ...(topic ? { topic } : {}),
    ...(kind ? { kind } : {}),
    ...(where ? { where } : {}),
    ...(unless ? { unless } : {}),
  };
}

/** Validate an activation filter and return a clean copy; throws on any invalid entry. */
export function normalizeActorActivationFilter(value: unknown): FabricActorActivationFilter {
  if (!Array.isArray(value) || value.length > MAX_RULES) {
    throw new Error(`Invalid activationFilter: use a list of up to ${MAX_RULES} preset names or rules`);
  }
  const filter = value.map((entry, index) => {
    if (typeof entry === "string") {
      if (!isPreset(entry)) {
        throw new Error(`Unknown activationFilter preset: ${entry} (use ${Object.keys(FABRIC_ACTOR_ACTIVATION_PRESETS).join(", ")})`);
      }
      return entry;
    }
    return normalizeRule(entry, index);
  });
  const ids = new Set<string>();
  for (const rule of expandActorActivationFilter(filter)) {
    if (ids.has(rule.id)) throw new Error(`Duplicate activationFilter rule id: ${rule.id}`);
    ids.add(rule.id);
  }
  return filter;
}

export function expandActorActivationFilter(filter: FabricActorActivationFilter): readonly ActorActivationSkipRule[] {
  return filter.flatMap((entry) => isPreset(entry) ? FABRIC_ACTOR_ACTIVATION_PRESETS[entry] : [entry]);
}

const matchesPattern = (value: unknown, list: readonly string[] | undefined): boolean =>
  list === undefined ||
  (typeof value === "string" && list.some((p) => p.endsWith("*") ? value.startsWith(p.slice(0, -1)) : value === p));

/** Every value at a dotted path; arrays fan out. Missing gives none. */
function valuesAt(root: unknown, path: string): unknown[] {
  let values: unknown[] = [root];
  for (const segment of path.split(".")) {
    values = values.flatMap((value) => {
      const next = isRecord(value) && Object.hasOwn(value, segment) ? value[segment] : undefined;
      return next === undefined ? [] : Array.isArray(next) ? next : [next];
    });
    if (values.length === 0) return values;
  }
  return values;
}

function predicateMatches(predicate: ActorActivationFilterPredicate, payload: unknown): boolean {
  const paths = typeof predicate.path === "string" ? [predicate.path] : predicate.path;
  let values: unknown[] = [];
  for (const path of paths) {
    values = valuesAt(payload, path);
    if (values.length > 0) break;
  }
  if (predicate.exists) return values.length > 0;
  if (predicate.in) return values.some((value) => predicate.in!.includes(value as ActorActivationFilterScalar));
  return values.some((value) => value === predicate.equals);
}

/**
 * The id of the first rule that skips this queued item, or undefined to deliver it. For a mesh
 * item the payload is the mesh event (topic, kind, data); for a host item, the host event data.
 */
export function activationFilterSkip(
  filter: FabricActorActivationFilter | undefined,
  source: string,
  payload: unknown,
): string | undefined {
  if (!filter || filter.length === 0) return undefined;
  const mesh = source.startsWith("mesh:") && isRecord(payload);
  const topic = mesh ? payload.topic : undefined;
  const kind = mesh ? payload.kind : undefined;
  for (const rule of expandActorActivationFilter(filter)) {
    if (!matchesPattern(source, rule.source)) continue;
    if (rule.topic && !matchesPattern(topic, rule.topic)) continue;
    if (rule.kind && !matchesPattern(kind, rule.kind)) continue;
    if (rule.where && !rule.where.every((predicate) => predicateMatches(predicate, payload))) continue;
    if (rule.unless?.every((predicate) => predicateMatches(predicate, payload))) continue;
    return rule.id;
  }
  return undefined;
}
