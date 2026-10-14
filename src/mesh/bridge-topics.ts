/**
 * Cheap pure work-topic helpers shared by the mesh bridge and MeshProvider. No imports: a module
 * that needs only the factory route predicate must not statically load the whole bridge.
 */

/** A mesh topic is at most this long (the event log's topic pattern); the parsers read no further. */
const MAX_TOPIC_LENGTH = 128;
const WORK_TOPIC_PREFIX = "fleet.work.";
const WORK_SEGMENT = /^[A-Za-z0-9_-]+$/;
const WORK_REPO = /^[A-Za-z0-9._-]+$/;
const WORK_NUMBER = /^[1-9][0-9]{0,15}$/;

/**
 * A well-formed work topic, `fleet.work.<name>` (pi-fabric#876 security pass, (a)): dot-separated,
 * non-empty ASCII segments of [A-Za-z0-9_-], at most 128 characters in all. Every work topic the
 * bridge allows, to any recipient, therefore also passes the mesh event log's and mesh_answers'
 * topic pattern. The one empty segment allowed is a dot-led repo name such as `.github`
 * (`fleet.work..github.7`), and only before two more segments. No `fleet.work.`, `fleet.work..1`,
 * `fleet.work.a..b.1`, trailing dot, NUL, space, lookalike or other non-ASCII character. Linear: a
 * length cap, one split and a character class per segment, never a backtracking pattern.
 */
export const isWorkTopic = (topic: unknown): topic is string => {
  if (typeof topic !== "string" || topic.length > MAX_TOPIC_LENGTH || !topic.startsWith(WORK_TOPIC_PREFIX)) return false;
  const segments = topic.slice(WORK_TOPIC_PREFIX.length).split(".");
  for (let index = segments[0] === "" && segments.length > 2 ? 1 : 0; index < segments.length; index++) {
    if (!WORK_SEGMENT.test(segments[index]!)) return false;
  }
  return true;
};

/**
 * The hub factory's role address (smarty-dev#9352). The factory on the hub reads work events and
 * owner wakes by topic, addressed to this literal name rather than to a participant id, so no hub
 * record answers to it. Only from the remote to the hub, only this exact name, and only on a
 * parsed `fleet.work.<repo>.<n>` topic (parseWorkTopic) or the allow-listed `ops.owner` kinds is it
 * an address across; every other check (allow-list, sender identity, reserved ids, live sender at
 * commit, frame budget) still holds, and the link's factory rate bound (factoryRate) applies.
 */
export const HUB_FACTORY_RECIPIENT = "factory";

/**
 * A work topic as work-host Mains name it, `fleet.work.<repo>.<n>` (pi-fabric#876 review, P1-1): a
 * non-empty repo segment of [A-Za-z0-9._-] (never `.` or `..`, which name no repo) and a decimal
 * issue or PR number without a sign or leading zero. The number is the segment after the last
 * dot, so a dotted repo name still parses. It is a well-formed work topic first (isWorkTopic).
 * Linear in the topic: an untrusted remote's string meets no backtracking pattern. Anything else
 * (`fleet.work.`, `fleet.work.<repo>`, `fleet.work.inbox-receipts`, `fleet.work..1`,
 * `fleet.work.<repo>.1.extra`, `fleet.work.a:b.1`, a lookalike or a NUL) is undefined.
 */
export const parseWorkTopic = (topic: unknown): { repo: string; number: number } | undefined => {
  if (!isWorkTopic(topic)) return undefined;
  const rest = topic.slice(WORK_TOPIC_PREFIX.length);
  const dot = rest.lastIndexOf(".");
  if (dot <= 0) return undefined;
  const repo = rest.slice(0, dot);
  const digits = rest.slice(dot + 1);
  if (!WORK_REPO.test(repo) || repo === "." || repo === ".." || !WORK_NUMBER.test(digits)) return undefined;
  const number = Number(digits);
  return Number.isSafeInteger(number) ? { repo, number } : undefined;
};

/**
 * Whether an event is explicitly addressed to the hub factory: exactly `to: factory` on a parsed
 * work issue topic (parseWorkTopic) or an `ops.owner` owner wake (`pr.wake`, `inbox.age.alarm`).
 * Pure and host-independent: the provider's recipient exception on any host, and the literal
 * branch of the bridge's isHubFactoryEvent.
 */
export const isAddressedHubFactoryEvent = (event: { topic: unknown; kind?: unknown; to?: unknown }): boolean =>
  event.to === HUB_FACTORY_RECIPIENT &&
  (parseWorkTopic(event.topic) !== undefined ||
    (event.topic === "ops.owner" && (event.kind === "pr.wake" || event.kind === "inbox.age.alarm")));
