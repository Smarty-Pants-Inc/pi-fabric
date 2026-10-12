import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { fileURLToPath } from "node:url";

/**
 * smarty-dev#6207: every model request names its sender. Names only, never secrets;
 * an unknown value is omitted, never guessed. Header-only: no Fabric graph, no task text.
 */
export const SENDER_HEADER_NAMES = { role: "X-Smarty-Role", agent: "X-Smarty-Agent", spawner: "X-Smarty-Spawner" } as const;

export interface SenderHeaders { role: string | undefined; agent: string | undefined; spawner: string | undefined }

// The gateway's sender allowlist, exactly: ASCII letters, digits and . _ : / - (no '@'), 1-64
// characters. Anything else (CR/LF, spaces, '@', free text, longer values) is rejected, never repaired.
export const SENDER_VALUE_MAX_LENGTH = 64;
const SAFE_VALUE = /^[A-Za-z0-9._:/-]{1,64}$/;
export const senderHeaderValue = (value: string | undefined | null): string | undefined =>
  typeof value === "string" && SAFE_VALUE.test(value) ? value : undefined;

/** The actor name's route class, shared with the run record's derived routeClass. */
export const actorNameClass = (name: string): "review" | "security" | "status-groom" | "other" =>
  /(?:^|-)review-astra$/.test(name) ? "review"
    : /(?:^|-)security-astra$/.test(name) ? "security"
      : /(?:^|-)supervisor$/.test(name) ? "status-groom" : "other";

const set = (value: string | undefined): string | undefined => value?.trim() ? value.trim() : undefined;
const isActor = (env: NodeJS.ProcessEnv): boolean => Boolean(set(env.PI_FABRIC_ACTOR_ID));
const isFabricChild = (env: NodeJS.ProcessEnv): boolean => isActor(env) || Boolean(set(env.PI_FABRIC_PARENT_RUN));

/** A smarty-task lane: the launcher exports only TASK_OUT=<tasks>/<lane>/artifacts (no lane variable). */
export const smartyTaskLane = (env: NodeJS.ProcessEnv = process.env): string | undefined => {
  const parts = (env.TASK_OUT ?? "").split(/[\\/]+/).filter(Boolean);
  return parts.length >= 2 && parts.at(-1) === "artifacts" ? parts.at(-2) : undefined;
};

export const senderRole = (env: NodeJS.ProcessEnv = process.env): string | undefined => {
  if (isActor(env)) {
    const name = set(env.PI_FABRIC_ACTOR_NAME);
    return name ? `actor:${actorNameClass(name)}` : "actor";
  }
  if (env.PI_FABRIC_TASK_PROCESS_CHILD === "1") return "task";
  return senderHeaderValue(env.SMARTY_ROLE?.split("@")[0]);
};

/** This process's own name; also the SPAWNER its Fabric children receive. */
export const senderAgent = (env: NodeJS.ProcessEnv = process.env, sessionName?: string): string | undefined => {
  const candidates = isActor(env) ? [env.PI_FABRIC_ACTOR_NAME]
    : set(env.PI_FABRIC_PARENT_RUN) ? [env.PI_FABRIC_AGENT_NAME]
      // A Main: its smarty-task lane, else its mesh participant (Pi session) name, else SMARTY_LANE.
      : [smartyTaskLane(env), sessionName, env.SMARTY_LANE];
  for (const candidate of candidates) {
    // Raw, never trimmed: a value carrying CR/LF or spaces is rejected, not repaired.
    const value = senderHeaderValue(candidate);
    if (value) return value;
  }
  return undefined;
};

/**
 * Only a Fabric child has a spawner, and only a named one: a spawner's session id (or a
 * `session:` spawner id) never reaches a provider header. No name: the header is omitted.
 */
export const senderSpawner = (env: NodeJS.ProcessEnv = process.env): string | undefined =>
  isFabricChild(env) ? senderHeaderValue(env.PI_FABRIC_SPAWNER_NAME) : undefined;

export const resolveSenderHeaders = (env: NodeJS.ProcessEnv = process.env, sessionName?: string): Record<string, string> => {
  const values: SenderHeaders = { role: senderRole(env), agent: senderAgent(env, sessionName), spawner: senderSpawner(env) };
  const headers: Record<string, string> = {};
  for (const key of ["role", "agent", "spawner"] as const) {
    const value = values[key];
    if (value) headers[SENDER_HEADER_NAMES[key]] = value;
  }
  return headers;
};

const RESERVED_SENDER_HEADERS: ReadonlySet<string> = new Set(Object.values(SENDER_HEADER_NAMES).map(name => name.toLowerCase()));

/**
 * The one header write both hooks share (the Main's and every worker child's): Pi's header map is
 * mutated in place. HTTP header names are case-insensitive, so every reserved X-Smarty-* key
 * already present, in any letter case, is removed first: a value this request's resolver omits
 * never leaves a stale or caller-supplied attribution behind.
 */
export const applySenderHeaders = (
  headers: Record<string, string | null>, env: NodeJS.ProcessEnv = process.env, sessionName?: string,
): void => {
  for (const name of Object.keys(headers)) if (RESERVED_SENDER_HEADERS.has(name.toLowerCase())) delete headers[name];
  Object.assign(headers, resolveSenderHeaders(env, sessionName));
};

/** The hook's stable package-local path beside a Fabric entry module (src/*.ts or dist/*.js): the worker's -e. */
export const senderHeaderHookPath = (entryUrl: string): string => fileURLToPath(new URL(
  entryUrl.endsWith(".ts") ? "./guards/sender-headers.ts" : "./guards/sender-headers.js", entryUrl));

/** Loaded by the Main (Fabric's entry) and by every Pi worker child (explicit -e). */
export default function senderHeadersHook(pi: ExtensionAPI): void {
  pi.on("before_provider_headers", event => {
    // Pi ignores returns from this hook: mutate the supplied header map in place.
    // Resolved per request: a Main's session name can change during the session.
    applySenderHeaders(event.headers, process.env, pi.getSessionName?.());
  });
}
