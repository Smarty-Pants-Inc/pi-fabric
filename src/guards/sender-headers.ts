import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/**
 * smarty-dev#6207: every model request names its sender. Names only, never secrets;
 * an unknown value is omitted, never guessed. Header-only: no Fabric graph, no task text.
 */
export const SENDER_HEADER_NAMES = { role: "X-Smarty-Role", agent: "X-Smarty-Agent", spawner: "X-Smarty-Spawner" } as const;

export interface SenderHeaders { role: string | undefined; agent: string | undefined; spawner: string | undefined }

// Bounded ASCII, as model-route-hook: no CR/LF, spaces or free text can reach the wire.
const SAFE_VALUE = /^[A-Za-z0-9._:@/-]{1,128}$/;
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

/** Only a Fabric child has a spawner: its bound name, else (no name) the spawner's session id. */
export const senderSpawner = (env: NodeJS.ProcessEnv = process.env): string | undefined => {
  if (!isFabricChild(env)) return undefined;
  const id = env.PI_FABRIC_SPAWNER_ID;
  return senderHeaderValue(env.PI_FABRIC_SPAWNER_NAME) ?? senderHeaderValue(env.PI_FABRIC_SPAWNER_SESSION_ID) ??
    (id?.startsWith("session:") ? senderHeaderValue(id) : undefined);
};

export const resolveSenderHeaders = (env: NodeJS.ProcessEnv = process.env, sessionName?: string): Record<string, string> => {
  const values: SenderHeaders = { role: senderRole(env), agent: senderAgent(env, sessionName), spawner: senderSpawner(env) };
  const headers: Record<string, string> = {};
  for (const key of ["role", "agent", "spawner"] as const) {
    const value = values[key];
    if (value) headers[SENDER_HEADER_NAMES[key]] = value;
  }
  return headers;
};

/** Loaded by the Main (Fabric's entry) and by every Pi worker child (explicit -e). */
export default function senderHeadersHook(pi: ExtensionAPI): void {
  pi.on("before_provider_headers", event => {
    // Pi ignores returns from this hook: mutate the supplied header map in place.
    // Resolved per request: a Main's session name can change during the session.
    Object.assign(event.headers, resolveSenderHeaders(process.env, pi.getSessionName?.()));
  });
}
