/**
 * Host-issued principal and narrowing scope (`pi-fabric/scope`). Trusted host
 * adapter code, not a verified kernel: see docs/providers.md "Principal and scope".
 * Lightweight: no extension, registry, UI or provider runtime.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import type { FabricMessageSender, FabricScope, FabricScopeAction, FabricScopeGrant } from "./protocol.js";

export type { FabricMessageSender, FabricScope, FabricScopeAction, FabricScopeGrant } from "./protocol.js";

export const FABRIC_SCOPE_ENV = "PI_FABRIC_SCOPE";
export const FABRIC_SCOPE_FILE_ENV = "PI_FABRIC_SCOPE_FILE";
export const MAX_FABRIC_SCOPE_GRANTS = 64;
export const MAX_FABRIC_SCOPE_RESOURCE_CHARS = 512;
const MAX_PRINCIPAL_CHARS = 128;
const MAX_SCOPE_JSON_BYTES = 64 * 1024;
const ACTIONS: readonly FabricScopeAction[] = ["read", "write", "execute"];
const NAMESPACE = /^[a-z][a-z0-9._-]{0,63}$/;
const SEGMENT = /^[^\s/*\u0000-\u001f\u007f]+$/u;
const DIGEST = /^[0-9a-f]{64}$/;

/** Process-wide issuance record shared with the extension entry and the registry. */
interface ScopeHolder {
  sealed?: boolean;
  scope?: FabricScope;
  error?: string;
}
const HOLDER_KEY = Symbol.for("pi-fabric:scope:v1");
const holder = (): ScopeHolder =>
  ((globalThis as Record<symbol, ScopeHolder | undefined>)[HOLDER_KEY] ??= {});

interface ParsedResource {
  ns: string;
  /** `ns:*`: every resource in the namespace. */
  all: boolean;
  absolute: boolean;
  segments: string[];
  wildcard: "none" | "one" | "any";
}

const parseResource = (resource: unknown): ParsedResource => {
  if (typeof resource !== "string" || resource.length === 0 || resource.length > MAX_FABRIC_SCOPE_RESOURCE_CHARS) {
    throw new TypeError(`Scope resource must be a string of 1-${MAX_FABRIC_SCOPE_RESOURCE_CHARS} characters`);
  }
  const colon = resource.indexOf(":");
  const ns = resource.slice(0, Math.max(0, colon));
  const path = resource.slice(colon + 1);
  const invalid = (): never => {
    throw new TypeError(`Invalid scope resource ${JSON.stringify(resource)}: expected <ns>:<path>, <ns>:<path>/*, <ns>:<path>/** or <ns>:*`);
  };
  if (colon < 1 || !NAMESPACE.test(ns) || !path) return invalid();
  if (path === "*") return { ns, all: true, absolute: false, segments: [], wildcard: "none" };
  const absolute = path.startsWith("/");
  const segments = (absolute ? path.slice(1) : path).split("/");
  const last = segments.at(-1);
  const wildcard = last === "**" ? "any" : last === "*" ? "one" : "none";
  if (wildcard !== "none") segments.pop();
  if (
    (!absolute && segments.length === 0) ||
    segments.some((segment) => !SEGMENT.test(segment) || segment === "." || segment === "..")
  ) {
    return invalid();
  }
  return { ns, all: false, absolute, segments, wildcard };
};

/** True when every concrete resource matched by `inner` is matched by `outer`. */
const covers = (outer: ParsedResource, inner: ParsedResource): boolean => {
  if (outer.ns !== inner.ns) return false;
  if (outer.all) return true;
  if (inner.all || outer.absolute !== inner.absolute) return false;
  const prefix = outer.segments;
  if (inner.segments.length < prefix.length || prefix.some((segment, index) => inner.segments[index] !== segment)) {
    return false;
  }
  const extra = inner.segments.length - prefix.length;
  if (outer.wildcard === "none") return inner.wildcard === "none" && extra === 0;
  // `/*` covers exactly one more segment; `/**` covers one or more.
  if (outer.wildcard === "one") return inner.wildcard === "none" ? extra === 1 : inner.wildcard === "one" && extra === 0;
  return inner.wildcard === "none" ? extra >= 1 : true;
};

const checkKeys = (value: Record<string, unknown>, allowed: readonly string[], label: string): void => {
  const unknown = Object.keys(value).filter((key) => !allowed.includes(key));
  if (unknown.length > 0) throw new TypeError(`${label} has unsupported fields: ${unknown.join(", ")}`);
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Validate grants and return them merged per resource, sorted, with ordered actions. */
const normalizeGrants = (value: unknown): FabricScopeGrant[] => {
  if (!Array.isArray(value) || value.length > MAX_FABRIC_SCOPE_GRANTS) {
    throw new TypeError(`Scope grants must be an array of at most ${MAX_FABRIC_SCOPE_GRANTS} grants`);
  }
  const merged = new Map<string, Set<FabricScopeAction>>();
  for (const grant of value) {
    if (!isObject(grant)) throw new TypeError("Each scope grant must be an object { resource, actions }");
    checkKeys(grant, ["resource", "actions"], "Scope grant");
    parseResource(grant.resource);
    const actions = grant.actions;
    if (!Array.isArray(actions) || actions.length === 0 || actions.length > ACTIONS.length * 2 ||
        actions.some((action) => !ACTIONS.includes(action as FabricScopeAction))) {
      throw new TypeError(`Scope grant ${String(grant.resource)} needs actions from read, write, execute`);
    }
    const set = merged.get(grant.resource as string) ?? new Set<FabricScopeAction>();
    for (const action of actions) set.add(action as FabricScopeAction);
    merged.set(grant.resource as string, set);
  }
  return [...merged.keys()].sort().map((resource) => ({
    resource,
    actions: ACTIONS.filter((action) => merged.get(resource)!.has(action)),
  }));
};

const canonicalDigest = (principalId: string, grants: readonly FabricScopeGrant[], parentDigest?: string): string =>
  createHash("sha256").update(JSON.stringify({
    grants: grants.map((grant) => ({ actions: grant.actions, resource: grant.resource })),
    ...(parentDigest ? { parentDigest } : {}),
    principal: { id: principalId, issuer: "host" },
  })).digest("hex");

const verified = new WeakSet<FabricScope>();

const freeze = (principalId: string, grants: FabricScopeGrant[], parentDigest?: string): FabricScope => {
  const scope: FabricScope = Object.freeze({
    version: 1 as const,
    principal: Object.freeze({ id: principalId, issuer: "host" as const }),
    grants: Object.freeze(grants.map((grant) => Object.freeze({ ...grant, actions: Object.freeze(grant.actions) }))),
    digest: canonicalDigest(principalId, grants, parentDigest),
    ...(parentDigest ? { parentDigest } : {}),
  }) as FabricScope;
  verified.add(scope);
  return scope;
};

/**
 * Validate a scope (fail closed) and return its canonical frozen form. `digest`
 * is computed when omitted and must match when present.
 */
export const normalizeScope = (input: unknown): FabricScope => {
  if (verified.has(input as FabricScope)) return input as FabricScope;
  if (!isObject(input)) throw new TypeError("A Fabric scope must be an object { principal, grants }");
  checkKeys(input, ["version", "principal", "grants", "digest", "parentDigest"], "Fabric scope");
  if (input.version !== undefined && input.version !== 1) throw new TypeError("Fabric scope version must be 1");
  const principal = input.principal;
  if (!isObject(principal)) throw new TypeError("Fabric scope principal must be an object { id, issuer: \"host\" }");
  checkKeys(principal, ["id", "issuer"], "Fabric scope principal");
  if (typeof principal.id !== "string" || !principal.id || principal.id.length > MAX_PRINCIPAL_CHARS ||
      /[\u0000-\u001f\u007f]/u.test(principal.id)) {
    throw new TypeError(`Fabric scope principal id must be 1-${MAX_PRINCIPAL_CHARS} printable characters`);
  }
  if (principal.issuer !== undefined && principal.issuer !== "host") throw new TypeError('Fabric scope principal issuer must be "host"');
  const parentDigest = input.parentDigest;
  if (parentDigest !== undefined && (typeof parentDigest !== "string" || !DIGEST.test(parentDigest))) {
    throw new TypeError("Fabric scope parentDigest must be a lowercase sha256 hex digest");
  }
  const scope = freeze(principal.id, normalizeGrants(input.grants), parentDigest as string | undefined);
  if (input.digest !== undefined && input.digest !== scope.digest) {
    throw new TypeError("Fabric scope digest does not match its canonical content");
  }
  return scope;
};

/** True when a grant covers `resource` (a concrete resource or a pattern) for `action`. */
export const scopeAllows = (scope: FabricScope, resource: string, action: FabricScopeAction): boolean => {
  if (!ACTIONS.includes(action)) throw new TypeError(`Unknown scope action: ${String(action)}`);
  const target = parseResource(resource);
  return normalizeScope(scope).grants.some((grant) =>
    grant.actions.includes(action) && covers(parseResource(grant.resource), target));
};

/**
 * Narrow `parent`: every requested grant must be covered by one parent grant
 * (resource subset and action subset). The principal is always the parent's.
 */
export const deriveScope = (parent: FabricScope, grants: readonly FabricScopeGrant[]): FabricScope => {
  const base = normalizeScope(parent);
  const requested = normalizeGrants(grants);
  for (const grant of requested) {
    if (!grantCovered(base, grant)) {
      throw new Error(`Scope grant ${grant.resource} [${grant.actions.join(", ")}] is not covered by the parent scope`);
    }
  }
  return freeze(base.principal.id, requested, base.digest);
};

const grantCovered = (outer: FabricScope, grant: FabricScopeGrant): boolean => {
  const target = parseResource(grant.resource);
  return outer.grants.some((candidate) =>
    grant.actions.every((action) => candidate.actions.includes(action)) &&
    covers(parseResource(candidate.resource), target));
};

/** True when `outer` has the same principal and covers every grant of `inner`. */
export const scopeCovers = (outer: FabricScope, inner: FabricScope): boolean => {
  const a = normalizeScope(outer);
  const b = normalizeScope(inner);
  return a.principal.id === b.principal.id && b.grants.every((grant) => grantCovered(a, grant));
};

const MAX_SENDER_GRANT_BYTES = 4 * 1024;

/** @internal The host stamp for a message sent with `scope` (absent: unscoped host authority). */
export const senderStamp = (scope: FabricScope | undefined): FabricMessageSender => {
  if (!scope) return { authority: "host" };
  const grants = scope.grants.map((grant) => ({ resource: grant.resource, actions: [...grant.actions] }));
  // Oversized grants are left out; such a stamp is trusted only on an exact digest match.
  const fits = Buffer.byteLength(JSON.stringify(grants)) <= MAX_SENDER_GRANT_BYTES;
  return {
    authority: "scope",
    principalId: scope.principal.id,
    digest: scope.digest,
    ...(fits ? { grants } : {}),
    ...(fits && scope.parentDigest ? { parentDigest: scope.parentDigest } : {}),
  };
};

/** @internal This process's stamp; failed issuance yields a stamp no actor trusts. */
export const processSender = (): FabricMessageSender => {
  try {
    return senderStamp(sessionScope());
  } catch {
    return { authority: "scope", principalId: "", digest: "" };
  }
};

/**
 * @internal Whether an actor bound to `actorScope` may treat a message from
 * `sender` as coming from its own authority. Unscoped senders are trusted; a
 * scoped sender must cover the actor's scope; an unstamped (older build)
 * message is trusted only by an unscoped actor. Malformed stamps never are.
 */
export const senderTrusted = (actorScope: FabricScope | undefined, sender: unknown): boolean => {
  if (sender === undefined) return actorScope === undefined;
  if (!isObject(sender)) return false;
  if (sender.authority === "host") return true;
  if (sender.authority !== "scope" || !actorScope) return false;
  try {
    const actor = normalizeScope(actorScope);
    if (sender.principalId !== actor.principal.id) return false;
    if (sender.grants === undefined) return sender.digest === actor.digest;
    return scopeCovers(normalizeScope({
      principal: { id: sender.principalId },
      grants: sender.grants,
      digest: sender.digest,
      ...(sender.parentDigest !== undefined ? { parentDigest: sender.parentDigest } : {}),
    }), actor);
  } catch {
    return false;
  }
};

/**
 * Issue the root session's scope in-process. Call once, before Pi emits
 * session_start; later or repeated calls throw. Invalid input throws and also
 * leaves the extension refusing provider calls (fail closed).
 */
export const issueRootScope = (scope: Omit<FabricScope, "version" | "digest" | "principal"> & {
  version?: 1;
  digest?: string;
  principal: { id: string; issuer?: "host" };
}): FabricScope => {
  const state = holder();
  if (state.sealed) throw new Error("issueRootScope must be called before session_start");
  if (state.scope || state.error) throw new Error("The root Fabric scope was already issued");
  try {
    state.scope = normalizeScope(scope);
    return state.scope;
  } catch (error) {
    state.error = error instanceof Error ? error.message : String(error);
    throw error;
  }
};

const readEnvScope = (json: string | undefined, file: string | undefined): FabricScope | undefined => {
  if (json && file) throw new Error(`Set only one of ${FABRIC_SCOPE_ENV} or ${FABRIC_SCOPE_FILE_ENV}`);
  const source = json || (file ? readFileSync(file, "utf8") : undefined);
  if (source === undefined) return undefined;
  if (Buffer.byteLength(source) > MAX_SCOPE_JSON_BYTES) throw new Error("Fabric scope JSON exceeds 64 KiB");
  let parsed: unknown;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new Error(`${json ? FABRIC_SCOPE_ENV : FABRIC_SCOPE_FILE_ENV} is not valid JSON`);
  }
  return normalizeScope(parsed);
};

/**
 * @internal Extension entry: resolve the environment issuance once and seal
 * further issueRootScope calls. Any failure is retained so the registry refuses
 * provider calls instead of running unscoped.
 */
export const sealRootScope = (env: { json?: string | undefined; file?: string | undefined }): void => {
  const state = holder();
  if (state.sealed) return;
  state.sealed = true;
  if (state.error) return;
  try {
    const fromEnv = readEnvScope(env.json, env.file);
    if (fromEnv && state.scope) throw new Error("A Fabric scope was issued by both the host API and the environment");
    if (fromEnv) state.scope = fromEnv;
  } catch (error) {
    delete state.scope;
    state.error = error instanceof Error ? error.message : String(error);
  }
};

/** @internal The current session scope; throws when issuance failed. */
export const sessionScope = (): FabricScope | undefined => {
  const state = holder();
  if (state.error) throw new Error(`Fabric scope issuance failed; provider calls are refused: ${state.error}`);
  return state.scope;
};

/**
 * @internal Scope for a child launch. Without a request the child inherits the
 * session scope unchanged; a request `{ grants }` narrows it and is refused in
 * an unscoped session (there is no principal to narrow from).
 */
export const childScope = (request: unknown): FabricScope | undefined => narrowScope(sessionScope(), request);

/**
 * @internal Scope for a launch whose parent scope was forwarded by the
 * requesting host (durable spawns, actor turns). The forwarded scope must be
 * canonical (digest checked) and, in a scoped process, covered by its scope;
 * a host without its own scope relies on the single-user boundary.
 */
export const launchScope = (request: unknown, inherited: unknown): FabricScope | undefined => {
  if (inherited === undefined) return childScope(request);
  let parent: FabricScope;
  try {
    parent = normalizeScope(inherited);
  } catch (error) {
    throw new Error(`Invalid forwarded Fabric scope: ${error instanceof Error ? error.message : String(error)}`);
  }
  const session = sessionScope();
  if (session && !scopeCovers(session, parent)) {
    throw new Error("A forwarded Fabric scope must be covered by this session's scope");
  }
  return narrowScope(parent, request);
};

const narrowScope = (parent: FabricScope | undefined, request: unknown): FabricScope | undefined => {
  if (request === undefined) return parent;
  if (!isObject(request)) throw new TypeError("scope must be an object { grants }");
  checkKeys(request, ["grants"], "scope (only grants can be narrowed; a program never sets a principal)");
  if (!parent) throw new Error("scope narrowing requires a scoped parent session; this session has no host-issued principal");
  return deriveScope(parent, request.grants as FabricScopeGrant[]);
};
