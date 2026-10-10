import { existsSync, readFileSync, realpathSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";
import path from "node:path";

// Shared host/agent input metadata lives beside existing public policy errors so
// eager configuration and lazy launch validation do not add a startup chunk.
export const MAX_AGENT_REQUIRED_INPUTS = 64;
export const MAX_AGENT_REQUIRED_INPUT_BYTES = 4096;

/** Invalid declarations and missing local inputs are known-unlaunched refusals. */
export class AgentInputError extends Error {
  readonly code = "FABRIC_AGENT_INPUT_ERROR";
  readonly launchOutcome = "unlaunched";

  constructor(readonly field: string, message: string) {
    super(message);
    this.name = "AgentInputError";
  }
}

// ponytail: a fixed list, not a placement decider (smarty-dev#6779). A prompt or
// cwd naming the Main's private corpus or per-user runtime dir only adds the
// "corpus" need; placement (and the #2890 router) still makes the one decision.
const CORPUS_MARKERS = ["org-context/tree", "org-search", "/run/user/"];
export const withCorpusNeed = (needs: string[] | undefined, task: string, cwd = ""): string[] | undefined =>
  needs?.includes("corpus") || !CORPUS_MARKERS.some(marker => task.includes(marker) || cwd.includes(marker))
    ? needs : [...needs ?? [], "corpus"];

/** Needs and launcher guarantees share the same canonical token vocabulary. */
export const normalizeAgentCapabilityTokens = (value: unknown, field = "needs"): string[] | undefined => {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) throw new AgentInputError(field, `Invalid agent ${field}: expected capability strings`);
  const tokens: string[] = [];
  for (const [index, entry] of value.entries()) {
    const parts = typeof entry === "string"
      ? entry.normalize("NFKC").toLowerCase().split(/[\p{White_Space},]+/u).filter(Boolean)
      : [];
    if (!parts.length) throw new AgentInputError(field, `Invalid agent ${field}[${index}]: expected nonempty capability tokens`);
    for (const token of parts) {
      if (!/^[a-z0-9._-]+$/u.test(token)) {
        throw new AgentInputError(field, `Invalid agent ${field}[${index}]: token ${JSON.stringify(token)} must contain only ASCII letters, digits, dot, underscore or hyphen`);
      }
      tokens.push(token);
    }
  }
  return [...new Set(tokens)];
};

/** A target-side preflight proved that this literal input was absent before launch. */
export class RequiredInputMissingError extends AgentInputError {
  readonly path: string;
  readonly index: number | undefined;

  constructor(path: string, index?: number) {
    const slot = index === undefined ? "requires" : `requires[${index}]`;
    super("requires", `Missing required agent input ${slot}: ${path}; path must exist on the selected host; no worker started`);
    this.name = "RequiredInputMissingError";
    this.path = path;
    this.index = index;
  }
}

/** Trusted host policy for every Fabric participant model selection. */
export interface FabricModelPolicy {
  deniedModels?: readonly string[];
  deniedModelReplacement?: string;
}

/** Refusal, not a retryable availability miss or an automatic model-family switch. */
export class FabricModelDeniedError extends Error {
  readonly code = "FABRIC_MODEL_DENIED";
  constructor(readonly model: string, readonly replacement?: string) {
    super(`Fabric model ${JSON.stringify(model)} is denied by fleet policy #2236. ` +
      (replacement ? `Use ${replacement} instead.` : "Ask the host administrator for an allowed replacement."));
    this.name = "FabricModelDeniedError";
  }
}

/** Check both raw intent and the canonical resolved key, case-insensitively. */
export const assertFabricModelAllowed = (model: string | undefined, policy?: FabricModelPolicy): void => {
  const key = model?.trim().toLowerCase();
  if (key && policy?.deniedModels?.some((denied) => denied.trim().toLowerCase() === key)) {
    throw new FabricModelDeniedError(key, policy.deniedModelReplacement?.trim() || undefined);
  }
};

export const MINIMUM_PI_HOST_VERSION = "0.80.6";

/** Pi 0.86.0 intercepts both RPC/TUI user shell requests and propagates hook failures. */
export const MINIMUM_FIXTURE_PI_HOST_VERSION = "0.86.0";

/** Only verifiable releases establish the fixture contract; unknown/prerelease hosts fail closed. */
export const fixturePiHostSupported = (version: string | undefined): boolean =>
  version !== undefined &&
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/.test(version) &&
  (compareVersions(version, MINIMUM_FIXTURE_PI_HOST_VERSION) ?? -1) >= 0;

const PI_HOST_PACKAGE_NAMES = new Set([
  "@earendil-works/pi-coding-agent",
  "@mariozechner/pi-coding-agent",
]);

interface ParsedVersion {
  numbers: [number, number, number];
  prerelease?: string;
}

const parseVersion = (value: string): ParsedVersion | undefined => {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?/.exec(value.trim());
  if (!match) return undefined;
  return {
    numbers: [Number(match[1]), Number(match[2]), Number(match[3])],
    ...(match[4] ? { prerelease: match[4] } : {}),
  };
};

export const compareVersions = (left: string, right: string): number | undefined => {
  const a = parseVersion(left);
  const b = parseVersion(right);
  if (!a || !b) return undefined;
  for (let index = 0; index < a.numbers.length; index++) {
    const delta = a.numbers[index]! - b.numbers[index]!;
    if (delta !== 0) return Math.sign(delta);
  }
  if (a.prerelease && !b.prerelease) return -1;
  if (!a.prerelease && b.prerelease) return 1;
  if (a.prerelease === b.prerelease) return 0;
  return (a.prerelease ?? "").localeCompare(b.prerelease ?? "");
};

export const detectPiHostVersion = (
  cliPath: string | undefined = process.argv[1],
): string | undefined => {
  if (!cliPath) return undefined;
  let directory: string;
  try {
    directory = path.dirname(realpathSync(cliPath));
  } catch {
    return undefined;
  }
  while (true) {
    const manifestPath = path.join(directory, "package.json");
    if (existsSync(manifestPath)) {
      try {
        const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
          name?: unknown;
          version?: unknown;
        };
        if (
          typeof manifest.name === "string" &&
          PI_HOST_PACKAGE_NAMES.has(manifest.name) &&
          typeof manifest.version === "string"
        ) {
          return manifest.version;
        }
      } catch {
        // Keep walking when a parent package manifest is unreadable.
      }
    }
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
};

export const piHostCompatibilityWarning = (
  version: string | undefined = detectPiHostVersion(),
): string | undefined => {
  if (!version) return undefined;
  const comparison = compareVersions(version, MINIMUM_PI_HOST_VERSION);
  if (comparison === undefined || comparison >= 0) return undefined;
  return "Pi Fabric requires Pi >= " + MINIMUM_PI_HOST_VERSION + "; detected " + version + ". Actor triggerTurn and other host continuations may be ignored. Upgrade Pi before relying on actor delivery.";
};

/** Pi 0.87.0 added agent_before_settle, where the followUp drain hands its messages back to Pi. */
export const FOLLOW_UP_DRAIN_MIN_PI_VERSION = "0.87.0";

/**
 * Whether this Pi host can run the followUp drain (smarty-dev#1495). An older host keeps Pi's own
 * followUp queue: without agent_before_settle, a held followUp could only restart Main after the
 * run, which a cancel there must prevent. An unknown host (an SDK embedding) is assumed current.
 */
export const followUpDrainSupported = (version: string | undefined = detectPiHostVersion()): boolean =>
  version === undefined || (compareVersions(version, FOLLOW_UP_DRAIN_MIN_PI_VERSION) ?? 1) >= 0;

// Shared startup metadata belongs in this existing eager/lazy chunk, keeping the file-count budget.
/** Admission metadata only. Pi owns turnId and receivedAt at first receipt. */
export interface FabricTurnProvenance {
  v: 1;
  channel: "fabric";
  principal?: FabricPrincipal | undefined;
  sender: {
    id: string;
    kind: "main" | "actor" | "agent" | "remote";
    name?: string;
    verified: "mesh" | "bridge";
  };
  via: "steer" | "followUp" | "actor" | "replay";
}

/** Attribution only. org-agent is a reserved binding, never inferred from a name/role. */
export interface FabricPrincipal {
  readonly id: string;
  readonly binding: "herdr-client" | "voice-call" | "org-agent";
}

/** Integration port for #808's authority mapping. No default allow/refuse policy. */
export type FabricPrincipalAuthorityCheck = (request: {
  readonly principal: FabricPrincipal | undefined;
  readonly action: string;
  readonly target: string;
}) => { decision: "allow" | "refuse" | "unknown"; reason?: string } | Promise<{ decision: "allow" | "refuse" | "unknown"; reason?: string }>;

/** Snapshot a host/envelope field; never call on message text or model payload data. */
export const copyFabricPrincipal = (value: unknown): FabricPrincipal | undefined => {
  const p = value as Partial<FabricPrincipal> | null | undefined;
  return typeof p?.id === "string" && p.id.trim() && p.id.length <= 256 &&
    (p.binding === "herdr-client" || p.binding === "voice-call" || p.binding === "org-agent")
    ? Object.freeze({ id: p.id, binding: p.binding }) : undefined;
};

/** Only Pi-stamped v1 receipts can start a scope; claims are not receipts. */
export const principalFromReceipt = (value: unknown): FabricPrincipal | undefined => {
  const p = value as { v?: unknown; channel?: unknown; turnId?: unknown; receivedAt?: unknown; principal?: unknown; sender?: { verified?: unknown } } | undefined;
  if (p?.v !== 1 || typeof p.turnId !== "string" || !p.turnId || typeof p.receivedAt !== "string" || !p.receivedAt) return undefined;
  const principal = copyFabricPrincipal(p.principal);
  if (p.channel === "keyboard" && principal?.binding === "herdr-client") return principal;
  if (p.channel === "voice" && principal?.binding === "voice-call") return principal;
  if (p.channel === "fabric" && (p.sender?.verified === "mesh" || p.sender?.verified === "bridge")) return principal;
  return undefined;
};

const turnPrincipals = new WeakMap<object, FabricPrincipal | undefined>();
// Partial/legacy host contexts need not expose an object session manager.
// Without that host-owned key there is no scope, never a process-wide fallback.
const principalSessionKey = (context: unknown): object | undefined => {
  if (typeof context !== "object" || context === null) return undefined;
  const session = (context as { sessionManager?: unknown }).sessionManager;
  return typeof session === "object" && session !== null ? session : undefined;
};
const requestMessage = (message: { role?: unknown; customType?: unknown }): boolean =>
  message.role === "user" || (message.role === "custom" &&
    !["pi-fabric-skill-reference", "pi-fabric-proxy", "pi-fabric-shell-awareness"].includes(String(message.customType)));

/** Cheap observers only: no engine imports, identity mapping or filesystem work. */
export const registerFabricPrincipalCapture = (pi: ExtensionAPI): void => {
  pi.on("before_agent_start", (_event, context) => {
    const session = principalSessionKey(context);
    if (session) turnPrincipals.set(session, undefined);
  });
  pi.on("message_start", (event, context) => {
    const session = principalSessionKey(context);
    if (session && requestMessage(event.message)) turnPrincipals.set(session, principalFromReceipt((event.message as { provenance?: unknown }).provenance));
  });
  pi.on("context", (event, context) => {
    const session = principalSessionKey(context);
    if (!session) return;
    // Actual inference input includes queued deliveries and survives live reload.
    // Passive skill/proxy/shell-awareness notices do not replace the requester.
    const message = [...event.messages].reverse().find(requestMessage);
    if (message) turnPrincipals.set(session, principalFromReceipt((message as { provenance?: unknown }).provenance));
  });
};

export const currentFabricPrincipal = (context?: { sessionManager?: object }): FabricPrincipal | undefined => {
  const session = principalSessionKey(context);
  return session ? copyFabricPrincipal(turnPrincipals.get(session)) : undefined;
};

// A private host-owned token survives invocation-context spreads. Presence with an
// undefined principal is an immutable UNKNOWN snapshot, not permission to re-sample.
const invocationPrincipal = Symbol("fabric.invocation-principal");
type PrincipalInvocation = { extensionContext?: { sessionManager?: object } };
type CapturedInvocation = { [invocationPrincipal]?: Readonly<{ principal: FabricPrincipal | undefined }> };
export const snapshotFabricInvocation = <T extends PrincipalInvocation>(context: T): T => {
  if ((context as CapturedInvocation)[invocationPrincipal]) return context;
  return { ...context, [invocationPrincipal]: Object.freeze({ principal: currentFabricPrincipal(context.extensionContext) }) };
};
export const invocationFabricPrincipal = (context: PrincipalInvocation): FabricPrincipal | undefined => {
  const captured = (context as CapturedInvocation)[invocationPrincipal];
  return captured ? captured.principal : currentFabricPrincipal(context.extensionContext);
};

// Trusted extension components run outside the guest interpreter. A private host token,
// not a caller-supplied field or a component-looking tool-call id, admits their host-only
// provider arguments. Like the principal snapshot it survives host context spreads; guest
// execution services construct fresh contexts and never copy it into a program's calls.
const hostCaller = Symbol("fabric.host-caller");
type HostCallerInvocation = { [hostCaller]?: Readonly<{ componentId: string }> };
export const withFabricHostCaller = <T extends object>(context: T, componentId: string): T => ({
  ...context,
  [hostCaller]: Object.freeze({ componentId }),
});
export const fabricHostCallerId = (context: object): string | undefined =>
  (context as HostCallerInvocation)[hostCaller]?.componentId;

export interface FabricIdentityResolution {
  identity: MeshIdentity;
  mainAgentId: string;
}

/** Identity queries never import Main's journal engine. */
export const resolveFabricIdentity = (
  sessionId: string,
  environment: NodeJS.ProcessEnv = process.env,
): FabricIdentityResolution => {
  const identity = fabricHostIdentity(sessionId, environment);
  const inheritedMainAgentId = environment.PI_FABRIC_MAIN_AGENT_ID?.trim();
  return {
    identity,
    mainAgentId: inheritedMainAgentId || (identity.kind === "main" ? identity.id : `session:${sessionId}`),
  };
};

/** Explicit host capability: option acceptance cannot be inferred from a JS function's arity. */
export const fabricProvenanceSupported = (pi: ExtensionAPI): boolean =>
  (pi as ExtensionAPI & { hostCapabilities?: { turnProvenance?: unknown } }).hostCapabilities?.turnProvenance === 1;

// Survives extension generations in the same Pi process.
const WARNING_KEY = Symbol.for("pi-fabric.turn-provenance.compatibility-warning.v1");

/** Keep unsupported hosts' arguments unchanged. Never retry a send: it may have been received. */
export const fabricProvenanceOptions = <Options extends object | undefined>(
  pi: ExtensionAPI,
  options: Options,
  provenance: FabricTurnProvenance | (() => FabricTurnProvenance),
): Options | (Options & { provenance: FabricTurnProvenance }) => {
  if (fabricProvenanceSupported(pi)) {
    return { ...options, provenance: typeof provenance === "function" ? provenance() : provenance } as Options & { provenance: FabricTurnProvenance };
  }
  const diagnostics = globalThis as typeof globalThis & { [key: symbol]: unknown };
  if (diagnostics[WARNING_KEY] !== true) {
    diagnostics[WARNING_KEY] = true;
    console.warn("[pi-fabric] Pi does not advertise hostCapabilities.turnProvenance === 1; delivering without turn provenance (legacy behavior). Upgrade Pi to a host with turn provenance v1 support and configure global turnProvenance.fabricExtensions trust for this Fabric extension.");
  }
  return options;
};

/** Snapshot only the verified admission envelope, never message text or arbitrary payload data. */
export const fabricTurnProvenance = (
  from: MeshIdentity,
  via: FabricTurnProvenance["via"],
  verified: FabricTurnProvenance["sender"]["verified"],
  principal?: FabricPrincipal,
): FabricTurnProvenance => ({
  v: 1,
  channel: "fabric",
  ...(copyFabricPrincipal(principal) ? { principal: copyFabricPrincipal(principal) } : {}),
  sender: {
    id: from.id,
    kind: verified === "bridge" ? "remote" : from.kind,
    ...(typeof from.name === "string" && from.name ? { name: from.name } : {}),
    verified,
  },
  via,
});

/** Rehydrate only host-owned queue/envelope metadata; strip foreign fields and receipt stamps. */
export const copyFabricProvenance = (value: unknown): FabricTurnProvenance | undefined => {
  const p = value as Partial<FabricTurnProvenance> | null | undefined;
  const s = p?.sender;
  if (p?.v !== 1 || p.channel !== "fabric" || typeof s?.id !== "string" || !s.id ||
    !["main", "actor", "agent", "remote"].includes(s.kind) ||
    (s.verified !== "mesh" && s.verified !== "bridge") ||
    !["steer", "followUp", "actor", "replay"].includes(String(p.via))) return undefined;
  return {
    v: 1, channel: "fabric", via: p.via!,
    sender: { id: s.id, kind: s.verified === "bridge" ? "remote" : s.kind,
      ...(typeof s.name === "string" ? { name: s.name } : {}), verified: s.verified },
    ...(copyFabricPrincipal(p.principal) ? { principal: copyFabricPrincipal(p.principal) } : {}),
  };
};
/** Only a recorded admission or an explicit in-process producer may supply verification. */
export const sendFabricMessage = (
  pi: ExtensionAPI,
  message: Parameters<ExtensionAPI["sendMessage"]>[0],
  options: Parameters<ExtensionAPI["sendMessage"]>[1],
  from?: MeshIdentity | (() => MeshIdentity),
  via: FabricTurnProvenance["via"] = "actor",
  verification?: "mesh" | "bridge",
  principal?: FabricPrincipal,
  wakeCause?: FabricWakeCause,
): void => {
  const sender = options?.triggerTurn === true && options.deliverAs !== "nextTurn" ? (typeof from === "function" ? from() : from) : undefined;
  const inbox = isWakeInbox(message.customType);
  const wake = wakeCause ?? (sender ? fabricWakeCause(sender, inbox ? "inbox" : via === "steer" || via === "followUp" ? via : "actor") : undefined);
  const deliveryOptions = from && (verification === "mesh" || verification === "bridge")
    ? fabricProvenanceOptions(pi, options, () =>
      fabricTurnProvenance(sender ?? (typeof from === "function" ? from() : from), via, verification, principal))
    : options;
  pi.sendMessage(fabricWakeMessage(pi, message, options, wake), deliveryOptions);
};

export const sendFabricUserMessage = (
  pi: ExtensionAPI,
  content: Parameters<ExtensionAPI["sendUserMessage"]>[0],
  from: MeshIdentity | (() => MeshIdentity),
  via: FabricTurnProvenance["via"],
  options?: Parameters<ExtensionAPI["sendUserMessage"]>[1],
  verification?: "mesh" | "bridge",
): void => {
  const state = wakeCaptures.get(pi);
  const sender = state ? (typeof from === "function" ? from() : from) : undefined;
  if (state && sender) {
    // Attempts explain ambiguity, never establish admission identity. Native input hooks
    // may handle these requests before our observer; there is no completion receipt.
    state.attempts.push(fabricWakeCause(sender, via === "steer" ? "steer" : via === "actor" ? "actor" : "followUp"));
    if (state.attempts.length > 128) state.attempts.shift();
  }
  const deliveryOptions = verification === "mesh" || verification === "bridge"
    ? fabricProvenanceOptions(pi, options, () =>
      fabricTurnProvenance(sender ?? (typeof from === "function" ? from() : from), via, verification))
    : options;
  if (deliveryOptions === undefined) pi.sendUserMessage(content);
  else pi.sendUserMessage(content, deliveryOptions);
};

/** Diagnostic attribution only: this never supplies provenance or authority. */
export interface FabricWakeCause {
  cause: "steer" | "followUp" | "actor" | "inbox" | "host-event" | "mesh";
  from: { id: string; name: string; kind: "main" | "actor" | "agent" | "remote" };
  topic?: string;
  key?: string;
}

export const fabricWakeCause = (
  from: { id: string; name?: string; kind: FabricWakeCause["from"]["kind"] }, cause: FabricWakeCause["cause"], topic?: string, key?: string,
): FabricWakeCause => ({
  cause, from: { id: from.id, name: from.name || from.id, kind: from.kind },
  ...(topic ? { topic } : {}), ...(key ? { key } : {}),
});

export const copyFabricWakeCause = (value: unknown): FabricWakeCause | undefined => {
  const wake = value as Partial<FabricWakeCause> | undefined;
  if (!wake || !["steer", "followUp", "actor", "inbox", "host-event", "mesh"].includes(String(wake.cause)) ||
    typeof wake.from?.id !== "string" || typeof wake.from.name !== "string" ||
    !["main", "actor", "agent", "remote"].includes(wake.from.kind)) return;
  return fabricWakeCause(wake.from, wake.cause!, typeof wake.topic === "string" ? wake.topic : undefined,
    typeof wake.key === "string" ? wake.key : undefined);
};

// Private in-process admission metadata survives routing spreads but is never
// serialized into a sender command. Receiving adapters derive these snapshots
// from authenticated envelopes; plain wakeCause/data/details fields cannot forge it.
const wakeAdmission = Symbol("fabric-wake-admission");
type WakeAdmitted = { [wakeAdmission]?: readonly FabricWakeCause[] };
export const withFabricWakeAdmission = <T extends object>(request: T, causes: readonly FabricWakeCause[]): T =>
  ({ ...request, [wakeAdmission]: causes.map(cause => copyFabricWakeCause(cause)!).filter(Boolean) });
export const copyFabricWakeAdmission = <T extends object>(from: object, request: T): T => {
  const causes = (from as WakeAdmitted)[wakeAdmission];
  return causes ? withFabricWakeAdmission(request, causes) : request;
};
export const admittedFabricWakeCauses = (request: object): FabricWakeCause[] | undefined =>
  (request as WakeAdmitted)[wakeAdmission]?.map(cause => copyFabricWakeCause(cause)!);

/** Raw-user diagnostics make NO origin claim; candidates are unconfirmed send attempts. */
type RawWakeDiagnostic = { cause: "unattributed" } | {
  cause: "ambiguous";
  candidates: FabricWakeCause[];
  basis: "unconfirmed-raw-input-attempts";
};
export type FabricWakeSource = (FabricWakeCause & { exact: true }) | (RawWakeDiagnostic & { exact: false });
export type FabricWakeDiagnostic = RawWakeDiagnostic | { cause: "multiple"; exact: false; causes: FabricWakeSource[] };

interface WakeCapture {
  identity: MeshIdentity;
  sources: FabricWakeSource[];
  attempts: FabricWakeCause[];
  keyedSources: Set<string>;
}
// Native sendCustomMessage preserves details by reference. Only our in-process
// producer can register this receipt; serialized sender fields cannot forge it.
// Weak ownership neither retains messages nor backfills historical session data.
type WakeAdmissionSource = FabricWakeCause | { cause: "unattributed" };
const wakeMessageReceipts = new WeakMap<object, WakeAdmissionSource[]>();
const wakeCaptures = new WeakMap<ExtensionAPI, WakeCapture>();
const isWakeInbox = (type: string): boolean => type.includes("inbox") || type.includes("completion") ||
  type === "pi-fabric-agent-complete" || type === "pi-fabric-shell-event" || type === "pi-fabric-records";
const wakeHost = (pi: ExtensionAPI): MeshIdentity => wakeCaptures.get(pi)?.identity ??
  { id: "fabric:host", name: "Fabric host", kind: "main" };

/** Preserve model text; persist attribution on the exact native custom-message entry. */
export const fabricWakeMessage = (
  pi: ExtensionAPI, message: Parameters<ExtensionAPI["sendMessage"]>[0],
  options: Parameters<ExtensionAPI["sendMessage"]>[1], wake?: FabricWakeCause | readonly WakeAdmissionSource[],
): Parameters<ExtensionAPI["sendMessage"]>[0] => {
  const supplied = message.details && typeof message.details === "object" ? message.details : {};
  const { wakeCause: _foreignCause, wakeCauses: _foreignCauses, ...details } = supplied as Record<string, unknown>;
  if (options?.triggerTurn !== true || options.deliverAs === "nextTurn") {
    return "wakeCause" in supplied || "wakeCauses" in supplied ? { ...message, details } : message;
  }
  const sources = (Array.isArray(wake) ? wake : [wake ?? fabricWakeCause(wakeHost(pi),
    isWakeInbox(message.customType) ? "inbox" : "host-event")])
    .map((source): WakeAdmissionSource | undefined => source?.cause === "unattributed"
      ? { cause: "unattributed" } : copyFabricWakeCause(source))
    .filter((source): source is WakeAdmissionSource => source !== undefined);
  const causes = sources.filter((source): source is FabricWakeCause => source.cause !== "unattributed");
  const stamped = { ...details, ...(causes.length === 1 ? { wakeCause: causes[0] } : causes.length ? { wakeCauses: causes } : {}) };
  // Do not alias mutable message.details with the private admission snapshot.
  wakeMessageReceipts.set(stamped, sources.map(source => source.cause === "unattributed"
    ? { cause: "unattributed" } : copyFabricWakeCause(source)!));
  return { ...message, details: stamped };
};

/** Exact only for one admitted source; mixed turns retain every source without a single-origin claim. */
export const registerFabricWakeCapture = (pi: ExtensionAPI): void => {
  if (wakeCaptures.has(pi)) return;
  const state: WakeCapture = { identity: wakeHost(pi), sources: [], attempts: [], keyedSources: new Set() };
  wakeCaptures.set(pi, state);
  const reset = (): void => { state.sources = []; state.keyedSources.clear(); };
  const admitSource = (source: FabricWakeSource): void => {
    if (source.exact && source.key) {
      // Sender-scoped delivery identity, not text or cause classification. Unkeyed
      // admissions remain distinct, and each inference boundary starts fresh.
      const key = JSON.stringify([source.from.kind, source.from.id, source.topic, source.key]);
      if (state.keyedSources.has(key)) return;
      state.keyedSources.add(key);
    }
    state.sources.push(source);
  };
  const clear = (): void => { reset(); state.attempts = []; };
  pi.on("session_start", (_event, context) => {
    clear();
    state.identity = fabricHostIdentity(context.sessionManager.getSessionId());
  });
  pi.on("session_before_switch", clear);
  pi.on("session_tree", clear);
  pi.on("session_shutdown", clear);
  pi.on("agent_settled", (event, context) => {
    if (context.signal?.aborted || (event as { outcome?: string }).outcome === "aborted") clear();
  });
  pi.on("turn_start", reset);
  pi.on("message_start", event => {
    if (event.message.role === "custom") {
      const details = event.message.details;
      if (details && typeof details === "object") {
        const causes = wakeMessageReceipts.get(details);
        wakeMessageReceipts.delete(details);
        for (const cause of causes ?? []) admitSource(cause.cause === "unattributed"
          ? { cause: "unattributed", exact: false } : { ...copyFabricWakeCause(cause)!, exact: true });
      }
    } else if (event.message.role === "user") {
      // Only a host-owned admission receipt is exact. Even equal text observed in
      // input preflight is not identity: queued inputs and handled commands break it.
      const receipt = (event.message as { provenance?: FabricTurnProvenance }).provenance;
      const admitted = fabricProvenanceSupported(pi) ? copyFabricProvenance(receipt) : undefined;
      if (admitted) state.sources.push({ ...fabricWakeCause(admitted.sender,
        admitted.via === "steer" ? "steer" : admitted.via === "actor" ? "actor" : "followUp"), exact: true });
      else state.sources.push(state.attempts.length > 1
        ? { cause: "ambiguous", exact: false, candidates: state.attempts.map(candidate => fabricWakeCause(candidate.from, candidate.cause)),
          basis: "unconfirmed-raw-input-attempts" }
        : { cause: "unattributed", exact: false });
      state.attempts = [];
    }
  });
  pi.on("context", () => {
    const sources = state.sources;
    reset(); // A repeated context with no new admissions is not another wake.
    if (sources.length > 1) pi.appendEntry("pi-fabric.wake-diagnostic", { cause: "multiple", exact: false, causes: sources });
    else if (sources.length === 1) {
      const { exact, ...record } = sources[0]!;
      pi.appendEntry(exact ? "pi-fabric.wake-cause" : "pi-fabric.wake-diagnostic", record);
    }
  });
};

/** Runtime participant identity, with no human principal. Participant-free notices make no claim. */
export const fabricHostIdentity = (
  sessionId: string,
  environment: NodeJS.ProcessEnv = process.env,
): MeshIdentity => {
  const actorId = environment.PI_FABRIC_ACTOR_ID?.trim();
  const agentId = environment.PI_FABRIC_PARENT_RUN?.trim();
  if (actorId) return { id: actorId, name: environment.PI_FABRIC_ACTOR_NAME?.trim() || actorId.slice(0, 8), kind: "actor", sessionId };
  if (agentId) return { id: agentId, name: environment.PI_FABRIC_AGENT_NAME?.trim() || agentId.slice(0, 8), kind: "agent", sessionId };
  return { id: `session:${sessionId}`, name: "main", kind: "main", sessionId };
};
