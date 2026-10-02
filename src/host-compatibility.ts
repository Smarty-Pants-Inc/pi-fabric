import { existsSync, readFileSync, realpathSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";
import path from "node:path";

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

export const MINIMUM_PI_HOST_VERSION = "0.99.0";

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
  return "Pi Fabric requires Pi >= " + MINIMUM_PI_HOST_VERSION + "; detected " + version + ". Native tool loadouts and nested execution require Pi 0.99. Upgrade Pi before using Fabric.";
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
): void => {
  const deliveryOptions = from && (verification === "mesh" || verification === "bridge")
    ? fabricProvenanceOptions(pi, options, () =>
      fabricTurnProvenance(typeof from === "function" ? from() : from, via, verification, principal))
    : options;
  pi.sendMessage(message, deliveryOptions);
};

export const sendFabricUserMessage = (
  pi: ExtensionAPI,
  content: Parameters<ExtensionAPI["sendUserMessage"]>[0],
  from: MeshIdentity | (() => MeshIdentity),
  via: FabricTurnProvenance["via"],
  options?: Parameters<ExtensionAPI["sendUserMessage"]>[1],
  verification?: "mesh" | "bridge",
): void => {
  const deliveryOptions = verification === "mesh" || verification === "bridge"
    ? fabricProvenanceOptions(pi, options, () =>
      fabricTurnProvenance(typeof from === "function" ? from() : from, via, verification))
    : options;
  if (deliveryOptions === undefined) pi.sendUserMessage(content);
  else pi.sendUserMessage(content, deliveryOptions);
};

/** Fabric's own runtime identity for host-generated messages, with no human principal. */
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
