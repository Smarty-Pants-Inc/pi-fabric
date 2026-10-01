import { existsSync, readFileSync, realpathSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";
import path from "node:path";

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
  sender: {
    id: string;
    kind: "main" | "actor" | "agent" | "remote";
    name?: string;
    verified: "mesh" | "bridge";
  };
  via: "steer" | "followUp" | "actor" | "replay";
}

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
): FabricTurnProvenance => ({
  v: 1,
  channel: "fabric",
  sender: {
    id: from.id,
    kind: verified === "bridge" ? "remote" : from.kind,
    ...(typeof from.name === "string" && from.name ? { name: from.name } : {}),
    verified,
  },
  via,
});

/** Only a recorded admission or an explicit in-process producer may supply verification. */
export const sendFabricMessage = (
  pi: ExtensionAPI,
  message: Parameters<ExtensionAPI["sendMessage"]>[0],
  options: Parameters<ExtensionAPI["sendMessage"]>[1],
  from?: MeshIdentity | (() => MeshIdentity),
  via: FabricTurnProvenance["via"] = "actor",
  verification?: "mesh" | "bridge",
): void => {
  const deliveryOptions = from && (verification === "mesh" || verification === "bridge")
    ? fabricProvenanceOptions(pi, options, () =>
      fabricTurnProvenance(typeof from === "function" ? from() : from, via, verification))
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
