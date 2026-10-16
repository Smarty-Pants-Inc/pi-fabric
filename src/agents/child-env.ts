import fs from "node:fs";
import path from "node:path";
import type { FabricMeshConfig } from "../config.js";
import { isInside, readWritePolicy, type FabricWritePolicy } from "./write-guard.js";

export { readWritePolicy, type FabricWritePolicy };

const MAX_WRITABLE_ROOTS = 32;
const MAX_ROOT_CHARS = 4_096;

/**
 * Carry the resolved parent's carrier into a local task child, even when the child
 * loads a different workspace config. Call after assembling the child's base env.
 * This copies the environment; it never reads credentials or changes stateBackend.
 * NATS inheritance contains only messaging settings, not state-backend replicas.
 */
export const childMeshTransportEnvironment = (
  mesh: Pick<FabricMeshConfig, "transport" | "hostTransport" | "nats">,
  environment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv => {
  const child: NodeJS.ProcessEnv = { ...environment, PI_FABRIC_MESH_TRANSPORT: mesh.transport };
  delete child.PI_FABRIC_MESH_HOST_TRANSPORT;
  delete child.PI_FABRIC_MESH_NATS;
  if (mesh.hostTransport !== undefined) child.PI_FABRIC_MESH_HOST_TRANSPORT = mesh.hostTransport;
  if (mesh.transport === "nats") {
    const { servers, credentials, machine, inboxLanes } = mesh.nats;
    child.PI_FABRIC_MESH_NATS = JSON.stringify({ servers, credentials, machine, inboxLanes: inboxLanes ?? 8 });
  }
  return child;
};

/** Stable PI_FABRIC_LINEAGE contract for a child process. */
export interface FabricAgentLineage {
  version: 1;
  rootSessionId: string;
  parentSessionId?: string;
  parentRunId?: string;
  runId: string;
  depth: number;
  childIndex: number;
  worker: true;
}

export const readAgentLineage = (
  raw = process.env.PI_FABRIC_LINEAGE,
): FabricAgentLineage | undefined => {
  if (!raw) return undefined;
  try {
    const value = JSON.parse(raw) as FabricAgentLineage;
    return value?.version === 1 && typeof value.rootSessionId === "string" && typeof value.runId === "string"
      ? value
      : undefined;
  } catch {
    return undefined;
  }
};

export interface AgentWritePolicyRequest {
  /** Explicitly opt out of the task default; never escapes an inherited policy. */
  writePolicy?: "unrestricted";
  readOnly?: boolean;
  writableRoots?: string[];
  shell?: "deny" | "unconfined";
}

export const requestsWritePolicy = (request: AgentWritePolicyRequest): boolean =>
  request.writePolicy !== undefined || request.readOnly !== undefined || request.writableRoots !== undefined || request.shell !== undefined;

/** Shape checks that must pass before admission or worktree side effects. */
export const checkWritePolicyRequest = (request: AgentWritePolicyRequest): void => {
  if (request.writePolicy !== undefined && request.writePolicy !== "unrestricted") {
    throw new Error('writePolicy must be "unrestricted" or omitted');
  }
  if (request.writePolicy === "unrestricted" &&
    (request.readOnly !== undefined || request.writableRoots !== undefined || request.shell !== undefined)) {
    throw new Error('writePolicy: "unrestricted" cannot be combined with readOnly, writableRoots, or shell');
  }
  if (request.readOnly !== undefined && typeof request.readOnly !== "boolean") {
    throw new Error("readOnly must be a boolean");
  }
  if (request.shell !== undefined && request.shell !== "deny" && request.shell !== "unconfined") {
    throw new Error('shell must be "deny" or "unconfined"');
  }
  const roots = request.writableRoots;
  if (roots === undefined) return;
  if (
    !Array.isArray(roots) ||
    roots.length > MAX_WRITABLE_ROOTS ||
    roots.some((root) => typeof root !== "string" || !root.trim() || root.length > MAX_ROOT_CHARS)
  ) {
    throw new Error(`writableRoots must be at most ${MAX_WRITABLE_ROOTS} non-empty paths`);
  }
  if (request.readOnly === true && roots.length > 0) {
    throw new Error("readOnly agents cannot also request writableRoots");
  }
};

const nearestExisting = (target: string): string => {
  let current = target;
  while (!fs.existsSync(current)) {
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return path.join(fs.realpathSync(current), path.relative(current, target));
};

export class AgentWriteRootOverlapError extends Error {
  readonly code = "FABRIC_AGENT_WRITE_ROOT_OVERLAP";
  readonly launchOutcome = "unlaunched";
  constructor(readonly runRoot: string, readonly writableRoot: string) {
    super(`Default task write root ${writableRoot} contains host-owned run root ${runRoot}; move the run root outside the task cwd or choose an explicit writableRoots policy`);
    this.name = "AgentWriteRootOverlapError";
  }
}

/** A default cwd grant must not implicitly grant host-owned run records. */
export const assertDefaultTaskWriteRoots = (runRoot: string, roots: readonly string[]): void => {
  const canonicalRunRoot = nearestExisting(path.resolve(runRoot));
  for (const root of roots) {
    const canonicalRoot = nearestExisting(path.resolve(root));
    if (isInside(canonicalRoot, canonicalRunRoot)) throw new AgentWriteRootOverlapError(canonicalRunRoot, canonicalRoot);
  }
};

/**
 * Effective child policy: explicit request narrowed by the caller's own
 * PI_FABRIC_WRITE_POLICY. A confined caller cannot drop readOnly, enable an
 * unconfined shell, or name roots outside its own; an empty request inherits.
 */
export const resolveChildWritePolicy = (
  request: AgentWritePolicyRequest,
  parent: FabricWritePolicy | undefined,
  cwd: string,
): FabricWritePolicy | undefined => {
  if (request.writePolicy === "unrestricted") {
    if (parent) throw new Error("A write-confined agent cannot launch an unrestricted child");
    return undefined;
  }
  if (!requestsWritePolicy(request)) return parent;
  const readOnly = request.readOnly ?? parent?.readOnly ?? false;
  if (parent?.readOnly && (!readOnly || request.writableRoots?.length)) {
    throw new Error("A read-only agent cannot launch a writable child");
  }
  const shell = request.shell ?? parent?.shell ?? "deny";
  if (parent?.shell === "deny" && shell === "unconfined") {
    throw new Error('A shell-confined agent cannot launch a child with shell: "unconfined"');
  }
  if (readOnly) return { readOnly, writableRoots: [], shell };
  const realCwd = fs.realpathSync(cwd);
  const within = (target: string): boolean => !parent || parent.writableRoots.some((root) => isInside(root, target));
  const roots = request.writableRoots ?? (parent ? parent.writableRoots : [cwd]);
  const resolved = new Set<string>();
  for (const root of roots) {
    const absolute = path.resolve(cwd, root);
    if (!fs.existsSync(absolute)) {
      // Missing roots are created, but only inside the child cwd and caller roots.
      const target = nearestExisting(absolute);
      if (!isInside(cwd, absolute) || !isInside(realCwd, target) || !within(target)) {
        throw new Error(`Writable root must exist or be creatable inside the agent cwd: ${root}`);
      }
      fs.mkdirSync(absolute, { recursive: true });
    }
    const real = fs.realpathSync(absolute);
    if (!fs.statSync(real).isDirectory()) throw new Error(`Writable root is not a directory: ${root}`);
    if (!within(real)) throw new Error(`Writable root is outside the caller's writable roots: ${root}`);
    resolved.add(real);
  }
  return { readOnly, writableRoots: [...resolved].sort(), shell };
};
