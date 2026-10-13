import fs from "node:fs";
import path from "node:path";
import { isInside, readWritePolicy, type FabricWritePolicy } from "./write-guard.js";

export { readWritePolicy, type FabricWritePolicy };

const MAX_WRITABLE_ROOTS = 32;
const MAX_ROOT_CHARS = 4_096;

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
  readOnly?: boolean;
  writableRoots?: string[];
  shell?: "deny" | "unconfined";
}

export const requestsWritePolicy = (request: AgentWritePolicyRequest): boolean =>
  request.readOnly !== undefined || request.writableRoots !== undefined || request.shell !== undefined;

/** Shape checks that must pass before admission or worktree side effects. */
export const checkWritePolicyRequest = (request: AgentWritePolicyRequest): void => {
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
