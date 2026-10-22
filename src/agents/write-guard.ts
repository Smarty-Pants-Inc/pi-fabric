import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Child write confinement passed by the parent as PI_FABRIC_WRITE_POLICY. */
export interface FabricWritePolicy {
  readOnly: boolean;
  /** Canonical absolute roots; empty with readOnly. */
  writableRoots: string[];
  shell: "deny" | "unconfined";
}

const DENY_ALL: FabricWritePolicy = { readOnly: true, writableRoots: [], shell: "deny" };
const WRITE_TOOLS = new Set(["write", "edit"]);
const SHELL_TOOLS = new Set(["bash", "powershell"]);

/** Malformed policy text fails closed to read-only with shell denied. */
export const readWritePolicy = (
  raw = process.env.PI_FABRIC_WRITE_POLICY,
): FabricWritePolicy | undefined => {
  if (raw === undefined || raw === "") return undefined;
  try {
    const value = JSON.parse(raw) as Partial<FabricWritePolicy>;
    const roots = value.writableRoots;
    if (
      typeof value.readOnly !== "boolean" ||
      (value.shell !== "deny" && value.shell !== "unconfined") ||
      !Array.isArray(roots) ||
      roots.some((root) => typeof root !== "string" || !path.isAbsolute(root))
    ) return DENY_ALL;
    return { readOnly: value.readOnly, writableRoots: value.readOnly ? [] : roots, shell: value.shell };
  } catch {
    return DENY_ALL;
  }
};

export const isInside = (root: string, target: string): boolean => {
  const relative = path.relative(root, target);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};

// Mirrors Pi's write/edit resolution (unicode spaces, @ prefix, ~, file://).
export const resolveToolPath = (input: string, cwd: string): string => {
  let value = input.replace(/[  -   　]/g, " ");
  if (value.startsWith("@")) value = value.slice(1);
  if (value === "~") return os.homedir();
  if (value.startsWith("~/") || (process.platform === "win32" && value.startsWith("~\\"))) {
    return path.join(os.homedir(), value.slice(2));
  }
  if (/^file:\/\//.test(value)) return fileURLToPath(value);
  return path.resolve(cwd, value);
};

/** Real path of the target or its nearest existing ancestor; throws on dangling links. */
const nearestRealPath = (target: string): string => {
  let current = target;
  for (;;) {
    let exists = true;
    try {
      fs.lstatSync(current);
    } catch {
      exists = false;
    }
    if (exists) return path.join(fs.realpathSync(current), path.relative(current, target));
    const parent = path.dirname(current);
    if (parent === current) throw new Error("no existing ancestor");
    current = parent;
  }
};

/** Return a refusal reason, or undefined when the policy allows the call. */
export const writePolicyDenial = (
  policy: FabricWritePolicy,
  toolName: string,
  input: unknown,
  cwd: string,
): string | undefined => {
  if (SHELL_TOOLS.has(toolName)) {
    return policy.shell === "unconfined"
      ? undefined
      : `Fabric write policy refuses ${toolName}: shell commands cannot be confined; the parent must request shell: "unconfined"`;
  }
  if (!WRITE_TOOLS.has(toolName)) return undefined;
  const args = typeof input === "object" && input !== null ? input as Record<string, unknown> : {};
  const raw = [args.path, args.file_path, args.filePath].find((value) => typeof value === "string");
  const scope = `refused path: ${JSON.stringify(raw ?? null)}; allowed roots: ${JSON.stringify(policy.writableRoots)}`;
  if (policy.readOnly) return `Fabric write policy refuses ${toolName}: this agent is read-only; ${scope}`;
  if (typeof raw !== "string" || raw === "") return `Fabric write policy refuses ${toolName}: no path argument; ${scope}`;
  try {
    const lexical = resolveToolPath(raw, cwd);
    const real = nearestRealPath(lexical);
    if (
      policy.writableRoots.some((root) => isInside(root, lexical)) &&
      policy.writableRoots.some((root) => isInside(root, real))
    ) return undefined;
  } catch {
    // Unresolvable paths fail closed.
  }
  const actorHint = process.env.PI_FABRIC_ACTOR_ID || process.env.PI_FABRIC_ACTOR_NAME
    ? "; write outside actor scratch refused; use the shell or a trusted helper for factory state"
    : "";
  return `Fabric write policy refuses ${toolName} outside writable roots; ${scope}${actorHint}`;
};

const privateTempRoots = (directories: (string | undefined)[]): string[] => {
  const uid = process.getuid?.();
  if (uid === undefined) return [];
  let systemTemp: string | undefined;
  try { systemTemp = fs.realpathSync("/tmp"); } catch { /* missing system temp means no special exclusion needed */ }
  const roots = new Set<string>();
  for (const directory of directories) {
    if (!directory || !path.isAbsolute(directory)) continue;
    try {
      const real = fs.realpathSync(directory);
      if (real === path.resolve("/tmp") || real === systemTemp) continue;
      const stat = fs.statSync(real);
      if (stat.isDirectory() && stat.uid === uid && (stat.mode & 0o777) === 0o700) roots.add(real);
    } catch {
      // Missing, inaccessible, or non-directory temp roots add no authority.
    }
  }
  return [...roots];
};

/** Child-side guard loaded by the worker with -e only when a policy is set. */
export default function fabricWriteGuard(pi: ExtensionAPI): void {
  const extensionTmpDir = process.env.TMPDIR;
  const policy = readWritePolicy();
  if (!policy) return;
  pi.on("tool_call", (event, context) => {
    let effectivePolicy = policy;
    if (!policy.readOnly && policy.shell === "unconfined" && WRITE_TOOLS.has(event.toolName)) {
      // An unconfined shell can already write its own private TMPDIR; allowing native write there only fixes refusals of that same session scratch.
      effectivePolicy = {
        ...policy,
        writableRoots: [...new Set([
          ...policy.writableRoots,
          ...privateTempRoots([process.env.TMPDIR, extensionTmpDir]),
        ])],
      };
    }
    const reason = writePolicyDenial(effectivePolicy, event.toolName, event.input, context.cwd);
    return reason ? { block: true, reason } : undefined;
  });
}
