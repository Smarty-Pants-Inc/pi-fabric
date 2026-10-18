import { spawn } from "node:child_process";
import os from "node:os";
import {
  PLACEMENT_MAX_BYTES,
  parsePlacementJson,
  type PlacementDecideRequest,
} from "./placement-contract.js";
import type { FabricThinking } from "../thinking.js";
import type { FabricPlacementDeciderConfig } from "./placement-decider-config.js";

export type PlacementContextRefusal = "not-configured" | "unavailable" | "timeout" | "invalid-response" | "helper-teardown-unconfirmed";
export type PlacementContextField<T> =
  | { trust: "trusted"; value: T }
  | { trust: "missing"; reason: PlacementContextRefusal };

/** Verified caller launch-record facts returned by smarty-launch-context. */
export interface PlacementCallerRecord {
  native_launch_id: string;
  launch_spec_id: string;
  generation: number;
  fence: { token: number | string; state: string };
  host: string;
  unit: string;
  boot_id: string;
  invocation_id: string;
  main_pid_start: string;
  org_id: string;
  work_id: string;
  parent_native_launch_id?: string | null;
  role: string;
  profile: string;
  source_pin: string;
  memory_profile: string;
  origin: { class: "live-human" | "background-agent"; origin_ref: string };
  terminal?: { outcome: string; at: string };
}

export interface PlacementContextSnapshot {
  record: PlacementContextField<PlacementCallerRecord>;
  /** The only identity passed to the resolver is this process's own PID. */
  callerPid: number;
  missingFields: string[];
}

export interface PlacementContextResult {
  stdout: string;
  exitCode: number | null;
  failure?: Exclude<PlacementContextRefusal, "not-configured">;
}

const missing = (callerPid: number, reason: PlacementContextRefusal): PlacementContextSnapshot => ({
  record: { trust: "missing", reason }, callerPid,
  missingFields: ["native_launch_id", "launch_spec_id", "generation", "fence", "host", "unit", "boot_id", "invocation_id",
    "main_pid_start", "org_id", "work_id", "role", "profile", "source_pin", "memory_profile", "origin"],
});

const recordFrom = (value: unknown): PlacementCallerRecord | undefined => {
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== null) return undefined;
  const row = value as Record<string, unknown>;
  const allowed = new Set(["native_launch_id", "launch_spec_id", "generation", "fence", "host", "unit", "boot_id", "invocation_id",
    "main_pid_start", "org_id", "work_id", "parent_native_launch_id", "role", "profile", "source_pin", "memory_profile", "origin", "terminal"]);
  if (Object.keys(row).some(key => !allowed.has(key))) return undefined;
  const text = (field: unknown): field is string => typeof field === "string" && field.length > 0 && field.length <= 512 &&
    !/[\u0000-\u001f\u007f]/u.test(field);
  if (!text(row.native_launch_id) || !text(row.launch_spec_id) || !Number.isSafeInteger(row.generation) || (row.generation as number) < 1 ||
      !text(row.host) || !text(row.unit) || !text(row.boot_id) || !text(row.invocation_id) || !text(row.main_pid_start) ||
      !text(row.org_id) || !text(row.work_id) || (row.parent_native_launch_id !== undefined && row.parent_native_launch_id !== null && !text(row.parent_native_launch_id)) ||
      !text(row.role) || !text(row.profile) || !text(row.source_pin) || !text(row.memory_profile)) return undefined;
  if (!row.fence || typeof row.fence !== "object" || Array.isArray(row.fence) || Object.getPrototypeOf(row.fence) !== null) return undefined;
  const fence = row.fence as Record<string, unknown>;
  if (Object.keys(fence).some(key => key !== "token" && key !== "state") ||
      !(typeof fence.token === "number" && Number.isSafeInteger(fence.token) && fence.token >= 1 || text(fence.token)) || !text(fence.state)) return undefined;
  if (!row.origin || typeof row.origin !== "object" || Array.isArray(row.origin) || Object.getPrototypeOf(row.origin) !== null) return undefined;
  const origin = row.origin as Record<string, unknown>;
  if (Object.keys(origin).some(key => key !== "class" && key !== "origin_ref") ||
      (origin.class !== "live-human" && origin.class !== "background-agent") || !text(origin.origin_ref)) return undefined;
  // A process with terminal launch evidence cannot be the live caller for a new child.
  if (row.terminal !== undefined && row.terminal !== null) return undefined;
  return value as PlacementCallerRecord;
};

const HELPER_TEARDOWN_TIMEOUT_MS = 2_000;
const confirmHelperTeardown = async (pid: number | undefined, close: Promise<void>, isClosed: () => boolean): Promise<boolean> => {
  const deadline = performance.now() + HELPER_TEARDOWN_TIMEOUT_MS;
  if (!isClosed()) {
    const remaining = deadline - performance.now();
    if (remaining <= 0) return false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const closed = await Promise.race([
      close.then(() => true),
      new Promise<boolean>(resolve => { timer = setTimeout(() => resolve(false), remaining); }),
    ]);
    if (timer) clearTimeout(timer);
    if (!closed) return false;
  }
  if (process.platform === "win32" || pid === undefined) return true;
  while (performance.now() < deadline) {
    try { process.kill(-pid, 0); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return true; }
    const remaining = deadline - performance.now();
    if (remaining <= 0) break;
    await new Promise(resolve => setTimeout(resolve, Math.min(25, remaining)));
  }
  try { process.kill(-pid, 0); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
};
const warnUnconfirmedHelperTeardown = (pid: number | undefined): void => {
  const pgid = process.platform === "win32" ? "unavailable" : pid ?? "unknown";
  console.warn(`[pi-fabric] placement helper teardown unconfirmed pid=${pid ?? "unknown"} pgid=${pgid}`);
};

const cleanEnvironment = (): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = { PATH: process.platform === "win32" ? "" : "/usr/bin:/bin" };
  for (const key of ["HOME", "LANG", "TZ", "SystemRoot", "SYSTEMROOT", "USERPROFILE", "TEMP", "TMP"]) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  return env;
};

const runContextCommand = (command: readonly string[], callerPid: number, deadline: number): Promise<PlacementContextResult> =>
  new Promise(resolve => {
    if (performance.now() >= deadline) { resolve({ stdout: "", exitCode: null, failure: "timeout" }); return; }
    const executable = command[0]!;
    const args = [...command.slice(1), "--caller-pid", String(callerPid)];
    const child = spawn(executable, args, { shell: false, windowsHide: true, cwd: os.homedir(), env: cleanEnvironment(),
      stdio: ["ignore", "pipe", "ignore"], detached: process.platform !== "win32" });
    let settled = false;
    let aborting = false;
    let childClosed = false;
    let resolveChildClose: () => void = () => {};
    const childClose = new Promise<void>(resolve => { resolveChildClose = resolve; });
    let bytes = 0;
    const chunks: Buffer[] = [];
    const finish = (result: PlacementContextResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.stdout.destroy();
      resolve(result);
    };
    const abort = (failure: "unavailable" | "timeout" | "invalid-response"): void => {
      if (settled || aborting) return;
      aborting = true;
      const pid = child.pid;
      try {
        if (process.platform !== "win32" && pid !== undefined) process.kill(-pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch { /* The close and process-group checks below decide whether teardown is proven. */ }
      void confirmHelperTeardown(pid, childClose, () => childClosed).then(confirmed => {
        if (!confirmed) {
          warnUnconfirmedHelperTeardown(pid);
          finish({ stdout: "", exitCode: null, failure: "helper-teardown-unconfirmed" });
        } else finish({ stdout: "", exitCode: null, failure });
      });
    };
    const timer = setTimeout(() => abort("timeout"), Math.max(0, deadline - performance.now()));
    child.on("error", () => abort("unavailable"));
    child.stdout.on("error", () => abort("invalid-response"));
    child.stdout.on("data", (chunk: Buffer) => {
      if (settled) return;
      bytes += chunk.length;
      if (bytes > PLACEMENT_MAX_BYTES) { abort("invalid-response"); return; }
      chunks.push(chunk);
    });
    child.on("close", (exitCode, signal) => {
      childClosed = true;
      resolveChildClose();
      if (settled || aborting) return;
      if (performance.now() >= deadline) { abort("timeout"); return; }
      if (signal !== null) { finish({ stdout: "", exitCode, failure: "unavailable" }); return; }
      try {
        const stdout = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks));
        finish({ stdout, exitCode });
      } catch { finish({ stdout: "", exitCode, failure: "invalid-response" }); }
    });
  });

/**
 * Resolve the caller by process identity only. A request never supplies the PID,
 * parent record, org, work, origin, or source pin. Failure produces explicitly
 * missing context for shadow diagnostics and a fail-closed enforce decision.
 */
export async function resolvePlacementCallerContext(
  config: FabricPlacementDeciderConfig | undefined,
  deadline: number,
): Promise<PlacementContextSnapshot> {
  const callerPid = process.pid;
  if (!config?.contextCommand) return missing(callerPid, "not-configured");
  let result: PlacementContextResult;
  try { result = await runContextCommand(config.contextCommand, callerPid, deadline); }
  catch { return missing(callerPid, "unavailable"); }
  if (result.failure) return missing(callerPid, result.failure);
  if (result.exitCode !== 0) return missing(callerPid, "unavailable");
  try {
    const record = recordFrom(parsePlacementJson(result.stdout));
    if (!record) return missing(callerPid, "invalid-response");
    return { record: { trust: "trusted", value: record }, callerPid, missingFields: [] };
  } catch { return missing(callerPid, "invalid-response"); }
}

export interface PlacementRequestSeed {
  launchId: string;
  generation: number;
  /** Fabric-generated stable name; never prompt or request text. */
  agentName: string;
  child?: {
    role?: string;
    complexity?: "simple" | "normal" | "complex" | "delicate";
    profile?: string;
    memoryProfileRef?: string;
    actorActivation?: { actorId: string; rootId: string };
  };
  model?: string;
  thinking?: FabricThinking;
  capabilities: string[];
}
export interface PlacementRequestDraft {
  request: PlacementDecideRequest;
  missingFields: string[];
  trust: Record<string, "trusted" | "missing">;
  roleAllowed: boolean;
  /** Local launch-record inheritance; intentionally not added to the decider wire schema. */
  parentNativeLaunchId?: string;
}
const missingValue = (field: string): string => `MISSING:${field}`;

/** Static two-hop limit from the accepted host binding; request role never expands its parent grant. */
export const placementRoleAllowed = (parentRole: string, childRole: string,
  actorActivation?: { actorId: string; rootId: string }): boolean => {
  if (parentRole === "resident-host") return childRole === "actor-activation" && Boolean(actorActivation?.actorId && actorActivation.rootId);
  if (parentRole === "project-agent" || parentRole === "project-lead" || parentRole === "org-agent") {
    return ["worktree-agent", "task-agent", "review-agent", "supervisor", "resident-host", "actor-activation"].includes(childRole);
  }
  if (parentRole === "worktree-agent") return childRole === "task-agent";
  return false;
};

/**
 * Build a prompt-free draft from the verified caller record plus host-generated
 * child metadata. Missing facts use inert markers only for shadow and are
 * separately enumerated; enforce callers must refuse any draft with missing fields.
 */
export const buildPlacementRequestDraft = (context: PlacementContextSnapshot, seed: PlacementRequestSeed): PlacementRequestDraft => {
  const missingFields: string[] = [];
  const trust: Record<string, "trusted" | "missing"> = {
    launch_id: "trusted", generation: "trusted", agent_name: "trusted", mode: "trusted", runtime: "trusted",
  };
  const parent = context.record.trust === "trusted" ? context.record.value : undefined;
  const parentValue = (key: keyof PlacementCallerRecord): string => {
    const value = parent?.[key];
    const field = `parent.${String(key)}`;
    if (typeof value === "string" && value) { trust[field] = "trusted"; return value; }
    trust[field] = "missing"; missingFields.push(field); return missingValue(field);
  };
  const child = seed.child;
  const childValue = (key: "role" | "profile" | "memoryProfileRef" | "complexity"): string => {
    const value = child?.[key];
    const field = `child.${key}`;
    if (typeof value === "string" && value) { trust[field] = "trusted"; return value; }
    trust[field] = "missing"; missingFields.push(field); return missingValue(field);
  };
  const role = childValue("role");
  const complexityValue = childValue("complexity");
  const complexity = complexityValue === "simple" || complexityValue === "normal" || complexityValue === "complex" || complexityValue === "delicate"
    ? complexityValue : "normal";
  const profile = childValue("profile");
  const memoryProfile = childValue("memoryProfileRef");
  const org = parentValue("org_id");
  const work = parentValue("work_id");
  const sourcePin = parentValue("source_pin");
  const originClass = parent?.origin.class;
  if (originClass) trust["parent.origin.class"] = "trusted";
  else { trust["parent.origin.class"] = "missing"; missingFields.push("parent.origin.class"); }
  const safeOriginRef = parent?.origin.origin_ref;
  if (safeOriginRef) { trust["parent.origin.origin_ref"] = "trusted"; }
  else { trust["parent.origin.origin_ref"] = "missing"; missingFields.push("parent.origin.origin_ref"); }
  const inheritedParentId = parent?.native_launch_id;
  if (inheritedParentId) trust.parent_native_launch_id = "trusted";
  else { trust.parent_native_launch_id = "missing"; missingFields.push("parent.native_launch_id"); }
  const roleAllowed = Boolean(parent && child?.role && placementRoleAllowed(parent.role, child.role, child.actorActivation));
  if (parent && child?.role && !roleAllowed) missingFields.push("child.role-not-authorized");
  const request: PlacementDecideRequest = {
    schema: "smarty.placement.v1", operation: "decide", launch_id: seed.launchId, agent_name: seed.agentName,
    generation: seed.generation, mode: "enforced",
    assignment: { org_id: org, work_id: work },
    role, complexity, required_profile: profile,
    interaction: { class: originClass ?? "background-agent", origin_ref: safeOriginRef ?? missingValue("parent.origin.origin_ref") },
    requirements: {
      capabilities: [...new Set(seed.capabilities)], runtime: "pi-rpc", source_pin: sourcePin, memory_profile_ref: memoryProfile,
    },
    ...(seed.model || seed.thinking ? { requested: {
      ...(seed.model ? { model: seed.model } : {}), ...(seed.thinking ? { thinking: seed.thinking } : {}),
    } } : {}),
  };
  return { request, missingFields: [...new Set([...context.missingFields, ...missingFields])], trust, roleAllowed,
    ...(inheritedParentId ? { parentNativeLaunchId: inheritedParentId } : {}) };
};
