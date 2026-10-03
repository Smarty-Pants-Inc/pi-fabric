/** Exact spawn-bound addresses; never resolve an escalation target by role or name. */
export interface TaskReturnAddress {
  spawnerId: string;
  spawnerSessionId?: string;
  ancestors: string[];
  escalationTargets: string[];
}

const addresses = (source: string | undefined, name: string, sessionsOnly = false): string[] => {
  if (!source) return [];
  let value: unknown;
  try { value = JSON.parse(source); } catch { /* rejected below */ }
  if (!Array.isArray(value) || value.some(id => typeof id !== "string" || !id.trim() ||
      (sessionsOnly && (!id.trim().startsWith("session:") || !id.trim().slice(8))))) {
    throw new Error(`${name} must be a JSON array of exact ${sessionsOnly ? "session:<id>" : "participant id"} strings`);
  }
  return [...new Set((value as string[]).map(id => id.trim()))];
};

/** Snapshot once per runtime, so a later ambient environment change cannot retarget a send. */
export const readTaskReturnAddress = (env: NodeJS.ProcessEnv = process.env): TaskReturnAddress | undefined => {
  if (env.PI_FABRIC_TASK_PROCESS_CHILD !== "1" || !env.PI_FABRIC_PARENT_RUN?.trim() || env.PI_FABRIC_ACTOR_ID?.trim()) return undefined;
  const spawnerId = env.PI_FABRIC_SPAWNER_ID?.trim() || env.PI_FABRIC_MAIN_AGENT_ID?.trim();
  return {
    spawnerId: spawnerId ?? "",
    ...(env.PI_FABRIC_SPAWNER_SESSION_ID?.trim() ? { spawnerSessionId: env.PI_FABRIC_SPAWNER_SESSION_ID.trim() } : {}),
    ancestors: addresses(env.PI_FABRIC_SPAWNER_CHAIN, "PI_FABRIC_SPAWNER_CHAIN"),
    escalationTargets: addresses(env.PI_FABRIC_TASK_ESCALATION_TARGETS, "PI_FABRIC_TASK_ESCALATION_TARGETS", true),
  };
};

/** Manager-owned flag snapshot travels through the real worker, including a remote launcher. */
export const snapshotTaskReturnAddress = (
  spawnerId: string | undefined,
  spawnerSessionId: string | undefined,
  mainAgentId: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): TaskReturnAddress => {
  const inherited = readTaskReturnAddress(env);
  const address: TaskReturnAddress = {
    spawnerId: spawnerId || mainAgentId || "",
    ...(spawnerSessionId ? { spawnerSessionId } : {}),
    ancestors: [...new Set([
      ...(inherited ? [inherited.spawnerId, ...inherited.ancestors] : []),
      ...(mainAgentId ? [mainAgentId] : []),
    ].filter(Boolean))],
    escalationTargets: addresses(env.PI_FABRIC_TASK_ESCALATION_TARGETS, "PI_FABRIC_TASK_ESCALATION_TARGETS", true),
  };
  return address;
};

export const taskReturnAddressArguments = (
  spawnerId: string | undefined,
  spawnerSessionId: string | undefined,
  mainAgentId: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string[] => ["--task-return-address", JSON.stringify(snapshotTaskReturnAddress(spawnerId, spawnerSessionId, mainAgentId, env))];

/** Apply the bound snapshot before worker exec and again before the Pi subprocess exec. */
export const applyTaskReturnAddress = (env: NodeJS.ProcessEnv, args: readonly string[]): NodeJS.ProcessEnv => {
  const child = { ...env };
  const actor = args.some((arg, index) => index % 2 === 0 && arg === "--actor-id");
  if (actor) {
    delete child.PI_FABRIC_TASK_PROCESS_CHILD;
    delete child.PI_FABRIC_SPAWNER_ID;
    delete child.PI_FABRIC_SPAWNER_SESSION_ID;
    delete child.PI_FABRIC_SPAWNER_CHAIN;
    return child;
  }
  const index = args.findIndex((arg, index) => index % 2 === 0 && arg === "--task-return-address");
  if (index < 0) return child; // Older launchers retain their inherited root address.
  const address = JSON.parse(args[index + 1]!) as TaskReturnAddress;
  child.PI_FABRIC_TASK_PROCESS_CHILD = "1";
  child.PI_FABRIC_SPAWNER_ID = address.spawnerId;
  child.PI_FABRIC_SPAWNER_SESSION_ID = address.spawnerSessionId ?? "";
  child.PI_FABRIC_SPAWNER_CHAIN = JSON.stringify(address.ancestors);
  child.PI_FABRIC_TASK_ESCALATION_TARGETS = JSON.stringify(address.escalationTargets);
  return child;
};

export class TaskEscalationTargetError extends Error {
  readonly code = "FABRIC_TASK_ESCALATION_TARGET_DENIED";
  constructor(target: string, allowed: readonly string[]) {
    super(`Task agent send to Main ${target} refused; allowed targets: ${allowed.length ? allowed.join(", ") : "(none; missing spawn return address)"}. Report to agents.main() or explicitly allowlist the exact session with PI_FABRIC_TASK_ESCALATION_TARGETS. Nothing was delivered.`);
    this.name = "TaskEscalationTargetError";
  }
}

export const assertTaskMainTarget = (address: TaskReturnAddress | undefined, target: string): void => {
  if (!address) return; // Mains and durable actors are unaffected.
  const allowed = [...new Set([address.spawnerId, ...address.ancestors, ...address.escalationTargets].filter(Boolean))];
  if (!allowed.includes(target)) throw new TaskEscalationTargetError(target, allowed);
};
