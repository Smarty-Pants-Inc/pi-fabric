import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { fabricTurnProvenance } from "../fabric-provenance.js";
import { normalizeAgentCapabilityTokens } from "../host-compatibility.js";
import { normalizeAgentRunRequest } from "./request.js";
import { checkWritePolicyRequest, resolveChildWritePolicy } from "./child-env.js";
import { terminalAgentStatuses } from "./lifecycle.js";
import { RemoteTaskError } from "./remote-task-errors.js";
import type { AgentManager } from "./manager.js";
import type { CompletionRecipient } from "./completion-journal.js";
import type { AgentHandleInfo, AgentRunRecord, AgentRunRequest, AgentRunResult } from "./types.js";
import type { FabricAgentConfig } from "../config.js";
import type { FabricControlCommand, FabricControlAcceptance, FabricControlOperation, FabricControlPlane } from "../topology/control-plane.js";
import type { FabricHostRecord, FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";
import type { MeshIdentity } from "../mesh/store.js";

const hash = (text: string): string => createHash("sha256").update(text).digest("hex");
const digestOf = (value: unknown): string => hash(JSON.stringify(value, (_key, item) =>
  object(item) ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item));
const read = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
};
const save = (file: string, value: unknown): void => writeJsonAtomic(file, value, { durable: true });
const refused = (message: string): never => { throw new RemoteTaskError("FABRIC_REMOTE_TASK_POLICY_REFUSED", message); };
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);

/** The sender envelope AND live directory grant, not a request's role/root/name. */
export const assertRemoteTaskLead = (from: MeshIdentity, caller: FabricParticipantInfo | undefined): FabricParticipantInfo => {
  if (from.kind !== "main" || !caller || caller.kind !== "root" || caller.stale || caller.interactive === false ||
    caller.id !== from.id || caller.rootId !== from.id || caller.ownerIdentityId !== from.id ||
    !caller.sessionId || caller.id !== `session:${caller.sessionId}` || (from.sessionId !== undefined && from.sessionId !== caller.sessionId) ||
    (caller.role !== "project-agent" && caller.role !== "org-agent")) {
    return refused("Only a verified live lead Main may spawn remote tasks; tasks, actors and worktree Mains cannot add a hop");
  }
  return caller;
};

// The wire never carries seeds, launch paths, provenance, or ambient host authority.
const fields = new Set(["task", "name", "runner", "kernel", "model", "modelReason", "thinking", "thinkingBounds", "tools", "timeoutMs",
  "extensions", "cwd", "needs", "requires", "readOnly", "writableRoots", "shell", "schema", "replyTool", "systemPrompt", "nice", "bashIdleSeconds"]);
export const remoteTaskRequest = (request: AgentRunRequest): AgentRunRequest => {
  if (request.recursive || request.worktree || request.worktreeSetup !== undefined || request.sessionSeed || request.forkSeed ||
    request.actorId || request.actorName || request.sessionFile || request.residentStartupProbe || request.scope || request.inheritedScope ||
    request.routeDecision || request.inheritedSessionPins?.length || request.images?.length || request.residency === "durable" ||
    (request.transport !== undefined && request.transport !== "process" && request.transport !== "auto")) {
    return refused("Remote spawn supports an unseeded, nonrecursive, session-owned process task without ambient scope/pins");
  }
  return Object.fromEntries(Object.entries(request).filter(([field]) => fields.has(field))) as unknown as AgentRunRequest;
};
const recipientOf = (caller: FabricParticipantInfo): CompletionRecipient => ({
  rootId: caller.id, sessionId: caller.sessionId!, cwd: caller.cwd ?? "", projectRoot: caller.projectRoot ?? caller.cwd ?? "",
  name: caller.name, role: caller.role, startedAt: caller.startedAt,
});
interface ProxyRecord {
  format: 1; key: string; digest: string; owner: FabricHostRecord; recipient: CompletionRecipient;
  request: AgentRunRequest; handle?: AgentHandleInfo; result?: AgentRunResult;
}

/** Durable session-local proxy intents; restart never selects a new owner for an old key. */
export class RemoteTaskClient {
  readonly directory: string;
  readonly #pending = new Map<string, Promise<AgentHandleInfo>>();
  constructor(readonly meshRoot: string, readonly from: MeshIdentity, readonly hostId: string,
    readonly participants: FabricParticipantSource, readonly control: FabricControlPlane,
    readonly onComplete: (result: AgentRunResult, recipient: CompletionRecipient) => void = () => {},
    readonly onConsumed: (id: string) => void = () => {}) {
    this.directory = path.join(meshRoot, "remote-tasks", "callers", hash(from.id));
  }
  #intent(key: string): string { return path.join(this.directory, `intent-${hash(key)}.json`); }
  #index(id: string): string { return path.join(this.directory, `agent-${hash(id)}.json`); }
  #record(id: string): ProxyRecord | undefined {
    const index = read<{ key: string }>(this.#index(id));
    return index && read<ProxyRecord>(this.#intent(index.key));
  }
  has(id: string): boolean { return this.#record(id)?.handle?.id === id || this.#record(id)?.result?.id === id; }

  spawn(input: AgentRunRequest, signal?: AbortSignal): Promise<AgentHandleInfo> {
    const caller = assertRemoteTaskLead(this.from, this.participants.self());
    const request = remoteTaskRequest(input);
    const key = input.idempotencyKey ?? randomUUID();
    if (!key || key.length > 256) return Promise.reject(new RemoteTaskError("FABRIC_REMOTE_TASK_POLICY_REFUSED", "idempotencyKey must be 1-256 characters"));
    const digest = digestOf(request);
    const existing = read<ProxyRecord>(this.#intent(key));
    if (existing && existing.digest !== digest) return Promise.reject(new RemoteTaskError("FABRIC_REMOTE_TASK_POLICY_REFUSED", "Retry key has different spawn settings"));
    const pending = this.#pending.get(key);
    if (pending) return pending;
    const launched = this.#spawn(key, digest, request, caller, existing, signal);
    this.#pending.set(key, launched);
    void launched.finally(() => this.#pending.delete(key)).catch(() => undefined);
    return launched;
  }
  async #spawn(key: string, digest: string, request: AgentRunRequest, caller: FabricParticipantInfo, existing?: ProxyRecord, signal?: AbortSignal): Promise<AgentHandleInfo> {
    if (existing?.handle) return existing.handle;
    const needs = normalizeAgentCapabilityTokens(request.needs) ?? [];
    const candidates = (this.participants.taskOwners?.() ?? []).filter(owner => needs.every(need => owner.taskCapabilities?.includes(need)));
    const owner = existing?.owner ?? candidates.filter(owner => !owner.stale).sort((a, b) => a.id.localeCompare(b.id))[0];
    if (!owner) throw new RemoteTaskError(candidates.length ? "FABRIC_REMOTE_TASK_BRIDGE_STALE" : "FABRIC_REMOTE_TASK_NO_OWNER",
      `No live capable resident owner for needs [${needs.join(", ")}]`);
    const record: ProxyRecord = existing ?? { format: 1, key, digest, owner, recipient: recipientOf(caller), request };
    save(this.#intent(key), record); // before publication; exact route survives either-side restart
    const handle = await this.#request<AgentHandleInfo>(record, "spawn", { key, request, returnTo: caller.id, sessionId: caller.sessionId }, signal, key);
    // Completion can arrive before the spawn ACK. Merge rather than overwrite it.
    const latest = read<ProxyRecord>(this.#intent(key)) ?? record;
    save(this.#intent(key), { ...latest, handle });
    save(this.#index(handle.id), { key });
    return handle;
  }
  async #request<T>(record: ProxyRecord, operation: FabricControlOperation, data: unknown, signal?: AbortSignal, key?: string): Promise<T> {
    try {
      const owner = operation === "spawn" ? record.owner : this.participants.taskOwners?.().find(candidate =>
        candidate.id === record.owner.id && candidate.identity.id === record.owner.identity.id && candidate.remoteHost === record.owner.remoteHost && !candidate.stale);
      if (!owner) throw new RemoteTaskError("FABRIC_REMOTE_TASK_BRIDGE_STALE", `Spawn-bound owner unavailable for ${record.key}`);
      return await this.control.requestResult<T>(owner.id, owner.id, operation,
        { data, ownerIncarnation: owner.ownerIncarnation }, owner.identity.id,
        { routedRemoteHost: owner.remoteHost ?? null, timeoutMs: 60_000, ...(signal ? { signal } : {}), ...(key ? { idempotencyKey: key } : {}) });
    } catch (error) {
      if (error instanceof RemoteTaskError) throw error;
      throw new RemoteTaskError("FABRIC_REMOTE_TASK_BRIDGE_STALE", `Remote task control unavailable; key=${record.key}. Re-observe with unchanged key/settings, never respawn under a new key. ${String(error)}`, { cause: error });
    }
  }
  async observe(id: string, operation: "taskStatus" | "taskWait" | "taskResult", timeoutMs: number, signal?: AbortSignal,
    deferConsumption?: (consume: () => void, abandon?: () => void) => void): Promise<AgentRunRecord | AgentHandleInfo> {
    const record = this.#record(id);
    if (!record) return refused(`Unknown remote task proxy ${id}`);
    const result = record.result ?? await this.#request<AgentRunRecord>(record, operation, { id, key: record.key, timeoutMs }, signal);
    if ("text" in result && terminalAgentStatuses.has(result.status) && !("waitTimedOut" in result)) {
      save(this.#intent(record.key), { ...record, result });
      this.onComplete(result as AgentRunResult, record.recipient);
      if (deferConsumption) deferConsumption(() => this.onConsumed(id));
      else this.onConsumed(id);
    }
    return result;
  }
  async stop(id: string, signal?: AbortSignal): Promise<unknown> {
    const record = this.#record(id);
    if (!record) return refused(`Unknown remote task proxy ${id}`);
    return this.#request(record, "taskStop", { id, key: record.key }, signal);
  }
  /** Called only by the receiving Main's control handler; owner/source binding survives withdrawal. */
  acceptCompletion(command: FabricControlCommand, from: MeshIdentity, verification?: "mesh" | "bridge"): FabricControlAcceptance {
    if (command.targetId !== this.from.id || (verification !== "mesh" && verification !== "bridge") || !object(command.data) ||
      typeof command.data.key !== "string" || !object(command.data.result)) return { accepted: false, error: "Invalid remote completion", errorCode: "FABRIC_REMOTE_TASK_POLICY_REFUSED" };
    const record = read<ProxyRecord>(this.#intent(command.data.key));
    const result = command.data.result as unknown as AgentRunResult;
    if (!record || record.recipient.rootId !== this.from.id || record.owner.identity.id !== from.id || from.kind !== "agent" ||
      command.senderRemoteHost !== record.owner.remoteHost ||
      typeof result.id !== "string" || !terminalAgentStatuses.has(result.status) || typeof result.text !== "string" ||
      (record.handle && record.handle.id !== result.id) || (record.result && record.result.id !== result.id)) {
      return { accepted: false, error: "Completion does not match the spawn-bound owner/caller", errorCode: "FABRIC_REMOTE_TASK_POLICY_REFUSED" };
    }
    save(this.#intent(record.key), { ...record, result });
    save(this.#index(result.id), { key: record.key });
    // The callback durably journals before it enqueues the normal local completion message.
    this.onComplete(result, record.recipient);
    return { accepted: true, result: true };
  }
}

interface OwnerIntent { format: 1; digest: string; handle?: AgentHandleInfo }
interface OwnerRecord {
  format: 1; id: string; key: string; callerId: string; callerSessionId: string; recipient: CompletionRecipient;
  callerOwnerHostId: string; callerOwnerIdentityId: string; callerRemoteHost?: string; callerIncarnation?: string;
  result?: AgentRunResult; delivered?: boolean;
}
/** The existing resident's receiver. No transport, launcher or model backend of its own. */
export class RemoteTaskOwner {
  readonly directory: string;
  #flushing: Promise<void> | undefined;
  readonly #pendingCompletions = new Set<string>();
  readonly #spawns = new Map<string, Promise<AgentHandleInfo>>();
  get pendingCompletions(): boolean { return this.#pendingCompletions.size > 0; }
  constructor(readonly root: string, readonly hostId: string, readonly manager: AgentManager,
    readonly participants: FabricParticipantSource, readonly control: FabricControlPlane, readonly config: FabricAgentConfig) {
    this.directory = path.join(root, "remote-tasks");
    if (fs.existsSync(this.directory)) for (const file of fs.readdirSync(this.directory).filter(file => /^[a-f0-9]{64}\.json$/.test(file))) {
      const record = read<OwnerRecord>(path.join(this.directory, file));
      if (record?.result && !record.delivered) this.#pendingCompletions.add(record.id);
    }
  }
  #file(id: string): string { return path.join(this.directory, `${hash(id)}.json`); }
  owns(id: string): boolean { return read<OwnerRecord>(this.#file(id))?.id === id; }
  remember(result: AgentRunResult): boolean {
    const record = read<OwnerRecord>(this.#file(result.id));
    if (!record) return false;
    save(this.#file(result.id), { ...record, result });
    if (!record.delivered) this.#pendingCompletions.add(result.id);
    return true;
  }
  async accept(command: FabricControlCommand, from: MeshIdentity, signal?: AbortSignal, verification?: "mesh" | "bridge"): Promise<FabricControlAcceptance> {
    try {
      if (command.targetId !== this.hostId || (verification !== "mesh" && verification !== "bridge") || !object(command.data)) return refused("Remote tasks require the addressed resident and a verified sender");
      const caller = assertRemoteTaskLead(from, this.participants.get(from.id, undefined, { fresh: true }));
      if ((verification === "bridge") !== (caller.remoteHost !== undefined) || command.senderRemoteHost !== caller.remoteHost) return refused("Sender's verified origin does not match its owner directory");
      if (command.operation === "spawn") return { accepted: true, result: await this.#spawn(command, from, caller, signal) };
      const { id, key } = command.data;
      if (typeof id !== "string" || typeof key !== "string") return refused("Remote task control requires its id and spawn key");
      const record = read<OwnerRecord>(this.#file(id));
      if (!record || record.callerId !== caller.id || record.callerSessionId !== caller.sessionId || record.key !== key) return refused("Remote task is not owned by this caller/key");
      if (record.result) return { accepted: true, result: command.operation === "taskStop" ? { stopped: true, status: record.result.status } : record.result };
      try { this.manager.status(id); } catch { throw new RemoteTaskError("FABRIC_REMOTE_TASK_LOST", `Owner restarted without native custody for ${id}; do not replay`); }
      if (command.operation === "taskStop") {
        await this.manager.stop(id);
        return { accepted: true, result: { stopped: this.manager.isSettled(id), ...this.manager.status(id) } };
      }
      const timeoutMs = typeof command.data.timeoutMs === "number" ? Math.max(1, Math.min(60_000, command.data.timeoutMs)) : 60_000;
      if (command.operation === "taskWait" || command.operation === "taskResult") {
        try {
          const result = await this.manager.wait(id, { timeoutMs, ...(signal ? { signal } : {}) });
          this.remember(result);
          return { accepted: true, result };
        } catch (error) {
          if (this.manager.isSettled(id)) throw error;
          return { accepted: true, result: { ...this.manager.status(id), waitTimedOut: true } };
        }
      }
      if (command.operation !== "taskStatus") return refused("Unsupported remote task operation");
      if (this.manager.isSettled(id)) {
        const result = await this.manager.wait(id);
        this.remember(result);
        return { accepted: true, result };
      }
      return { accepted: true, result: { ...this.manager.status(id), waitTimedOut: true } };
    } catch (error) {
      return { accepted: false, error: error instanceof Error ? error.message : String(error),
        errorCode: error instanceof RemoteTaskError ? error.code : "FABRIC_REMOTE_TASK_POLICY_REFUSED" };
    }
  }
  async #spawn(command: FabricControlCommand, from: MeshIdentity, caller: FabricParticipantInfo, signal?: AbortSignal): Promise<AgentHandleInfo> {
    const data = command.data as Record<string, unknown>;
    if (data.returnTo !== caller.id || data.sessionId !== caller.sessionId || typeof data.key !== "string" || !data.key || data.key.length > 256 || !object(data.request)) return refused("Remote spawn return address must be the caller's exact session");
    if (Object.keys(data.request).some(field => !fields.has(field))) return refused("Remote request contains non-public launch authority");
    const intentPath = path.join(this.directory, "intents", `${hash(`${caller.id}\0${data.key}`)}.json`);
    const digest = digestOf(data.request);
    const previous = read<OwnerIntent>(intentPath);
    if (previous && previous.digest !== digest) return refused("Remote spawn key has different settings");
    if (previous?.handle) return previous.handle;
    const pending = this.#spawns.get(intentPath);
    if (pending) return pending;
    if (previous) throw new RemoteTaskError("FABRIC_REMOTE_TASK_LOST", "Owner restarted during spawn admission; native outcome is unknown, never replay this key");
    const admitted = this.#admitSpawn(command, from, caller, intentPath, digest, signal);
    this.#spawns.set(intentPath, admitted);
    void admitted.finally(() => this.#spawns.delete(intentPath)).catch(() => undefined);
    return admitted;
  }
  async #admitSpawn(command: FabricControlCommand, from: MeshIdentity, caller: FabricParticipantInfo, intentPath: string, digest: string, signal?: AbortSignal): Promise<AgentHandleInfo> {
    const data = command.data as Record<string, unknown>;
    const request = normalizeAgentRunRequest(data.request as Record<string, unknown>, this.config);
    if ((request.runner ?? this.config.runner) !== "pi") return refused("Remote tasks require the Pi process runner");
    const needs = normalizeAgentCapabilityTokens(request.needs) ?? [];
    if (!needs.length || needs.includes("local") || needs.some(need => !this.config.hostCapabilities?.includes(need))) return refused("Resident cannot satisfy the requested host capabilities");
    checkWritePolicyRequest(request);
    const policy = this.config.remoteSpawnPolicy ?? { writableRoots: [], shell: "deny" as const, tools: this.config.defaultTools };
    const tools = request.tools ?? policy.tools;
    if (tools.some(tool => !policy.tools.includes(tool))) return refused("Remote task tools exceed the host ceiling");
    if (request.cwd !== undefined && (!path.isAbsolute(request.cwd) || !fs.statSync(request.cwd).isDirectory())) return refused("Remote cwd must be an existing absolute path on the execution host");
    const cwd = request.cwd ?? fs.mkdtempSync(path.join(os.tmpdir(), "fabric-remote-task-"));
    const baseline = { readOnly: policy.writableRoots.length === 0, writableRoots: policy.writableRoots.map(root => fs.realpathSync(root)), shell: policy.shell };
    if (request.writableRoots?.some(root => !path.isAbsolute(root) || !fs.existsSync(root))) return refused("Remote writable roots must already exist on the execution host");
    const writes = resolveChildWritePolicy({ readOnly: request.readOnly ?? baseline.readOnly, writableRoots: request.writableRoots ?? baseline.writableRoots, shell: request.shell ?? "deny" }, baseline, cwd)!;
    const returnAddress = { spawnerId: caller.id, spawnerSessionId: caller.sessionId!, ancestors: [caller.id], escalationTargets: [] };
    const recipient = recipientOf(caller);
    const revalidate = (): void => {
      const current = assertRemoteTaskLead(from, this.participants.get(from.id, undefined, { fresh: true }));
      if (current.ownerHostId !== caller.ownerHostId || current.ownerIdentityId !== caller.ownerIdentityId || current.remoteHost !== caller.remoteHost || current.ownerIncarnation !== caller.ownerIncarnation) return refused("Caller ownership changed before remote launch");
    };
    save(intentPath, { format: 1, digest } satisfies OwnerIntent); // no replay after an interrupted native admission
    const handle = await this.manager.spawn({ ...request, ...writes, cwd, tools, transport: "process", residency: "session", needs: [...needs, "local"],
      provenance: fabricTurnProvenance(from, "actor", caller.remoteHost ? "bridge" : "mesh", command.principal) }, signal,
      () => { revalidate(); return true; }, id => {
        revalidate();
        const record: OwnerRecord = { format: 1, id, key: data.key as string, callerId: caller.id, callerSessionId: caller.sessionId!, recipient,
          callerOwnerHostId: caller.ownerHostId, callerOwnerIdentityId: caller.ownerIdentityId,
          ...(caller.remoteHost ? { callerRemoteHost: caller.remoteHost } : {}), ...(caller.ownerIncarnation ? { callerIncarnation: caller.ownerIncarnation } : {}) };
        save(this.#file(id), record); // allocated native id retained before launching
      }, undefined, undefined, undefined, returnAddress, recipient);
    this.manager.detachSignal(handle.id);
    save(intentPath, { format: 1, digest, handle } satisfies OwnerIntent);
    return handle;
  }
  /** Reuses the resident's delivery retry lifecycle; no new polling/transport. */
  flushCompletions(): Promise<void> {
    return this.#flushing ??= this.#flush().finally(() => { this.#flushing = undefined; });
  }
  async #flush(): Promise<void> {
    for (const id of [...this.#pendingCompletions].slice(0, 32)) {
      const record = read<OwnerRecord>(this.#file(id));
      if (!record?.result || record.delivered) { this.#pendingCompletions.delete(id); continue; }
      // Root names and cwd never retarget a completion. A reload may re-resolve the
      // same exact session's current activation, but never another session/owner.
      const caller = this.participants.get(record.callerId, undefined, { fresh: true });
      if (!caller || caller.sessionId !== record.callerSessionId || caller.stale) throw new RemoteTaskError("FABRIC_REMOTE_TASK_BRIDGE_STALE", `Completion remains retained for ${record.callerId}`);
      await this.control.requestResult<boolean>(caller.ownerHostId, caller.id, "taskComplete",
        { data: { key: record.key, result: record.result }, ownerIncarnation: caller.ownerIncarnation }, caller.ownerIdentityId,
        // Control receipts retain their claiming activation. A reloaded caller needs
        // a new control key; its durable completion journal still deduplicates the result.
        { routedRemoteHost: caller.remoteHost ?? null, idempotencyKey: `remote-completion:${record.id}:${caller.ownerIncarnation ?? "legacy"}`, timeoutMs: 60_000 });
      save(this.#file(record.id), { ...record, delivered: true });
      this.#pendingCompletions.delete(record.id);
    }
  }
}
