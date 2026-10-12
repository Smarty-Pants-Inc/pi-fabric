import { randomUUID } from "node:crypto";
import { ResidentRequestExpiredError } from "./request-expiry.js";
import { FabricModelDeniedError } from "../core/model-policy.js";
import { ActorSessionResetCancelledError } from "../actors/session-reset-error.js";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { throwIfAborted } from "../async-settlement.js";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricActorInfo, FabricActorCreateRequest } from "../actors/types.js";
import {
  abandonResidentRequest,
  acknowledgeResidentResponse,
  residentCommandForOwner,
  ResidentActorAuthorizationError,
  ResidentCommandUnsupportedError,
  assertResidentCommandSupported,
  prepareResidentCreationCommand,
  type ResidentHostOwner,
  assertResidentActorToolCeiling,
  type ResidentActorCaller,
  ResidentOutcomeUnknownError,
  readResidentRequestDecision,
  registerResidentCancellation,
  residentRequestExpiredOutcome,
  RESIDENT_HOST_FORMAT,
  RESIDENT_ACTOR_COMMAND_FORMAT,
  residentHostStateNote,
  residentRoot,
  type ResidentCommand,
  type ResidentCommandResponse,
  type ResidentActorMutation,
} from "./protocol.js";

const COMMAND_TIMEOUT_MS = 30_000;

const readJson = <T>(filePath: string): T | undefined => {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
  } catch {
    return undefined;
  }
};

/**
 * Durable actor lifecycle proxy for nested Fabric runtimes.
 * Communication remains on the Fabric control plane; only authoritative
 * registry mutations are routed through the root resident host.
 */
export class ResidentActorClient {
  readonly #rootId: string;
  readonly #toolCeiling = readChildToolAllowlist();
  readonly #requestsPath: string;
  readonly #responsesPath: string;
  readonly #ownerPath: string;
  readonly #residencyDir: string;

  constructor(meshRoot: string, rootId: string, readonly commandTimeoutMs = COMMAND_TIMEOUT_MS) {
    this.#rootId = rootId;
    const residencyDir = residentRoot(meshRoot, rootId);
    this.#residencyDir = residencyDir;
    this.#requestsPath = path.join(residencyDir, "requests");
    this.#responsesPath = path.join(residencyDir, "responses");
    this.#ownerPath = path.join(residencyDir, "owner.json");
  }

  static fromEnv(): ResidentActorClient | undefined {
    const rootId = process.env.PI_FABRIC_MAIN_AGENT_ID;
    const meshRoot = process.env.PI_FABRIC_MESH_ROOT;
    if (!rootId || !meshRoot) return undefined;
    return new ResidentActorClient(meshRoot, rootId);
  }

  isLive(): boolean {
    const owner = readJson<{ pid?: number }>(this.#ownerPath);
    if (!owner?.pid || !Number.isSafeInteger(owner.pid) || owner.pid <= 0) return false;
    try { process.kill(owner.pid, 0); return true; }
    catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
  }

  async setActor(mutation: ResidentActorMutation, signal?: AbortSignal, caller?: ResidentActorCaller): Promise<FabricActorInfo> {
    // Nested proxies cannot manufacture the Main's control identity. A genuine
    // Main fallback must supply the identity captured by its provider.
    if (this.#toolCeiling !== undefined && caller) caller = { ...caller, toolCeiling: [...this.#toolCeiling] };
    if (mutation.operation === "setTools") assertResidentActorToolCeiling(mutation.tools, caller?.toolCeiling);
    const response = await this.#send({
      ...mutation, ...(caller ? { caller } : {}), format: RESIDENT_ACTOR_COMMAND_FORMAT, requestId: randomUUID(),
      rootId: this.#rootId, createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Resident host returned no actor from setter");
    return response.actor;
  }

  async actorStatus(id: string, signal?: AbortSignal): Promise<FabricActorInfo> {
    const response = await this.#send({
      format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "actorStatus", id,
      requestId: randomUUID(), rootId: this.#rootId, createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Resident host returned no actor status");
    return response.actor;
  }

  async actors(signal?: AbortSignal): Promise<FabricActorInfo[]> {
    const response = await this.#send({
      format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "actors", requestId: randomUUID(),
      rootId: this.#rootId, createdAt: Date.now(),
    }, signal);
    if (!response.actors) throw new Error("Resident host returned no actors");
    return response.actors;
  }

  async createActor(request: FabricActorCreateRequest, signal?: AbortSignal): Promise<FabricActorInfo> {
    const { idempotencyKey, ...creationRequest } = request;
    const response = await this.#send({
      format: RESIDENT_HOST_FORMAT,
      operation: "createActor",
      ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
      requestId: randomUUID(),
      rootId: this.#rootId,
      request: creationRequest,
      createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Resident host returned no actor from createActor");
    return response.actor;
  }

  async removeActor(id: string, signal?: AbortSignal): Promise<{ removed: true; pending?: string; cleaned?: boolean }> {
    const response = await this.#send({
      format: RESIDENT_HOST_FORMAT,
      operation: "removeActor",
      requestId: randomUUID(),
      rootId: this.#rootId,
      id,
      createdAt: Date.now(),
    }, signal);
    return { removed: true, ...(response.pending === undefined ? {} : { pending: response.pending }),
      ...(response.cleaned === undefined ? {} : { cleaned: response.cleaned }) };
  }

  /** Same-user operator control; the resident executor requires root confirmation and vetoes live leases. */
  async operatorActor(action: "stop" | "remove", id: string,
    options: { dryRun?: boolean; confirmDeadRoot?: string } = {}, signal?: AbortSignal): Promise<ResidentCommandResponse> {
    return this.#send({ format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "operatorActor",
      action, id, ...options, requestId: randomUUID(), rootId: this.#rootId, createdAt: Date.now() }, signal);
  }

  async #send(command: ResidentCommand, signal?: AbortSignal): Promise<ResidentCommandResponse> {
    if (!this.isLive()) throw new Error("Root resident host is not live");
    const owner = readJson<ResidentHostOwner>(this.#ownerPath);
    if (!owner) throw new Error("Root resident host is not live");
    assertResidentCommandSupported(owner, command.operation);
    if (owner.requestFence !== 1) {
      throw new Error("Root resident host lacks the abandonment fence; restart the resident host before retrying. No request was dispatched.");
    }
    command = prepareResidentCreationCommand(owner, command);
    throwIfAborted(signal);
    command = residentCommandForOwner(command, owner);
    registerResidentCancellation(signal, this.#residencyDir, command);
    fs.mkdirSync(this.#requestsPath, { recursive: true });
    fs.mkdirSync(this.#responsesPath, { recursive: true });
    const responsePath = path.join(this.#responsesPath, `${command.requestId}.json`);
    // Event-driven wait (smarty-dev#7817): watch the responses and the owner record's directory
    // BEFORE the request is written, so no response or host exit can slip between write and wait.
    // Wake only on those events, one deadline timer and the abort signal; no polling fallback.
    let dirty = true;
    let wake: (() => void) | undefined;
    const notify = (): void => { dirty = true; wake?.(); };
    const watchers: fs.FSWatcher[] = [];
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    let expired = false;
    const onAbort = (): void => notify();
    const cleanup = (): void => {
      for (const watcher of watchers) watcher.close();
      if (deadlineTimer !== undefined) clearTimeout(deadlineTimer);
      signal?.removeEventListener("abort", onAbort);
    };
    try {
      for (const directory of [this.#responsesPath, path.dirname(this.#ownerPath)]) {
        const watcher = fs.watch(directory, { persistent: false }, notify);
        watcher.on("error", notify);
        watchers.push(watcher);
      }
    } catch (error) {
      cleanup();
      throw new Error(`Cannot watch the resident host's responses (${error instanceof Error ? error.message : String(error)}); no request was dispatched`);
    }
    try {
      writeJsonAtomic(path.join(this.#requestsPath, `${command.requestId}.json`), command);
      // Unref'd as before: a standalone caller (the fabric-actors CLI) keeps its own ref'd deadline timer.
      deadlineTimer = setTimeout(() => { expired = true; notify(); }, this.commandTimeoutMs);
      deadlineTimer.unref?.();
      signal?.addEventListener("abort", onAbort, { once: true });
      for (;;) {
        if (signal?.aborted) throw new Error("Resident host actor request was aborted");
        if (!dirty) {
          if (expired) break;
          await new Promise<void>(resolve => { wake = resolve; if (dirty || expired || signal?.aborted) resolve(); });
          wake = undefined;
          continue;
        }
        dirty = false;
        const response = readJson<ResidentCommandResponse>(responsePath);
        if (response?.format === RESIDENT_HOST_FORMAT && response.requestId === command.requestId) {
          if (acknowledgeResidentResponse(this.#residencyDir, response, Date.now(), command.format)) fs.rmSync(responsePath, { force: true });
          if (!response.ok) {
            if (command.operation === "resetSession" && response.errorCode === "ACTOR_SESSION_RESET_CANCELLED") {
              throw new ActorSessionResetCancelledError(command.id, response.error, command.requestId);
            }
            if (response.errorCode === "RESIDENT_REQUEST_EXPIRED") throw new ResidentRequestExpiredError(command.requestId);
            if (response.errorCode === "RESIDENT_ACTOR_FORBIDDEN") throw new ResidentActorAuthorizationError(response.error);
            if (response.errorCode === "RESIDENT_COMMAND_UNSUPPORTED") throw new ResidentCommandUnsupportedError(response.error);
            if (response.errorCode === "FABRIC_MODEL_DENIED" && typeof response.modelDenied?.model === "string") {
              throw new FabricModelDeniedError(response.modelDenied.model,
                typeof response.modelDenied.replacement === "string" ? response.modelDenied.replacement : undefined);
            }
            throw new Error(response.error ?? "Resident host rejected actor request");
          }
          if (command.operation === "createActor" && !response.actor) throw new Error("Resident host returned no created actor");
          return response;
        }
        const owner = readJson<{ pid?: number }>(this.#ownerPath);
        if (!owner?.pid) throw new Error("Root resident host exited during actor request");
      }
      const note = residentHostStateNote(this.#residencyDir);
      throw new Error(`Timed out waiting for resident host actor response (${command.operation})${note ? `: ${note}` : ""}`);
    } catch (error) {
      // This acknowledged terminal response is known, unlike a lost post-commit reply.
      if (error instanceof ActorSessionResetCancelledError) throw error;
      if (error instanceof ResidentRequestExpiredError) throw residentRequestExpiredOutcome(this.#residencyDir, command, signal);
      let decision;
      try {
        decision = abandonResidentRequest(this.#requestsPath, this.#responsesPath, command.requestId, command.format);
      } catch (fenceError) {
        if (fenceError instanceof ResidentRequestExpiredError) throw residentRequestExpiredOutcome(this.#residencyDir, command, signal);
        let known;
        try { known = readResidentRequestDecision(this.#residencyDir, command.requestId); } catch { /* unreadable fence */ }
        throw new ResidentOutcomeUnknownError(command, known, fenceError, signal);
      }
      if (decision.state === "committed") throw new ResidentOutcomeUnknownError(command, decision, error, signal);
      throw error;
    } finally { cleanup(); }
  }
}
