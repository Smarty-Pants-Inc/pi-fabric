import { randomUUID } from "node:crypto";
import { throwIfAborted } from "../async-settlement.js";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricActorInfo, FabricActorRequest } from "../actors/types.js";
import {
  abandonResidentRequest,
  ResidentOutcomeUnknownError,
  readResidentRequestDecision,
  registerResidentCancellation,
  RESIDENT_HOST_FORMAT,
  residentHostStateNote,
  residentRoot,
  sleepUnlessAborted,
  type ResidentCommand,
  type ResidentCommandResponse,
  type ResidentHostOwner,
} from "./protocol.js";

const COMMAND_TIMEOUT_MS = 30_000;
const STATUS_POLL_MS = 100;

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

  async createActor(request: FabricActorRequest, signal?: AbortSignal): Promise<FabricActorInfo> {
    const response = await this.#send({
      format: RESIDENT_HOST_FORMAT,
      operation: "createActor",
      requestId: randomUUID(),
      rootId: this.#rootId,
      request,
      createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Resident host returned no actor from createActor");
    return response.actor;
  }

  async removeActor(id: string, signal?: AbortSignal): Promise<{ removed: true; pending?: string }> {
    const response = await this.#send({
      format: RESIDENT_HOST_FORMAT,
      operation: "removeActor",
      requestId: randomUUID(),
      rootId: this.#rootId,
      id,
      createdAt: Date.now(),
    }, signal);
    return { removed: true, ...(response.pending ? { pending: response.pending } : {}) };
  }

  async #send(command: ResidentCommand, signal?: AbortSignal): Promise<ResidentCommandResponse> {
    if (readJson<ResidentHostOwner>(this.#ownerPath)?.requestFence !== 1) {
      throw new Error("Root resident host lacks the abandonment fence; restart the resident host before retrying. No request was dispatched.");
    }
    throwIfAborted(signal);
    registerResidentCancellation(signal, this.#residencyDir, command);
    fs.mkdirSync(this.#requestsPath, { recursive: true });
    const responsePath = path.join(this.#responsesPath, `${command.requestId}.json`);
    try {
      writeJsonAtomic(path.join(this.#requestsPath, `${command.requestId}.json`), command);
      const deadline = Date.now() + this.commandTimeoutMs;
      while (Date.now() < deadline) {
        if (signal?.aborted) throw new Error("Resident host actor request was aborted");
        const response = readJson<ResidentCommandResponse>(responsePath);
        if (response?.format === RESIDENT_HOST_FORMAT && response.requestId === command.requestId) {
          fs.rmSync(responsePath, { force: true });
          if (!response.ok) throw new Error(response.error ?? "Resident host rejected actor request");
          if (command.operation === "createActor" && !response.actor) throw new Error("Resident host returned no created actor");
          return response;
        }
        const owner = readJson<{ pid?: number }>(this.#ownerPath);
        if (!owner?.pid) throw new Error("Root resident host exited during actor request");
        await sleepUnlessAborted(STATUS_POLL_MS, signal).catch(() => undefined);
      }
      const note = residentHostStateNote(this.#residencyDir);
      throw new Error(`Timed out waiting for resident host actor response (${command.operation})${note ? `: ${note}` : ""}`);
    } catch (error) {
      let decision;
      try {
        decision = abandonResidentRequest(this.#requestsPath, this.#responsesPath, command.requestId);
      } catch (fenceError) {
        let known;
        try { known = readResidentRequestDecision(this.#residencyDir, command.requestId); } catch { /* unreadable fence */ }
        throw new ResidentOutcomeUnknownError(command, known, fenceError);
      }
      if (decision.state === "committed") throw new ResidentOutcomeUnknownError(command, decision, error);
      throw error;
    }
  }
}
