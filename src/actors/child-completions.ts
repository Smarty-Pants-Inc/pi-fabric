import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { AgentRunResult, AgentSpawner } from "../agents/types.js";

export type ActorChildResult = Pick<AgentRunResult, "id" | "name" | "status" | "text" | "error" | "startedAt" | "finishedAt">;
export interface ActorChildCompletion {
  format: 1;
  spawner: AgentSpawner;
  result: ActorChildResult;
}
const ID = /^[a-f0-9]{32}$/;
const STORED_FILE = /^([a-f0-9]{32})(?:\.result\.json|\.json|\.receipt|\.live-receipt)$/;

/**
 * Write-ahead inbox shared by an actor activation and its authoritative owner.
 * Only unread outcomes retain a full result. A handoff receipt transfers ownership
 * to the serial mailbox; that result lives until an activation consumes its context.
 */
export class ActorChildCompletionStore {
  readonly directory: string;
  // A completed spawning activation cannot add native live receipts later. Check
  // each eligible envelope once, even when mailbox/receipt I/O needs many retries.
  readonly #checked = new Map<string, boolean>();
  readonly #consumed = new Set<string>();
  constructor(readonly sessionFile: string) {
    this.directory = path.join(path.dirname(sessionFile), "child-completions");
  }

  enqueue(result: AgentRunResult, spawner: AgentSpawner, notify = true): void {
    if (!ID.test(result.id) || spawner.kind !== "actor") throw new Error("Invalid actor child completion identity");
    if (this.#consumed.has(result.id) || this.received(result.id) || fs.existsSync(this.#file(result.id))) return;
    writeJsonAtomic(this.resultFile(result.id), { ...result, spawner }, { durable: true });
    if (!notify) return; // Unread, but no automatic mailbox activation was requested.
    writeJsonAtomic(this.#file(result.id), {
      format: 1, spawner,
      result: {
        id: result.id, name: result.name, status: result.status,
        text: result.text.slice(0, 4000), startedAt: result.startedAt,
        ...(result.finishedAt !== undefined ? { finishedAt: result.finishedAt } : {}),
        ...(result.error !== undefined ? { error: result.error.slice(0, 4000) } : {}),
      },
    } satisfies ActorChildCompletion, { durable: true });
  }

  acknowledge(id: string, options: { handoff?: boolean } = {}): void {
    if (!ID.test(id)) return;
    this.consume(id, options);
    fs.rmSync(this.#file(id), { force: true });
    this.#checked.delete(id);
  }

  /** Shared live/mailbox receipt, durable before an activation can observe it. */
  consume(id: string, options: { handoff?: boolean } = {}): void {
    if (!ID.test(id)) return;
    if (!this.received(id)) writeJsonAtomic(this.#receipt(id), { id, acknowledgedAt: Date.now() }, { durable: true });
    if (!options.handoff) this.releaseResult(id);
  }

  /** Preparation checks recoverability, but must not claim or remove an outcome. */
  prepareLive(id: string): void {
    fs.accessSync(this.resultFile(id), fs.constants.R_OK);
  }

  /** One atomic claim for the entire prepared batch, immediately before sending. */
  consumeLiveBatch(ids: string[]): void {
    if (ids.some((id) => !ID.test(id))) throw new Error("Invalid actor child completion id");
    const batchId = ids[0];
    if (!batchId) return;
    // Staged receipts are unread until the batch marker commits. No shared mutable
    // ledger: owner pruning and a new activation cannot overwrite another batch.
    for (const id of ids) {
      if (!this.received(id)) writeJsonAtomic(this.#receipt(id), { id, batchId, acknowledgedAt: Date.now() }, { durable: true });
    }
    writeJsonAtomic(this.#batchReceipt(batchId), { ids }, { durable: true });
  }

  #batchReceipt(id: string): string { return path.join(this.directory, `${id}.live-receipt`); }

  /** A foreground result was durably consumed: cleanup cannot undo its receipt. */
  discard(id: string): void {
    if (!ID.test(id)) return;
    this.consume(id, { handoff: true });
    // status.json may become terminal before the manager receives its settle event.
    this.#consumed.add(id);
    try {
      fs.rmSync(this.#file(id), { force: true });
      this.releaseResult(id);
      this.#checked.delete(id);
    } catch { /* Receipt is durable; the owner/retention sweep can retry cleanup. */ }
  }

  releaseResult(id: string): void {
    if (ID.test(id)) fs.rmSync(this.resultFile(id), { force: true });
  }

  #committedIds(): Set<string> {
    const ids = new Set<string>();
    // A committed native completion also proves consumption, including older workers
    // whose post-send receipt failed. Do not infer unread from a surviving envelope.
    let text: string;
    try { text = fs.readFileSync(this.sessionFile, "utf8"); } catch { return ids; }
    for (const line of text.split("\n")) {
      try {
        const entry = JSON.parse(line);
        const message = entry.type === "custom_message" ? entry : entry.type === "message" ? entry.message : undefined;
        if (message?.customType === "pi-fabric-agent-complete") {
          for (const id of message.details?.ids ?? []) if (typeof id === "string" && ID.test(id)) ids.add(id);
        }
      } catch { /* Ignore an interrupted last session entry. */ }
    }
    return ids;
  }

  pending(options: { actorId?: string; inFlightRunId?: string } = {}): ActorChildCompletion[] {
    let files: string[];
    try { files = fs.readdirSync(this.directory); } catch { return []; }
    const envelopes = files.filter((file) => file.endsWith(".json") && ID.test(file.slice(0, -5)));
    const ids = new Set(envelopes.map((file) => file.slice(0, -5)));
    for (const id of this.#checked.keys()) if (!ids.has(id)) this.#checked.delete(id);
    const eligible: ActorChildCompletion[] = [];
    for (const file of envelopes) {
      const id = file.slice(0, -5);
      try {
        if (this.received(id)) {
          fs.rmSync(this.#file(id), { force: true });
          this.#checked.delete(id);
          continue;
        }
        const value = JSON.parse(fs.readFileSync(this.#file(id), "utf8")) as ActorChildCompletion;
        if (value.format !== 1 || value.result?.id !== id || value.spawner?.kind !== "actor") continue;
        if (options.actorId && value.spawner.id !== options.actorId) continue;
        // Crucially this precedes the session read, not merely mailbox enqueue.
        if (options.inFlightRunId && value.spawner.runId === options.inFlightRunId) continue;
        eligible.push(value);
        if (eligible.length === 100) break;
      } catch { /* Retry unread envelopes on the next owner poll. */ }
    }
    const unchecked = eligible.filter(({ result }) => !this.#checked.has(result.id));
    if (unchecked.length) {
      const committed = this.#committedIds();
      for (const { result } of unchecked) this.#checked.set(result.id, committed.has(result.id));
    }
    return eligible.filter(({ result }) => {
      if (this.#checked.get(result.id)) {
        try { this.acknowledge(result.id); } catch { /* Retry cleanup without rescanning the session. */ }
        return false;
      }
      return !this.received(result.id);
    });
  }

  /** On the existing retention sweep, bound unread archives and receipt tombstones. */
  prune(retentionMs: number, now = Date.now(), keepIds: ReadonlySet<string> = new Set()): void {
    const groups = new Map<string, { files: string[]; newest: number }>();
    try {
      for (const name of fs.readdirSync(this.directory)) {
        const id = STORED_FILE.exec(name)?.[1];
        if (!id || keepIds.has(id)) continue;
        const file = path.join(this.directory, name);
        const group = groups.get(id) ?? { files: [], newest: 0 };
        group.files.push(file);
        group.newest = Math.max(group.newest, fs.statSync(file).mtimeMs);
        groups.set(id, group);
      }
    } catch { return; }
    for (const [id, group] of groups) {
      if (now - group.newest < retentionMs) continue;
      try {
        for (const file of group.files) fs.rmSync(file, { force: true });
        this.#checked.delete(id);
      } catch { /* Best effort, like actor run retention. */ }
    }
  }

  resultFile(id: string): string {
    if (!ID.test(id)) throw new Error("Invalid actor child completion id");
    return path.join(this.directory, `${id}.result.json`);
  }

  received(id: string): boolean {
    if (!ID.test(id)) return false;
    let receipt: { batchId?: string };
    try { receipt = JSON.parse(fs.readFileSync(this.#receipt(id), "utf8")); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw error; // Unreadable consumption evidence is not evidence of non-consumption.
    }
    return !receipt.batchId || (ID.test(receipt.batchId) && fs.existsSync(this.#batchReceipt(receipt.batchId)));
  }
  #file(id: string): string { return path.join(this.directory, `${id}.json`); }
  #receipt(id: string): string { return path.join(this.directory, `${id}.receipt`); }
}
