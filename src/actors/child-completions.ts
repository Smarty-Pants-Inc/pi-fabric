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

/**
 * Write-ahead inbox shared by an actor activation and its authoritative owner.
 * Live inbox delivery/wait records a receipt. Otherwise, after that activation ends,
 * its owner transfers the result to the actor's persisted serial mailbox. Never Main.
 * Receipts stay with the actor so replay/restart cannot resurrect a delivered result.
 */
export class ActorChildCompletionStore {
  readonly directory: string;
  constructor(readonly sessionFile: string) {
    this.directory = path.join(path.dirname(sessionFile), "child-completions");
  }

  enqueue(result: AgentRunResult, spawner: AgentSpawner, notify = true): void {
    if (!ID.test(result.id) || spawner.kind !== "actor") throw new Error("Invalid actor child completion identity");
    if (this.received(result.id) || fs.existsSync(this.#file(result.id))) return;
    // Keep the complete outcome accessible after the spawning worker and its
    // local wait/status handles disappear. Mailbox notifications remain bounded.
    writeJsonAtomic(this.resultFile(result.id), { ...result, spawner }, { durable: true });
    if (!notify) return; // Archived, but no automatic mailbox activation was requested.
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

  acknowledge(id: string): void {
    if (!ID.test(id)) return;
    if (!fs.existsSync(this.#file(id))) return;
    this.consume(id);
    fs.rmSync(this.#file(id), { force: true });
  }

  /** Shared live/mailbox consumption record, durable before an activation can observe it. */
  consume(id: string): void {
    if (!ID.test(id) || this.received(id)) return;
    writeJsonAtomic(this.#receipt(id), { id, acknowledgedAt: Date.now() }, { durable: true });
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

  pending(): ActorChildCompletion[] {
    let files: string[];
    try { files = fs.readdirSync(this.directory); } catch { return []; }
    const pending = files.filter((file) => file.endsWith(".json") && ID.test(file.slice(0, -5))).slice(0, 100);
    if (!pending.length) return [];
    const committed = this.#committedIds();
    return pending.flatMap((file) => {
      const id = file.slice(0, -5);
      if (!ID.test(id)) return [];
      try {
        if (committed.has(id)) this.consume(id);
        if (this.received(id)) {
          fs.rmSync(this.#file(id), { force: true });
          return [];
        }
        const value = JSON.parse(fs.readFileSync(this.#file(id), "utf8")) as ActorChildCompletion;
        return value.format === 1 && value.result?.id === id && value.spawner?.kind === "actor" ? [value] : [];
      } catch { return []; }
    });
  }

  resultFile(id: string): string {
    if (!ID.test(id)) throw new Error("Invalid actor child completion id");
    return path.join(this.directory, `${id}.result.json`);
  }

  received(id: string): boolean { return ID.test(id) && fs.existsSync(this.#receipt(id)); }
  #file(id: string): string { return path.join(this.directory, `${id}.json`); }
  #receipt(id: string): string { return path.join(this.directory, `${id}.receipt`); }
}
