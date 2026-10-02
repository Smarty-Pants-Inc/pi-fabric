import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

/**
 * Activation inference must be empty before Pi starts, not only after its
 * context hook: preflight hooks inspect the native session too (#3238).
 * Keep the actor's journal append-only; only the disposable child sees this
 * header-only session. The actor manager serializes writers/reset at run boundaries.
 */
export class ActivationSession {
  readonly file: string;
  private readonly journal: string;
  private readonly before: string;

  constructor(journal: string, runDirectory: string, cwd: string) {
    this.journal = journal;
    this.before = fs.existsSync(journal) ? fs.readFileSync(journal, "utf8") : "";
    // Beside the durable journal, not the disposable run directory: a worker
    // crash must not let run cleanup erase unmerged activation evidence.
    fs.mkdirSync(path.dirname(journal), { recursive: true, mode: 0o700 });
    this.file = path.join(path.dirname(journal), `.activation-${path.basename(runDirectory)}.jsonl`);
    fs.writeFileSync(this.file, JSON.stringify({
      type: "session", version: 3, id: randomUUID(), timestamp: new Date().toISOString(), cwd,
    }) + "\n", { mode: 0o600, flag: "wx" });
  }

  /** Child has exited: retain its complete tree before publishing terminal status. */
  retain(): void {
    const current = fs.existsSync(this.journal) ? fs.readFileSync(this.journal, "utf8") : "";
    if (current !== this.before) throw new Error(`Actor journal changed during activation; isolated session retained at ${this.file}`);
    const prior = current.split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>);
    const entries = fs.readFileSync(this.file, "utf8").split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as Record<string, unknown>);
    const header = entries.shift();
    if (header?.type !== "session") throw new Error("Activation session has no native header");
    const ids = new Set(prior.map(entry => entry.id));
    const leaf = prior.filter(entry => entry.type !== "session" && typeof entry.id === "string").at(-1)?.id ?? null;
    for (const entry of entries) {
      if (typeof entry.id !== "string" || ids.has(entry.id)) throw new Error("Activation session has invalid or duplicate entry IDs");
      ids.add(entry.id);
      if (entry.parentId === null) entry.parentId = leaf;
    }
    if (!entries.length) {
      fs.unlinkSync(this.file);
      return;
    }
    const prefix = current ? (current.endsWith("\n") ? "" : "\n") : JSON.stringify(header) + "\n";
    fs.mkdirSync(path.dirname(this.journal), { recursive: true, mode: 0o700 });
    fs.appendFileSync(this.journal, prefix + entries.map(entry => JSON.stringify(entry)).join("\n") + "\n", { mode: 0o600 });
    fs.unlinkSync(this.file);
  }
}
