import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { parsePiSessionHeader } from "../core/pi-session-header.js";
import { archiveTraceCopy } from "../storage/trace-archive.js";

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
    // Isolation changes inference history, not actor identity (#4313). Native
    // Bash exports the live SessionManager ID, while review fences read the
    // registered journal header. Read that header anew for every activation,
    // including the first one after resident restart or session rotation.
    const header = this.before ? parsePiSessionHeader(this.before.split("\n", 1)[0]!) : undefined;
    if (this.before && !header) throw new Error("Actor journal has no valid native session header");
    fs.writeFileSync(this.file, JSON.stringify(header ?? {
      type: "session", version: 3, id: randomUUID(), timestamp: new Date().toISOString(), cwd,
    }) + "\n", { mode: 0o600, flag: "wx" });
  }

  /** Child has exited: retain its complete tree before publishing terminal status. */
  retain(): void {
    const current = fs.existsSync(this.journal) ? fs.readFileSync(this.journal, "utf8") : "";
    if (current !== this.before) throw new Error(`Actor journal changed during activation; isolated session retained at ${this.file}`);
    // Semantic journal records can normalize or transform the child (notably compaction/context edits).
    // Preserve its ORIGINAL bytes, even an empty or malformed child, before parsing or any unlink.
    const archived = archiveTraceCopy(this.file, path.join(path.dirname(this.journal), "session-archive"),
      { reason: "activation retention", journal: this.journal });
    const assertOriginal = (): void => {
      const same = (stat: fs.Stats): boolean => stat.isFile() && stat.dev === archived.stat.dev &&
        stat.ino === archived.stat.ino && stat.size === archived.stat.size && stat.mode === archived.stat.mode &&
        stat.mtimeMs === archived.stat.mtimeMs && stat.ctimeMs === archived.stat.ctimeMs &&
        stat.uid === archived.stat.uid && stat.gid === archived.stat.gid && stat.nlink === archived.stat.nlink;
      if (!same(fs.lstatSync(this.file))) throw new Error("Activation source changed after archive; original kept");
      const raw = fs.readFileSync(this.file);
      if (raw.length !== archived.bytes || createHash("sha256").update(raw).digest("hex") !== archived.sha256 ||
          !same(fs.lstatSync(this.file))) throw new Error("Activation source changed after archive; original kept");
      const copy = fs.lstatSync(archived.to);
      if (!copy.isFile() || copy.isSymbolicLink()) throw new Error("Activation archive changed; original kept");
      const retainedBytes = fs.readFileSync(archived.to);
      const after = fs.lstatSync(archived.to);
      if (retainedBytes.length !== archived.bytes || createHash("sha256").update(retainedBytes).digest("hex") !== archived.sha256 ||
          copy.dev !== after.dev || copy.ino !== after.ino || copy.mtimeMs !== after.mtimeMs || copy.ctimeMs !== after.ctimeMs) {
        throw new Error("Activation archive changed; original kept");
      }
      if (!same(fs.lstatSync(this.file))) throw new Error("Activation source changed after archive; original kept");
    };
    assertOriginal();
    const prior = current.split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>);
    const raw = fs.readFileSync(archived.to, "utf8");
    const header = parsePiSessionHeader(raw.split("\n", 1)[0]!);
    if (!header) throw new Error("Activation session has no valid native header");
    const entries = raw.split("\n").filter(Boolean).slice(1)
      .map(line => JSON.parse(line) as Record<string, unknown>);
    const ids = new Set(prior.map(entry => entry.id));
    const leaf = prior.filter(entry => entry.type !== "session" && typeof entry.id === "string").at(-1)?.id ?? null;
    const retained = entries.map(entry => {
      if (typeof entry.id !== "string" || ids.has(entry.id)) throw new Error("Activation session has invalid or duplicate entry IDs");
      ids.add(entry.id);
      // Native checkpoints/edits apply to an entire linked branch. They are
      // inference-only in this isolated activation, not durable history policy.
      // Keep their complete original records as native non-message audit entries:
      // Pi ignores `custom` in model context but still traverses its ancestry.
      // No existing journal bytes or genuine full-history compactions change.
      const durable = entry.type === "compaction" || entry.type === "context_edit"
        ? { type: "custom", customType: "fabric-activation-context", id: entry.id,
          parentId: entry.parentId, timestamp: entry.timestamp,
          data: { scope: "activation", activationId: header.id, entry } }
        : entry;
      if (durable.parentId === null) durable.parentId = leaf;
      return durable;
    });
    if (!entries.length) {
      assertOriginal();
      fs.unlinkSync(this.file);
      return;
    }
    const prefix = current ? (current.endsWith("\n") ? "" : "\n") : JSON.stringify(header) + "\n";
    assertOriginal();
    fs.mkdirSync(path.dirname(this.journal), { recursive: true, mode: 0o700 });
    fs.appendFileSync(this.journal, prefix + retained.map(entry => JSON.stringify(entry)).join("\n") + "\n", { mode: 0o600 });
    assertOriginal();
    fs.unlinkSync(this.file);
  }
}
