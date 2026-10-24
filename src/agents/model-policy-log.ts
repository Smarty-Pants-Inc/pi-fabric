import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { isFabricThinking, thinkingRank, type FabricThinking } from "../thinking.js";

/** Model identifiers or a known Claude-capable native backend, not substrings. */
export function isClaudeModel(model: string | undefined, runner?: string, backend?: string): boolean {
  return runner?.toLowerCase() === "claude" ||
    (runner?.toLowerCase() === "veda" && backend?.trim().toLowerCase() === "claude-code") ||
    (typeof model === "string" && /(?:^|[/:._-])(?:claude|opus)(?=$|[/:._-])/i.test(model));
}

/** A reason alone grants nothing: callers must require an actual issue reference. */
export function opusIssueReference(reason: unknown): string | undefined {
  if (typeof reason !== "string") return undefined;
  // Consume whole URLs so an arbitrary URL's #123 fragment cannot grant an exception.
  const candidates = /https?:\/\/[^\s<>"']+|(?:^|[\s([])(#[1-9]\d*)\b/gi;
  for (const match of reason.matchAll(candidates)) {
    if (match[1]) return match[1];
    const candidate = match[0].replace(/[.,;:)\]}]+$/, "");
    try {
      const url = new URL(candidate);
      if (/^https?:$/.test(url.protocol) && /\/issues\/[1-9]\d*\/?$/.test(url.pathname)) return candidate;
    } catch { /* Malformed URLs are not issue references. */ }
  }
  return undefined;
}

export const MODEL_EXCEPTION_DENIED_CODE = "FABRIC_MODEL_EXCEPTION_DENIED" as const;

export type ModelExceptionDenial =
  | "invalid-reason" | "missing-reference" | "file-unavailable" | "untrusted-source" | "malformed"
  | "unlisted" | "expired" | "revoked" | "expiry-too-long" | "model-mismatch" | "role-mismatch"
  | "lane-mismatch" | "host-mismatch" | "thinking-exceeds"
  /** The bound ROOT lane could not be read (stale/missing record or lookup error): fail closed, retryable (#16331). */
  | "lane-unresolved"
  /** The bound ROOT session is gone or was last seen beyond the host's limit: terminal, never retried (#16357). */
  | "root-ended";

/** Version 1 grant from the fixed root-owned host source. */
export interface ModelExceptionEntry {
  id: string;
  issue: string;
  reason_url: string;
  role: string;
  lane: string;
  host: string;
  model: string;
  thinking: FabricThinking;
  granted_by: string;
  granted_at: string;
  expires_at: string;
  status: "active" | "revoked";
}

/** Typed refusal: a Claude/Opus selection lacks a trusted, current, matching exception. */
export class FabricModelExceptionDeniedError extends Error {
  readonly code = MODEL_EXCEPTION_DENIED_CODE;
  readonly denial: ModelExceptionDenial;
  readonly ref: string | undefined;
  readonly model: string;
  readonly role: string;
  constructor(input: { denial: ModelExceptionDenial; ref?: string | undefined; model: string; role: string; detail?: string }) {
    super(`model ${input.model} (role ${input.role}) requires modelReason (host-approved model exception) ` +
      `(${input.ref ?? "missing reference"}: ${input.denial}${input.detail ? `; ${input.detail}` : ""}); ` +
      "Paul's Opus-only org/leads rule (smarty-dev#2264): every Fabric-created agent is nonlead; " +
      "modelReason must cite a ref listed in the host model-exceptions.json; no worker started");
    this.name = "FabricModelExceptionDeniedError";
    this.denial = input.denial;
    this.ref = input.ref;
    this.model = input.model;
    this.role = input.role;
  }
}

const REF_PATTERN = /^smarty-pants-inc\/([a-z0-9._-]+)#([1-9]\d*)$/i;
const canonicalRef = (repo: string, number: string): string => `Smarty-Pants-Inc/${repo.toLowerCase()}#${number}`;

/**
 * Canonical `Smarty-Pants-Inc/<repo>#N` references in order. Bare `#N` means
 * smarty-dev; foreign owner refs and arbitrary URL fragments never grant.
 */
export function canonicalIssueReferences(reason: unknown): string[] {
  if (typeof reason !== "string") return [];
  const refs: string[] = [];
  const candidates = /https?:\/\/[^\s<>"']+|(?:^|[\s([])((?:[A-Za-z0-9._-]+\/)?[A-Za-z0-9._-]+)?#([1-9]\d*)(?![\w#/])/gi;
  for (const match of reason.matchAll(candidates)) {
    let ref: string | undefined;
    if (match[2]) {
      const parts = (match[1] ?? "smarty-dev").split("/");
      if (parts.length === 1 || parts[0]!.toLowerCase() === "smarty-pants-inc") {
        ref = canonicalRef(parts.at(-1)!, match[2]);
      }
    } else {
      const candidate = match[0].replace(/[.,;:)\]}]+$/, "");
      try {
        const url = new URL(candidate);
        const issue = /^\/smarty-pants-inc\/([a-z0-9._-]+)\/issues\/([1-9]\d*)\/?$/i.exec(url.pathname);
        if (url.protocol === "https:" && url.hostname.toLowerCase() === "github.com" && !url.username && !url.password && !url.port && issue) {
          ref = canonicalRef(issue[1]!, issue[2]!);
        }
      } catch { /* Malformed URLs are not issue references. */ }
    }
    if (ref && !refs.includes(ref)) refs.push(ref);
  }
  return refs;
}

const MAX_EXCEPTIONS_BYTES = 1024 * 1024;
const MAX_GRANT_TTL_MS = 72 * 60 * 60 * 1_000;
const ISO_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/;

/** Date.parse alone accepts rolled-over dates such as February 30. */
const validTimestamp = (value: unknown): value is string => {
  if (typeof value !== "string") return false;
  const parts = ISO_PATTERN.exec(value);
  if (!parts) return false;
  const year = Number(parts[1]), month = Number(parts[2]), day = Number(parts[3]);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1]! &&
    Number(parts[4]) <= 23 && Number(parts[5]) <= 59 && Number(parts[6]) <= 59 &&
    (!parts[7] || (Number(parts[8]) <= 23 && Number(parts[9]) <= 59)) && Number.isFinite(Date.parse(value));
};

class UntrustedModelExceptionSourceError extends Error {
  readonly code = "FABRIC_MODEL_EXCEPTION_UNTRUSTED_SOURCE";
  constructor(detail: string) {
    super(detail);
    this.name = "UntrustedModelExceptionSourceError";
  }
}

const trustedMetadata = (stat: fs.Stats): boolean =>
  stat.uid === 0 && (stat.mode & 0o022) === 0;

const sourceUnsafe = (detail: string): never => {
  throw new UntrustedModelExceptionSourceError(detail);
};

const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : undefined;

/** Validate every existing ancestor before opening the grant source. */
const validateTrustedParents = (file: string): void => {
  const parent = path.dirname(file);
  const root = path.parse(parent).root;
  const directories: string[] = [];
  for (let directory = parent;; directory = path.dirname(directory)) {
    directories.push(directory);
    if (directory === root) break;
  }
  for (let index = directories.length - 1; index >= 0; index -= 1) {
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(directories[index]!);
    } catch {
      sourceUnsafe(`cannot validate model exceptions directory ${directories[index]}`);
    }
    if (stat!.isSymbolicLink() || !stat!.isDirectory() || !trustedMetadata(stat!)) {
      sourceUnsafe(`unsafe model exceptions directory ${directories[index]}`);
    }
  }
};

const validateTrustedFile = (stat: fs.Stats): void => {
  if (stat.isSymbolicLink() || !stat.isFile() || !trustedMetadata(stat) ||
      !Number.isSafeInteger(stat.size) || stat.size < 0 || stat.size > MAX_EXCEPTIONS_BYTES) {
    sourceUnsafe("model exceptions source is not a bounded root-owned regular file");
  }
};

const validEntry = (value: unknown): value is ModelExceptionEntry => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const entry = value as Record<string, unknown>;
  const fields = ["id", "issue", "reason_url", "role", "lane", "host", "model", "granted_by"];
  if (fields.some(field => typeof entry[field] !== "string" || !entry[field] ||
      (entry[field] as string).trim() !== entry[field])) return false;
  if (!REF_PATTERN.test(entry.issue as string) || !/^(?:task|lane|actor:[^\s]+)$/.test(entry.role as string) ||
      !/^[^\s/\\]+$/.test(entry.lane as string) || entry.lane === "." || entry.lane === ".." ||
      !/^[^\s./\\]+$/.test(entry.host as string) || /[\s*<>]/.test(entry.model as string) ||
      !isFabricThinking(entry.thinking) || (entry.status !== "active" && entry.status !== "revoked")) return false;
  // The linked issue/comment is the record, not an alternate admission key.
  const issue = REF_PATTERN.exec(entry.issue as string)!;
  try {
    const url = new URL(entry.reason_url as string);
    const linked = /^\/smarty-pants-inc\/([a-z0-9._-]+)\/issues\/([1-9]\d*)\/?$/i.exec(url.pathname);
    if (url.protocol !== "https:" || url.hostname.toLowerCase() !== "github.com" ||
        url.username || url.password || url.port || !linked ||
        canonicalRef(linked[1]!, linked[2]!) !== canonicalRef(issue[1]!, issue[2]!)) return false;
  } catch { return false; }
  return validTimestamp(entry.granted_at) && validTimestamp(entry.expires_at) &&
    Date.parse(entry.expires_at) > Date.parse(entry.granted_at);
};

/** Read the host-owned exception list now. Throws on any missing/unreadable/malformed state. */
export function readModelExceptions(file: string): ModelExceptionEntry[] {
  if (process.platform === "win32" || typeof process.getuid !== "function") {
    sourceUnsafe("model exceptions source ownership cannot be proven on this platform");
  }
  const noFollow = fs.constants.O_NOFOLLOW;
  const nonBlock = fs.constants.O_NONBLOCK;
  if (typeof noFollow !== "number" || noFollow === 0 || typeof nonBlock !== "number" || nonBlock === 0) {
    sourceUnsafe("model exceptions source requires O_NOFOLLOW and O_NONBLOCK");
  }
  if (!path.isAbsolute(file)) sourceUnsafe("model exceptions source must be an absolute path");

  // A missing grant remains an ordinary unavailable-file denial. Any present
  // source must prove its metadata before its parent chain is trusted.
  let before: fs.Stats;
  try {
    before = fs.lstatSync(file);
  } catch (error) {
    if (errorCode(error) === "ENOENT") throw error;
    sourceUnsafe("model exceptions source metadata is unreadable");
  }
  validateTrustedFile(before!);
  validateTrustedParents(file);

  let descriptor: number | undefined;
  let source: string;
  try {
    try {
      descriptor = fs.openSync(file, fs.constants.O_RDONLY | noFollow | nonBlock);
    } catch (error) {
      if (errorCode(error) === "ENOENT") throw error;
      if (errorCode(error) === "ELOOP" || errorCode(error) === "ENOTDIR") {
        sourceUnsafe("model exceptions source changed to a symlink or non-directory path");
      }
      throw error;
    }
    let opened: fs.Stats;
    try {
      opened = fs.fstatSync(descriptor);
    } catch {
      sourceUnsafe("model exceptions source metadata cannot be verified after open");
    }
    validateTrustedFile(opened!);

    const bytes = Buffer.alloc(opened!.size);
    let count = 0;
    while (count < bytes.length) {
      const read = fs.readSync(descriptor, bytes, count, bytes.length - count, count);
      if (read <= 0) break;
      count += read;
    }
    let after: fs.Stats;
    try {
      after = fs.fstatSync(descriptor);
    } catch {
      sourceUnsafe("model exceptions source metadata cannot be verified after read");
    }
    validateTrustedFile(after!);
    if (count !== opened!.size || after!.size !== opened!.size || after!.dev !== opened!.dev ||
        after!.ino !== opened!.ino || after!.uid !== opened!.uid || after!.mode !== opened!.mode ||
        after!.mtimeMs !== opened!.mtimeMs || after!.ctimeMs !== opened!.ctimeMs) {
      sourceUnsafe("model exceptions source changed while reading");
    }
    source = bytes.toString("utf8");
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }

  const parsed: unknown = JSON.parse(source);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new SyntaxError("model exceptions file is malformed");
  const document = parsed as Record<string, unknown>;
  if (document.version !== 1 || !Array.isArray(document.grants) || !document.grants.every(validEntry)) {
    throw new SyntaxError("model exceptions file is malformed");
  }
  const entries = document.grants as ModelExceptionEntry[];
  if (new Set(entries.map(entry => entry.id)).size !== entries.length) throw new SyntaxError("duplicate model exception grant id");
  return entries.map(entry => {
    const issue = REF_PATTERN.exec(entry.issue)!;
    return {
      id: entry.id, issue: canonicalRef(issue[1]!, issue[2]!), reason_url: entry.reason_url,
      role: entry.role, lane: entry.lane, host: entry.host, model: entry.model, thinking: entry.thinking,
      granted_by: entry.granted_by, granted_at: entry.granted_at, expires_at: entry.expires_at, status: entry.status,
    };
  });
}

/** Admit one active grant matching the complete host-bound key and effort ceiling. */
export function admitModelException(input: {
  path: string | undefined; reason: unknown; model: string; role: string;
  lane?: string | undefined; host: string; thinking: FabricThinking; now?: number;
  /** Why a bound ROOT lane is absent: a distinct, retryable denial instead of lane-mismatch (#16331). */
  laneUnresolved?: string | undefined;
  /** Why the bound ROOT session counts as ended (gone, or last seen beyond the limit): terminal (#16357). */
  rootEnded?: string | undefined;
}): { ref: string; entry: Readonly<ModelExceptionEntry> } {
  const model = input.model;
  const refs = canonicalIssueReferences(input.reason);
  const deny = (denial: ModelExceptionDenial, ref?: string, detail?: string): never => {
    throw new FabricModelExceptionDeniedError({ denial, ref, model, role: input.role, ...(detail ? { detail } : {}) });
  };
  if (refs.length === 0) deny("missing-reference");
  if (!input.path) deny("file-unavailable", refs[0], "no host model-exceptions.json configured");
  let entries: ModelExceptionEntry[] = [];
  try { entries = readModelExceptions(input.path!); }
  catch (error) {
    deny(error instanceof UntrustedModelExceptionSourceError ? "untrusted-source" :
      error instanceof SyntaxError ? "malformed" : "file-unavailable", refs[0]);
  }
  const now = input.now ?? Date.now();
  // #16357: a lane that is unavailable (not merely absent) is decided only AFTER the lane-independent
  // grant fields. A revoked, expired, over-long or unlisted grant (or one for another host, role, model
  // or effort) is its own terminal denial; only a grant that would admit but for the lane gets
  // lane-unresolved (retryable) or root-ended (terminal). A host with no root binding is unchanged.
  const laneUnavailable: { denial: ModelExceptionDenial; detail: string } | undefined = input.lane ? undefined
    : input.rootEnded ? { denial: "root-ended", detail: input.rootEnded }
    : input.laneUnresolved ? { denial: "lane-unresolved", detail: input.laneUnresolved } : undefined;
  if (!input.lane && !laneUnavailable) deny("lane-mismatch", refs[0], "no bound ROOT session worktree");
  if (!isFabricThinking(input.thinking)) deny("thinking-exceeds", refs[0], "effective thinking is unknown");
  if (!Number.isFinite(now)) deny("expired", refs[0], "current time is unknown");
  const rank: ModelExceptionDenial[] = ["unlisted", "lane-mismatch", "host-mismatch", "role-mismatch",
    "model-mismatch", "thinking-exceeds", "expired", "revoked", "expiry-too-long"];
  let best: { denial: ModelExceptionDenial; ref: string } | undefined;
  let laneIndependentRef: string | undefined;
  for (const ref of refs) {
    for (const entry of entries) {
      if (entry.issue !== ref) continue;
      let denial: ModelExceptionDenial | undefined;
      if (!laneUnavailable && entry.lane !== input.lane) denial = "lane-mismatch";
      else if (entry.host !== input.host) denial = "host-mismatch";
      else if (entry.role !== input.role) denial = "role-mismatch";
      else if (entry.model !== model) denial = "model-mismatch";
      else if (thinkingRank(input.thinking) > thinkingRank(entry.thinking)) denial = "thinking-exceeds";
      else if (Date.parse(entry.expires_at) - Date.parse(entry.granted_at) > MAX_GRANT_TTL_MS) denial = "expiry-too-long";
      else if (entry.status === "revoked") denial = "revoked";
      else if (Date.parse(entry.expires_at) <= now) denial = "expired";
      if (!denial) {
        if (!laneUnavailable) return { ref, entry: Object.freeze({ ...entry }) };
        laneIndependentRef ??= ref;
        continue;
      }
      if (!best || rank.indexOf(denial) > rank.indexOf(best.denial)) best = { denial, ref };
    }
  }
  if (laneUnavailable && laneIndependentRef) deny(laneUnavailable.denial, laneIndependentRef, laneUnavailable.detail);
  return deny(best?.denial ?? "unlisted", best?.ref ?? refs[0]);
}

/** Best-effort host-local audit: one append write, no retries or global output. */
export function appendOpusPolicyLog(input: {
  role: string; model: string; ref: string | null; reason: string;
  grant?: Readonly<ModelExceptionEntry> | undefined; thinking?: FabricThinking | undefined; runId?: string | undefined;
  /** #16357: where the admitted lane came from: the bound root's fresh record, or the actor's stored copy. */
  laneSource?: "root" | "persisted" | undefined;
}): string | undefined {
  let fd: number | undefined;
  let warning: string | undefined;
  try {
    if (input.grant && (!isFabricThinking(input.thinking) || !input.runId)) {
      throw new Error("exception audit requires actual thinking and run id");
    }
    const directory = path.join(os.homedir(), ".local", "state", "smarty", "route");
    const time = new Date().toISOString();
    const line = Buffer.from(`${JSON.stringify({
      ts: time, role: input.role, model: input.model,
      ref: input.ref, reason: input.reason, launcher: "fabric", host: os.hostname(),
      ...(input.grant ? {
        grant_id: input.grant.id, issue: input.grant.issue, lane: input.grant.lane,
        host: input.grant.host, role: input.grant.role, model: input.grant.model,
        thinking: input.thinking, run_id: input.runId, time,
        ...(input.laneSource === "root" || input.laneSource === "persisted" ? { lane_source: input.laneSource } : {}),
      } : {}),
    })}\n`, "utf8");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND |
      fs.constants.O_NONBLOCK | (fs.constants.O_NOFOLLOW ?? 0);
    fd = fs.openSync(path.join(directory, "opus-exceptions.jsonl"), flags, 0o600);
    if (fs.writeSync(fd, line) !== line.length) warning = "Opus policy audit log short write; exception not fully recorded.";
  } catch {
    warning = "Opus policy audit log failed; exception not fully recorded.";
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); }
      catch { warning ??= "Opus policy audit log close failed; recording not confirmed."; }
    }
  }
  return warning;
}
