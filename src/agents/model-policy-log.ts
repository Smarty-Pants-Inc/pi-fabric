import fs from "node:fs";
import os from "node:os";
import path from "node:path";

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
  | "invalid-reason" | "missing-reference" | "file-unavailable" | "malformed" | "unlisted" | "expired" | "model-mismatch" | "role-mismatch";

/** Host-owned `<agentDir>/model-exceptions.json` entry. */
export interface ModelExceptionEntry {
  ref: string;
  models: string[];
  roles?: string[];
  expires: string;
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

const REF_PATTERN = /^([a-z0-9._-]+)#([1-9]\d*)$/;

/**
 * Canonical `<repo>#N` references in order. Bare `#N` means smarty-dev#N; GitHub
 * Smarty-Pants-Inc issue URLs map to their repo; any other URL is consumed whole
 * so its fragment can never grant.
 */
export function canonicalIssueReferences(reason: unknown): string[] {
  if (typeof reason !== "string") return [];
  const refs: string[] = [];
  const candidates = /https?:\/\/[^\s<>"']+|(?:^|[\s([])([A-Za-z0-9._-]+)?#([1-9]\d*)(?![\w#/])/gi;
  for (const match of reason.matchAll(candidates)) {
    let ref: string | undefined;
    if (match[2]) {
      ref = `${(match[1] ?? "smarty-dev").toLowerCase()}#${match[2]}`;
    } else {
      const candidate = match[0].replace(/[.,;:)\]}]+$/, "");
      try {
        const url = new URL(candidate);
        const issue = /^\/smarty-pants-inc\/([a-z0-9._-]+)\/issues\/([1-9]\d*)\/?$/i.exec(url.pathname);
        if (url.protocol === "https:" && url.hostname.toLowerCase() === "github.com" && !url.username && !url.password && !url.port && issue) {
          ref = `${issue[1]!.toLowerCase()}#${issue[2]}`;
        }
      } catch { /* Malformed URLs are not issue references. */ }
    }
    if (ref && !refs.includes(ref)) refs.push(ref);
  }
  return refs;
}

const MAX_EXCEPTIONS_BYTES = 1024 * 1024;
const ISO_PATTERN = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?)?$/;

const validEntry = (value: unknown): value is ModelExceptionEntry => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const entry = value as Record<string, unknown>;
  if (typeof entry.ref !== "string" || !REF_PATTERN.test(entry.ref.trim().toLowerCase())) return false;
  if (!Array.isArray(entry.models) || entry.models.length === 0 ||
      entry.models.some(model => typeof model !== "string" || !model.trim())) return false;
  if (entry.roles !== undefined && (!Array.isArray(entry.roles) || entry.roles.length === 0 ||
      entry.roles.some(role => typeof role !== "string" || !role.trim()))) return false;
  return typeof entry.expires === "string" && ISO_PATTERN.test(entry.expires.trim()) &&
    Number.isFinite(Date.parse(entry.expires.trim()));
};

/** Read the host-owned exception list now. Throws on any missing/unreadable/malformed state. */
export function readModelExceptions(file: string): ModelExceptionEntry[] {
  const stat = fs.statSync(file);
  if (!stat.isFile() || stat.size > MAX_EXCEPTIONS_BYTES) throw new Error("model exceptions file is not a bounded regular file");
  const parsed: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
  if (!Array.isArray(parsed) || !parsed.every(validEntry)) throw new SyntaxError("model exceptions file is malformed");
  return parsed.map(entry => ({
    ref: entry.ref.trim().toLowerCase(),
    models: entry.models.map(model => model.trim().toLowerCase()),
    ...(entry.roles ? { roles: entry.roles.map(role => role.trim()) } : {}),
    expires: entry.expires.trim(),
  }));
}

/**
 * Admit only a reason citing a host-listed, unexpired ref whose model prefixes match
 * one of the candidate (resolved/provenance) models and whose roles include `role`.
 */
export function admitModelException(input: {
  path: string | undefined; reason: unknown; models: string[]; role: string; now?: number;
}): { ref: string; entry: ModelExceptionEntry } {
  const model = input.models[0] ?? "<unknown>";
  const refs = canonicalIssueReferences(input.reason);
  const deny = (denial: ModelExceptionDenial, ref?: string, detail?: string): never => {
    throw new FabricModelExceptionDeniedError({ denial, ref, model, role: input.role, ...(detail ? { detail } : {}) });
  };
  if (refs.length === 0) deny("missing-reference");
  if (!input.path) deny("file-unavailable", refs[0], "no host model-exceptions.json configured");
  let entries: ModelExceptionEntry[] = [];
  try { entries = readModelExceptions(input.path!); }
  catch (error) {
    deny(error instanceof SyntaxError ? "malformed" : "file-unavailable", refs[0]);
  }
  const now = input.now ?? Date.now();
  const candidates = input.models.map(value => value.trim().toLowerCase()).filter(Boolean);
  const rank: ModelExceptionDenial[] = ["unlisted", "expired", "model-mismatch", "role-mismatch"];
  let best: { denial: ModelExceptionDenial; ref: string } | undefined;
  for (const ref of refs) {
    for (const entry of entries) {
      if (entry.ref !== ref) continue;
      let denial: ModelExceptionDenial | undefined;
      if (Date.parse(entry.expires) <= now) denial = "expired";
      else if (!entry.models.some(prefix => candidates.some(candidate => candidate.startsWith(prefix)))) denial = "model-mismatch";
      else if (entry.roles && !entry.roles.includes(input.role)) denial = "role-mismatch";
      if (!denial) return { ref, entry };
      if (!best || rank.indexOf(denial) > rank.indexOf(best.denial)) best = { denial, ref };
    }
  }
  return deny(best?.denial ?? "unlisted", best?.ref ?? refs[0]);
}

/** Best-effort host-local audit: one append write, no retries or global output. */
export function appendOpusPolicyLog(input: {
  role: string; model: string; ref: string | null; reason: string;
}): string | undefined {
  let fd: number | undefined;
  let warning: string | undefined;
  try {
    const directory = path.join(os.homedir(), ".local", "state", "smarty", "route");
    const line = Buffer.from(`${JSON.stringify({
      ts: new Date().toISOString(), role: input.role, model: input.model,
      ref: input.ref, reason: input.reason, launcher: "fabric", host: os.hostname(),
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
