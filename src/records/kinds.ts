import { createHash } from "node:crypto";

/** Record kinds and their data fields (smarty-dev#754 §2, interface v1.1). */
export const RECORD_KINDS = ["issue", "status", "comment", "decision", "ask", "answer", "handoff", "link", "close", "reopen", "mirror"] as const;
export type RecordKind = (typeof RECORD_KINDS)[number];

/** Kinds the GitHub mirror writes (§7, D2); handoff, link and mirror are never mirrored. */
export const MIRRORED_KINDS: ReadonlySet<RecordKind> = new Set(["issue", "status", "comment", "decision", "ask", "answer", "close", "reopen"]);

/** Fields only the importer role may set (C13): the source of an imported record. */
export const IMPORT_FIELDS = ["via", "githubId"] as const;

export const MAX_TEXT_BYTES = 64 * 1024;
export const MAX_DATA_BYTES = 64 * 1024;
export const MAX_KEY_LENGTH = 256;
/**
 * A record's text and data as they are sent (JSON, escapes included) are at most this, so every
 * record fits one response with room to spare (F10): a raw-byte limit alone lets escapes grow it.
 */
export const MAX_ENCODED_RECORD_BYTES = 192 * 1024;

type FieldType = "string" | "strings" | "number" | "boolean" | "uuid" | "eta";
interface FieldSpec { type: FieldType; required?: boolean; values?: readonly string[]; pattern?: RegExp; hint?: string }

/**
 * A recipient: a participant id or name. It is the relay's `to` and an index key, so it is short
 * and plain (F10): at most 256 characters of letters, digits and . _ @ : / -.
 */
const RECIPIENT = /^[A-Za-z0-9._@:/-]{1,256}$/;

const STATUS_STATES = ["in progress", "waiting", "blocked", "done"] as const;
const MIRROR_STATES = ["pending", "mirrored", "refused", "skipped", "unknown"] as const;

const KIND_FIELDS: Record<RecordKind, Record<string, FieldSpec>> = {
  issue: { title: { type: "string" }, body: { type: "string" }, owner: { type: "string" }, acceptance: { type: "string" }, labels: { type: "strings" }, nextAction: { type: "string" }, stage: { type: "string" } },
  status: { eta: { type: "eta" }, state: { type: "string", values: STATUS_STATES }, waitOn: { type: "string" } },
  comment: { deleted: { type: "boolean" } },
  decision: { by: { type: "string" }, where: { type: "string" } },
  ask: { to: { type: "string", required: true, pattern: RECIPIENT, hint: "a participant id or name (1-256 of A-Z a-z 0-9 . _ @ : / -)" }, class: { type: "string" }, minutes: { type: "number" } },
  answer: { ask: { type: "uuid", required: true }, outcome: { type: "string", required: true, values: ["answered", "withdrawn"] } },
  handoff: { to: { type: "string", required: true, pattern: RECIPIENT, hint: "a participant id or name (1-256 of A-Z a-z 0-9 . _ @ : / -)" }, key: { type: "string" } },
  link: { pr: { type: "string" }, issue: { type: "string" }, commit: { type: "string" }, url: { type: "string" }, forge: { type: "string" }, number: { type: "number" } },
  close: { reason: { type: "string" } },
  reopen: { reason: { type: "string" } },
  mirror: {
    mirrorOf: { type: "uuid", required: true }, target: { type: "string", required: true }, url: { type: "string" }, githubId: { type: "string" },
    state: { type: "string", required: true, values: MIRROR_STATES }, attempts: { type: "number" }, error: { type: "string" },
  },
};

/** Text is required where the body is the record. */
const TEXT_REQUIRED: ReadonlySet<RecordKind> = new Set(["comment", "decision", "ask"]);

const REF = /^([A-Za-z0-9_.-]{1,100})\/([A-Za-z0-9_.-]{1,100})#(L?)([1-9][0-9]{0,9})$/;
const REPO = /^([A-Za-z0-9_.-]{1,100})\/([A-Za-z0-9_.-]{1,100})$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface ParsedRef { ref: string; owner: string; repo: string; number: number; native: boolean }

/** `Owner/repo#754` (a forge number) or `Owner/repo#L12` (a Node-native number, C11). */
export const parseRef = (ref: string): ParsedRef => {
  const match = REF.exec(ref);
  if (!match) throw new RecordsArgumentError(`ref must look like Owner/repo#123 or Owner/repo#L12, not ${JSON.stringify(ref.slice(0, 120))}`);
  return { ref, owner: match[1]!, repo: match[2]!, native: match[3] === "L", number: Number(match[4]) };
};

export const parseRepo = (repo: string): { owner: string; repo: string } => {
  const match = REPO.exec(repo);
  if (!match) throw new RecordsArgumentError(`repo must look like Owner/repo, not ${JSON.stringify(repo.slice(0, 120))}`);
  return { owner: match[1]!, repo: match[2]! };
};

/** The mesh topic that carries a ref's nudges: record/<owner>/<repo>/<n> (L<n> for a native ref). */
export const recordTopic = (parsed: ParsedRef): string => `record/${parsed.owner}/${parsed.repo}/${parsed.native ? "L" : ""}${parsed.number}`;

export class RecordsArgumentError extends Error {
  readonly code = "RECORD_INVALID";
  constructor(message: string) { super(`Invalid records call: ${message}`); }
}

export interface AppendArgs {
  ref?: string;
  repo?: string;
  kind: RecordKind;
  key: string;
  text?: string;
  data?: Record<string, unknown>;
  supersedes?: string;
  /** The importer role only (C13): the author of an imported record. */
  author?: string;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const checkField = (kind: string, name: string, spec: FieldSpec, value: unknown): void => {
  const bad = (what: string) => new RecordsArgumentError(`data.${name} of a ${kind} record must be ${what}`);
  switch (spec.type) {
    case "string":
      if (typeof value !== "string") throw bad("a string");
      if (spec.values && !spec.values.includes(value)) throw bad(`one of ${spec.values.map((v) => JSON.stringify(v)).join(", ")}`);
      if (spec.pattern && !spec.pattern.test(value)) throw bad(spec.hint ?? `a string matching ${spec.pattern}`);
      return;
    case "strings":
      if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) throw bad("an array of strings");
      return;
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) throw bad("a finite number");
      return;
    case "boolean":
      if (typeof value !== "boolean") throw bad("a boolean");
      return;
    case "uuid":
      if (typeof value !== "string" || !UUID.test(value)) throw bad("a record id (UUID)");
      return;
    case "eta":
      // A single promise ("~22:15Z installed") or C14's list of {stage, at}.
      if (typeof value === "string") return;
      if (Array.isArray(value) && value.every((item) => isObject(item) && typeof item.stage === "string" && typeof item.at === "string"
        && Object.keys(item).every((field) => field === "stage" || field === "at"))) return;
      throw bad("a string or a list of {stage, at}");
  }
};

/**
 * Check a records.append call: known kind, known fields of the right types, bounded sizes.
 * Unknown kinds and fields are refused (§5), with the messaging calls' argument style (#459).
 */
export const validateAppend = (input: unknown, options: { importer: boolean; mirror: boolean }): AppendArgs => {
  if (!isObject(input)) throw new RecordsArgumentError("pass one object: {ref, kind, key, text?, data?, supersedes?}");
  const allowed = new Set(["ref", "repo", "kind", "key", "text", "data", "supersedes", "author"]);
  const extra = Object.keys(input).filter((name) => !allowed.has(name));
  if (extra.length) throw new RecordsArgumentError(`unknown field ${extra.map((name) => JSON.stringify(name)).join(", ")}; allowed: ${[...allowed].join(", ")}`);
  const { ref, repo, kind, key, text, data, supersedes, author } = input;
  if (typeof kind !== "string" || !(RECORD_KINDS as readonly string[]).includes(kind)) {
    throw new RecordsArgumentError(`kind must be one of ${RECORD_KINDS.join(", ")}`);
  }
  const recordKind = kind as RecordKind;
  if (typeof key !== "string" || !key.trim() || key.length > MAX_KEY_LENGTH) {
    throw new RecordsArgumentError(`key is required: a stable idempotency key of 1-${MAX_KEY_LENGTH} characters (retry with the same key)`);
  }
  if (ref !== undefined && typeof ref !== "string") throw new RecordsArgumentError("ref must be a string");
  if (repo !== undefined && typeof repo !== "string") throw new RecordsArgumentError("repo must be a string");
  if (ref === undefined && repo === undefined) throw new RecordsArgumentError("ref is required (repo alone only creates an issue)");
  if (ref !== undefined && repo !== undefined) throw new RecordsArgumentError("pass ref or repo, not both");
  if (repo !== undefined) {
    if (recordKind !== "issue" || supersedes !== undefined) throw new RecordsArgumentError("repo without ref only creates an issue (kind issue, no supersedes)");
    parseRepo(repo);
  } else parseRef(ref as string);
  if (text !== undefined && typeof text !== "string") throw new RecordsArgumentError("text must be a string");
  if (typeof text === "string" && Buffer.byteLength(text) > MAX_TEXT_BYTES) throw new RecordsArgumentError(`text exceeds ${MAX_TEXT_BYTES} bytes`);
  if (TEXT_REQUIRED.has(recordKind) && !(typeof text === "string" && text.trim())) throw new RecordsArgumentError(`a ${recordKind} record needs text`);
  if (supersedes !== undefined && (typeof supersedes !== "string" || !UUID.test(supersedes))) throw new RecordsArgumentError("supersedes must be a record id (UUID)");
  if (data !== undefined && !isObject(data)) throw new RecordsArgumentError("data must be an object");
  const fields = (data ?? {}) as Record<string, unknown>;
  if (Buffer.byteLength(JSON.stringify(fields)) > MAX_DATA_BYTES) throw new RecordsArgumentError(`data exceeds ${MAX_DATA_BYTES} bytes`);
  if (Buffer.byteLength(JSON.stringify({ text: text ?? null, data: fields })) > MAX_ENCODED_RECORD_BYTES) {
    throw new RecordsArgumentError(`text and data exceed ${MAX_ENCODED_RECORD_BYTES} bytes as JSON (escapes count)`);
  }
  const specs = KIND_FIELDS[recordKind];
  for (const [name, value] of Object.entries(fields)) {
    if ((IMPORT_FIELDS as readonly string[]).includes(name) && !(recordKind === "mirror" && name === "githubId")) {
      if (!options.importer) throw new RecordsArgumentError(`data.${name} is set only by the importer role`);
      if (typeof value !== "string" || !value.trim()) throw new RecordsArgumentError(`data.${name} must be a non-empty string`);
      continue;
    }
    const spec = specs[name];
    if (!spec) throw new RecordsArgumentError(`unknown field data.${name} for a ${recordKind} record; allowed: ${Object.keys(specs).join(", ") || "none"}`);
    checkField(recordKind, name, spec, value);
  }
  for (const [name, spec] of Object.entries(specs)) {
    if (spec.required && fields[name] === undefined) throw new RecordsArgumentError(`a ${recordKind} record needs data.${name}`);
  }
  if (recordKind === "issue" && repo !== undefined && typeof fields.title !== "string") throw new RecordsArgumentError("a new issue needs data.title");
  if (recordKind === "link" && !["pr", "issue", "commit", "url", "forge"].some((name) => fields[name] !== undefined)) {
    throw new RecordsArgumentError("a link record needs data.pr, data.issue, data.commit, data.url or data.forge");
  }
  if (recordKind === "mirror" && !options.mirror) throw new RecordsArgumentError("record.mirror is written only by the mirror role");
  if (author !== undefined) {
    if (!options.importer) throw new RecordsArgumentError("author is taken from the authenticated caller; only the importer role sets it");
    if (typeof author !== "string" || !author.trim() || author.length > 256) throw new RecordsArgumentError("author must be a non-empty string");
    if (typeof fields.via !== "string") throw new RecordsArgumentError("an imported record (author set) needs data.via");
  }
  // A record id is stored in one canonical (lowercase) form, so the folds that compare and group
  // by it (an answer closing its ask, the newest mirror per record) see one id, not two spellings.
  const canonicalData = Object.fromEntries(Object.entries(fields).map(([name, value]) =>
    [name, specs[name]?.type === "uuid" && typeof value === "string" ? value.toLowerCase() : value]));
  return {
    ...(ref !== undefined ? { ref: ref as string } : {}),
    ...(repo !== undefined ? { repo: repo as string } : {}),
    kind: recordKind,
    key: key as string,
    ...(text !== undefined ? { text: text as string } : {}),
    data: canonicalData,
    ...(supersedes !== undefined ? { supersedes: (supersedes as string).toLowerCase() } : {}),
    ...(author !== undefined ? { author: (author as string).trim() } : {}),
  };
};

const canonical = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(canonical);
  if (isObject(value)) return Object.fromEntries(Object.keys(value).sort().map((name) => [name, canonical(value[name])]));
  return value;
};

/** The payload's hash (C3): what a retry with the same key must repeat exactly. */
export const payloadHash = (args: AppendArgs): string =>
  createHash("sha256").update(JSON.stringify(canonical({
    ref: args.ref ?? null, repo: args.repo ?? null, kind: args.kind, text: args.text ?? null, data: args.data ?? {}, supersedes: args.supersedes ?? null,
  }))).digest("hex");
