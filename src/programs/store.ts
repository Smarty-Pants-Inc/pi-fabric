import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Value } from "typebox/value";
import { writeJsonAtomicAsync } from "../core/atomic-write.js";

// Content-addressed program store under `<project>/.pi/fabric/programs`.
// `<sha256>.json` holds one record; the digest covers only the executable
// content ({kind, kernel, code|jevProgram, inputSchema}), so the same content
// always has the same identity. `index.json` maps names to digests. Every
// mutation runs under one directory lock; status lives only in the record.
// Promotion and retirement are host/user operations: the `programs` provider
// never calls them.

export type FabricProgramKind = "fabric" | "jev";
export type FabricProgramKernel = "typescript" | "python";
export type FabricProgramStatus = "candidate" | "promoted" | "retired";

export interface FabricProgramRecord {
  version: 1;
  digest: string;
  name: string;
  kind: FabricProgramKind;
  kernel?: FabricProgramKernel;
  code?: string;
  jevProgram?: Record<string, unknown>;
  description?: string;
  inputSchema?: Record<string, unknown>;
  createdAt: number;
  status: FabricProgramStatus;
  trial?: unknown;
}

export interface FabricProgramSummary {
  ref: string;
  name: string;
  digest: string;
  kind: FabricProgramKind;
  kernel?: FabricProgramKernel;
  description?: string;
  createdAt: number;
  status: FabricProgramStatus;
}

export interface FabricProgramSaveInput {
  name?: unknown;
  kind?: unknown;
  kernel?: unknown;
  code?: unknown;
  jevProgram?: unknown;
  description?: unknown;
  inputSchema?: unknown;
}

interface ProgramIndex {
  version: 1;
  names: Record<string, Array<{ digest: string; createdAt: number }>>;
}

export const PROGRAM_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]*$/;
export const MAX_PROGRAM_NAME_CHARS = 64;
export const MAX_PROGRAM_CODE_CHARS = 65_536;
export const MAX_PROGRAM_DESCRIPTION_CHARS = 1_000;
export const MAX_PROGRAM_SCHEMA_BYTES = 16 * 1024;
export const MAX_PROGRAM_INPUT_BYTES = 64 * 1024;
export const MAX_PROGRAM_REF_CHARS = 129;
const MAX_PROGRAM_NAMES = 1_024;
const MAX_PROGRAM_VERSIONS = 256;
const MIN_DIGEST_PREFIX = 12;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const LOCK_STALE_MS = 30_000;
const LOCK_ATTEMPTS = 500;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const jsonBytes = (value: unknown): number => Buffer.byteLength(JSON.stringify(value) ?? "", "utf8");

/** Sorted-key JSON; `undefined` members are omitted like JSON.stringify. */
export const canonicalProgramJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map((item) => canonicalProgramJson(item ?? null)).join(",")}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .filter((key) => value[key] !== undefined)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalProgramJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
};

export const programDigest = (content: Pick<FabricProgramRecord, "kind" | "kernel" | "code" | "jevProgram" | "inputSchema">): string =>
  createHash("sha256").update(canonicalProgramJson({
    kind: content.kind,
    kernel: content.kernel,
    ...(content.kind === "jev" ? { jevProgram: content.jevProgram } : { code: content.code }),
    inputSchema: content.inputSchema,
  })).digest("hex");

export const programRef = (record: Pick<FabricProgramRecord, "name" | "digest">, full = false): string =>
  `${record.name}@${full ? record.digest : record.digest.slice(0, MIN_DIGEST_PREFIX)}`;

export const assertProgramName = (value: unknown): string => {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_PROGRAM_NAME_CHARS || !PROGRAM_NAME_PATTERN.test(value)) {
    throw new Error(`Invalid program name: use 1-${MAX_PROGRAM_NAME_CHARS} characters of [a-z0-9._-], starting with a letter or digit`);
  }
  return value;
};

/** JSON-compatible, bounded input; returns the parsed copy. */
export const normalizeProgramInput = (input: unknown): unknown => {
  if (input === undefined) return undefined;
  let text: string | undefined;
  try {
    text = JSON.stringify(input);
  } catch {
    throw new Error("Program input must be JSON-compatible");
  }
  if (text === undefined) throw new Error("Program input must be JSON-compatible");
  if (Buffer.byteLength(text, "utf8") > MAX_PROGRAM_INPUT_BYTES) {
    throw new Error(`Program input exceeds ${MAX_PROGRAM_INPUT_BYTES} bytes`);
  }
  return JSON.parse(text) as unknown;
};

/** Validates input against the record's inputSchema; undefined input is checked as null. */
export const programInputError = (record: Pick<FabricProgramRecord, "inputSchema">, input: unknown): string | undefined => {
  if (!record.inputSchema) return undefined;
  try {
    if (Value.Check(record.inputSchema, input ?? null)) return undefined;
    const errors = [...Value.Errors(record.inputSchema, input ?? null)].slice(0, 5).map((error) => {
      const at = (error as { path?: unknown }).path;
      return typeof at === "string" && at !== "" && at !== "/" ? `${at}: ${error.message}` : error.message;
    });
    return errors.join("; ") || "Schema validation failed";
  } catch {
    return "Program inputSchema could not be evaluated";
  }
};

const normalizeSchema = (value: unknown): Record<string, unknown> | undefined => {
  if (value === undefined) return undefined;
  if (!isRecord(value)) throw new Error("Program inputSchema must be a JSON Schema object");
  if (jsonBytes(value) > MAX_PROGRAM_SCHEMA_BYTES) throw new Error(`Program inputSchema exceeds ${MAX_PROGRAM_SCHEMA_BYTES} bytes`);
  const schema = JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
  try {
    Value.Check(schema, null);
  } catch {
    throw new Error("Program inputSchema is not a usable JSON Schema");
  }
  return schema;
};

const normalizeJevProgram = (value: unknown): Record<string, unknown> => {
  if (!isRecord(value)) throw new Error("jev programs require jevProgram: { name, code, inputSchema, outputSchema, requires }");
  if (typeof value.name !== "string" || typeof value.code !== "string" || !isRecord(value.inputSchema) || !isRecord(value.outputSchema) || !Array.isArray(value.requires)) {
    throw new Error("jevProgram must have name and code strings, inputSchema and outputSchema objects, and a requires array");
  }
  if (jsonBytes(value) > MAX_PROGRAM_CODE_CHARS + MAX_PROGRAM_SCHEMA_BYTES * 2) throw new Error("jevProgram is too large");
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
};

/** Validates save arguments into the content-addressed record (status candidate). */
export const buildProgramRecord = (input: FabricProgramSaveInput, defaultKernel: FabricProgramKernel, now = Date.now()): FabricProgramRecord => {
  const name = assertProgramName(input.name);
  const kind = input.kind ?? "fabric";
  if (kind !== "fabric" && kind !== "jev") throw new Error('Program kind must be "fabric" or "jev"');
  if (input.description !== undefined && (typeof input.description !== "string" || input.description.length > MAX_PROGRAM_DESCRIPTION_CHARS)) {
    throw new Error(`Program description must be a string of at most ${MAX_PROGRAM_DESCRIPTION_CHARS} characters`);
  }
  const inputSchema = normalizeSchema(input.inputSchema);
  let content: Pick<FabricProgramRecord, "kind" | "kernel" | "code" | "jevProgram">;
  if (kind === "fabric") {
    if (input.jevProgram !== undefined) throw new Error("fabric programs take code, not jevProgram");
    if (typeof input.code !== "string" || !input.code.trim() || input.code.length > MAX_PROGRAM_CODE_CHARS) {
      throw new Error(`fabric programs require non-empty code of at most ${MAX_PROGRAM_CODE_CHARS} characters`);
    }
    const kernel = input.kernel ?? defaultKernel;
    if (kernel !== "typescript" && kernel !== "python") throw new Error('Program kernel must be "typescript" or "python"');
    content = { kind, kernel, code: input.code };
  } else {
    if (input.code !== undefined || input.kernel !== undefined) throw new Error("jev programs take jevProgram, not code or kernel");
    content = { kind, jevProgram: normalizeJevProgram(input.jevProgram) };
  }
  const record: FabricProgramRecord = {
    version: 1,
    digest: programDigest({ ...content, ...(inputSchema ? { inputSchema } : {}) }),
    name,
    ...content,
    ...(typeof input.description === "string" && input.description ? { description: input.description } : {}),
    ...(inputSchema ? { inputSchema } : {}),
    createdAt: now,
    status: "candidate",
  };
  return record;
};

const readRecordFile = (file: string): FabricProgramRecord | undefined => {
  let text: string;
  try {
    text = fs.readFileSync(file, "utf8");
  } catch (error) {
    if ((error as { code?: string }).code === "ENOENT") return undefined;
    throw error;
  }
  const record = JSON.parse(text) as unknown;
  if (
    !isRecord(record) || record.version !== 1 || typeof record.digest !== "string" || typeof record.name !== "string" ||
    (record.kind !== "fabric" && record.kind !== "jev") || typeof record.createdAt !== "number" ||
    (record.status !== "candidate" && record.status !== "promoted" && record.status !== "retired")
  ) {
    throw new Error(`Malformed program record: ${path.basename(file)}`);
  }
  const typed = record as unknown as FabricProgramRecord;
  // Fail closed on edited content: the file name, the digest field, and the
  // content must agree before anything runs it.
  if (path.basename(file) !== `${typed.digest}.json` || programDigest(typed) !== typed.digest) {
    throw new Error(`Program record ${path.basename(file)} does not match its content digest`);
  }
  return typed;
};

export const summarizeProgram = (record: FabricProgramRecord): FabricProgramSummary => ({
  ref: programRef(record),
  name: record.name,
  digest: record.digest,
  kind: record.kind,
  ...(record.kernel ? { kernel: record.kernel } : {}),
  ...(record.description ? { description: record.description } : {}),
  createdAt: record.createdAt,
  status: record.status,
});

export const programsDirectory = (cwd: string): string =>
  path.join(process.env.PI_FABRIC_PROJECT_ROOT ?? cwd, ".pi", "fabric", "programs");

export class ProgramStore {
  constructor(readonly directory: string) {}

  async save(input: FabricProgramSaveInput, defaultKernel: FabricProgramKernel): Promise<{ record: FabricProgramRecord; created: boolean }> {
    const candidate = buildProgramRecord(input, defaultKernel);
    return this.#locked(async () => {
      const existing = readRecordFile(this.#file(candidate.digest));
      if (existing) {
        if (existing.name !== candidate.name) {
          throw new Error(`Identical program content is already saved as ${programRef(existing)}; reuse that ref`);
        }
        return { record: existing, created: false };
      }
      const index = this.#readIndex();
      const versions = index.names[candidate.name] ?? [];
      if (!index.names[candidate.name] && Object.keys(index.names).length >= MAX_PROGRAM_NAMES) {
        throw new Error(`Program store holds the maximum of ${MAX_PROGRAM_NAMES} names`);
      }
      if (versions.length >= MAX_PROGRAM_VERSIONS) {
        throw new Error(`Program ${candidate.name} holds the maximum of ${MAX_PROGRAM_VERSIONS} versions`);
      }
      await writeJsonAtomicAsync(this.#file(candidate.digest), candidate, { space: 2, newline: true });
      index.names[candidate.name] = [...versions, { digest: candidate.digest, createdAt: candidate.createdAt }];
      await this.#writeIndex(index);
      return { record: candidate, created: true };
    });
  }

  async list(filter: { name?: string; status?: FabricProgramStatus } = {}): Promise<FabricProgramSummary[]> {
    const index = this.#readIndex();
    const names = filter.name !== undefined ? [assertProgramName(filter.name)] : Object.keys(index.names).sort();
    const summaries: FabricProgramSummary[] = [];
    for (const name of names) {
      for (const entry of index.names[name] ?? []) {
        const record = readRecordFile(this.#file(entry.digest));
        if (!record || (filter.status && record.status !== filter.status)) continue;
        summaries.push(summarizeProgram(record));
      }
    }
    return summaries.sort((left, right) => left.name.localeCompare(right.name) || right.createdAt - left.createdAt);
  }

  /**
   * `name` resolves to the latest promoted version, else the latest
   * non-retired one; `name@<hex ≥12>` and a full digest name one version.
   */
  async resolve(ref: unknown, options: { requirePromoted?: boolean } = {}): Promise<FabricProgramRecord> {
    if (typeof ref !== "string" || !ref || ref.length > MAX_PROGRAM_REF_CHARS) throw new Error("Program ref must be a name, name@digest, or a full digest");
    let record: FabricProgramRecord | undefined;
    if (DIGEST_PATTERN.test(ref)) {
      record = readRecordFile(this.#file(ref));
      if (!record) throw new Error(`Unknown program: ${ref}`);
    } else {
      const at = ref.indexOf("@");
      const name = assertProgramName(at < 0 ? ref : ref.slice(0, at));
      const versions = this.#readIndex().names[name] ?? [];
      if (versions.length === 0) throw new Error(`Unknown program: ${name}`);
      if (at >= 0) {
        const prefix = ref.slice(at + 1);
        if (prefix.length < MIN_DIGEST_PREFIX || prefix.length > 64 || !/^[0-9a-f]+$/.test(prefix)) {
          throw new Error(`Program ref digest must be at least ${MIN_DIGEST_PREFIX} lowercase hex characters`);
        }
        const matches = versions.filter((entry) => entry.digest.startsWith(prefix));
        if (matches.length === 0) throw new Error(`Unknown program: ${ref}`);
        if (matches.length > 1) throw new Error(`Ambiguous program ref ${ref}; use more digest characters`);
        record = readRecordFile(this.#file(matches[0]!.digest));
        if (!record) throw new Error(`Unknown program: ${ref}`);
      } else {
        const records = [...versions]
          .sort((left, right) => right.createdAt - left.createdAt)
          .map((entry) => readRecordFile(this.#file(entry.digest)))
          .filter((entry): entry is FabricProgramRecord => entry !== undefined);
        record = records.find((entry) => entry.status === "promoted") ??
          (options.requirePromoted ? undefined : records.find((entry) => entry.status === "candidate"));
        if (!record) {
          throw new Error(options.requirePromoted
            ? `Program ${name} has no promoted version`
            : `Program ${name} has no runnable version (all retired); name a version explicitly`);
        }
      }
    }
    if (options.requirePromoted && record.status !== "promoted") {
      throw new Error(`Program ${programRef(record)} is ${record.status}, not promoted`);
    }
    return record;
  }

  /** Host/user only: never reachable from the programs provider. */
  promote(ref: string): Promise<FabricProgramRecord> {
    return this.#setStatus(ref, "promoted");
  }

  /** Host/user only: never reachable from the programs provider. */
  retire(ref: string): Promise<FabricProgramRecord> {
    return this.#setStatus(ref, "retired");
  }

  async #setStatus(ref: string, status: FabricProgramStatus): Promise<FabricProgramRecord> {
    return this.#locked(async () => {
      const record = await this.#resolveForStatus(ref);
      if (record.status === status) return record;
      const next: FabricProgramRecord = { ...record, status };
      await writeJsonAtomicAsync(this.#file(record.digest), next, { space: 2, newline: true });
      return next;
    });
  }

  // Bare names include retired versions here so a user can re-promote one.
  async #resolveForStatus(ref: string): Promise<FabricProgramRecord> {
    if (DIGEST_PATTERN.test(ref) || ref.includes("@")) return this.resolve(ref);
    const name = assertProgramName(ref);
    const latest = [...(this.#readIndex().names[name] ?? [])].sort((left, right) => right.createdAt - left.createdAt)[0];
    const record = latest ? readRecordFile(this.#file(latest.digest)) : undefined;
    if (!record) throw new Error(`Unknown program: ${name}`);
    return record;
  }

  #file(digest: string): string {
    if (!DIGEST_PATTERN.test(digest)) throw new Error(`Invalid program digest: ${digest}`);
    return path.join(this.directory, `${digest}.json`);
  }

  #readIndex(): ProgramIndex {
    let text: string;
    try {
      text = fs.readFileSync(path.join(this.directory, "index.json"), "utf8");
    } catch (error) {
      if ((error as { code?: string }).code === "ENOENT") return { version: 1, names: {} };
      throw error;
    }
    const index = JSON.parse(text) as unknown;
    if (!isRecord(index) || index.version !== 1 || !isRecord(index.names)) throw new Error("Malformed program index");
    const names: ProgramIndex["names"] = {};
    for (const [name, versions] of Object.entries(index.names)) {
      if (!PROGRAM_NAME_PATTERN.test(name) || !Array.isArray(versions)) throw new Error("Malformed program index");
      names[name] = versions.map((entry: unknown) => {
        if (!isRecord(entry) || typeof entry.digest !== "string" || !DIGEST_PATTERN.test(entry.digest) || typeof entry.createdAt !== "number") {
          throw new Error("Malformed program index");
        }
        return { digest: entry.digest, createdAt: entry.createdAt };
      });
    }
    return { version: 1, names };
  }

  #writeIndex(index: ProgramIndex): Promise<void> {
    return writeJsonAtomicAsync(path.join(this.directory, "index.json"), index, { space: 2, newline: true });
  }

  async #locked<T>(operation: () => Promise<T>): Promise<T> {
    // Load at mutation time, not registration: shared chunk splitting must not
    // pull the optional lock implementation into the startup graph.
    const { withExclusiveFileLockAsync } = await import("../core/file-lock.js");
    return withExclusiveFileLockAsync({
      directory: this.directory,
      lockName: ".lock",
      staleMs: LOCK_STALE_MS,
      attempts: LOCK_ATTEMPTS,
      delayMs: 10,
      requireDeadOwner: true,
      timeoutMessage: "Timed out waiting for the Fabric program store lock",
    }, operation);
  }
}
