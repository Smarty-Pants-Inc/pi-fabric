import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { lockFile } from "./file-lock.js";

export type LaunchFenceState = "open" | "creating" | "closed-unstarted" | "closed-uncertain" | "closed-started";
export type LaunchRole = "org-agent" | "project-agent" | "worktree-agent" | "task-agent" | "review-agent" | "resident-host" | "actor-activation";
export type LaunchOriginClass = "live-human" | "background-agent";

export interface LaunchRecord {
  schema: "smarty.launch-record.v1";
  native_launch_id: string;
  launch_spec_id: string;
  generation: number;
  fence: { token: number; state: LaunchFenceState };
  host: string;
  unit: string;
  boot_id: string | null;
  invocation_id: string | null;
  main_pid_start: string | null;
  org_id: string;
  work_id: string;
  parent_native_launch_id: string | null;
  role: LaunchRole;
  profile: string;
  source_pin: string;
  memory_profile: string;
  origin: { class: LaunchOriginClass; origin_ref: string };
  terminal: { outcome: "never_started" | "exited"; at: string } | null;
}

export interface LaunchPlacementMetadata {
  nativeLaunchId?: string;
  launchSpecId?: string;
  generation?: number;
  fenceToken?: number;
  orgId: string;
  workId: string;
  parentNativeLaunchId?: string | null;
  role: LaunchRole;
  profile: string;
  sourcePin: string;
  memoryProfile: string;
  origin: { class: LaunchOriginClass; originRef: string };
}

const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$/u;
const REF = /^[^\u0000-\u001f\u007f]+$/u;
const BOOT = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/u;
const INVOCATION = /^[0-9a-f]{32}$/u;
const PID_START = /^[1-9][0-9]{0,9}:[1-9][0-9]{0,19}$/u;
const HOST = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,252}$/u;
const TERMINAL_AT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/u;
const ROLES: readonly LaunchRole[] = ["org-agent", "project-agent", "worktree-agent", "task-agent", "review-agent", "resident-host", "actor-activation"];

export const defaultLaunchRecordRoot = (): string => path.join(os.homedir(), ".local", "state", "smarty", "launches");
export const launchRecordPath = (nativeLaunchId: string, root = defaultLaunchRecordRoot()): string => path.join(root, `${nativeLaunchId}.json`);
export const unitName = (nativeLaunchId: string, generation: number): string => {
  assertSafeId(nativeLaunchId, "native_launch_id");
  assertCounter(generation, "generation");
  if (String(generation).length > 16) throw new Error("Invalid launch record generation for unit");
  return `smarty-launch-${nativeLaunchId}-g${generation}.service`;
};

function assertSafeId(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !SAFE_ID.test(value)) throw new Error(`Invalid launch record ${field}`);
}
function assertRef(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.length < 1 || value.length > 200 || !REF.test(value)) throw new Error(`Invalid launch record ${field}`);
}
function assertCounter(value: unknown, field: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) throw new Error(`Invalid launch record ${field}`);
}
function assertHost(value: unknown): asserts value is string {
  if (typeof value !== "string" || !HOST.test(value)) throw new Error("Invalid launch record host");
}
function assertBoot(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !BOOT.test(value)) throw new Error(`Invalid launch record ${field}`);
}
function assertInvocation(value: unknown): asserts value is string {
  if (typeof value !== "string" || !INVOCATION.test(value)) throw new Error("Invalid launch record invocation_id");
}
function assertPidStart(value: unknown): asserts value is string {
  if (typeof value !== "string" || !PID_START.test(value)) throw new Error("Invalid launch record main_pid_start");
}
function assertOrigin(value: unknown): asserts value is LaunchRecord["origin"] {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid launch record origin");
  const origin = value as Record<string, unknown>;
  if (Object.keys(origin).some(key => key !== "class" && key !== "origin_ref") ||
      (origin.class !== "live-human" && origin.class !== "background-agent")) throw new Error("Invalid launch record origin");
  assertRef(origin.origin_ref, "origin.origin_ref");
};

export function validateLaunchRecord(value: unknown): asserts value is LaunchRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid launch record");
  const row = value as Record<string, unknown>;
  const required = ["schema", "native_launch_id", "launch_spec_id", "generation", "fence", "host", "unit", "boot_id", "invocation_id",
    "main_pid_start", "org_id", "work_id", "parent_native_launch_id", "role", "profile", "source_pin", "memory_profile", "origin", "terminal"];
  if (Object.keys(row).some(key => !required.includes(key)) || required.some(key => !Object.hasOwn(row, key))) throw new Error("Invalid launch record fields");
  if (row.schema !== "smarty.launch-record.v1") throw new Error("Invalid launch record schema");
  assertSafeId(row.native_launch_id, "native_launch_id");
  assertRef(row.launch_spec_id, "launch_spec_id");
  assertCounter(row.generation, "generation");
  if (!row.fence || typeof row.fence !== "object" || Array.isArray(row.fence)) throw new Error("Invalid launch record fence");
  const fence = row.fence as Record<string, unknown>;
  if (Object.keys(fence).some(key => key !== "token" && key !== "state")) throw new Error("Invalid launch record fence fields");
  assertCounter(fence.token, "fence.token");
  if (!(["open", "creating", "closed-unstarted", "closed-uncertain", "closed-started"] as string[]).includes(String(fence.state))) throw new Error("Invalid launch record fence.state");
  assertHost(row.host);
  const expectedUnit = unitName(row.native_launch_id, row.generation);
  if (row.unit !== expectedUnit) throw new Error("Invalid launch record unit");
  if (row.boot_id !== null) assertBoot(row.boot_id, "boot_id");
  if (row.invocation_id !== null) assertInvocation(row.invocation_id);
  if (row.main_pid_start !== null) assertPidStart(row.main_pid_start);
  assertRef(row.org_id, "org_id"); assertRef(row.work_id, "work_id");
  if (row.parent_native_launch_id !== null) assertSafeId(row.parent_native_launch_id, "parent_native_launch_id");
  if (row.parent_native_launch_id === row.native_launch_id) throw new Error("Launch record cannot parent itself");
  if (typeof row.role !== "string" || !ROLES.includes(row.role as LaunchRole)) throw new Error("Invalid launch record role");
  assertRef(row.profile, "profile"); assertRef(row.source_pin, "source_pin"); assertRef(row.memory_profile, "memory_profile");
  assertOrigin(row.origin);
  if (row.terminal !== null) {
    const terminal = row.terminal as { outcome?: unknown; at?: unknown };
    if (!row.terminal || typeof row.terminal !== "object" || Array.isArray(row.terminal) ||
        Object.keys(row.terminal).some(key => key !== "outcome" && key !== "at") ||
        (terminal.outcome !== "never_started" && terminal.outcome !== "exited") ||
        typeof terminal.at !== "string" || !TERMINAL_AT.test(terminal.at) || !Number.isFinite(Date.parse(terminal.at))) {
      throw new Error("Invalid launch record terminal");
    }
  }
  const state = fence.state as LaunchFenceState;
  if (state === "open") {
    if (row.invocation_id !== null || row.main_pid_start !== null || row.terminal !== null) throw new Error("Open launch record has native identity");
  } else if (state === "creating" || state === "closed-uncertain") {
    if (row.boot_id === null || row.invocation_id !== null || row.main_pid_start !== null || row.terminal !== null) throw new Error("Uncertain launch record identity");
  } else if (state === "closed-unstarted") {
    if (row.boot_id === null || row.invocation_id !== null || row.main_pid_start !== null || (row.terminal as { outcome?: unknown } | null)?.outcome === "exited") throw new Error("Unstarted launch record identity");
  } else if (state === "closed-started") {
    if (row.boot_id === null || row.invocation_id === null || row.main_pid_start === null || (row.terminal as { outcome?: unknown } | null)?.outcome === "never_started") throw new Error("Started launch record identity");
  }
};

export const readBootId = (): string | null => {
  try {
    const value = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim().toLowerCase();
    return BOOT.test(value) ? value : null;
  } catch { return null; }
};

export const processPidStart = (pid: number): string | null => {
  if (!Number.isSafeInteger(pid) || pid < 1 || process.platform !== "linux") return null;
  try {
    const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/u);
    const ticks = fields[19];
    return ticks && /^[1-9][0-9]{0,19}$/u.test(ticks) ? `${pid}:${ticks}` : null;
  } catch { return null; }
};

const hostName = (): string => {
  const value = os.hostname();
  assertHost(value);
  return value;
};
const readPrivateRecord = (file: string): unknown => {
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || (stat.mode & 0o077) !== 0 || stat.nlink !== 1) throw new Error("Launch record path is not a private regular file");
  return JSON.parse(fs.readFileSync(file, "utf8")) as unknown;
};
const writeRecord = (file: string, record: LaunchRecord): void => {
  validateLaunchRecord(record);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const directory = fs.statSync(path.dirname(file));
  if (!directory.isDirectory() || (directory.mode & 0o077) !== 0 || (process.getuid && directory.uid !== process.getuid())) throw new Error("Launch record directory is not private");
  const existing = fs.lstatSync(file, { throwIfNoEntry: false });
  if (existing && (!existing.isFile() || (existing.mode & 0o077) !== 0 || existing.nlink !== 1)) throw new Error("Launch record path is not a private regular file");
  writeJsonAtomic(file, record, { mode: 0o600, dirMode: 0o700, durable: true });
  const stat = fs.statSync(file);
  if (!stat.isFile() || (stat.mode & 0o777) !== 0o600 || stat.nlink !== 1) throw new Error("Launch record publication is not private");
};

export const makeLaunchRecord = (metadata: LaunchPlacementMetadata, nativeLaunchId = metadata.nativeLaunchId ?? randomUUID(), generation = metadata.generation ?? 1): LaunchRecord => {
  assertSafeId(nativeLaunchId, "native_launch_id"); assertCounter(generation, "generation");
  assertCounter(metadata.fenceToken ?? 1, "fence.token");
  assertRef(metadata.launchSpecId ?? "resident-host", "launch_spec_id");
  const record: LaunchRecord = {
    schema: "smarty.launch-record.v1", native_launch_id: nativeLaunchId, launch_spec_id: metadata.launchSpecId ?? "resident-host",
    generation, fence: { token: metadata.fenceToken ?? 1, state: "open" }, host: hostName(), unit: unitName(nativeLaunchId, generation),
    boot_id: readBootId(), invocation_id: null, main_pid_start: null, org_id: metadata.orgId, work_id: metadata.workId,
    parent_native_launch_id: metadata.parentNativeLaunchId ?? null, role: metadata.role, profile: metadata.profile,
    source_pin: metadata.sourcePin, memory_profile: metadata.memoryProfile, origin: { class: metadata.origin.class, origin_ref: metadata.origin.originRef }, terminal: null,
  };
  validateLaunchRecord(record);
  return record;
};

export class LaunchRecordStore {
  readonly root: string;
  constructor(root = defaultLaunchRecordRoot()) { this.root = path.resolve(root); }
  path(nativeLaunchId: string): string { assertSafeId(nativeLaunchId, "native_launch_id"); return launchRecordPath(nativeLaunchId, this.root); }
  get(nativeLaunchId: string): LaunchRecord | undefined {
    try {
      const value = readPrivateRecord(this.path(nativeLaunchId));
      validateLaunchRecord(value); return structuredClone(value);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
  }
  list(): LaunchRecord[] {
    let names: string[];
    try { names = fs.readdirSync(this.root); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
    return names.filter(name => /^[A-Za-z0-9][A-Za-z0-9_-]{0,95}\.json$/u.test(name)).map(name => {
      const value = readPrivateRecord(path.join(this.root, name));
      validateLaunchRecord(value); return value;
    });
  }
  prepare(record: LaunchRecord): LaunchRecord {
    validateLaunchRecord(record);
    const file = this.path(record.native_launch_id);
    const current = this.get(record.native_launch_id);
    if (current) {
      if (current.generation === record.generation) {
        if (JSON.stringify(current) !== JSON.stringify(record)) throw new Error("Launch record identity already exists with different bytes");
        return current;
      }
      if (record.generation <= current.generation) throw new Error("Launch record generation moved backwards");
    }
    writeRecord(file, record);
    return structuredClone(record);
  }
  async admit(record: LaunchRecord, enforce = false): Promise<LaunchRecord> {
    const lockPath = path.join(this.root, ".admission.lock");
    fs.mkdirSync(this.root, { recursive: true, mode: 0o700 });
    const directory = fs.statSync(this.root);
    if (!directory.isDirectory() || (directory.mode & 0o077) !== 0 || (process.getuid && directory.uid !== process.getuid())) throw new Error("Launch record directory is not private");
    const fd = await lockFile(lockPath, 2, process.platform === "linux");
    try {
      this.prepare(record);
      if (enforce) {
        if (!record.parent_native_launch_id) throw new Error("Enforce launch requires a trusted parent record");
        authorizeLaunchChild(record.parent_native_launch_id, record, this.census());
      }
      return this.transition(record.native_launch_id, record.generation, "creating");
    } finally { fs.closeSync(fd); }
  }
  verify(nativeLaunchId: string, generation: number, native?: { boot_id: string; invocation_id: string; main_pid_start: string }): LaunchRecord {
    const current = this.get(nativeLaunchId);
    if (!current || current.generation !== generation) throw new Error("Unknown launch record generation");
    if (current.fence.state === "closed-started" || current.fence.state === "closed-unstarted") return current;
    if (native) return this.transition(nativeLaunchId, generation, "closed-started", native);
    if (current.fence.state === "open" || current.fence.state === "creating") return this.transition(nativeLaunchId, generation, "closed-uncertain");
    return current;
  }
  transition(nativeLaunchId: string, generation: number, state: LaunchFenceState, native?: { boot_id: string; invocation_id: string; main_pid_start: string }): LaunchRecord {
    const current = this.get(nativeLaunchId);
    if (!current || current.generation !== generation) throw new Error("Unknown launch record generation");
    if (current.fence.state === state) return current;
    const allowed: Record<LaunchFenceState, LaunchFenceState[]> = {
      open: ["creating", "closed-unstarted", "closed-uncertain"], creating: ["closed-started", "closed-uncertain"], "closed-unstarted": [], "closed-uncertain": ["closed-started"], "closed-started": [],
    };
    if (!allowed[current.fence.state].includes(state)) throw new Error(`Invalid launch record transition ${current.fence.state}->${state}`);
    const next = structuredClone(current);
    next.fence.state = state;
    if (state === "creating" || state === "closed-uncertain") {
      if (!next.boot_id) next.boot_id = readBootId();
      if (!next.boot_id) throw new Error("Cannot fence launch without observed boot id");
      next.invocation_id = null; next.main_pid_start = null; next.terminal = null;
    } else if (state === "closed-started") {
      if (!native) throw new Error("Started launch record requires native identity");
      next.boot_id = native.boot_id; next.invocation_id = native.invocation_id; next.main_pid_start = native.main_pid_start; next.terminal = null;
    } else if (state === "closed-unstarted") {
      if (!next.boot_id) next.boot_id = readBootId();
      if (!next.boot_id) throw new Error("Cannot close launch without observed boot id");
      next.invocation_id = null; next.main_pid_start = null; next.terminal = { outcome: "never_started", at: new Date().toISOString() };
    }
    writeRecord(this.path(nativeLaunchId), next); return next;
  }
  terminal(nativeLaunchId: string, generation: number): LaunchRecord {
    const current = this.get(nativeLaunchId);
    if (!current || current.generation !== generation || current.fence.state !== "closed-started") throw new Error("Launch is not a registered started record");
    if (current.terminal) return current;
    const next = structuredClone(current); next.terminal = { outcome: "exited", at: new Date().toISOString() }; writeRecord(this.path(nativeLaunchId), next); return next;
  }
  census(): LaunchRecord[] { return this.list(); }
}

const rootRoles = new Set<LaunchRole>(["org-agent", "project-agent", "worktree-agent", "review-agent"]);
export const resolveLaunchLineage = (nativeLaunchId: string, records: readonly LaunchRecord[]): { rootNativeLaunchId: string; depth: number } => {
  const byId = new Map(records.map(record => [record.native_launch_id, record]));
  const seen = new Set<string>(); let current = byId.get(nativeLaunchId); let depth = 0;
  if (!current) throw new Error("Unknown launch record");
  while (current.parent_native_launch_id !== null) {
    if (seen.has(current.native_launch_id)) throw new Error("Launch record ancestry cycle");
    seen.add(current.native_launch_id); depth++;
    if (depth > 2) throw new Error("Launch record ancestry exceeds two edges");
    current = byId.get(current.parent_native_launch_id);
    if (!current) throw new Error("Launch record parent is missing");
  }
  return { rootNativeLaunchId: current.native_launch_id, depth };
};

export const validateLaunchCensus = (records: readonly LaunchRecord[]): Map<string, { rootNativeLaunchId: string; depth: number }> => {
  const byId = new Map<string, LaunchRecord>();
  for (const record of records) { validateLaunchRecord(record); if (byId.has(record.native_launch_id)) throw new Error("Duplicate launch record"); byId.set(record.native_launch_id, record); }
  const lineage = new Map<string, { rootNativeLaunchId: string; depth: number }>();
  for (const record of records) {
    const value = resolveLaunchLineage(record.native_launch_id, records); lineage.set(record.native_launch_id, value);
    if (record.parent_native_launch_id === null && !rootRoles.has(record.role)) throw new Error("Non-root role without parent");
    if (record.parent_native_launch_id !== null) {
      const parent = byId.get(record.parent_native_launch_id)!;
      if (parent.role === "resident-host" && record.role !== "actor-activation") throw new Error("Resident host may create only actor activations");
      if (record.role === "resident-host" && value.depth !== 1) throw new Error("Resident host must be a direct root child");
      if (record.role === "actor-activation" && value.depth !== 2) throw new Error("Actor activation must be a resident child");
      if (record.role === "actor-activation" && record.origin.class !== "background-agent") throw new Error("Actor activation must be background work");
      if (record.org_id !== parent.org_id || record.work_id !== parent.work_id) throw new Error("Cross-root launch assignment");
    }
  }
  const residents = new Set<string>();
  for (const record of records) if (record.role === "resident-host" && record.terminal?.outcome !== "exited") {
    const root = lineage.get(record.native_launch_id)!.rootNativeLaunchId;
    if (residents.has(root)) throw new Error("More than one resident host per root");
    residents.add(root);
  }
  return lineage;
};

export const authorizeLaunchChild = (callerNativeLaunchId: string, child: LaunchRecord, records: readonly LaunchRecord[]): void => {
  const census = [...records.filter(record => record.native_launch_id !== child.native_launch_id), child];
  const lineage = validateLaunchCensus(census);
  const caller = census.find(record => record.native_launch_id === callerNativeLaunchId);
  if (!caller) throw new Error("Unknown launch caller");
  if (child.parent_native_launch_id !== callerNativeLaunchId) throw new Error("Launch parent is not the trusted caller");
  const callerLineage = lineage.get(callerNativeLaunchId)!; const childLineage = lineage.get(child.native_launch_id)!;
  if (callerLineage.rootNativeLaunchId !== childLineage.rootNativeLaunchId) throw new Error("Cross-root activation");
  if (caller.role === "resident-host" && child.role !== "actor-activation") throw new Error("Resident host child role refused");
};