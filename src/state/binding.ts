import { execFile } from "node:child_process";
import type { FabricSchemaMode } from "../config.js";
import type {
  StateCertificateBinding,
  StateCertificateObserved,
  StateCertificateRequester,
} from "./types.js";

// Bound certificates: a caller-supplied binding (commit, spec digest, node
// address, ...) plus the host's own git observation of the verification cwd.
// These are host payload metadata. They never feed the verified state kernel
// (`headReadable`); the commit check can only add a blocking failure.

export const STATE_BINDING_MAX_KEYS = 16;
export const STATE_BINDING_VALUE_MAX_CHARS = 512;
const STATE_BINDING_KEY = /^[a-z][a-zA-Z0-9_.-]{0,63}$/;
const GIT_OBJECT_NAME = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const GIT_PROBE_TIMEOUT_MS = 5_000;
const GIT_HEAD_MAX_BUFFER = 1024;
// Any porcelain output means dirty, so the buffer stays bounded; overflowing
// it still proves at least one changed path.
const GIT_STATUS_MAX_BUFFER = 64 * 1024;

const SCHEMA_MODES: ReadonlySet<string> = new Set(["off", "audit", "enforce"]);

/** Validate a caller binding; throws a descriptive error instead of dropping input. */
export const normalizeStateBinding = (
  value: unknown,
): StateCertificateBinding | undefined => {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("state.verify binding must be an object of string values");
  }
  const keys = Object.keys(value);
  if (keys.length > STATE_BINDING_MAX_KEYS) {
    throw new Error(
      `state.verify binding has ${keys.length} keys; at most ${STATE_BINDING_MAX_KEYS} are allowed`,
    );
  }
  const binding: StateCertificateBinding = {};
  for (const key of keys.sort()) {
    if (!STATE_BINDING_KEY.test(key)) {
      throw new Error(
        `state.verify binding key ${JSON.stringify(key.slice(0, 80))} must match [a-z][a-zA-Z0-9_.-]{0,63}`,
      );
    }
    const item = (value as Record<string, unknown>)[key];
    if (typeof item !== "string") {
      throw new Error(`state.verify binding.${key} must be a string`);
    }
    if (item.length > STATE_BINDING_VALUE_MAX_CHARS) {
      throw new Error(
        `state.verify binding.${key} exceeds ${STATE_BINDING_VALUE_MAX_CHARS} characters`,
      );
    }
    binding[key] = item;
  }
  return keys.length > 0 ? binding : undefined;
};

/** Lenient read-side parse: malformed stored bindings fold as absent. */
export const toStateBinding = (value: unknown): StateCertificateBinding | undefined => {
  try {
    return normalizeStateBinding(value);
  } catch {
    return undefined;
  }
};

export const toStateObserved = (value: unknown): StateCertificateObserved | undefined => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const raw = value as Record<string, unknown>;
  const observed: StateCertificateObserved = {
    ...(typeof raw.commit === "string" && GIT_OBJECT_NAME.test(raw.commit)
      ? { commit: raw.commit }
      : {}),
    ...(typeof raw.dirty === "boolean" ? { dirty: raw.dirty } : {}),
  };
  return observed.commit !== undefined || observed.dirty !== undefined ? observed : undefined;
};

export const toStateRequester = (value: unknown): StateCertificateRequester | undefined =>
  value === "program" || value === "host" ? value : undefined;

export const toStateSchemaMode = (value: unknown): FabricSchemaMode | undefined =>
  typeof value === "string" && SCHEMA_MODES.has(value) ? (value as FabricSchemaMode) : undefined;

const runGit = (
  cwd: string,
  args: string[],
  maxBuffer: number,
  signal: AbortSignal | undefined,
): Promise<{ stdout: string; overflow: boolean } | undefined> =>
  new Promise((resolve) => {
    try {
      execFile(
        "git",
        ["-C", cwd, ...args],
        {
          encoding: "utf8",
          maxBuffer,
          timeout: GIT_PROBE_TIMEOUT_MS,
          windowsHide: true,
          // Observation must not take the index lock or refresh stat data.
          env: { ...process.env, GIT_OPTIONAL_LOCKS: "0" },
          ...(signal ? { signal } : {}),
        },
        (error, stdout) => {
          if (!error) {
            resolve({ stdout, overflow: false });
            return;
          }
          const code = (error as { code?: unknown }).code;
          if (code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER") {
            resolve({ stdout, overflow: true });
            return;
          }
          resolve(undefined);
        },
      );
    } catch {
      resolve(undefined);
    }
  });

/**
 * Observe HEAD and worktree cleanliness for `cwd`. Returns undefined outside a
 * git work tree or before the first commit; a failed status probe omits `dirty`.
 */
export const observeGitWorkspace = async (
  cwd: string,
  signal?: AbortSignal,
): Promise<StateCertificateObserved | undefined> => {
  if (signal?.aborted) return undefined;
  const head = await runGit(cwd, ["rev-parse", "--verify", "--quiet", "HEAD"], GIT_HEAD_MAX_BUFFER, signal);
  const commit = head && !head.overflow ? head.stdout.trim().toLowerCase() : "";
  if (!GIT_OBJECT_NAME.test(commit)) return undefined;
  const status = await runGit(
    cwd,
    ["status", "--porcelain", "--untracked-files=normal"],
    GIT_STATUS_MAX_BUFFER,
    signal,
  );
  return {
    commit,
    ...(status ? { dirty: status.overflow || status.stdout.trim().length > 0 } : {}),
  };
};
