import { accessSync, constants, statSync } from "node:fs";
import path from "node:path";
import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Type, type TSchema } from "typebox";
import { PiCodingAgent } from "../core/host-sdk-runtime.js";
import type { PiShellToolName } from "../core/pi-tools.js";

// Per-call execution directory for Pi's shell tools.
//
// Pi's shell backend already takes cwd as a first-class parameter
// (BashOperations.exec(command, cwd, options)), and pi-agent-core's harness
// shell goes further with a per-command ShellExecOptions.cwd. The shell tool
// schemas omit it, so Fabric advertises and honors it instead of silently
// executing in the session directory.
//
// The alternative — telling models to write `cd <dir> && <command>` — buries
// the directory in the command string, where it defeats extensions that match
// on command names (permission prompts, sandboxing) and hides the target from
// Fabric's own approval classifier.

export const PI_BASH_CWD_KEY = "cwd";

/**
 * Resolve and validate a single shell call's execution directory.
 *
 * Relative paths resolve against the session cwd. Unlike the leaf-agent
 * resolver this deliberately does NOT canonicalize symlinks: agents commonly
 * target git worktrees whose paths are symlinks, and rewriting those to their
 * real targets changes what `pwd` reports inside the command and breaks
 * tooling that keys on the worktree path. `path.resolve` normalizes `..`
 * lexically, which is all the approval classifier needs to see a truthful
 * absolute target.
 *
 * Containment is intentionally not enforced. Models can already reach any
 * directory by changing directories inside a shell command, which Fabric
 * neither inspects nor contains. Directory containment belongs to pi's
 * project-trust layer or a shell spawn hook.
 */
const resolvePiShellCwd = (
  toolName: PiShellToolName,
  sessionCwd: string,
  requested: unknown,
): string => {
  if (typeof requested !== "string" || requested.trim().length === 0) {
    throw new Error(
      `Invalid pi.${toolName} cwd ${JSON.stringify(requested)}: path must be a non-empty string`,
    );
  }
  const resolved = path.isAbsolute(requested)
    ? path.resolve(requested)
    : path.resolve(sessionCwd, requested);
  try {
    if (!statSync(resolved).isDirectory()) throw new Error("path is not a directory");
    accessSync(resolved, constants.R_OK | constants.X_OK);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Invalid pi.${toolName} cwd ${JSON.stringify(requested)} (${resolved}): ${reason}`,
    );
  }
  return resolved;
};

/** Compatibility wrapper retained for existing callers and tests. */
export const resolvePiBashCwd = (sessionCwd: string, requested: unknown): string =>
  resolvePiShellCwd("bash", sessionCwd, requested);

/**
 * Rewrite a shell call's cwd while leaving every other argument untouched.
 *
 * The extra key stays in the arguments so events, approval, and previews see
 * the resolved absolute target. The cwd-bound definition selected by the
 * provider is what ultimately passes it to the native shell backend.
 */
export const resolveShellCwdArgument = (
  toolName: PiShellToolName,
  sessionCwd: string,
  args: Record<string, unknown>,
): Record<string, unknown> =>
  Object.hasOwn(args, PI_BASH_CWD_KEY)
    ? {
        ...args,
        [PI_BASH_CWD_KEY]: resolvePiShellCwd(
          toolName,
          sessionCwd,
          args[PI_BASH_CWD_KEY],
        ),
      }
    : args;

/** Compatibility wrapper retained for existing callers and tests. */
export const resolveBashCwdArgument = (
  sessionCwd: string,
  args: Record<string, unknown>,
): Record<string, unknown> => resolveShellCwdArgument("bash", sessionCwd, args);

const CWD_PROPERTY = Type.Optional(
  Type.String({
    description:
      "Execution directory for this command; relative paths resolve from the session cwd.",
  }),
);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * Declare `cwd` on a shell descriptor.
 *
 * Rebuilt as a fresh TObject rather than spread-cloned: TypeBox schemas carry
 * Symbol keys that a spread drops, which would leave Value.Check unable to
 * validate the descriptor. Property values are reused by reference, so they
 * retain their own Symbols.
 */
export const withShellCwdSchema = (schema: unknown): unknown => {
  if (!isRecord(schema) || !isRecord(schema.properties)) return schema;
  if (Object.hasOwn(schema.properties, PI_BASH_CWD_KEY)) return schema;
  const { type: _type, properties, required, ...rest } = schema;
  return Type.Object(
    { ...properties, [PI_BASH_CWD_KEY]: CWD_PROPERTY } as Record<string, TSchema>,
    { ...rest, ...(Array.isArray(required) ? { required } : {}) },
  );
};

/** Compatibility wrapper retained for existing callers and tests. */
export const withBashCwdSchema = (schema: unknown): unknown => withShellCwdSchema(schema);

const MAX_CACHED_DEFINITIONS = 16;

export type ShellDefinitionFactory = (cwd: string) => ToolDefinition<any, any, any>;

/**
 * PowerShell entered Pi's host API after Fabric's minimum supported version.
 * Namespace lookup is deliberate: a named ESM import would reject the whole
 * Fabric module before an older host could reach this feature gate.
 */
export const resolvePowerShellToolDefinitionFactory = (
  moduleExports: unknown,
): ShellDefinitionFactory | undefined => {
  if (
    moduleExports === null ||
    (typeof moduleExports !== "object" && typeof moduleExports !== "function")
  ) {
    return undefined;
  }
  const candidate = Reflect.get(moduleExports, "createPowerShellToolDefinition");
  return typeof candidate === "function" ? candidate as ShellDefinitionFactory : undefined;
};

export const powerShellToolDefinitionFactory =
  resolvePowerShellToolDefinitionFactory(PiCodingAgent);

/** Definitions bound to execution directories, with a small LRU cache. */
class ShellCwdDefinitions {
  readonly #cache = new Map<string, ToolDefinition<any, any, any>>();

  constructor(private readonly createDefinition: ShellDefinitionFactory) {}

  get(cwd: string): ToolDefinition<any, any, any> {
    const cached = this.#cache.get(cwd);
    if (cached) {
      this.#cache.delete(cwd);
      this.#cache.set(cwd, cached);
      return cached;
    }
    const definition = this.createDefinition(cwd);
    this.#cache.set(cwd, definition);
    if (this.#cache.size > MAX_CACHED_DEFINITIONS) {
      const oldest = this.#cache.keys().next();
      if (!oldest.done) this.#cache.delete(oldest.value);
    }
    return definition;
  }
}

export class BashCwdDefinitions extends ShellCwdDefinitions {
  constructor() {
    super(PiCodingAgent.createBashToolDefinition);
  }
}

export class PowerShellCwdDefinitions extends ShellCwdDefinitions {
  constructor(createDefinition: ShellDefinitionFactory) {
    super(createDefinition);
  }
}
