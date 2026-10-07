import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricState } from "../fabric-state.js";
import type { FabricProgramRunReplyV1, FabricProgramRunRequestV1 } from "../protocol.js";
import { hostProgramRunSource } from "./source.js";
import {
  MAX_PROGRAM_REF_CHARS,
  normalizeProgramInput,
  ProgramStore,
  programRef,
  programsDirectory,
  type FabricProgramSummary,
} from "./store.js";

// Host-invoked program runs: `/fabric run`, `/fabric programs ...`, and the
// `pi-fabric:program:run:v1` event. A host run is a one-call program that
// invokes `programs.run`, so it takes the session's root capability view and
// the same approval policy as a model call; only the caller differs, and the
// trace records it as `invokedBy: "host"`.

export const PROGRAM_RUN_MESSAGE_TYPE = "pi-fabric-program-run";
const MAX_MESSAGE_VALUE_CHARS = 8_000;

export interface ProgramHostDeps {
  state: Pick<FabricState, "ensure" | "config" | "execution" | "registry">;
  pi: Pick<ExtensionAPI, "sendMessage">;
}

export interface HostProgramRunRequest {
  ref: string;
  input?: unknown;
  requirePromoted?: boolean;
  signal?: AbortSignal;
}

const errorText = (error: unknown): string => error instanceof Error ? error.message : String(error);

// Local on purpose: importing the shared util would split it into its own
// startup chunk.
const bounded = (value: string, maxChars: number): string =>
  value.length <= maxChars ? value : `${value.slice(0, maxChars)}\n… ${value.length - maxChars} characters omitted`;

const valueText = (value: unknown): string => {
  if (value === undefined) return "(no result)";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    return String(value);
  }
};

export const runHostProgram = async (
  deps: ProgramHostDeps,
  context: ExtensionContext,
  request: HostProgramRunRequest,
): Promise<FabricProgramRunReplyV1> => {
  let program: string | undefined;
  let reply: FabricProgramRunReplyV1;
  let details: Record<string, unknown> = {};
  try {
    await deps.state.ensure(context);
    if (!deps.state.registry.has("programs")) throw new Error("Saved programs are unavailable in this session");
    const store = new ProgramStore(programsDirectory(context.cwd));
    const record = await store.resolve(request.ref, { requirePromoted: request.requirePromoted === true });
    program = programRef(record, true);
    const input = normalizeProgramInput(request.input);
    const result = await deps.state.execution.execute({
      // Pin the resolved digest so a concurrent promotion cannot swap versions.
      code: hostProgramRunSource(deps.state.config.executor.kernel, {
        ref: record.digest,
        ...(input !== undefined ? { input } : {}),
        ...(request.requirePromoted ? { requirePromoted: true } : {}),
      }),
      signal: request.signal,
      parentToolCallId: `fabric_program_${randomUUID()}`,
      context,
      display: { name: `Program ${record.name}` },
      invokedBy: "host",
      onPartial() {},
    });
    details = { trace: result.trace };
    reply = result.success
      ? { ok: true, program, value: result.value, logs: result.logs }
      : {
          ok: false,
          program,
          error: result.error
            ?? (result.typeErrors?.length ? `Type errors: ${result.typeErrors.map((error) => error.message).join("; ")}` : "Program run failed"),
        };
  } catch (error) {
    reply = { ok: false, error: errorText(error), ...(program ? { program } : {}) };
  }
  const label = reply.program ?? request.ref;
  const body = reply.ok
    ? [...reply.logs, valueText(reply.value)].join("\n")
    : `Error: ${reply.error}`;
  try {
    deps.pi.sendMessage({
      customType: PROGRAM_RUN_MESSAGE_TYPE,
      content: `Program ${label} (host run, ${reply.ok ? "succeeded" : "failed"})\n${bounded(body, MAX_MESSAGE_VALUE_CHARS)}`,
      display: true,
      details: {
        version: 1,
        invokedBy: "host",
        ...(reply.program ? { program: reply.program } : { ref: request.ref }),
        success: reply.ok,
        ...(reply.ok ? {} : { error: reply.error }),
        ...details,
      },
    }, { triggerTurn: false });
  } catch {
    // The transcript entry is best effort; the reply still reports the run.
  }
  return reply;
};

const isAbortSignal = (value: unknown): value is AbortSignal =>
  typeof value === "object" && value !== null && typeof (value as AbortSignal).aborted === "boolean" &&
  typeof (value as AbortSignal).addEventListener === "function";

/** `pi-fabric:program:run:v1`; replies exactly once, never throws. */
export const handleFabricProgramRunEvent = async (
  value: unknown,
  deps: ProgramHostDeps & { context: ExtensionContext | undefined },
): Promise<void> => {
  const request = value as Partial<FabricProgramRunRequestV1>;
  if (typeof request?.reply !== "function") return;
  const respond = request.reply;
  let replied = false;
  const once = (result: FabricProgramRunReplyV1): void => {
    if (replied) return;
    replied = true;
    try {
      respond(result);
    } catch {
      // A throwing listener must not turn into an unhandled rejection.
    }
  };
  if (typeof request.ref !== "string" || !request.ref || request.ref.length > MAX_PROGRAM_REF_CHARS) {
    once({ ok: false, error: "Invalid program run request: ref must be a non-empty string" });
    return;
  }
  if (request.requirePromoted !== undefined && typeof request.requirePromoted !== "boolean") {
    once({ ok: false, error: "Invalid program run request: requirePromoted must be a boolean" });
    return;
  }
  if (request.signal !== undefined && !isAbortSignal(request.signal)) {
    once({ ok: false, error: "Invalid program run request: signal must be an AbortSignal" });
    return;
  }
  if (!deps.context) {
    once({ ok: false, error: "No active Pi session to run the program in" });
    return;
  }
  once(await runHostProgram(deps, deps.context, {
    ref: request.ref,
    ...(request.input !== undefined ? { input: request.input } : {}),
    ...(request.requirePromoted ? { requirePromoted: true } : {}),
    ...(request.signal ? { signal: request.signal } : {}),
  }));
};

const summaryLine = (program: FabricProgramSummary): string =>
  `${program.ref} [${program.status}] ${program.kind}${program.kernel ? `/${program.kernel}` : ""} · ${new Date(program.createdAt).toISOString()}${program.description ? ` — ${program.description}` : ""}`;

/** `/fabric programs [list|promote <ref>|retire <ref>]` and `/fabric run <ref> [json input]`. */
export const runFabricProgramsCommand = async (
  deps: ProgramHostDeps,
  context: ExtensionContext,
  command: "programs" | "run",
  argumentsText: string,
): Promise<void> => {
  const store = new ProgramStore(programsDirectory(context.cwd));
  try {
    if (command === "run") {
      const text = argumentsText.trim();
      const space = text.search(/\s/);
      const ref = space < 0 ? text : text.slice(0, space);
      const inputText = space < 0 ? "" : text.slice(space).trim();
      if (!ref) {
        context.ui.notify("Usage: /fabric run <ref> [json input]", "warning");
        return;
      }
      let input: unknown;
      if (inputText) {
        try {
          input = JSON.parse(inputText) as unknown;
        } catch {
          context.ui.notify(`Program input is not valid JSON: ${bounded(inputText, 200)}`, "error");
          return;
        }
      }
      const reply = await runHostProgram(deps, context, { ref, ...(input !== undefined ? { input } : {}) });
      context.ui.notify(
        reply.ok ? `Program ${reply.program} finished` : `Program ${reply.program ?? ref} failed: ${reply.error}`,
        reply.ok ? "info" : "error",
      );
      return;
    }
    const [action = "list", ref] = argumentsText.trim().split(/\s+/).filter(Boolean);
    if (action === "list") {
      const programs = await store.list();
      context.ui.notify(programs.length ? programs.map(summaryLine).join("\n") : "No saved Fabric programs", "info");
      return;
    }
    if ((action === "promote" || action === "retire") && ref) {
      const record = action === "promote" ? await store.promote(ref) : await store.retire(ref);
      context.ui.notify(`Program ${programRef(record)} is ${record.status}`, "info");
      return;
    }
    context.ui.notify("Usage: /fabric programs [list|promote <ref>|retire <ref>]", "warning");
  } catch (error) {
    context.ui.notify(`Fabric programs: ${errorText(error)}`, "error");
  }
};
