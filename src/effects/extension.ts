import { createHash } from "node:crypto";
import path from "node:path";
import { types } from "node:util";
import type { AgentToolResult, ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { FabricInvocationContext, FabricProvider } from "../protocol.js";
import type { EffectAdapterControls } from "./failpoints.js";
import { EffectContractError } from "./contracts.js";
import type {
  AcceptedInput, EffectIntent, EffectRecord, EffectSink, EffectSinkResult, EffectSlot, EffectToolResult, JsonValue,
  ReadToolResult, RecoveryOwner, SettledEffect, SettlePendingIntentsResult,
} from "./contracts.js";
import { canonicalEncode, mintEffectKey, resolveEffectSlot, sha256Bytes } from "./slots.js";
import { EffectStore } from "./store.js";
import { createEffectReconciler, readEffectResult } from "./reconcile.js";
import { prepareFileEffect, reconcileFileEffect, type PreparedFileEffect } from "./file.js";
import { executeShellEffect, prepareShellEffect, type PreparedShellEffect, type CertifiedShellProfile } from "./shell.js";

export interface CertifiedDispatchRequest {
  ref: string;
  mode: "invoke" | "acquire" | "speculate" | "replay";
  args: Record<string, unknown>;
  context: FabricInvocationContext;
  provider?: FabricProvider;
}

/** Host-owned live dispatch capability, never a provider's read-only hint or
 * a model argument. Registry/provider composition is B3's exclusive scope. */
export interface CertifiedDispatchPolicy {
  assertDispatch(request: CertifiedDispatchRequest): void | Promise<void>;
}
export interface CertifiedToolDispatcher {
  assertTool(name: string, definition?: unknown): void;
  execute(name: string, toolCallId: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<AgentToolResult<unknown>>;
  /** Preserve host-typed effect refusal identity/reference through final result
   * hooks, without restoring redacted content or exposing saved result bytes. */
  projectError?(error: unknown, finalText: string): Error | undefined;
}

export type CertifiedSettledEffect = SettledEffect;
export type CertifiedSettleResult = SettlePendingIntentsResult;

/** One retained-volume record boundary shared by every dispatch path. Historical
 * bindings resolve BEFORE prepare; the concrete implementation below serializes
 * the whole session, confirms native assistant durability, persists INTENT before
 * the sink and RESULT before returning. No ambient/in-memory fallback exists. */
export interface CertifiedEffectRecordPort {
  execute(request: {
    canonicalRef: string;
    adapterVersion: string;
    kind: string;
    canonicalTarget: string;
    canonicalArguments: Record<string, unknown>;
    toolCallId: string;
    parentToolCallId?: string;
    context: ExtensionContext;
    prepare(): Promise<PreparedFileEffect | PreparedShellEffect>;
  }): Promise<AgentToolResult<unknown>>;
  settlePendingIntents(sessionId: string, recoveryGeneration: string): Promise<CertifiedSettleResult>;
  readToolResult(effect: SettledEffect): ReturnType<typeof readCertifiedToolResult>;
}

export interface CertifiedEffectRuntimeOptions {
  logicalSessionId: string;
  workspaceRoot: string;
  records: CertifiedEffectRecordPort;
  shellProfile: CertifiedShellProfile | ((call: { toolCallId: string; parentToolCallId?: string; context: ExtensionContext }) => CertifiedShellProfile | Promise<CertifiedShellProfile>);
  assertLive(): void | Promise<void>;
  /** Check the actual host-selected supported QuickJS execution profile, not
   * just model input. B3 calls assertProgram again at the final outer execute.
   * Native bash defaults must be disabled, or this runtime's tool_call hook
   * must precede them; earlier command/default rewrites are never undone. */
  assertExecutionProfile(): void | Promise<void>;
  /** B3 supplies actual selected registered definitions, not names/descriptors. */
  getToolDefinition(name: string): unknown;
  /** Actual provider objects are pinned; replacing one under the same ref is not certification. */
  providers: { pi: FabricProvider; captured: FabricProvider };
  /** B5 can bind only its own typed sink; no generic risk=read exemption. */
  additionalDispatch?: readonly { ref: string; provider: FabricProvider; assert(args: Record<string, unknown>, context: FabricInvocationContext): void | Promise<void> }[];
  /** Exact complete outer call inputs. Pinning only code would leave execution
   * backend/options, saved-program and artifact-helper escape routes open. */
  programs?: readonly { toolName: "fabric_exec" | "codemode"; input: Record<string, unknown>; definition: unknown }[];
  kinds?: { write: string; edit: string; bash: string };
  adapterVersion?: string;
}

// Dispatch snapshots and record identities use the SAME strict encoding. This
// digest is never substituted for a manifest slot or business effect key.
const canonicalInput = canonicalEncode;
const deferredBashDefaults = Symbol.for("pi-fabric.bash-defaults.deferred");
/** Only Fabric's nonenumerable deferred-default holder is transport metadata.
 * Never apply it, erase caller flags, invoke accessors, or accept other symbols.
 * Nested providers install it before hooks; native dispatch installs it in
 * our hook before subsequent defaults handlers. Pi clones prepareArguments,
 * so metadata placed there cannot establish this ordering. */
const certifiedToolInput = (name: string, input: unknown): string => {
  if (name !== "bash" || input === null || typeof input !== "object" || types.isProxy(input)) return canonicalInput(input);
  const holder = Object.getOwnPropertyDescriptor(input, deferredBashDefaults);
  if (!holder) return canonicalInput(input);
  if (!("value" in holder) || holder.enumerable || holder.writable || holder.configurable || !holder.value || typeof holder.value !== "object") {
    throw new Error("EFFECT_UNSUPPORTED: invalid host shell-default metadata");
  }
  const descriptors = Object.getOwnPropertyDescriptors(input);
  Reflect.deleteProperty(descriptors, deferredBashDefaults);
  return canonicalInput(Object.create(Object.getPrototypeOf(input), descriptors));
};
const certifiedNames = new Set(["write", "edit", "bash"]);

/** A single session-local runtime serves direct tools, pi.*, captured fallback
 * and native codemode calls. No ambient runtime, second store or startup hook. */
export const createCertifiedEffectRuntime = (options: CertifiedEffectRuntimeOptions) => {
  if (!options.logicalSessionId || !path.isAbsolute(options.workspaceRoot)) throw new Error("Certified effects require provisioned session and workspace roots");
  const definitions = new Map<string, ToolDefinition<any>>();
  const calls = new Map<string, { parentToolCallId?: string }>();
  const programs = (options.programs ?? []).map(program => ({ ...program, inputBytes: canonicalInput(program.input) }));
  const extra = new Map((options.additionalDispatch ?? []).map(binding => [binding.ref, binding]));
  const kinds = Object.freeze({ ...(options.kinds ?? { write: "file-write", edit: "file-edit", bash: "shell-check" }) });
  const adapterVersion = options.adapterVersion ?? "1";
  let closed = false;
  const assertOpen = () => { if (closed) throw new Error("Certified effect runtime is closed"); };
  const assertTool = (name: string, definition?: unknown): void => {
    assertOpen();
    if (!certifiedNames.has(name) || !definitions.has(name)) throw new Error(`EFFECT_UNSUPPORTED: ${name} (adapter not registered)`);
    // Undefined is the provider's stock-tool path: it will be replaced, never
    // executed. A captured/native registration must be ours by identity.
    if (definition !== undefined && definition !== definitions.get(name)) throw new Error(`EFFECT_UNSUPPORTED: opaque ${name} override`);
  };
  const assertRegisteredTool = (name: string): void => {
    const selected = options.getToolDefinition(name);
    if (selected === undefined) throw new Error(`EFFECT_UNSUPPORTED: selected ${name} registration is unconfirmed`);
    assertTool(name, selected);
  };
  const assertProgram = (name: string, input: Record<string, unknown>, definition: unknown): void => {
    const bytes = canonicalInput(input);
    if (definition === undefined || !programs.some(program => program.toolName === name && program.definition === definition && program.inputBytes === bytes)) {
      throw new Error(`EFFECT_UNSUPPORTED: unapproved ${name} program or execution options`);
    }
  };
  const execute = async (name: string, toolCallId: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<AgentToolResult<unknown>> => {
    assertTool(name);
    await options.assertLive();
    await options.assertExecutionProfile();
    if (context.signal?.aborted) throw context.signal.reason ?? new Error("Effect call aborted");
    const canonicalArguments = JSON.parse(certifiedToolInput(name, args)) as Record<string, unknown>;
    const parentToolCallId = calls.get(toolCallId)?.parentToolCallId ?? context.parentToolCallId;
    const canonicalRef = `pi.${name}`;
    const kind = kinds[name as keyof typeof kinds];
    const shellProfile = name === "bash" ? JSON.parse(canonicalInput(typeof options.shellProfile === "function"
      ? await options.shellProfile({ toolCallId, ...(parentToolCallId ? { parentToolCallId } : {}), context: context.extensionContext })
      : options.shellProfile)) as CertifiedShellProfile : undefined;
    // Absolute resolution is not a filesystem safety check. The sink verifies
    // the full namespace before preparing AND at the actual commit boundary.
    const canonicalTarget = path.resolve(options.workspaceRoot, name === "bash" ? shellProfile!.path : String(canonicalArguments.path ?? ""));
    try {
      return await options.records.execute({
        canonicalRef, adapterVersion, kind, canonicalTarget, canonicalArguments, toolCallId,
        ...(parentToolCallId ? { parentToolCallId } : {}),
        context: context.extensionContext,
        prepare: async () => {
          if (name === "bash") {
            return prepareShellEffect(options.workspaceRoot, canonicalArguments, shellProfile!);
          }
          return prepareFileEffect(options.workspaceRoot, name as "write" | "edit", canonicalArguments);
        },
      });
    } catch (error) {
      if (!(error instanceof EffectContractError)) throw error;
      const reference = error.original ? `; saved effect ${error.original.key} (${error.original.state})` : "";
      const labelled = new EffectContractError(error.code, `${error.code}: ${error.message}${reference}`, error.original);
      labelled.name = error.code;
      throw labelled;
    } finally { calls.delete(toolCallId); }
  };
  const dispatcher: CertifiedToolDispatcher = { assertTool, execute,
    projectError(error, finalText) {
      if (!(error instanceof EffectContractError)) return undefined;
      const original = error.original ? { key: error.original.key, state: error.original.state } : undefined;
      const projected = new EffectContractError(error.code, finalText, original);
      // QuickJS already carries host Error.name, without exporting arbitrary
      // error properties. Its actual caller sees the typed refusal as well.
      projected.name = error.code;
      return projected;
    },
  };
  const policy: CertifiedDispatchPolicy = {
    async assertDispatch(request) {
      assertOpen();
      // Speculation/acquisition/replayed values bypass immutable sink receipts.
      if (request.mode !== "invoke") throw new Error(`EFFECT_UNSUPPORTED: ${request.mode} in certified profile`);
      await options.assertLive();
      await options.assertExecutionProfile();
      const separator = request.ref.indexOf(".");
      const providerName = request.ref.slice(0, separator);
      const name = request.ref.slice(separator + 1);
      if (providerName === "pi" || providerName === "extensions") {
        if (request.provider !== options.providers[providerName === "pi" ? "pi" : "captured"]) throw new Error("EFFECT_UNSUPPORTED: unbound tool provider");
        assertTool(name, options.getToolDefinition(name));
        return;
      }
      const binding = extra.get(request.ref);
      if (!binding || binding.provider !== request.provider) throw new Error(`EFFECT_UNSUPPORTED: ${request.ref}`);
      await binding.assert(request.args, request.context);
    },
  };
  return {
    dispatcher,
    policy,
    getToolDefinition: (name: string): ToolDefinition<any> | undefined => definitions.get(name),
    /** Required final outer-executor check after all tool_call middleware.
     * A hook alone cannot authorize bytes rewritten by a later hook. */
    async assertProgram(name: string, input: Record<string, unknown>, definition: unknown): Promise<void> {
      assertOpen();
      await options.assertLive();
      await options.assertExecutionProfile();
      assertProgram(name, input, definition);
    },
    /** B3 awaits this exact interface before continuation. The physical-call
     * aliases in its result are never used to generate the business key. */
    settlePendingIntents: (sessionId: string, recoveryGeneration: string): Promise<CertifiedSettleResult> => {
      assertOpen();
      if (sessionId !== options.logicalSessionId) throw new Error("Effect settlement session mismatch");
      if (typeof recoveryGeneration !== "string" || !recoveryGeneration.trim()) throw new Error("Invalid recovery generation");
      return options.records.settlePendingIntents(sessionId, recoveryGeneration);
    },
    readToolResult: (effect: SettledEffect): ReturnType<typeof readCertifiedToolResult> => {
      assertOpen();
      return options.records.readToolResult(effect);
    },
    /** Register before model/input admission. Hooks do not return cached tool
     * results: only these execute functions can serve a saved result. */
    register(pi: ExtensionAPI): () => void {
      assertOpen();
      if (definitions.size) throw new Error("Certified effect tools already registered");
      const schemas: Record<string, Record<string, unknown>> = {
        write: { type: "object", properties: { path: { type: "string" }, content: { type: "string" } }, required: ["path", "content"], additionalProperties: false },
        edit: { type: "object", properties: { path: { type: "string" }, edits: { type: "array", minItems: 1, items: { type: "object", properties: { oldText: { type: "string" }, newText: { type: "string" } }, required: ["oldText", "newText"], additionalProperties: false } } }, required: ["path", "edits"], additionalProperties: false },
        bash: { type: "object", properties: { command: { type: "string" } }, required: ["command"], additionalProperties: false },
      };
      for (const name of certifiedNames) {
        const definition: ToolDefinition<any> = {
          name, label: name, description: name === "bash" ? "Run the certified fixed read/check command" : `Certified retained-volume ${name}; parent directories must already exist`,
          parameters: schemas[name]!, executionMode: "sequential",
          execute: (toolCallId, args: Record<string, unknown>, signal, _onUpdate, context) => {
            assertRegisteredTool(name);
            return execute(name, toolCallId, args, {
              cwd: context.cwd, signal, parentToolCallId: calls.get(toolCallId)?.parentToolCallId ?? "", nestedToolCallId: toolCallId,
              extensionContext: context, update: () => {},
            });
          },
        };
        Object.freeze(definition);
        definitions.set(name, definition);
        pi.registerTool(definition);
      }
      const unsubscribers = [
        pi.on("tool_call", async (event) => {
          assertOpen();
          await options.assertLive();
          await options.assertExecutionProfile();
          if (certifiedNames.has(event.toolName)) {
            assertRegisteredTool(event.toolName);
            certifiedToolInput(event.toolName, event.input);
            if (event.toolName === "bash" && !Object.hasOwn(event.input, deferredBashDefaults)) {
              // Future run-defaults hooks defer rather than rewrite this fixed
              // program. Earlier rewrites remain visible and are refused.
              Object.defineProperty(event.input, deferredBashDefaults, { value: {} });
            }
            calls.set(event.toolCallId, { ...(event.parentToolCallId ? { parentToolCallId: event.parentToolCallId } : {}) });
          } else assertProgram(event.toolName, event.input, options.getToolDefinition(event.toolName));
        }),
        // RPC/user bash does not traverse ordinary tool_call. Refuse it here;
        // only the registered bounded read-only adapter is supported.
        pi.on("user_bash", () => { throw new Error("EFFECT_UNSUPPORTED: user/RPC bash in certified profile"); }),
      ];
      return () => { closed = true; calls.clear(); for (const unsubscribe of unsubscribers) unsubscribe(); };
    },
    close(): void { closed = true; calls.clear(); },
  };
};

/** Narrow composition API for B3; no edits to shared startup files are needed
 * from the dispatch lane. Bind before ordinary model/inbox work is enabled. */
export const bindCertifiedEffectRuntime = (runtime: ReturnType<typeof createCertifiedEffectRuntime>, targets: {
  registry: { setCertifiedDispatchPolicy(policy: CertifiedDispatchPolicy | undefined): void };
  piTools: { setCertifiedToolDispatcher(dispatcher: CertifiedToolDispatcher | undefined): void };
  capturedTools: { setCertifiedToolDispatcher(dispatcher: CertifiedToolDispatcher | undefined): void };
}): void => {
  targets.registry.setCertifiedDispatchPolicy(runtime.policy);
  targets.piTools.setCertifiedToolDispatcher(runtime.dispatcher);
  targets.capturedTools.setCertifiedToolDispatcher(runtime.dispatcher);
};

/** Evidence helper for a provisioner freezing exact program inputs. This digest
 * is not a slot key and must never become acceptedInputId/effectSlotId. */
export const certifiedProgramDigest = (input: Record<string, unknown>): string => createHash("sha256").update(canonicalInput(input), "utf8").digest("hex");

export interface CertifiedEffectAdmission {
  readonly acceptedInput: AcceptedInput;
  readonly nativeAssistantEntryId: string;
  /** Trusted admission/manifest phase, never a model-provided ordinal. */
  readonly phase?: string;
}
type EffectCall = Pick<Parameters<CertifiedEffectRecordPort["execute"]>[0], "toolCallId" | "parentToolCallId" | "context">;
export interface CertifiedEffectRecordOptions {
  readonly owner: RecoveryOwner;
  readonly store: EffectStore;
  /** B5 resolves the durable accepted input associated with THIS native call. */
  readonly admissionForCall: (call: EffectCall) => CertifiedEffectAdmission | Promise<CertifiedEffectAdmission>;
  /** Await B6's exact assistant-entry barrier, not message_end/tool_call. */
  readonly confirmNativeAssistant: (call: EffectCall & { nativeAssistantEntryId: string }) => Promise<void>;
  /** B2/B5 interpret the frozen manifest's workload-specific constraints. */
  readonly assertManifestConstraints: (slot: EffectSlot, prepared: PreparedFileEffect | PreparedShellEffect) => void | Promise<void>;
  readonly failpoint?: EffectAdapterControls["failpoint"];
  readonly kinds?: { readonly write: string; readonly edit: string; readonly bash: string };
  readonly adapterVersion?: string;
  /** B5's typed stored-intent sinks, not generic provider risk exemptions. */
  readonly additionalSinks?: readonly EffectSink[];
  /** Optional B3/B6 exact native-entry reader. It must verify the durable
   * sink-to-native join; a raw adapter hash is NOT a native-envelope hash.
   * Absent a joined entry, B3 re-runs hooks on the labelled adapter-original.
   * This port is not proof that the post-append boundary is installed. */
  readonly readNativeToolResult?: (effect: SettledEffect) => Promise<Awaited<ReturnType<ReadToolResult>> | undefined>;
}

const terminalResult = (record: EffectRecord) => record.transitions.find(row => row.state === "RESULT");
const jsonCopy = <T>(value: T): T => JSON.parse(canonicalEncode(value)) as T;
const jsonObject = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const adapterError = (message: string): never => { throw new EffectContractError("EFFECT_ADAPTER_DATA_INVALID", message); };

/** Check the complete immutable adapter envelope against its enclosing intent.
 * The sink additionally validates its version, internal hash, namespace and
 * destination pre/post state. Recovery never calls prepare on regenerated args. */
const storedAdapter = (intent: EffectIntent, owner: RecoveryOwner): PreparedFileEffect | PreparedShellEffect => {
  if (intent.adapterData === undefined) return adapterError("Intent has no original prepared adapter bytes");
  const prepared = jsonCopy(intent.adapterData) as unknown as PreparedFileEffect | PreparedShellEffect;
  if (!prepared || typeof prepared !== "object" || prepared.workspaceRoot !== owner.workspaceRoot ||
      typeof prepared.path !== "string" || path.resolve(owner.workspaceRoot, prepared.path) !== intent.canonicalTarget) {
    return adapterError("Stored adapter root/path does not match its owner and canonical target");
  }
  const args = intent.canonicalArguments;
  if (!jsonObject(args)) return adapterError("Adapter arguments must be an object");
  if (prepared.format === "fabric-file-effect-v1") {
    const file = intent.file;
    if (!file || intent.message !== undefined || intent.tool.canonicalRef !== `pi.${prepared.tool}` ||
        (prepared.tool !== "write" && prepared.tool !== "edit") || prepared.displayPath !== args.path ||
        file.preExists !== prepared.pre.exists || file.preHash !== prepared.pre.sha256 || file.preMode !== prepared.pre.mode ||
        file.postHash !== prepared.post.sha256 || file.postMode !== prepared.post.mode ||
        intent.payload.bytes !== prepared.desiredBase64 || intent.payload.digest !== prepared.post.sha256 ||
        canonicalEncode(file.desiredBytes) !== canonicalEncode(intent.payload) ||
        canonicalEncode(file.preparedResult) !== canonicalEncode(prepared.preparedResult)) {
      return adapterError("Stored file metadata, result or desired bytes disagree with the intent");
    }
    if (prepared.tool === "write" && (typeof args.content !== "string" || Buffer.from(args.content, "utf8").toString("base64") !== prepared.desiredBase64)) {
      return adapterError("Stored write payload differs from its original arguments");
    }
    if (prepared.tool === "edit" && (!Array.isArray(args.edits) || args.edits.length !== prepared.replacementCount)) {
      return adapterError("Stored edit count differs from its original arguments");
    }
    return prepared;
  }
  if (prepared.format === "fabric-read-sha256-v1" && intent.file === undefined && intent.message === undefined &&
      intent.tool.canonicalRef === "pi.bash" && args.command === prepared.command &&
      intent.payload.bytes === Buffer.from(canonicalEncode(prepared), "utf8").toString("base64") &&
      intent.payload.digest === sha256Bytes(canonicalEncode(prepared))) return prepared;
  return adapterError("Unsupported or inconsistent stored adapter format");
};

/** The hash verifies stored adapter-original bytes, not a made-up native
 * envelope. B3 prefers B6's actual post-hook entry; otherwise it must re-run
 * emitToolResult before materialization. Physical aliases keep the ORIGINAL
 * message ID here; B3 owns the verified alias/context projection. No timestamp
 * or unevaluated parent fabric_exec result is manufactured by this reader. */
export const readCertifiedToolResult = async (options: Pick<CertifiedEffectRecordOptions, "owner" | "store" | "readNativeToolResult">, effect: SettledEffect) => {
  if (effect.outcome === "uncertain" || !effect.resultBytesHash) throw new EffectContractError("EFFECT_RESULT_UNAVAILABLE", "No acknowledged effect can authorize result repair");
  const original = await readEffectResult(options.owner, options.store, effect.key, effect.toolCallId, effect.resultBytesHash);
  const native = await options.readNativeToolResult?.(effect);
  if (native) {
    const message = jsonCopy(native);
    if (message.role !== "toolResult" || message.toolCallId !== effect.toolCallId || !message.toolName ||
        !Array.isArray(message.content) || typeof message.isError !== "boolean" || !Number.isFinite(message.timestamp)) {
      throw new EffectContractError("EFFECT_NATIVE_RESULT_INVALID", "Native receipt did not identify the actual call's complete original result");
    }
    return { source: "native-entry" as const, message };
  }
  const record = await options.store.withSession("settle", transaction => transaction.read(effect.key));
  const alias = record.intent.tool.originalToolCallId === effect.toolCallId ? record.intent.tool : record.transitions.find(row => row.toolCallId === effect.toolCallId);
  if (alias?.parentToolCallId !== undefined) {
    throw new EffectContractError("EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR", "A nested adapter result is not an original native parent return; consume labelled logical repairs");
  }
  if (!jsonObject(original) || original.role !== "toolResult" ||
      original.toolCallId !== record.intent.tool.originalToolCallId || original.toolName !== record.intent.tool.canonicalRef.slice(3) ||
      !Array.isArray(original.content) || typeof original.isError !== "boolean" || Object.hasOwn(original, "timestamp")) {
    throw new EffectContractError("EFFECT_ADAPTER_RESULT_INVALID", "No complete labelled adapter original exists; do not invent native metadata");
  }
  return { source: "adapter-original" as const, message: jsonCopy(original) as unknown as Omit<EffectToolResult, "timestamp"> };
};

/** Concrete B4-records bridge. B3 supplies one owner/store and admission/native
 * capabilities; the same transaction surrounds resolution, INTENT, sink action
 * and RESULT. It owns no second journal, input queue, lease or startup hook. */
export const createCertifiedEffectRecordPort = (options: CertifiedEffectRecordOptions): CertifiedEffectRecordPort & {
  reconcilePending: ReturnType<typeof createEffectReconciler>["reconcilePending"];
  sinks: readonly EffectSink[];
} => {
  const { owner, store } = options;
  if (store.owner !== owner) throw new EffectContractError("EFFECT_OWNER_MISMATCH", "Certified dispatch must use its store's exact owner capability");
  const kinds = Object.freeze({ ...(options.kinds ?? { write: "file-write", edit: "file-edit", bash: "shell-check" }) });
  const adapterVersion = options.adapterVersion ?? "1";
  const controls = (intent: EffectIntent, call?: { toolCallId: string; parentToolCallId?: string }): EffectAdapterControls => ({
    assertLive: () => owner.assertLive("settle"),
    async failpoint(point, details) {
      const toolCallId = call?.toolCallId ?? intent.tool.originalToolCallId;
      const parentToolCallId = call ? call.parentToolCallId : intent.tool.parentToolCallId;
      await options.failpoint?.(point, { ...details, logicalSessionId: owner.logicalSessionId, key: intent.key, toolCallId,
        ...(parentToolCallId === undefined ? {} : { parentToolCallId }) });
    },
  });
  const reconcileAdapter = async (name: "write" | "edit" | "bash", intent: EffectIntent, live: EffectAdapterControls): Promise<EffectSinkResult> => {
    if (intent.tool.adapterVersion !== adapterVersion || intent.slot.kind !== kinds[name] || intent.tool.canonicalRef !== `pi.${name}`) {
      return adapterError("Sink identity does not match the stored kind/version/tool");
    }
    const prepared = storedAdapter(intent, owner);
    const original = (result: AgentToolResult<unknown>): JsonValue => ({ ...jsonCopy(result), role: "toolResult",
      toolCallId: intent.tool.originalToolCallId, toolName: name, isError: result.isError === true }) as unknown as JsonValue;
    if (name === "bash") {
      if (prepared.format !== "fabric-read-sha256-v1") return adapterError("Shell sink requires original shell data");
      return { outcome: "committed", result: original(await executeShellEffect(prepared, live)) };
    }
    if (prepared.format !== "fabric-file-effect-v1" || prepared.tool !== name) return adapterError("File sink requires original file data");
    const outcome = await reconcileFileEffect(prepared, live);
    return { outcome: outcome.outcome, ...(outcome.result === undefined ? {} : { result: original(outcome.result) }),
      ...(outcome.reason === undefined ? {} : { reason: outcome.reason }) };
  };
  const makeSink = (name: "write" | "edit" | "bash"): EffectSink => ({
    kind: kinds[name], adapterVersion,
    async reconcile(intent, context) {
      if (context.owner !== owner) return adapterError("Sink owner capability does not match");
      return reconcileAdapter(name, intent, controls(intent));
    },
  });
  const sinks = Object.freeze([makeSink("write"), makeSink("edit"), makeSink("bash"), ...(options.additionalSinks ?? [])]);
  const identities = new Set(sinks.map(sink => canonicalEncode([sink.kind, sink.adapterVersion])));
  if (identities.size !== sinks.length) throw new EffectContractError("EFFECT_SINK_AMBIGUOUS", "Certified sink kind/version must be unique");
  const recovery = createEffectReconciler({ owner, store, sinks });
  let needsSettlement = false;
  const settle = async (generation: string) => {
    needsSettlement = true;
    const result = await recovery.reconcilePending(generation);
    needsSettlement = result.uncertain > 0;
    return result;
  };
  const asToolResult = (record: EffectRecord): AgentToolResult<unknown> => {
    const result = terminalResult(record);
    if (!result || result.outcome !== "committed" || result.result === undefined) {
      throw new EffectContractError("EFFECT_NOT_COMMITTED", "Stored effect has no committed original tool result", { key: record.intent.key, state: result?.state ?? "INTENT" });
    }
    const value = jsonCopy(result.result);
    if (!jsonObject(value) || !Array.isArray(value.content)) {
      return adapterError("Stored outcome is not an adapter tool result");
    }
    const { role: _role, toolCallId: _call, toolName: _name, ...adapterResult } = value;
    return adapterResult as unknown as AgentToolResult<unknown>;
  };
  return {
    sinks,
    reconcilePending: settle,
    async settlePendingIntents(sessionId, generation) {
      if (sessionId !== owner.logicalSessionId) throw new EffectContractError("EFFECT_SESSION_MISMATCH", "Cannot settle another logical session");
      const { settled, uncertain } = await settle(generation);
      return { settled, uncertain };
    },
    readToolResult: effect => readCertifiedToolResult(options, effect),
    async execute(request) {
      if (request.adapterVersion !== adapterVersion || !["write", "edit", "bash"].some(name => request.canonicalRef === `pi.${name}` && request.kind === kinds[name as keyof typeof kinds])) {
        throw new EffectContractError("EFFECT_ADAPTER_VERSION_UNSUPPORTED", "Dispatch is not a bound certified file/shell adapter");
      }
      const admission = jsonCopy(await options.admissionForCall(request));
      if (!admission.nativeAssistantEntryId) throw new EffectContractError("EFFECT_NATIVE_ASSISTANT_UNCONFIRMED", "No durable native assistant call association");
      const canonicalArguments = jsonCopy(request.canonicalArguments) as JsonValue;
      let issued = false;
      return store.withSession("new-intent", async transaction => {
        if (needsSettlement) throw new EffectContractError("EFFECT_SETTLEMENT_REQUIRED", "An earlier uncertain/barrier failure requires explicit B3 settlement");
        const records = transaction.list();
        const slot = resolveEffectSlot({ acceptedInput: admission.acceptedInput, kind: request.kind,
          canonicalRef: request.canonicalRef, adapterVersion, canonicalTarget: request.canonicalTarget, canonicalArguments,
          ...(admission.phase === undefined ? {} : { phase: admission.phase }), records });
        const keyTuple = [1, owner.orgId, owner.logicalSessionId, admission.acceptedInput.id, slot.id, slot.kind] as const;
        const key = mintEffectKey(keyTuple);
        let record = records.find(row => row.intent.key === key);
        // No later mutation may obscure an unresolved destination. B3 must run
        // settlement first; replay of an existing RESULT remains historical.
        if (!record && records.some(row => !terminalResult(row))) {
          throw new EffectContractError("EFFECT_PENDING_INTENT", "Settle all issued intents before admitting a new effect");
        }
        await options.confirmNativeAssistant({ toolCallId: request.toolCallId,
          ...(request.parentToolCallId === undefined ? {} : { parentToolCallId: request.parentToolCallId }),
          context: request.context, nativeAssistantEntryId: admission.nativeAssistantEntryId });
        await owner.assertLive("new-intent");
        if (record) {
          issued = true;
          const alias = record.intent.tool.originalToolCallId === request.toolCallId
            ? record.intent.tool : record.transitions.find(row => row.toolCallId === request.toolCallId);
          if (alias && alias.parentToolCallId !== request.parentToolCallId) {
            throw new EffectContractError("EFFECT_CALL_ALIAS_CONFLICT", "A physical call alias changed its parent");
          }
          if (!alias) record = transaction.append(key, { state: "ALIAS", toolCallId: request.toolCallId,
            ...(request.parentToolCallId === undefined ? {} : { parentToolCallId: request.parentToolCallId }),
            attempt: owner.attempt, at: new Date().toISOString(), receipt: { nativeAssistantEntryId: admission.nativeAssistantEntryId } });
          if (terminalResult(record)) return asToolResult(record);
        } else {
          const prepared = jsonCopy(await request.prepare());
          await options.assertManifestConstraints(slot, prepared);
          const payload = prepared.format === "fabric-file-effect-v1"
            ? { encoding: "base64" as const, bytes: prepared.desiredBase64, digest: prepared.post.sha256! }
            : { encoding: "base64" as const, bytes: Buffer.from(canonicalEncode(prepared), "utf8").toString("base64"), digest: sha256Bytes(canonicalEncode(prepared)) };
          const intent: EffectIntent = { format: "fabric.effect-intent", version: 1, key, keyTuple,
            manifestDigest: admission.acceptedInput.manifestDigest,
            acceptedInput: { id: admission.acceptedInput.id, digest: admission.acceptedInput.digest, nativeEntryId: admission.acceptedInput.nativeEntryId },
            slot, tool: { canonicalRef: request.canonicalRef, adapterVersion, originalToolCallId: request.toolCallId,
              nativeAssistantEntryId: admission.nativeAssistantEntryId,
              ...(request.parentToolCallId === undefined ? {} : { parentToolCallId: request.parentToolCallId }) },
            canonicalArguments, canonicalTarget: request.canonicalTarget, payload, adapterData: prepared as unknown as JsonValue,
            ...(prepared.format !== "fabric-file-effect-v1" ? {} : { file: {
              preExists: prepared.pre.exists, preHash: prepared.pre.sha256, preMode: prepared.pre.mode,
              postHash: prepared.post.sha256!, postMode: prepared.post.mode!, desiredBytes: payload,
              preparedResult: prepared.preparedResult as unknown as JsonValue,
            } }), originAttempt: owner.attempt };
          storedAdapter(intent, owner);
          await owner.assertLive("new-intent");
          issued = true;
          record = transaction.bind(intent);
          await controls(record.intent, request).failpoint?.("intent-durable");
        }
        transaction.append(key, { state: "ATTEMPT", toolCallId: request.toolCallId,
          ...(request.parentToolCallId === undefined ? {} : { parentToolCallId: request.parentToolCallId }),
          attempt: owner.attempt, at: new Date().toISOString() });
        const name = request.canonicalRef.slice(3) as "write" | "edit" | "bash";
        let outcome: EffectSinkResult;
        try { outcome = await reconcileAdapter(name, record.intent, controls(record.intent, request)); }
        catch (error) { outcome = { outcome: "uncertain" as const, reason: error instanceof Error ? error.message : String(error) }; }
        if (outcome.outcome === "uncertain") {
          transaction.append(key, { state: "UNCERTAIN", toolCallId: request.toolCallId, attempt: owner.attempt,
            at: new Date().toISOString(), outcome: "uncertain", reason: outcome.reason ?? "Destination state is uncertain" });
          throw new EffectContractError("EFFECT_UNCERTAIN", outcome.reason ?? "Destination state is uncertain", { key, state: "UNCERTAIN" });
        }
        // A failed result barrier can follow an actual rename. Propagate it;
        // do not append a false absence or retry the sink within this dispatch.
        record = transaction.append(key, { state: "RESULT", toolCallId: request.toolCallId, attempt: owner.attempt,
          at: new Date().toISOString(), outcome: outcome.outcome,
          ...(outcome.result === undefined ? {} : { result: outcome.result }),
          ...(outcome.resultBytesHash === undefined ? {} : { resultBytesHash: outcome.resultBytesHash }),
          ...(outcome.receipt === undefined ? {} : { receipt: outcome.receipt }) });
        await controls(record.intent, request).failpoint?.("result-durable");
        return asToolResult(record);
      }).catch((error: unknown) => {
        if (issued || (error instanceof EffectContractError && error.code === "EFFECT_STORE_UNCERTAIN")) needsSettlement = true;
        throw error;
      });
    },
  };
};
