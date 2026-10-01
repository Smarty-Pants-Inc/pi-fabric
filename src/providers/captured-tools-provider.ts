import path from "node:path";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { runAbortable, throwIfAborted } from "../async-settlement.js";
import type { AgentToolResult, SourceInfo } from "@earendil-works/pi-coding-agent";
import { CapturedToolCatalog, type CapturedToolEntry } from "../capture/catalog.js";
import { classifyPiBashError, classifyPiBashResult, piBashResultError } from "../core/pi-bash-error.js";
import { isPiShellToolName } from "../core/pi-tools.js";
import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";

export interface CapturedToolInvocationResult {
  content: AgentToolResult<unknown>["content"];
  text: string;
  details?: unknown;
  isError: boolean;
  structuredContent?: unknown;
  terminate?: boolean;
  source: SourceInfo;
}

const textFromContent = (content: AgentToolResult<unknown>["content"]): string =>
  content
    .filter((part): part is { type: "text"; text: string } => part.type === "text")
    .map((part) => part.text)
    .join("\n");

const sourceLabel = (sourceInfo: SourceInfo): string => {
  if (sourceInfo.path.startsWith("<")) return sourceInfo.source;
  const segments = sourceInfo.path.split(/[\\/]/);
  const packageSegment = [...segments].reverse().find((segment) => segment.startsWith("pi-"));
  if (packageSegment) return packageSegment;
  const filename = path.basename(sourceInfo.path).replace(/\.[^.]+$/, "");
  if (filename && filename !== "index") return filename;
  return path.basename(path.dirname(sourceInfo.path)) || sourceInfo.source;
};

const capturedToolNamespace = (entry: CapturedToolEntry): string =>
  `extension:${sourceLabel(entry.sourceInfo)}`;

const descriptorFrom = (entry: CapturedToolEntry): FabricActionDescriptor => ({
  name: entry.name,
  description: `${entry.definition.description} (captured from ${sourceLabel(entry.sourceInfo)})`,
  inputSchema: entry.definition.parameters as Record<string, unknown>,
  risk: entry.risk,
  namespace: capturedToolNamespace(entry),
});

const asInvocationResult = (
  entry: CapturedToolEntry,
  result: AgentToolResult<unknown>,
  isError: boolean,
): CapturedToolInvocationResult => ({
  content: result.content,
  text: textFromContent(result.content),
  ...(result.details !== undefined ? { details: result.details } : {}),
  isError,
  ...(result.structuredContent !== undefined ? { structuredContent: result.structuredContent } : {}),
  ...(result.terminate !== undefined ? { terminate: result.terminate } : {}),
  source: entry.sourceInfo,
});

class CapturedToolScheduler {
  #sequentialTail: Promise<void> = Promise.resolve();
  readonly #parallel = new Set<Promise<unknown>>();

  run<T>(mode: "sequential" | "parallel" | undefined, operation: () => Promise<T>): Promise<T> {
    if (mode === "sequential") {
      const precedingParallel = [...this.#parallel];
      const result = this.#sequentialTail
        .then(() => Promise.allSettled(precedingParallel))
        .then(operation);
      this.#sequentialTail = result.then(
        () => undefined,
        () => undefined,
      );
      return result;
    }

    const result = this.#sequentialTail.then(operation);
    this.#parallel.add(result);
    void result.then(
      () => this.#parallel.delete(result),
      () => this.#parallel.delete(result),
    );
    return result;
  }
}

export class CapturedToolsProvider implements FabricProvider {
  readonly name = "extensions";
  readonly description =
    "Tools captured from other Pi extensions and invoked lazily through Fabric";

  readonly #scheduler = new CapturedToolScheduler();
  readonly #allowedTools = readChildToolAllowlist();

  constructor(
    readonly catalog: CapturedToolCatalog,
    private readonly omitFromDiscovery: (entry: CapturedToolEntry) => boolean = () => false,
  ) {}

  async list(
    request: FabricProviderListRequest,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor[]> {
    const query = request.query?.trim().toLowerCase();
    const descriptors = this.catalog.list().filter((entry) =>
      (!this.#allowedTools || this.#allowedTools.has(entry.name)) && !this.omitFromDiscovery(entry),
    ).map(descriptorFrom);
    if (!query) return descriptors;
    return descriptors.filter((descriptor) =>
      `${descriptor.name} ${descriptor.description} ${descriptor.namespace ?? ""}`
        .toLowerCase()
        .includes(query),
    );
  }

  async describe(
    actionName: string,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor | undefined> {
    if (this.#allowedTools && !this.#allowedTools.has(actionName)) return undefined;
    const entry = this.catalog.get(actionName);
    return entry ? descriptorFrom(entry) : undefined;
  }

  prepareArguments(actionName: string, args: Record<string, unknown>): Record<string, unknown> {
    this.#assertAllowed(actionName);
    const prepare = this.catalog.require(actionName).wrappedTool.prepareArguments;
    if (!prepare) return args;
    const prepared = prepare(args);
    if (typeof prepared !== "object" || prepared === null || Array.isArray(prepared)) {
      throw new Error(`Captured tool ${actionName} prepared non-object arguments`);
    }
    return prepared as Record<string, unknown>;
  }

  async invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<CapturedToolInvocationResult> {
    this.#assertAllowed(actionName);
    const entry = this.catalog.require(actionName);
    return this.#scheduler.run(entry.definition.executionMode, () =>
      runAbortable(context.signal, () => this.#invokeCaptured(entry, args, context)),
    );
  }

  #assertAllowed(name: string): void {
    if (this.#allowedTools && !this.#allowedTools.has(name)) {
      throw new Error(`Extension tool ${name} is not permitted by this child's tool allowlist`);
    }
  }

  async #invokeCaptured(
    entry: CapturedToolEntry,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<CapturedToolInvocationResult> {
    const { runner, wrappedTool } = entry;
    const native = context.extensionContext as Partial<import("@earendil-works/pi-coding-agent").ExtensionToolContext>;
    // Native calls own validation, middleware, child IDs, execution events and
    // usage accounting. Shells retain the adapted boundary so native exit status
    // is captured BEFORE tool_result redaction/recovery (and cwd stays scoped).
    // Tools with prepareArguments use Fabric's wrapper: the registry already
    // prepared the authorized arguments; native dispatch would prepare twice.
    // On a native host, its live callable set is authoritative even when
    // shell/prepared-argument adapters need the legacy execution boundary.
    if (native.executeTool && !native.tools?.some((tool) => tool.name === entry.name)) {
      throw new Error(`Captured tool ${entry.name} is unavailable in the native callable tool set`);
    }
    if (native.executeTool && !isPiShellToolName(entry.name) && !wrappedTool.prepareArguments) {
      const activeBefore = runner.getActiveTools();
      const outcome = await native.executeTool(entry.name, args, {
        ...(context.signal ? { signal: context.signal } : {}),
        onUpdate: (partial) => {
          const progress = textFromContent(partial.content).trim();
          if (progress) context.update(`${entry.name}: ${progress.slice(0, 500)}`);
        },
      });
      const activeAfter = new Set(runner.getActiveTools());
      this.catalog.remove(activeBefore.filter((name) => !activeAfter.has(name)));
      context.updateArguments?.(outcome.toolCall.arguments);
      const images = outcome.result.content.filter((part) => part.type === "image");
      if (images.length) context.attachMedia?.(images);
      if (outcome.isError) throw new Error(textFromContent(outcome.result.content) || `Captured tool ${entry.name} failed`);
      return asInvocationResult(entry, outcome.result, false);
    }
    const toolCallId = context.nestedToolCallId;
    await runAbortable(context.signal, () => runner.emit({
      type: "tool_execution_start",
      toolCallId,
      toolName: entry.name,
      args,
    }));

    let result: AgentToolResult<unknown>;
    let isError = false;
    let thrown: unknown;
    let executionStarted = false;
    let updateTail: Promise<void> = Promise.resolve();
    try {
      const preflight = await runAbortable(context.signal, () => runner.emitToolCall({
        type: "tool_call",
        toolName: entry.name,
        toolCallId,
        input: args,
      }));
      context.updateArguments?.(args);
      if (preflight?.block) {
        throw new Error(preflight.reason || `Captured tool ${entry.name} was blocked`);
      }
      executionStarted = true;
      const requestedCwd = args.cwd;
      const executionContext = isPiShellToolName(entry.name) && typeof requestedCwd === "string"
        ? Object.defineProperty(Object.create(runner.createToolContext
            ? runner.createToolContext(toolCallId, context.signal)
            : runner.createContext()), "cwd", { value: requestedCwd, enumerable: true })
        : undefined;
      result = await runAbortable(context.signal, () =>
        wrappedTool.execute(toolCallId, args, context.signal, (partialResult) => {
        const progress = textFromContent(partialResult.content).trim();
        if (progress) context.update(`${entry.name}: ${progress.slice(0, 500)}`);
        updateTail = updateTail
          .then(() =>
            runAbortable(context.signal, () => runner.emit({
              type: "tool_execution_update",
              toolCallId,
              toolName: entry.name,
              args,
              partialResult,
            })),
          )
          .catch(() => undefined);
        }, executionContext),
      );
      isError = result.isError === true;
      if (isError && isPiShellToolName(entry.name)) thrown = classifyPiBashResult(result);
    } catch (error) {
      thrown = isPiShellToolName(entry.name) && executionStarted ? classifyPiBashError(error) : error;
      isError = true;
      result = {
        content: [
          {
            type: "text",
            text: error instanceof Error ? error.message : String(error),
          },
        ],
        details: { capturedToolError: true },
      };
    }

    await updateTail;
    throwIfAborted(context.signal);
    const patch = await runAbortable(context.signal, () => runner.emitToolResult({
      type: "tool_result",
      toolName: entry.name,
      toolCallId,
      input: args,
      content: result.content,
      details: result.details,
      ...(result.structuredContent !== undefined ? { structuredContent: result.structuredContent } : {}),
      isError,
    }));
    if (patch) {
      result = {
        ...result,
        content: patch.content ?? result.content,
        ...(patch.details !== undefined ? { details: patch.details } : {}),
        ...((patch.content !== undefined || patch.structuredContent !== undefined)
          ? { structuredContent: patch.structuredContent } : {}),
      };
      isError = patch.isError ?? isError;
    }

    await runAbortable(context.signal, () => runner.emit({
      type: "tool_execution_end",
      toolCallId,
      toolName: entry.name,
      result,
      isError,
    }));

    // Keep observations emitted before a failed batch as well as successful
    // results. Hooks have already produced the final model-facing content.
    const images = result.content.filter((part) => part.type === "image");
    if (images.length > 0) context.attachMedia?.(images);
    if (isError) {
      if (isPiShellToolName(entry.name)) {
        throw piBashResultError(thrown, textFromContent(result.content));
      }
      const text = textFromContent(result.content).trim();
      throw new Error(
        text || (thrown instanceof Error ? thrown.message : `Captured tool ${entry.name} failed`),
      );
    }
    return asInvocationResult(entry, result, false);
  }
}
