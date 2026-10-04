import { validationMessage } from "../core/action-arguments.js";
import type { FabricNestedProgramRunner } from "../execution-service.js";
import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import { programSourceWithInput } from "../programs/source.js";
import {
  MAX_PROGRAM_CODE_CHARS,
  MAX_PROGRAM_DESCRIPTION_CHARS,
  MAX_PROGRAM_NAME_CHARS,
  MAX_PROGRAM_REF_CHARS,
  normalizeProgramInput,
  programInputError,
  programRef,
  type FabricProgramKernel,
  type FabricProgramStatus,
  type ProgramStore,
} from "../programs/store.js";
import { actionArgNormalizer } from "./arg-normalization.js";

// Content-addressed saved programs. Programs save candidates and run saved
// programs nested in the caller's own execution; promotion and retirement are
// user commands (`/fabric programs promote|retire`) with no action here.

const resource = ["fabric:programs"];
const refSchema = { type: "string", minLength: 1, maxLength: MAX_PROGRAM_REF_CHARS };
const descriptors: FabricActionDescriptor[] = [
  {
    name: "save",
    description: "Save a program as a content-addressed candidate; identical content returns the same ref. Returns { ref: \"name@digest12\", digest }",
    inputSchema: {
      type: "object",
      required: ["name"],
      additionalProperties: false,
      properties: {
        name: { type: "string", minLength: 1, maxLength: MAX_PROGRAM_NAME_CHARS, pattern: "^[a-z0-9][a-z0-9._-]*$" },
        kind: { type: "string", enum: ["fabric", "jev"] },
        kernel: { type: "string", enum: ["typescript", "python"] },
        code: { type: "string", minLength: 1, maxLength: MAX_PROGRAM_CODE_CHARS },
        jevProgram: { type: "object" },
        description: { type: "string", maxLength: MAX_PROGRAM_DESCRIPTION_CHARS },
        inputSchema: { type: "object" },
      },
    },
    risk: "write",
    namespace: "programs",
    effect: { kind: "transactional", resources: resource, ordering: "ordered" },
  },
  {
    name: "list",
    description: "List saved programs newest first per name, optionally by name and status (candidate, promoted, retired)",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        name: { type: "string", minLength: 1, maxLength: MAX_PROGRAM_NAME_CHARS },
        status: { type: "string", enum: ["candidate", "promoted", "retired"] },
      },
    },
    risk: "read",
    namespace: "programs",
    effect: { kind: "none", resources: resource, ordering: "commutative" },
  },
  {
    name: "get",
    description: "Read one saved program by ref: name (latest promoted, else latest candidate), name@<digest prefix ≥12>, or a full digest",
    inputSchema: {
      type: "object",
      required: ["ref"],
      additionalProperties: false,
      properties: { ref: refSchema },
    },
    risk: "read",
    namespace: "programs",
    effect: { kind: "none", resources: resource, ordering: "commutative" },
  },
  {
    name: "run",
    description: "Run a saved program nested in this execution with the caller's capabilities and approvals; input becomes the program's `input` global. Returns the program's result",
    inputSchema: {
      type: "object",
      required: ["ref"],
      additionalProperties: false,
      properties: {
        ref: refSchema,
        input: {},
        requirePromoted: { type: "boolean" },
      },
    },
    risk: "execute",
    namespace: "programs",
  },
];

export const normalizeProgramsArgs = actionArgNormalizer(() => descriptors);

export class ProgramsProvider implements FabricProvider {
  readonly name = "programs";
  readonly description = "Content-addressed saved programs run nested in the caller's execution";

  constructor(
    readonly store: ProgramStore,
    readonly defaultKernel: () => FabricProgramKernel,
    readonly nestedRunner: (parentToolCallId: string) => FabricNestedProgramRunner | undefined,
  ) {}

  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return descriptors.filter((action) => !query || `${action.name} ${action.description}`.toLowerCase().includes(query));
  }

  async describe(name: string): Promise<FabricActionDescriptor | undefined> {
    return descriptors.find((action) => action.name === name);
  }

  prepareArguments(name: string, args: Record<string, unknown>): Record<string, unknown> {
    return normalizeProgramsArgs(name, args);
  }

  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = descriptors.find((action) => action.name === name);
    if (!descriptor) throw new Error(`Unknown programs action: ${name}`);
    const error = validationMessage(descriptor.inputSchema, args);
    if (error) throw new Error(`Invalid programs.${name} arguments: ${error}`);
    switch (name) {
      case "save": {
        const { record, created } = await this.store.save(args, this.defaultKernel());
        if (created) context.activity?.({ type: "progress", message: `Program saved: ${programRef(record)}` });
        return { ref: programRef(record), digest: record.digest };
      }
      case "list":
        return this.store.list({
          ...(typeof args.name === "string" ? { name: args.name } : {}),
          ...(typeof args.status === "string" ? { status: args.status as FabricProgramStatus } : {}),
        });
      case "get": {
        const record = await this.store.resolve(args.ref);
        return { ref: programRef(record), ...record };
      }
      case "run":
        return this.#run(args, context);
      default:
        throw new Error(`Unknown programs action: ${name}`);
    }
  }

  async #run(args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const runner = context.nestedProgramRunner ?? this.nestedRunner(context.parentToolCallId);
    if (!runner) throw new Error("programs.run runs only inside a fabric_exec program or a host program run");
    const record = await this.store.resolve(args.ref, { requirePromoted: args.requirePromoted === true });
    const label = programRef(record, true);
    if (record.status === "retired") throw new Error(`Program ${label} is retired; a user can promote it again with /fabric programs promote`);
    const input = normalizeProgramInput(args.input);
    const invalid = programInputError(record, input);
    if (invalid) throw new Error(`Invalid input for program ${label}: ${invalid}`);
    if (record.kind === "jev") {
      return runner.run({ program: label, call: { ref: "jev.run", args: { program: record.jevProgram, input: input ?? null } } }, context.signal);
    }
    if (record.kernel !== runner.kernel) {
      throw new Error(`Program ${label} is a ${record.kernel} program; this session's kernel is ${runner.kernel}`);
    }
    return runner.run({ program: label, code: programSourceWithInput(record.code ?? "", runner.kernel, input) }, context.signal);
  }
}
