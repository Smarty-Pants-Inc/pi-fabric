import { PI_CORE_TOOL_NAME_SET } from "../core/pi-tools.js";
import { isFabricThinking, type FabricThinking } from "../thinking.js";
import type { FabricParticipantInfo } from "../topology/types.js";

/** Host activation authority. Never normalized from public agent arguments. */
export interface NativeRoleBinding {
  readonly role: "review-agent" | "security-agent";
  readonly model: string;
  readonly thinking: FabricThinking;
  readonly tools: readonly string[];
}

export const isNativePassRole = (role: string): role is NativeRoleBinding["role"] =>
  role === "review-agent" || role === "security-agent";

function refusal(reason: string): never {
  throw Object.assign(new Error(`NATIVE_ROLE_BINDING_MISMATCH: ${reason}; task was not sent`), {
    code: "NATIVE_ROLE_BINDING_MISMATCH",
  });
}

/** Detach and freeze before any asynchronous preparation, queue or process boundary. */
export function snapshotNativeRoleBinding(value: unknown): NativeRoleBinding {
  const binding = value as Partial<NativeRoleBinding> | null;
  if (!binding || !isNativePassRole(String(binding.role)) ||
      typeof binding.model !== "string" || !/^[^\s/]+\/[^\s]+$/.test(binding.model) ||
      !isFabricThinking(binding.thinking) || !Array.isArray(binding.tools) ||
      binding.tools.some(tool => typeof tool !== "string" || !PI_CORE_TOOL_NAME_SET.has(tool)) ||
      !binding.tools.includes("bash")) refusal("invalid native activation contract");
  return Object.freeze({ role: binding.role!, model: binding.model!, thinking: binding.thinking!,
    tools: Object.freeze([...new Set(binding.tools!)]) });
}

export function assertNativeRoleTools(binding: NativeRoleBinding, tools: readonly string[], phase: string): void {
  // fabric_exec is orchestration, not part of the role's native set.
  const actual = new Set(tools.filter(tool => tool !== "fabric_exec"));
  const required = new Set(binding.tools);
  if (actual.size !== required.size || [...required].some(tool => !actual.has(tool))) {
    refusal(`${binding.role} ${phase} native tools must be exactly [${[...required].sort().join(", ")}], got [${[...actual].sort().join(", ")}]`);
  }
}

export function assertNativeRolePair(binding: NativeRoleBinding, model: string | undefined, thinking: string | undefined): void {
  if (model !== binding.model || thinking !== binding.thinking) {
    refusal(`${binding.role} requires ${binding.model}/${binding.thinking}, got ${model ?? "missing"}/${thinking ?? "missing"}`);
  }
}

export function assertNativeRoleParticipant(binding: NativeRoleBinding, participant: FabricParticipantInfo, id: string, kind: "actor" | "agent"): void {
  if (participant.id !== id || participant.kind !== kind || participant.stale) refusal("self metadata does not attest this worker identity");
  assertNativeRolePair(binding, participant.model, participant.thinking);
}

// Explicit Pi hooks may be loaded by jiti while lazy Fabric uses native ESM.
// Share authority through the supplied native host interface, never module globals
// or mutable environment identity. Only the fd-bound hook answers this query.
type NativeRoleHost = import("@earendil-works/pi-coding-agent").ExtensionAPI["events"];
type ParticipantAttester = (model: string, thinking: FabricThinking) => Promise<FabricParticipantInfo>;
interface NativeRoleAuthority {
  binding: NativeRoleBinding;
  setAttester(attest: ParticipantAttester): void;
  attest: ParticipantAttester;
}
const ACTIVATION_QUERY = "fabric.native-role.activation-query";
function nativeRoleAuthority(host: NativeRoleHost | undefined): NativeRoleAuthority | undefined {
  if (!host?.emit) return undefined;
  let authority: NativeRoleAuthority | undefined;
  let duplicate = false;
  host.emit(ACTIVATION_QUERY, { accept(value: NativeRoleAuthority) {
    if (authority) duplicate = true;
    else authority = value;
  } });
  if (duplicate) refusal("multiple native activation authorities");
  return authority;
}
export function installNativeRoleBinding(host: NativeRoleHost, binding: NativeRoleBinding): void {
  if (nativeRoleAuthority(host)) refusal("activation binding already installed");
  const expected = snapshotNativeRoleBinding(binding);
  let attester: ParticipantAttester | undefined;
  const authority: NativeRoleAuthority = Object.freeze({ binding: expected,
    setAttester(attest: ParticipantAttester) { attester = attest; },
    async attest(model: string, thinking: FabricThinking) {
      if (!attester) refusal("Fabric self metadata attester is unavailable");
      return attester(model, thinking);
    },
  });
  host.on(ACTIVATION_QUERY, query => {
    if (query && typeof query === "object" && "accept" in query && typeof query.accept === "function") query.accept(authority);
  });
}
export const nativeRoleBinding = (host: NativeRoleHost | undefined): NativeRoleBinding | undefined => nativeRoleAuthority(host)?.binding;
export function registerNativeRoleAttester(host: NativeRoleHost, attest: ParticipantAttester): void {
  const authority = nativeRoleAuthority(host);
  if (!authority) refusal("missing native activation binding");
  authority.setAttester(attest);
}
export async function attestNativeRoleParticipant(host: NativeRoleHost, model: string, thinking: FabricThinking): Promise<FabricParticipantInfo> {
  const authority = nativeRoleAuthority(host);
  if (!authority) refusal("missing native activation binding");
  return authority.attest(model, thinking);
}
