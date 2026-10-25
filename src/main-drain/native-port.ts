import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { drainSlot, peekDrainSlot, pruneDrainSlot } from "./slot.js";

/**
 * D3 seat drain (smarty-dev#15310): the typed port to Pi's native input fence.
 *
 * Pi 031c7499 has `AgentSession.fenceInputs()`, `inputsFenced` and `inputAdmissionCount`, but no
 * extension-facing surface. This port detects the pass-through specified in .local/PI-FENCE-API.md
 * (`pi.hostCapabilities.extensionInputFence === 1`, `ctx.fenceInputs()`, `ctx.getInputState()`).
 * Without it, the port reports `inputsFenced: false` and null native counts, so the drain is
 * honestly NOT drained. Once Pi ships the pass-through, it plugs in here with no seam change.
 *
 * Fail closed: `inputsFenced` is true only when the API exists, THIS process holds the fence for
 * the root, AND Pi reads back `inputsFenced: true`.
 */
export interface ExtensionInputFence { release(): void }
export interface ExtensionInputState {
  readonly inputsFenced: boolean;
  readonly inputAdmissionCount: number;
  readonly queued: number;
  readonly drained: boolean;
}
type FenceContext = ExtensionContext & {
  fenceInputs?: () => Promise<ExtensionInputFence>;
  getInputState?: () => ExtensionInputState;
};
type Capabilities = { inputAdmission?: unknown; extensionInputFence?: unknown };

export interface NativeInputObservation {
  /** `pi.hostCapabilities.inputAdmission` (1 on an input-admission host); null when absent. */
  inputAdmission: number | null;
  /** The extension-facing fence API is present on this host and context. */
  fenceApi: boolean;
  /** This process holds the root's fence and Pi reads it back as fenced. */
  inputsFenced: boolean;
  /** Pi's own fence state, whoever holds it (null without the API): what an undo's canary must see as false. */
  piInputsFenced: boolean | null;
  /** Pi's exact counts, only with the fence API; otherwise null (unknown). */
  inputAdmissionCount: number | null;
  queued: number | null;
  drained: boolean | null;
  /** Lower-bound observations available on every host; null when the context is gone. */
  idle: boolean | null;
  promptPending: boolean | null;
  pendingMessages: boolean | null;
  /** The last fence refusal code (INPUT_ADMISSION_BUSY, FENCE_API_UNAVAILABLE, ...). */
  fenceError: string | null;
}

// The release handle lives in the process-global drain slot (slot.ts): it survives a live extension
// reload (Pi keeps the fence across a reload, and the ORIGINAL release still reopens it); a restart
// starts unfenced. Only a lost handle means restart required.

const count = (value: unknown): number | null => Number.isSafeInteger(value) && (value as number) >= 0 ? value as number : null;
const flag = (read: () => unknown): boolean | null => {
  try { const value = read(); return typeof value === "boolean" ? value : null; } catch { return null; }
};
const capabilities = (pi: ExtensionAPI): Capabilities | undefined => {
  try { return (pi as { hostCapabilities?: Capabilities }).hostCapabilities; } catch { return undefined; }
};
const fenceApi = (pi: ExtensionAPI, ctx: ExtensionContext | undefined): ctx is FenceContext => {
  if (!ctx || capabilities(pi)?.extensionInputFence !== 1) return false;
  try {
    return typeof (ctx as FenceContext).fenceInputs === "function" && typeof (ctx as FenceContext).getInputState === "function";
  } catch { return false; }
};
const inputState = (ctx: FenceContext): ExtensionInputState | undefined => {
  try {
    const state = ctx.getInputState!();
    if (typeof state !== "object" || state === null || typeof state.inputsFenced !== "boolean" || typeof state.drained !== "boolean" ||
      count(state.inputAdmissionCount) === null || count(state.queued) === null) return undefined;
    return { inputsFenced: state.inputsFenced, inputAdmissionCount: state.inputAdmissionCount, queued: state.queued, drained: state.drained };
  } catch { return undefined; }
};
const errorCode = (error: unknown): string => {
  const code = typeof error === "object" && error !== null ? (error as { code?: unknown }).code : undefined;
  return typeof code === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(code) ? code : "FENCE_FAILED";
};

export const observeNativeInput = (pi: ExtensionAPI, ctx: ExtensionContext | undefined, rootId: string): NativeInputObservation => {
  const admission = capabilities(pi)?.inputAdmission;
  const api = fenceApi(pi, ctx);
  const state = api ? inputState(ctx) : undefined;
  return {
    inputAdmission: Number.isSafeInteger(admission) ? admission as number : null,
    fenceApi: api,
    inputsFenced: api && peekDrainSlot(rootId)?.fence !== undefined && state?.inputsFenced === true,
    piInputsFenced: state?.inputsFenced ?? null,
    inputAdmissionCount: state?.inputAdmissionCount ?? null,
    queued: state?.queued ?? null,
    drained: state?.drained ?? null,
    idle: ctx ? flag(() => ctx.isIdle()) : null,
    promptPending: ctx ? flag(() => (ctx as { isPromptPending?: () => boolean }).isPromptPending?.()) : null,
    pendingMessages: ctx ? flag(() => ctx.hasPendingMessages()) : null,
    fenceError: api ? peekDrainSlot(rootId)?.fenceError ?? null : "FENCE_API_UNAVAILABLE",
  };
};

/** Acquire the root's native fence once. Never passes rejectQueuedInput: a BUSY refusal keeps every queued input. */
export const fenceNativeInput = async (pi: ExtensionAPI, ctx: ExtensionContext | undefined, rootId: string): Promise<boolean> => {
  if (!fenceApi(pi, ctx)) return false;
  const slot = drainSlot(rootId);
  if (slot.fence) return inputState(ctx)?.inputsFenced === true;
  try {
    const fence = await ctx.fenceInputs!();
    if (typeof fence?.release !== "function") { slot.fenceError = "FENCE_HANDLE_INVALID"; return false; }
    slot.fence = fence;
    delete slot.fenceError;
    return inputState(ctx)?.inputsFenced === true;
  } catch (error) {
    slot.fenceError = errorCode(error);
    return false;
  }
};

/** Release only the fence this process acquired for the root, through its original handle. Idempotent. */
export const releaseNativeInput = (rootId: string): boolean => {
  const slot = peekDrainSlot(rootId);
  const fence = slot?.fence;
  if (slot) { delete slot.fence; delete slot.fenceError; }
  pruneDrainSlot(rootId);
  if (!fence) return false;
  fence.release();
  return true;
};

export const nativeFenceHeld = (rootId: string): boolean => peekDrainSlot(rootId)?.fence !== undefined;
