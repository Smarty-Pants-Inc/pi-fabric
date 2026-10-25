import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { fenceNativeInput, nativeFenceHeld, observeNativeInput, releaseNativeInput,
  type ExtensionInputFence as PortFence, type ExtensionInputState as PortState } from "../src/main-drain/native-port.js";

// D3 seat drain (smarty-dev#15310): the NativeInputPort against Pi's extension-facing fence as declared in
// Smarty-Pants-Inc/pi#309 head c13c7a4c (diff sha256 dfb9bce8...). The declarations below are copied verbatim from
// that diff's packages/coding-agent/src/core/extensions/types.ts and host-capabilities.ts; the stub mirrors its
// AgentSession bindCore actions (fenceInputs: async () => ({ release: await this.fenceInputs() }), getInputState).

/** A held native input fence. */
interface ExtensionInputFence {
  /** Reopen admission. Idempotent; releases only this fence and never reopens a disposed session. */
  release(): void;
}
/** Read-only native input state, read at call time. */
interface ExtensionInputState {
  readonly inputsFenced: boolean;
  readonly inputAdmissionCount: number;
  readonly queued: number;
  readonly drained: boolean;
}
interface Pi309Context { fenceInputs(): Promise<ExtensionInputFence>; getInputState(): ExtensionInputState }
interface Pi309Capabilities { inputAdmission: 1; extensionInputFence: 1 }

// The port's own types and the diff's must be mutually assignable (compile-time check).
const toPort = (fence: ExtensionInputFence, state: ExtensionInputState): [PortFence, PortState] => [fence, state];
const toDiff = (fence: PortFence, state: PortState): [ExtensionInputFence, ExtensionInputState] => [fence, state];
void toPort; void toDiff;

const ROOT = "session:00000000-0000-4000-8000-0000000000f1";
class NativeSession {
  fence: object | undefined;
  disposed = false;
  admissions = 0;
  queue: string[] = [];
  settling = false;
  get drained(): boolean { return this.admissions === 0 && this.queue.length === 0 && !this.settling; }
  // AgentSession.fenceInputs() (031c7499 agent-session.js:2909-2947): set synchronously, BUSY releases its own fence.
  async fenceInputs(): Promise<() => void> {
    if (this.disposed) throw Object.assign(new Error("disposed"), { code: "INPUT_ADMISSION_DISPOSED" });
    if (this.fence) throw Object.assign(new Error("fenced"), { code: "INPUT_ADMISSION_FENCED" });
    const fence = {};
    this.fence = fence;
    if (!this.drained) { this.fence = undefined; throw Object.assign(new Error("busy"), { code: "INPUT_ADMISSION_BUSY" }); }
    return () => { if (this.fence === fence) this.fence = undefined; };
  }
  /** The bindCore context actions the diff adds, on a FRESH ctx object (as after a reload). */
  ctx(): ExtensionContext & Pi309Context {
    return {
      isIdle: () => true, isPromptPending: () => this.admissions > 0, hasPendingMessages: () => this.admissions + this.queue.length > 0,
      fenceInputs: async () => ({ release: await this.fenceInputs() }),
      getInputState: () => ({ inputsFenced: this.fence !== undefined || this.disposed, inputAdmissionCount: this.admissions,
        queued: this.queue.length, drained: this.drained }),
    } as unknown as ExtensionContext & Pi309Context;
  }
}
const pi = (capabilities: Partial<Pi309Capabilities> = { inputAdmission: 1, extensionInputFence: 1 }) =>
  ({ hostCapabilities: capabilities }) as unknown as ExtensionAPI;

afterEach(() => {
  releaseNativeInput(ROOT);
  ((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.main.drain-slot.v1")] as Map<string, unknown> | undefined)?.delete(ROOT);
});

describe("NativeInputPort against pi#309's extension fence", () => {
  it("fences an idle session and reports Pi's exact state; the ORIGINAL handle releases it after a reload", async () => {
    const session = new NativeSession();
    expect(await fenceNativeInput(pi(), session.ctx(), ROOT)).toBe(true);
    expect(observeNativeInput(pi(), session.ctx(), ROOT)).toMatchObject({ inputAdmission: 1, fenceApi: true, inputsFenced: true,
      piInputsFenced: true, inputAdmissionCount: 0, queued: 0, drained: true, fenceError: null });
    // A reload hands a NEW ctx; the process-global slot still holds the original release, which still reopens.
    expect(releaseNativeInput(ROOT)).toBe(true);
    expect(observeNativeInput(pi(), session.ctx(), ROOT)).toMatchObject({ inputsFenced: false, piInputsFenced: false });
    expect(releaseNativeInput(ROOT)).toBe(false);
  });

  it("a BUSY refusal keeps every queued input, leaves admission open and records the native code", async () => {
    const session = new NativeSession();
    session.queue.push("queued follow-up");
    expect(await fenceNativeInput(pi(), session.ctx(), ROOT)).toBe(false);
    expect(session.queue).toEqual(["queued follow-up"]);
    expect(session.fence).toBeUndefined();
    expect(observeNativeInput(pi(), session.ctx(), ROOT)).toMatchObject({ inputsFenced: false, queued: 1, drained: false,
      fenceError: "INPUT_ADMISSION_BUSY" });
    expect(nativeFenceHeld(ROOT)).toBe(false);
  });

  it("another holder's fence is not ours: FENCED, and inputsFenced stays false while Pi reads fenced (a lost handle)", async () => {
    const session = new NativeSession();
    await session.fenceInputs();
    expect(await fenceNativeInput(pi(), session.ctx(), ROOT)).toBe(false);
    expect(observeNativeInput(pi(), session.ctx(), ROOT)).toMatchObject({ inputsFenced: false, piInputsFenced: true, fenceError: "INPUT_ADMISSION_FENCED" });
  });

  it("fails closed without the capability, without the hooks, or when getInputState throws", async () => {
    const session = new NativeSession();
    const none = observeNativeInput(pi({ inputAdmission: 1 }), session.ctx(), ROOT);
    expect(none).toMatchObject({ fenceApi: false, inputsFenced: false, piInputsFenced: null, queued: null, drained: null,
      fenceError: "FENCE_API_UNAVAILABLE" });
    expect(await fenceNativeInput(pi({ inputAdmission: 1 }), session.ctx(), ROOT)).toBe(false);
    const throwing = { ...session.ctx(), getInputState: () => { throw new Error("Native input state is not available in this context"); } };
    expect(observeNativeInput(pi(), throwing as unknown as ExtensionContext, ROOT)).toMatchObject({ fenceApi: true, inputsFenced: false,
      piInputsFenced: null, drained: null });
    const hookless = { isIdle: () => true, hasPendingMessages: () => false } as unknown as ExtensionContext;
    expect(observeNativeInput(pi(), hookless, ROOT)).toMatchObject({ fenceApi: false });
    expect(observeNativeInput(pi(), undefined, ROOT)).toMatchObject({ fenceApi: false, idle: null });
  });

  it("a disposed session reads fenced but is never reported as this drain's fence", async () => {
    const session = new NativeSession();
    session.disposed = true;
    expect(await fenceNativeInput(pi(), session.ctx(), ROOT)).toBe(false);
    expect(observeNativeInput(pi(), session.ctx(), ROOT)).toMatchObject({ inputsFenced: false, piInputsFenced: true,
      fenceError: "INPUT_ADMISSION_DISPOSED" });
  });
});
