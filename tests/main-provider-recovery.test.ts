import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { fabricDiagnosticsFile, flushFabricDiagnostics } from "../src/core/diagnostics.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { sendFabricMessage } from "../src/fabric-provenance.js";
import { registerMainProviderRecovery } from "../src/main-provider-recovery.js";
import { MainAgentController } from "../src/main-agent.js";

const identityState = vi.hoisted(() => ({ background: false }));
vi.mock("../src/fabric-provenance.js", () => ({
  resolveFabricIdentity: () => ({ identity: { kind: identityState.background ? "actor" : "main", id: identityState.background ? "actor:x" : "main" }, mainAgentId: "main" }),
  sendFabricMessage: vi.fn(),
}));
vi.mock("@earendil-works/pi-ai/compat", () => ({ isContextOverflow: () => false }));

type Handler = (event: unknown, context: ExtensionContext) => unknown;
const harness = (options: { errorMessage?: string; rawStopReason?: string; recoveryRetryable?: boolean; owns?: boolean } = {}) => {
  const handlers = new Map<string, Handler[]>();
  let idle = false;
  let settling = false;
  let halted = false;
  let owner = new AbortController();
  const turn: { role: string; stopReason: string; errorMessage: string; rawStopReason?: string; recovery?: { retryable: boolean } } = {
    role: "assistant", stopReason: "error", errorMessage: options.errorMessage ?? "disconnect\nprivate diagnostic",
    ...(options.rawStopReason === undefined ? {} : { rawStopReason: options.rawStopReason }),
    ...(options.recoveryRetryable === undefined ? {} : { recovery: { retryable: options.recoveryRetryable } }),
  };
  const context = {
    get signal() { return owner.signal; },
    isIdle: () => idle,
    isSettling: () => settling,
    sessionManager: {
      getSessionId: () => "main",
      getEntries: () => [{ type: "message", message: turn }],
    },
    hasUI: true,
    compact: vi.fn(),
    ui: { notify: vi.fn() },
  } as unknown as ExtensionContext;
  const report = vi.fn(async () => undefined);
  const pi = { on: (name: string, handler: Handler) => {
    handlers.set(name, [...(handlers.get(name) ?? []), handler]);
    return () => handlers.set(name, (handlers.get(name) ?? []).filter(fn => fn !== handler));
  } } as unknown as ExtensionAPI;
  registerMainProviderRecovery(pi, { halted: () => halted, report, ownsNonRetryable: () => options.owns ?? false });
  return {
    emit: async (name: string, event: unknown = { outcome: "error" }) => {
      for (const handler of handlers.get(name) ?? []) await handler(event, context);
    },
    report, context, pi, turn,
    idle: () => { idle = true; },
    settling: (value: boolean) => { settling = value; },
    abort: () => { owner.abort(); },
    halt: () => { halted = true; },
    resume: () => { owner = new AbortController(); halted = false; },
  };
};

beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); identityState.background = false; });
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe("Main provider recovery waiting for settlement compaction", () => {
  it("does not retry or latch a structured model refusal", async () => {
    const h = harness({ errorMessage: "private refusal detail", rawStopReason: "refusal" });
    h.idle();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(h.report).not.toHaveBeenCalled();
    expect(h.context.ui.notify).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("honors Pi recovery.retryable=false even when the error text is unclassified", async () => {
    const h = harness({ errorMessage: "private network diagnostic", recoveryRetryable: false });
    h.idle();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(h.report).not.toHaveBeenCalled();
    expect(h.context.ui.notify).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("clears the blocked retry latch on principal input but not extension input", async () => {
    const h = harness();
    h.idle();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(1);
    await h.emit("agent_start");
    await h.emit("agent_settled");
    expect(sendFabricMessage).toHaveBeenCalledTimes(2);
    expect(h.report).toHaveBeenCalledTimes(1);

    await h.emit("input", { source: "extension" });
    await h.emit("agent_start");
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(2);

    await h.emit("input", { source: "rpc" });
    await h.emit("agent_start");
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(3);
    expect(sendFabricMessage).toHaveBeenLastCalledWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
    expect(h.report).toHaveBeenCalledTimes(1);
  });

  it.each(["org-agent@x", "project-agent@x"])("keeps the principal's own failed-turn report visible for %s", async role => {
    vi.stubEnv("SMARTY_ROLE", role);
    try {
      const h = harness(); h.idle();
      await h.emit("agent_settled"); await vi.advanceTimersByTimeAsync(1_000);
      await h.emit("agent_start"); await h.emit("agent_settled");
      expect(h.context.ui.notify).toHaveBeenCalledExactlyOnceWith("BLOCKED: provider error: unclassified", "error");
    } finally { vi.unstubAllEnvs(); }
  });

  it("logs a failed background report without hiding the principal's own-turn error", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "principal-provider-diagnostics-"));
    vi.stubEnv("SMARTY_ROLE", "org-agent@x"); vi.stubEnv("PI_CODING_AGENT_DIR", root);
    try {
      const h = harness(); h.idle();
      h.report.mockRejectedValue(new Error("report transport unavailable"));
      await h.emit("agent_settled"); await vi.advanceTimersByTimeAsync(1_000);
      await h.emit("agent_start"); await h.emit("agent_settled");
      expect(h.context.ui.notify).toHaveBeenCalledExactlyOnceWith("BLOCKED: provider error: unclassified", "error");
      await flushFabricDiagnostics();
      const lines = fs.readFileSync(fabricDiagnosticsFile("main"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(lines).toContainEqual(expect.objectContaining({ level: "error", source: "src/main-provider-recovery.ts",
        message: expect.stringContaining("report transport unavailable") }));
    } finally { vi.unstubAllEnvs(); }
  });

  it("a successor session_start gets its own retry budget", async () => {
    const h = harness(); h.idle();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(1);
    await h.emit("session_start");
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(2);
    expect(sendFabricMessage).toHaveBeenLastCalledWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
    expect(h.report).not.toHaveBeenCalled();
  });

  it("drops a late report failure after session shutdown without touching the stale ctx (#5962)", async () => {
    const h = harness(); h.idle();
    await h.emit("agent_settled"); await vi.advanceTimersByTimeAsync(1_000);
    let entered!: () => void; let reject!: (error: Error) => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    h.report.mockImplementation(() => { entered(); return new Promise((_yes, no) => { reject = no; }); });
    const reporting = h.emit("agent_settled"); await started;
    await h.emit("session_shutdown");
    Object.defineProperty(h.context, "ui", { get() { throw new Error("stale ctx"); } });
    reject(new Error("retired report"));
    await expect(reporting).resolves.toBeUndefined();
  });

  it("keeps the wake pending while busy, then retries once and reports once without a third attempt", async () => {
    const h = harness();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(2_500);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(h.report).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
    h.idle();
    await vi.advanceTimersByTimeAsync(100);
    expect(sendFabricMessage).toHaveBeenCalledExactlyOnceWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
    await h.emit("agent_start");
    await h.emit("agent_settled");
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(2); // retry + non-triggering BLOCKED
    expect(sendFabricMessage).toHaveBeenLastCalledWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-blocked", content: "BLOCKED: provider error: unclassified" }),
      { deliverAs: "followUp", triggerTurn: false });
    expect(h.report).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("waits for the remaining settlement hooks even after Pi is idle", async () => {
    const h = harness();
    h.idle();
    h.settling(true);
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(2_500);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    h.settling(false);
    await vi.advanceTimersByTimeAsync(100);
    expect(sendFabricMessage).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["input", "agent_start", "session_start", "session_before_switch", "session_tree", "session_shutdown", "owner-abort", "owner-halt"])(
    "%s cancels a busy wake without consuming the recovery budget", async fence => {
      const h = harness();
      await h.emit("agent_settled");
      await vi.advanceTimersByTimeAsync(1_200); // cancellation after the original timer sampled busy
      if (fence === "owner-abort") h.abort();
      else if (fence === "owner-halt") h.halt();
      else await h.emit(fence);
      h.idle();
      await vi.advanceTimersByTimeAsync(5_000);
      expect(sendFabricMessage).not.toHaveBeenCalled();
      expect(h.report).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
      h.resume();
      await h.emit("agent_settled");
      await vi.advanceTimersByTimeAsync(1_000);
      expect(sendFabricMessage).toHaveBeenCalledExactlyOnceWith(expect.anything(),
        expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
      expect(h.report).not.toHaveBeenCalled();
    });
});

describe("Main provider recovery reports only allowlisted fields after a retryable failure (smarty-dev#8742)", () => {
  const secret = "FAKE-SECRET-8742 echoed prompt hunter2";
  it.each([
    ["a 5xx body that echoes a secret", `503 ${JSON.stringify({ type: "error", error: { type: "api_error", message: `upstream echoed Authorization: Bearer ${secret}` } })}`, "503 api_error"],
    ["a secret as the type and code", `502 ${JSON.stringify({ error: { type: secret, code: secret, message: secret } })}`, "502"],
    ["a secret on the raw first line", `${secret} connection reset by peer\nprivate diagnostic`, "unclassified"],
  ])("%s never reaches the UI notice or the report payload", async (_name, errorMessage, short) => {
    const h = harness({ errorMessage, owns: true });
    h.idle();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledExactlyOnceWith(expect.anything(),   // retryable: the one retry
      expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
    await h.emit("agent_start");
    await h.emit("agent_settled");
    const expected = `BLOCKED: provider error: ${short}`;
    expect(h.report).toHaveBeenCalledExactlyOnceWith(expected, h.context);
    expect(h.context.ui.notify).toHaveBeenCalledExactlyOnceWith(expected, "error");
    expect(sendFabricMessage).toHaveBeenLastCalledWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-blocked", content: expected }), { deliverAs: "followUp", triggerTurn: false });
    const published = JSON.stringify([vi.mocked(sendFabricMessage).mock.calls.map(call => call.slice(1)),
      vi.mocked(h.context.ui.notify).mock.calls, (h.report.mock.calls as unknown[][]).map(call => call[0])]);
    expect(published).not.toContain(secret);
    expect(published).not.toContain("private diagnostic");
  });
});

describe("Main provider recovery leaves non-retryable errors to Main's drain (smarty-dev#8742)", () => {
  const affinity = `409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "compaction_affinity_missing" } })}`;

  it("shows an owned principal 409 once across the actual drain and registered recovery handler", async () => {
    vi.stubEnv("SMARTY_ROLE", "org-agent@x");
    const secret = "FAKE-SECRET-own-turn-409";
    const errorMessage = `409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: `compaction_affinity_missing ${secret}` } })}`;
    const h = harness({ errorMessage, owns: true });
    const main = new MainAgentController(h.pi, "main", true, "/project", "main");
    main.attachFollowUpDrain(h.context, 120_000);
    try {
      expect(main.ownsProviderRecovery()).toBe(true);
      await h.emit("agent_start");
      await h.emit("turn_end", { message: { role: "assistant", stopReason: "error", errorMessage } });
      expect(h.context.ui.notify).toHaveBeenCalledExactlyOnceWith(
        "Provider rejected Main's request as non-retryable (409 invalid_request_error: compaction_affinity_missing); compacting once to recover", "warning");
      h.idle();
      await h.emit("agent_settled");
      await h.emit("agent_settled");
      await vi.advanceTimersByTimeAsync(60_000); // Compaction remains held; no retry or duplicate notice.
      expect(h.context.compact).toHaveBeenCalledTimes(1);
      expect(h.context.ui.notify).toHaveBeenCalledTimes(1);
      expect(JSON.stringify(vi.mocked(h.context.ui.notify).mock.calls)).not.toMatch(/undefined|FAKE-SECRET/);
      expect(sendFabricMessage).not.toHaveBeenCalled();
      expect(h.report).not.toHaveBeenCalled();
    } finally { main.closeFollowUpDrain(); vi.unstubAllEnvs(); }
  });

  it("shows the settlement fallback when an owner exists but has not displayed a notice, once per run", async () => {
    vi.stubEnv("SMARTY_ROLE", "org-agent@x");
    try {
      const h = harness({ errorMessage: affinity, owns: true });
      await h.emit("agent_settled");
      await h.emit("agent_settled");
      expect(h.context.ui.notify).toHaveBeenCalledTimes(1);
      expect(vi.mocked(h.context.ui.notify).mock.calls[0]?.[0]).toContain("409 invalid_request_error: compaction_affinity_missing");
      expect(JSON.stringify(vi.mocked(h.context.ui.notify).mock.calls)).not.toContain("undefined");
      await h.emit("agent_start");
      await h.emit("agent_settled");
      expect(h.context.ui.notify).toHaveBeenCalledTimes(2);
      expect(sendFabricMessage).not.toHaveBeenCalled();
    } finally { vi.unstubAllEnvs(); }
  });

  it("keeps the same actor 409 private in a principal-role process", async () => {
    vi.stubEnv("SMARTY_ROLE", "org-agent@x");
    identityState.background = true;
    const h = harness({ errorMessage: affinity, owns: true });
    const main = new MainAgentController(h.pi, "main", false, "/project", "main");
    main.attachFollowUpDrain(h.context, 120_000);
    try {
      await h.emit("turn_end", { message: { role: "assistant", stopReason: "error", errorMessage: affinity } });
      await h.emit("agent_settled");
      await vi.advanceTimersByTimeAsync(2_000);
      expect(main.ownsProviderRecovery()).toBe(false);
      expect(h.context.ui.notify).not.toHaveBeenCalled();
      expect(sendFabricMessage).not.toHaveBeenCalled();
      expect(h.report).not.toHaveBeenCalled();
    } finally { main.closeFollowUpDrain(); vi.unstubAllEnvs(); }
  });

  it.each([
    ["owned non-retryable", affinity, true, 0],
    ["unowned non-retryable", affinity, false, 1],
    ["owned retryable", "503 upstream connect error", true, 1],
  ])("%s", async (_name, errorMessage, owns, retries) => {
    const h = harness({ errorMessage, owns });
    h.idle();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(2_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(retries);
    expect(h.report).not.toHaveBeenCalled();
  });
});
