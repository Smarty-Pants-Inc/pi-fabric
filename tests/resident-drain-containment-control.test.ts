// Keep reason: native IPC trust boundary and retained custody on deadline/channel failure (#11962).
import { EventEmitter } from "node:events";
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  LAUNCHER_CONTAINMENT_ABI, createLauncherContainmentClient, createLauncherContainmentServer,
  type LauncherContainmentBinding, type LauncherContainmentChannel,
} from "../src/residency/launcher-containment-control.js";
import {
  AttemptExitUnconfirmedError, ContainmentUnavailableError,
  type AttemptContainment, type AttemptMembership,
} from "../src/residency/attempt-containment-contract.js";
import type { ResidentLaunchSpec } from "../src/residency/handover.js";

const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const binding = (): LauncherContainmentBinding => ({
  launchAttestation: { attemptId: "attempt-A", scope: "owned-A", cgroupPath: "/opaque/not-opened/A", launchedInScope: true, at: Date.now() - 1_000 },
  launcher: { pid: 100, processStartTime: "launcher-birth", token: "launcher-token", entry: "/retained/entry", runtime: process.execPath },
  child: { pid: 101, processStartTime: "child-birth" }, generation: "generation-A",
});
class Channel extends EventEmitter implements LauncherContainmentChannel {
  connected = true;
  pid = 101;
  peer?: Channel;
  sent: any[] = [];
  send(value: object, callback: (error: Error | null) => void): boolean {
    this.sent.push(clone(value));
    queueMicrotask(() => { this.peer?.emit("message", clone(value)); callback(null); });
    return false; // Exercise native backpressure: false is NOT failed send.
  }
}
const disposals: (() => void)[] = [];
afterEach(() => { for (const dispose of disposals.splice(0)) dispose(); vi.useRealTimers(); vi.restoreAllMocks(); });
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
function fixture(containment?: AttemptContainment, timeoutMs = 1_000) {
  const expected = binding();
  const host = new Channel(); const child = new Channel();
  host.peer = child; child.peer = host;
  const native = { exited: false, stop: vi.fn() };
  let current = true;
  const server = createLauncherContainmentServer({ child: child as unknown as ChildProcess, native,
    binding: expected, isCurrent: () => current, ...(containment ? { containment } : {}) });
  const client = createLauncherContainmentClient({ channel: host, binding: expected, timeoutMs });
  disposals.push(() => { client.dispose(); server.dispose(); });
  return { expected, host, child, native, server, client, loseCurrent: () => { current = false; } };
}
const adapter = (overrides: Partial<AttemptContainment> = {}): AttemptContainment => ({
  launchInScope: vi.fn(async () => { throw new Error("Never launch from the control helper"); }),
  attemptMembership: vi.fn(async id => ({ attemptId: id, scope: "owned-A", populated: true, pids: [101] })),
  joinAttemptExit: vi.fn(async (id: string) => ({ attemptId: id, scope: "owned-A", populated: false as const, at: Date.now() })),
  ...overrides,
});
function request(expected: LauncherContainmentBinding, extra: Record<string, unknown> = {}) {
  return { abi: LAUNCHER_CONTAINMENT_ABI, kind: "request", requestId: randomUUID(), operation: "membership",
    binding: clone(expected), deadlineAt: Date.now() + 500, ...extra };
}
async function raw(child: Channel, value: Record<string, unknown>): Promise<any> {
  const receipt = new Promise<any>(resolve => child.peer!.once("message", resolve));
  child.emit("message", value);
  return receipt;
}

// These are component-boundary tests, not assertions about source text or a fake cgroup implementation.
describe("private launcher containment channel", () => {
  it("serializes the default refusal and refuses launch authority and live-host self-join", async () => {
    const f = fixture();
    await expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    expect(f.child.sent[0].error).toMatchObject({ name: "ContainmentUnavailableError", code: "CONTAINMENT_UNAVAILABLE" });
    await expect(f.client.launchInScope({} as ResidentLaunchSpec, "attempt-A")).rejects.toThrow("no containment launch authority");
    await expect(f.client.joinAttemptExit("attempt-A", Date.now() + 500)).rejects.toThrow("live host cannot certify its own exit");
    expect(f.native.stop).not.toHaveBeenCalled();
  });

  it("correlates simultaneous requests even when adapter receipts arrive out of order", async () => {
    const first = deferred<AttemptMembership>(); const second = deferred<AttemptMembership>();
    const dependency = adapter({ attemptMembership: vi.fn().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise) });
    const f = fixture(dependency);
    const one = f.client.attemptMembership("attempt-A"); const two = f.client.attemptMembership("attempt-A");
    await Promise.resolve(); await Promise.resolve();
    expect(f.host.sent[0].requestId).not.toBe(f.host.sent[1].requestId);
    expect(f.server.pending).toBe(2);
    second.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [202] });
    await expect(two).resolves.toMatchObject({ pids: [202] });
    first.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [201] });
    await expect(one).resolves.toMatchObject({ pids: [201] });
    expect(f.server.pending).toBe(0);
    expect(dependency.launchInScope).not.toHaveBeenCalled();
  });

  it("allows a valid injected binding but membership never enables a live-host join", async () => {
    const dependency = adapter(); const f = fixture(dependency);
    await expect(f.client.attemptMembership("attempt-A")).resolves.toEqual({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101] });
    await expect(f.server.joinAttemptExit(Date.now() + 500)).rejects.toBeInstanceOf(ContainmentUnavailableError);
    expect(dependency.joinAttemptExit).not.toHaveBeenCalled();
    f.native.exited = true;
    f.child.connected = false; f.child.emit("disconnect");
    const deadline = Date.now() + 500;
    await expect(f.server.joinAttemptExit(deadline)).resolves.toMatchObject({ attemptId: "attempt-A", scope: "owned-A", populated: false });
    expect(dependency.joinAttemptExit).toHaveBeenCalledWith("attempt-A", deadline);
    expect(f.native.stop).not.toHaveBeenCalled();
  });

  it("cannot substitute a drain operation ID for the current launch attempt", async () => {
    const dependency = adapter(); const f = fixture(dependency);
    await expect(f.client.attemptMembership("drain-op-id")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    await expect(f.client.joinAttemptExit("attempt-B", Date.now() + 500)).rejects.toBeInstanceOf(ContainmentUnavailableError);
    expect(f.host.sent).toHaveLength(0); expect(dependency.attemptMembership).not.toHaveBeenCalled();
  });

  it.each([
    ["attempt", (b: LauncherContainmentBinding) => { b.launchAttestation.attemptId = "other"; }],
    ["scope", (b: LauncherContainmentBinding) => { b.launchAttestation.scope = "other"; }],
    ["path", (b: LauncherContainmentBinding) => { b.launchAttestation.cgroupPath = "/other"; }],
    ["attestation", (b: LauncherContainmentBinding) => { b.launchAttestation.launchedInScope = false as true; }],
    ["child pid", (b: LauncherContainmentBinding) => { b.child.pid++; }],
    ["child birth", (b: LauncherContainmentBinding) => { b.child.processStartTime = "reused"; }],
    ["generation", (b: LauncherContainmentBinding) => { b.generation = "other"; }],
    ["launcher token", (b: LauncherContainmentBinding) => { b.launcher.token = "other"; }],
    ["launcher birth", (b: LauncherContainmentBinding) => { b.launcher.processStartTime = "reused"; }],
  ] as const)("refuses a forged %s before adapter entry", async (_name, mutate) => {
    const dependency = adapter(); const f = fixture(dependency);
    const forged = clone(f.expected); mutate(forged);
    const reply = await raw(f.child, request(f.expected, { binding: forged }));
    expect(reply).toMatchObject({ ok: false, error: { code: "CONTAINMENT_UNAVAILABLE" } });
    expect(dependency.attemptMembership).not.toHaveBeenCalled(); expect(dependency.joinAttemptExit).not.toHaveBeenCalled();
  });

  it.each([{ operation: "launch" }, { operation: "terminate" }, { operation: "membership", target: "B" },
    { operation: "membership", root: "/other" }, { attemptId: "attempt-B" }, { deadlineAt: Infinity },
    { deadlineAt: "later" }, { deadlineAt: Date.now() - 1 }, { deadlineAt: Date.now() + 130_000 }])(
    "rejects unsupported/extra/invalid request fields %j", async extra => {
      const dependency = adapter(); const f = fixture(dependency);
      expect(await raw(f.child, request(f.expected, extra))).toMatchObject({ ok: false, error: { code: "CONTAINMENT_UNAVAILABLE" } });
      expect(dependency.attemptMembership).not.toHaveBeenCalled(); expect(dependency.joinAttemptExit).not.toHaveBeenCalled();
    });

  it("deduplicates an in-flight request and checks current custody again at receipt emission", async () => {
    const work = deferred<AttemptMembership>(); const dependency = adapter({ attemptMembership: vi.fn(() => work.promise) });
    const f = fixture(dependency); const value = request(f.expected);
    f.child.emit("message", value); await Promise.resolve();
    expect(await raw(f.child, value)).toMatchObject({ ok: false });
    expect(dependency.attemptMembership).toHaveBeenCalledTimes(1);
    f.loseCurrent();
    const receipt = new Promise<any>(resolve => f.host.once("message", resolve));
    work.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101] });
    expect(await receipt).toMatchObject({ ok: false, error: { code: "CONTAINMENT_UNAVAILABLE" } });
    f.child.pid = 999;
    await expect(f.server.attemptMembership()).rejects.toBeInstanceOf(ContainmentUnavailableError);
  });

  it("retains pending native adapter custody across request timeout and suppresses late success", async () => {
    vi.useFakeTimers();
    const work = deferred<AttemptMembership>(); const f = fixture(adapter({ attemptMembership: () => work.promise }), 10);
    const pending = expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    await vi.advanceTimersByTimeAsync(10); await pending;
    expect(f.server.pending).toBe(1); expect(f.native.stop).not.toHaveBeenCalled();
    work.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101] });
    await vi.advanceTimersByTimeAsync(0);
    expect(f.server.pending).toBe(0);
    expect(f.child.sent.every(response => response.ok === false)).toBe(true);
  });

  it("channel loss rejects callers but preserves pending custody and the after-exit local join", async () => {
    const work = deferred<AttemptMembership>(); const dependency = adapter({ attemptMembership: () => work.promise });
    const f = fixture(dependency);
    const pending = expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    await Promise.resolve(); await Promise.resolve();
    f.host.connected = false; f.host.emit("disconnect"); f.child.connected = false; f.child.emit("disconnect");
    await pending; expect(f.server.pending).toBe(1);
    f.native.exited = true;
    await expect(f.server.joinAttemptExit(Date.now() + 500)).resolves.toMatchObject({ populated: false });
    work.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101] });
    await Promise.resolve(); await Promise.resolve();
    expect(f.native.stop).not.toHaveBeenCalled();
  });

  it("bounds local launcher joins with the exact absolute deadline and retains a losing adapter promise", async () => {
    vi.useFakeTimers();
    const work = deferred<any>(); const dependency = adapter({ joinAttemptExit: vi.fn(() => work.promise) });
    const f = fixture(dependency); f.native.exited = true;
    const deadlineAt = Date.now() + 10;
    const pending = expect(f.server.joinAttemptExit(deadlineAt)).rejects.toBeInstanceOf(AttemptExitUnconfirmedError);
    await vi.advanceTimersByTimeAsync(10); await pending;
    expect(f.server.pending).toBe(1);
    expect(dependency.joinAttemptExit).toHaveBeenCalledWith("attempt-A", deadlineAt);
    f.server.dispose(); expect(f.server.pending).toBe(1);
    work.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: false, at: Date.now() });
    await vi.advanceTimersByTimeAsync(0); expect(f.server.pending).toBe(0);
  });

  it.each([
    { attemptId: "attempt-B" }, { scope: "other" }, { pids: [0] }, { pids: [101, 101] },
    { pids: ["101"] }, { pids: null }, { populated: "false" }, { populated: false, pids: [101] }, { target: "B" },
  ])("validates adapter membership receipts before emitting them: %j", async malformed => {
    const dependency = adapter({ attemptMembership: async () => ({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101], ...malformed }) as AttemptMembership });
    const f = fixture(dependency);
    await expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    expect(f.child.sent[0]).toMatchObject({ ok: false });
  });

  it.each([{ attemptId: "B" }, { scope: "other" }, { populated: true }, { at: 0 }, { at: Date.now() + 100_000 }, { pids: [] }])(
    "validates complete-exit receipts: %j", async malformed => {
      const dependency = adapter({ joinAttemptExit: async () => ({ attemptId: "attempt-A", scope: "owned-A", populated: false, at: Date.now(), ...malformed }) as any });
      const f = fixture(dependency); f.native.exited = true;
      await expect(f.server.joinAttemptExit(Date.now() + 500)).rejects.toBeInstanceOf(ContainmentUnavailableError);
    });

  it.each(["correlation", "binding", "shape", "receipt", "error"])("client rejects a forged %s response on the exact channel", async field => {
    const expected = binding(); const host = new Channel();
    const client = createLauncherContainmentClient({ channel: host, binding: expected, timeoutMs: 500 });
    disposals.push(() => client.dispose());
    const pending = expect(client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    const sent = host.sent[0];
    // An unrelated UUID cannot settle the current exchange.
    host.emit("message", { ...sent, kind: "response", requestId: randomUUID(), ok: true,
      receipt: { attemptId: "B", scope: "other", populated: false, pids: [] } });
    const response: any = { ...sent, kind: "response", ok: true,
      receipt: { attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101] } };
    delete response.deadlineAt;
    if (field === "correlation") response.operation = "join";
    if (field === "binding") response.binding.generation = "other";
    if (field === "shape") response.target = "B";
    if (field === "receipt") response.receipt.scope = "other";
    if (field === "error") { response.ok = false; delete response.receipt;
      response.error = { name: "AttemptExitUnconfirmedError", code: "ATTEMPT_EXIT_UNCONFIRMED", reason: "bad", attemptId: "B", remainingPids: [] }; }
    host.emit("message", response); await pending;
  });

  it("pins the launch binding and normalizes unknown adapter failures without leaking details", async () => {
    const dependency = adapter({ attemptMembership: vi.fn(async () => { throw new Error("private auth material SENTINEL"); }) });
    const f = fixture(dependency);
    f.expected.launchAttestation.attemptId = "other"; f.expected.generation = "other";
    await expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    expect(f.child.sent[0].binding.launchAttestation.attemptId).toBe("attempt-A");
    expect(JSON.stringify(f.child.sent)).not.toContain("SENTINEL");
    expect(dependency.attemptMembership).toHaveBeenCalledWith("attempt-A");
  });

  it.each([Infinity, NaN, 100, Date.now() + 130_000])("client refuses a non-future/uncapped absolute join deadline %s", async deadline => {
    const f = fixture(adapter());
    await expect(f.client.joinAttemptExit("attempt-A", deadline)).rejects.toBeInstanceOf(ContainmentUnavailableError);
    expect(f.host.sent).toHaveLength(0);
  });

  it("revalidates current custody at native send, not only adapter receipt generation", async () => {
    const expected = binding(); const child = new Channel(); const host = new Channel();
    child.peer = host; host.peer = child;
    let currentChecks = 0;
    const server = createLauncherContainmentServer({ child: child as unknown as ChildProcess,
      native: { exited: false }, binding: expected, containment: adapter(),
      isCurrent: () => ++currentChecks < 5 }); // Fifth check is reply emission after all adapter checks.
    disposals.push(() => server.dispose());
    expect(await raw(child, request(expected))).toMatchObject({ ok: false, error: { code: "CONTAINMENT_UNAVAILABLE" } });
    expect(currentChecks).toBe(5);
  });

  it("caps retained adapter promises rather than dropping timed-out custody to admit more", async () => {
    vi.useFakeTimers();
    const work = deferred<AttemptMembership>(); const dependency = adapter({ attemptMembership: vi.fn(() => work.promise) });
    const f = fixture(dependency, 10);
    const requests = Array.from({ length: 16 }, () => expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError));
    await vi.advanceTimersByTimeAsync(10); await Promise.all(requests);
    expect(f.server.pending).toBe(16);
    await expect(f.client.attemptMembership("attempt-A")).rejects.toThrow("custody is full");
    expect(dependency.attemptMembership).toHaveBeenCalledTimes(16);
    work.resolve({ attemptId: "attempt-A", scope: "owned-A", populated: true, pids: [101] });
    await vi.advanceTimersByTimeAsync(0);
    expect(f.server.pending).toBe(0);
  });

  it("refuses missing attestation, absent channel and native send failure", async () => {
    expect(() => createLauncherContainmentClient({ channel: new Channel(), binding: {} as LauncherContainmentBinding })).toThrow(ContainmentUnavailableError);
    const f = fixture(); f.host.connected = false;
    await expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    f.host.connected = true;
    vi.spyOn(f.host, "send").mockImplementation((_value, callback) => { callback(new Error("closed IPC")); return false; });
    await expect(f.client.attemptMembership("attempt-A")).rejects.toBeInstanceOf(ContainmentUnavailableError);
  });

  it("crosses real Node ChildProcess IPC, refuses live self-join, then permits only launcher-local after-exit join", async () => {
    const compiled = buildSync({ entryPoints: ["src/residency/launcher-containment-control.ts"], bundle: true,
      write: false, format: "esm", platform: "node", target: "node24" }).outputFiles[0]!.text;
    const expected = binding();
    const source = `
      const guard = setTimeout(() => process.exit(71), 3000);
      const { createLauncherContainmentClient } = await import(${JSON.stringify("data:text/javascript;base64," + Buffer.from(compiled).toString("base64"))});
      process.once("message", async binding => {
        const client = createLauncherContainmentClient({ channel: process, binding, timeoutMs: 1000 });
        try {
          const membership = await client.attemptMembership(binding.launchAttestation.attemptId);
          let joinError;
          try { await client.joinAttemptExit(binding.launchAttestation.attemptId, Date.now() + 1000); }
          catch (error) { joinError = { name: error.name, code: error.code }; }
          client.dispose();
          process.send({ event: "proof", membership, joinError }, () => { clearTimeout(guard); process.disconnect(); });
        } catch (error) { process.send({ event: "failure", reason: String(error) }, () => process.exit(72)); }
      });
    `;
    const child = spawn(process.execPath, ["--input-type=module", "--eval", source], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
    const native = { exited: false };
    const exit = new Promise<number | null>(resolve => child.once("exit", code => { native.exited = true; resolve(code); }));
    const proof = new Promise<any>((resolve, reject) => {
      child.on("message", value => { if ((value as any).event === "proof") resolve(value); if ((value as any).event === "failure") reject(new Error((value as any).reason)); });
      child.once("error", reject);
      child.once("exit", code => { if (code !== 0) reject(new Error(`Native IPC proof child exited ${code}`)); });
    });
    expected.child.pid = child.pid!;
    const dependency = adapter();
    const server = createLauncherContainmentServer({ child, native, binding: expected, containment: dependency, isCurrent: () => true });
    disposals.push(() => server.dispose());
    child.send(expected);
    const result = await proof;
    expect(result).toMatchObject({ event: "proof", membership: { attemptId: "attempt-A", scope: "owned-A", populated: true },
      joinError: { name: "ContainmentUnavailableError", code: "CONTAINMENT_UNAVAILABLE" } });
    expect(dependency.joinAttemptExit).not.toHaveBeenCalled();
    expect(await exit).toBe(0);
    await expect(server.joinAttemptExit(Date.now() + 500)).resolves.toMatchObject({ attemptId: "attempt-A", populated: false });
    expect(server.pending).toBe(0); expect(dependency.launchInScope).not.toHaveBeenCalled();
  }, 5_000);
});
