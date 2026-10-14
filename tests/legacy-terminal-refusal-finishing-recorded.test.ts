import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { TaskCgroupStopUnconfirmedError } from "../src/agents/transports/task-cgroup.js";
import { TerminalNoticeOutbox, readTerminalAdmission } from "../src/agents/terminal-notices.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";

// Keep reason: recorded real catch of pi-fabric#722 hosted Windows failures.
// Drive the real manager/notice outbox over the legacy worker's persisted rows.
// No retention entrypoint, real native launch, process signal, or product deletion.
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(() => { throw new Error("Unexpected native launch in recorded refusal probe"); }) };
});

// Removal recorders cannot release physical mkdir locks. This single-writer
// probe replaces only lock acquisition/release, not receipt validation or atomic
// writes. It is not a lock-contention/durability proof and bypasses no delete guard.
vi.mock("../src/core/atomic-write.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/core/atomic-write.js")>(),
  withExclusiveFileLock: <T>(_options: unknown, operation: () => T): T => operation(),
}));

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const managers: { manager: AgentManager; debt?: boolean }[] = [];
const exits: (() => void)[] = [];
beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  vi.spyOn(fs.promises, "rm").mockResolvedValue();
  vi.spyOn(process, "kill").mockImplementation(() => { throw new Error("Unexpected process signal in recorded refusal probe"); });
});
afterEach(async () => {
  Object.defineProperty(process, "platform", platformDescriptor);
  exits.splice(0).forEach(exit => exit());
  try {
    await vi.advanceTimersByTimeAsync(500);
    for (const { manager, debt } of managers.splice(0)) {
      const closing = manager.close().then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(8_000);
      const failure = await closing;
      if (!debt && failure !== undefined) throw failure;
    }
  } finally {
    vi.useRealTimers();
    vi.restoreAllMocks();
  }
  // Fixture trees belong to the lane's private test TMPDIR; its owner cleans them.
});

const sender = { id: "session:original", kind: "main" as const, verified: "mesh" as const };
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const, sender };
const cases = [
  { variant: "legacy-refused", state: "refused", routed: false },
  { variant: "legacy-queued", state: "queued", routed: false },
  { variant: "legacy-delivered", state: "delivered", routed: false },
  { variant: "mismatched-sender", state: "refused", forwarded: false, wrongSender: true, routed: false },
  { variant: "refused-unforwarded", state: "refused", forwarded: false, routed: true },
  { variant: "refused-discard-ack", state: "refused", forwarded: true, routed: true },
] as const;

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-refusal-recorded-"));
  const records = new Map<string, AgentRunRecord>();
  const closures = new Map<string, () => void>();
  const notices: AgentTerminalNotice[] = [];
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const args = new Map<string, string>();
    for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
    const record: AgentRunRecord = { id: request.id, name: request.name, task: "task", status: "running", runner: "pi", transport: "process",
      cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, text: "intermediate prose",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
    records.set(request.id, record);
    writeJsonAtomic(args.get("--status-file")!, record);
    let alive = true;
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = () => { alive = false; resolve(); }; });
    closures.set(request.id, close); exits.push(close);
    return { kind: "process", closed, stopDebt: () => undefined, waitForClose: () => closed,
      isAlive: async () => alive, stop: async () => { await closed; } };
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns: true, sessionExport: false },
    { workerPath: path.join(root, "recorded-worker.mjs"), runRoot: root, onTerminalNotice: notice => { notices.push(notice); } });
  managers.push({ manager });
  return { root, manager, launch, notices, close: (id: string) => closures.get(id)!(), finish: (id: string) => {
    const receipt = saveFinalAnswerReceipt(path.join(root, id), id, "done");
    writeJsonAtomic(path.join(root, id, "status.json"), { ...records.get(id)!, status: "completed", text: "done",
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    return receipt;
  } };
};

const holdUntilClose = async (f: ReturnType<typeof fixture>, id: string) => {
  const receipt = f.finish(id);
  let waited = false;
  let joined = false;
  const wait = f.manager.wait(id).then(result => { waited = true; return result; });
  const join = f.manager.join(id).then(() => { joined = true; });
  expect(f.manager.status(id)).toMatchObject({ status: "finishing", text: "done", finalAnswerReceipt: { id: receipt.id } });
  const next = await f.manager.spawn({ task: "native-close barrier", transport: "process" });
  expect(next.status).toBe("queued");
  // Advance the ordinary status wake, not the finite cleanup deadline. Nothing
  // can settle or launch next while this exact native close is still pending.
  await vi.advanceTimersByTimeAsync(500);
  expect(f.manager.status(id).status).toBe("finishing");
  expect(f.manager.listForUi().find(run => run.id === id)?.status).toBe("finishing");
  expect(f.manager.isSettled(id)).toBe(false);
  expect(f.manager.hasRetainedCustody()).toBe(true);
  expect([waited, joined]).toEqual([false, false]);
  expect(f.launch).toHaveBeenCalledOnce();
  return { receipt, next, wait, join };
};
const closeAndJoin = async (f: ReturnType<typeof fixture>, id: string, held: Awaited<ReturnType<typeof holdUntilClose>>) => {
  f.close(id);
  await vi.advanceTimersByTimeAsync(0);
  expect(await held.wait).toMatchObject({ status: "completed", text: "done" });
  await held.join;
  expect(f.manager.status(id).status).toBe("completed");
  expect(f.launch).toHaveBeenCalledTimes(2);
  f.close(held.next.id);
};

describe("recorded legacy refusal attribution and #722 finishing admission", () => {
  it.each(cases)("$variant: sender notice routed=$routed without early settlement", async variant => {
    const f = fixture();
    const handle = await f.manager.spawn({ task: "ordinary", transport: "process" });
    const directory = path.join(f.root, handle.id);
    const sent = f.manager.steer(handle.id, "late steer", undefined, provenance);
    expect(sent.undelivered).toBeUndefined();
    const registry = path.join(directory, "terminal-controls", `${sent.messageId}.json`);
    writeJsonAtomic(registry, { id: sent.messageId, delivery: "steer", state: variant.state,
      provenance: "wrongSender" in variant ? { ...provenance, sender: { ...sender, id: "session:other" } } : provenance,
      ...("forwarded" in variant ? { forwarded: variant.forwarded } : {}) });
    const persisted = fs.readFileSync(registry, "utf8");
    const held = await holdUntilClose(f, handle.id);
    const expected: AgentTerminalNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: held.receipt.id, messageId: sent.messageId, delivery: "steer", sender };
    // A proven refusal is routed to its original admitted sender even BEFORE close;
    // legacy/forged rows remain uncertain. The receipt freezes any new input now.
    expect(f.notices).toEqual(variant.routed ? [expected] : []);
    expect(() => f.manager.steer(handle.id, "after receipt", undefined, provenance))
      .toThrowError(expect.objectContaining({ code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: held.receipt.id }));
    await closeAndJoin(f, handle.id, held);
    expect(f.notices).toEqual(variant.routed ? [expected] : []);
    const pending = path.join(directory, "terminal-notices", `${sent.messageId}.json`);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(variant.routed);
    // Removal is recorded only: retain the pending bytes while proving its routed
    // receipt still suppresses duplicate sender delivery during restart recovery.
    if (variant.routed) expect(fs.rmSync).toHaveBeenCalledWith(pending);
    const recovered: AgentTerminalNotice[] = [];
    const outbox = new TerminalNoticeOutbox(notice => { recovered.push(notice); });
    outbox.recover(directory); outbox.recover(directory);
    await vi.advanceTimersByTimeAsync(0);
    expect(recovered).toEqual([]);
    expect(readTerminalAdmission(directory, sent.messageId)).toEqual({ id: sent.messageId, delivery: "steer", sender });
    expect(fs.readFileSync(registry, "utf8")).toBe(persisted);
    expect(fs.existsSync(directory)).toBe(true);
  });

  it("does not route a stale pending notice or old routed receipt for a legacy refusal", async () => {
    const f = fixture();
    const handle = await f.manager.spawn({ task: "ordinary", transport: "process" });
    const directory = path.join(f.root, handle.id);
    const sent = f.manager.steer(handle.id, "late steer", undefined, provenance);
    writeJsonAtomic(path.join(directory, "terminal-controls", `${sent.messageId}.json`),
      { id: sent.messageId, delivery: "steer", state: "refused", provenance });
    const held = await holdUntilClose(f, handle.id);
    await closeAndJoin(f, handle.id, held);
    const pending = path.join(directory, "terminal-notices", `${sent.messageId}.json`);
    const notice: AgentTerminalNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: held.receipt.id, messageId: sent.messageId, delivery: "steer", sender };
    writeJsonAtomic(pending, notice);
    const recovered: AgentTerminalNotice[] = [];
    const outbox = new TerminalNoticeOutbox(value => { recovered.push(value); });
    outbox.recover(directory);
    await vi.advanceTimersByTimeAsync(0);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(false);
    writeJsonAtomic(`${pending}.delivered`, notice);
    outbox.recover(directory); f.manager.status(handle.id);
    await vi.advanceTimersByTimeAsync(0);
    expect(recovered).toEqual([]);
    expect(f.notices).toEqual([]);
    expect(fs.existsSync(pending)).toBe(true);
    expect(fs.rmSync).not.toHaveBeenCalledWith(pending);
  });
});

// Same public stop boundary as process-transport.test.ts, with removers recorded.
// This mocked manager contract is portable: native Linux scope eligibility does
// not decide whether the public catch preserves an adapter's typed stop failure.
describe("recorded AgentManager lostContact stop contract", () => {
  const scenario = async (options: { cgroup?: string; genericFailure?: boolean; branch?: "native" | "win32" } = {}) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-stop-debt-recorded-"));
    const { cgroup } = options;
    const debt = "Owned scope watcher failed: Error: EIO";
    const failure = cgroup ? new TaskCgroupStopUnconfirmedError(cgroup, [424242], "SIGTERM/SIGKILL did not empty the subtree")
      : options.genericFailure ? new Error("recorded generic native stop failure") : undefined;
    const stop = vi.fn(async () => { if (failure) throw failure; });
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = resolve; }); exits.push(close);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => ({
      kind: "process", ...(cgroup ? { cgroup } : {}), isAlive: async () => true,
      lostContact: () => options.genericFailure ? undefined : debt,
      stopDebt: () => options.genericFailure ? undefined : debt, waitForClose: async () => undefined,
      closed, stop,
    }));
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false, retainRuns: true, maxConcurrent: 1 },
      { workerPath: path.join(root, "recorded-worker.mjs"), runRoot: root });
    managers.push({ manager, debt: true });
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    // Keep construction/imports native; force only the manager's existing runtime branch.
    if (options.branch === "win32") Object.defineProperty(process, "platform", { ...platformDescriptor, value: "win32" });
    const outcome = await manager.stop(handle.id).then(result => result, (error: unknown) => error);
    return { root, manager, handle, stop, failure, outcome };
  };
  it.each(["native", "win32"] as const)("preserves the exact owned cgroup stop failure at public stop (%s branch)", async branch => {
    // Model the manager's existing Windows branch without invoking a Windows
    // transport or pretending a real cgroup can launch there.
    const cgroup = "/user.slice/user-1.slice/user@1.service/app.slice/fabric-task-x.scope";
    const f = await scenario({ cgroup, branch });
    expect(f.stop).toHaveBeenCalledOnce();
    expect(f.outcome).toBe(f.failure); // Do not lose the typed instance, code, or owned PID receipt.
    expect(f.outcome).toBeInstanceOf(TaskCgroupStopUnconfirmedError);
    expect(f.outcome).toMatchObject({ code: "TASK_CGROUP_STOP_UNCONFIRMED", cgroup, pids: [424242] });
    expect(f.manager.hasRetainedCustody()).toBe(true);
    expect(fs.existsSync(path.join(f.root, f.handle.id, "unresolved-worker.json"))).toBe(true);
    const next = await f.manager.spawn({ task: "must not overlap failed cgroup stop", transport: "process" });
    expect(next.status).toBe("queued");
    await f.manager.stop(next.id);
  });
  it.each(["native", "win32"] as const)("keeps uncapped native stop debt logical, but retains files and admission (%s branch)", async branch => {
    const f = await scenario({ branch });
    expect(f.stop).not.toHaveBeenCalled();
    expect(f.outcome).not.toBeInstanceOf(Error);
    expect(f.outcome).toMatchObject({ status: "stopped" });
    expect(f.manager.hasRetainedCustody()).toBe(true);
    expect(f.manager.heldPermits()).toBe(1);
    expect(fs.existsSync(path.join(f.root, f.handle.id, "unresolved-worker.json"))).toBe(true);
    const next = await f.manager.spawn({ task: "must not overlap native stop debt", transport: "process" });
    expect(next.status).toBe("queued");
    await f.manager.stop(next.id);
  });
  it("keeps a generic native stop failure generic while retaining custody", async () => {
    const f = await scenario({ genericFailure: true });
    expect(f.stop).toHaveBeenCalled();
    expect(f.outcome).toBeInstanceOf(Error);
    expect(f.outcome).not.toBeInstanceOf(TaskCgroupStopUnconfirmedError);
    expect(f.outcome).not.toBe(f.failure);
    expect(f.outcome).toMatchObject({ name: "Error", message: expect.stringContaining("execution exit unconfirmed; custody retained") });
    expect(f.manager.hasRetainedCustody()).toBe(true);
    expect(fs.existsSync(path.join(f.root, f.handle.id, "unresolved-worker.json"))).toBe(true);
  });
  // Launch cancellation is covered by the two canonical recorded cases in
  // tests/agent-manager-cancelled-launch-no-delete.test.ts; do not duplicate it here.
});
