import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { externalSessionHandle, externalSessionInventory, observeExternalSession } from "../src/agents/transports/external-session.js";
import { TmuxTransport } from "../src/agents/transports/tmux-transport.js";
import { ScreenTransport } from "../src/agents/transports/screen-transport.js";

const session = "pi-fabric-0123456789ab";
const screenInventory = (name: string, status = "Detached") => `There is a screen on:\n\t123.${name}\t(10/02/26 19:00:00)\t(${status})\n1 Socket in /tmp/screen.\n`;
const inventory = (kind: "tmux" | "screen", name = session) => kind === "tmux" ? `$0|${name}\n` : screenInventory(name);
afterEach(() => vi.restoreAllMocks());

describe.each(["tmux", "screen"] as const)("checked %s session observation", kind => {
  it("matches full session names and reports absence only from a complete successful inventory", () => {
    expect(externalSessionInventory(kind, session, inventory(kind))).toEqual({ state: "alive" });
    expect(externalSessionInventory(kind, session, inventory(kind, `${session}-other`))).toEqual({ state: "absent" });
    expect(externalSessionInventory(kind, session, inventory(kind, session.slice(0, -1)))).toEqual({ state: "absent" });
    expect(externalSessionInventory(kind, session, "")).toMatchObject({ state: "unknown" });
    expect(externalSessionInventory(kind, session, "No server running / No Sockets found.")).toMatchObject({ state: "unknown" });
  });

  it.each(["socket unavailable", "permission denied", "timeout", "cancelled", "binary missing"])("does not interpret %s query failures as exit", async message => {
    vi.spyOn(processUtils, "executeFile").mockRejectedValue(Object.assign(new Error(message), { code: 1, stdout: inventory(kind, "other") }));
    const handle = externalSessionHandle(kind, session);
    expect(await handle.observe!()).toMatchObject({ state: "unknown", reason: expect.stringContaining(message) });
    expect(await handle.isAlive()).toBe(false);
    expect(handle.lostContact?.()).toContain(message);
    expect(handle.relaunchable).toBe(false);
  });

  it("preserves unknown diagnostics and recovers contact without minting an exit receipt", async () => {
    const execute = vi.spyOn(processUtils, "executeFile").mockResolvedValue({ stdout: inventory(kind, "other"), stderr: "socket warning" });
    const handle = externalSessionHandle(kind, session);
    expect(await handle.observe!()).toMatchObject({ state: "unknown" });
    expect(handle.lostContact?.()).toBeDefined();
    execute.mockResolvedValue({ stdout: inventory(kind), stderr: "" });
    expect(await handle.isAlive()).toBe(true);
    expect(handle.lostContact?.()).toBeUndefined();
    execute.mockResolvedValue({ stdout: inventory(kind, "other"), stderr: "" });
    expect(await handle.isAlive()).toBe(false);
    expect(handle.lostContact?.()).toBeUndefined();
    expect(handle.relaunchable).toBe(false);
  });

  it("does not launch an already-canceled or expired observation and clamps a future deadline", async () => {
    const execute = vi.spyOn(processUtils, "executeFile").mockResolvedValue({ stdout: inventory(kind), stderr: "" });
    const abort = new AbortController(); abort.abort();
    expect(await observeExternalSession(kind, session, { signal: abort.signal })).toMatchObject({ state: "unknown" });
    expect(await observeExternalSession(kind, session, { deadline: Date.now() - 1 })).toMatchObject({ state: "unknown" });
    expect(await observeExternalSession(kind, session, { deadline: NaN })).toMatchObject({ state: "unknown" });
    expect(execute).not.toHaveBeenCalled();
    const active = new AbortController();
    expect(await observeExternalSession(kind, session, { signal: active.signal, deadline: Date.now() + 200 })).toEqual({ state: "alive" });
    const [command, args, options] = execute.mock.calls[0]!;
    expect(command).toBe(kind);
    expect(args).toEqual(kind === "tmux" ? ["list-sessions", "-F", "#{session_id}|#{session_name}"] : ["-ls"]);
    expect(options!.signal).toBe(active.signal);
    expect(options!.timeoutMs).toBeGreaterThan(0); expect(options!.timeoutMs).toBeLessThanOrEqual(200);
    expect(options!.env!.LC_ALL).toBe("C");
    expect(options!.killSignal).toBe("SIGKILL");
    await observeExternalSession(kind, session);
    expect(execute.mock.calls[1]![2]!.timeoutMs).toBe(3_000);
  });

  it("bounds stop commands and treats stop acknowledgment as neither membership nor exit proof", async () => {
    const execute = vi.spyOn(processUtils, "executeFile").mockResolvedValue({ stdout: "", stderr: "" });
    const handle = externalSessionHandle(kind, session);
    await handle.stop({ deadline: Date.now() + 100 });
    expect(execute.mock.calls[0]![1]).toEqual(kind === "tmux" ? ["kill-session", "-t", `=${session}`] : ["-S", session, "-X", "quit"]);
    expect(execute.mock.calls[0]![2]!.timeoutMs).toBeLessThanOrEqual(100);
    expect(handle.relaunchable).toBe(false);
    execute.mockRejectedValue(new Error("unreachable"));
    await expect(handle.stop()).resolves.toBeUndefined();
    expect(await handle.observe!()).toMatchObject({ state: "unknown" });
  });

  it("routes ambiguous launch failures to the existing persistent unresolved-worker contract", async () => {
    const execute = vi.spyOn(processUtils, "executeFile").mockRejectedValue(new Error("lost reply after session creation"));
    const adapter = kind === "tmux" ? new TmuxTransport() : new ScreenTransport();
    const abort = new AbortController();
    const request = { id: "0123456789abcdef", name: "fixture", cwd: process.cwd(), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), workerArguments: [], signal: abort.signal };
    await expect(adapter.launch(request)).rejects.toMatchObject({ launchOutcome: "unknown", cleanupPending: true, transport: kind, sessionId: session });
    expect(execute.mock.calls[0]![2]).toMatchObject({ signal: abort.signal, timeoutMs: 3_000, cwd: request.cwd });
    execute.mockClear(); abort.abort();
    await expect(adapter.launch(request)).rejects.toThrow(/aborted/);
    expect(execute).not.toHaveBeenCalled();
  });
});

describe("screen inventory completeness", () => {
  it.each([
    screenInventory(session, "Dead ???"),
    screenInventory(session).replace("1 Socket", "2 Sockets"),
    screenInventory(session).replace("1 Socket in /tmp/screen.\n", ""),
    screenInventory(session).replace("There is a screen on:", "unsupported localized header:"),
    screenInventory(session) + "unexpected diagnostic\n",
  ])("vetoes stale, malformed, truncated or unsupported inventories", stdout => {
    expect(externalSessionInventory("screen", session, stdout)).toMatchObject({ state: "unknown" });
  });
  it("supports a healthy multi-socket inventory and screen versions without timestamps", () => {
    const stdout = `There are screens on:\n\t123.other\t(Attached)\n\t456.${session}\t(Detached)\n2 Sockets in /tmp/screen.\n`;
    expect(externalSessionInventory("screen", session, stdout)).toEqual({ state: "alive" });
  });
});
