import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { DEFAULT_JEV_CONFIG } from "../src/jev/config.js";
const { spawn } = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn }));
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const fakeChild = (pid: number) => Object.assign(new EventEmitter(), {
  pid, kill: vi.fn(), stdout: new EventEmitter(), stderr: new EventEmitter(),
});
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.useRealTimers(); vi.restoreAllMocks(); vi.clearAllMocks();
});

describe("SR-7 Windows credential retirement through the real tree helper", () => {
  const cases = (["cancel", "parent error event", "early error", "early success"] as const).flatMap(terminal =>
    (["spawn failure", "error", "nonzero", "timeout"] as const).map(failure => ({ terminal, failure })));
  it.each(cases)("$terminal / $failure cannot discharge the owned-tree obligation", async ({ terminal, failure }) => {
    vi.useFakeTimers();
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const alarm = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const child = fakeChild(1234); const killer = fakeChild(5678);
    spawn.mockReturnValueOnce(child);
    if (failure === "spawn failure") spawn.mockImplementationOnce(() => { throw new Error("FAKE_SECRET"); });
    else spawn.mockReturnValueOnce(killer);
    const credentials = new JevCredentials(["offline-credential-fixture"], {});
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    const controller = new AbortController();
    const pending = credentials.resolve(controller.signal).then(value => value, error => String(error));
    child.stdout.emit("data", Buffer.from("offline-key\n"));
    child.stderr.emit("data", Buffer.from("FAKE_SECRET"));
    if (terminal === "cancel") controller.abort();
    if (terminal === "parent error event") child.emit("error", new Error("FAKE_SECRET"));
    // A pipe-independent descendant can still be alive after this parent close.
    child.emit("close", terminal === "early success" ? 0 : 7);
    expect(await pending).toBe(terminal === "early success" ? "offline-key" : "Error: Jev credential resolver failed");
    expect(spawn).toHaveBeenNthCalledWith(2, "taskkill", ["/pid", "1234", "/T", "/F"], { windowsHide: true, stdio: "ignore" });
    let retired = false;
    void client.drainCredentials().then(() => { retired = true; });
    if (failure === "error") killer.emit("error", new Error("FAKE_SECRET"));
    if (failure === "timeout") {
      await vi.advanceTimersByTimeAsync(1000);
      expect(killer.kill).toHaveBeenCalledWith("SIGKILL");
    }
    // An error or timeout cannot be undone by a late successful helper close.
    if (failure !== "spawn failure") killer.emit("close", failure === "nonzero" ? 1 : 0);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(retired, "parent-only fallback and helper close must not claim the descendant was joined").toBe(false);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    expect(alarm).toHaveBeenCalledOnce();
    expect(alarm).toHaveBeenCalledWith(expect.stringContaining("retirement remains pending"), { code: "FABRIC_PROCESS_TREE_UNCONFIRMED" });
    expect(alarm.mock.calls[0]![0]).not.toContain("FAKE_SECRET");
    expect(vi.getTimerCount()).toBe(0);
  });
});
