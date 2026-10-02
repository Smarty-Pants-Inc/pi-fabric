import { execFile, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { executeFile } from "../src/agents/transports/process-utils.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { externalSessionHandle } from "../src/agents/transports/external-session.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, execFile: vi.fn(actual.execFile) };
});

const children: Array<{ child: ChildProcess; closed: Promise<void>; didClose: boolean }> = [];
const capture = async () => {
  vi.mocked(execFile).mockClear();
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  vi.mocked(execFile).mockImplementation((...args: Parameters<typeof execFile>) => {
    const child = actual.execFile(...args);
    const owned = { child, closed: Promise.resolve(), didClose: false };
    owned.closed = new Promise<void>(resolve => child.once("close", () => { owned.didClose = true; resolve(); }));
    children.push(owned);
    return child;
  });
};
afterEach(async () => {
  for (const owned of children.splice(0)) {
    if (owned.child.exitCode === null && owned.child.signalCode === null) owned.child.kill("SIGKILL");
    await owned.closed;
  }
  vi.restoreAllMocks();
});

describe.each(["tmux", "screen"] as const)("native %s observation query teardown", kind => {
  it.each(["deadline", "abort"] as const)("reports unknown on %s only after the hung CLI closed", async mode => {
    await capture();
    const execute = processUtils.executeFile;
    vi.spyOn(processUtils, "executeFile").mockImplementation((_command, _args, options) =>
      execute(process.execPath, ["-e", "process.on('SIGTERM',()=>{});setInterval(()=>{},1000)"], options));
    const handle = externalSessionHandle(kind, "pi-fabric-native-query");
    const abort = new AbortController();
    const pending = handle.observe!({ signal: abort.signal, deadline: Date.now() + (mode === "deadline" ? 80 : 5_000) });
    if (mode === "abort") abort.abort();
    try {
      expect(await pending).toMatchObject({ state: "unknown" });
      expect(handle.lostContact?.()).toBeDefined();
      expect(children).toHaveLength(1); expect(children[0]!.didClose).toBe(true);
      expect(vi.mocked(execFile).mock.calls[0]![2]).toMatchObject({ killSignal: "SIGKILL" });
      expect(handle.relaunchable).toBe(false);
    } finally { abort.abort(); await pending; }
  });
});
describe("executeFile native bounded queries (including Windows)", () => {
  it("waits for native close on success and preserves failed-command output", async () => {
    await capture();
    await expect(executeFile(process.execPath, ["-e", "console.log('out');console.error('err')"])).resolves.toEqual({ stdout: "out\n", stderr: "err\n" });
    expect(children[0]!.didClose).toBe(true);
    expect(vi.mocked(execFile).mock.calls[0]![2]).toMatchObject({ killSignal: "SIGTERM" });
    await expect(executeFile(process.execPath, ["-e", "console.log('partial');console.error('denied');process.exit(2)"])).rejects.toMatchObject({ code: 2, stdout: "partial\n", stderr: "denied\n" });
    expect(children[1]!.didClose).toBe(true);
  });

  it("does not create a child for an already-aborted query", async () => {
    await capture();
    const abort = new AbortController(); abort.abort();
    await expect(executeFile(process.execPath, ["-e", "setInterval(()=>{},1000)"], { signal: abort.signal })).rejects.toThrow(/cancelled/);
    expect(children).toHaveLength(0);
  });

  it.each(["timeout", "abort"] as const)("kills a hung query on %s and rejects only after native close", async mode => {
    await capture();
    const abort = new AbortController();
    // On POSIX a cooperative SIGTERM is insufficient; Windows exercises native
    // TerminateProcess and close-before-cwd-cleanup through the same API.
    const source = "process.on('SIGTERM',()=>{});console.log('ready');setInterval(()=>{},1000)";
    const pending = executeFile(process.execPath, ["-e", source], { timeoutMs: mode === "timeout" ? 300 : 5_000, signal: abort.signal, killSignal: "SIGKILL" });
    const outcome = pending.then(() => "accepted", error => error);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      if (mode === "abort") {
        await new Promise<void>((resolve, reject) => {
          timer = setTimeout(() => reject(new Error("Query fixture did not become ready")), 4_000);
          children[0]!.child.stdout!.once("data", () => { clearTimeout(timer); resolve(); });
        });
        abort.abort();
      }
      expect(await outcome).toBeInstanceOf(Error);
      expect(children[0]!.didClose).toBe(true);
      expect(children[0]!.child.exitCode !== null || children[0]!.child.signalCode !== null).toBe(true);
    } finally {
      clearTimeout(timer); abort.abort(); await outcome;
    }
  }, 10_000);

  it("waits for close on a missing executable rather than inferring session absence", async () => {
    await capture();
    await expect(executeFile("fabric-deliberately-missing-command-3104", [], { timeoutMs: 100 })).rejects.toMatchObject({ code: "ENOENT" });
    expect(children[0]!.didClose).toBe(true);
  });
});
