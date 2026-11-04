import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentNativeExitDiagnostic, AgentTransportHandle } from "../src/agents/types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { observeProcessCgroup } from "../src/agents/transports/process-utils-legacy.js";

// Keep reason: #17191 lost workers had no retained native exit cause. These
// subprocess checks cross the native/transport boundary, including its tree debt.
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.mocked(spawn).mockImplementation((await vi.importActual<typeof import("node:child_process")>("node:child_process")).spawn);
});

const routes = ["default actor", "scoped actor"] as const;
type Route = typeof routes[number];
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
const processStartTime = (pid: number): string | undefined => {
  if (process.platform !== "linux") return undefined;
  const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
};

async function withWorker(
  route: Route,
  run: (handle: AgentTransportHandle, child: ChildProcess, root: string) => Promise<void>,
  options: { descendant?: boolean; missingCgroup?: boolean } = {},
) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-exit-"));
  let handle: AgentTransportHandle | undefined;
  let child: ChildProcess | undefined;
  let nativeClosed: Promise<void> | undefined;
  try {
    // Actors must retain the uncapped path, regardless of the host's user bus.
    vi.stubEnv("XDG_RUNTIME_DIR", undefined);
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
    vi.stubEnv("FABRIC_TASK_SYSTEMD_RUN_WRAPPER", undefined);
    if (route === "scoped actor") {
      // Exec-in-place backend exercises the scoped launcher without pretending
      // that this caller cgroup is an admitted tree/kill boundary.
      fs.writeFileSync(path.join(root, "systemd-run"), '#!/bin/sh\nwhile [ "$1" != "--" ]; do shift; done\nshift\nexec "$@"\n', { mode: 0o700 });
      vi.stubEnv("PATH", `${root}${path.delimiter}${process.env.PATH ?? ""}`);
    }
    if (options.missingCgroup) {
      const open = fs.openSync.bind(fs);
      vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        if (/^\/proc\/\d+\/cgroup$/.test(String(file))) throw Object.assign(new Error("diagnostic denied"), { code: "EACCES" });
        return open(file, ...args as [string | number, fs.Mode?]);
      }) as typeof fs.openSync);
    }
    const descendant = `import fs from "node:fs";
const watch = fs.watch(".", (_event, file) => {
  if (file?.toString() === "finish-tree") {
    fs.writeFileSync("descendant-done", "done"); watch.close();
  }
});
fs.writeFileSync("descendant-ready", String(process.pid));`;
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from "node:fs";
${options.descendant ? `import { spawn } from "node:child_process";\nspawn(process.execPath, ["--input-type=module", "-e", ${JSON.stringify(descendant)}], { stdio: "ignore" });` : ""}
const watch = fs.watch(".", (_event, file) => {
  if (file?.toString() === "finish") { watch.close(); process.exit(23); }
});
fs.writeFileSync("ready", String(process.pid));`);
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    vi.mocked(spawn).mockClear();
    vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => {
      child = actual.spawn(...args);
      nativeClosed = new Promise<void>(resolve => child!.once("close", () => resolve()));
      return child;
    });
    handle = await new ProcessTransport(route === "scoped actor" ? "diagnostic.slice" : undefined).launch({
      id: "native-exit", name: "native-exit", cwd: root, workerPath: worker,
      workerArguments: ["--actor-id", "diagnostic-actor", "--actor-name", "diagnostic-actor"],
    });
    expect(child?.pid).toBe(Number(handle.sessionId));
    await vi.waitFor(() => expect(fs.existsSync(path.join(root, "ready"))).toBe(true));
    if (options.descendant) await vi.waitFor(() => expect(fs.existsSync(path.join(root, "descendant-ready"))).toBe(true));
    if (route === "scoped actor") expect(vi.mocked(spawn).mock.calls[0]?.[0]).toBe(path.join(root, "systemd-run"));
    await run(handle, child!, root);
  } finally {
    try {
      if (handle) { await handle.stop(); await handle.waitForClose?.(); }
    } finally {
      // If launch/assertion failed, still join the captured native child, not a
      // possibly recycled PID. Normal descendant teardown belongs to handle.stop.
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      if (nativeClosed) await nativeClosed;
      fs.rmSync(root, { recursive: true, force: true });
    }
  }
}

function expectCgroup(diagnostic: AgentNativeExitDiagnostic, membership: string | undefined) {
  if (process.platform !== "linux") expect(diagnostic.cgroup).toEqual({ state: "unsupported", reason: "linux-procfs-required" });
  else if (diagnostic.cgroup.state === "observed") {
    expect(diagnostic.cgroup.source).toBe("procfs");
    expect(diagnostic.cgroup.membership).toBe(membership);
    expect(Buffer.byteLength(diagnostic.cgroup.membership)).toBeLessThanOrEqual(4096);
    expect(diagnostic.cgroup.observedAt).toBeLessThanOrEqual(diagnostic.exitedAt);
  } else expect(diagnostic.cgroup).toMatchObject({ state: "unavailable", reason: expect.any(String) });
}

for (const route of routes) describe.skipIf(route === "scoped actor" && process.platform !== "linux")(`${route} native exit diagnostics`, () => {
  it("retains a real exit code at primaryExit, binding launch birth without granting cgroup authority", async () => {
    await withWorker(route, async (handle, child, root) => {
      const started = processStartTime(child.pid!);
      let membership: string | undefined;
      if (process.platform === "linux") { try { membership = fs.readFileSync(`/proc/${child.pid}/cgroup`, "utf8").trim(); } catch { /* allowed unavailable */ } }
      expect(handle.nativeExit?.()).toBeUndefined();
      expect(handle.cgroup).toBeUndefined();
      expect(handle.treeClosed).toBeUndefined();
      const before = Date.now();
      const atPrimaryExit = handle.primaryExit!.then(() => handle.nativeExit?.());
      fs.writeFileSync(path.join(root, "finish"), "exit");
      const diagnostic = await atPrimaryExit;
      expect(diagnostic).toMatchObject({ scope: "primary", pid: child.pid, code: 23, signal: null,
        ...(started ? { processStartTime: started } : {}) });
      expect(diagnostic!.exitedAt).toBeGreaterThanOrEqual(before);
      expect(diagnostic!.exitedAt).toBeLessThanOrEqual(Date.now());
      expectCgroup(diagnostic!, membership);
      expect(Object.isFrozen(diagnostic)).toBe(true);
      expect(Object.isFrozen(diagnostic!.cgroup)).toBe(true);
      await handle.stop();
      expect(handle.nativeExit?.()).toBe(diagnostic); // stop cannot rewrite native cause
      expect(handle.cgroup).toBeUndefined();
    });
  });

  it.skipIf(process.platform === "win32")("retains real SIGKILL with a null code even when diagnostic cgroup I/O is unavailable", async () => {
    await withWorker(route, async (handle, child) => {
      const started = processStartTime(child.pid!);
      expect(handle.nativeExit?.()).toBeUndefined();
      const atPrimaryExit = handle.primaryExit!.then(() => handle.nativeExit?.());
      child.kill("SIGKILL"); // exact captured child, not numeric PID signaling
      const diagnostic = await atPrimaryExit;
      expect(diagnostic).toMatchObject({ scope: "primary", pid: child.pid, code: null, signal: "SIGKILL",
        ...(started ? { processStartTime: started } : {}) });
      expect(diagnostic!.cgroup.state).toBe(process.platform === "linux" ? "unavailable" : "unsupported");
      expect(handle.cgroup).toBeUndefined();
      await handle.stop();
      expect(handle.nativeExit?.()).toBe(diagnostic);
      expect(handle.lostContact?.()).toBeUndefined();
    }, { missingCgroup: true });
  });

  it.skipIf(process.platform !== "linux")("keeps surviving descendant custody after primary SIGKILL and native close", async () => {
    await withWorker(route, async (handle, child, root) => {
      expect(await handle.isAlive()).toBe(true); // retain descendant birth while parent pins it
      child.kill("SIGKILL");
      await handle.closed;
      const diagnostic = handle.nativeExit?.();
      expect(diagnostic).toMatchObject({ scope: "primary", pid: child.pid, code: null, signal: "SIGKILL" });
      expect(await handle.isAlive()).toBe(true);
      expect(handle.treeClosed).toBeUndefined();
      expect(handle.cgroup).toBeUndefined();
      fs.writeFileSync(path.join(root, "finish-tree"), "exit");
      await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false));
      expect(fs.existsSync(path.join(root, "descendant-done"))).toBe(true);
      expect(handle.nativeExit?.()).toBe(diagnostic);
    }, { descendant: true });
  });
});

for (const route of routes) it.skipIf(route === "scoped actor" && process.platform !== "linux")(`${route}: publishes diagnostics at primaryExit even when captured close lags`, async () => {
  // Recorded inherited handles can keep native close pending indefinitely after
  // primary exit; diagnostics must be retainable without waiting for those handles.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-close-lag-"));
  const child = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn() });
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    if (route === "scoped actor") {
      const markerIndex = args!.indexOf("fabric-scope") + 1;
      fs.writeFileSync(args![markerIndex]!, `admitted ${args![markerIndex + 1]}`);
    }
    return child as unknown as ChildProcess;
  });
  try {
    const handle = await spawnDetached("worker.mjs", [], root, undefined, undefined,
      route === "scoped actor" ? { executable: "/fixture-systemd-run", slice: "diagnostic.slice", warn: vi.fn() } : undefined);
    expect(handle.primaryExit).toBeInstanceOf(Promise);
    let closed = false;
    void handle.closed.then(() => { closed = true; });
    const atPrimaryExit = handle.primaryExit!.then(() => handle.nativeExit?.());
    const timeout = vi.spyOn(globalThis, "setTimeout");
    const interval = vi.spyOn(globalThis, "setInterval");
    const read = vi.spyOn(fs, "readFileSync");
    const open = vi.spyOn(fs, "openSync");
    const kill = vi.spyOn(process, "kill");
    child.emit("exit", null, "SIGKILL");
    const diagnostic = await atPrimaryExit;
    expect(diagnostic).toMatchObject({ scope: "primary", pid: child.pid, code: null, signal: "SIGKILL",
      cgroup: { state: process.platform === "linux" ? "unavailable" : "unsupported" } });
    await tick();
    expect(closed).toBe(false);
    expect(handle.nativeExit?.()).toBe(diagnostic);
    expect(timeout).not.toHaveBeenCalled();
    expect(interval).not.toHaveBeenCalled();
    expect(read).not.toHaveBeenCalled();
    expect(open).not.toHaveBeenCalled();
    expect(kill).not.toHaveBeenCalled();
    child.emit("close", null, "SIGKILL");
    await handle.closed;
    expect(handle.nativeExit?.()).toBe(diagnostic);
  } finally {
    child.emit("close", null, "SIGKILL");
    fs.rmSync(root, { recursive: true, force: true });
  }
});

it.skipIf(process.platform !== "linux").each([false, true])("bounds cgroup diagnostics and closes its fd even with partial reads (%s)", partial => {
  const started = processStartTime(process.pid)!;
  const close = vi.spyOn(fs, "closeSync");
  const requestedLengths: number[] = [];
  const read = vi.spyOn(fs, "readSync").mockImplementation(((_fd: number, buffer: Buffer, offset: number, length: number) => {
    requestedLengths.push(length);
    const size = partial && offset === 0 ? 4096 : length;
    buffer.fill(120, offset, offset + size);
    return size;
  }) as typeof fs.readSync);
  expect(observeProcessCgroup(process.pid, started)).toEqual({ state: "unavailable", reason: "cgroup-membership-too-large" });
  expect(requestedLengths[0]).toBe(4097);
  expect(read).toHaveBeenCalledTimes(partial ? 2 : 1);
  expect(close).toHaveBeenCalledOnce();
});
