import * as childProcess from "node:child_process";
import net from "node:net";
import { performance } from "node:perf_hooks";
import type { Duplex } from "node:stream";
import { vi } from "vitest";
import { AgentManager } from "../../src/agents/manager.js";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import { ResidencyClient } from "../../src/residency/client.js";
import { CPythonRuntime } from "../../src/runtime/cpython-runtime.js";

/** Test-only observation and teardown reaping; no execution deadline changes. */
export const captureDurableExecutionTrace = async () => {
  const startedAt = performance.now();
  const events: { step: string; at: string; elapsedMs: number; details?: Record<string, unknown> }[] = [];
  const guests: { closed: Promise<void>; diagnostics: {
    pid: number | null; stderrTail: string; exited: boolean; closed: boolean;
    exitCode: number | null; signal: NodeJS.Signals | null;
  } }[] = [];
  const record = (step: string, details?: Record<string, unknown>) => {
    events.push({ step, at: new Date().toISOString(), elapsedMs: Math.round(performance.now() - startedAt), ...(details ? { details } : {}) });
  };
  const observeFrames = (stream: NodeJS.ReadableStream, label: string) => {
    let pending = "";
    stream.on("data", (chunk: Buffer) => {
      pending += chunk.toString("utf8");
      let newline: number;
      while ((newline = pending.indexOf("\n")) !== -1) {
        const frame = pending.slice(0, newline); pending = pending.slice(newline + 1);
        try { record(`${label} frame`, { type: JSON.parse(frame).type }); }
        catch { record(`${label} unparseable frame`, { bytes: frame.length }); }
      }
      // This fixture has tiny messages. Diagnostics must not retain arbitrary guest data.
      if (pending.length > 4096) { record(`${label} large frame`, { bytes: pending.length }); pending = ""; }
    });
  };
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  vi.mocked(childProcess.spawn).mockImplementation(((...args: Parameters<typeof actual.spawn>) => {
    const python = Array.isArray(args[1]) && args[1].includes("-I") && args[1].includes("-c");
    const label = python ? "CPython guest" : "worker";
    record(`${label} spawn requested`, { command: args[0] });
    if (python) args[1] = [...args[1] as string[], "--fabric-startup-trace"];
    const child = actual.spawn(...args);
    if (python) {
      const diagnostics = { pid: child.pid ?? null, stderrTail: "", exited: false, closed: false,
        exitCode: null as number | null, signal: null as NodeJS.Signals | null };
      child.stderr?.on("data", (chunk: Buffer) => {
        diagnostics.stderrTail = (diagnostics.stderrTail + chunk.toString("utf8")).slice(-4000);
      });
      child.once("exit", (code, signal) => {
        diagnostics.exited = true; diagnostics.exitCode = code; diagnostics.signal = signal;
      });
      const closed = new Promise<void>(resolve => child.once("close", (code, signal) => {
        diagnostics.closed = true; diagnostics.exitCode = code; diagnostics.signal = signal;
        record(`${label} closed`, { code, signal }); resolve();
      }));
      guests.push({ closed, diagnostics });
    }
    record(`${label} spawn returned`, { pid: child.pid });
    child.once("spawn", () => record(`${label} spawned`, { pid: child.pid }));
    child.once("error", error => record(`${label} error`, { error: error.message }));
    child.once("exit", (code, signal) => record(`${label} exited`, { code, signal }));
    if (python && process.platform !== "win32" && child.stdio[3]) observeFrames(child.stdio[3] as Duplex, "CPython pipe");
    return child;
  }) as typeof actual.spawn);

  const listen = net.Server.prototype.listen;
  vi.spyOn(net.Server.prototype, "listen").mockImplementation(function (this: net.Server, ...args) {
    record("IPC listener binding");
    this.once("listening", () => record("IPC listener bound"));
    this.once("connection", socket => { record("IPC guest connected"); observeFrames(socket, "CPython TCP"); });
    return listen.apply(this, args);
  });
  const execute = CPythonRuntime.prototype.execute;
  vi.spyOn(CPythonRuntime.prototype, "execute").mockImplementation(async function (this: CPythonRuntime, code, hostCall, options) {
    record("CPython execute entered", { binary: this.binary, timeoutMs: options.timeoutMs });
    const result = await execute.call(this, code, async (ref, args, signal) => {
      record("CPython host call entered", { ref });
      try {
        const value = await hostCall(ref, args, signal);
        record("CPython host call returned", { ref }); return value;
      } catch (error) { record("CPython host call failed", { ref, error: String(error) }); throw error; }
    }, options);
    record("CPython execute returned", { reason: result.terminationReason, error: result.error });
    return result;
  });
  const ensureHost = ResidencyClient.prototype.ensureHost;
  vi.spyOn(ResidencyClient.prototype, "ensureHost").mockImplementation(async function (this: ResidencyClient) {
    record("resident owner check entered");
    const owner = await ensureHost.call(this); record("resident owner check returned"); return owner;
  });
  const clientSpawn = ResidencyClient.prototype.spawnAgent;
  vi.spyOn(ResidencyClient.prototype, "spawnAgent").mockImplementation(async function (this: ResidencyClient, ...args) {
    record("resident client spawn entered");
    const result = await clientSpawn.apply(this, args);
    record("resident client response and publication complete", { id: result.id }); return result;
  });
  const managerSpawn = AgentManager.prototype.spawn;
  vi.spyOn(AgentManager.prototype, "spawn").mockImplementation(async function (this: AgentManager, request, signal, beforeCommit) {
    record("resident manager preparation entered");
    const handle = await managerSpawn.call(this, request, signal, id => {
      beforeCommit?.(id); record("resident spawn committed", { id });
    });
    record("resident manager spawn returned", { id: handle.id }); return handle;
  });
  const launch = ProcessTransport.prototype.launch;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    record("worker transport launch entered");
    const handle = await launch.call(this, request);
    record("worker transport launch returned", { sessionId: handle.sessionId }); return handle;
  });
  record("trace installed");
  return {
    record, events,
    // The runtime kills on settlement but bounds its own exit wait to 250 ms.
    // Do not remove a Windows guest's cwd until its process and pipes close.
    waitForGuests: () => Promise.all(guests.map(guest => guest.closed)),
    report: (engine: string, operation: string, snapshot: unknown) => console.error("Durable execution trace", JSON.stringify({
      platform: process.platform, engine, operation, events, guests: guests.map(guest => guest.diagnostics), snapshot,
    }, null, 2)),
  };
};
