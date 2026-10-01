import * as childProcess from "node:child_process";
import { EventEmitter } from "node:events";
import { Duplex, PassThrough } from "node:stream";
import fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { classifyPiBashError } from "../src/core/pi-bash-error.js";
import { CPYTHON_CHILD_SOURCE } from "../src/runtime/cpython-child-source.js";
import { CPythonRuntime, LINUX_BWRAP_ISOLATION_ARGS } from "../src/runtime/cpython-runtime.js";
import type { FabricHostCall, FabricSandboxOptions } from "../src/runtime/kernel.js";
import { captureDurableExecutionTrace } from "./helpers/durable-execution-trace.js";

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});
vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return { ...actual, access: vi.fn(actual.access) };
});

const python = childProcess.spawnSync("python3", ["-I", "-B", "-c", "import sys; print(sys.executable)"]);
const hasPython = python.status === 0;
const binary = hasPython ? python.stdout.toString().trim() : "python3";
// smarty-dev#883: the execution deadline also measures interpreter startup,
// which a loaded runner can stretch past any small budget. Tests that do not
// test the deadline therefore get a hang guard, and the tests that do run it
// on a controlled clock.
const HANG_GUARD_MS = 60_000;
const options: FabricSandboxOptions = { timeoutMs: HANG_GUARD_MS, memoryLimitBytes: 256 * 1024 * 1024 };
const roots: string[] = [];
const temp = (): string => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cpython-runtime-"));
  roots.push(cwd);
  return cwd;
};
const echo: FabricHostCall = async (ref, args) => ({ ref, args });
const run = (code: string, call: FabricHostCall = echo, overrides: Partial<FabricSandboxOptions> = {}) =>
  new CPythonRuntime(binary).execute(code, call, { ...options, ...overrides });

afterEach(() => {
  vi.restoreAllMocks();
  vi.mocked(fsPromises.access).mockReset();
  vi.mocked(childProcess.spawn).mockReset();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(!hasPython)("CPythonRuntime", { timeout: HANG_GUARD_MS + 30_000 }, () => {
  it("normalizes CRLF guest stdout and stderr in product logs", async () => {
    const result = await run('import sys\nsys.stdout.write("out one\\r\\nout two\\r\\n")\nsys.stdout.flush()\nsys.stderr.write("err one\\r\\nerr two\\r\\n")\nsys.stderr.flush()\nawait schema.status()\nreturn "delivered"');
    expect(result).toMatchObject({ terminationReason: "completed", value: "delivered" });
    expect(result.logs).toEqual(expect.arrayContaining(["out one", "out two", "err one", "err two"]));
    expect(result.logs.every(line => !line.endsWith("\r"))).toBe(true);
  });

  it("keeps guest startup diagnostics opt-in", async () => {
    expect(await run("return await schema.status()")).toMatchObject({ terminationReason: "completed", logs: [] });
  });

  it.each([["native", "LF"], ["loopback", "LF"], ["native", "CRLF"], ["loopback", "CRLF"]] as const)("captures guest startup timestamps and reaps its exit over %s IPC (%s)", async (transport, newline) => {
    const output = vi.spyOn(console, "error").mockImplementation(() => {});
    const trace = await captureDurableExecutionTrace(newline === "CRLF");
    if (transport === "loopback") vi.stubGlobal("process", new Proxy(process, {
      get(target, key) { return key === "platform" ? "win32" : Reflect.get(target, key); },
    }));
    try {
      const result = await run("await schema.status()\nreturn await schema.status()");
      expect(result.terminationReason, result.error).toBe("completed");
      await trace.waitForGuests();
      expect(output).not.toHaveBeenCalled();
      trace.report("cpython", "diagnostic-probe", result);
      const report = JSON.parse(String(output.mock.calls[0]?.[1]));
      const guest = report.guests[0];
      expect(guest).toMatchObject({ exited: true, closed: true });
      expect(guest.exitCode !== null || guest.signal !== null).toBe(true);
      const stages = guest.stderrTail.split(/\r?\n/).filter(Boolean);
      for (const stage of stages) expect(stage).toMatch(/^\[fabric-cpython-startup\] at=\d+\.\d+ elapsedMs=\d+\.\d+ /);
      expect(stages.map((stage: string) => stage.replace(/^.*elapsedMs=\S+ /, ""))).toEqual([
        "interpreter start", "imports done", "event loop starting", "event loop running",
        expect.stringMatching(transport === "loopback" || process.platform === "win32"
          ? /^connecting to 127\.0\.0\.1:\d+$/ : /^connecting to inherited socket fd 3$/),
        "connected", ...(transport === "loopback" || process.platform === "win32" ? ["IPC hello written"] : []),
        "waiting for execute request", "execute request received", "first request written",
      ]);
      expect(guest.stderrTail).not.toContain("token");
    } finally {
      await trace.waitForGuests();
      vi.unstubAllGlobals();
    }
  });

  it.each(["LF", "CRLF"])("drains delayed loopback startup stderr through guest close (%s)", async (newline) => {
    // Windows delivers TCP and anonymous stderr pipes independently. Model a
    // flushed startup record reaching the host after result/exit, before close.
    vi.stubGlobal("process", new Proxy(process, {
      get(target, key) { return key === "platform" ? "win32" : Reflect.get(target, key); },
    }));
    const marker = "[fabric-cpython-startup] at=1.000000 elapsedMs=1.000 first request written";
    let socket: net.Socket | undefined;
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = resolve; });
    const child = Object.assign(new EventEmitter(), {
      pid: 999_999_999,
      stdout: new PassThrough(), stderr: new PassThrough(), stdio: [],
      kill: vi.fn(() => {
        child.emit("exit", 0, null);
        setTimeout(() => {
          child.stderr.end(marker + (newline === "CRLF" ? "\r\n" : "\n"));
          child.stdout.end();
          child.emit("close", 0, null);
          close();
        }, 30);
        return true;
      }),
    });
    vi.mocked(childProcess.spawn).mockImplementationOnce(((...args: Parameters<typeof childProcess.spawn>) => {
      const env = args[2]!.env!;
      socket = net.createConnection({ host: "127.0.0.1", port: Number(env.FABRIC_IPC_PORT) });
      socket.on("error", () => {});
      socket.once("connect", () => socket!.write(JSON.stringify({ type: "hello", token: env.FABRIC_IPC_TOKEN }) + "\n"));
      let pending = "";
      socket.on("data", chunk => {
        pending += chunk.toString();
        if (!pending.includes("\n")) return;
        const request = JSON.parse(pending.slice(0, pending.indexOf("\n")));
        expect(request.type).toBe("execute");
        socket!.write(JSON.stringify({ type: "result", result: { terminationReason: "completed", value: 1 } }) + "\n");
      });
      return child as unknown as ReturnType<typeof childProcess.spawn>;
    }) as typeof childProcess.spawn);
    try {
      const result = await run("return 1");
      expect(result).toMatchObject({ terminationReason: "completed", value: 1 });
      expect(result.logs).toEqual([marker]);
      expect(child.kill).toHaveBeenCalledExactlyOnceWith("SIGKILL");
    } finally {
      await closed;
      if (socket && !socket.closed) {
        const socketClosed = new Promise<void>(resolve => socket!.once("close", () => resolve()));
        socket.destroy(); await socketClosed;
      }
      vi.unstubAllGlobals();
    }
  });

  it("waits for guest close before cwd removal and retains only the stderr tail with exit status", async () => {
    const output = vi.spyOn(console, "error").mockImplementation(() => {});
    const trace = await captureDurableExecutionTrace();
    const cwd = temp();
    const child = childProcess.spawn(binary, ["-I", "-B", "-c",
      'import sys, time; sys.stderr.write("x" * 5000 + "\\nlast startup marker\\n"); sys.stderr.flush(); time.sleep(0.4); sys.exit(7)',
    ], { cwd, stdio: ["ignore", "pipe", "pipe"] });
    try {
      let reaped = false;
      const reaping = trace.waitForGuests().then(() => { reaped = true; });
      await new Promise<void>(resolve => child.once("spawn", () => resolve()));
      expect(reaped).toBe(false);
      await reaping;
      expect(reaped).toBe(true);
      fs.rmSync(cwd, { recursive: true, force: true });
      expect(output).not.toHaveBeenCalled();
      trace.report("cpython", "failed-startup-probe", {});
      const report = JSON.parse(String(output.mock.calls[0]?.[1]));
      expect(report.guests[0]).toMatchObject({ exited: true, closed: true, exitCode: 7, signal: null });
      expect(report.guests[0].stderrTail).toHaveLength(4000);
      expect(report.guests[0].stderrTail).toContain("last startup marker");
    } finally {
      child.kill("SIGKILL");
      await trace.waitForGuests();
    }
  });

  it("routes the records primitive through the same host bridge", async () => {
    expect(await run('return await records.read(after=3, limit=2)')).toMatchObject({
      terminationReason: "completed", value: { ref: "records.read", args: { after: 3, limit: 2 } },
    });
  });

  it("commits loopback IPC receipts before guest continuation and preserves CRLF/Unicode values", async () => {
    // Exercise Windows' real TCP bridge even on POSIX; an inherited fd 3 alone
    // cannot cover its token handshake or response delivery path.
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const receipt = vi.fn();
    const firstArgs = { text: "first\r\nUnicode π" };
    try {
      const result = await run('value = await schema.status(text="first\\r\\nUnicode π")\nreturn await memory.sessions(value=value)', async (ref, args) => {
        if (ref === "schema.status") {
          expect(args).toEqual(firstArgs);
          return args.text;
        }
        // This call is issued only after the guest decoded the first response.
        expect(receipt).toHaveBeenCalledExactlyOnceWith(firstArgs);
        return args.value;
      }, { onHostResultDelivered: receipt });
      expect(result).toMatchObject({ terminationReason: "completed", value: firstArgs.text });
      expect(receipt).toHaveBeenCalledTimes(2);
    } finally { Object.defineProperty(process, "platform", platform); }
  });

  it("does not commit a loopback IPC receipt when encoding crosses the deadline", async () => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const startedAt = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(startedAt);
    const receipt = vi.fn();
    const encode = vi.fn(() => { clock.mockReturnValue(startedAt + options.timeoutMs); return "late"; });
    const host = vi.fn(async () => ({ get text() { return encode(); } }));
    try {
      const result = await run("return await schema.status()", host, { onHostResultDelivered: receipt });
      expect(host).toHaveBeenCalledOnce();
      expect(encode).toHaveBeenCalledOnce();
      expect(result.terminationReason).toBe("timed_out");
      expect(receipt).not.toHaveBeenCalled();
    } finally { clock.mockRestore(); Object.defineProperty(process, "platform", platform); }
  });

  it("routes the cache primitive through the same host bridge", async () => {
    expect(await run('return await cache.status(target="self")')).toMatchObject({
      terminationReason: "completed", value: { ref: "cache.status", args: { target: "self" } },
    });
  });

  it("supports async bodies, native stdlib, dictionary results and fresh invocations", async () => {
    const runtime = new CPythonRuntime(binary);
    const result = await runtime.execute('import json\nlocal = 12\nreturn {"items": json.loads("[1,2]"), "call": await schema.status()}', echo, options);
    expect(result).toMatchObject({ terminationReason: "completed", value: { items: [1, 2], call: { ref: "schema.status", args: {} } } });
    expect(await runtime.execute('return "local" in globals()', echo, options)).toMatchObject({ value: false });
  });

  it("preserves multiline literals and exact shared payload keys", async () => {
    const result = await run('text = """first\n  second\nthird"""\nreturn [text, π.body, payloads["body"], π is payloads, payloads["not-an-id"]]', echo, { strings: { body: "quoted \"Unicode π\"\nline", "not-an-id": "yes" } });
    expect(result.terminationReason, result.error).toBe("completed");
    expect(result.value).toEqual(["first\n  second\nthird", 'quoted "Unicode π"\nline', 'quoted "Unicode π"\nline', true, "yes"]);
  });

  it.each(['π.missing', 'payloads["missing"]'])("preflights missing %s before host effects", async (accessor) => {
    const host = vi.fn(echo);
    const result = await run(`await schema.status()\nreturn ${accessor}`, host);
    expect(result.error).toContain("Pre-execution check: missing payloads missing");
    expect(host).not.toHaveBeenCalled();
  });

  it("does not preflight examples inside comments and strings", async () => {
    expect(await run('# π.missing\nreturn "payloads[\'missing\']"')).toMatchObject({ terminationReason: "completed", value: "payloads['missing']" });
  });

  it("routes discovery, generic, MCP and core positional/keyword calls", async () => {
    const result = await run('return await asyncio.gather(tools.search("example"), tools.call(ref="demo.echo", args={"n": 1}), mcp.server.tool(n=2), pi.read("a", offset=2), pi.grep("needle", "src", 3), pi.read("a", {"limit": 4}))');
    expect(result.value).toEqual([
      { ref: "fabric.$search", args: { query: "example" } },
      { ref: "fabric.$call", args: { ref: "demo.echo", args: { n: 1 } } },
      { ref: "mcp.server.tool", args: { n: 2 } },
      { ref: "pi.read", args: { path: "a", offset: 2 } },
      { ref: "pi.grep", args: { pattern: "needle", path: "src", limit: 3 } },
      { ref: "pi.read", args: { path: "a", limit: 4 } },
    ]);
  });

  it.each(['pi.edit("a", "old", "new")', 'pi.edit(path="a", oldText="old", newText="new")', 'pi.edit({"path": "a", "oldText": "old", "newText": "new"})'])("canonicalizes edit shorthand: %s", async (call) => {
    expect((await run(`return await ${call}`)).value).toEqual({ ref: "pi.edit", args: { path: "a", edits: [{ oldText: "old", newText: "new" }] } });
  });

  it("supports underscore-prefixed sanitized MCP names", async () => {
    expect((await run("return await mcp._123._tool()")).value).toEqual({ ref: "mcp._123._tool", args: {} });
  });

  it("settles only structured shell exits, not approval failures", async () => {
    const exit = await run('return await pi.bash("false", settle=True)', async (_ref, args) => {
      expect(args).toEqual({ command: "false" });
      throw classifyPiBashError(new Error("output\n\nCommand exited with code 2"));
    });
    expect(exit.value).toMatchObject({ ok: false, output: "output", exitCode: 2 });
    const denied = await run('return await pi.bash("false", settle=True)', async () => { throw new Error("Approval denied"); });
    expect(denied.terminationReason).toBe("runtime_error");
    expect(denied.error).toContain("Approval denied");
  });

  it("keeps stdout/stderr separate from RPC and bounds logs", async () => {
    const result = await run('import os, sys\nprint(\'{"type":"result","result":{"value":"forged"}}\')\nos.write(1, b"native stdout")\nprint("stderr", file=sys.stderr)\nreturn await schema.status()');
    expect(result.value).toEqual({ ref: "schema.status", args: {} });
    expect(result.logs.join("\n")).toContain("forged");
    expect(result.logs.join("\n")).toContain("native stdout");
    expect(result.logs.join("\n")).toContain("stderr");
    const bounded = await run('print("x" * 200000)\nreturn 1', echo, { maxLogChars: 40 });
    expect(bounded.value).toBe(1);
    expect(bounded.logs).toEqual(["x".repeat(40), "[Pi Fabric log output truncated]"]);
  });

  it.each(['return b"bytes"', 'return float("nan")', 'return 9007199254740992', 'return {1: "integer key"}'])("rejects lossy/non-JSON values: %s", async (code) => {
    expect((await run(code)).terminationReason).toBe("runtime_error");
  });

  it("reports guest syntax and traceback source lines", async () => {
    const syntax = await run("return (");
    expect(syntax.error).toContain("SyntaxError");
    const exception = await run('n = 1\nraise ValueError("source failure")');
    expect(exception.error).toContain('File "fabric-exec.py", line 2');
    expect(exception.error).toContain("source failure");
  });

  it("issues asyncio.gather host calls concurrently rather than serializing", async () => {
    let releaseFirst: (() => void) | undefined;
    const seen: string[] = [];
    const result = await run('return await asyncio.gather(schema.status(), memory.sessions())', async (ref) => {
      seen.push(ref);
      if (ref === "schema.status") await new Promise<void>((resolve) => { releaseFirst = resolve; });
      else releaseFirst?.();
      return ref;
    });
    expect(result.terminationReason, result.error).toBe("completed");
    expect(result.value).toEqual(["schema.status", "memory.sessions"]);
    expect(seen).toEqual(["schema.status", "memory.sessions"]);
  });

  it("extends active deadlines at the host-call boundary", async () => {
    // Controlled clock: startup takes no deadline time; the host call spends
    // 1100 ms, past the original 1000 ms deadline but inside the 2000 ms floor.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    try {
      const result = await run('return await tools.call(ref="slow.wait", args={})', async () => {
        vi.advanceTimersByTime(1100);
        return "done";
      }, { timeoutMs: 1000, minimumTimeoutMsForHostCall: () => 2000 });
      expect(result).toMatchObject({ terminationReason: "completed", value: "done" });
    } finally {
      vi.useRealTimers();
    }
  });

  it("kills synchronous infinite loops and preserves pre-timeout logs", async () => {
    // smarty-dev#883: a wall deadline also measured interpreter startup, so a
    // loaded runner timed out before the guest printed. The deadline runs on a
    // controlled clock, advanced only once the guest has printed and entered its loop.
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    // Watch the guest's stdout from spawn on, so no early chunk is missed.
    let started!: () => void;
    const printed = new Promise<void>((resolve) => { started = resolve; });
    vi.mocked(childProcess.spawn).mockImplementationOnce(((...args: Parameters<typeof actual.spawn>) => {
      const child = actual.spawn(...args);
      let output = "";
      child.stdout?.on("data", (chunk: Buffer) => { output += chunk; if (output.includes("started")) started(); });
      return child;
    }) as typeof actual.spawn);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    try {
      const pending = run('print("started", flush=True)\nwhile True:\n    pass', echo, { timeoutMs: 1500 });
      // Once "started" arrives, the guest has returned from print and has no
      // await point left before its loop. A run that ends first (a startup
      // error) settles pending, so the race shows that error instead of hanging.
      await Promise.race([printed, pending]);
      vi.advanceTimersByTime(1500);
      const result = await pending;
      expect(result.terminationReason).toBe("timed_out");
      expect(result.logs).toContain("started");
    } finally {
      vi.useRealTimers();
    }
  });

  it("aborts outstanding host calls and rejects pre-aborted invocations without spawn", async () => {
    const controller = new AbortController();
    let hostSignal: AbortSignal | undefined;
    const result = await run('return await schema.status()', async (_ref, _args, signal) => {
      hostSignal = signal;
      controller.abort();
      return new Promise(() => undefined);
    }, { signal: controller.signal });
    expect(result.terminationReason).toBe("aborted");
    expect(hostSignal?.aborted).toBe(true);
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    expect((await run("return 1", echo, { signal: controller.signal })).terminationReason).toBe("aborted");
    expect(spawn).not.toHaveBeenCalled();
  });

  it("does not spawn after cancellation during interpreter resolution", async () => {
    const controller = new AbortController();
    const { access } = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
    vi.mocked(fsPromises.access).mockImplementationOnce(async (file, mode) => {
      await access(file, mode);
      controller.abort();
    });
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    expect((await run("return 1", echo, { signal: controller.signal })).terminationReason).toBe("aborted");
    expect(spawn).not.toHaveBeenCalled();
  });

  it("does not spawn after cancellation during Windows IPC listener binding", async () => {
    const controller = new AbortController();
    const originalListen = net.Server.prototype.listen;
    let server: net.Server | undefined;
    // Exercise the real asynchronous Windows pre-spawn boundary on every OS.
    vi.stubGlobal("process", new Proxy(process, {
      get(target, key) { return key === "platform" ? "win32" : Reflect.get(target, key); },
    }));
    vi.spyOn(net.Server.prototype, "listen").mockImplementation(function (this: net.Server, ...args) {
      server = this;
      this.once("listening", () => controller.abort());
      return originalListen.apply(this, args);
    });
    const spawn = vi.mocked(childProcess.spawn); spawn.mockClear();
    try {
      expect(await run("return 1", echo, { signal: controller.signal })).toMatchObject({ terminationReason: "aborted" });
      expect(spawn.mock.calls.length).toBe(0);
      await new Promise<void>((done) => setImmediate(done));
      expect(server?.listening).toBe(false);
    } finally {
      if (server?.listening) await new Promise<void>((done) => server!.close(() => done()));
      vi.unstubAllGlobals();
    }
  });

  it.skipIf(process.platform === "win32")("kills same-group subprocesses when cancelled", async () => {
    const controller = new AbortController();
    let pid: number | undefined;
    try {
      const result = await run('import subprocess, sys\nchild = subprocess.Popen([sys.executable, "-I", "-B", "-c", "import time; time.sleep(30)"])\nawait schema.status(pid=child.pid)', async (_ref, args) => {
        pid = Number(args.pid);
        controller.abort();
      }, { signal: controller.signal });
      expect(result.terminationReason).toBe("aborted");
      expect(Number.isSafeInteger(pid)).toBe(true);
      await expect.poll(() => {
        const status = childProcess.spawnSync("ps", ["-o", "stat=", "-p", String(pid)]);
        return status.stdout?.toString().trim() ?? "";
      }, { timeout: 2000 }).toMatch(/^(?:Z.*)?$/);
    } finally {
      if (pid && Number.isSafeInteger(pid)) {
        try { process.kill(pid, "SIGKILL"); } catch { /* Already reaped. */ }
      }
    }
  });

  it("settles issued background host calls before completing", async () => {
    let completed = false;
    const result = await run('task = asyncio.create_task(schema.status())\nawait asyncio.sleep(0.03)\nreturn 1', async () => {
      await new Promise((resolve) => setTimeout(resolve, 100));
      completed = true;
    });
    expect(result.terminationReason, result.error).toBe("completed");
    expect(result.value).toBe(1);
    expect(completed).toBe(true);
  });

  it.skipIf(process.platform === "win32")("does not write late host replies after a terminal guest frame", async () => {
    const writes: string[] = [];
    const channel = new Duplex({
      read() {},
      write(chunk, _encoding, callback) {
        const message = JSON.parse(chunk.toString());
        writes.push(message.type);
        if (message.type === "response") {
          callback(new Error("EPIPE: guest already exited"));
          return;
        }
        callback();
        queueMicrotask(() => channel.push([
          JSON.stringify({ type: "call", id: 1, ref: "schema.status", args: {} }),
          JSON.stringify({ type: "result", result: { terminationReason: "completed", value: 1 } }),
          "",
        ].join("\n")));
      },
    });
    const child = Object.assign(new EventEmitter(), {
      pid: undefined,
      stdout: new PassThrough(), stderr: new PassThrough(),
      stdio: [null, null, null, channel], kill: vi.fn(),
    });
    vi.mocked(childProcess.spawn).mockReturnValue(child as unknown as ReturnType<typeof childProcess.spawn>);
    let completed = false;
    const result = await run("return 1", async () => {
      await new Promise(resolve => setImmediate(resolve));
      completed = true;
      return "late result";
    });
    expect(result).toMatchObject({ terminationReason: "completed", value: 1 });
    expect(completed).toBe(true);
    expect(writes).toEqual(["execute"]);
  });

  // Security review on #146: a broken pipe must end host authority at once,
  // even when the child's "close" is held back (a descendant keeps stdout open).
  it.skipIf(process.platform === "win32")("aborts issued host calls at a pipe error while the child's close is withheld", async () => {
    const channel = new Duplex({
      read() {},
      write(_chunk, _encoding, callback) {
        callback();
        queueMicrotask(() => channel.push(`${JSON.stringify({ type: "call", id: 1, ref: "schema.status", args: {} })}\n`));
      },
    });
    const child = Object.assign(new EventEmitter(), {
      pid: undefined,
      stdout: new PassThrough(), stderr: new PassThrough(),
      stdio: [null, null, null, channel], kill: vi.fn(),
    });
    vi.mocked(childProcess.spawn).mockReturnValue(child as unknown as ReturnType<typeof childProcess.spawn>);
    const calls: string[] = [];
    let abortedAtError: boolean | undefined;
    const started = Date.now();
    const result = await run("return 1", (ref, _args, signal) => {
      calls.push(ref);
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        if (calls.length === 1) queueMicrotask(() => {
          channel.emit("error", new Error("read ECONNRESET"));
          abortedAtError = signal?.aborted;
          // A late guest frame after the break is not admitted.
          channel.emit("data", Buffer.from(`${JSON.stringify({ type: "call", id: 2, ref: "memory.sessions", args: {} })}\n`));
        });
      });
    }, { timeoutMs: 60_000 });
    expect(abortedAtError).toBe(true);
    expect(calls).toEqual(["schema.status"]);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    expect(result).toMatchObject({ terminationReason: "runtime_error" });
    expect(result.error).toContain("CPython IPC failed: read ECONNRESET");
    expect(result.error).toContain("did not report its exit");
    expect(Date.now() - started).toBeLessThan(30_000);
  });

  // Astra round 2: the execution deadline may expire during the diagnosis wait;
  // it must settle the recorded pipe failure, not replace it with a timeout.
  it.skipIf(process.platform === "win32")("keeps the pipe failure when the deadline expires during its diagnosis", async () => {
    const channel = new Duplex({
      read() {},
      write(_chunk, _encoding, callback) {
        callback();
        queueMicrotask(() => channel.push(`${JSON.stringify({ type: "call", id: 1, ref: "schema.status", args: {} })}\n`));
      },
    });
    const child = Object.assign(new EventEmitter(), {
      pid: undefined,
      stdout: new PassThrough(), stderr: new PassThrough(),
      stdio: [null, null, null, channel], kill: vi.fn(),
    });
    vi.mocked(childProcess.spawn).mockReturnValue(child as unknown as ReturnType<typeof childProcess.spawn>);
    let signalBroken!: () => void;
    const broken = new Promise<void>((resolve) => { signalBroken = resolve; });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    try {
      const pending = run("return 1", (_ref, _args, signal) => new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        // The pipe breaks with 500 ms left, less than the 2 s diagnosis bound.
        queueMicrotask(() => {
          vi.advanceTimersByTime(500);
          channel.emit("error", new Error("read ECONNRESET"));
          signalBroken();
        });
      }), { timeoutMs: 1000 });
      await broken;
      vi.advanceTimersByTime(3000);
      const result = await pending;
      expect(result).toMatchObject({ terminationReason: "runtime_error" });
      expect(result.error).toContain("CPython IPC failed: read ECONNRESET");
    } finally {
      vi.useRealTimers();
    }
  });

  it("bounds non-cooperative host calls after guest failure", async () => {
    // smarty-dev#883: time from the sibling failure, not from before
    // interpreter startup. Without the settle bound, the never-settling call
    // would hold the run to its hang-guard deadline (timed_out).
    let failedAt = 0;
    const result = await run('await asyncio.gather(schema.status(), memory.sessions())', async (ref) => {
      if (ref === "schema.status") return new Promise(() => undefined);
      failedAt = Date.now();
      throw new Error("sibling failed");
    });
    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toContain("sibling failed");
    expect(Date.now() - failedAt).toBeLessThan(10_000);
  });

  it("rejects malformed/oversized IPC before calling the host", async () => {
    const host = vi.fn(echo);
    if (process.platform !== "win32") {
      // Windows carries IPC over loopback TCP, so there is no writable fd 3.
      const malformed = await run('import os\nos.write(3, bytes([123, 10]))\nawait asyncio.sleep(1)', host);
      expect(malformed.error).toContain("Invalid CPython IPC");
    }
    const oversized = await run('return "x" * (17 * 1024 * 1024)', host);
    expect(oversized.error).toContain("16 MiB");
    expect(host).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")("resolves relative PATH entries against invocation cwd", async () => {
    const cwd = temp();
    fs.mkdirSync(path.join(cwd, "bin"));
    fs.symlinkSync(binary, path.join(cwd, "bin", "python-fixture"));
    vi.stubEnv("PATH", "bin");
    try {
      const result = await new CPythonRuntime("python-fixture").execute('import os\nreturn os.getcwd()', echo, { ...options, cwd });
      expect(result.terminationReason, result.error).toBe("completed");
      expect(result.value).toBe(fs.realpathSync(cwd));
    } finally { vi.unstubAllEnvs(); }
  });

  it("reports missing configured interpreters without any fallback", async () => {
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    const result = await new CPythonRuntime(path.join(temp(), "missing-python")).execute("return 1", echo, options);
    expect(result.error).toContain("executor.cpython.binary");
    expect(spawn).not.toHaveBeenCalled();
  });

  // smarty-dev#883: bwrap without user namespaces exits before it reads the
  // execute frame; the reset pipe raced "close" and hid the sandbox diagnosis.
  it.skipIf(process.platform === "win32")("reports a child that exits at startup, not the pipe reset it leaves", async () => {
    const dying = path.join(temp(), "dying-python");
    fs.writeFileSync(dying, "#!/bin/sh\necho 'bwrap: setting up uid map: Permission denied' >&2\nexit 1\n", { mode: 0o755 });
    const result = await new CPythonRuntime(dying).execute(`return "${"x".repeat(256 * 1024)}"`, echo, options);
    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toMatch(/process exited before returning a result \(1\)/);
    expect(result.error).toContain("setting up uid map");
  });

  it.each(['sys.version_info = (3, 9, 0)', 'sys.implementation.name = "pypy"'])("rejects unsupported interpreter identity before imports/RPC: %s", (override) => {
    const result = childProcess.spawnSync(binary, ["-I", "-B", "-c", `import sys\n${override}\nexec(${JSON.stringify(CPYTHON_CHILD_SOURCE)})`]);
    expect(result.status).toBe(1);
    expect(result.stderr.toString()).toContain("CPython 3.10 or newer");
  });
});

const supportedSandbox = process.platform === "darwin" || process.platform === "linux";
const installedSandbox = process.platform === "darwin" ? fs.existsSync("/usr/bin/sandbox-exec") : fs.existsSync("/usr/bin/bwrap") || fs.existsSync("/bin/bwrap");

// An installed bwrap can still be unusable: a kernel that forbids the sandbox's
// namespaces exits it before the child connects, and the runtime fails closed
// there instead of running unsandboxed. The boundary tests therefore assert the
// real sandbox only where the runtime's own isolation flags actually start.
const linuxSandboxStarts = (() => {
  if (process.platform !== "linux" || !installedSandbox) return false;
  const bwrap = fs.existsSync("/usr/bin/bwrap") ? "/usr/bin/bwrap" : "/bin/bwrap";
  try {
    return childProcess.spawnSync(bwrap, [...LINUX_BWRAP_ISOLATION_ARGS, "--", "/bin/true"], { stdio: "ignore", timeout: 10_000 }).status === 0;
  } catch {
    return false;
  }
})();
const usableSandbox = installedSandbox && (process.platform !== "linux" || linuxSandboxStarts);

describe.skipIf(!hasPython || !supportedSandbox)("CPython OS sandbox", { timeout: HANG_GUARD_MS + 30_000 }, () => {
  it.skipIf(process.platform !== "linux" || !usableSandbox)("starts the real Linux sandbox and carries the result over inherited IPC", async () => {
    const result = await new CPythonRuntime(binary, true).execute("return 6 * 7", echo, options);
    expect(result.terminationReason, `${result.error}\n${result.logs.join("\n")}`).toBe("completed");
    expect(result.value).toBe(42);
  });

  it("fails closed when the OS sandbox binary is missing", async () => {
    const { access } = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
    vi.mocked(fsPromises.access).mockImplementation(async (name, mode) => {
      if (["/usr/bin/sandbox-exec", "/usr/bin/bwrap", "/bin/bwrap"].includes(String(name))) throw new Error("ENOENT");
      return access(name, mode);
    });
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    const result = await new CPythonRuntime(binary, true).execute("return 1", echo, options);
    expect(result.error).toMatch(/requires.*(?:sandbox-exec|bubblewrap)/);
    expect(result.error).toContain("no unsandboxed fallback");
    expect(spawn).not.toHaveBeenCalled();
  });

  it.skipIf(!usableSandbox)("denies native and subprocess writes/network while host schema calls still execute", async () => {
    const cwd = temp();
    let connections = 0;
    const server = net.createServer((socket) => { connections++; socket.destroy(); });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as net.AddressInfo).port;
    try {
      const result = await new CPythonRuntime(binary, true).execute(`
import os, socket, subprocess, sys
blocked = []
try:
    open("native.txt", "w").write("escape")
    blocked.append(False)
except OSError:
    blocked.append(True)
try:
    with socket.socket() as client:
        client.settimeout(1)
        client.connect(("127.0.0.1", ${port}))
    blocked.append(False)
except OSError:
    blocked.append(True)
child = subprocess.run([sys.executable, "-I", "-B", "-c", "open('child.txt', 'w').write('escape')"], capture_output=True)
blocked.append(child.returncode != 0)
result = await schema.commit(hypothesisId="probe", certificate="probe", operations=[])
return {"blocked": blocked, "host": result}
`, async (ref) => {
        expect(ref).toBe("schema.commit");
        fs.writeFileSync(path.join(cwd, "host.txt"), "host effect");
        return { outcome: "committed" };
      }, { ...options, cwd });
      // Deliberately no availability-success branch: this proves an actual sandbox run.
      expect(result.terminationReason, result.error).toBe("completed");
      expect(result.value).toEqual({ blocked: [true, true, true], host: { outcome: "committed" } });
      expect(connections).toBe(0);
      expect(fs.existsSync(path.join(cwd, "native.txt"))).toBe(false);
      expect(fs.existsSync(path.join(cwd, "child.txt"))).toBe(false);
      expect(fs.readFileSync(path.join(cwd, "host.txt"), "utf8")).toBe("host effect");
    } finally { await new Promise<void>((resolve) => server.close(() => resolve())); }
  });

  it.skipIf(process.platform !== "darwin" || !installedSandbox)("denies signaling and ptrace against a sacrificial host process", async () => {
    // Computed so knip does not treat the binary path as a resolvable import.
    const target = childProcess.spawn(path.join("/bin", "sleep"), ["30"]);
    try {
      const result = await new CPythonRuntime(binary, true).execute(`
import ctypes, os, signal
try:
    os.kill(${target.pid}, signal.SIGUSR1)
    denied_signal = False
except PermissionError:
    denied_signal = True
libc = ctypes.CDLL(None, use_errno=True)
attached = libc.ptrace(10, ${target.pid}, 0, 0)
return {"signalDenied": denied_signal, "ptraceResult": attached, "errno": ctypes.get_errno()}
`, echo, options);
      expect(result.terminationReason, result.error).toBe("completed");
      expect(result.value).toEqual({ signalDenied: true, ptraceResult: -1, errno: 1 });
      expect(target.exitCode).toBeNull();
      expect(target.signalCode).toBeNull();
    } finally { target.kill("SIGKILL"); }
  });

  it.skipIf(process.platform !== "linux" || !usableSandbox)("denies pathname Unix sockets, including a socketpair/sendto bypass", async () => {
    const cwd = temp();
    const address = path.join(cwd, "host.sock");
    let connections = 0;
    const server = net.createServer((socket) => { connections++; socket.destroy(); });
    await new Promise<void>((resolve) => server.listen(address, resolve));
    try {
      const result = await new CPythonRuntime(binary, true).execute(`
import socket
blocked = []
try:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(${JSON.stringify(address)})
    blocked.append(False)
except OSError:
    blocked.append(True)
a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
try:
    a.sendto(b"escape", ${JSON.stringify(address)})
    blocked.append(False)
except PermissionError:
    blocked.append(True)
return blocked
`, echo, { ...options, cwd });
      expect(result.terminationReason, result.error).toBe("completed");
      expect(result.value).toEqual([true, true]);
      expect(connections).toBe(0);
    } finally { await new Promise<void>((resolve) => server.close(() => resolve())); }
  });
});
