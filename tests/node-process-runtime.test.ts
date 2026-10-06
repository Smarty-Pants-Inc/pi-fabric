import * as childProcess from "node:child_process";
import { execFileSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { BunProcessRuntime, NodeProcessRuntime } from "../src/runtime/node-process-runtime.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});
vi.mock("../src/agents/transports/process-utils.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/agents/transports/process-utils.js")>();
  return { ...actual, resolveScriptRuntime: vi.fn(actual.resolveScriptRuntime) };
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.mocked(childProcess.spawn).mockReset();
  vi.mocked(processUtils.resolveScriptRuntime).mockReset();
});

const options = {
  timeoutMs: 5_000,
  memoryLimitBytes: 128 * 1024 * 1024,
};

const hasBun = (() => {
  try {
    execFileSync("bun", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

// An OS process-list check complements the spawn spy: the cancelled startup
// must not leave a real idle guest behind. Inspect only this test worker's children.
const guestChildren = (): number[] => {
  if (process.platform === "win32") {
    const text = execFileSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command",
      `Get-CimInstance Win32_Process -Filter "ParentProcessId = ${process.pid}" | Where-Object { $_.Name -in @('node.exe', 'bun.exe') } | ForEach-Object { $_.ProcessId }`,
    ], { encoding: "utf8" });
    return text.trim().split(/\s+/).filter(Boolean).map(Number);
  }
  return execFileSync("ps", ["-eo", "pid=,ppid=,comm="], { encoding: "utf8" }).split("\n").flatMap((line) => {
    const [pid, parent, command] = line.trim().split(/\s+/);
    return Number(parent) === process.pid && /(?:^|[\/])(node|bun)$/.test(command ?? "") ? [Number(pid)] : [];
  });
};
const reap = async (child: childProcess.ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Guest ${child.pid} did not exit after SIGKILL`)), 2_000);
    child.once("exit", () => { clearTimeout(timer); resolve(); });
    child.kill("SIGKILL");
  });
};

describe("process runtime startup ownership", () => {
  it.skipIf(!hasBun)("spawns no Bun guest when cancelled during slowed runtime resolution", async () => {
    const actualProcess = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    const actualUtils = await vi.importActual<typeof import("../src/agents/transports/process-utils.js")>("../src/agents/transports/process-utils.js");
    const children: childProcess.ChildProcess[] = [];
    vi.mocked(childProcess.spawn).mockImplementation(((...args: Parameters<typeof actualProcess.spawn>) => {
      const child = actualProcess.spawn(...args); children.push(child); return child;
    }) as typeof actualProcess.spawn);
    let entered!: () => void; let release!: () => void;
    const resolving = new Promise<void>((done) => { entered = done; });
    const held = new Promise<void>((done) => { release = done; });
    vi.mocked(processUtils.resolveScriptRuntime).mockImplementationOnce(async (args) => {
      const binary = await actualUtils.resolveScriptRuntime(args);
      entered(); await held; return binary;
    });
    const before = new Set(guestChildren());
    const controller = new AbortController();
    const pending = new BunProcessRuntime().execute("return 1;", async () => undefined, { ...options, signal: controller.signal });
    try {
      await resolving;
      expect(vi.mocked(childProcess.spawn).mock.calls.length).toBe(0);
      controller.abort(); release();
      expect(await pending).toMatchObject({ terminationReason: "aborted", error: "Execution cancelled" });
      // Give any incorrectly spawned native guest time to appear in the process list.
      await new Promise<void>((done) => setImmediate(done));
      const alivePids = children.filter(child => child.pid && actualUtils.processIsAlive(child.pid)).map(child => child.pid);
      const newGuestPids = guestChildren().filter(pid => !before.has(pid));
      expect({ spawnCount: vi.mocked(childProcess.spawn).mock.calls.length, alivePids, newGuestPids })
        .toEqual({ spawnCount: 0, alivePids: [], newGuestPids: [] });
    } finally {
      controller.abort(); release(); await pending;
      await Promise.all(children.map(reap));
    }
  });

  it.each(["node", "bun"] as const)("does not acquire a %s child before fallible guest setup", async (engine) => {
    if (engine === "bun" && !hasBun) return;
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    const children: childProcess.ChildProcess[] = [];
    vi.mocked(childProcess.spawn).mockImplementation(((...args: Parameters<typeof actual.spawn>) => {
      const child = actual.spawn(...args); children.push(child); return child;
    }) as typeof actual.spawn);
    try {
      await expect(new NodeProcessRuntime(engine).execute("return 1;", async () => undefined, {
        ...options, get transpiledCode(): string { throw new Error("injected guest setup failure"); },
      })).rejects.toThrow("injected guest setup failure");
      expect(vi.mocked(childProcess.spawn).mock.calls.length).toBe(0);
    } finally { await Promise.all(children.map(reap)); }
  });
});

describe("NodeProcessRuntime", () => {
  it("rejects unencodable host results without committing their observation", async () => {
    let delivered = 0;
    const result = await new NodeProcessRuntime().execute("return tools.providers();", async () => ({ value: 1n }), {
      ...options, onHostResultDelivered() { delivered++; },
    });
    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toMatch(/BigInt|serialize/i);
    expect(delivered).toBe(0);
  });
  it("routes the records primitive through the shared guest setup", async () => {
    const result = await new NodeProcessRuntime().execute('return records.read({after:3});',
      async (ref, args) => ({ref, args}), options);
    expect(result).toMatchObject({terminationReason:"completed", value:{ref:"records.read", args:{after:3}}});
  });

  it("routes the thinking primitive through the shared guest setup", async () => {
    const result = await new NodeProcessRuntime().execute('return thinking.status();',
      async (ref, args) => ({ref, args}), options);
    expect(result).toMatchObject({terminationReason:"completed", value:{ref:"thinking.status", args:{}}});
  });

  it("routes the cache primitive through the shared guest setup", async () => {
    const result = await new NodeProcessRuntime().execute('return cache.status({target:"self"});',
      async (ref, args) => ({ref, args}), options);
    expect(result).toMatchObject({terminationReason:"completed", value:{ref:"cache.status", args:{target:"self"}}});
  });

  it("runs guest code in a disposable process and bridges host calls", async () => {
    const result = await new NodeProcessRuntime().execute(
      `
const models = await tools.models();
print("models", models.length);
return { models, process: typeof process, require: typeof require };
`,
      async (ref) => ref === "fabric.$models" ? [{ id: "large-model" }] : undefined,
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.logs).toEqual(["models 1"]);
    expect(result.value).toEqual({
      models: [{ id: "large-model" }],
      process: "undefined",
      require: "undefined",
    });
  });
  it.each([Number.NaN, Number.POSITIVE_INFINITY, 0, -100])("rejects non-positive timeout %s without spawning", async (timeoutMs) => {
    const result = await new NodeProcessRuntime().execute(
      "return 1;",
      async () => undefined,
      { ...options, timeoutMs },
    );

    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toBe("Process timeout must be positive");
  });


  it("leaves __bun undefined on the node runtime", async () => {
    const result = await new NodeProcessRuntime().execute(
      "return typeof __bun;",
      async () => undefined,
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toBe("undefined");
  });

  it("normalizes the string shorthand for tools.search", async () => {
    const result = await new NodeProcessRuntime().execute(
      'return tools.search("fovea");',
      async (ref, args) => {
        expect(ref).toBe("fabric.$search");
        expect(args).toEqual({ query: "fovea" });
        return [{ ref: "extensions.fovea_focus" }];
      },
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toEqual([{ ref: "extensions.fovea_focus" }]);
  });


  it("provides memory.walk callbacks in the process runtime", async () => {
    let calls = 0;
    const result = await new NodeProcessRuntime().execute(
      `
const texts = [];
const walk = await memory.walk({ session: "session", indices: [4] }, async (entry) => {
  await Promise.resolve();
  texts.push(entry.text + ":" + entry.parentId);
});
return { texts, walk };
`,
      async (ref) => {
        expect(ref).toBe("memory.expand");
        calls += 1;
        return {
          entries: [{
            index: 4,
            entryId: "e4",
            parentId: "e3",
            type: "message",
            role: "user",
            timestamp: 4,
            isError: false,
            text: "remember",
            textRange: { start: 0, end: 8, total: 8, complete: true },
          }],
          next: null,
        };
      },
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toEqual({
      texts: ["remember:e3"],
      walk: { visited: 1, stopped: false },
    });
    expect(calls).toBe(1);
  });

  it("extends the active deadline for a long host call", async () => {
    let admitted = false;
    const result = await executeAfterAdmission(signal => new NodeProcessRuntime().execute(
      'await tools.call({ ref: "pi.bash", args: { timeout: 1 } }); return "ok";',
      async () => {
        admitted = true;
        await new Promise((resolve) => setTimeout(resolve, 1_250));
        return { output: "ok" };
      },
      {
        ...options,
        timeoutMs: 1_000, signal,
        minimumTimeoutMsForHostCall(ref) {
          return ref === "fabric.$call" ? 3_000 : undefined;
        },
      },
    ), () => admitted);

    expect(result.terminationReason).toBe("completed");
    expect(result.value).toBe("ok");
  });

  it("preserves named string payloads", async () => {
    const content = [
      "multiline",
      "` ${value} { braces }",
      "quotes: \" '",
      "nul:" + String.fromCharCode(0) + " end",
    ].join("\n");
    const result = await new NodeProcessRuntime().execute(
      "return π.content;",
      async () => undefined,
      { ...options, strings: { content } },
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toBe(content);
  });

  it("accepts a heap limit above the QuickJS WASM32 ceiling", async () => {
    const result = await new NodeProcessRuntime().execute(
      "return 1;",
      async () => undefined,
      { ...options, memoryLimitBytes: 5 * 1024 ** 3 },
    );

    expect(result.terminationReason).toBe("completed");
    expect(result.value).toBe(1);
  });

  it("waits for issued host calls before completing", async () => {
    let settled = false;
    const result = await new NodeProcessRuntime().execute(
      'void tools.call({ ref: "demo.background" }); return "done";',
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 25));
        settled = true;
      },
      options,
    );

    expect(result.value).toBe("done");
    expect(settled).toBe(true);
  });

  it("does not wait for a non-cooperative sibling host call after guest failure", async () => {
    const startedAt = Date.now();
    const result = await new NodeProcessRuntime().execute(
      `
await Promise.all([
  tools.call({ ref: "demo.never" }),
  Promise.reject(new Error("branch failed")),
]);
`,
      async () => new Promise(() => undefined),
      options,
    );

    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toContain("branch failed");
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  });

  it("bounds non-cooperative fire-and-forget host calls", async () => {
    const startedAt = Date.now();
    const result = await new NodeProcessRuntime().execute(
      'void tools.call({ ref: "demo.never" }); return "done";',
      async () => new Promise(() => undefined),
      options,
    );

    expect(result.terminationReason).toBe("completed");
    expect(result.value).toBe("done");
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  });

  it("forcibly terminates synchronous infinite loops", async () => {
    let admitted = false;
    const result = await executeAfterAdmission(signal => new NodeProcessRuntime().execute(
      'await tools.call({ ref: "demo.ready" }); while (true) {}',
      async () => { admitted = true; },
      { ...options, timeoutMs: 50, signal },
    ), () => admitted);

    expect(result.terminationReason).toBe("timed_out");
    expect(result.error).toContain("timed out after 50ms");
  });

  it("surfaces unbounded recursion as a runtime error", async () => {
    const result = await new NodeProcessRuntime().execute(
      "function f() { return f() + 1; } f();",
      async () => undefined,
      options,
    );

    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toContain("Maximum call stack size exceeded");
  });

  it("terminates the child process when externally aborted", async () => {
    const controller = new AbortController();
    const result = await new NodeProcessRuntime().execute(
      'await tools.call({ ref: "demo.ready" }); await new Promise(() => {});',
      async () => { controller.abort(new Error("stop")); },
      { ...options, signal: controller.signal },
    );

    expect(result.terminationReason).toBe("aborted");
    expect(result.error).toBe("Execution cancelled");
  });
});

describe("NodeProcessRuntime guest stack remapping", () => {
  it("remaps child error frames to user code lines", async () => {
    const result = await new NodeProcessRuntime().execute(
      ["const before = 1;", "print(before);", 'throw new Error("boom");'].join("\n"),
      async () => undefined,
      { timeoutMs: 5_000, memoryLimitBytes: 128 * 1024 * 1024 },
    );

    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toContain("guest code:3:");
    expect(result.error).toContain("boom");
  });
});

describe.skipIf(!hasBun)("BunProcessRuntime", () => {
  it("runs guest code in a disposable Bun process and bridges host calls", async () => {
    const result = await new BunProcessRuntime().execute(
      `
const models = await tools.models();
print("models", models.length);
return { models, process: typeof process };
`,
      async (ref) => ref === "fabric.$models" ? [{ id: "large-model" }] : undefined,
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.logs).toEqual(["models 1"]);
    expect(result.value).toEqual({
      models: [{ id: "large-model" }],
      process: "undefined",
    });
  });

  it("exposes the Bun module namespace as __bun", async () => {
    const result = await new BunProcessRuntime().execute(
      'return { file: typeof __bun.file, glob: typeof __bun.Glob };',
      async () => undefined,
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toEqual({ file: "function", glob: "function" });
  });

  it("preserves named string payloads", async () => {
    const content = [
      "multiline",
      "` ${value} { braces }",
      "quotes: \" '",
      "nul:" + String.fromCharCode(0) + " end",
    ].join("\n");
    const result = await new BunProcessRuntime().execute(
      "return π.content;",
      async () => undefined,
      { ...options, strings: { content } },
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toBe(content);
  });

  it("waits for issued host calls before completing", async () => {
    let settled = false;
    const result = await new BunProcessRuntime().execute(
      'void tools.call({ ref: "demo.background" }); return "done";',
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 25));
        settled = true;
      },
      options,
    );

    expect(result.value).toBe("done");
    expect(settled).toBe(true);
  });

  it("forcibly terminates synchronous infinite loops", async () => {
    let admitted = false;
    const result = await executeAfterAdmission(signal => new BunProcessRuntime().execute(
      'await tools.call({ ref: "demo.ready" }); while (true) {}',
      async () => { admitted = true; },
      { ...options, timeoutMs: 50, signal },
    ), () => admitted);

    expect(result.terminationReason).toBe("timed_out");
    expect(result.error).toContain("timed out after 50ms");
  });

  it("terminates the child process when externally aborted", async () => {
    const controller = new AbortController();
    const result = await new BunProcessRuntime().execute(
      'await tools.call({ ref: "demo.ready" }); await new Promise(() => {});',
      async () => { controller.abort(new Error("stop")); },
      { ...options, signal: controller.signal },
    );

    expect(result.terminationReason).toBe("aborted");
    expect(result.error).toBe("Execution cancelled");
  });

  it("surfaces guest errors as runtime errors", async () => {
    const result = await new BunProcessRuntime().execute(
      'throw new Error("bun boom");',
      async () => undefined,
      options,
    );

    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toContain("bun boom");
  });
});

describe("process runtime dynamic imports", () => {
  it("resolves guest import() on the node vm", async () => {
    const result = await new NodeProcessRuntime().execute(
      'const diff = await import("diff"); return { diffChars: typeof diff.diffChars };',
      async () => undefined,
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toEqual({ diffChars: "function" });
  });

  it.skipIf(!hasBun)("bridges __fabricImport for bun guests", async () => {
    const result = await new BunProcessRuntime().execute(
      'const diff = await __fabricImport("diff"); return { diffChars: typeof diff.diffChars };',
      async () => undefined,
      options,
    );

    expect(result.error).toBeUndefined();
    expect(result.value).toEqual({ diffChars: "function" });
  });
});

describe("process runtime guest timers", () => {
  it("resolves guest setTimeout via fabric.$timer intercept (node)", async () => {
    const result = await new NodeProcessRuntime().execute(
      'await new Promise((r) => setTimeout(r, 50)); return "timer-ok";',
      async (ref) => {
        if (ref === "fabric.$timer") throw new Error("should not reach hostCall");
        return undefined;
      },
      options,
    );

    expect(result.terminationReason).toBe("completed");
    expect(result.value).toBe("timer-ok");
  });

  it.skipIf(!hasBun)("resolves guest setTimeout via fabric.$timer intercept (bun)", async () => {
    const result = await new BunProcessRuntime().execute(
      'await new Promise((r) => setTimeout(r, 50)); return "timer-ok";',
      async (ref) => {
        if (ref === "fabric.$timer") throw new Error("should not reach hostCall");
        return undefined;
      },
      options,
    );

    expect(result.terminationReason).toBe("completed");
    expect(result.value).toBe("timer-ok");
  });
});
