import fs from "node:fs";
import os from "node:os";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import type { ExtensionRunner } from "@earendil-works/pi-coding-agent";
import type { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { SessionsProvider } from "../src/providers/sessions-provider.js";

const fake = fileURLToPath(new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url));
// This regression must never substitute the protocol fixture for the native backend.
const realBackend = (() => {
  try { return createRequire(import.meta.url)("jev-fabric").binaryPath() as string | undefined; }
  catch { return undefined; }
})();
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.unstubAllEnvs(); });

const setup = (options: { shellOverride?: boolean; root?: string; hook?: (event: any) => Promise<any>; delaySpawnResponse?: boolean } = {}) => {
  const root = options.root ?? fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sessions-")));
  // PI_FABRIC_JEV_FABRIC_BIN runs the same contract against a real jev-fabric build.
  const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(root, "jev-fabric");
  if (!process.env.PI_FABRIC_JEV_FABRIC_BIN && !fs.existsSync(binary)) fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
  let selected = binary;
  if (options.delaySpawnResponse) {
    selected = path.join(root, "jev-fabric-delayed");
    const proxy = fileURLToPath(new URL("./fixtures/delayed-jev-fabric.mjs", import.meta.url));
    fs.writeFileSync(selected, `#!/bin/sh\nexec "${process.execPath}" "${proxy}" "${binary}" "$@"\n`, { mode: 0o755 });
  }
  const bridge = { home: path.join(root, "home"), resolve: async () => ({ path: selected }) } as unknown as DurableShellBridge;
  const catalog = new CapturedToolCatalog();
  catalog.replace([], { emitToolCall: options.hook ?? (async () => undefined) } as unknown as ExtensionRunner,
    DEFAULT_FABRIC_CONFIG.capture, "fabric");
  const piTools = new PiToolsProvider(root, catalog);
  const provider = new SessionsProvider(bridge, { cwd: root, shellOverride: () => options.shellOverride === true,
    admitShell: (args, context) => piTools.admitInteractiveShell(args, context),
  });
  cleanups.push(() => piTools.close());
  if (!options.root) cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  cleanups.push(() => provider.close());
  const call = (name: string, args: Record<string, unknown>, parentToolCallId = "fabric_exec_1") =>
    provider.invoke(name, args, { parentToolCallId, signal: new AbortController().signal } as unknown as FabricInvocationContext) as Promise<Record<string, any>>;
  return { root, provider, call };
};

describe.skipIf(process.platform === "win32")("sessions through jev-fabric serve", () => {
  it("drives a persistent interactive child with write and read by offset", async () => {
    const { call } = setup();
    const opened = await call("open", { argv: ["cat"], label: "echo" });
    expect(opened).toMatchObject({ id: expect.stringMatching(/^s-/), lifetime: "session", state: "running" });
    let offset = 0;
    for (const message of ["ping\n", "pong\n"]) {
      await call("write", { id: opened.id, text: message });
      const record = await call("read", { id: opened.id, offset, waitMs: 5000 });
      expect(record).toMatchObject({ stream: "stdout", offset, text: message, next: offset + message.length, eof: false });
      offset = record.next;
    }
    await call("closeInput", { id: opened.id });
    expect(await call("wait", { id: opened.id, timeoutMs: 5000 })).toMatchObject({ state: "exited", exitCode: 0 });
    expect(await call("read", { id: opened.id, offset })).toMatchObject({ bytes: 0, eof: true });
  });

  it("opens a durable interactive child that keeps running after the connection ends", async () => {
    const { call, provider, root } = setup();
    const opened = await call("open", { argv: ["cat"], durable: true, label: "durable echo", cwd: root });
    expect(opened).toMatchObject({ lifetime: "durable" });
    expect(opened.id).not.toMatch(/^s-/);
    await call("write", { id: opened.id, text: "kept\n" });
    expect(await call("read", { id: opened.id, offset: 0, waitMs: 10000 })).toMatchObject({ text: "kept\n", next: 5 });
    await provider.close();
    // Another connection (another Pi session, or the CLI) still reaches the durable child.
    const other = setup({ root });
    await other.call("closeInput", { id: opened.id });
    expect(await other.call("wait", { id: opened.id, timeoutMs: 10000 })).toMatchObject({ state: "exited" });
  });

  it("answers other requests while a long-poll read is pending", async () => {
    const { call } = setup();
    const { id } = await call("open", { cmd: "read line; echo \"got $line\"" });
    const pending = call("read", { id, offset: 0, waitMs: 10000 });
    expect(await call("status", { id })).toMatchObject({ state: "running" });
    await call("write", { id, text: "x\n" });
    expect(await pending).toMatchObject({ text: "got x\n" });
  });

  it("stops a Jev program's session children when the program ends, but not fabric_exec ones", async () => {
    const { call, provider } = setup();
    const program = await call("open", { argv: ["sleep", "30"] }, "jev:run-1");
    const agent = await call("open", { argv: ["sleep", "30"] });
    await provider.invocationEnded("jev:run-1");
    expect(await call("status", { id: program.id })).toMatchObject({ state: "cancelled" });
    expect(await call("status", { id: agent.id })).toMatchObject({ state: "running" });
    expect((await call("list", {}) as unknown as Array<{ id: string }>).map(entry => entry.id)).toEqual([agent.id]);
  });

  it.skipIf(!realBackend)("SEC-8 closes every settled owner's real backend and confirms exit without touching another owner", async () => {
    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sec8-real-")));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const backendPids = path.join(root, "backend-pids");
    const launcher = path.join(root, "record-real-backend");
    // Record the serve PID, not the session worker's PPID. exec preserves it;
    // all protocol handling and children remain in the unmodified real binary.
    fs.writeFileSync(launcher, `#!/bin/sh\necho $$ >> '${backendPids}'\nexec '${realBackend}' "$@"\n`, { mode: 0o755 });
    vi.stubEnv("PI_FABRIC_JEV_FABRIC_BIN", launcher);
    const { call, provider } = setup({ root });
    const open = async (owner: string) => {
      const receipt = await call("open", { cmd: "echo $$; exec sleep 60" }, owner);
      const output = await call("read", { id: receipt.id, offset: 0, waitMs: 5000 });
      const child = Number(output.text.trim());
      const backend = Number(fs.readFileSync(backendPids, "utf8").trim().split("\n").at(-1));
      expect(child).toBeGreaterThan(0);
      expect(backend).toBeGreaterThan(0);
      expect(() => process.kill(backend, 0)).not.toThrow();
      return { id: receipt.id, child, backend };
    };
    // Two fully settled opens by A each create an isolated serve connection.
    const owned = [await open("jev:owner-A"), await open("jev:owner-A")];
    const survivor = await open("jev:owner-B");
    expect(new Set([...owned, survivor].map(child => child.backend)).size).toBe(3);
    try {
      await provider.invocationEnded("jev:owner-A");
      // No polling after retirement: its promise must confirm backend exit.
      for (const { id, child, backend } of owned) {
        expect(() => process.kill(backend, 0)).toThrow();
        expect(() => process.kill(child, 0)).toThrow();
        const receipt = await call("status", { id });
        expect(receipt).toMatchObject({ state: "cancelled" });
        expect(await call("wait", { id })).toEqual(receipt);
        expect(await call("stop", { id })).toEqual(receipt);
      }
      // Terminal reads must not replace retired connections with live backends.
      expect(fs.readFileSync(backendPids, "utf8").trim().split("\n")).toHaveLength(3);
      expect(await call("list", {})).toEqual([expect.objectContaining({ id: survivor.id })]);
      expect(() => process.kill(survivor.backend, 0)).not.toThrow();
      expect(() => process.kill(survivor.child, 0)).not.toThrow();
      expect(await call("status", { id: survivor.id })).toMatchObject({ state: "running" });
      await provider.invocationEnded("jev:owner-A");
      expect(await call("status", { id: survivor.id })).toMatchObject({ state: "running" });
      await provider.invocationEnded("jev:owner-B");
      expect(() => process.kill(survivor.backend, 0)).toThrow();
      expect(() => process.kill(survivor.child, 0)).toThrow();
    } finally {
      // Also reap this test's backends on the vulnerable head.
      await provider.close();
    }
  });

  it("refuses to bypass an extension's bash override and needs exactly one of argv or cmd", async () => {
    await expect(setup({ shellOverride: true }).call("open", { argv: ["true"] })).rejects.toThrow("bypass its shell protection");
    const { call } = setup();
    await expect(call("open", {})).rejects.toThrow("exactly one of argv or cmd");
    await expect(call("open", { argv: ["true"], cmd: "true" })).rejects.toThrow("exactly one of argv or cmd");
    await expect(call("open", { argv: ["true"], cwd: "missing-dir" })).rejects.toThrow("Working directory does not exist");
  });

  it("SEC-1 excludes sessions from discovery and invocation when bash authority is absent", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", JSON.stringify(["read"]));
    const { provider, call } = setup();
    expect(await provider.list({})).toEqual([]);
    expect(await provider.describe("open")).toBeUndefined();
    // execute-risk approval does not grant the inherited bash capability.
    await expect(call("open", { cmd: "touch escaped-authority" })).rejects.toThrow("allowlist");
  });

  it("SEC-1 refuses direct interactive launch despite execute-risk approval without bash authority", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", JSON.stringify(["read"]));
    const { call } = setup();
    await expect(call("open", { cmd: "true" })).rejects.toThrow("allowlist");
  });
  it("SEC-1 routes hook-only command denial through real bash admission before spawn", async () => {
    const hook = vi.fn(async (event: any) => event.toolName === "bash" ? { block: true, reason: "host command denied" } : undefined);
    const { root, call } = setup({ hook });
    await expect(call("open", { cmd: "touch escaped-hook" })).rejects.toThrow("host command denied");
    expect(hook).toHaveBeenCalledWith(expect.objectContaining({ toolName: "bash", input: expect.objectContaining({ command: "touch escaped-hook", cwd: root }) }));
    expect(fs.existsSync(path.join(root, "escaped-hook"))).toBe(false);
  });

  it("SEC-1 preserves host-owned timeout injection and fails closed without admission", async () => {
    const { call } = setup({ hook: async (event: any) => { event.input.timeout = 1; } });
    const { id } = await call("open", { cmd: "sleep 30" });
    await vi.waitFor(async () => expect((await call("status", { id })).state).not.toBe("running"), { timeout: 4000 });
    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-no-admission-")));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const provider = new SessionsProvider({ resolve: async () => { throw new Error("must not connect"); } } as unknown as DurableShellBridge,
      { cwd: root, shellOverride: () => false });
    await expect(provider.invoke("open", { argv: ["true"] }, { signal: new AbortController().signal } as FabricInvocationContext)).rejects.toThrow("host shell admission");
  });
  it.each(["abort", "owner-retire"])("SEC-4 cleans an in-flight Jev launch real child process group on %s", async (mode) => {
    const { root, provider } = setup({ delaySpawnResponse: true });
    const ready = path.join(root, "spawn-ready.json");
    const pids = path.join(root, "child-pids");
    vi.stubEnv("JEV_TEST_SPAWN_READY_FILE", ready);
    const controller = new AbortController();
    const script = `sleep 60 & child=$!; echo "$$ $child" > '${pids}'; trap 'kill "$child" 2>/dev/null; wait "$child"; exit 0' TERM; wait "$child"`;
    const launch = provider.invoke("open", { argv: ["/bin/bash", "-c", script] }, {
      parentToolCallId: "jev:cancel-in-flight", nestedToolCallId: "security-launch", signal: controller.signal,
    } as FabricInvocationContext);
    const outcome = launch.then(() => undefined, error => error);
    await vi.waitFor(() => expect(fs.existsSync(ready) && fs.existsSync(pids)).toBe(true), { timeout: 5000 });
    const descendants = fs.readFileSync(pids, "utf8").trim().split(" ").map(Number);
    const pid = descendants[0]!;
    expect(() => process.kill(-pid, 0)).not.toThrow();
    if (mode === "abort") controller.abort(new Error("security cancellation"));
    const retired = mode === "owner-retire" ? provider.invocationEnded("jev:cancel-in-flight") : Promise.resolve();
    fs.writeFileSync(ready + ".release", "release");
    try {
      expect(await outcome).toBeInstanceOf(Error);
      await retired;
      await vi.waitFor(() => {
        expect(() => process.kill(-pid, 0)).toThrow();
        for (const child of descendants) expect(() => process.kill(child, 0)).toThrow();
      }, { timeout: 2000 });
    } finally {
      // Only the provider/test-owned connection and group are stopped, even on
      // the vulnerable head where assertions fail.
      await provider.close();
    }
  });
  it.each(["abort", "owner-retire", "close"])("SEC-4 bounds cleanup with a withheld receipt and real child group on %s", async (mode) => {
    const { root, provider, call } = setup({ delaySpawnResponse: true });
    const ready = path.join(root, "withheld-ready.json");
    const pids = path.join(root, "withheld-pids");
    // Configure the gate before even the base head opens its shared connection.
    vi.stubEnv("JEV_TEST_SPAWN_READY_FILE", ready);
    // A separate owner's real child must survive owner-specific retirement.
    const survivor = await call("open", { argv: ["sleep", "60"], label: "ungated-survivor" }, "fabric_exec_survivor");
    const controller = new AbortController();
    const script = `sleep 60 & child=$!; echo "$$ $child" > '${pids}'; trap 'kill "$child" 2>/dev/null; wait "$child"; exit 0' TERM; wait "$child"`;
    const launch = provider.invoke("open", { argv: ["/bin/bash", "-c", script] }, {
      parentToolCallId: "jev:withheld-owner", nestedToolCallId: "withheld-launch", signal: controller.signal,
    } as FabricInvocationContext);
    const outcome = launch.then(() => undefined, error => error);
    await vi.waitFor(() => expect(fs.existsSync(ready) && fs.existsSync(pids)).toBe(true), { timeout: 5000 });
    const descendants = fs.readFileSync(pids, "utf8").trim().split(" ").map(Number);
    const group = descendants[0]!;
    expect(() => process.kill(-group, 0)).not.toThrow();
    let ended: Promise<void> = Promise.resolve();
    try {
      if (mode === "abort") controller.abort(new Error("withheld cancellation"));
      if (mode === "owner-retire") ended = provider.invocationEnded("jev:withheld-owner");
      if (mode === "close") ended = provider.close();
      // Keep the response gate closed throughout the cleanup deadline.
      await vi.waitFor(() => {
        expect(fs.existsSync(ready + ".release")).toBe(false);
        expect(() => process.kill(-group, 0)).toThrow();
        for (const pid of descendants) expect(() => process.kill(pid, 0)).toThrow();
      }, { timeout: 1800 });
      await ended;
      expect(await outcome).toBeInstanceOf(Error);
      if (mode !== "close") expect(await call("status", { id: survivor.id })).toMatchObject({ state: "running" });
    } finally {
      // On the base head, release only AFTER the deadline assertion failed,
      // then observe compensation and reap every process this test started.
      fs.writeFileSync(ready + ".release", "release for cleanup");
      await outcome;
      await ended;
      await provider.close();
    }
  });

  it("ends session children when the provider closes", async () => {
    const { call, provider } = setup();
    const { id } = await call("open", { cmd: "echo $$; exec sleep 30" });
    const pid = Number((await call("read", { id, offset: 0, waitMs: 5000 })).text.trim());
    expect(() => process.kill(pid, 0)).not.toThrow();
    await provider.close();
    await vi.waitFor(() => expect(() => process.kill(pid, 0)).toThrow(), { timeout: 5000 });
    await expect(call("status", { id })).rejects.toThrow("closed");
  });
});
