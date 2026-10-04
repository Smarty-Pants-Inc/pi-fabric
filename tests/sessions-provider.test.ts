import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionRegistry } from "../src/core/action-registry.js";
import { JevFabricServe } from "../src/jev-fabric/serve.js";
import type { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { SessionsProvider } from "../src/providers/sessions-provider.js";

const fake = fileURLToPath(new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url));
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

const setup = (options: { shellOverride?: boolean; root?: string; writePolicy?: () => any; landlockEnforced?: () => boolean; trustedExternalControl?: () => boolean; launchDelayMs?: number } = {}) => {
  const root = options.root ?? fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sessions-")));
  // PI_FABRIC_JEV_FABRIC_BIN runs the same contract against a real jev-fabric build.
  const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(root, "jev-fabric");
  if (!process.env.PI_FABRIC_JEV_FABRIC_BIN && !fs.existsSync(binary)) fs.writeFileSync(binary, `#!/bin/sh\nFAKE_JEV_FABRIC_SERVE_LAUNCH_DELAY_MS="${options.launchDelayMs ?? 0}" exec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
  const bridge = { home: path.join(root, "home"), resolve: async () => ({ path: binary }) } as unknown as DurableShellBridge;
  const provider = new SessionsProvider(bridge, {
    cwd: root,
    shellOverride: () => options.shellOverride === true,
    ...(options.writePolicy ? { writePolicy: options.writePolicy } : {}),
    ...(options.landlockEnforced ? { landlockEnforced: options.landlockEnforced } : {}),
    ...(options.trustedExternalControl ? { trustedExternalControl: options.trustedExternalControl } : {}),
  });
  if (!options.root) cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  cleanups.push(() => provider.close());
  const call = (name: string, args: Record<string, unknown>, parentToolCallId = "fabric_exec_1") =>
    provider.invoke(name, args, { parentToolCallId, signal: new AbortController().signal } as unknown as FabricInvocationContext) as Promise<Record<string, any>>;
  return { root, provider, call };
};

describe.skipIf(process.platform === "win32")("sessions through jev-fabric serve", () => {
  it.each([false, true])("fails closed for child write policy and Landlock, for both command forms (durable=%s)", async durable => {
    const policy = () => ({ readOnly: false, writableRoots: [process.cwd()], shell: "deny" as const });
    await expect(setup({ writePolicy: policy }).call("open", { argv: ["true"], durable })).rejects.toThrow(/write policy|shell/);
    await expect(setup({ writePolicy: policy }).call("open", { cmd: "true", durable })).rejects.toThrow(/write policy|shell/);
    await expect(setup({ landlockEnforced: () => true }).call("open", { argv: ["true"], durable })).rejects.toThrow(/Landlock enforce/);
    await expect(setup({ landlockEnforced: () => true }).call("open", { cmd: "true", durable })).rejects.toThrow(/Landlock enforce/);
  });

  it.each([
    { writePolicy: () => ({ readOnly: false, writableRoots: [process.cwd()], shell: "deny" as const }) },
    { writePolicy: () => ({ readOnly: false, writableRoots: [process.cwd()], shell: "unconfined" as const }) },
    { landlockEnforced: () => true },
    { shellOverride: true },
    {},
  ])("SR-2 refuses existing durable job input/control without effective confinement and trusted authority: %j", async restriction => {
    const owner = setup();
    const opened = await owner.call("open", { cmd: "cat", durable: true });
    cleanups.push(async () => { await owner.call("closeInput", { id: opened.id }); await owner.call("wait", { id: opened.id, timeoutMs: 10000 }); });
    const other = setup({ root: owner.root, ...restriction });
    await expect(other.call("write", { id: opened.id, text: "escaped\n" })).rejects.toThrow(/shell|policy|Landlock|trusted/i);
    await expect(other.call("closeInput", { id: opened.id })).rejects.toThrow(/shell|policy|Landlock|trusted/i);
    expect(fs.readFileSync(path.join(owner.root, "home", opened.id, "input"), "utf8")).toBe("");
    expect(fs.existsSync(path.join(owner.root, "home", opened.id, "input.closed"))).toBe(false);
    expect(await owner.call("status", { id: opened.id })).toMatchObject({ state: "running" });
  });

  it.each([false, true])("SR-7 retains delayed launch receipts and stops cancelled children (durable=%s)", async durable => {
    const { provider, call, root } = setup({ launchDelayMs: 250 });
    const abort = new AbortController();
    const pending = provider.invoke("open", { argv: ["sleep", "30"], durable }, {
      parentToolCallId: "fabric_exec_cancel", signal: abort.signal,
    } as unknown as FabricInvocationContext);
    const outcome = pending.then(value => ({ value }), error => ({ error }));
    const marker = path.join(root, "home", "launch-submitted.json");
    await vi.waitFor(() => expect(fs.existsSync(marker)).toBe(true));
    const { id } = JSON.parse(fs.readFileSync(marker, "utf8"));
    cleanups.push(async () => { await call("stop", { id }); });
    abort.abort(new Error("launch cancelled"));
    expect(await outcome).toMatchObject({ error: expect.any(Error) });
    expect(await call("status", { id })).toMatchObject({ state: "cancelled" });
  });

  it("SR-7 ends a Jev invocation before acknowledgement without losing its session child", async () => {
    const { provider, call, root } = setup({ launchDelayMs: 250 });
    const pending = call("open", { argv: ["sleep", "30"] }, "jev:delayed-owner");
    const outcome = pending.then(value => ({ value }), error => ({ error }));
    const marker = path.join(root, "home", "launch-submitted.json");
    await vi.waitFor(() => expect(fs.existsSync(marker)).toBe(true));
    const { id } = JSON.parse(fs.readFileSync(marker, "utf8"));
    await provider.invocationEnded("jev:delayed-owner");
    expect(await outcome).toMatchObject({ error: expect.any(Error) });
    expect(await call("status", { id })).toMatchObject({ state: "cancelled" });
  });

  it("SR-7 joins a delayed durable acknowledgement and stops it before provider close", async () => {
    const { provider, call, root } = setup({ launchDelayMs: 250 });
    const pending = call("open", { argv: ["sleep", "30"], durable: true });
    const outcome = pending.then(value => ({ value }), error => ({ error }));
    const marker = path.join(root, "home", "launch-submitted.json");
    await vi.waitFor(() => expect(fs.existsSync(marker)).toBe(true));
    const { id } = JSON.parse(fs.readFileSync(marker, "utf8"));
    const other = setup({ root, trustedExternalControl: () => true });
    cleanups.push(async () => { await other.call("stop", { id }); });
    await provider.close();
    expect(await outcome).toMatchObject({ error: expect.any(Error) });
    expect(await other.call("status", { id })).toMatchObject({ state: "cancelled" });
  });

  it("A19 registry teardown ends a live child with a silent launch acknowledgement and retains custody", async () => {
    const { provider, root } = setup({ launchDelayMs: 60_000 });
    const registry = new ActionRegistry();
    registry.register(provider);
    cleanups.push(() => registry.close());
    const close = vi.spyOn(provider, "close");
    const invoke = vi.spyOn(provider, "invoke");
    const pidFile = path.join(root, "child.pid");
    const pending = registry.invoke("sessions.open", { cmd: `echo $$ > "${pidFile}"; exec sleep 90` }, {
      cwd: root, parentToolCallId: "jev:registry-teardown", nestedToolCallId: "pending-launch",
      update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
    } as unknown as FabricInvocationContext & { approve: () => Promise<void>; audits: []; maxResultChars: number });
    const outcome = pending.then(value => ({ value }), error => ({ error }));
    const marker = path.join(root, "home", "launch-submitted.json");
    await vi.waitFor(() => {
      expect(fs.existsSync(marker)).toBe(true);
      expect(fs.existsSync(pidFile)).toBe(true);
    });
    const pid = Number(fs.readFileSync(pidFile, "utf8").trim());
    expect(() => process.kill(pid, 0)).not.toThrow();
    const actual = invoke.mock.results[0]!.value as Promise<unknown>;
    const actualOutcome = actual.then(value => ({ value }), error => ({ error }));
    // Both launch custody and invocationEnded remain in-flight. The lifecycle
    // must still drain before close; shutdown's signal must arm the fallback.
    const ended = registry.endInvocation("jev:registry-teardown");
    await registry.close();
    await ended;
    expect(await outcome).toMatchObject({ error: expect.any(Error) });
    expect(close).not.toHaveBeenCalled();
    expect(() => process.kill(pid, 0)).not.toThrow();
    await vi.waitFor(() => expect(() => process.kill(pid, 0)).toThrow(), { timeout: 35_000, interval: 100 });
    expect(await actualOutcome).toMatchObject({ error: expect.objectContaining({ message: expect.stringMatching(/receipt is uncertain.*custody retained/) }) });
    await vi.waitFor(() => expect(close).toHaveBeenCalled());
    const directory = path.join(root, "home", ".fabric-launch-custody");
    const records = fs.readdirSync(directory);
    expect(records).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, records[0]!), "utf8")))
      .toMatchObject({ version: 1, owner: "jev:registry-teardown", lifetime: "session", state: "awaiting-receipt" });
    expect(registry.providerStatus()).toEqual([]);
  }, 40_000);

  it("SR-7 refuses an already cancelled launch before connecting or submitting", async () => {
    const { provider } = setup();
    const resolve = vi.spyOn(provider.bridge, "resolve");
    const abort = new AbortController(); abort.abort(new Error("pre-launch abort"));
    await expect(provider.invoke("open", { argv: ["true"] }, {
      parentToolCallId: "cancelled-owner", signal: abort.signal,
    } as unknown as FabricInvocationContext)).rejects.toThrow("pre-launch abort");
    expect(resolve).not.toHaveBeenCalled();
  });

  it("SR-7 retains a durable uncertain-launch obligation when the connection loses the receipt", async () => {
    const { provider, root } = setup();
    const request = vi.fn(async () => { throw new Error("connection lost after submission"); });
    const mock = { request, exited: new Promise<void>(() => {}), close: vi.fn(async () => {}) };
    const open = vi.spyOn(JevFabricServe, "open").mockResolvedValueOnce(mock as unknown as JevFabricServe);
    try {
      await expect(provider.invoke("open", { argv: ["true"], durable: true }, {
        parentToolCallId: "uncertain-owner", signal: new AbortController().signal,
      } as unknown as FabricInvocationContext)).rejects.toThrow(/receipt is uncertain.*custody retained/);
      const directory = path.join(root, "home", ".fabric-launch-custody");
      const records = fs.readdirSync(directory);
      expect(records).toHaveLength(1);
      expect(JSON.parse(fs.readFileSync(path.join(directory, records[0]!), "utf8")))
        .toMatchObject({ version: 1, owner: "uncertain-owner", lifetime: "durable", state: "awaiting-receipt" });
      expect(request).toHaveBeenCalledWith("start", expect.any(Object)); // No observation-only abort signal.
    } finally { open.mockRestore(); }
  });

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
    const other = setup({ root, trustedExternalControl: () => true });
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

  it("refuses to bypass an extension's bash override and needs exactly one of argv or cmd", async () => {
    await expect(setup({ shellOverride: true }).call("open", { argv: ["true"] })).rejects.toThrow("bypass its shell protection");
    const { call } = setup();
    await expect(call("open", {})).rejects.toThrow("exactly one of argv or cmd");
    await expect(call("open", { argv: ["true"], cmd: "true" })).rejects.toThrow("exactly one of argv or cmd");
    await expect(call("open", { argv: ["true"], cwd: "missing-dir" })).rejects.toThrow("Working directory does not exist");
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
