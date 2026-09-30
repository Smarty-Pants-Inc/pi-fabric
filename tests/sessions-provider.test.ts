import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { SessionsProvider } from "../src/providers/sessions-provider.js";

const fake = fileURLToPath(new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url));
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

const setup = (options: { shellOverride?: boolean; root?: string } = {}) => {
  const root = options.root ?? fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sessions-")));
  // PI_FABRIC_JEV_FABRIC_BIN runs the same contract against a real jev-fabric build.
  const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(root, "jev-fabric");
  if (!process.env.PI_FABRIC_JEV_FABRIC_BIN && !fs.existsSync(binary)) fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
  const bridge = { home: path.join(root, "home"), resolve: async () => ({ path: binary }) } as unknown as DurableShellBridge;
  const provider = new SessionsProvider(bridge, { cwd: root, shellOverride: () => options.shellOverride === true });
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
