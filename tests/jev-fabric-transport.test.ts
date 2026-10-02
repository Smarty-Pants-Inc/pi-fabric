import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import type { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import { JevProvider } from "../src/providers/jev-provider.js";
import { callProgram, jevContext, launch } from "./jev-test-helpers.js";

const fake = fileURLToPath(new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url));
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

const question = { state: "build log: 0 failures", questions: { healthy: { type: "noul", instructions: "Does the log show a healthy build?" } } };
const answer = { model: "jev-1.13.0", answers: { healthy: { type: "noul", noul: 0.2 } }, usage: { input_tokens: 3, output_tokens: 1 } };

const setup = (transport: "auto" | "fabric" | "jev-fabric", bridgeAvailable = true) => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-jev-transport-")));
  const binary = path.join(root, "jev-fabric");
  fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
  const config = normalizeFabricConfig({ approvals: { network: "allow", execute: "allow", read: "allow", write: "allow" }, jev: { transport } });
  const registry = new ActionRegistry();
  const fetcher = vi.fn(async () => new Response(JSON.stringify(answer), { status: 200 }));
  const client = new JevClient(config.jev, fetcher as unknown as typeof fetch, new JevCredentials([], { TYPESAFE_API_KEY: "test-only-never-a-real-key" }));
  const bridge = { home: path.join(root, "home"), options: { cwd: root }, resolve: async () => ({ path: binary }) } as unknown as DurableShellBridge;
  const provider = new JevProvider({ registry, config, ...(bridgeAvailable ? { jevFabric: bridge } : {}) }, client);
  registry.register(provider);
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }), () => registry.close());
  return { provider, fetcher, root };
};
const program = launch("return (await jev.evaluate(input)).model;", { requires: ["jev.evaluate"], limits: { maxEvaluations: 2 } }, question);

describe.skipIf(process.platform === "win32")("Jev decisions through jev-fabric", () => {
  it("routes a program's decisions through its own serve connection with a per-request credential", async () => {
    const { provider, fetcher, root } = setup("auto");
    const run = await callProgram(provider, "run", program);
    expect(run).toMatchObject({ state: "completed", result: "fake:typesafe:credential:jev-latest", evaluations: 1 });
    expect(fetcher).not.toHaveBeenCalled();
    expect(await provider.invoke("status", {}, jevContext())).toMatchObject({ transport: { configured: "auto", last: "jev-fabric" } });
    // Jev-only connections never create a job store.
    expect(fs.existsSync(path.join(root, "home"))).toBe(false);
  });

  it("keeps direct calls and the fabric transport in-process", async () => {
    const direct = setup("auto");
    expect(await direct.provider.invoke("evaluate", question, jevContext())).toMatchObject({ model: "jev-1.13.0" });
    expect(direct.fetcher).toHaveBeenCalledOnce();
    const inProcess = setup("fabric");
    expect(await callProgram(inProcess.provider, "run", program)).toMatchObject({ state: "completed", result: "jev-1.13.0" });
  });

  it("stays in-process by default, even with jev-fabric available", async () => {
    expect(normalizeFabricConfig({}).jev.transport).toBe("fabric");
    const { root } = setup("fabric");
    const config = normalizeFabricConfig({ approvals: { network: "allow", execute: "allow", read: "allow", write: "allow" } });
    const registry = new ActionRegistry();
    const fetcher = vi.fn(async () => new Response(JSON.stringify(answer), { status: 200 }));
    const resolve = vi.fn();
    const provider = new JevProvider({ registry, config, jevFabric: { home: root, options: { cwd: root }, resolve } as unknown as DurableShellBridge },
      new JevClient(config.jev, fetcher as unknown as typeof fetch, new JevCredentials([], { TYPESAFE_API_KEY: "test-only-never-a-real-key" })));
    registry.register(provider);
    cleanups.push(() => registry.close());
    expect(await callProgram(provider, "run", program)).toMatchObject({ state: "completed", result: "jev-1.13.0" });
    expect(resolve).not.toHaveBeenCalled();
  });

  it("falls back only when no binary is available under auto, and fails under jev-fabric", async () => {
    expect(await callProgram(setup("auto", false).provider, "run", program)).toMatchObject({ state: "completed", result: "jev-1.13.0" });
    expect(await callProgram(setup("jev-fabric", false).provider, "run", program)).toMatchObject({ state: "failed", error: expect.stringContaining("needs jev-fabric") });
  });
});
