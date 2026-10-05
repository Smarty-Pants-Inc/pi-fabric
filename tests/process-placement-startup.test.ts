import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ loads: 0, launches: 0, local: 0 }));
vi.mock("../src/agents/transports/placement.js", () => {
  calls.loads++;
  return { launchPlacedTask: async () => {
    calls.launches++;
    return { kind: "process", isAlive: async () => false, stop: async () => {} };
  } };
});
vi.mock("../src/agents/transports/process-utils.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/agents/transports/process-utils.js")>(),
  spawnDetached: async () => {
    calls.local++;
    return { pid: 12345, isAlive: async () => false, stop: async () => {} };
  },
}));
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { agentPlacementProbe, normalizeAgentPlacement, probeAgentPlacement } from "../src/agents/placement-config.js";
import { FabricState } from "../src/fabric-state.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  calls.launches = 0; calls.local = 0;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const raw = (command = process.execPath) => ({ default: "remote", command: [command, "{id}"], resultDirectory: "/unused/{id}", cancelCommand: [command, "--cancel", "{id}"] });
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-startup-")); roots.push(directory); return directory; };
const contextAt = (cwd: string): ExtensionContext => ({
  cwd, isProjectTrusted: () => false, ui: { setStatus: vi.fn() },
} as unknown as ExtensionContext);
const stateAt = (placement: unknown, cwd: string) => {
  const profile = root();
  vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ agents: { placement }, mesh: { enabled: false } }));
  const runtimeLoader = vi.fn();
  const state = new FabricState({} as ExtensionAPI, new CapturedToolCatalog(), { runtimeLoader });
  return { state, runtimeLoader, context: contextAt(cwd) };
};
const documentedPlacement = () => {
  const docs = fs.readFileSync(new URL("../docs/configuration.md", import.meta.url), "utf8");
  return normalizeAgentPlacement(JSON.parse(docs.match(/Ryzen 1 example[\s\S]*?```json\n([\s\S]*?)\n```/)![1]!).agents.placement)!;
};

describe("placement import/registration/idle boundary", () => {
  it("does not load or launch the optional adapter until first eligible use", async () => {
    const placement = normalizeAgentPlacement(raw())!;
    const transport = new ProcessTransport(undefined, placement);
    expect(await transport.available()).toBe(true);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(calls).toEqual({ loads: 0, launches: 0, local: 0 });
    const handle = await transport.launch({ id: "first-use", name: "probe", cwd: "/unused", workerPath: "/unused", workerArguments: [] });
    expect(calls).toEqual({ loads: 1, launches: 1, local: 0 });
    expect(handle.kind).toBe("process");
    await handle.stop();
  });
  it("validates the documented Ryzen 1 --src flags and the work-host alias map", () => {
    const placement = documentedPlacement();
    expect(placement.default).toBe("remote");
    expect(placement.command.slice(placement.command.indexOf("--src"), placement.command.indexOf("--src") + 2)).toEqual(["--src", "{cwd}"]);
    expect(placement.command).not.toContain("--cwd");
    expect(placement.resultCommand).toContain("{sshAlias}");
    expect(placement.sshAliases).toEqual({ ryzen2: "ryzen2-agent", ryzen3: "forge-agent", ryzen4: "ryzen4-agent", ryzen5: "ryzen5-agent" });
  });
  it("probes startup with a Ryzen 1-style cwd without running the launcher or loading the runtime", async () => {
    const placement = documentedPlacement();
    placement.command[0] = process.execPath; // Only a local executable check, never SSH.
    const fixture = stateAt(placement, "/home/paul/smarty/smarty-pants/pi-fabric");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const loads = calls.loads;
    await fixture.state.bootstrap(fixture.context);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("startup probe: executable"));
    expect(fixture.runtimeLoader).not.toHaveBeenCalled();
    expect(calls).toEqual({ loads, launches: 0, local: 0 });
    expect(agentPlacementProbe(fixture.state.config.agents.placement!, fixture.context.cwd).reason).toBeUndefined();
    fixture.state.reloadConfig(fixture.context);
    expect(warn).toHaveBeenCalledTimes(1);
    await fixture.state.shutdown();
  });
  it.each(["missing", "non-executable", "directory"])("keeps %s startup placement local with one diagnostic and one audit reason", async kind => {
    if (process.platform === "win32" && kind === "non-executable") return;
    const cwd = root(); const launcher = path.join(cwd, "launcher");
    if (kind === "non-executable") fs.writeFileSync(launcher, "not executable", { mode: 0o600 });
    if (kind === "directory") fs.mkdirSync(launcher, { mode: 0o700 });
    const fixture = stateAt(raw(launcher), cwd);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const loads = calls.loads;
    await fixture.state.bootstrap(fixture.context);
    expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("placement-probe-failed:"));
    expect(warn.mock.calls[0]![0]).toContain("falling back to local");
    expect(fixture.state.config.agents.placement!.default).toBe("remote");
    expect(fixture.runtimeLoader).not.toHaveBeenCalled();
    const log = path.join(cwd, "events.jsonl");
    const handle = await new ProcessTransport(undefined, fixture.state.config.agents.placement).launch({
      id: "fallback", name: "probe", cwd, workerPath: path.join(cwd, "unused-worker"), workerArguments: ["--log-file", log],
    });
    expect(calls).toEqual({ loads, launches: 0, local: 1 });
    const events = fs.readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events).toEqual([expect.objectContaining({ type: "placement.local", reason: `placement-probe-failed: command missing or not executable: ${launcher}` })]);
    await handle.stop(); await fixture.state.shutdown();
  });
  it("is silent with placement absent", async () => {
    const fixture = stateAt(undefined, root()); const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await fixture.state.bootstrap(fixture.context);
    expect(warn).not.toHaveBeenCalled(); expect(fixture.runtimeLoader).not.toHaveBeenCalled();
    await fixture.state.shutdown();
  });
  it("resolves executable names on PATH and relative paths without executing them", () => {
    const cwd = root(); const launcher = path.join(cwd, process.platform === "win32" ? "launcher.exe" : "launcher");
    fs.writeFileSync(launcher, "never execute this", { mode: 0o700 });
    vi.stubEnv("PATH", cwd);
    expect(probeAgentPlacement(normalizeAgentPlacement(raw("launcher"))!, cwd).executable).toBe(launcher);
    expect(probeAgentPlacement(normalizeAgentPlacement(raw(`./${path.basename(launcher)}`))!, cwd).executable).toBe(launcher);
  });
  it("rechecks a repaired executable on config reload and does not repeat the unchanged diagnostic", async () => {
    const cwd = root(); const launcher = path.join(cwd, process.platform === "win32" ? "launcher.exe" : "launcher");
    const fixture = stateAt(raw(launcher), cwd); const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await fixture.state.bootstrap(fixture.context);
    fs.writeFileSync(launcher, "never execute this", { mode: 0o700 });
    fixture.state.reloadConfig(fixture.context);
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toContain("startup probe: executable");
    expect(agentPlacementProbe(fixture.state.config.agents.placement!, cwd).reason).toBeUndefined();
    fixture.state.reloadConfig(fixture.context);
    expect(warn).toHaveBeenCalledTimes(2);
    await fixture.state.shutdown();
  });
});
