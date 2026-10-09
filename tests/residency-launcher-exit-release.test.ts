import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const waitFor = async (predicate: () => boolean, boundMs: number): Promise<void> => {
  const until = Date.now() + boundMs;
  while (!predicate()) {
    if (Date.now() >= until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const snapshot = (directory: string, files = new Map<string, string>()): Map<string, string> => {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) snapshot(file, files);
    else try { const stat = fs.statSync(file); files.set(file, `${stat.size}:${stat.mtimeMs}`); } catch { /* removed meanwhile */ }
  }
  return files;
};
const previousConfigEnv = process.env.PI_FABRIC_RESIDENT_CONFIG;
afterEach(() => {
  vi.restoreAllMocks();
  if (previousConfigEnv === undefined) delete process.env.PI_FABRIC_RESIDENT_CONFIG;
  else process.env.PI_FABRIC_RESIDENT_CONFIG = previousConfigEnv;
});

// smarty-dev#7770 / #1882: the exit marker (which the launcher logs into
// launcher.log) and error.json are root writes. After the host releases
// owner.json the root may belong to the next generation: nothing more.
it("a failure from close() after the release writes no marker, no error.json, nothing late", { timeout: 60_000 }, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-exit-release-"));
  const sessionId = "exit-release";
  const meshRoot = path.join(root, "mesh");
  const residencyRoot = residentRoot(meshRoot, `session:${sessionId}`);
  const config: ResidentHostConfig = {
    format: 1, rootId: `session:${sessionId}`, sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"), residencyRoot,
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(residencyRoot, { recursive: true, mode: 0o700 });
  const configPath = path.join(residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  process.env.PI_FABRIC_RESIDENT_CONFIG = configPath;
  const ownerPath = path.join(residencyRoot, "owner.json");
  const markers: Array<{ line: string; owned: boolean }> = [];
  const write = process.stderr.write.bind(process.stderr);
  vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array, ...rest: never[]) => {
    const line = String(chunk);
    if (line.startsWith("pi-fabric-resident-exit ")) markers.push({ line, owned: fs.existsSync(ownerPath) });
    return write(chunk, ...rest);
  }) as typeof process.stderr.write);
  // The release is the last step of close(); this failure surfaces after it.
  vi.spyOn(AgentManager.prototype, "close").mockRejectedValue(new Error("injected teardown failure"));
  let released: Map<string, string> | undefined;
  const rmSync = fs.rmSync;
  vi.spyOn(fs, "rmSync").mockImplementation(((file: fs.PathLike, options?: fs.RmOptions) => {
    rmSync(file, options);
    if (String(file) === ownerPath) released = snapshot(meshRoot);
  }) as typeof fs.rmSync);
  const controller = new AbortController();
  try {
    const run = runResidentHostFromConfigPath(configPath, controller.signal);
    await waitFor(() => fs.existsSync(ownerPath), 30_000);
    controller.abort();
    await expect(run).rejects.toThrow("injected teardown failure");
    expect(released).toBeDefined();
    const late = [...snapshot(meshRoot)].filter(([file, stamp]) => released!.get(file) !== stamp).map(([file]) => path.relative(root, file));
    expect(late).toEqual([]);
    expect(fs.existsSync(path.join(residencyRoot, "error.json"))).toBe(false);
    // One marker, written while the host still owned the root.
    expect(markers).toEqual([{ line: 'pi-fabric-resident-exit {"reason":"stopped"}\n', owned: true }]);
  } finally {
    vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
