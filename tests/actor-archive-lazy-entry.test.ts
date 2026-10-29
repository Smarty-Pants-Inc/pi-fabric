import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import type { BuildOptions, BuildResult, Metafile } from "esbuild";
import { expect, it, vi } from "vitest";

const discoveryLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/agents/actor-archive-discovery.js", async original => {
  discoveryLoads();
  return original<typeof import("../src/agents/actor-archive-discovery.js")>();
});

// Keep: the archive adapter exposed a real hashed-dynamic-import build failure. Exercise the
// production split (including its shared chunks), not a copied entry list or source-text assertion.
// Stop before standalone/native/artifact work; write:false never touches the shared dist/ tree.
async function splitGraph(): Promise<Metafile> {
  const root = fileURLToPath(new URL("..", import.meta.url));
  const source = fs.readFileSync(path.join(root, "scripts/build.mjs"), "utf8");
  const start = source.indexOf("const primaryEntryPoints = [");
  const end = source.indexOf("// Advertise the exact manager/worker contract");
  if (start < 0 || end <= start) throw new Error("Cannot locate the production split build");
  const execute = new Function("esbuild", "buildLandlock", "resolve", `
    return (async () => { ${source.slice(start, end)}; return result; })();
  `) as (
    compile: (options: BuildOptions) => Promise<BuildResult>,
    nativeDigests: () => Record<string, string>,
    resolvePath: typeof path.resolve,
  ) => Promise<BuildResult>;
  const result = await execute(
    options => build({ ...options, absWorkingDir: root, write: false, logLevel: "silent" }),
    () => ({}), // Only the embedded native digests are irrelevant to this JS graph probe.
    (...parts) => path.resolve(root, ...parts),
  );
  if (!result.metafile) throw new Error("Production split did not return its metafile");
  return result.metafile;
}

it("emits a stable archive entry reached dynamically by the manager, outside every startup shared chunk", async () => {
  const { outputs } = await splitGraph();
  const source = "src/agents/actor-archive-discovery.ts";
  const stable = "dist/agents/actor-archive-discovery.js";
  expect(outputs[stable]?.entryPoint).toBe(source);
  const dynamic = Object.entries(outputs).flatMap(([from, output]) => output.imports
    .filter(entry => !entry.external && entry.kind === "dynamic-import")
    .map(entry => ({ from, to: entry.path })));
  expect(dynamic.filter(entry => entry.to.includes("/chunks/"))).toEqual([]);
  const managerImports = dynamic.filter(entry => entry.to === stable &&
    Object.hasOwn(outputs[entry.from]!.inputs, "src/agents/manager.ts"));
  expect(managerImports.length).toBeGreaterThan(0);

  const staticInputs = (entries: string[]): Set<string> => {
    const visited = new Set<string>(), inputs = new Set<string>(), pending = [...entries];
    while (pending.length > 0) {
      const file = pending.pop()!;
      if (visited.has(file)) continue;
      visited.add(file);
      const output = outputs[file];
      if (!output) throw new Error(`Missing static output: ${file}`);
      for (const [input, contribution] of Object.entries(output.inputs)) {
        if (contribution.bytesInOutput > 0) inputs.add(input);
      }
      for (const entry of output.imports) {
        if (!entry.external && entry.kind !== "dynamic-import") pending.push(entry.path);
      }
    }
    return inputs;
  };
  // Cover Pi's entry, its bootstrap, the public agents entry and the actual manager chunk.
  // The manager can itself be lazy; its shared static closure must still exclude discovery.
  const initial = staticInputs(["dist/index.js", "dist/extension-bootstrap.js", "dist/agents.js",
    ...managerImports.map(entry => entry.from)]);
  expect(initial.has(source)).toBe(false);
  expect(staticInputs([stable]).has(source)).toBe(true);
});

it("keeps discovery unloaded through cold manager import and idle, then loads it once on actual first use", async () => {
  const { AgentManager } = await import("../src/agents/manager.js");
  const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
  expect(discoveryLoads).not.toHaveBeenCalled();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-archive-lazy-entry-"));
  const runRoot = path.join(root, "runs"), runId = "1".repeat(32), actorId = "lazy-archive-owner";
  const sessionFile = path.join(root, "session.jsonl"), directory = path.join(runRoot, runId);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(directory, "actor-run-archive-pending.json"),
    JSON.stringify({ format: 1, runId, actorId, sessionFile }), { mode: 0o600 });
  const manager = new AgentManager(root,
    { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, { runRoot });
  await new Promise<void>(resolve => setImmediate(resolve));
  expect(discoveryLoads).not.toHaveBeenCalled();
  for (let request = 0; request < 2; request++) {
    expect(await manager.actorArchiveSources(actorId, sessionFile, runId)).toEqual(new Map([[runId, directory]]));
    expect(discoveryLoads).toHaveBeenCalledTimes(1);
  }
  // Read-only discovery only: no commit, collection, source removal or fixture cleanup.
  expect(fs.existsSync(path.join(directory, "actor-run-archive-pending.json"))).toBe(true);
});
