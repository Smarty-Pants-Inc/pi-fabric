import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";

// Review round 2 on pi-fabric#160: a Main that accepted a removal behind a run (a same-name
// create) and restarted before the run ended must finish that removal at its next start.
const runtimeAt = (cwd: string) => {
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn(), on: vi.fn() } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    model: { provider: "dest", id: "old", contextWindow: 48_000 },
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "t" }), refresh: async () => ({}) },
    sessionManager: { getSessionId: () => "removal-restart", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"), residentHost: path.join(cwd, "unused.mjs"), skills: cwd,
  } });
  const config = normalizeFabricConfig({ fullCodeMode: false, agents: { enabled: true, budgetUsd: 0 }, mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, mesh: { enabled: true, root: path.join(cwd, "mesh") }, prewalk: { enabled: false, alwaysRearm: false } });
  return { runtime, init: () => runtime.initialize(context, config) };
};

const registries = (dir: string): string[] => fs.readdirSync(dir, { recursive: true, encoding: "utf8" })
  .filter((entry) => path.basename(entry) === "actors.json").map((entry) => path.join(dir, entry));

it("a restarted Main finishes the removal it accepted before its restart", async () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-removal-restart-"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  for (const name of ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID"]) vi.stubEnv(name, undefined);
  try {
    const first = runtimeAt(cwd);
    await first.init();
    const old = await first.runtime.actors.create({ name: "reviewer", instructions: "Review.", responseMode: "text" });
    await first.runtime.shutdown();
    // What a Main that died mid-removal leaves: the stopped predecessor with its saved marker.
    const file = registries(path.join(cwd, "mesh")).find((candidate) => fs.readFileSync(candidate, "utf8").includes(old.id))!;
    const registry = JSON.parse(fs.readFileSync(file, "utf8")) as { actors: Array<Record<string, unknown>> };
    const record = registry.actors.find((entry) => entry.id === old.id)!;
    Object.assign(record, { status: "stopped", removal: { requestedAt: Date.now() - 5_000, runId: "c".repeat(32) } });
    fs.writeFileSync(file, JSON.stringify(registry));

    const second = runtimeAt(cwd);
    await second.init();
    try {
      await vi.waitFor(() => expect(second.runtime.actors.list().map((actor) => actor.id)).not.toContain(old.id), { timeout: 5_000 });
      const successor = await second.runtime.actors.create({ name: "reviewer", instructions: "Review.", responseMode: "text" });
      expect(second.runtime.actors.list().map((actor) => actor.id)).toEqual([successor.id]);
      expect(fs.readFileSync(file, "utf8")).not.toContain(old.id);
    } finally {
      await second.runtime.shutdown();
    }
  } finally {
    vi.unstubAllEnvs();
    fs.rmSync(cwd, { recursive: true, force: true });
  }
}, 30_000);
