// Real native runtime/process; only the Pi API boundary is synthetic. No graceful
// shutdown: the parent stops heartbeats with SIGSTOP, then checks native SIGKILL exit.
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { pathToFileURL } from "node:url";
// A local git-archive of the actual merge-base runtime exercises mixed-version admission.
const legacy = process.argv[4] === "legacy";
const source = legacy ? process.env.PI_FABRIC_PRUNE_LEGACY_SOURCE : undefined;
if (legacy && !source) {
  // Previous native admission had neither a lifetime fence nor root-bound identity.
  // Keep a fully initialized real Main; omit only that new admission protocol.
  const builtin: string = "bun:test";
  const { mock } = await import(builtin) as { mock: { module: (file: string, factory: () => object) => void } };
  mock.module(path.resolve("src/residency/main-startup-fence.ts"), () => ({
    acquireNativeMainStartupFence: async () => () => {},
  }));
}
const load = (file: string) => import(source ? pathToFileURL(path.join(source, "src", file)).href : `../../src/${file}`);
const { FabricRuntimeState } = await load("fabric-runtime-state.js") as typeof import("../../src/fabric-runtime-state.js");
const { CapturedToolCatalog } = await load("capture/catalog.js") as typeof import("../../src/capture/catalog.js");
const { normalizeFabricConfig } = await load("config.js") as typeof import("../../src/config.js");

const [root, meshRoot] = process.argv.slice(2) as [string, string];
process.env.PI_CODING_AGENT_DIR = path.join(root, "agent");
process.env.PI_FABRIC_PROJECT_ROOT = root;
process.env.PI_FABRIC_MESH_ROOT = meshRoot;
const noop = () => {};
const pi = { events: { emit: noop }, getThinkingLevel: () => "off", sendMessage: noop } as unknown as ExtensionAPI;
const context = { cwd: root, hasUI: false, isProjectTrusted: () => true, isIdle: () => true,
  hasPendingMessages: () => false, modelRegistry: { find: noop, getApiKeyAndHeaders: noop },
  sessionManager: { getSessionId: () => "old-main", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
  ui: { setStatus: noop, notify: noop } } as unknown as ExtensionContext;
const worker = path.resolve("tests/fixtures/fake-worker.mjs");
const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
  extension: worker, worker, residentHost: worker, skills: root } });
await runtime.initialize(context, normalizeFabricConfig({ fullCodeMode: true,
  mesh: { enabled: true, actorPollMs: 60_000 }, mcp: { enabled: false, cache: { enabled: false } },
  memory: { enabled: false }, jev: { enabled: false }, agents: { enabled: false },
  residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
console.log(JSON.stringify({ ready: true, initialized: runtime.initialized, pid: process.pid,
  actors: runtime.actors.listOwned().map(actor => actor.id) }));
setInterval(noop, 60_000);
