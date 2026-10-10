import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";

// smarty-dev#6997: /fabric reload and bootstrap replacement (#closeInternal) drained the mesh users
// and dropped the MeshStore without closing its state backend, leaking the SQLite handle.

const probe: MeshIdentity = { id: "backend-close-probe", name: "probe", kind: "agent" };
const roots: string[] = [];
afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** Open descriptors of this process on `file` (Linux /proc only; undefined elsewhere). */
const openHandles = (file: string): number | undefined => {
  if (process.platform !== "linux") return undefined;
  let count = 0;
  for (const fd of fs.readdirSync("/proc/self/fd")) {
    try { if (fs.readlinkSync(path.join("/proc/self/fd", fd)) === file) count += 1; } catch { /* closed meanwhile */ }
  }
  return count;
};

const fixture = async () => {
  const cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-runtime-backend-close-")));
  roots.push(cwd);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
  vi.stubEnv("PI_FABRIC_MESH_ROOT", path.join(cwd, ".pi", "fabric", "mesh"));
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
    sessionManager: { getSessionId: () => "backend-close", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const config = normalizeFabricConfig({
    mesh: { enabled: true, stateBackend: "sqlite" }, mcp: { enabled: false, cache: { enabled: false } },
    agents: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
  const unused = path.join(cwd, "unused.mjs");
  fs.writeFileSync(unused, "export default {};");
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: unused, worker: unused, residentHost: unused, skills: cwd } });
  await runtime.initialize(context, config);
  return { runtime, context, config };
};

describe("FabricRuntimeState closes the state backend on reload (smarty-dev#6997)", () => {
  it("bootstrap replacement closes the previous SQLite backend and its database handle before dropping the store", async () => {
    const { runtime, context, config } = await fixture();
    try {
      const first = runtime.mesh;
      expect(first.stateBackend).toBe("sqlite");
      const database = path.join(first.root, "state.db");
      // Use the backend so the database handle is actually open.
      await first.put({ key: "reload/probe", value: 1, identity: probe });
      expect(first.get("reload/probe", { fresh: true })?.value).toBe(1);
      const backend = first.stateBackendHandle;
      const close = vi.spyOn(backend, "close");
      const before = openHandles(database);
      if (before !== undefined) expect(before).toBeGreaterThan(0);

      // A second initialize() is the reload / bootstrap replacement path (#closeInternal).
      await runtime.initialize(context, config);
      const second = runtime.mesh;
      expect(second).not.toBe(first);
      expect(close).toHaveBeenCalled();
      // The old backend is closed for good: it cannot serve (or reopen) the database.
      expect(() => first.get("reload/probe", { fresh: true })).toThrow(/closed/);
      // The new store reads what the old one wrote.
      expect(second.get("reload/probe", { fresh: true })?.value).toBe(1);
      const secondClose = vi.spyOn(second.stateBackendHandle, "close");
      await runtime.shutdown();
      expect(secondClose).toHaveBeenCalled();
      // Every handle of this process on state.db is released once both stores are gone.
      const after = openHandles(database);
      if (after !== undefined) expect(after).toBe(0);
    } finally { await runtime.shutdown(); }
  }, 60_000);
});
