import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { MainAgentController } from "../src/main-agent.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import { mainInboxActive } from "../src/topology/stall-alarms.js";

// A genuinely foreign, live owner with a complete legacy receipt. Keep it alive until
// release; no shortened timeout, fake timers, or mocked timeout error in this regression.
const holderSource = `
const fs = require("node:fs"), path = require("node:path"), { randomUUID } = require("node:crypto");
const lock = path.join(process.argv[1], process.argv[2]), token = randomUUID();
fs.mkdirSync(lock, { recursive: true, mode: 0o700 });
fs.writeFileSync(path.join(lock, "owner"), token + "\\n" + process.pid + "\\n" + Date.now() + "\\n", { mode: 0o600 });
process.stdin.once("data", () => {
  const released = lock + ".released." + token;
  fs.renameSync(lock, released);
  fs.rmSync(released, { recursive: true });
  process.stdin.destroy();
});
process.stdout.write("ready\\n");
`;

const fixture = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-init-lock-retry-"));
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(base, "runs"));
  const meshRoot = path.join(base, "mesh"), sessionId = "custody-init-retry";
  const notify = vi.fn();
  const context = {
    cwd: base, hasUI: true, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getEntries: () => [], getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify },
  } as unknown as ExtensionContext;
  const pi = { on: () => () => {}, events: { emit: vi.fn() }, getThinkingLevel: () => "off",
    getSessionName: () => "custody-main", sendMessage: vi.fn(), appendEntry: vi.fn() } as unknown as ExtensionAPI;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(base, "unused-extension.mjs"), worker: path.join(base, "unused-worker.mjs"),
    residentHost: path.join(base, "unused-resident.mjs"), skills: base,
  } });
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
    residency: { enabled: false }, agents: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false }, speculation: { enabled: false },
  });
  const cleanup = async () => {
    try { await runtime.shutdown(); }
    finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(base, { recursive: true, force: true }); }
  };
  return { base, meshRoot, sessionId, notify, context, runtime, config, cleanup };
};

it.each(["custody.lock", ".lock"])("initializes under a live foreign %s timeout and recovers in the background", async lockName => {
  const f = fixture();
  const holder = spawn(process.execPath, ["-e", holderSource, f.meshRoot, lockName], { stdio: ["pipe", "pipe", "pipe"] });
  const exited = once(holder, "exit");
  void exited.catch(() => undefined);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const custody = vi.spyOn(MeshStore.prototype, "custody");
  const attachDrain = vi.spyOn(MainAgentController.prototype, "attachFollowUpDrain");
  let released = false;
  try {
    await once(holder.stdout, "data");
    expect(holder.pid).not.toBe(process.pid);
    const owner = fs.readFileSync(path.join(f.meshRoot, lockName, "owner"), "utf8");
    expect(Number(owner.split("\n")[1])).toBe(holder.pid);
    const started = Date.now();
    // Failing-first on 0d592311: custody.lock rejects initialize before the warning/retry path.
    await expect(f.runtime.initialize(f.context, f.config)).resolves.toBeUndefined();
    expect(Date.now() - started).toBeGreaterThanOrEqual(10_000);
    expect(fs.readFileSync(path.join(f.meshRoot, lockName, "owner"), "utf8")).toBe(owner);
    process.kill(holder.pid!, 0); // The receipt is not a recoverable dead holder.
    expect(f.runtime.initialized).toBe(true);
    expect(f.runtime.execution).toBeDefined();
    expect(attachDrain).not.toHaveBeenCalled();
    const initialWarnings = () => warn.mock.calls.filter(([message]) => String(message).includes("Initial mesh publish failed"));
    expect(initialWarnings()).toHaveLength(1);
    expect(f.notify).toHaveBeenCalledExactlyOnceWith(expect.stringMatching(/could not reach the mesh.*retrying in the background/), "warning");
    expect(readHostLeases(f.meshRoot).has(`session:${f.sessionId}`)).toBe(false);
    // No explicit refresh/ensure/reload: a heartbeat retries the pending activation itself.
    await vi.waitFor(() => expect(custody.mock.calls.length).toBeGreaterThanOrEqual(2), { timeout: 8_000, interval: 25 });
    holder.stdin.end("release\n");
    await exited;
    released = true;
    await vi.waitFor(() => expect(readHostLeases(f.meshRoot).get(`session:${f.sessionId}`)).toMatchObject({
      identityId: `session:${f.sessionId}`, rootId: `session:${f.sessionId}`,
    }), { timeout: 8_000, interval: 50 });
    expect(mainInboxActive(f.meshRoot, `session:${f.sessionId}`)).toBe(true);
    expect(attachDrain).toHaveBeenCalledOnce();
    expect(f.runtime.participantInfos({ kinds: ["root"], fresh: true })).toMatchObject([
      { id: `session:${f.sessionId}`, name: "custody-main", stale: false },
    ]);
    expect(initialWarnings()).toHaveLength(1);
    expect(f.notify).toHaveBeenCalledTimes(1);
  } finally {
    if (!released) {
      holder.stdin.end("release\n");
      const kill = setTimeout(() => holder.kill(), 2_000);
      try { await exited; } finally { clearTimeout(kill); }
    }
    await f.cleanup();
  }
}, 40_000);

it("still rejects non-contention inbox activation errors", async () => {
  const f = fixture();
  try {
    vi.spyOn(MeshStore.prototype, "custody").mockRejectedValueOnce(new Error("inbox storage failure"));
    await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow("inbox storage failure");
    expect(f.notify).not.toHaveBeenCalled();
  } finally { await f.cleanup(); }
});
