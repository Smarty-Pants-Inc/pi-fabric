// Keep: reload fencing is a data-safety boundary; stale startup receipts must never regain lease authority.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { startingHost, type StartingHostClaim } from "../src/topology/starting-host.js";
import { readHostLeaseCurrent, renewHostLease, FabricHostLeaseSupersededError } from "../src/topology/host-leases.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
  vi.unstubAllEnvs();
});

const fixture = () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "starting-main-reload-"));
  const meshRoot = path.join(cwd, "mesh");
  for (const name of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_OWNER_HOST_ID", "PI_FABRIC_MAIN_AGENT_ID",
    "PI_FABRIC_SESSION_ID", "PI_FABRIC_PROJECT_ROOT"]) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
    components: [], mesh: { enabled: true, announce: true, root: meshRoot, stateBackend: "file", lockProtocol: 1 },
  }));
  const sessionId = randomUUID(), id = `session:${sessionId}`;
  const context = { cwd, isProjectTrusted: () => true, sessionManager: { getSessionId: () => sessionId },
    ui: { setStatus: vi.fn() } } as unknown as ExtensionContext;
  const identity: MeshIdentity = { id, kind: "main", name: "reload-test" };
  const mesh = new MeshStore(meshRoot, 65_536, 1_000);
  const received: Array<StartingHostClaim | undefined> = [];
  let failures = 0;
  class Runtime {
    initialized = false;
    directory?: ParticipantDirectory;
    async initialize(_context: ExtensionContext, _config: unknown, options: { startingHost?: StartingHostClaim }) {
      received.push(options.startingHost);
      if (failures > 0) { failures--; throw new Error("injected first initialize failure"); }
      if (this.directory) { await this.directory.quiesce("reload"); await this.directory.close(); }
      const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity,
        heartbeatMs: 60_000, leaseMs: 120_000, startingHost: options.startingHost });
      this.directory = directory;
      directory.registerSource(() => [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id,
        ownerIdentityId: id, name: "reload-test", status: "idle", runner: "pi", transport: "process",
        capabilities: ["steer", "followUp", "fabric"], cwd, sessionId, startedAt: 1, updatedAt: 2,
        pendingMessages: false, controlProtocol: "v1" }]);
      await directory.start();
      await directory.finishStartup();
      this.initialized = true;
    }
    async shutdown() { await this.directory?.close(); this.initialized = false; }
  }
  const state = new FabricState({} as ExtensionAPI, new CapturedToolCatalog(), {
    runtimeLoader: (async () => ({ FabricRuntimeState: Runtime })) as never,
  });
  cleanups.push(async () => { await state.shutdown(); fs.rmSync(cwd, { recursive: true, force: true }); });
  return { state, context, mesh, meshRoot, id, received, fail: () => { failures++; } };
};

describe("FabricState consumes the native startup handoff once", () => {
  it("adopts the early token/epoch once, then public-style initialize fences both and cannot resurrect startup", async () => {
    const f = fixture();
    await f.state.claimSessionStart(f.context);
    const early = readHostLeaseCurrent(f.meshRoot, f.id)!;
    expect(early.startup).toBeDefined();
    await f.state.initialize(f.context);
    const first = readHostLeaseCurrent(f.meshRoot, f.id)!;
    expect(first.incarnationToken).toBe(early.incarnationToken);
    expect(first.ownerIncarnation).toBe(early.ownerIncarnation);
    expect(first.startup).toBeUndefined();
    await f.state.initialize(f.context); // /fabric reload: no session_start event
    const next = readHostLeaseCurrent(f.meshRoot, f.id)!;
    expect(next.incarnationToken).not.toBe(first.incarnationToken);
    expect(next.ownerIncarnation).not.toBe(first.ownerIncarnation);
    expect(f.received[1]).toBeUndefined();
    expect(next.startup).toBeUndefined();
    expect(startingHost(f.meshRoot, f.id)).toBeUndefined();
    await expect(renewHostLease(f.mesh, { ...early, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 }))
      .rejects.toBeInstanceOf(FabricHostLeaseSupersededError);
  });

  it("retains a failed first initialize's exact early receipt for retry", async () => {
    const f = fixture();
    await f.state.claimSessionStart(f.context);
    const early = readHostLeaseCurrent(f.meshRoot, f.id)!;
    f.fail();
    await expect(f.state.initialize(f.context)).rejects.toThrow("injected first initialize failure");
    await f.state.initialize(f.context);
    expect(f.received[1]).toBe(f.received[0]);
    expect(readHostLeaseCurrent(f.meshRoot, f.id)).toMatchObject({
      incarnationToken: early.incarnationToken, ownerIncarnation: early.ownerIncarnation,
    });
  });

  it("ordinary same-process session_start takes the normal fresh path", async () => {
    const f = fixture();
    await f.state.initialize(f.context);
    const first = readHostLeaseCurrent(f.meshRoot, f.id)!;
    await f.state.claimSessionStart(f.context);
    await f.state.initialize(f.context);
    const next = readHostLeaseCurrent(f.meshRoot, f.id)!;
    expect(f.received).toEqual([undefined, undefined]);
    expect(next.incarnationToken).not.toBe(first.incarnationToken);
    expect(next.ownerIncarnation).not.toBe(first.ownerIncarnation);
    expect(next.startup).toBeUndefined();
  });
});
