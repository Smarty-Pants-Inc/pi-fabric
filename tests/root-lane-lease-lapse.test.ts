/**
 * smarty-dev#16357 follow-up: a resident resolves its bound root's lane (basename of the root's
 * cwd) through a fresh participant lookup that needs the root's owner host lease LIVE. A busy
 * Main renews late, so the lease lapses for seconds and actors were blocked `lane-unresolved`.
 * The lookup now falls back to the SAME root's last-known record for a lapse of at most
 * ROOT_LANE_LEASE_LAPSE_GRACE_MS when that root's route is not ended.
 *
 * Real directories on a private mesh: a Main publishes the root; the ResidentHost's
 * AgentManager reads the lane (exceptionLaneCapture -> #rootLane -> rootSessionCwd).
 * Date.now is moved past the root's lease expiry only inside one synchronous read, so no
 * background renewal can interleave.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ROOT_LANE_LEASE_LAPSE_GRACE_MS, boundRootSessionCwd, type RootSessionCwdSource } from "../src/topology/root-session-cwd.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

beforeEach(() => installInProcessResidentFence());

const ROOT_ID = "session:lapse-root";
const OTHER_ID = "session:lapse-other";
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  while (cleanups.length) await cleanups.pop()!();
});

const rootRecord = (id: string, sessionId: string, cwd: string) => ({
  format: 1 as const, id, kind: "root" as const, rootId: id, ownerHostId: id, ownerIdentityId: id,
  name: "Main", status: "idle" as const, residency: "session" as const, runner: "pi" as const, transport: "host" as const,
  capabilities: [], sessionId, cwd, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" as const,
});

/** A Main publishing root `id` (cwd .../<lane>) on `meshRoot`. */
const startMain = async (meshRoot: string, id: string, lane: string) => {
  const sessionId = id.slice("session:".length);
  const identity = { id, name: "Main", kind: "main" as const, sessionId };
  const mesh = new MeshStore(meshRoot, 65536, 1000);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity });
  directory.registerSource(() => [rootRecord(id, sessionId, path.join(os.tmpdir(), "lanes", lane))]);
  await directory.start();
  cleanups.push(() => directory.close());
  return { directory, mesh, identity };
};

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lane-lapse-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: ROOT_ID, sessionId: "lapse-root",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot,
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, ROOT_ID),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config, vi.fn());
  cleanups.push(() => host.close().catch(() => undefined));
  return { config, host, meshRoot };
};

type Host = Awaited<ReturnType<typeof fixture>>["host"];

/** The root's effective owner lease expiry, read from the directory's own lapse arithmetic. */
const leaseExpiry = (host: Host, id: string): number => {
  const probe = Date.now() + 10 * 60_000;
  const known = host.participants.lastKnown(id, probe);
  expect(known?.participant).toMatchObject({ id, kind: "root", stale: true });
  expect(Number.isFinite(known!.lapsedMs)).toBe(true);
  return probe - known!.lapsedMs;
};

/** The lane the resident's AgentManager resolves when the root lease lapsed `lapseMs` ago. */
const laneAfterLapse = (host: Host, id: string, lapseMs: number) => {
  const at = leaseExpiry(host, id) + lapseMs;
  const now = vi.spyOn(Date, "now").mockReturnValue(at);
  try {
    expect(host.participants.get(id, undefined, { fresh: true })).toBeUndefined(); // the fresh lookup alone misses it
    return host.agents.exceptionLaneCapture();
  } finally { now.mockRestore(); }
};

describe("resident bound-root lane across a short root lease lapse (smarty-dev#16357)", () => {
  it("resolves the lane from the root's own last-known record 10 s after its lease lapsed", async () => {
    const { host, meshRoot } = await fixture();
    await startMain(meshRoot, ROOT_ID, "lapse-lane");
    await host.start();
    expect(laneAfterLapse(host, ROOT_ID, 10_000)).toEqual({ rootId: ROOT_ID, lane: "lapse-lane" });
  });

  it("stays unresolved once the lease lapsed 120 s ago (beyond the grace)", async () => {
    const { host, meshRoot } = await fixture();
    await startMain(meshRoot, ROOT_ID, "lapse-lane");
    await host.start();
    expect(ROOT_LANE_LEASE_LAPSE_GRACE_MS).toBe(60_000);
    expect(laneAfterLapse(host, ROOT_ID, 120_000)).toBeUndefined();
    expect(laneAfterLapse(host, ROOT_ID, ROOT_LANE_LEASE_LAPSE_GRACE_MS + 1)).toBeUndefined();
  });

  it("stays unresolved for an ended (closed) root, even 10 s after the lapse", async () => {
    const { host, meshRoot } = await fixture();
    const main = await startMain(meshRoot, ROOT_ID, "lapse-lane");
    await host.start();
    // The root's own clean-close receipt, the closure get() treats as an ended route.
    const key = `topology/lineage-closures/${createHash("sha256").update(ROOT_ID).digest("hex")}`;
    await main.mesh.put({ key, identity: main.identity,
      value: { format: 1, rootId: ROOT_ID, ownerHostId: ROOT_ID, ownerIdentityId: ROOT_ID, closedAt: Date.now() } });
    expect(laneAfterLapse(host, ROOT_ID, 10_000)).toBeUndefined();
    expect(host.agents.exceptionLaneCapture()).toBeUndefined(); // and live: get() already refuses it
  });

  it("never uses another participant's record", async () => {
    const { host, meshRoot } = await fixture();
    await startMain(meshRoot, OTHER_ID, "other-lane"); // the bound root has no record at all
    await host.start();
    const at = leaseExpiry(host, OTHER_ID) + 10_000;
    const now = vi.spyOn(Date, "now").mockReturnValue(at);
    try { expect(host.agents.exceptionLaneCapture()).toBeUndefined(); }
    finally { now.mockRestore(); }
    // A source answering lastKnown with another record (wrong id or not a root) is refused too.
    const other = { id: OTHER_ID, kind: "root", cwd: "/x/other-lane" } as FabricParticipantInfo;
    const source = (participant: FabricParticipantInfo): RootSessionCwdSource => ({
      get: () => undefined, lastKnown: () => ({ participant, lapsedMs: 1_000 }), routeEnded: () => false,
    });
    expect(boundRootSessionCwd(source(other), ROOT_ID)).toBeUndefined();
    expect(boundRootSessionCwd(source({ ...other, id: ROOT_ID, kind: "actor" }), ROOT_ID)).toBeUndefined();
    expect(boundRootSessionCwd(source({ ...other, id: ROOT_ID }), ROOT_ID)).toBe("/x/other-lane");
    expect(boundRootSessionCwd({ ...source({ ...other, id: ROOT_ID }), routeEnded: () => true }, ROOT_ID)).toBeUndefined();
    expect(boundRootSessionCwd({ ...source({ ...other, id: ROOT_ID }), lastKnown: () => { throw new Error("EIO"); } }, ROOT_ID))
      .toBeUndefined();
  });

  it("keeps the live-lease path unchanged and never consults lastKnown for it", async () => {
    const { host, meshRoot } = await fixture();
    await startMain(meshRoot, ROOT_ID, "live-lane");
    await host.start();
    const lastKnown = vi.spyOn(host.participants, "lastKnown");
    expect(host.agents.exceptionLaneCapture()).toEqual({ rootId: ROOT_ID, lane: "live-lane" });
    expect(lastKnown).not.toHaveBeenCalled();
    // A live record that is not this root's (no cwd) stays unresolved without a fallback.
    const live = { id: ROOT_ID, kind: "root" } as FabricParticipantInfo;
    const fallback = vi.fn();
    expect(boundRootSessionCwd({ get: () => live, lastKnown: fallback, routeEnded: () => false }, ROOT_ID)).toBeUndefined();
    expect(fallback).not.toHaveBeenCalled();
  });
});
