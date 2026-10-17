import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import type { FabricState } from "../src/fabric-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory, type FabricPeerListOptions } from "../src/topology/participant-directory.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import * as hostLeases from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import * as participantFiles from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { FabricUiController } from "../src/ui/controller.js";

// Keep reason: #13895 real Main display performance path, plus the display/authority trust boundary.
const NOW = 1_800_000_000_000;
const roots: string[] = [];
const stores: MeshStore[] = [];
const controllers: FabricUiController[] = [];
afterEach(() => {
  for (const controller of controllers.splice(0)) controller.stop();
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const identity = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });

const fixture = async (persistent?: string) => {
  vi.spyOn(Date, "now").mockReturnValue(NOW);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "dashboard-directory-"));
  roots.push(root);
  if (persistent && fs.existsSync(persistent)) {
    for (const [file, bytes] of Object.entries(JSON.parse(fs.readFileSync(persistent, "utf8")) as Record<string, string>)) {
      fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
      fs.writeFileSync(path.join(root, file), Buffer.from(bytes, "base64"));
    }
  }
  const mesh = new MeshStore(root, 256 * 1024, 1000);
  stores.push(mesh);
  const self = identity("self");
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: self.id, rootId: self.id, identity: self,
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false });
  const rows: FabricParticipantRecord[] = [];
  const add = (name: string, patch: Partial<FabricParticipantRecord> = {}) => {
    const owner = identity(name);
    rows.push({ format: 1, id: owner.id, kind: "root", rootId: owner.id, ownerHostId: owner.id, ownerIdentityId: owner.id,
      name: "main", status: "idle", runner: "pi", transport: "host", capabilities: ["fabric", "steer"],
      cwd: "/project", sessionId: name, startedAt: rows.length + 1, updatedAt: NOW, controlProtocol: "v1", ...patch });
  };
  add("self", { label: "Peer 1" });
  add("native", { label: "Peer 2", model: "model", thinking: "high", role: "review", project: "project", interactive: true });
  add("named", { name: "Reviewer", label: "Peer 3", status: "running", pendingMessages: true });
  add("remote", { remoteHost: "forge", label: "Peer 2" });
  add("unnamed-long-session");
  add("stopping", { status: "stopping" });
  add("reloading", { status: "reloading", reloadUntil: NOW + 60_000 });
  add("no-cwd", { cwd: "" });
  add("no-session", { sessionId: "" });
  add("terminal", { status: "completed" });
  add("child", { kind: "agent" });
  for (let n = 0; n < 50; n++) add(`stale-${n}`);
  if (!fs.existsSync(path.join(root, "fixture-ready"))) {
    for (const row of rows) {
      await mesh.put({ identity: identity(row.id.slice(8)), key: key("topology/hosts/", row.ownerHostId),
        value: { format: 1, id: row.ownerHostId, rootId: row.rootId, identity: identity(row.id.slice(8)),
          startedAt: row.startedAt, updatedAt: NOW, expiresAt: row.id.includes(":stale-") ? NOW - 1 : NOW + 120_000,
          ...(row.remoteHost ? { remoteHost: row.remoteHost } : {}) } });
      writeParticipantFile(root, { key: key("topology/participants/", row.id), value: row, version: 1,
        updatedAt: NOW, updatedBy: identity(row.id.slice(8)) });
      writeHostLease(root, { id: row.ownerHostId, rootId: row.rootId, identityId: row.ownerIdentityId,
        startedAt: row.startedAt, updatedAt: NOW, expiresAt: row.id.includes(":stale-") ? NOW - 1 : NOW + 120_000 });
    }
    fs.writeFileSync(path.join(root, "fixture-ready"), "61 rows; 50 stale\n");
    if (persistent) {
      const files: Record<string, string> = {};
      const capture = (dir: string) => { for (const name of fs.readdirSync(path.join(root, dir))) {
        const file = path.join(dir, name);
        if (fs.statSync(path.join(root, file)).isDirectory()) capture(file);
        else files[file] = fs.readFileSync(path.join(root, file)).toString("base64");
      } };
      capture("");
      fs.writeFileSync(persistent, JSON.stringify(files));
    }
  }
  vi.spyOn(directory, "list"); // Install before binding the host adapter, so both entry paths count.
  // The facade/runtime forward this options object unchanged. Directory owns self rootId,
  // not the presentation alias deliberately returned by this host adapter.
  const state = {
    initialized: true, config: { ui: { enabled: true, widget: "hidden", refreshMs: 1000 }, mesh: { enabled: false } },
    activity: { runs: () => [], subscribe: () => () => {} },
    agents: { list: () => [], subscribeUi: () => () => {} },
    actors: { list: () => [], subscribe: () => () => {} }, globalActors: { list: () => [] },
    participantInfos: directory.list.bind(directory),
    peerInfos: (options: FabricPeerListOptions) => directory.peers(undefined, options),
    mainAgentInfo: () => ({ id: "main", name: "Main", status: "idle", local: true }),
  } as unknown as FabricState;
  const controller = new FabricUiController(state);
  controllers.push(controller);
  controller.start({ mode: "tui", isIdle: () => true, ui: { setWidget: () => {} } } as unknown as ExtensionContext);
  const refresh = () => controller.setHostStreaming(false);
  return { root, mesh, directory, controller, refresh, rows };
};

it("uses one directory traversal per idle Main controller refresh over 50 stale rows", async () => {
  const f = await fixture(process.env.DISPLAY_13895_FIXTURE);
  const standalone = f.directory.peers(undefined, { background: true });
  expect(f.controller.snapshot().peers).toEqual(standalone);
  expect(standalone.map(p => [p.id, p.name, p.label, p.status])).toEqual([
    ["session:native", "Peer 2", "Peer 2", "idle"],
    ["session:named", "Reviewer", "Peer 3", "running"],
    ["session:remote", "Peer 2@forge", "Peer 2@forge", "idle"],
    ["session:unnamed-long-session", "Peer unnamed-", undefined, "idle"],
    ["session:stopping", "Peer stopping", undefined, "idle"],
    ["session:reloading", "Peer reloadin", undefined, "idle"],
  ]);
  expect(standalone[0]).toEqual({ id: "session:native", name: "Peer 2", label: "Peer 2", role: "review", project: "project",
    interactive: true, kind: "peer", status: "idle", runner: "pi", transport: "host", cwd: "/project", sessionId: "native",
    model: "model", thinking: "high", startedAt: 2, updatedAt: NOW, pendingMessages: false, local: false });
  expect(standalone[2]).toEqual({ id: "session:remote", name: "Peer 2@forge", label: "Peer 2@forge", host: "forge",
    kind: "peer", status: "idle", runner: "pi", transport: "host", cwd: "/project", sessionId: "remote",
    startedAt: 4, updatedAt: NOW, pendingMessages: false, local: false });
  const listings = vi.spyOn(f.directory, "list");
  const hosts = vi.spyOn(hostLeases, "readHostLeases");
  const files = vi.spyOn(participantFiles, "readParticipantFiles");
  const stats = vi.spyOn(fs, "statSync");
  for (let n = 0; n < 20; n++) f.refresh(); // warm identical state and JIT
  listings.mockClear(); hosts.mockClear(); files.mockClear(); stats.mockClear();
  const samples: number[] = [];
  for (let batch = 0; batch < 5; batch++) {
    const cpu = process.cpuUsage();
    for (let n = 0; n < 20; n++) f.refresh();
    const used = process.cpuUsage(cpu);
    samples.push((used.user + used.system) / 20);
  }
  const hostStats = stats.mock.calls.filter(([file]) => String(file).includes(`${path.sep}host-leases${path.sep}`) && String(file).endsWith(".json")).length;
  const participantStats = stats.mock.calls.filter(([file]) => String(file).includes(`${path.sep}participants${path.sep}`) && String(file).endsWith(".json")).length;
  console.log(JSON.stringify({ scope: "real Main controller refresh; directory/mesh real; other domains empty host adapters",
    repeats: 100, cpuUsPerRefreshN20: samples, listPassesPerRefresh: listings.mock.calls.length / 100,
    hostReadPassesPerRefresh: hosts.mock.calls.length / 100, participantFileReadPassesPerRefresh: files.mock.calls.length / 100,
    hostFileStatsPerRefresh: hostStats / 100, participantFileStatsPerRefresh: participantStats / 100,
    snapshotSha256: createHash("sha256").update(JSON.stringify(f.controller.snapshot())).digest("hex") }));
  expect(f.controller.snapshot().peers).toEqual(standalone);
  expect(hostStats / 100).toBe(f.rows.length);
  expect(listings).toHaveBeenCalledTimes(100);
});

it("does not let a display projection weaken fresh owner, lease or bound routing reads", async () => {
  const f = await fixture();
  const tokens = vi.spyOn(f.mesh, "stateToken");
  f.refresh();
  expect(tokens.mock.calls.some(([read]) => read?.displayOnly)).toBe(true);
  tokens.mockClear();
  expect(f.directory.get("session:native", NOW, { fresh: true })).toMatchObject({ stale: false });
  expect(tokens.mock.calls.every(([read]) => read?.displayOnly !== true)).toBe(true);
  const native = f.rows[1]!;
  await f.mesh.put({ key: key("topology/hosts/", native.ownerHostId), identity: identity("native"), value: {
    format: 1, id: native.ownerHostId, rootId: "session:changed-owner", identity: identity("native"),
    startedAt: native.startedAt, updatedAt: NOW + 1, expiresAt: NOW + 120_000 } });
  f.refresh();
  expect(f.controller.snapshot().peers.some(p => p.id === native.id)).toBe(false);
  expect(f.directory.get(native.id, NOW, { fresh: true })).toBeUndefined();
  const named = f.rows[2]!;
  await f.mesh.put({ key: key("topology/hosts/", named.ownerHostId), identity: identity("named"), value: {
    format: 1, id: named.ownerHostId, rootId: named.rootId, identity: identity("named"),
    startedAt: named.startedAt, updatedAt: NOW - 100, expiresAt: NOW - 1 } });
  f.refresh();
  expect(f.controller.snapshot().peers.some(p => p.id === named.id)).toBe(true); // file lease alone keeps it live
  const file = hostLeasePath(f.root, named.ownerHostId);
  const lease = JSON.parse(fs.readFileSync(file, "utf8"));
  // In-place rewrite, no parent directory stamp change: file stats must remain fresh.
  fs.writeFileSync(file, JSON.stringify({ ...lease, updatedAt: NOW + 1, expiresAt: NOW - 1 }));
  f.refresh();
  expect(f.controller.snapshot().peers.some(p => p.id === named.id)).toBe(false);
  expect(f.directory.get(named.id, NOW, { fresh: true })).toBeUndefined();
});
