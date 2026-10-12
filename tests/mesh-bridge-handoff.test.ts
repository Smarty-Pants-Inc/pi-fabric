import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_HANDOFF_PREFIX, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { BridgeHandoffRefusedError, handOffNativeIdentity, listHandoffs } from "../src/mesh/bridge-handoff.js";
import { MeshStore, type MeshEvent, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { participantFilePresent, writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#9074: a Main moved off the hub keeps its session id; the hub's handoff lets exactly one
// bridge link present it. Real MeshStores, the real bridge presence computation, a stdio transport;
// the handoff itself runs as move-main.sh runs it, the built CLI in its own process on the mesh root.
const roots: string[] = [];
const links: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of links.splice(0)) await close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const handoffKey = (id: string) => key(BRIDGE_HANDOFF_PREFIX, id);
const sessionOf = (name: string) => `${name}-0000-9074`;
/** No such pid exists on Linux (pid_max is at most 2^22). */
const DEAD_PID = 2_147_483_000;

interface NativeOptions { lifeMs?: number; mainProcess?: { pid: number; host: string; startTime?: string } }

// A file-only native root, as deployed on the fleet: host record in state, lease and participant in files.
const native = async (store: MeshStore, name: string, now: number, options: NativeOptions = {}) => {
  const sessionId = sessionOf(name);
  const id = `session:${sessionId}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId };
  const expiresAt = now + (options.lifeMs ?? 60_000);
  await store.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files", participants: "files" } });
  await store.put({ key: hostKey(id), identity, value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt });
  const entry: MeshStateEntry = { key: participantKey(id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name, label: name.toUpperCase(),
    status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"], sessionId, startedAt: now, updatedAt: now,
    controlProtocol: "v1", ...(options.mainProcess ? { mainProcess: options.mainProcess } : {}),
  } };
  writeParticipantFile(store.root, entry);
  return { id, sessionId, identity, entry };
};

const command = (targetId: string, replyTo: string) => ({
  version: 1, commandId: `c-${Math.random()}`, targetId, operation: "followUp", replyTo, message: "hi",
  requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, destinationRemoteHost: "ryzen1",
});
const on = (store: MeshStore, topic: string): MeshEvent[] => store.read({ after: 0, limit: 100 }).filter((event) => event.topic === topic);

// The hub ("ryzen1") bridged to one remote mesh over a stdio pair, as the ssh transport does.
const link = (hub: MeshStore, far: MeshStore, remoteName: string, root: string) => {
  const input = new PassThrough();
  const output = new PassThrough();
  const serving = serveBridgeAgent(new StoreBridgeSide(far, "ryzen1"), input, output);
  const remote = new RemoteBridgeSide(output, input);
  links.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  const logs: string[] = [];
  const local = new StoreBridgeSide(hub, remoteName);
  const bridge = new MeshBridge({ localName: "ryzen1", remoteName, local, remote, presenceMs: 0,
    cursorPath: path.join(root, `${remoteName}.cursor.json`), log: (message) => logs.push(message) });
  return { bridge, local, logs };
};

/**
 * The incident: "mover" ran on the hub and stopped (its records stay, its lease expired); it now runs
 * on the remote host qs32729 with the same session id. "lead" is a live hub Main.
 */
const fixture = async (options: { mainProcess?: NativeOptions["mainProcess"] } = {}) => {
  // Ten minutes back on the real clock: once the fixture is built, the frozen test clock is the real
  // time it started, so the built CLI (its own process, the real clock) sees the same dead and live leases.
  let now = Date.now() - 10 * 60_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-handoff-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
  const far = new MeshStore(path.join(root, "qs32729"), 64 * 1024, 100);
  const stale = await native(hub, "mover", now, options.mainProcess ? { mainProcess: options.mainProcess } : {});
  // One of its agents, left behind with it: it names the Main as root and owner.
  const child = `agent:${stale.sessionId}:worker`;
  writeParticipantFile(hub.root, { key: participantKey(child), version: 1, updatedAt: now, updatedBy: stale.identity, value: {
    format: 1, id: child, kind: "agent", rootId: stale.id, ownerHostId: stale.id, ownerIdentityId: stale.id, parentId: stale.id,
    name: "mover-worker", status: "completed", runner: "pi", transport: "host", capabilities: [], sessionId: "worker-0000-9074",
    startedAt: now, updatedAt: now,
  } });
  now += 10 * 60_000;
  const lead = await native(hub, "lead", now);
  const moved = await native(far, "mover", now);
  const qs = link(hub, far, "qs32729", root);
  return { root, hub, far, stale, child, lead, moved, ...qs, advance: (ms: number) => { now += ms; }, now: () => now };
};

/**
 * The built `mesh-bridge handoff` CLI (bin/mesh-bridge runs dist: build first; CI builds before its tests), as
 * move-main.sh runs it on the hub: a real process on real files, on the real clock. Its exit status and output.
 */
const cli = (...args: string[]) => new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
  const child = spawn(process.execPath, [path.resolve("bin/mesh-bridge"), ...args], { stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (code) => resolve({ code, stdout, stderr }));
});
/** One CLI run: its exit status, and its stdout parsed as the one JSON object it must be. */
const cliJson = async (...args: string[]) => {
  const run = await cli(...args);
  let json: Record<string, unknown>;
  try { json = JSON.parse(run.stdout) as Record<string, unknown>; }
  catch { throw new Error(`mesh-bridge ${args.join(" ")} printed no single JSON object (exit ${run.code}):\n${run.stdout}${run.stderr}`); }
  return { code: run.code, json, stderr: run.stderr };
};

const send = (f: Awaited<ReturnType<typeof fixture>>, far = f.far) =>
  far.publish({ topic: "fabric.control.command", kind: "followUp", from: f.moved.identity, to: f.lead.id, data: command(f.lead.id, f.moved.id) });

describe("hub handoff of a moved Main's native identity (smarty-dev#9074)", () => {
  it("refuses a moved Main while its dead native records stay on the hub, and mirrors it both ways once handed off to its link", async () => {
    const f = await fixture();
    await f.bridge.start();
    await f.bridge.step();
    // Regression: today's behaviour without a handoff. The dead native reserves the id.
    expect((await f.local.presence()).reserved).toEqual(expect.arrayContaining([f.stale.id, f.stale.sessionId, "mover"]));
    expect(f.hub.get(hostKey(f.stale.id))?.value).not.toHaveProperty("remoteHost");
    await send(f);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(on(f.hub, "fabric.control.command")).toHaveLength(0);
    expect(f.logs.join("\n")).toMatch(/sender claims a hub identity/);

    // The handoff as move-main.sh runs it on the hub: the built CLI, its own process, the hub's mesh root.
    const handed = await cliJson("handoff", "--mesh", f.hub.root, "--session", f.stale.sessionId, "--link", "qs32729", "--operator", "hosts-lead");
    expect(handed.code, handed.stderr).toBe(0);
    const result = handed.json as { record: { handedOffAt: number }; retired: string[] };
    expect(result.record).toEqual({ format: 1, id: f.stale.id, sessionId: f.stale.sessionId, name: "mover", link: "qs32729",
      handedOffAt: expect.any(Number), operator: "hosts-lead", hub: os.hostname() });
    // Stamped on the CLI's own (real) clock, after the fixture's.
    expect(result.record.handedOffAt).toBeGreaterThanOrEqual(f.now());
    expect(result.record.handedOffAt).toBeLessThan(f.now() + 60_000);
    expect(handed.json).toMatchObject({ ok: true, action: "handoff", id: f.stale.id, link: "qs32729",
      message: `native identity ${f.stale.id} handed off to link qs32729 at ${new Date(result.record.handedOffAt).toISOString()} by hosts-lead`,
      evidence: { id: f.stale.id, hub: os.hostname(), participants: [{ source: "file" }] } });
    expect(result.retired).toEqual([`participant file ${participantKey(f.stale.id)}`, `host record ${hostKey(f.stale.id)}`]);
    expect(participantFilePresent(f.hub.root, participantKey(f.stale.id))).toBe(false);
    expect(f.hub.get(hostKey(f.stale.id))).toBeUndefined();
    expect(f.hub.get(handoffKey(f.stale.id))?.value).toEqual(result.record);

    await f.bridge.step();
    // Its left-behind agent no longer reserves the Main's id on this link; its own ids stay reserved.
    expect(participantFilePresent(f.hub.root, participantKey(f.child))).toBe(true);
    const reserved = (await f.local.presence()).reserved;
    expect(reserved).not.toContain(f.stale.id);
    expect(reserved).toEqual(expect.arrayContaining([f.child, "mover-worker", "worker-0000-9074"]));
    expect(f.hub.get(hostKey(f.stale.id))?.value).toMatchObject({ id: f.stale.id, remoteHost: "qs32729" });
    expect(f.hub.get(participantKey(f.stale.id))?.value).toMatchObject({ id: f.stale.id, remoteHost: "qs32729", name: "mover" });
    // Hub Mains see it as a live remote participant, not a collision.
    const directory = new ParticipantDirectory(f.hub, { enabled: true, hostId: f.lead.id, rootId: f.lead.id, identity: f.lead.identity });
    expect(directory.get(f.stale.id)).toMatchObject({ id: f.stale.id, remoteHost: "qs32729", stale: false });

    await send(f);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(on(f.hub, "fabric.control.command")).toMatchObject([{ from: { id: f.stale.id }, to: f.lead.id }]);
    await f.hub.publish({ topic: "fleet.work.smarty-dev.9074", kind: "ask", from: f.lead.identity, to: f.stale.id, text: "welcome" });
    expect(await f.bridge.step()).toMatchObject({ toRemote: 1, dropped: 0 });
    expect(on(f.far, "fleet.work.smarty-dev.9074")).toMatchObject([{ to: f.stale.id, text: "welcome" }]);
  });

  it("keeps refusing the handed-off identity on every other link", async () => {
    const f = await fixture();
    await handOffNativeIdentity(f.hub, { session: f.stale.id, link: "qs32729", operator: "hosts-lead" });
    const intel = new MeshStore(path.join(f.root, "intel1"), 64 * 1024, 100);
    await native(intel, "mover", f.now());
    const other = link(f.hub, intel, "intel1", f.root);
    expect((await other.local.presence()).reserved).toEqual(expect.arrayContaining([f.stale.id, f.stale.sessionId, "mover"]));
    await other.bridge.start();
    await other.bridge.step();
    expect(f.hub.get(hostKey(f.stale.id))).toBeUndefined();
    await send(f, intel);
    expect(await other.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(other.logs.join("\n")).toMatch(/sender claims a hub identity/);
    // The link it was handed to still gets it, and the other link stays refused.
    await f.bridge.start();
    await f.bridge.step();
    expect(f.hub.get(hostKey(f.stale.id))?.value).toMatchObject({ remoteHost: "qs32729" });
    await other.bridge.step();
    expect(f.hub.get(hostKey(f.stale.id))?.value).toMatchObject({ remoteHost: "qs32729" });
    expect(on(f.hub, "fabric.control.command")).toHaveLength(0);
  });

  it.each(["file", "sqlite"] as const)("refuses the handoff while a live native holds the identity (%s state)", async (backend) => {
    const f = await fixture();
    const hub = backend === "file" ? f.hub : new MeshStore(path.join(f.root, "hub-sqlite"), 64 * 1024, 100, { stateBackend: "sqlite" });
    const live = await native(hub, "mover", f.now());
    const before = hub.listAll();
    const refused = handOffNativeIdentity(hub, { session: live.sessionId, link: "qs32729", operator: "hosts-lead" });
    await expect(refused).rejects.toBeInstanceOf(BridgeHandoffRefusedError);
    await expect(refused).rejects.toThrow(/native host lease is live until/);
    expect(hub.listAll()).toEqual(before);
    expect(hub.get(handoffKey(live.id))).toBeUndefined();
    expect(participantFilePresent(hub.root, participantKey(live.id))).toBe(true);
    // Once it is dead, the same store takes the handoff.
    f.advance(10 * 60_000);
    await expect(handOffNativeIdentity(hub, { session: live.sessionId, link: "qs32729", operator: "hosts-lead" }))
      .resolves.toMatchObject({ record: { id: live.id, link: "qs32729" } });
    expect(hub.get(hostKey(live.id))).toBeUndefined();
    if (backend === "sqlite") hub.closeState();
  });

  it.runIf(process.platform === "linux")("refuses while the recorded Main process is alive, even with every lease expired", async () => {
    const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
    const startTime = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]!;
    const f = await fixture({ mainProcess: { pid: process.pid, host: os.hostname(), startTime } });
    await expect(handOffNativeIdentity(f.hub, { session: f.stale.sessionId, link: "qs32729", operator: "hosts-lead" }))
      .rejects.toThrow(new RegExp(`Main process: process ${process.pid} is alive on this host`));
    expect(f.hub.get(handoffKey(f.stale.id))).toBeUndefined();
    // A gone (or reused) pid does not hold it.
    const gone = await fixture({ mainProcess: { pid: DEAD_PID, host: os.hostname(), startTime: "1" } });
    await expect(handOffNativeIdentity(gone.hub, { session: gone.stale.sessionId, link: "qs32729", operator: "hosts-lead" }))
      .resolves.toMatchObject({ evidence: { participants: [{ source: "file", process: `pid ${DEAD_PID} does not exist on this host` }] } });
  });

  it("voids the handoff when the native comes back live: it wins, and the void handoff is removed", async () => {
    const f = await fixture();
    await handOffNativeIdentity(f.hub, { session: f.stale.sessionId, link: "qs32729", operator: "hosts-lead" });
    await f.bridge.start();
    await f.bridge.step();
    expect(f.hub.get(hostKey(f.stale.id))?.value).toMatchObject({ remoteHost: "qs32729" });
    expect(listHandoffs(f.hub)).toMatchObject([{ handoff: { id: f.stale.id, link: "qs32729" }, inForce: true }]);

    await native(f.hub, "mover", f.now());
    expect(listHandoffs(f.hub)).toMatchObject([{ inForce: false }]);
    expect((await f.local.presence()).reserved).toEqual(expect.arrayContaining([f.stale.id, f.stale.sessionId]));
    await f.bridge.step();
    expect(f.hub.get(hostKey(f.stale.id))?.value).not.toHaveProperty("remoteHost");
    expect(f.hub.get(participantKey(f.stale.id))).toBeUndefined();
    expect(f.hub.get(handoffKey(f.stale.id))).toBeUndefined();
    await send(f);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
    expect(f.logs.join("\n")).toMatch(/sender claims a hub identity/);
    // And a new handoff is refused while it lives.
    await expect(handOffNativeIdentity(f.hub, { session: f.stale.sessionId, link: "qs32729", operator: "hosts-lead" }))
      .rejects.toThrow(/live/);
  });

  it("the built CLI prints one JSON result for move-main.sh: handed off, refused, listed and revoked", async () => {
    const f = await fixture();
    // Live on the CLI's real clock for the whole test.
    const live = await native(f.hub, "lead2", f.now(), { lifeMs: 10 * 60_000 });
    const refused = await cliJson("handoff", "--mesh", f.hub.root, "--session", live.sessionId, "--link", "qs32729", "--operator", "paul");
    expect(refused.code, refused.stderr).toBe(1);
    expect(refused.json).toMatchObject({ ok: false, action: "handoff", refused: expect.stringMatching(/live/),
      message: expect.stringMatching(new RegExp(`^refused: native identity ${live.id} not handed off to link qs32729: `)) });
    expect(f.hub.get(handoffKey(live.id))).toBeUndefined();
    const handed = await cliJson("handoff", "--mesh", f.hub.root, "--session", f.stale.sessionId, "--link", "qs32729", "--operator", "paul");
    expect(handed.code, handed.stderr).toBe(0);
    const at = (handed.json as { record: { handedOffAt: number } }).record.handedOffAt;
    expect(handed.json).toMatchObject({ ok: true, id: f.stale.id, link: "qs32729", record: { operator: "paul" },
      message: `native identity ${f.stale.id} handed off to link qs32729 at ${new Date(at).toISOString()} by paul` });
    const listed = await cliJson("handoff-list", "--mesh", f.hub.root);
    expect(listed.code, listed.stderr).toBe(0);
    expect(listed.json).toMatchObject({ ok: true, handoffs: [{ handoff: { id: f.stale.id, link: "qs32729" }, inForce: true }] });
    const revoked = await cliJson("handoff-revoke", "--mesh", f.hub.root, "--session", f.stale.sessionId);
    expect(revoked.code, revoked.stderr).toBe(0);
    expect(revoked.json).toMatchObject({ ok: true, message: `handoff of native identity ${f.stale.id} revoked` });
    expect(f.hub.get(handoffKey(f.stale.id))).toBeUndefined();
    const missing = await cliJson("handoff", "--mesh", path.join(f.root, "missing"), "--session", f.stale.sessionId, "--link", "qs32729");
    expect(missing).toMatchObject({ code: 2, json: { ok: false, message: `usage: Not a Fabric mesh directory: ${path.join(f.root, "missing")}` } });
    expect(fs.existsSync(path.join(f.root, "missing"))).toBe(false);
    expect(await cliJson("handoff", "--mesh", f.hub.root, "--session", "x", "--link", "qs32729"))
      .toMatchObject({ code: 2, json: { ok: false, message: "usage: Not a Main session id: x" } });
  });

  it("the built CLI answers a missing or malformed flag with the JSON usage result and exit 2, writing nothing", async () => {
    const f = await fixture();
    const before = f.hub.listAll();
    const synopsis = {
      handoff: "mesh-bridge handoff --mesh ROOT --session ID --link NAME [--operator NAME] [--lock-protocol 1|2]",
      "handoff-revoke": "mesh-bridge handoff-revoke --mesh ROOT --session ID [--lock-protocol 1|2]",
      "handoff-list": "mesh-bridge handoff-list --mesh ROOT [--lock-protocol 1|2]",
    } as const;
    const cases: Array<[keyof typeof synopsis, string[], string]> = [
      ["handoff", ["--mesh", f.hub.root, "--session", f.stale.sessionId, "--operator", "paul"], "--link is required"],
      ["handoff", ["--mesh", f.hub.root, "--link", "qs32729"], "--session is required"],
      ["handoff", ["--session", f.stale.sessionId, "--link", "qs32729"], "--mesh is required"],
      ["handoff", ["--mesh", f.hub.root, "--session", f.stale.sessionId, "--link"], "Bad argument: --link"],
      ["handoff-revoke", ["--mesh", f.hub.root], "--session is required"],
      ["handoff-list", [], "--mesh is required"],
      ["handoff-list", ["--mesh", f.hub.root, "--lock-protocol", "4"], "--lock-protocol must be 1, 2 or 3"],
    ];
    for (const [mode, flags, message] of cases) {
      const run = await cliJson(mode, ...flags);
      expect({ args: [mode, ...flags], code: run.code, json: run.json, stderr: run.stderr })
        .toEqual({ args: [mode, ...flags], code: 2, json: { ok: false, action: mode, message: `usage: ${message}`, usage: synopsis[mode] }, stderr: "" });
    }
    expect(f.hub.listAll()).toEqual(before);
    expect(participantFilePresent(f.hub.root, participantKey(f.stale.id))).toBe(true);
  });
});
