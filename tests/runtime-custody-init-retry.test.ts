import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createHash } from "node:crypto";
import * as custodyLocks from "../src/mesh/custody-lock.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { RootInbox, type RootInboxSession } from "../src/topology/root-inbox.js";
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
  return { base, meshRoot, sessionId, notify, pi, context, runtime, config, cleanup };
};

it.each(["custody.lock", ".lock"])("initializes under a live foreign %s timeout and recovers in the background", async lockName => {
  const f = fixture();
  const holder = spawn(process.execPath, ["-e", holderSource, f.meshRoot, lockName], { stdio: ["pipe", "pipe", "pipe"] });
  const exited = once(holder, "exit");
  void exited.catch(() => undefined);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const custody = vi.spyOn(custodyLocks, "acquireMeshCustodyLock");
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

it.each(["dual", "own"])("keeps lineage and every inbox pending under foreign custody (%s), then consumes once", async mode => {
  const f = fixture();
  vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", mode);
  const identity = { id: `session:${f.sessionId}`, name: "custody-main", kind: "main" as const };
  const sender = { id: "queued-sender", name: "queued-sender", kind: "main" as const };
  const mesh = new MeshStore(f.meshRoot, f.config.mesh.maxEventBytes, f.config.mesh.maxReadEvents);
  const closureKey = "topology/lineage-closures/" + createHash("sha256").update(identity.id).digest("hex");
  const closure = await mesh.put({ key: closureKey, identity, value: {
    format: 1, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id, closedAt: Date.now(),
  } });
  const inbox = new RootInbox(mesh, identity, () => [identity.id]);
  const cursor = await mesh.put({ key: inbox.key, identity, value: { after: 0 } });
  // Age only the queued event past production's steer grace; all lock waits use real time.
  const timestamp = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 61_000);
  let event;
  try {
    event = await mesh.publish({ topic: "fleet.custody-retry", kind: "p0", from: sender, to: identity.id,
      text: "queued root work", data: { key: "queued-root-work" } });
  } finally { timestamp.mockRestore(); }
  const journal = path.join(f.meshRoot, "main-followups", `${encodeURIComponent(f.sessionId)}.json`);
  fs.mkdirSync(path.dirname(journal), { recursive: true });
  fs.writeFileSync(journal, JSON.stringify({ version: 1, items: [
    { id: "queued-followup", from: sender, message: "queued follow-up work", sentAt: Date.now() },
  ] }));
  const originalJournal = fs.readFileSync(journal, "utf8");
  const holder = spawn(process.execPath, ["-e", holderSource, f.meshRoot, "custody.lock"], { stdio: ["pipe", "pipe", "pipe"] });
  const exited = once(holder, "exit");
  void exited.catch(() => undefined);
  const custody = vi.spyOn(custodyLocks, "acquireMeshCustodyLock");
  const originalResume = ParticipantDirectory.prototype.resumeLineage;
  let firstResume = true;
  const resume = vi.spyOn(ParticipantDirectory.prototype, "resumeLineage").mockImplementation(async function (this: ParticipantDirectory) {
    if (firstResume) {
      firstResume = false;
      // The awaited deletion executes while this process owns custody, not just after an acquisition.
      expect(Number(fs.readFileSync(path.join(f.meshRoot, "custody.lock", "owner"), "utf8").split("\n")[1])).toBe(process.pid);
      await originalResume.call(this);
      expect(Number(fs.readFileSync(path.join(f.meshRoot, "custody.lock", "owner"), "utf8").split("\n")[1])).toBe(process.pid);
    } else await originalResume.call(this);
  });
  const start = vi.spyOn(RootInbox.prototype, "start");
  const next = vi.spyOn(RootInbox.prototype, "next");
  const wake = vi.spyOn(RootInbox.prototype, "wake");
  const attachDrain = vi.spyOn(MainAgentController.prototype, "attachFollowUpDrain");
  vi.spyOn(console, "warn").mockImplementation(() => {});
  const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
  const held: RootInboxSession = { holdsBatch: () => true, holdsSteer: () => false };
  let released = false;
  try {
    await once(holder.stdout, "data");
    await expect(f.runtime.initialize(f.context, f.config)).resolves.toBeUndefined();
    // Observe a real retry beginning while the foreign owner still holds custody.
    await vi.waitFor(() => expect(custody.mock.calls.length).toBeGreaterThanOrEqual(2), { timeout: 8_000, interval: 25 });
    expect(resume).not.toHaveBeenCalled();
    expect(mesh.get(closureKey, { fresh: true })).toEqual(closure);
    expect(start).not.toHaveBeenCalled();
    expect(attachDrain).not.toHaveBeenCalled();
    await expect(f.runtime.nextRootInbox(notHeld)).resolves.toBeUndefined();
    await expect(f.runtime.nextRootInbox(notHeld, () => true)).resolves.toBeUndefined();
    await expect(f.runtime.nextRootInbox(held)).resolves.toBeUndefined();
    expect(next).not.toHaveBeenCalled();
    expect(wake).not.toHaveBeenCalled();
    expect(mesh.get(inbox.key, { fresh: true })).toEqual(cursor);
    expect(fs.readFileSync(journal, "utf8")).toBe(originalJournal);
    expect(f.pi.sendMessage).not.toHaveBeenCalled();
    holder.stdin.end("release\n");
    await exited;
    released = true;
    await vi.waitFor(() => expect(readHostLeases(f.meshRoot).has(identity.id)).toBe(true), { timeout: 8_000, interval: 50 });
    expect(mesh.get(closureKey, { fresh: true })).toBeUndefined();
    expect(start).toHaveBeenCalledOnce();
    expect(attachDrain).toHaveBeenCalledOnce();
    expect(f.pi.sendMessage).toHaveBeenCalledOnce();
    expect(f.pi.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ details: expect.objectContaining({ id: "queued-followup" }) }), expect.anything());
    const batch = await f.runtime.nextRootInbox(notHeld, () => true);
    expect(batch?.events.map(value => value.id)).toEqual([event.id]);
    expect((await f.runtime.nextRootInbox(held))?.events).toEqual([]);
    expect((await f.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
    await expect(f.runtime.nextRootInbox(notHeld, () => true)).resolves.toBeUndefined();
    expect(f.pi.sendMessage).toHaveBeenCalledOnce();
  } finally {
    if (!released) {
      holder.stdin.end("release\n");
      const kill = setTimeout(() => holder.kill(), 2_000);
      try { await exited; } finally { clearTimeout(kill); }
    }
    await inbox.close();
    await f.cleanup();
  }
}, 40_000);

it("fences inbox reads and wakes before initialization and after shutdown", async () => {
  const f = fixture();
  const session: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
  const next = vi.spyOn(RootInbox.prototype, "next");
  const wake = vi.spyOn(RootInbox.prototype, "wake");
  try {
    await expect(f.runtime.nextRootInbox(session)).resolves.toBeUndefined();
    await expect(f.runtime.nextRootInbox(session, () => true)).resolves.toBeUndefined();
    expect(next).not.toHaveBeenCalled();
    expect(wake).not.toHaveBeenCalled();

    await f.runtime.initialize(f.context, f.config);
    expect((await f.runtime.nextRootInbox(session))?.events).toEqual([]);
    await expect(f.runtime.nextRootInbox(session, () => true)).resolves.toBeUndefined();
    expect(next).toHaveBeenCalledTimes(2);
    expect(wake).toHaveBeenCalledOnce();

    await f.runtime.shutdown();
    next.mockClear();
    wake.mockClear();
    await expect(f.runtime.nextRootInbox(session)).resolves.toBeUndefined();
    await expect(f.runtime.nextRootInbox(session, () => true)).resolves.toBeUndefined();
    expect(next).not.toHaveBeenCalled();
    expect(wake).not.toHaveBeenCalled();
  } finally { await f.cleanup(); }
});

it("still rejects non-contention inbox activation errors", async () => {
  const f = fixture();
  try {
    vi.spyOn(custodyLocks, "acquireMeshCustodyLock").mockRejectedValueOnce(new Error("inbox storage failure"));
    await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow("inbox storage failure");
    expect(f.notify).not.toHaveBeenCalled();
  } finally { await f.cleanup(); }
});
