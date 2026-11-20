import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { main } from "../src/actors-cli.js";
import { offlineRemovalHooks } from "../src/actors/remove-offline.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { lockFile } from "../src/residency/file-lock.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentDirectories, retainedRoutesAt } from "../src/residency/wake-index.js";
import { retryResidentWakes, wakeResidentActors } from "../src/residency/wake.js";
import { mainPublicationFencePath, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// catch: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20687
// Keep: operator-entrypoint real-file regression and never-OK trace/queue custody guards.
const deadPid = 2147483647;
const json = (file: string, value: unknown): void => { fs.writeFileSync(file, JSON.stringify(value)); };
const snapshot = (root: string): Record<string, { bytes: string; ino: number; mode: number }> => {
  const result: ReturnType<typeof snapshot> = {};
  for (const entry of fs.readdirSync(root, { recursive: true, withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const file = path.join(entry.parentPath, entry.name), stat = fs.statSync(file);
    result[path.relative(root, file)] = { bytes: fs.readFileSync(file).toString("base64"), ino: stat.ino, mode: stat.mode };
  }
  return result;
};
const fixture = async (options: { owner?: boolean; participant?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retire-"));
  const meshRoot = path.join(root, "mesh"), rootId = "session:retire-dead-root";
  const directory = residentRoot(meshRoot, rootId);
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "retire-dead-root", cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"), residencyRoot: directory,
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend: "file" },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention }, workerPath: path.join(root, "worker.js"),
    fabricExtensionPath: path.join(root, "dist", "index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  for (const dir of [directory, config.actorRoot, config.sessionActorRoot!, ...["requests", "responses", "processing", "runs", "agents"].map(name => path.join(directory, name))]) {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  }
  json(path.join(directory, "config.json"), config);
  if (options.owner !== false) json(path.join(directory, "owner.json"), { format: 1, hostId: residentHostId(rootId), pid: deadPid, processStartTime: "1", token: "dead", startedAt: 1, readyAt: 1 });
  json(path.join(directory, "host.lock"), { pid: deadPid, processStartTime: "1", token: "dead" });
  fs.writeFileSync(path.join(directory, "host-fence-establish.lock"), "");
  fs.writeFileSync(path.join(directory, "trace.jsonl"), Buffer.from([0, 255, 10, 13, 127]));
  fs.mkdirSync(path.join(directory, "runs-retired", "old"), { recursive: true });
  fs.writeFileSync(path.join(directory, "runs-retired", "old", "session.jsonl"), "old trace\n");
  const mainSession = path.join(root, "main.jsonl");
  fs.writeFileSync(mainSession, "Main session must remain byte-identical\n");
  const participant = (pid = deadPid, at = Date.now() - 3 * ROOT_PARTICIPANT_FRESH_MS, startTime = "1") => writeParticipantFile(meshRoot, {
    key: "topology/participants/" + createHash("sha256").update(rootId).digest("hex"),
    value: { id: rootId, rootId, ownerHostId: rootId, kind: "root", mainProcess: { pid, host: os.hostname(), startTime } },
    version: 1, updatedAt: at, updatedBy: { id: rootId, name: "main", kind: "main" },
  });
  if (options.participant !== false) participant();
  const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents, { stateBackend: "file" });
  await mesh.put({ key: "retirement-fixture", value: true, identity: { id: "fixture", name: "fixture", kind: "agent" } });
  const evidenceFile = path.join(root, "evidence.txt");
  fs.writeFileSync(evidenceFile, "herdr: no Main pane; ps: root Main pid is gone (scratch fixture)");
  const run = async (extra: string[] = [], base = ["--confirm-dead-root", rootId, "--main-stopped", "--evidence-file", evidenceFile]) => {
    let out = "", err = "";
    const code = await main(["retire-residency", "--mesh-root", meshRoot, "--resident", directory, ...base, ...extra], {
      out: text => { out += text; }, err: text => { err += text; },
    });
    return { code, out, err };
  };
  const actor = (scope: "project" | "session" = "project") => {
    const actorRoot = scope === "project" ? config.actorRoot : config.sessionActorRoot!;
    const id = "a".repeat(32), dir = path.join(actorRoot, id);
    fs.mkdirSync(dir, { mode: 0o700 });
    json(path.join(actorRoot, "actors.json"), { format: 1, actors: [{ id, name: "leftover", rootId, residency: "durable", status: "dormant", scope }] });
    fs.writeFileSync(path.join(dir, "session.jsonl"), "actor trace\n");
    return dir;
  };
  return { root, config, directory, mesh, evidenceFile, mainSession, run, actor, participant };
};

describe.skipIf(process.platform !== "linux")("fabric-actors retire-residency (#20687)", () => {
  it("retires a dead root by rename, stops matching wake/retry routing and keeps every original byte and inode", async () => {
    const f = await fixture();
    try {
      const actorDir = f.actor();
      const routes = { format: 1, rootId: f.config.rootId, hostId: residentHostId(f.config.rootId),
        configJson: canonicalResidentWakeConfig(f.config), actors: [{ id: "a".repeat(32), name: "leftover", topics: ["retire.work"] }] };
      json(path.join(f.directory, "wake-routes.json"), routes);
      // Real retryable mismatch: both the publication and maintenance paths inspect this root.
      json(path.join(f.directory, "config.json"), { ...f.config, mainName: "drifted" });
      const event = await f.mesh.publish({ topic: "retire.work", data: "kept event", from: { id: "sender", name: "sender", kind: "agent" } });
      expect(JSON.parse(fs.readFileSync(path.join(f.directory, "wake-failure.json"), "utf8")).error).toContain("ResidentWakeConfigMismatch");
      const before = snapshot(f.directory), actorsBefore = snapshot(actorDir), mainBefore = fs.readFileSync(f.mainSession);
      const rootInode = fs.statSync(f.directory).ino;
      const result = await f.run();
      expect(result, result.err).toMatchObject({ code: 0, err: "" });
      const receipt = JSON.parse(result.out);
      expect(receipt).toMatchObject({ action: "retire-residency", retired: true, dryRun: false, resident: f.directory });
      expect(fs.existsSync(f.directory)).toBe(false);
      expect(fs.statSync(receipt.archive).ino).toBe(rootInode);
      expect(receipt.archive).toContain(path.join(f.config.meshRoot, "residency-retired") + path.sep);
      const after = snapshot(receipt.archive);
      for (const [name, contents] of Object.entries(before)) expect(after[name], name).toEqual(contents);
      expect(snapshot(actorDir)).toEqual(actorsBefore);
      expect(fs.readFileSync(f.mainSession)).toEqual(mainBefore);
      expect(JSON.parse(fs.readFileSync(receipt.auditFile, "utf8"))).toMatchObject({
        mainStopped: true, rootId: f.config.rootId, operatorAttestation: fs.readFileSync(f.evidenceFile, "utf8"),
        toolEvidence: { participants: expect.arrayContaining([expect.objectContaining({ pid: deadPid })]) },
      });
      expect(residentDirectories(f.config.meshRoot)).not.toContain(f.directory);
      expect(retainedRoutesAt(f.directory)).toBeUndefined();
      const launches: string[] = [];
      await wakeResidentActors(f.mesh, [event], async file => { launches.push(file); });
      await retryResidentWakes(f.mesh, async file => { launches.push(file); }, Date.now() + 1_000_000);
      await f.mesh.publish({ topic: "retire.work", data: "after retirement", from: { id: "sender", name: "sender", kind: "agent" } });
      expect(launches).toEqual([]);
      expect(fs.existsSync(f.directory)).toBe(false);
      expect(snapshot(receipt.archive)).toEqual(after); // No moved failure receipt was rewritten on retry.
    } finally { f.mesh.closeState(); }
  });

  it("dry-run prints the exact rename/audit/fence plan and changes no existing bytes or directory entries", async () => {
    const f = await fixture();
    try {
      const before = snapshot(f.root), entries = fs.readdirSync(f.root, { recursive: true }).sort();
      const result = await f.run(["--dry-run"]);
      expect(result, result.err).toMatchObject({ code: 0, err: "" });
      const value = JSON.parse(result.out);
      expect(value).toMatchObject({ action: "retire-residency", retired: false, dryRun: true,
        plan: { rename: { from: f.directory, to: expect.any(String) }, auditFile: expect.any(String), create: expect.any(Array), delete: [] } });
      expect(value.plan.rename.to).toContain(path.join(f.config.meshRoot, "residency-retired"));
      expect(snapshot(f.root)).toEqual(before);
      expect(fs.readdirSync(f.root, { recursive: true }).sort()).toEqual(entries);
    } finally { f.mesh.closeState(); }
  });

  it.each(["owner.json", "host.lock", "main-generation.json"])("refuses a live pid in %s even with operator confirmation", async file => {
    const f = await fixture();
    try {
      const current = fs.existsSync(path.join(f.directory, file)) ? JSON.parse(fs.readFileSync(path.join(f.directory, file), "utf8")) : {};
      json(path.join(f.directory, file), { ...current, pid: process.pid });
      const before = snapshot(f.directory);
      const result = await f.run();
      expect(result.code).toBe(1); expect(result.err).toMatch(/live|not proven dead/);
      expect(snapshot(f.directory)).toEqual(before);
    } finally { f.mesh.closeState(); }
  });

  it.each(["host.lock", "host-fence-establish.lock", "wake.lock", "wake-intent.lock", "handover.lock", "main-publication-fence"])(
    "refuses held %s without moving the directory", async name => {
      const f = await fixture();
      const file = name === "main-publication-fence" ? mainPublicationFencePath(f.config.meshRoot, f.config.rootId) : path.join(f.directory, name);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const fd = await lockFile(file, 0, true);
      try {
        const before = snapshot(f.directory);
        const result = await f.run();
        expect(result.code).toBe(1); expect(result.err).toMatch(/held|busy|lock|fence/);
        expect(snapshot(f.directory)).toEqual(before);
      } finally { fs.closeSync(fd); f.mesh.closeState(); }
    });

  it.each(["queued", "parked", "overflow", "settled", "pending-outcome", "dead-letter", "stop-archive", "session-queue"])(
    "refuses %s work and preserves it", async kind => {
      const f = await fixture();
      try {
        const dir = f.actor(kind === "session-queue" ? "session" : "project");
        if (kind === "dead-letter") fs.writeFileSync(path.join(dir, "dead-letter.jsonl"), '{"heldBy":"filter","payload":"keep"}\n');
        else if (kind === "stop-archive") {
          fs.mkdirSync(path.join(dir, "stopped-queues")); fs.writeFileSync(path.join(dir, "stopped-queues", "kept.jsonl"), "kept archive\n");
        } else json(path.join(dir, "queue-1234.json"), { format: 1, items: ["settled", "pending-outcome"].includes(kind) ? [] : [{ id: kind, deferredHandoff: kind === "parked" }],
          ...(kind === "settled" ? { settledWindows: [{ pending: { id: "pending" } }] } : {}),
          ...(kind === "pending-outcome" ? { pendingDeliveryOutcomes: [{ eventId: "pending" }] } : {}) });
        const before = snapshot(dir), residentBefore = snapshot(f.directory);
        const result = await f.run();
        expect(result.code).toBe(1); expect(result.err).toMatch(/FABRIC_ACTOR_QUEUED_WORK|held work/);
        expect(snapshot(dir)).toEqual(before);
        for (const [name, contents] of Object.entries(residentBefore)) expect(snapshot(f.directory)[name]).toEqual(contents);
      } finally { f.mesh.closeState(); }
    });

  it.each([[], ["--confirm-dead-root", "wrong"], ["--confirm-dead-root", "session:retire-dead-root"],
    ["--confirm-dead-root", "session:retire-dead-root", "--main-stopped"]].map(flags => [flags] as const))("refuses missing/mismatched authorization %j", async flags => {
    const f = await fixture();
    try {
      const result = await f.run([], flags);
      expect(result.code).toBe(1); expect(fs.existsSync(f.directory)).toBe(true);
    } finally { f.mesh.closeState(); }
  });

  it("refuses a fresh Main participant even though its recorded pid is dead", async () => {
    const f = await fixture();
    try { f.participant(deadPid, Date.now()); const result = await f.run(); expect(result.code).toBe(1); expect(result.err).toContain("fresh"); }
    finally { f.mesh.closeState(); }
  });

  it("allows a cleanly withdrawn owner record without recreating it", async () => {
    const f = await fixture({ owner: false });
    try {
      const result = await f.run();
      expect(result, result.err).toMatchObject({ code: 0, err: "" });
      expect(fs.existsSync(path.join(JSON.parse(result.out).archive, "owner.json"))).toBe(false);
    } finally { f.mesh.closeState(); }
  });

  it("keeps a pre-existing Main fence's exact bytes and inode, releasing only its kernel lock", async () => {
    const f = await fixture();
    try {
      const file = mainPublicationFencePath(f.config.meshRoot, f.config.rootId);
      fs.mkdirSync(path.dirname(file));
      fs.writeFileSync(file, "original historical fence bytes\n");
      const before = snapshot(path.dirname(file));
      const result = await f.run();
      expect(result, result.err).toMatchObject({ code: 0, err: "" });
      expect(snapshot(path.dirname(file))).toEqual(before);
      expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(false);
    } finally { f.mesh.closeState(); }
  });

  it("preserves writes through an already-open trace descriptor after rename", async () => {
    const f = await fixture();
    const fd = fs.openSync(path.join(f.directory, "trace.jsonl"), "a");
    try {
      const before = fs.readFileSync(path.join(f.directory, "trace.jsonl"));
      const result = await f.run();
      expect(result, result.err).toMatchObject({ code: 0, err: "" });
      fs.writeSync(fd, "late descriptor write\n");
      expect(fs.readFileSync(path.join(JSON.parse(result.out).archive, "trace.jsonl"))).toEqual(Buffer.concat([before, Buffer.from("late descriptor write\n")]));
    } finally { fs.closeSync(fd); f.mesh.closeState(); }
  });

  it.each(["live Main", "live root lease", "missing Main identity"])("refuses %s despite attestation", async kind => {
    const f = await fixture({ participant: kind !== "missing Main identity" });
    try {
      if (kind === "live Main") f.participant(process.pid, Date.now() - 3 * ROOT_PARTICIPANT_FRESH_MS, processStartTime(process.pid)!);
      if (kind === "live root lease") writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId,
        identityId: f.config.rootId, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      const before = snapshot(f.directory);
      const result = await f.run();
      expect(result.code).toBe(1);
      expect(result.err).toMatch(kind === "missing Main identity" ? /identity.*unavailable/ : /live/);
      expect(snapshot(f.directory)).toEqual(before);
    } finally { f.mesh.closeState(); }
  });

  it.each(["owner", "queue"])("rechecks racing %s custody under the Main publication fence", async kind => {
    const f = await fixture();
    const dir = f.actor();
    try {
      offlineRemovalHooks.beforeFinalCheck = () => {
        expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(true);
        if (kind === "owner") {
          const file = path.join(f.directory, "owner.json");
          json(file, { ...JSON.parse(fs.readFileSync(file, "utf8")), pid: process.pid });
        } else json(path.join(dir, "queue-racing.json"), { format: 1, items: [{ id: "arrived-before-rename" }] });
      };
      const result = await f.run();
      expect(result.code).toBe(1); expect(result.err).toMatch(/live|FABRIC_ACTOR_QUEUED_WORK/);
      expect(fs.existsSync(f.directory)).toBe(true);
      expect(fs.existsSync(path.join(f.config.meshRoot, "residency-retired"))).toBe(false);
    } finally { delete offlineRemovalHooks.beforeFinalCheck; f.mesh.closeState(); }
  });

  it("refuses invalid queues rather than treating unreadable work as empty", async () => {
    const f = await fixture();
    try {
      const dir = f.actor();
      fs.writeFileSync(path.join(dir, "queue-broken.json"), '{"format":1,"items":');
      const result = await f.run();
      expect(result.code).toBe(1); expect(fs.existsSync(f.directory)).toBe(true);
    } finally { f.mesh.closeState(); }
  });

  it("refuses a live recorded launcher even if the resident owner is dead", async () => {
    const f = await fixture();
    try {
      const file = path.join(f.directory, "owner.json");
      json(file, { ...JSON.parse(fs.readFileSync(file, "utf8")), handover: { launcher: { pid: process.pid } } });
      const result = await f.run();
      expect(result.code).toBe(1); expect(result.err).toContain("launcher is live");
    } finally { f.mesh.closeState(); }
  });

  it.each(["watchdog-custody.json", "requests/pending.json", "processing/claimed.json"])("refuses unresolved custody at %s", async file => {
    const f = await fixture();
    try { json(path.join(f.directory, file), { pid: deadPid }); const result = await f.run(); expect(result.code).toBe(1); expect(fs.existsSync(f.directory)).toBe(true); }
    finally { f.mesh.closeState(); }
  });
});
