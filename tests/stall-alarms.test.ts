import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MainInboxMaintenance, registerMainInbox, recordMainSuccessor, mainInboxOwns, mainInboxActive,
  rootPresenceAlarms, stageMainSuccessor, confirmMainSuccessor } from "../src/topology/stall-alarms.js";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig } from "../src/config.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

const dirs: string[] = [], controllers: MainAgentController[] = [];
afterEach(() => { for (const main of controllers.splice(0)) main.closeFollowUpDrain(); for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); vi.restoreAllMocks(); });
const identity = (id: string): MeshIdentity => ({ id: `session:${id}`, sessionId: id, name: id, kind: "main" });
const A = identity("A"), B = identity("B"), C = identity("C");
const setup = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stalls-")); dirs.push(dir); return { dir, mesh: new MeshStore(dir, 64 * 1024, 500) }; };
const participant = (id: string, kind: "root" | "actor" | "agent", rootId = B.id, status = "idle", hostId = C.id): FabricParticipantInfo => ({
  format: 1, id, kind, rootId, ownerHostId: hostId, ownerIdentityId: hostId, name: id, status,
  runner: "pi", transport: "host", capabilities: ["steer", "followUp"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: true, stale: false,
});
const options = { rootPresenceAlarmMs: 15 * 60_000, undeliveredAlarmMs: 30 * 60_000, rootGoneTtlMs: 2 * 60 * 60_000 };
const source = (roots: FabricParticipantInfo[]) => ({ list: () => roots }) as unknown as FabricParticipantSource;
const main = (mesh: MeshStore, who: MeshIdentity, idle = false, file?: string) => {
  const entries: unknown[] = [], sent: any[] = [], handlers = new Map<string, ((event: any, ctx: ExtensionContext) => unknown)[]>();
  const pi = { getThinkingLevel: () => "off", sendMessage: (message: any, opts: any) => { sent.push({ message, opts }); },
    on: (name: string, fn: (event: any, ctx: ExtensionContext) => unknown) => { handlers.set(name, [...handlers.get(name) ?? [], fn]); return () => {}; } } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => idle, hasPendingMessages: () => false, signal: { aborted: false }, sessionManager: {
    getEntries: () => entries, getSessionFile: () => file, getSessionId: () => who.sessionId,
  } } as unknown as ExtensionContext;
  const controller = new MainAgentController(pi, who.id, true, mesh.root, who.sessionId);
  controllers.push(controller);
  registerMainInbox(mesh.root, who, who.sessionId!, file);
  controller.attachFollowUpDrain(ctx, 60_000, path.join(mesh.root, "main-followups", `${who.sessionId}.json`), 600,
    { owns: id => mainInboxOwns(mesh.root, who.id, id), active: () => mainInboxActive(mesh.root, who.id) });
  return { controller, sent, entries, emit: (name: string, event: any) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); } };
};

it("root presence alarms once after 15 minutes with kind/status counts, across owners, then re-arms", async () => {
  const { mesh } = setup();
  const members = [participant("actor", "actor"), participant("agent", "agent", B.id, "running", A.id)];
  await rootPresenceAlarms(mesh, C, C.id, members, options.rootPresenceAlarmMs, 100);
  await rootPresenceAlarms(mesh, C, C.id, members, options.rootPresenceAlarmMs, 100 + options.rootPresenceAlarmMs);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
  await Promise.all([rootPresenceAlarms(mesh, C, C.id, members, options.rootPresenceAlarmMs, 101 + options.rootPresenceAlarmMs),
    rootPresenceAlarms(mesh, A, A.id, members, options.rootPresenceAlarmMs, 101 + options.rootPresenceAlarmMs)]);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(1);
  expect(mesh.read({ topic: "ops.owner" })[0]!.data).toMatchObject({ byKind: { actor: 1, agent: 1 }, byStatus: { idle: 1, running: 1 }, firstAbsentAt: 100 });
  await rootPresenceAlarms(mesh, C, C.id, [...members, participant(B.id, "root")], options.rootPresenceAlarmMs, 2_000_000);
  await rootPresenceAlarms(mesh, C, C.id, members, options.rootPresenceAlarmMs, 2_000_001);
  await rootPresenceAlarms(mesh, C, C.id, members, options.rootPresenceAlarmMs, 2_000_002 + options.rootPresenceAlarmMs);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(2);
});

it("local reaper alarms stale native members of a dead publishing runtime, never mirrored rows alone", async () => {
  const { mesh } = setup();
  const stale = { ...participant("stale-agent", "agent", B.id, "completed", A.id), stale: true };
  const mirrored = { ...stale, remoteHost: "remote" };
  await rootPresenceAlarms(mesh, C, C.id, [mirrored], 1, 1);
  await rootPresenceAlarms(mesh, C, C.id, [mirrored], 1, 3);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
  await rootPresenceAlarms(mesh, C, C.id, [stale], 1, 4);
  await rootPresenceAlarms(mesh, C, C.id, [stale], 1, 6);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(1);
});

it("older resident host mesh configs retain the 15-minute root-presence default", async () => {
  const { mesh } = setup(); const members = [participant("actor", "actor")];
  await rootPresenceAlarms(mesh, C, C.id, members, undefined, 1);
  await rootPresenceAlarms(mesh, C, C.id, members, undefined, 2);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
  await rootPresenceAlarms(mesh, C, C.id, members, undefined, 2 + options.rootPresenceAlarmMs);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(1);
});

it("age alarm goes once per message to sender and target owner, never for a native receipt", async () => {
  const { mesh } = setup(); const owner = main(mesh, B);
  const pending = owner.controller.deliverAgent({ from: A, message: "held", delivery: "followUp", deliveryId: "pending" });
  const maintenance = new MainInboxMaintenance(mesh, B, source([participant(B.id, "root", B.id, "running", B.id)]), owner.controller, options);
  const sentAt = JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items[0].sentAt;
  await maintenance.run(sentAt + options.undeliveredAlarmMs);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
  await maintenance.run(sentAt + options.undeliveredAlarmMs + 1);
  await maintenance.run(sentAt + options.undeliveredAlarmMs + 2);
  expect(mesh.read({ topic: "ops.owner" }).map(e => e.to).sort()).toEqual([A.id, B.id]);
  expect(mesh.read({ topic: "ops.owner" })[0]!.data).toMatchObject({ messageId: pending.messageId });
});

it("rotation moves original carrier once, fences predecessor replay and gives sender reroute receipt", async () => {
  const { mesh } = setup(); const old = main(mesh, B);
  const message = old.controller.deliverAgent({ from: A, message: "follow-up", delivery: "followUp", deliveryId: "original" });
  old.controller.closeFollowUpDrain();
  await recordMainSuccessor(mesh, B.id, "B", C.id);
  const next = main(mesh, C, true);
  const maintenance = new MainInboxMaintenance(mesh, C, source([]), next.controller, options);
  await maintenance.run(); await maintenance.run();
  expect(next.sent).toHaveLength(1); expect(next.sent[0].message.details.id).toBe(message.messageId);
  expect(old.sent).toHaveLength(0);
  expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items).toHaveLength(0);
  const resumed = main(mesh, B, true);
  expect(resumed.sent).toHaveLength(0);
  expect(() => resumed.controller.deliverAgent({ from: A, message: "retry", delivery: "followUp", deliveryId: "original" })).toThrow("rotated");
  const receipts = mesh.read({ topic: "fleet.work.inbox-receipts" });
  expect(receipts).toHaveLength(1); expect(receipts[0]).toMatchObject({ to: A.id, text: `rerouted: ${B.id} -> ${C.id}` });
});

it("native pre-switch abort/settle does not deliver to old inbox; canceled switch reopens on owner input", () => {
  const { mesh } = setup(); const old = main(mesh, B);
  old.controller.deliverAgent({ from: A, message: "held during rotation", delivery: "followUp" });
  old.emit("session_before_switch", { reason: "new" });
  old.emit("agent_settled", { outcome: "aborted" });
  expect(old.sent).toHaveLength(0);
  expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items).toHaveLength(1);
  old.emit("input", { source: "user" });
  old.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
  expect(old.sent).toHaveLength(1);
});

it("passive nextTurn context neither raises Main followUp age alarms nor crosses a rotation", async () => {
  const { mesh } = setup(); const old = main(mesh, B);
  const passive = old.controller.deliverAgent({ from: A, message: "old session context", delivery: "nextTurn", triggerTurn: false, deliveryId: "passive" });
  old.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, B.id, "B", C.id);
  const next = main(mesh, C, true);
  await new MainInboxMaintenance(mesh, C, source([]), next.controller, options).run(Date.now() + options.rootGoneTtlMs + 1);
  expect(next.sent).toHaveLength(0);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(0);
  expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items[0].id).toBe(passive.messageId);
});

it("resuming the same native session is not a self-successor and replays its own held carrier", async () => {
  const { mesh } = setup(); const file = path.join(mesh.root, "B-native.jsonl"); const old = main(mesh, B, false, file);
  const carrier = old.controller.deliverAgent({ from: A, message: "same-root resume", delivery: "followUp", deliveryId: "same-root" });
  old.controller.closeFollowUpDrain();
  await stageMainSuccessor(mesh, B.id, "B", file);
  expect(await confirmMainSuccessor(mesh, B.id, file)).toBe(false);
  expect(mainInboxActive(mesh.root, B.id)).toBe(true);
  const resumed = main(mesh, B, true, file);
  expect(resumed.sent).toHaveLength(1);
  expect(resumed.sent[0].message.details.id).toBe(carrier.messageId);
});

it("native targetSessionFile succession survives replacement; unrelated sessions and reloads never adopt", async () => {
  const { mesh } = setup(); main(mesh, B);
  const target = path.join(mesh.root, "C.jsonl");
  await stageMainSuccessor(mesh, B.id, "B", target);
  expect(await confirmMainSuccessor(mesh, C.id, path.join(mesh.root, "unrelated.jsonl"))).toBe(false);
  expect(await confirmMainSuccessor(mesh, C.id, target)).toBe(true);
  expect(mainInboxActive(mesh.root, B.id)).toBe(false);
});

it("no successor yields explicit undeliverable after TTL, but a live writer with a lapsed lease is never moved", async () => {
  const { mesh } = setup(); const old = main(mesh, B);
  const orphan = old.controller.deliverAgent({ from: A, message: "orphan", delivery: "followUp" });
  const next = main(mesh, C);
  const maintenance = new MainInboxMaintenance(mesh, C, source([]), next.controller, options);
  await maintenance.run(1_000); await maintenance.run(1_001 + options.rootGoneTtlMs);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(0);
  const ownerPath = path.join(mesh.root, "main-followups", "B.owner.json");
  const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
  fs.writeFileSync(ownerPath, JSON.stringify({ ...owner, pid: 999999999 }));
  await maintenance.run(1_002 + options.rootGoneTtlMs); await maintenance.run(1_003 + options.rootGoneTtlMs);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toMatchObject([{ to: A.id, text: "undeliverable: root gone" }]);
  expect(mainInboxOwns(mesh.root, B.id, orphan.messageId)).toBe(false);
  expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items).toHaveLength(0);
});

it("canonical persisted delivery stays at predecessor and is never rerouted", async () => {
  const { mesh } = setup(); const file = path.join(mesh.root, "B-session.jsonl"); const old = main(mesh, B, true, file);
  const receipt = old.controller.deliverAgent({ from: A, message: "already there", delivery: "steer", deliveryId: "delivered" });
  fs.writeFileSync(file, [JSON.stringify({ type: "session", id: "B" }), JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-message", details: { id: receipt.messageId } }), ""].join("\n"));
  old.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, B.id, "B", C.id);
  const next = main(mesh, C, true);
  await new MainInboxMaintenance(mesh, C, source([]), next.controller, options).run(Date.now() + options.undeliveredAlarmMs + 1);
  expect(next.sent).toHaveLength(0); expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(0);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
});

it("publication receipt recovers append-before-receipt without a second alarm, and survives log retirement", async () => {
  const { mesh } = setup();
  const packet = { topic: "ops.owner", from: A, dedupeKey: "crash-append", text: "alarm" };
  const first = await mesh.publish(packet);
  const receiptFile = path.join(mesh.root, "event-receipts", createHash("sha256").update(packet.dedupeKey).digest("hex") + ".json");
  fs.unlinkSync(receiptFile); // Exact persisted state of a crash between append and receipt.
  expect((await mesh.publish(packet)).id).toBe(first.id);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(1);
  fs.writeFileSync(path.join(mesh.root, "events.jsonl"), "");
  expect((await mesh.publish(packet)).id).toBe(first.id);
});

it("presence callback shares successful heartbeat even when dead-host reaper is disabled", async () => {
  const { mesh } = setup(); const pass = vi.fn(async () => {});
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: A.id, rootId: A.id, identity: A, reapDeadHosts: false, presencePass: pass, presencePassMs: 0 });
  try { await directory.refresh(); await vi.waitFor(() => expect(pass).toHaveBeenCalledTimes(1)); } finally { await directory.close(); }
});

it("stall thresholds are registered and configurable in mesh config", () => {
  const { dir } = setup(); const agentDir = path.join(dir, "agent"); fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { rootPresenceAlarmMs: 123, undeliveredAlarmMs: 456, rootGoneTtlMs: 789 } }));
  const config = loadFabricConfig({ cwd: dir, agentDir, projectTrusted: false });
  expect(config.mesh).toMatchObject({ rootPresenceAlarmMs: 123, undeliveredAlarmMs: 456, rootGoneTtlMs: 789 });
  expect(DEFAULT_FABRIC_CONFIG.mesh).toMatchObject(options);
});

it.skipIf(process.platform === "win32")("SIGKILL between successor admission and sender receipt recovers once from journal/claim", async () => {
  const { mesh } = setup(); const old = main(mesh, B);
  const packet = old.controller.deliverAgent({ from: A, message: "crash carrier", delivery: "followUp", deliveryId: "crash-carrier" });
  old.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, B.id, "B", C.id);
  const worker = spawnSync("nice", ["-n", "19", "bun", "run", path.resolve("tests/fixtures/stall-inbox-crash.ts"), mesh.root], { encoding: "utf8", timeout: 20_000 });
  expect(worker.signal).toBe("SIGKILL");
  const journal = JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "C.json"), "utf8"));
  expect(journal.items.map((item: any) => item.id)).toEqual([packet.messageId]);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(0);
  const recovered = main(mesh, C, true);
  await new MainInboxMaintenance(mesh, C, source([]), recovered.controller, options).run();
  expect(recovered.sent).toHaveLength(1); expect(recovered.sent[0].message.details.id).toBe(packet.messageId);
  await new MainInboxMaintenance(mesh, C, source([]), recovered.controller, options).run();
  expect(recovered.sent).toHaveLength(1); expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(1);
});
