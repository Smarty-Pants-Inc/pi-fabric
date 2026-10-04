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
  const activation = registerMainInbox(mesh.root, who, who.sessionId!, file);
  controller.attachFollowUpDrain(ctx, 60_000, path.join(mesh.root, "main-followups", `${who.sessionId}.json`), 600,
    { owns: id => mainInboxOwns(mesh.root, who.id, id), active: () => mainInboxActive(mesh.root, who.id, activation) });
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
  const original = JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items[0];
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
  expect(() => resumed.controller.receiveInboxItem(original)).toThrow("claim belongs elsewhere");
  expect(resumed.sent).toHaveLength(0);
  resumed.controller.deliverAgent({ from: A, message: "fresh after resume", delivery: "steer", deliveryId: "fresh" });
  expect(resumed.sent).toHaveLength(1);
  const receipts = mesh.read({ topic: "fleet.work.inbox-receipts" });
  expect(receipts).toHaveLength(1); expect(receipts[0]).toMatchObject({ to: A.id, text: `rerouted: ${B.id} -> ${C.id}` });
});

it("native new/resume/new scopes succession to each activation, never a moved carrier or old runtime", async () => {
  const { mesh } = setup();
  const aFile = path.join(mesh.root, "A.jsonl"), bFile = path.join(mesh.root, "B.jsonl"), cFile = path.join(mesh.root, "C.jsonl");
  const oldA = main(mesh, A, false, aFile);
  const carrier = oldA.controller.deliverAgent({ from: C, message: "moved once", delivery: "followUp", deliveryId: "move" });
  oldA.controller.closeFollowUpDrain();
  await stageMainSuccessor(mesh, A.id, "A", bFile);
  expect(await confirmMainSuccessor(mesh, B.id, bFile)).toBe(true);
  const b = main(mesh, B, true, bFile);
  await new MainInboxMaintenance(mesh, B, source([]), b.controller, options).run();
  expect(b.sent.map(packet => packet.message.details.id)).toEqual([carrier.messageId]);
  b.controller.closeFollowUpDrain();
  await stageMainSuccessor(mesh, B.id, "B", aFile);
  expect(await confirmMainSuccessor(mesh, A.id, aFile)).toBe(true);
  const resumedA = main(mesh, A, true, aFile);
  expect(mainInboxActive(mesh.root, A.id)).toBe(true);
  expect(resumedA.sent).toHaveLength(0);
  expect(mainInboxOwns(mesh.root, A.id, carrier.messageId)).toBe(false);
  expect(() => oldA.controller.deliverAgent({ from: C, message: "old runtime", delivery: "steer" })).toThrow("rotated");
  const fresh = resumedA.controller.deliverAgent({ from: C, message: "fresh to resumed A", delivery: "steer" });
  expect(resumedA.sent.map(packet => packet.message.details.id)).toEqual([fresh.messageId]);
  resumedA.controller.closeFollowUpDrain();
  await stageMainSuccessor(mesh, A.id, "A", cFile);
  expect(await confirmMainSuccessor(mesh, C.id, cFile)).toBe(true);
  main(mesh, C, true, cFile);
  expect(mainInboxActive(mesh.root, A.id)).toBe(false);
  // Consumed intents cannot re-confirm a historical B -> A on a later reload.
  expect(await confirmMainSuccessor(mesh, A.id, aFile)).toBe(false);
});

it("an interrupted switch intent cannot retire or block a newer native activation", async () => {
  const { mesh } = setup(); const b = main(mesh, B);
  const target = path.join(mesh.root, "A.jsonl");
  b.controller.closeFollowUpDrain(); await stageMainSuccessor(mesh, B.id, "B", target);
  main(mesh, B); // Explicit reload/resume after the switch did not complete.
  expect(await confirmMainSuccessor(mesh, A.id, target)).toBe(false);
  const a = main(mesh, A, true, target);
  a.controller.deliverAgent({ from: C, message: "fresh", delivery: "steer" });
  expect(a.sent).toHaveLength(1);
});

it("reload activation fences an older runtime with the same native root", () => {
  const { mesh } = setup();
  const old = main(mesh, A), reloaded = main(mesh, A, true);
  expect(() => old.controller.deliverAgent({ from: B, message: "stale", delivery: "steer" })).toThrow("rotated");
  reloaded.controller.deliverAgent({ from: B, message: "current", delivery: "steer" });
  expect(reloaded.sent).toHaveLength(1);
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

it("pending claim follows an authoritative onward successor, not a lapsed live lease", async () => {
  const { mesh } = setup(); const b = main(mesh, B);
  const packet = b.controller.deliverAgent({ from: A, message: "custody carrier", delivery: "followUp" });
  b.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, B.id, "B", C.id);
  const c = main(mesh, C), a = main(mesh, A, true);
  const maintenance = new MainInboxMaintenance(mesh, A, source([]), a.controller, options);
  const now = Date.now();
  await maintenance.run(now);
  expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items).toEqual([]);
  expect(fs.existsSync(path.join(mesh.root, "main-followups", "C.json"))).toBe(false);
  await maintenance.run(now + options.rootGoneTtlMs + 1);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(0);
  c.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, C.id, "C", A.id);
  await maintenance.run(now + options.rootGoneTtlMs + 2);
  await maintenance.run(now + options.rootGoneTtlMs + 3);
  expect(a.sent.map(entry => entry.message.details.id)).toEqual([packet.messageId]);
  expect(mainInboxOwns(mesh.root, B.id, packet.messageId)).toBe(false);
  expect(mainInboxOwns(mesh.root, C.id, packet.messageId)).toBe(false);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toMatchObject([{ to: A.id, text: `rerouted: ${B.id} -> ${A.id}`, data: { messageId: packet.messageId } }]);
});

it("a pending claim with a canonical successor receipt never alarms or moves again", async () => {
  const { mesh } = setup(); const b = main(mesh, B);
  const packet = b.controller.deliverAgent({ from: A, message: "persisted before receipt", delivery: "followUp" });
  const original = JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items[0];
  b.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, B.id, "B", C.id);
  const cFile = path.join(mesh.root, "C-native.jsonl"), c = main(mesh, C, false, cFile), a = main(mesh, A, true);
  const maintenance = new MainInboxMaintenance(mesh, A, source([]), a.controller, options);
  await maintenance.run(); // Saves claim for C, without successor journal admission.
  c.controller.receiveInboxItem(original);
  fs.writeFileSync(cFile, [JSON.stringify({ type: "session", id: "C" }), JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-message", details: { id: packet.messageId } }), ""].join("\n"));
  c.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, C.id, "C", A.id);
  await maintenance.run(Date.now() + options.rootGoneTtlMs + 1);
  expect(a.sent).toHaveLength(0);
  expect(mesh.read({ topic: "ops.owner" })).toHaveLength(0);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toMatchObject([{ to: A.id, text: `rerouted: ${B.id} -> ${C.id}` }]);
});

it.skipIf(process.platform === "win32")("SIGKILL before successor journal admission: other live Mains recover custody after TTL exactly once", async () => {
  const { mesh } = setup(); const b = main(mesh, B);
  const packet = b.controller.deliverAgent({ from: A, message: "pre-admission crash", delivery: "followUp", deliveryId: "pre-admission" });
  b.controller.closeFollowUpDrain(); await recordMainSuccessor(mesh, B.id, "B", C.id);
  const worker = spawnSync("nice", ["-n", "19", "bun", "run", path.resolve("tests/fixtures/stall-inbox-crash.ts"), mesh.root, "before-admission"], { encoding: "utf8", timeout: 20_000 });
  expect(worker.signal).toBe("SIGKILL");
  expect(fs.existsSync(path.join(mesh.root, "main-followups", "C.json"))).toBe(false);
  expect(JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items).toEqual([]);
  const routeFile = path.join(mesh.root, "main-followups", "routes", `${createHash("sha256").update(packet.messageId).digest("hex")}.json`);
  expect(JSON.parse(fs.readFileSync(routeFile, "utf8"))).toMatchObject({ newRoot: C.id, messageId: packet.messageId, item: { id: packet.messageId } });
  const d = identity("D"), a = main(mesh, A, true), other = main(mesh, d, true);
  const roots = source([participant(A.id, "root"), participant(d.id, "root")]);
  const recover = new MainInboxMaintenance(mesh, A, roots, a.controller, options);
  const competing = new MainInboxMaintenance(mesh, d, roots, other.controller, options);
  const firstAbsent = (mesh.get(`topology/root-absence/${createHash("sha256").update(C.id).digest("hex")}`, { fresh: true })!.value as { firstAbsentAt: number }).firstAbsentAt;
  const now = firstAbsent + options.undeliveredAlarmMs + 1;
  await recover.run(now);
  expect(mesh.read({ topic: "ops.owner" }).some(event => event.to === A.id && (event.data as any).messageId === packet.messageId)).toBe(true);
  await recover.run(firstAbsent + options.rootGoneTtlMs);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(0);
  await Promise.all([recover.run(firstAbsent + options.rootGoneTtlMs + 1), competing.run(firstAbsent + options.rootGoneTtlMs + 1)]);
  await recover.run(firstAbsent + options.rootGoneTtlMs + 2);
  expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toMatchObject([{ to: A.id, text: "undeliverable: root gone", data: { messageId: packet.messageId } }]);
  expect(JSON.parse(fs.readFileSync(routeFile, "utf8"))).toMatchObject({ done: true, messageId: packet.messageId });
  expect(JSON.parse(fs.readFileSync(routeFile, "utf8")).item).toBeUndefined();
  expect(mainInboxOwns(mesh.root, B.id, packet.messageId)).toBe(false);
  expect(mainInboxOwns(mesh.root, C.id, packet.messageId)).toBe(false);
  expect(a.sent).toHaveLength(0); expect(other.sent).toHaveLength(0);
  expect(main(mesh, C, true).sent).toHaveLength(0);
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
