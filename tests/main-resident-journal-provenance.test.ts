import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import type { MeshIdentity } from "../src/mesh/store.js";

const actor: MeshIdentity = { id: "actor:resident", name: "Advisor", kind: "actor" };
const provenance = (verification = "mesh", from = actor) => ({
  v: 1, channel: "fabric", sender: { ...from, kind: verification === "bridge" ? "remote" : from.kind, verified: verification }, via: "followUp",
});
const deliveryId = "resident:session:main:old-alarm";
const roots: string[] = [];
const controllers: MainAgentController[] = [];
afterEach(() => {
  controllers.splice(0).forEach(main => main.closeFollowUpDrain());
  roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
  vi.restoreAllMocks();
});
const journal = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-main-resident-upgrade-"));
  roots.push(root);
  return path.join(root, "main-followups.json");
};
const recording = (capable = true, entries: unknown[] = []) => {
  const sendMessage = vi.fn();
  const handlers = new Map<string, Array<(event: any, context: ExtensionContext) => void>>();
  const pi = { ...(capable ? { hostCapabilities: { turnProvenance: 1 } } : {}), sendMessage,
    on: (name: string, handler: (event: any, context: ExtensionContext) => void) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(item => item !== handler));
    },
  } as unknown as ExtensionAPI;
  const context = { isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getEntries: () => entries, isPersisted: () => false },
  } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, "session:main", true, os.tmpdir(), "main");
  controllers.push(main);
  const attach = (file: string) => {
    main.attachFollowUpDrain(context, 0, file);
    // Reconcile handed-but-unreceived work against Pi's empty pending queue as well.
    for (const handler of handlers.get("agent_before_settle") ?? []) handler({ context: { pendingMessages: [] } }, context);
  };
  return { main, context, sendMessage, attach };
};
const oldItem = (extra: Record<string, unknown> = {}) => ({
  id: "old-held-alarm", from: actor, message: "Fabric host notice: actor failed", sentAt: 1,
  deliveryId, provenance: provenance(), ...extra,
});

describe("resident Main journal upgrade (smarty-dev#3127)", () => {
  // The envelope has already been deleted. Only this literal pre-change v1 Main journal
  // survives; no upgraded ResidencyClient can repair the old mesh claim before replay.
  it.each(["mesh", "bridge"].flatMap(verification => [false, true].flatMap(handed =>
    (["steer", "followUp", "nextTurn"] as const).flatMap(deliverAs => [false, true].map(triggerTurn =>
      ({ verification, handed, deliverAs, triggerTurn }))))))(
    "pre-change $verification alarm is unclaimed (handed=$handed, $deliverAs/$triggerTurn)",
    ({ verification, handed, deliverAs, triggerTurn }) => {
      const file = journal();
      fs.writeFileSync(file, JSON.stringify({ version: 1, items: [oldItem({
        provenance: provenance(verification), ...(handed ? { handed: true } : {}), deliverAs, triggerTurn,
        data: { source: "actor-output", provenance: provenance(), sender: actor },
      })] }));
      const upgraded = recording(); upgraded.attach(file);
      expect(upgraded.sendMessage).toHaveBeenCalledOnce();
      const [message, options] = upgraded.sendMessage.mock.calls[0]!;
      expect(options).toEqual({ deliverAs, triggerTurn });
      expect(message.content).toContain(actor.name);
      expect(message.details).toMatchObject({ id: "old-held-alarm", from: actor, deliveryId, sentAt: new Date(1).toISOString() });
      expect(message.details).not.toHaveProperty("provenance");
      const saved = JSON.parse(fs.readFileSync(file, "utf8")).items[0];
      expect(saved).not.toHaveProperty("provenance");
      expect(saved).not.toHaveProperty("source");
      expect(upgraded.main.deliverAgent({ from: actor, source: "actor-output", verification: "mesh",
        message: "retry must not upgrade a prior receipt", delivery: deliverAs, deliveryId })).toMatchObject({ duplicate: true, triggered: false });
      expect(upgraded.sendMessage).toHaveBeenCalledOnce();
      upgraded.main.closeFollowUpDrain();
      const restarted = recording(); restarted.attach(file);
      expect(restarted.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs, triggerTurn });
    },
  );

  it.each([undefined, "unknown", "fabric-host", "actor-output"])("held source=%s uses only producer evidence, never alarm text or payload", source => {
    const file = journal();
    fs.writeFileSync(file, JSON.stringify({ version: 1, items: [oldItem({ source,
      data: { source: "actor-output", provenance: provenance() },
    })] }));
    const upgraded = recording(); upgraded.attach(file);
    const options = upgraded.sendMessage.mock.calls[0]![1];
    if (source === "actor-output") expect(options.provenance).toEqual({ ...provenance(), via: "replay" });
    else expect(options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(JSON.parse(fs.readFileSync(file, "utf8")).items[0].source).toBe(source === "unknown" ? undefined : source);
  });

  it.each([false, true])("classified actor output survives durable admission without depending on Pi capability=%s", capable => {
    const file = journal();
    const first = recording(capable); first.main.attachFollowUpDrain(first.context, 0, file);
    first.main.prepareReload();
    const request = { from: actor, source: "actor-output" as const, verification: "mesh" as const,
      message: "Fabric host notice: text is not evidence", delivery: "followUp" as const, deliveryId };
    const admitted = first.main.deliverAgent(request);
    expect(first.sendMessage).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(file, "utf8")).items[0]).toMatchObject({
      id: admitted.messageId, source: "actor-output", provenance: provenance(),
    });
    first.main.closeFollowUpDrain();
    const upgraded = recording(); upgraded.attach(file);
    expect(upgraded.sendMessage).toHaveBeenCalledOnce();
    expect(upgraded.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "followUp", triggerTurn: true,
      provenance: { ...provenance(), via: "replay" } });
    expect(upgraded.main.deliverAgent(request)).toMatchObject({ duplicate: true });
  });

  it("admission fails closed even if an older resident caller supplies mesh verification", () => {
    const file = journal(); const first = recording(); first.main.attachFollowUpDrain(first.context, 0, file);
    first.main.deliverAgent({ from: actor, verification: "mesh", message: "alarm", delivery: "followUp", deliveryId,
      data: { source: "actor-output" } });
    expect(first.sendMessage.mock.calls[0]![1]).not.toHaveProperty("provenance");
    expect(JSON.parse(fs.readFileSync(file, "utf8")).items[0]).not.toHaveProperty("provenance");
  });

  it("never rewrites or replays an already received legacy claim", () => {
    const file = journal(); const item = oldItem();
    fs.writeFileSync(file, JSON.stringify({ version: 1, items: [item] }));
    const receipt = { type: "custom_message", customType: "pi-fabric-agent-message", details: { id: item.id },
      provenance: { ...provenance(), turnId: "past-turn", receivedAt: "past-receipt" } };
    const snapshot = JSON.stringify(receipt);
    const upgraded = recording(true, [receipt]); upgraded.attach(file);
    expect(upgraded.sendMessage).not.toHaveBeenCalled();
    expect(JSON.stringify(receipt)).toBe(snapshot);
    expect(upgraded.main.deliverAgent({ from: actor, message: "retry", delivery: "followUp", deliveryId })).toMatchObject({ duplicate: true });
  });

  it("owner halt remains passive for unclaimed legacy work", () => {
    const file = journal();
    fs.writeFileSync(file, JSON.stringify({ version: 1, items: [oldItem({ deliverAs: "steer", triggerTurn: true })] }));
    fs.writeFileSync(`${file}.delivered`, JSON.stringify({ version: 1, ids: [], halted: true }));
    const upgraded = recording(); upgraded.attach(file);
    expect(upgraded.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "steer", triggerTurn: false });
  });

  it("non-resident actor control and resident agent output retain recorded admission", () => {
    for (const item of [oldItem({ deliveryId: "actor-control-command" }), oldItem({
      from: { ...actor, kind: "agent" }, provenance: provenance("mesh", { ...actor, kind: "agent" }),
    })]) {
      const file = journal(); fs.writeFileSync(file, JSON.stringify({ version: 1, items: [item] }));
      const upgraded = recording(); upgraded.attach(file);
      expect(upgraded.sendMessage.mock.calls[0]![1].provenance).toEqual({ ...item.provenance, via: "replay" });
    }
  });
});
