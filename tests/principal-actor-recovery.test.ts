import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { fabricTurnProvenance } from "../src/fabric-provenance.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const A = { id: "requester-a", binding: "voice-call" as const };
const B = { id: "requester-b", binding: "herdr-client" as const };
const identity: MeshIdentity = { id: "session:security", name: "main", kind: "main" };
const sender: MeshIdentity = { id: "agent:lead", name: "lead", kind: "agent" };
const cleanup: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); vi.restoreAllMocks(); });
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "principal-actor-recovery-")); cleanup.push(() => fs.rmSync(dir, { recursive: true, force: true })); return dir; };
const waitFor = (check: () => void) => vi.waitFor(check, { timeout: 10_000, interval: 20 });
const fixture = (dir = root(), ownership = { owned: true }) => {
  const pi = { hostCapabilities: { turnProvenance: 1 }, sendMessage: vi.fn(), sendUserMessage: vi.fn(), on: vi.fn() };
  const context = { cwd: process.cwd(), isIdle: () => false, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "security", getEntries: () => [], getBranch: () => [], isPersisted: () => false } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi as any, identity.id, true, process.cwd(), "security");
  main.attachFollowUpDrain(context, 120_000, path.join(dir, "main-followups.json"));
  cleanup.push(() => main.closeFollowUpDrain());
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker-principal-recovery.mjs"), runRoot: path.join(dir, "runs"),
  });
  cleanup.push(() => agents.close());
  const spawned = vi.spyOn(agents, "spawn");
  const actors = new ActorManager("security", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
    ({ actor, message }) => main.deliverAgent({ from: { id: actor.id, name: actor.name, kind: "actor" }, verification: "mesh", principal: message.principal, message: message.text!, delivery: "steer", deliveryId: message.id }),
    { actorRoot: path.join(dir, "actors"), persistent: true, canManageActor: () => ownership.owned, closeGraceMs: 100 });
  cleanup.push(() => actors.close());
  return { dir, ownership, actors, agents, mesh, pi, spawned };
};
const queue = (dir: string, id: string) => fs.readdirSync(path.join(dir, "actors", id)).filter(name => /^queue-.+\.json$/.test(name))
  .flatMap(name => JSON.parse(fs.readFileSync(path.join(dir, "actors", id, name), "utf8")).items as any[]);
const outgoing = (h: ReturnType<typeof fixture>, id: string) => h.actors.messages(id).filter(m => m.direction === "out" && !m.error);
const session = (dir: string, id: string) => path.join(dir, "actors", id, "session.jsonl");
const assertOutputs = async (h: ReturnType<typeof fixture>, id: string, expected: typeof A | typeof B | undefined, text: string) => {
  await waitFor(() => expect(outgoing(h, id).some(m => m.text?.includes(text))).toBe(true));
  const result = outgoing(h, id).find(m => m.text?.includes(text))!;
  expect.soft(result.principal).toEqual(expected);
  expect.soft(h.mesh.read({ topic: "fabric.actor.output" }).find(e => e.text?.includes(text))?.principal).toEqual(expected);
  await waitFor(() => expect(h.pi.sendMessage.mock.calls.some(call => String(call[0].content).includes(text))).toBe(true));
  const call = h.pi.sendMessage.mock.calls.find(call => String(call[0].content).includes(text))!;
  expect.soft(call[1].provenance.principal).toEqual(expected);
  await waitFor(() => {
    const records = JSON.parse(fs.readFileSync(path.join(h.dir, "actors", "actors.json"), "utf8"));
    const journal = (Array.isArray(records) ? records : records.actors).find((a: any) => a.id === id).messages;
    expect(journal.find((m: any) => m.id === result.id)).toBeTruthy();
    expect.soft(journal.find((m: any) => m.id === result.id).principal).toEqual(expected);
  });
  const mainJournal = JSON.parse(fs.readFileSync(path.join(h.dir, "main-followups.json"), "utf8"));
  const delivered = mainJournal.items.find((m: any) => m.id === result.id || m.deliveryId === result.id);
  expect(delivered).toBeTruthy();
  expect.soft(delivered.provenance?.principal).toEqual(expected);
  return result;
};

describe("round-three cumulative activation lineage (#821 F3)", () => {
  it.each((["ownership replay", "owner restart"] as const).flatMap(recovery => (["B", "UNKNOWN"] as const).flatMap(origin => (["steer", "followUp"] as const).map(method => ({ recovery, origin, method })))))
    ("A -> $origin via $method never regains A after $recovery", async ({ recovery, origin, method }) => {
      const first = fixture();
      const actor = await first.actors.create({ name: "lineage", instructions: "Harmless", responseMode: "text", delivery: "steer", triggerTurn: false });
      first.actors.tell(actor.id, "RECOVERY_LINEAGE", undefined, { provenance: fabricTurnProvenance(sender, "actor", "mesh", A) });
      await waitFor(() => expect(first.actors.status(actor.id).inFlightRun?.id).toBeTruthy());
      const id = first.actors.status(actor.id).inFlightRun!.id;
      expect(first.agents.outputPrincipal(id)).toEqual(A);
      const append = fs.appendFileSync.bind(fs);
      const spy = vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
        if (String(file).endsWith("steer.jsonl")) expect.soft(queue(first.dir, actor.id)[0].provenance?.principal, "downgrade persisted before steering admission").toBeUndefined();
        return append(file, data, options);
      });
      first.agents[method](id, "FOREIGN_INPUT", undefined, fabricTurnProvenance(sender, method, "mesh", origin === "B" ? B : undefined));
      first.agents.followUp(id, "A_AGAIN", undefined, fabricTurnProvenance(sender, "followUp", "mesh", A));
      spy.mockRestore();
      await waitFor(() => expect(fs.readFileSync(session(first.dir, actor.id), "utf8")).toContain("A_AGAIN"));
      expect.soft(queue(first.dir, actor.id)[0].provenance?.principal).toBeUndefined();
      let after = first;
      if (recovery === "owner restart") {
        await first.actors.close(); await first.agents.close();
        after = fixture(first.dir);
      } else {
        first.ownership.owned = false; first.actors.listOwned();
        await waitFor(() => expect(first.actors.inFlightCount()).toBe(0));
        first.ownership.owned = true; first.actors.listOwned();
      }
      const result = await assertOutputs(after, actor.id, undefined, "RECOVERY_LINEAGE");
      expect(result.text).toContain("FOREIGN_INPUT");
      const relaunched = after.spawned.mock.calls.at(-1)![0];
      expect(relaunched.sessionFile).toBe(session(first.dir, actor.id));
      expect.soft(relaunched.provenance?.principal).toBeUndefined();
      expect(fs.readFileSync(session(first.dir, actor.id), "utf8")).toContain("FOREIGN_INPUT");
    }, 20_000);

  it.each(["ownership replay", "owner restart"] as const)("preserves same-principal steering across %s", async recovery => {
    const first = fixture();
    const actor = await first.actors.create({ name: "same-lineage", instructions: "Harmless", responseMode: "text", delivery: "steer", triggerTurn: false });
    first.actors.tell(actor.id, "RECOVERY_LINEAGE", undefined, { provenance: fabricTurnProvenance(sender, "actor", "mesh", A) });
    await waitFor(() => expect(first.actors.status(actor.id).inFlightRun?.id).toBeTruthy());
    const id = first.actors.status(actor.id).inFlightRun!.id;
    first.agents.steer(id, "SAME_PRINCIPAL", undefined, fabricTurnProvenance(sender, "steer", "mesh", A));
    first.agents.followUp(id, "A_AGAIN", undefined, fabricTurnProvenance(sender, "followUp", "mesh", A));
    await waitFor(() => expect(fs.readFileSync(session(first.dir, actor.id), "utf8")).toContain("A_AGAIN"));
    expect(queue(first.dir, actor.id)[0].provenance.principal).toEqual(A);
    let after = first;
    if (recovery === "owner restart") {
      await first.actors.close(); await first.agents.close();
      after = fixture(first.dir);
    } else {
      first.ownership.owned = false; first.actors.listOwned();
      await waitFor(() => expect(first.actors.inFlightCount()).toBe(0));
      first.ownership.owned = true; first.actors.listOwned();
    }
    const result = await assertOutputs(after, actor.id, A, "RECOVERY_LINEAGE");
    expect(result.text).toContain("SAME_PRINCIPAL");
    expect(after.spawned.mock.calls.at(-1)![0].provenance?.principal).toEqual(A);
  });

  it.each(["B", "UNKNOWN"] as const)("rejects %s steering when its durable downgrade cannot be saved", async origin => {
    const h = fixture();
    const actor = await h.actors.create({ name: "persist-failure", instructions: "Harmless", responseMode: "text", delivery: "steer", triggerTurn: false });
    h.actors.tell(actor.id, "HANG", undefined, { provenance: fabricTurnProvenance(sender, "actor", "mesh", A) });
    await waitFor(() => expect(h.actors.status(actor.id).inFlightRun?.id).toBeTruthy());
    const id = h.actors.status(actor.id).inFlightRun!.id;
    const rename = fs.renameSync.bind(fs);
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to).includes("queue-") && String(to).endsWith(".json")) throw new Error("queue persistence unavailable");
      return rename(from, to);
    });
    expect.soft(() => h.agents.steer(id, "REJECTED_FOREIGN", undefined, fabricTurnProvenance(sender, "steer", "mesh", origin === "B" ? B : undefined))).toThrow(/persist|downgrade/);
    spy.mockRestore();
    const steer = path.join(h.agents.runDirectory(id)!, "steer.jsonl");
    expect.soft(fs.existsSync(steer) ? fs.readFileSync(steer, "utf8") : "").not.toContain("REJECTED_FOREIGN");
    // A retry from A cannot upgrade the conservative in-memory downgrade.
    h.agents.followUp(id, "A_AGAIN", undefined, fabricTurnProvenance(sender, "followUp", "mesh", A));
    await waitFor(() => expect(fs.readFileSync(session(h.dir, actor.id), "utf8")).toContain("A_AGAIN"));
    expect.soft(fs.readFileSync(session(h.dir, actor.id), "utf8")).not.toContain("REJECTED_FOREIGN");
    fs.writeFileSync(session(h.dir, actor.id) + ".release", "go");
    await assertOutputs(h, actor.id, undefined, "HANG");
  });

  it.each([undefined, 99])("clears recovery attribution when the durable lineage version is %s", async version => {
    const first = fixture();
    const actor = await first.actors.create({ name: "legacy-lineage", instructions: "Harmless", responseMode: "text", delivery: "steer", triggerTurn: false });
    first.actors.tell(actor.id, "RECOVERY_LINEAGE", undefined, { provenance: fabricTurnProvenance(sender, "actor", "mesh", A) });
    await waitFor(() => expect(first.actors.status(actor.id).inFlightRun?.id).toBeTruthy());
    first.agents.steer(first.actors.status(actor.id).inFlightRun!.id, "FOREIGN_INPUT");
    await waitFor(() => expect(fs.readFileSync(session(first.dir, actor.id), "utf8")).toContain("FOREIGN_INPUT"));
    await first.actors.close(); await first.agents.close();
    const directory = path.join(first.dir, "actors", actor.id);
    const file = path.join(directory, fs.readdirSync(directory).find(name => /^queue-.+\.json$/.test(name))!);
    const saved = JSON.parse(fs.readFileSync(file, "utf8"));
    // Model the pre-fix recovery record, including records saved before the in-flight flag.
    saved.items[0].provenance = fabricTurnProvenance(sender, "actor", "mesh", A);
    saved.items[0].principalLineageVersion = version;
    delete saved.items[0].resumed;
    fs.writeFileSync(file, JSON.stringify(saved));
    const after = fixture(first.dir);
    const result = await assertOutputs(after, actor.id, undefined, "RECOVERY_LINEAGE");
    expect(result.text).toContain("FOREIGN_INPUT");
    expect.soft(after.spawned.mock.calls.at(-1)![0].provenance?.principal).toBeUndefined();
  });
});

describe("round-three retroactive payload attribution (#821 F4)", () => {
  it.each((["queued", "parked", "restored"] as const).flatMap(storage => (["B", "UNKNOWN", "unverified"] as const).map(origin => ({ storage, origin }))))
    ("selects newest $origin provenance with the payload in $storage coalescing", async ({ storage, origin }) => {
      const first = fixture();
      const actor = await first.actors.create({ name: "coalesced", instructions: "Harmless", topics: ["work.security"], responseMode: "text", delivery: "steer", triggerTurn: false, coalesce: false });
      await first.mesh.publish({ topic: "work.security", from: sender, text: "HANG" });
      await waitFor(() => expect(first.actors.status(actor.id).inFlightRun?.id).toBeTruthy());
      await first.mesh.publish({ topic: "work.security", from: sender, text: "OLD_PAYLOAD", principal: A, data: { key: 1 } });
      await first.mesh.publish({ topic: "work.security", from: sender, text: "NEW_PAYLOAD", principal: origin === "B" ? B : undefined, data: { key: 1, ...(origin === "unverified" ? { bridge: { legacy: true } } : {}) } });
      await waitFor(() => expect(first.actors.status(actor.id).queued).toBe(2));
      let after = first;
      if (storage === "parked") {
        first.ownership.owned = false; first.actors.listOwned();
        await waitFor(() => expect(first.actors.inFlightCount()).toBe(0));
        expect(queue(first.dir, actor.id).some(item => item.payload.text === "OLD_PAYLOAD")).toBe(true);
        first.ownership.owned = true;
        await first.actors.setCoalesceKey(actor.id, "key");
      } else if (storage === "restored") {
        await first.actors.close(); await first.agents.close();
        const file = path.join(first.dir, "actors", "actors.json"), registry = JSON.parse(fs.readFileSync(file, "utf8"));
        (Array.isArray(registry) ? registry : registry.actors).find((a: any) => a.id === actor.id).coalesceKey = "key";
        fs.writeFileSync(file, JSON.stringify(registry));
        after = fixture(first.dir);
        await waitFor(() => expect(after.actors.status(actor.id).inFlightRun?.id).toBeTruthy());
      } else await first.actors.setCoalesceKey(actor.id, "key");
      const selected = queue(first.dir, actor.id).find(item => item.payload.text === "NEW_PAYLOAD");
      expect(selected).toBeTruthy();
      expect.soft(selected.provenance?.principal).toEqual(origin === "B" ? B : undefined);
      if (origin === "unverified") expect.soft(selected.provenance).toBeUndefined();
      fs.writeFileSync(session(first.dir, actor.id) + ".release", "go");
      const result = await assertOutputs(after, actor.id, origin === "B" ? B : undefined, "NEW_PAYLOAD");
      expect(result.text).not.toContain("OLD_PAYLOAD");
      const launched = after.spawned.mock.calls.find(([request]) => request.task.includes("NEW_PAYLOAD"))![0];
      if (origin === "unverified") expect.soft(launched.provenance).toBeUndefined();
      else expect.soft(launched.provenance?.principal).toEqual(origin === "B" ? B : undefined);
      expect(outgoing(after, actor.id).filter(m => m.text?.includes("NEW_PAYLOAD"))).toHaveLength(1);
    }, 20_000);
});
