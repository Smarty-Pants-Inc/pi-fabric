import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it } from "vitest";
import { BridgeOwnershipError, MeshBridge, type BridgePresence, type BridgeSide } from "../src/mesh/bridge.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });

// Keep: #1661's failure-path diagnostics must reveal capped AND duplicate misses without a broker outage.
it.each(["filter", "commit"] as const)("reports every suppressed %s sample and resets the period", async stage => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-miss-diagnostics-"));
  const peer: MeshIdentity = { id: "session:22222222-2222-4222-8222-222222222222", name: "peer", kind: "main" };
  const now = Date.now();
  const presence: BridgePresence = {
    hosts: [{ record: { format: 1, id: peer.id, rootId: peer.id, identity: peer,
      startedAt: now, updatedAt: now, expiresAt: now + 300_000 }, observedAt: now, expiresAt: now + 300_000 }],
    participants: [], reserved: [],
  };
  const queue: MeshEvent[] = [];
  const logs: string[] = [];
  let clock = 10_000;
  const empty = async (): Promise<BridgePresence> => ({ hosts: [], participants: [], reserved: [] });
  const side = (): BridgeSide => ({ latestSequence: async () => 0,
    read: async after => ({ events: [], through: after }), presence: empty, mirror: async () => undefined,
    publish: async () => ({ sequence: 1 }), bridgedIds: async after => ({ ids: [], through: after, done: true }) });
  const local: BridgeSide = { ...side(),
    // A native reservation is a definitive filter miss. Unknown canonical recipients
    // now retain their cursor until a post-miss membership sample (covered by the link tests).
    presence: async () => stage === "filter" ? { ...presence, reserved: [peer.id] } : empty(),
    read: async after => ({ events: queue.filter(event => event.sequence > after), through: queue.at(-1)?.sequence ?? after }),
    owned: async () => stage === "commit" ? presence : empty(),
  };
  const remote: BridgeSide = { ...side(), presence: async () => presence,
    publish: async () => { throw new BridgeOwnershipError("seeded commit refusal"); } };
  const bridge = new MeshBridge({ localName: "hub", remoteName: "peer", local, remote,
    cursorPath: path.join(root, "cursor.json"), clock: () => clock, log: line => logs.push(line) });
  cleanups.push(async () => { await bridge.stop(); fs.rmSync(root, { recursive: true, force: true }); });
  await bridge.start();
  const publish = (id: string) => queue.push({ id, sequence: queue.length + 1, createdAt: now, verification: "mesh",
    topic: "fleet.work.tasks", kind: "answer", from: peer, to: peer.id, text: "PRIVATE-body" });
  publish("duplicate");
  await bridge.step();
  publish("duplicate"); // Dedupe while still below the five-sample cap.
  for (let i = 2; i <= 7; i++) publish(`miss-${i}`);
  await bridge.step();
  const samples = () => logs.filter(line => line.startsWith("bridge miss:"));
  const reports = () => logs.filter(line => line.startsWith("bridge:"));
  expect(samples()).toHaveLength(5);
  expect(samples().every(line => line.endsWith(`stage=${stage}`))).toBe(true);
  // Eight attempts: five emitted, one duplicate suppressed, two over the cap.
  clock += 60_000;
  publish("next-period");
  await bridge.step();
  expect(samples()).toHaveLength(6);
  if (stage === "filter") expect(reports().at(-1)).toContain("suppressed miss samples 3");
  else {
    // With no filter aggregate, the first suppressed commit emits immediately, then two accumulate.
    expect(reports()).toHaveLength(2);
    expect(reports()[0]).toContain("suppressed miss samples 1");
    expect(reports()[1]).toContain("suppressed miss samples 2");
    expect(reports()[1]).toContain("0 events not addressed across");
  }
  clock += 60_000;
  publish("after-reset");
  await bridge.step();
  if (stage === "filter") expect(reports().at(-1)).toContain("suppressed miss samples 0");
  else {
    publish("after-reset");
    await bridge.step();
    expect(reports()).toHaveLength(3);
    expect(reports().at(-1)).toContain("suppressed miss samples 1");
  }
  expect(logs.join("\n")).not.toContain("PRIVATE-body");
});
