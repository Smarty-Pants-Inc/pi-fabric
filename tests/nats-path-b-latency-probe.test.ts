// Real Path B probe contract: distinct sender/owner Main processes, real NATS inboxes,
// durable Main journal evidence, and a FILE append counterprobe. No fake control receiver.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { observeFabricPathBProbe, recordFabricPathBProbe } from "../src/transport/probe-hooks.js";
import { runControlProbe, runFileCounterprobe } from "./helpers/nats-path-b-probe.js";
import { createPathBHarness, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: latency probe requires an executable NATS_BIN");
let broker: PathBHarness | undefined;

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  const saved = new Map<string, string | undefined>();
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) {
    saved.set(name, process.env[name]);
    process.env[name] = value;
  }
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally {
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}, 30_000);

afterAll(async () => { await broker?.stop(); });

describe("Path B acceptance probe", () => {
  it("keeps its process-local observer inert unless explicitly installed", () => {
    const observed: unknown[] = [];
    const dispose = observeFabricPathBProbe(event => observed.push(event));
    dispose();
    expect(recordFabricPathBProbe({ type: "main-followup-journal-commit", atNs: "1", commandId: "c", messageId: "m" })).toBe(false);
    expect(observed).toEqual([]);
  });

  it.skipIf(!binary || process.platform !== "linux")("measures accepted Main ACKs and confirms NATS leaves mesh logs/cursors unchanged", async () => {
    const ownerNats = broker!.runtimeOptions(broker!.users.machineA);
    const senderNats = broker!.runtimeOptions(broker!.users.machineB);
    const nats = await runControlProbe({
      n: 2,
      backend: "NATS Path B",
      transport: "nats",
      natsOwner: ownerNats,
      natsSender: senderNats,
      stateBackendHub: "file",
      stateBackendWork: "file",
      loadShape: "synchronized-burst",
      senderProcesses: 2,
      holdAdmission: true,
      holdWaitMs: 10_000,
      deadlineMs: 20_000,
      serverLabel: "LOCAL TEST SERVER",
      syncInterval: "always",
    });

    expect(nats.run.presenceProjection).toBe("pre-projection");
    expect(nats.run.barClaimable).toBe(false);
    expect(nats.primary).toMatchObject({ label: "send -> admission-ACK (sender clock)", count: 2 });
    expect(nats.counts).toEqual({ refusals: 0, redeliveries: 0, duplicateAdmissions: 0, lostMessages: 0, ackTimeouts: 0 });
    expect(nats.checks).toMatchObject({ acceptedCount: 2, allAcceptedHaveMainJournalCommit: true, natsLeavesMeshEventsAndCursorsUnchanged: true });
    expect(nats.evidence.eventsUnchanged).toBe(true);
    expect(nats.evidence.cursorsUnchanged).toBe(true);
    for (const before of Object.values(nats.evidence.eventsJsonlAndArchiveBefore)) {
      expect(before.eventsJsonlSha256).toMatch(/^[0-9a-f]{64}$/);
      expect(before.eventArchive.treeSha256).toMatch(/^[0-9a-f]{64}$/);
      expect(before.eventArchive.fileCount).toBeGreaterThan(0);
    }
    expect(nats.evidence.ownerAdmissionHold).toMatchObject({ releasedBy: "file", senderPendingWhileHeld: true, journalUnchangedWhileHeld: true });
    expect(nats.perSend).toHaveLength(2);
    for (const send of nats.perSend) {
      expect(send.accepted).toBe(true);
      expect(send.commandId).toMatch(/^[0-9a-f]{64}$/);
      expect(send.messageId).toBeTruthy();
      expect(send.logicalAdmissionCount).toBe(1);
      expect(send.primaryLatencyMs).toBeGreaterThanOrEqual(0);
      expect(send.pubAckAtNs).toMatch(/^\d+$/);
      expect(send.ownerAckAtNs).toMatch(/^\d+$/);
      expect(send.ackOfAckAtNs).toMatch(/^\d+$/);
      expect(send.ownerJournalCommitAtNs).toMatch(/^\d+$/);
      expect(send.secondarySameHostJournalCommitMs).toBeGreaterThanOrEqual(0);
    }

    const file = await runFileCounterprobe({ n: 1, stateBackend: "file", loadShape: "synchronized-burst", deadlineMs: 20_000 });
    expect(file.run.backend).toBe("FILE-fixed");
    expect(file.evidence.eventsUnchanged).toBe(false);
    expect(file.evidence.fileControlEvents).toMatchObject({ commandAppends: 1, acceptedAckAppends: 1 });
    expect(file.checks).toMatchObject({ acceptedCount: 1, allAcceptedHaveMainJournalCommit: true });
  }, 120_000);
});
