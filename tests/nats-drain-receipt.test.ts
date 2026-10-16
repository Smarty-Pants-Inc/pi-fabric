import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import {
  natsDrainReceiptPath,
  readNatsDrainReceipt,
  type NatsDrainReceipt,
} from "../src/topology/nats-drain-receipt.js";

const NOW = 1_700_000_000_000;
const HOST = "host:machine-b";
const EXPECTED_STREAM = "FABRIC_IN_machine-b";
const COMMAND = "command:retained-nats";
const identity: MeshIdentity = { id: HOST, name: HOST, kind: "main", sessionId: HOST };
const planes: FabricControlPlane[] = [];
const seenKey = "topology/control-seen/" +
  createHash("sha256").update(`${HOST}\0${COMMAND}`).digest("hex");

const validReceipt = (): NatsDrainReceipt => ({
  version: 1,
  hostId: HOST,
  flippedAt: NOW - 1_000,
  completedAt: NOW - 500,
  complete: true,
  consumer: { stream: EXPECTED_STREAM, numPending: 0, numAckPending: 0 },
  dispositions: [],
});

const fixture = async (receipt: NatsDrainReceipt, markedRecord = true, expectedStream: string | null = EXPECTED_STREAM) => {
  // Retained evidence only: no rm/unlink teardown. Local runs supply guarded custody
  // lock release and private TMPDIR before running this suite with --cache=false.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-drain-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { stateBackend: "file" });
  const seenRoot = path.join(mesh.root, "control-seen",
    createHash("sha256").update(HOST).digest("hex").slice(0, 32));
  const seen = new MeshStore(seenRoot, 64 * 1024, 1_000, { stateBackend: "file" });
  if (markedRecord) {
    const value = {
      format: 1, hostId: HOST, commandId: COMMAND, targetId: "agent:target",
      expiresAt: NOW + 60_000, carrier: "nats", writtenAt: NOW - 2_000,
      acceptance: { accepted: true, messageId: "retained-outcome" },
    };
    await seen.put({ key: seenKey, identity, ifVersion: 0, value });
    // Recover the marked record from disk, not an injected control-plane object.
    const reopened = new MeshStore(seenRoot, 64 * 1024, 1_000, { stateBackend: "file" });
    expect(reopened.get(seenKey, { fresh: true })?.value).toEqual(value);
  }
  fs.mkdirSync(seenRoot, { recursive: true });
  // Bypass the writer so schema-valid unsafe historical/operator receipts reach
  // the real reader and release boundary even if writer validation is strengthened.
  fs.writeFileSync(natsDrainReceiptPath(seenRoot), JSON.stringify(receipt));
  expect(readNatsDrainReceipt(seenRoot)).toEqual(receipt);
  const plane = new FabricControlPlane(mesh, identity, {
    enabled: true, hostId: HOST, now: () => NOW,
    ...(expectedStream === null ? {} : { expectedNatsInboxStream: expectedStream }),
  });
  planes.push(plane);
  expect(plane.transportKind).toBe("file");
  plane.pause();
  return { plane, seenRoot };
};

const expectUnproven = async (receipt: NatsDrainReceipt, reason: string) => {
  const { plane } = await fixture(receipt);
  await expect(plane.checkpointForRelease()).rejects.toMatchObject({
    code: "FABRIC_NATS_DRAIN_UNPROVEN", reason,
  });
  expect(() => plane.releaseCheckpointReceipt()).toThrow();
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  // Retain fixture directories and census evidence; no destructive teardown.
});

// Keep reason: E17 rollback release is a custody/data-safety trust boundary.
// Real paused release API, actual file-backed marked records, no validator mocks.
describe("NATS drain receipt proof at FILE rollback release", () => {
  it("rejects complete:true with numAckPending=1 (ack-pending)", async () => {
    const receipt = validReceipt();
    receipt.consumer!.numAckPending = 1;
    await expectUnproven(receipt, "ack-pending");
  });

  it("rejects complete:true with numPending=1 and no dispositions (dispositions)", async () => {
    const receipt = validReceipt();
    receipt.consumer!.numPending = 1;
    await expectUnproven(receipt, "dispositions");
  });

  it("rejects complete:true for a different stream than trusted host configuration (stream)", async () => {
    const receipt = validReceipt();
    receipt.consumer!.stream = "FABRIC_IN_machine-a";
    await expectUnproven(receipt, "stream");
  });

  it("rejects complete:true when flippedAt follows completedAt (timeline)", async () => {
    const receipt = validReceipt();
    receipt.flippedAt = NOW - 100;
    await expectUnproven(receipt, "timeline");
  });

  it("rejects a receipt completed beyond the approved five-minute clock tolerance (timeline)", async () => {
    const receipt = validReceipt();
    receipt.completedAt = NOW + 5 * 60_000 + 1;
    await expectUnproven(receipt, "timeline");
  });

  it("rejects the historical consumer:null receipt because stream/count binding is absent (consumer)", async () => {
    const receipt = validReceipt();
    receipt.consumer = null;
    await expectUnproven(receipt, "consumer");
  });

  it("rejects duplicate dispositions even when their raw length matches numPending (dispositions)", async () => {
    const receipt = validReceipt();
    receipt.consumer!.numPending = 2;
    receipt.dispositions = [
      { commandEventId: "event:same", disposition: "relay" },
      { commandEventId: "event:same", disposition: "completed" },
    ];
    await expectUnproven(receipt, "dispositions");
  });

  it("rejects excess unique dispositions that do not match numPending (dispositions)", async () => {
    const receipt = validReceipt();
    receipt.dispositions = [{ commandEventId: "event:extra", disposition: "relay" }];
    await expectUnproven(receipt, "dispositions");
  });

  it("refuses marked history without a trusted expected stream (missing-stream)", async () => {
    const { plane } = await fixture(validReceipt(), true, null);
    await expect(plane.checkpointForRelease()).rejects.toMatchObject({
      code: "FABRIC_NATS_DRAIN_UNPROVEN", reason: "missing-stream",
    });
  });

  it("allows a fresh host/stream-bound complete 0/0 receipt", async () => {
    const { plane } = await fixture(validReceipt());
    await expect(plane.checkpointForRelease()).resolves.toBeUndefined();
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it("allows completion exactly at the approved five-minute clock tolerance", async () => {
    const receipt = validReceipt();
    receipt.completedAt = NOW + 5 * 60_000;
    const { plane } = await fixture(receipt);
    await expect(plane.checkpointForRelease()).resolves.toBeUndefined();
  });

  it("allows nonzero pending with exactly matching unique dispositions and no pending ACKs", async () => {
    const receipt = validReceipt();
    receipt.consumer!.numPending = 2;
    receipt.dispositions = [
      { commandEventId: "event:relayed", disposition: "relay" },
      { commandEventId: "event:completed", disposition: "completed" },
    ];
    const { plane } = await fixture(receipt);
    await expect(plane.checkpointForRelease()).resolves.toBeUndefined();
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it("leaves ordinary FILE-only release unchanged without a trusted stream or marked records", async () => {
    const receipt = validReceipt();
    receipt.consumer = null;
    const { plane } = await fixture(receipt, false, null);
    await expect(plane.checkpointForRelease()).resolves.toBeUndefined();
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });
});
