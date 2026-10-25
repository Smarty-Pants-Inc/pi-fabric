import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  BRIDGE_NATS_LEDGER_FILE,
  BRIDGE_NATS_LEDGER_LOCK,
  BridgeNatsLedgerError,
  openBridgeNatsLedger,
  type FileBridgeNatsLedger,
} from "../src/mesh/bridge-nats-ledger.js";
import {
  BRIDGE_NATS_HORIZON_MS,
  bridgeNatsEnvelopeId,
  bridgeNatsMappedCommandId,
  type BridgeNatsAckRecord,
  type BridgeNatsClaimInput,
} from "../src/mesh/bridge-nats-types.js";

const LINK = "v2";
const LEAD = "session:00000000-0000-4000-8000-000000000001";
const MOVED = "session:00000000-0000-4000-8000-000000000002";
const OTHER = "session:00000000-0000-4000-8000-000000000003";
const T0 = 1_800_000_000_000;

let root: string;
let open: FileBridgeNatsLedger[] = [];
const clock = { now: T0 };

beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-ledger-"));
  clock.now = T0;
});
afterEach(async () => {
  for (const ledger of open) await ledger.close().catch(() => undefined);
  open = [];
  fs.rmSync(root, { recursive: true, force: true });
});

const dir = () => path.join(root, "ledger");
const ledgerAt = async (where = dir(), minCompactLines?: number) => {
  const ledger = await openBridgeNatsLedger({ dir: where, clock: () => clock.now, ...(minCompactLines ? { minCompactLines } : {}) });
  open.push(ledger);
  return ledger;
};

/** File -> NATS: the lead sends a followUp to the moved lead. */
const toNats = (original = "orig-1", fields: Partial<BridgeNatsClaimInput> = {}): BridgeNatsClaimInput => ({
  direction: "toNats", originalCommandId: original, commandId: bridgeNatsMappedCommandId(LINK, "toNats", LEAD, original),
  sourceEventId: `file-event-${original}`, envelopeId: bridgeNatsEnvelopeId(LINK, `file-event-${original}`),
  bodyDigest: "d".repeat(64), sourceMachine: "ryzen1-bridge", requesterId: LEAD, replyTo: "host-lead", targetId: MOVED,
  operation: "followUp", ownerHostId: "host-moved", ownerIdentityId: MOVED, ownerIncarnation: "inc-1", targetMachine: "lead-v2",
  deadlineAt: T0 + 30_000, resolvedBody: { version: 1, message: "hello", requestedAt: T0, deadlineAt: T0 + 30_000 }, ...fields,
});

/** NATS -> file: the moved lead sends a followUp to the lead. */
const toFile = (original = "orig-2", sender = MOVED, fields: Partial<BridgeNatsClaimInput> = {}): BridgeNatsClaimInput => ({
  direction: "toFile", originalCommandId: original, commandId: bridgeNatsMappedCommandId(LINK, "toFile", sender, original),
  sourceEventId: `nats-envelope-${original}`, bodyDigest: "e".repeat(64), sourceMachine: "lead-v2", requesterId: sender,
  replyTo: "host-moved", targetId: LEAD, operation: "followUp", ownerHostId: "host-lead", ownerIdentityId: LEAD,
  deadlineAt: T0 + 30_000, ...fields,
});

const ack = (commandId: string, accepted = true, extra: Record<string, unknown> = {}): BridgeNatsAckRecord => ({
  v: 1, commandId, accepted, fromId: MOVED, ackEventId: `ack:${commandId.slice(0, 8)}`,
  data: { version: 1, commandId: "orig-1", targetId: MOVED, accepted, ...extra }, receivedAt: clock.now,
});

describe("claims: exactly once by mapped commandId", () => {
  it("claims once, then reports duplicate, conflict or mismatch without writing", async () => {
    const ledger = await ledgerAt();
    const first = await ledger.claim(toNats());
    expect(first).toMatchObject({ status: "new", record: { state: "claimed", claimedAt: T0, expiresAt: T0 + 30_000 + BRIDGE_NATS_HORIZON_MS } });
    expect(Object.isFrozen(first.record)).toBe(true);
    const bytes = fs.statSync(path.join(dir(), BRIDGE_NATS_LEDGER_FILE)).size;
    expect((await ledger.claim(toNats())).status).toBe("duplicate");
    expect((await ledger.claim(toNats("orig-1", { bodyDigest: "f".repeat(64) }))).status).toBe("conflict");
    expect((await ledger.claim(toNats("orig-1", { replyTo: "elsewhere" }))).status).toBe("mismatch");
    expect((await ledger.claim(toNats("orig-1", { sourceMachine: "other-bridge" }))).status).toBe("mismatch");
    expect(fs.statSync(path.join(dir(), BRIDGE_NATS_LEDGER_FILE)).size).toBe(bytes);
    expect(ledger.lookup(first.record.commandId)?.replyTo).toBe("host-lead");
  });

  it("keeps two senders that reuse one original id apart (the mapping, both ways)", async () => {
    const ledger = await ledgerAt();
    const a = await ledger.claim(toFile("same", MOVED));
    const b = await ledger.claim(toFile("same", OTHER, { replyTo: "host-other", sourceMachine: "other-v2" }));
    expect(a.status).toBe("new");
    expect(b.status).toBe("new");
    expect(a.record.commandId).not.toBe(b.record.commandId);
    expect(ledger.lookupOriginal("toFile", MOVED, "same")?.replyTo).toBe("host-moved");
    expect(ledger.lookupOriginal("toFile", OTHER, "same")?.replyTo).toBe("host-other");
    expect(ledger.lookupOriginal("toNats", MOVED, "same")).toBeUndefined();
  });

  it("serializes concurrent claims: exactly one is new", async () => {
    const ledger = await ledgerAt();
    const results = await Promise.all(Array.from({ length: 8 }, () => ledger.claim(toNats())));
    expect(results.filter(result => result.status === "new")).toHaveLength(1);
    expect(results.filter(result => result.status === "duplicate")).toHaveLength(7);
  });

  it("validates claims: ids, required NATS fields, cancel mapping", async () => {
    const ledger = await ledgerAt();
    await expect(ledger.claim(toNats("x", { commandId: "not-hex" }))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    const { envelopeId: _e, ...noEnvelope } = toNats("y");
    await expect(ledger.claim(noEnvelope)).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    const { resolvedBody: _b, ...noBody } = toNats("z");
    await expect(ledger.claim(noBody)).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.claim(toFile("w", MOVED, { envelopeId: "early" }))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.claim(toFile("c", MOVED, { operation: "cancel" }))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    const cancel = await ledger.claim(toFile("c", MOVED, { operation: "cancel", originalCancelCommandId: "orig-2",
      cancelCommandId: bridgeNatsMappedCommandId(LINK, "toFile", MOVED, "orig-2") }));
    expect(cancel.record.cancelCommandId).toBe(bridgeNatsMappedCommandId(LINK, "toFile", MOVED, "orig-2"));
  });
});

describe("states", () => {
  it("moves claimed -> uncertain -> sent -> acked -> ackForwarded, never backwards", async () => {
    const ledger = await ledgerAt();
    const { commandId } = (await ledger.claim(toNats())).record;
    expect((await ledger.markUncertain(commandId)).state).toBe("uncertain");
    expect(ledger.pendingResends().map(record => record.commandId)).toEqual([commandId]);
    expect((await ledger.markSent(commandId, { sequence: 7 }))).toMatchObject({ state: "sent", destinationSequence: 7 });
    expect((await ledger.markUncertain(commandId)).state).toBe("sent"); // a late uncertain never downgrades
    expect(ledger.pendingResends()).toEqual([]);
    expect((await ledger.settle(commandId, ack(commandId))).state).toBe("acked");
    expect(ledger.pendingAcks().map(record => record.commandId)).toEqual([commandId]);
    expect((await ledger.markAckForwarded(commandId)).state).toBe("ackForwarded");
    expect(ledger.pendingAcks()).toEqual([]);
    expect((await ledger.markAckForwarded(commandId)).state).toBe("ackForwarded");
  });

  it("records the first owner ACK only; a replay or a different ACK changes nothing", async () => {
    const ledger = await ledgerAt();
    const { commandId } = (await ledger.claim(toNats())).record;
    await ledger.settle(commandId, ack(commandId, true));
    const after = await ledger.settle(commandId, ack(commandId, false, { error: "late" }));
    expect(after.ack?.accepted).toBe(true);
    await expect(ledger.settle(commandId, ack("0".repeat(64)))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.settle("1".repeat(64), ack("1".repeat(64)))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_UNKNOWN" });
  });

  it("an ACK may arrive before the file append receipt; the destination id is still recorded once", async () => {
    const ledger = await ledgerAt();
    const { commandId } = (await ledger.claim(toFile())).record;
    await expect(ledger.markSent(commandId, {})).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await ledger.settle(commandId, { ...ack(commandId), fromId: LEAD });
    const sent = await ledger.markSent(commandId, { envelopeId: "file-event-9", sequence: 9 });
    expect(sent).toMatchObject({ state: "acked", envelopeId: "file-event-9" });
    await expect(ledger.markSent(commandId, { envelopeId: "file-event-10" })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
  });

  it("dedupes events by direction and keeps the first receipt", async () => {
    const ledger = await ledgerAt();
    expect(ledger.seen("toFile", "nats-1")).toBeUndefined();
    const first = await ledger.markSeen("toFile", "nats-1", { destinationId: "evt-a", destinationSequence: 3 });
    expect((await ledger.markSeen("toFile", "nats-1", { destinationId: "evt-b" })).destinationId).toBe(first.destinationId);
    expect(ledger.seen("toNats", "nats-1")).toBeUndefined();
  });
});

describe("crash durability", () => {
  it("reopens with every resolved mutation: claims, uncertain bytes, ACKs, the mapping and seen events", async () => {
    let ledger = await ledgerAt();
    const sent = (await ledger.claim(toNats("a"))).record.commandId;
    const uncertain = (await ledger.claim(toNats("b"))).record.commandId;
    const back = (await ledger.claim(toFile("c"))).record.commandId;
    await ledger.markSent(sent, { sequence: 1 });
    await ledger.settle(sent, ack(sent));
    await ledger.markUncertain(uncertain);
    await ledger.markSeen("toFile", "nats-envelope-c", { destinationId: "evt" });
    await ledger.close();
    ledger = await ledgerAt();
    expect(ledger.lookup(sent)?.state).toBe("acked");
    expect(ledger.pendingAcks().map(record => record.commandId)).toEqual([sent]);
    // The resend after a restart uses the very same envelope id and bytes.
    const resend = ledger.pendingResends().find(record => record.commandId === uncertain)!;
    expect(resend).toMatchObject({ state: "uncertain", envelopeId: bridgeNatsEnvelopeId(LINK, "file-event-b"),
      resolvedBody: toNats("b").resolvedBody });
    expect(ledger.pendingResends().map(record => record.commandId)).toEqual([uncertain, back]);
    expect(ledger.lookupOriginal("toFile", MOVED, "c")?.commandId).toBe(back);
    expect(ledger.seen("toFile", "nats-envelope-c")?.destinationId).toBe("evt");
    expect((await ledger.claim(toNats("a"))).status).toBe("duplicate"); // exactly once across the restart
  });

  it("survives a process that died without closing (the stale lock is recovered)", async () => {
    const ledger = await ledgerAt();
    await ledger.claim(toNats("a"));
    // Simulate a crash: the holder vanished without close(); its lock names a dead pid.
    const lock = path.join(dir(), BRIDGE_NATS_LEDGER_LOCK);
    const record = JSON.parse(fs.readFileSync(lock, "utf8")) as Record<string, unknown>;
    const dead = spawnSync(process.execPath, ["-e", "process.stdout.write(String(process.pid))"], { encoding: "utf8" });
    open = [];
    fs.writeFileSync(lock, `${JSON.stringify({ ...record, pid: Number(dead.stdout), incarnation: undefined })}\n`);
    const reopened = await ledgerAt();
    expect((await reopened.claim(toNats("a"))).status).toBe("duplicate");
  });

  it("cuts an unfinished final append and opens", async () => {
    let ledger = await ledgerAt();
    await ledger.claim(toNats("a"));
    await ledger.close();
    const file = path.join(dir(), BRIDGE_NATS_LEDGER_FILE);
    const good = fs.statSync(file).size;
    fs.appendFileSync(file, '{"t":"cmd","r":{"v":1,"comm'); // torn: no newline
    ledger = await ledgerAt();
    expect(fs.statSync(file).size).toBe(good);
    expect(ledger.lookup(bridgeNatsMappedCommandId(LINK, "toNats", LEAD, "a"))).toBeDefined();
  });

  it("cuts a final line that fails its checksum (an append whose fsync never returned)", async () => {
    let ledger = await ledgerAt();
    await ledger.claim(toNats("a"));
    await ledger.close();
    const file = path.join(dir(), BRIDGE_NATS_LEDGER_FILE);
    const good = fs.statSync(file).size;
    fs.appendFileSync(file, "\u0000\u0000\u0000\n");
    ledger = await ledgerAt();
    expect(fs.statSync(file).size).toBe(good);
  });

  it("refuses to open over a damaged record followed by valid ones", async () => {
    let ledger = await ledgerAt();
    await ledger.claim(toNats("a"));
    await ledger.claim(toNats("b"));
    await ledger.close();
    const file = path.join(dir(), BRIDGE_NATS_LEDGER_FILE);
    const lines = fs.readFileSync(file, "utf8").split("\n");
    lines[0] = lines[0]!.replace('"orig', '"ORIG'); // checksum no longer matches
    fs.writeFileSync(file, lines.join("\n"), { mode: 0o600 });
    await expect(openBridgeNatsLedger({ dir: dir(), clock: () => clock.now })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CORRUPT" });
    // A refused open leaves no lock behind.
    expect(fs.existsSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK))).toBe(false);
    void ledger;
  });

  it("fails stop on a write error, leaves no torn line, and a reopen recovers", async () => {
    let ledger = await ledgerAt();
    const kept = (await ledger.claim(toNats("a"))).record.commandId;
    const real = fs.writeSync;
    const spy = vi.spyOn(fs, "writeSync").mockImplementationOnce(((fd: number, buffer: Buffer, offset?: number) => {
      real(fd, buffer, offset ?? 0, 10); // a partial write, then the disk is full
      throw Object.assign(new Error("ENOSPC: no space left on device"), { code: "ENOSPC" });
    }) as unknown as typeof fs.writeSync);
    await expect(ledger.claim(toNats("b"))).rejects.toMatchObject({ code: "ENOSPC" });
    spy.mockRestore();
    expect(ledger.lookup(bridgeNatsMappedCommandId(LINK, "toNats", LEAD, "b"))).toBeUndefined(); // never visible
    await expect(ledger.claim(toNats("c"))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
    await ledger.close();
    ledger = await ledgerAt();
    expect(ledger.lookup(kept)).toBeDefined();
    expect((await ledger.claim(toNats("b"))).status).toBe("new");
  });

  it("ignores a compaction that crashed before its rename", async () => {
    let ledger = await ledgerAt();
    await ledger.claim(toNats("a"));
    await ledger.close();
    const stray = path.join(dir(), `${BRIDGE_NATS_LEDGER_FILE}.compact-1-1`);
    fs.writeFileSync(stray, "partial", { mode: 0o600 });
    ledger = await ledgerAt();
    expect(fs.existsSync(stray)).toBe(false);
    expect(ledger.lookup(bridgeNatsMappedCommandId(LINK, "toNats", LEAD, "a"))).toBeDefined();
  });
});

describe("horizon and compaction", () => {
  it("drops records past the 24 h horizon and compacts the file; live ones survive a reopen", async () => {
    let ledger = await ledgerAt(dir(), 4);
    const old = (await ledger.claim(toNats("old"))).record.commandId;
    await ledger.markSeen("toFile", "old-event");
    clock.now = T0 + 30_000 + BRIDGE_NATS_HORIZON_MS + 1;
    expect(ledger.lookup(old)).toBeUndefined();
    expect(ledger.seen("toFile", "old-event")).toBeUndefined();
    const fresh = (await ledger.claim(toNats("fresh", { deadlineAt: clock.now + 30_000 }))).record.commandId;
    for (const state of ["markUncertain", "markSent"] as const) {
      if (state === "markSent") await ledger.markSent(fresh, {}); else await ledger.markUncertain(fresh);
    }
    await ledger.settle(fresh, ack(fresh));
    await ledger.markAckForwarded(fresh);
    const file = path.join(dir(), BRIDGE_NATS_LEDGER_FILE);
    const lines = fs.readFileSync(file, "utf8").trim().split("\n");
    expect(lines.length).toBeLessThanOrEqual(4);
    expect(lines.some(line => line.includes(old))).toBe(false);
    await ledger.close();
    ledger = await ledgerAt(dir(), 4);
    expect(ledger.lookup(fresh)?.state).toBe("ackForwarded");
    // An expired claim no longer blocks a new one with the same id (the destination's window is over too).
    expect((await ledger.claim(toNats("old", { deadlineAt: clock.now + 30_000 }))).status).toBe("new");
  });
});

describe("one writer, private files", () => {
  it("refuses a second open while the first holds the lock, and opens after close", async () => {
    const first = await ledgerAt();
    await expect(openBridgeNatsLedger({ dir: dir() })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_LOCKED" });
    await first.close();
    await expect(first.claim(toNats())).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
    expect((await (await ledgerAt()).claim(toNats())).status).toBe("new");
  });

  it("never steals a lock it cannot prove stale", async () => {
    fs.mkdirSync(dir(), { mode: 0o700 });
    fs.writeFileSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK), "not json\n", { mode: 0o600 });
    await expect(openBridgeNatsLedger({ dir: dir() })).rejects.toBeInstanceOf(BridgeNatsLedgerError);
    fs.writeFileSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK), `${JSON.stringify({ v: 1, pid: process.pid, machineId: "f".repeat(32), bootId: "x" })}\n`);
    await expect(openBridgeNatsLedger({ dir: dir() })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_LOCKED" });
  });

  it.skipIf(!process.geteuid)("creates a 0700 directory and a 0600 file, and refuses a shared directory", async () => {
    const ledger = await ledgerAt();
    await ledger.claim(toNats());
    expect(fs.statSync(dir()).mode & 0o777).toBe(0o700);
    expect(fs.statSync(path.join(dir(), BRIDGE_NATS_LEDGER_FILE)).mode & 0o777).toBe(0o600);
    const shared = path.join(root, "shared");
    fs.mkdirSync(shared, { mode: 0o755 });
    fs.chmodSync(shared, 0o755);
    await expect(openBridgeNatsLedger({ dir: shared })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_UNSAFE" });
  });
});
