import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NATIVE_INGRESS_DIRECTORY, readNativeIngressReceipt } from "../src/mesh/bridge-nats-ingress.js";
import {
  BRIDGE_NATS_LEDGER_FILE,
  BRIDGE_NATS_LEDGER_LOCK,
  BridgeNatsLedgerError,
  bridgeNatsClaimAction,
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

/** A real crash needs a separate process and machine evidence (the custody lock's dead-holder proof). */
const realCrash = process.platform === "linux" && fs.existsSync("/etc/machine-id") &&
  spawnSync("bun", ["--version"], { encoding: "utf8" }).status === 0;
const LEDGER_MODULE = fileURLToPath(new URL("../src/mesh/bridge-nats-ledger.ts", import.meta.url));

/** Open the ledger in another process (bun runs the TypeScript module), optionally claim, and hold it until killed. */
const holdInChild = async (where: string, claim?: BridgeNatsClaimInput): Promise<{ kill(): Promise<void> }> => {
  const script = path.join(root, `holder-${Math.random().toString(16).slice(2)}.ts`);
  fs.writeFileSync(script, [
    `import { openBridgeNatsLedger } from ${JSON.stringify(LEDGER_MODULE)};`,
    "const [where, claim] = process.argv.slice(2);",
    "const ledger = await openBridgeNatsLedger({ dir: where });",
    "if (claim) await ledger.claim(JSON.parse(claim));",
    "process.stdout.write('ready\\n');",
    "setInterval(() => {}, 1 << 30);",
  ].join("\n"));
  const child = spawn("bun", [script, where, ...(claim ? [JSON.stringify(claim)] : [])], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.on("data", chunk => { stderr += String(chunk); });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on("data", chunk => { if (String(chunk).includes("ready")) resolve(); });
    child.once("exit", code => reject(new Error(`holder exited ${code}: ${stderr}`)));
  });
  return {
    kill: () => new Promise<void>(resolve => { child.once("exit", () => resolve()); child.kill("SIGKILL"); }),
  };
};
const ledgerAt = async (where = dir(), minCompactLines?: number) => {
  const ledger = await openBridgeNatsLedger({ dir: where, clock: () => clock.now, ...(minCompactLines ? { minCompactLines } : {}) });
  open.push(ledger);
  return ledger;
};

/** File -> NATS: the lead sends a followUp to the moved lead. */
const toNats = (original = "orig-1", fields: Partial<BridgeNatsClaimInput> = {}): BridgeNatsClaimInput => ({
  direction: "toNats", origin: "control", originalCommandId: original, commandId: bridgeNatsMappedCommandId(LINK, "toNats", LEAD, original),
  sourceEventId: `file-event-${original}`, envelopeId: bridgeNatsEnvelopeId(LINK, `file-event-${original}`),
  bodyDigest: "d".repeat(64), sourceMachine: "ryzen1-bridge", requesterId: LEAD, replyTo: "host-lead", targetId: MOVED,
  from: { id: LEAD, name: "lead", kind: "main", sessionId: LEAD.slice(8) }, principal: { id: "herdr:paul", binding: "herdr-client" },
  operation: "followUp", ownerHostId: "host-moved", ownerIdentityId: MOVED, ownerIncarnation: "inc-1", targetMachine: "lead-v2",
  deadlineAt: T0 + 30_000, resolvedBody: { version: 1, message: "hello", requestedAt: T0, deadlineAt: T0 + 30_000 }, ...fields,
});

/** NATS -> file: the moved lead sends a followUp to the lead. */
const toFile = (original = "orig-2", sender = MOVED, fields: Partial<BridgeNatsClaimInput> = {}): BridgeNatsClaimInput => ({
  direction: "toFile", origin: "control", originalCommandId: original, commandId: bridgeNatsMappedCommandId(LINK, "toFile", sender, original),
  sourceEventId: `nats-envelope-${original}`, bodyDigest: "e".repeat(64), sourceMachine: "lead-v2", requesterId: sender,
  from: { id: sender, name: "moved", kind: "main" },
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
    expect(ledger.lookup("toNats", first.record.commandId)?.replyTo).toBe("host-lead");
    expect(ledger.lookup("toFile", first.record.commandId)).toBeUndefined(); // looked up by (direction, mapped id)
    expect(first.created).toBe(true);
    expect((await ledger.claim(toNats())).created).toBe(false);
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
    // A topic route (N3) is a followUp the link originates toward NATS; nothing else may claim that origin.
    const topic = await ledger.claim(toNats("t", { origin: "topic", requesterId: "bridge:v2", replyTo: "bridge:v2",
      from: { id: "bridge:v2", name: "mesh-bridge", kind: "main" } }));
    expect(topic.record.origin).toBe("topic");
    await expect(ledger.claim(toNats("t2", { origin: "topic", operation: "steer" }))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.claim(toFile("t3", MOVED, { origin: "topic" }))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.claim(toNats("o", { origin: "other" as never }))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.claim(toFile("f", MOVED, { from: { id: MOVED, name: "m", kind: "bogus" } as never })))
      .rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
    await expect(ledger.claim(toFile("p", MOVED, { principal: { id: "x", binding: "root" } as never })))
      .rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_INVALID" });
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

  it("decides every claim result at once: publish, resend, awaitAck, replayAck or refuse (E2E finding A)", async () => {
    const ledger = await ledgerAt();
    const first = await ledger.claim(toNats());
    expect(bridgeNatsClaimAction(first)).toMatchObject({ action: "publish" });
    const { commandId } = first.record;
    expect(bridgeNatsClaimAction(await ledger.claim(toNats()))).toMatchObject({ action: "resend" }); // claimed
    await ledger.markUncertain(commandId);
    expect(bridgeNatsClaimAction(await ledger.claim(toNats()))).toMatchObject({ action: "resend" }); // uncertain
    await ledger.markSent(commandId, {});
    expect(bridgeNatsClaimAction(await ledger.claim(toNats()))).toMatchObject({ action: "awaitAck" });
    await ledger.settle(commandId, ack(commandId, true, { messageId: "m-1" }));
    const replay = bridgeNatsClaimAction(await ledger.claim(toNats()));
    expect(replay).toMatchObject({ action: "replayAck", ack: { accepted: true, data: { messageId: "m-1" } } });
    await ledger.markAckForwarded(commandId);
    expect(bridgeNatsClaimAction(await ledger.claim(toNats()))).toMatchObject({ action: "replayAck" });
    expect(bridgeNatsClaimAction(await ledger.claim(toNats("orig-1", { bodyDigest: "f".repeat(64) }))))
      .toMatchObject({ action: "refuse", refuse: "BRIDGE_NATS_ID_CONFLICT" });
    expect(bridgeNatsClaimAction(await ledger.claim(toNats("orig-1", { replyTo: "elsewhere" }))))
      .toMatchObject({ action: "refuse", refuse: "BRIDGE_NATS_CLAIM_MISMATCH" });
    // An ACK recorded before the append receipt still replays (never waits for the receipt).
    const early = (await ledger.claim(toFile())).record.commandId;
    await ledger.settle(early, { ...ack(early), fromId: LEAD });
    expect(bridgeNatsClaimAction(await ledger.claim(toFile()))).toMatchObject({ action: "replayAck" });
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
    expect(ledger.lookup("toNats", sent)?.state).toBe("acked");
    expect(ledger.pendingAcks().map(record => record.commandId)).toEqual([sent]);
    // The resend after a restart uses the very same envelope id and bytes.
    const resend = ledger.pendingResends().find(record => record.commandId === uncertain)!;
    expect(resend).toMatchObject({ state: "uncertain", envelopeId: bridgeNatsEnvelopeId(LINK, "file-event-b"),
      resolvedBody: toNats("b").resolvedBody, from: toNats("b").from, principal: toNats("b").principal });
    expect(ledger.pendingResends().map(record => record.commandId)).toEqual([uncertain, back]);
    expect(ledger.lookupOriginal("toFile", MOVED, "c")?.commandId).toBe(back);
    expect(ledger.seen("toFile", "nats-envelope-c")?.destinationId).toBe("evt");
    expect((await ledger.claim(toNats("a"))).status).toBe("duplicate"); // exactly once across the restart
  });

  it.skipIf(!realCrash)("survives a holder process killed without closing: its lock is recovered, its claims kept", async () => {
    const holder = await holdInChild(dir(), toNats("a"));
    // A live holder in another process is never displaced.
    await expect(openBridgeNatsLedger({ dir: dir(), lockTimeoutMs: 300 })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_LOCKED" });
    await holder.kill();
    const reopened = await ledgerAt();
    expect((await reopened.claim(toNats("a"))).status).toBe("duplicate");
  });

  it.skipIf(!realCrash)("lets exactly one of two concurrent openers recover a dead holder's lock", async () => {
    await (await holdInChild(dir())).kill();
    const results = await Promise.allSettled([
      openBridgeNatsLedger({ dir: dir(), lockTimeoutMs: 500 }),
      openBridgeNatsLedger({ dir: dir(), lockTimeoutMs: 500 }),
    ]);
    const opened = results.filter(result => result.status === "fulfilled");
    for (const result of opened) open.push((result as PromiseFulfilledResult<FileBridgeNatsLedger>).value);
    expect(opened).toHaveLength(1);
    expect((results.find(result => result.status === "rejected") as PromiseRejectedResult).reason)
      .toMatchObject({ code: "BRIDGE_NATS_LEDGER_LOCKED" });
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
    expect(ledger.lookup("toNats", bridgeNatsMappedCommandId(LINK, "toNats", LEAD, "a"))).toBeDefined();
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

  it("fails stop after an actual partial write without a second write, rolls back the durable prefix, and preserves available errno/cause", async () => {
    for (const mode of ["short-count", "errno"] as const) {
      let ledger = await ledgerAt(path.join(root, mode));
      const file = path.join(root, mode, BRIDGE_NATS_LEDGER_FILE);
      const kept = (await ledger.claim(toNats("a"))).record.commandId, prefix = fs.readFileSync(file);
      const cause = new Error("storage capacity exhausted"), failure = Object.assign(new Error("ENOSPC: no space left on device", { cause }),
        { code: "ENOSPC", errno: -28 });
      const real = fs.writeSync;
      const spy = vi.spyOn(fs, "writeSync").mockImplementationOnce(((fd: number, buffer: Buffer, offset?: number) => {
        const written = real(fd, buffer, offset ?? 0, 10); // actual bytes, not a fabricated count
        if (mode === "errno") throw failure;
        return written; // writeAll would wrongly issue another write and make this claim visible
      }) as unknown as typeof fs.writeSync);
      try {
        const attempt = ledger.claim(toNats("b"));
        if (mode === "errno") await expect(attempt).rejects.toBe(failure);
        else await expect(attempt).rejects.toThrow(/append was short .*errno unavailable/);
        expect(spy).toHaveBeenCalledOnce();
        expect(fs.readFileSync(file)).toEqual(prefix);
        expect(ledger.lookup("toNats", bridgeNatsMappedCommandId(LINK, "toNats", LEAD, "b"))).toBeUndefined();
        await expect(ledger.claim(toNats("c"))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
        expect(spy).toHaveBeenCalledOnce(); // fail-stop: neither a continuation nor a later append
        if (mode === "errno") { expect(failure.errno).toBe(-28); expect(failure.cause).toBe(cause); }
      } finally { spy.mockRestore(); }
      await ledger.close();
      ledger = await ledgerAt(path.join(root, mode));
      expect(fs.readFileSync(file)).toEqual(prefix); // durable rollback survives close/reopen
      expect(ledger.lookup("toNats", kept)).toBeDefined();
      expect((await ledger.claim(toNats("b"))).status).toBe("new");
    }
  });

  it("refuses to open over a damaged suffix longer than one unfinished append", async () => {
    let ledger = await ledgerAt();
    for (const id of ["a", "b", "c"]) await ledger.claim(toNats(id));
    await ledger.close();
    const file = path.join(dir(), BRIDGE_NATS_LEDGER_FILE);
    const lines = fs.readFileSync(file, "utf8").split("\n");
    for (const index of [1, 2]) lines[index] = lines[index]!.replace('"bodyDigest":"d', '"bodyDigest":"e');
    fs.writeFileSync(file, lines.join("\n"), { mode: 0o600 });
    await expect(openBridgeNatsLedger({ dir: dir(), clock: () => clock.now })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CORRUPT" });
    // One damaged complete line followed by an unfinished segment is two damaged appends too.
    fs.writeFileSync(file, `${lines[0]}\n\u0000\u0000\n{"t":"cmd"`, { mode: 0o600 });
    await expect(openBridgeNatsLedger({ dir: dir(), clock: () => clock.now })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CORRUPT" });
    void ledger;
  });

  // Stated skip: nats-link is Linux/macOS only (POSIX file modes, fsync/rename and lock semantics); start refuses on Windows.
  it.skipIf(process.platform === "win32")("keeps single-write appends while retrying short compaction and receipt writes across reload", async () => {
    let ledger = await ledgerAt(dir(), 1);
    const file = path.join(dir(), BRIDGE_NATS_LEDGER_FILE);
    const ingress = toFile("receipt", MOVED, { operation: "ask" });
    const claim = toNats("a"), { commandId } = claim;
    const real = fs.writeSync, realOpen = fs.openSync;
    const retryable = new Set<number>();
    let appendWrites = 0, shortWrites = 0;
    const openSpy = vi.spyOn(fs, "openSync").mockImplementation(((name: fs.PathLike, flags?: fs.OpenMode, mode?: fs.Mode) => {
      const fd = realOpen(name, flags ?? "r", mode), opened = String(name);
      retryable.delete(fd); // descriptors are reused, including when compaction reopens the append file
      if (opened.startsWith(`${file}.compact-`) ||
          (path.dirname(opened) === path.join(dir(), NATIVE_INGRESS_DIRECTORY) && opened.endsWith(".tmp"))) retryable.add(fd);
      return fd;
    }) as typeof fs.openSync);
    const spy = vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, offset?: number, length?: number) => {
      // Append must get one complete line; only unpublished staging files use writeAll's retry contract.
      if (!retryable.has(fd)) {
        appendWrites += 1;
        return real(fd, buffer, offset ?? 0, length ?? buffer.length);
      }
      shortWrites += 1;
      return real(fd, buffer, offset ?? 0, Math.min(length ?? buffer.length, 20));
    }) as unknown as typeof fs.writeSync);
    try {
      await ledger.claim(claim);
      await ledger.markUncertain(commandId);
      await ledger.markSent(commandId, {});
      await ledger.settle(commandId, ack(commandId));
      await ledger.markAckForwarded(commandId); // the fifth line triggers a compaction
      expect(appendWrites).toBe(5);
      expect(shortWrites).toBeGreaterThan(1);
      expect(fs.readFileSync(file, "utf8").trim().split("\n")).toHaveLength(1);
      await ledger.claim({ ...ingress, resolvedBody: { version: 1, commandId: ingress.commandId, targetId: ingress.targetId,
        operation: ingress.operation, replyTo: ingress.replyTo, message: "hello", requestedAt: T0, deadlineAt: ingress.deadlineAt } });
      shortWrites = 0;
      await ledger.projectNativeIngress(ingress.commandId, LINK);
      expect(appendWrites).toBe(6);
      expect(shortWrites).toBeGreaterThan(1);
      expect(readNativeIngressReceipt(dir(), ingress.commandId)).toMatchObject({ commandId: ingress.commandId, operation: "ask" });
    } finally { spy.mockRestore(); openSpy.mockRestore(); }
    await ledger.close();
    ledger = await ledgerAt(dir(), 1);
    expect(ledger.lookup("toNats", commandId)?.state).toBe("ackForwarded");
    expect(ledger.lookupOriginal("toNats", LEAD, "a")?.commandId).toBe(commandId);
    expect(ledger.lookup("toFile", ingress.commandId)?.state).toBe("claimed");
    expect(readNativeIngressReceipt(dir(), ingress.commandId)).toMatchObject({ commandId: ingress.commandId, operation: "ask" });
  });

  it.skipIf(process.platform !== "linux")("never treats a failed directory fsync as durable", async () => {
    const realOpen = fs.openSync, realFsync = fs.fsyncSync;
    const opened: string[] = [];
    const openSpy = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags?: fs.OpenMode, mode?: fs.Mode) => {
      const fd = realOpen(file, flags ?? "r", mode);
      if (flags === "r" && fs.fstatSync(fd).isDirectory()) opened.push(String(file));
      return fd;
    }) as typeof fs.openSync);
    let failDirs = true;
    // Fail only a directory's fsync (descriptor numbers are reused, so judge the descriptor now).
    const fsyncSpy = vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      if (failDirs && fs.fstatSync(fd).isDirectory()) throw Object.assign(new Error("EIO: i/o error, fsync"), { code: "EIO" });
      realFsync(fd);
    });
    try {
      // A new ledger directory (two new components) whose namespace barrier fails: the open fails.
      const nested = path.join(root, "new", "ledger");
      await expect(openBridgeNatsLedger({ dir: nested, clock: () => clock.now })).rejects.toMatchObject({ code: "EIO" });
      failDirs = false;
      opened.length = 0;
      const deeper = path.join(root, "a", "b", "ledger");
      const ledger = await ledgerAt(deeper, 1);
      // Every newly created component's entry was synced in its parent.
      expect(opened).toEqual(expect.arrayContaining([root, path.join(root, "a"), path.join(root, "a", "b")]));
      const { commandId } = (await ledger.claim(toNats("a"))).record;
      await ledger.markUncertain(commandId);
      await ledger.markSent(commandId, {});
      await ledger.settle(commandId, ack(commandId));
      // The fifth line compacts; its rename cannot be confirmed: the mutation stands (it is in the new
      // file), then the ledger stops appending.
      failDirs = true;
      expect((await ledger.markAckForwarded(commandId)).state).toBe("ackForwarded");
      await expect(ledger.claim(toNats("b"))).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_CLOSED" });
      failDirs = false;
      await ledger.close();
      const reopened = await ledgerAt(deeper, 1);
      expect(reopened.lookup("toNats", commandId)?.state).toBe("ackForwarded");
    } finally { openSpy.mockRestore(); fsyncSpy.mockRestore(); }
  });

  it("ignores a compaction that crashed before its rename", async () => {
    let ledger = await ledgerAt();
    await ledger.claim(toNats("a"));
    await ledger.close();
    const stray = path.join(dir(), `${BRIDGE_NATS_LEDGER_FILE}.compact-1-1`);
    fs.writeFileSync(stray, "partial", { mode: 0o600 });
    ledger = await ledgerAt();
    expect(fs.existsSync(stray)).toBe(false);
    expect(ledger.lookup("toNats", bridgeNatsMappedCommandId(LINK, "toNats", LEAD, "a"))).toBeDefined();
  });
});

describe("horizon and compaction", () => {
  // Stated skip: nats-link is Linux/macOS only (POSIX file modes, fsync/rename and lock semantics); start refuses on Windows.
  it.skipIf(process.platform === "win32")("drops records past the 24 h horizon and compacts the file; live ones survive a reopen", async () => {
    let ledger = await ledgerAt(dir(), 4);
    const old = (await ledger.claim(toNats("old"))).record.commandId;
    await ledger.markSeen("toFile", "old-event");
    clock.now = T0 + 30_000 + BRIDGE_NATS_HORIZON_MS + 1;
    expect(ledger.lookup("toNats", old)).toBeUndefined();
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
    expect(ledger.lookup("toNats", fresh)?.state).toBe("ackForwarded");
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
    fs.mkdirSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK), { recursive: true, mode: 0o700 });
    fs.chmodSync(dir(), 0o700);
    fs.writeFileSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK, "owner"), "not a record\n", { mode: 0o600 });
    await expect(openBridgeNatsLedger({ dir: dir(), lockTimeoutMs: 300 })).rejects.toBeInstanceOf(BridgeNatsLedgerError);
    // A holder on another machine (or an unreadable one) stays, whatever its pid.
    fs.writeFileSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK, "owner"),
      `token\n${process.pid}\n${Date.now()}\n\n${"f".repeat(32)}\n00000000-0000-4000-8000-000000000000\n`);
    await expect(openBridgeNatsLedger({ dir: dir(), lockTimeoutMs: 300 })).rejects.toMatchObject({ code: "BRIDGE_NATS_LEDGER_LOCKED" });
    expect(fs.readFileSync(path.join(dir(), BRIDGE_NATS_LEDGER_LOCK, "owner"), "utf8")).toContain("f".repeat(32));
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
