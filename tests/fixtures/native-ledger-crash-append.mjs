// N12 test-only fault at the REAL candidate claim -> #append -> fs.writeSync boundary.
// SOURCE/Jiti, not dist or an old-reader substitute. No broker/model/native callback/app.
// "crash" persists a prefix of the actual candidate-supplied complete line, then SIGKILLs
// THIS process before writeSync returns. This is controlled partial-persistence injection,
// not a claim that a single regular-file write is atomic against disk/power failure.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createJiti } from "jiti";

const [mode, directory, clockText, inputJson, artifacts] = process.argv.slice(2);
assert.ok(mode === "normal" || mode === "crash", "unknown append fault mode");
assert.ok(directory && artifacts && path.isAbsolute(directory) && path.isAbsolute(artifacts));
assert.ok(Number.isFinite(Number(clockText)));
assert.ok(fs.statSync(artifacts).isDirectory());
const input = JSON.parse(inputJson);
const sourceFile = fileURLToPath(new URL("../../src/mesh/bridge-nats-ledger.ts", import.meta.url));
const candidateSourceSha256 = createHash("sha256").update(fs.readFileSync(sourceFile)).digest("hex");
const jiti = createJiti(import.meta.url, { tryNative: false });
const { openBridgeNatsLedger, BRIDGE_NATS_LEDGER_FILE } = await jiti.import(sourceFile);
const ledger = await openBridgeNatsLedger({ dir: directory, clock: () => Number(clockText), minCompactLines: 10_000 });
const file = path.join(directory, BRIDGE_NATS_LEDGER_FILE), inode = fs.statSync(file);
const realWrite = fs.writeSync.bind(fs), realSync = fs.fsyncSync.bind(fs), realTruncate = fs.ftruncateSync.bind(fs);
const isLedger = fd => {
  const stat = fs.fstatSync(fd);
  return stat.dev === inode.dev && stat.ino === inode.ino;
};
let writeCalls = 0, fsyncCalls = 0, truncateCalls = 0, requestedBytes = 0, persistedBytes = 0;
let visibleAtWrite, visibleAtFsync;
const observe = stage => fs.writeFileSync(path.join(artifacts, "observation.json"), `${JSON.stringify({
  stage, mode, pid: process.pid, candidateSourceSha256, writeCalls, fsyncCalls, truncateCalls,
  requestedBytes, persistedBytes, visibleAtWrite, visibleAtFsync,
})}\n`, { mode: 0o600, flag: "wx" });
fs.writeSync = (fd, buffer, offset, length, position) => {
  if (!isLedger(fd)) return realWrite(fd, buffer, offset, length, position);
  writeCalls++;
  assert.equal(writeCalls, 1, "candidate retried a journal append");
  assert.ok(Buffer.isBuffer(buffer));
  assert.equal(offset, 0);
  assert.equal(length, buffer.length);
  assert.equal(buffer.at(-1), 0x0a);
  assert.equal(buffer.indexOf(0x0a), buffer.length - 1, "not exactly one complete encoded line");
  visibleAtWrite = ledger.lookup(input.direction, input.commandId) !== undefined;
  assert.equal(visibleAtWrite, false, "claim visible before durable append");
  requestedBytes = buffer.length;
  fs.writeFileSync(path.join(artifacts, "candidate-append-line.bin"), buffer, { mode: 0o600, flag: "wx" });
  if (mode === "crash") {
    const prefix = Math.floor(buffer.length / 2);
    assert.ok(prefix > 0 && prefix < buffer.length - 1);
    persistedBytes = realWrite(fd, buffer, 0, prefix, position);
    assert.equal(persistedBytes, prefix);
    realSync(fd); // Deliberately persist ONLY the injected prefix, not the candidate's full line.
    observe("prefix-persisted-before-write-return"); // Diagnostic, NOT a terminal/process receipt.
    process.kill(process.pid, "SIGKILL");
    throw new Error("SIGKILL unexpectedly returned");
  }
  persistedBytes = realWrite(fd, buffer, offset, length, position);
  assert.equal(persistedBytes, requestedBytes);
  return persistedBytes;
};
fs.fsyncSync = fd => {
  if (isLedger(fd)) {
    fsyncCalls++;
    visibleAtFsync = ledger.lookup(input.direction, input.commandId) !== undefined;
    assert.equal(visibleAtFsync, false, "claim visible before fsync returned");
  }
  return realSync(fd);
};
fs.ftruncateSync = (fd, length) => {
  if (isLedger(fd)) truncateCalls++;
  return realTruncate(fd, length);
};
try {
  const result = await ledger.claim(input); // Actual candidate encoder/checksum/append/mutation queue.
  assert.equal(mode, "normal", "crash injection was not reached");
  assert.equal(result.created, true);
  assert.deepEqual(ledger.lookup(input.direction, input.commandId), result.record);
  assert.equal(writeCalls, 1);
  assert.equal(fsyncCalls, 1);
  assert.equal(truncateCalls, 0);
  observe("claim-resolved-after-fsync");
} finally {
  fs.writeSync = realWrite;
  fs.fsyncSync = realSync;
  fs.ftruncateSync = realTruncate;
  await ledger.close(); // Not entered by SIGKILL. The old reader must recover the real dead lock.
}
