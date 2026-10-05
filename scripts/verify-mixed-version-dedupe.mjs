import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

// Foreground proof: build this head, then nice -n 19 node scripts/verify-mixed-version-dedupe.mjs.
// Every mesh/archive and the actual old-code build are isolated below TMPDIR and removed.
const repo = process.cwd();
const oldCommit = "81c0f9f06c6737022ad75058b23a1640dc82bd4d";
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mixed-dedupe-"));
const oldTree = path.join(scratch, "old");
const from = { id: "session:mixed-version", name: "proof", kind: "main", sessionId: "mixed-version" };
const currentURL = pathToFileURL(path.join(repo, "dist", "mesh.js")).href;
const current = await import(currentURL);
const options = { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 };
const crashCode = "const { MeshStore } = await import(process.argv[1]); const store = new MeshStore(process.argv[2], 1024, 500, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 }); await store.publish(JSON.parse(process.argv[3]));";
const readJSON = file => JSON.parse(fs.readFileSync(file, "utf8"));
let added = false;
try {
  const worktree = spawnSync("git", ["worktree", "add", "--detach", oldTree, oldCommit], { cwd: repo, encoding: "utf8" });
  if (worktree.status !== 0) throw new Error(worktree.stderr || "git worktree add failed");
  added = true;
  const pinned = spawnSync("git", ["rev-parse", "HEAD"], { cwd: oldTree, encoding: "utf8" });
  assert.equal(pinned.status, 0);
  assert.equal(pinned.stdout.trim(), oldCommit);
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(oldTree, "node_modules"), "junction");
  const built = spawnSync("bun", ["run", "build"], { cwd: oldTree, env: { ...process.env }, stdio: "inherit" });
  if (built.status !== 0) throw new Error("old-version build failed");
  const old = await import(pathToFileURL(path.join(oldTree, "dist", "mesh.js")).href);
  for (const scenario of ["after-commit", "before-commit", "before-live-same-topic", "before-live-other-topic"]) {
    const base = path.join(scratch, scenario);
    const root = path.join(base, "mesh");
    const archive = path.join(base, "archive");
    fs.mkdirSync(root, { recursive: true });
    fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir: archive }));
    const packet = { topic: "mesh.mixed-version", from, dedupeKey: scenario, text: "exact-once" };
    const newStore = new current.MeshStore(root, 1024, 500, options);
    const seed = await newStore.publish({ topic: packet.topic, from, text: "seed" });
    assert.equal(seed.sequence, 1);
    const liveFile = path.join(root, "events.jsonl");
    const tailStart = newStore.latestOffset();
    const beforeLive = scenario.startsWith("before-live");
    const fence = beforeLive ? "PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_BEGIN" : scenario === "before-commit" ? "PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_COMMIT" : "PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND";
    const crashed = spawnSync(process.execPath, ["--input-type=module", "-e", crashCode, currentURL, root, JSON.stringify(packet)], {
      cwd: repo, env: { ...process.env, [fence]: "1" }, encoding: "utf8", timeout: 15_000, killSignal: "SIGKILL",
    });
    assert.equal(crashed.signal, "SIGKILL", `crash hook exited unexpectedly: ${crashed.status} ${crashed.stderr}`);
    assert.equal(crashed.error, undefined, "crash must be the protocol fence, not a timeout");
    const indexFile = path.join(archive, "sequence-index", "0", "2.json");
    const indexed = readJSON(indexFile);
    assert.equal(indexed.committed, scenario === "after-commit");
    const archivedFile = path.join(archive, indexed.file);
    const original = JSON.parse(fs.readFileSync(archivedFile, "utf8").slice(indexed.offset, indexed.offset + indexed.length));
    assert.equal(original.sequence, 2);
    assert.equal(original.dedupeKey, packet.dedupeKey);
    const intentName = fs.readdirSync(path.join(root, "event-receipts")).find(name => name.endsWith(".pending.json"));
    assert.ok(intentName, "crash must leave a durable intent");
    const intentFile = path.join(root, "event-receipts", intentName);
    assert.equal(readJSON(intentFile).eventId, original.id);
    assert.equal(fs.existsSync(intentFile.replace(".pending.json", ".json")), false);
    const beforeOldTail = newStore.tail(tailStart, 100);
    assert.deepEqual(beforeOldTail.events, beforeLive ? [] : [original]);
    assert.deepEqual(newStore.nextEventAfter(1), beforeLive ? undefined : original);

    // Actual pre-intent writer, not a mock: ordinary publish runs its archive recovery.
    const oldStore = new old.MeshStore(root, 1024, 500, options);
    const unrelated = await oldStore.publish({ topic: scenario === "before-live-other-topic" ? "mesh.other-topic" : packet.topic, from, text: "old-unrelated" });
    assert.equal(unrelated.sequence, 3);
    assert.deepEqual(readJSON(indexFile), indexed, "old recovery must leave the new sidecar untouched");
    const reader = new current.MeshStore(root, 1024, 500, options);
    const cursor = [];
    for (let after = 1; ;) {
      const event = reader.nextEventAfter(after);
      if (!event) break;
      cursor.push(event);
      after = event.sequence;
    }
    assert.deepEqual(cursor, beforeLive ? [unrelated] : [original, unrelated]);
    const afterOldTail = reader.tail(beforeOldTail.nextOffset, 100);
    assert.deepEqual(afterOldTail.events, [unrelated]);
    if (scenario === "before-live-other-topic") {
      assert.equal(fs.statSync(archivedFile).size, indexed.offset, "old cutback must leave exact EOF negative evidence");
    } else if (beforeLive) {
      assert.equal(JSON.parse(fs.readFileSync(archivedFile, "utf8").slice(indexed.offset)).id, unrelated.id, "old writer must replace the stale indexed address with a different event");
    } else {
      for (let index = 0; index < 12; index++) await oldStore.publish({ topic: packet.topic, from, text: "old-" + index + "x".repeat(500) });
      assert.equal(fs.readFileSync(liveFile, "utf8").includes(original.id), false, "old compaction must drop the live anchor");
      assert.ok(fs.existsSync(intentFile), "old compaction cannot settle the new intent");
      assert.deepEqual(readJSON(indexFile), indexed, "old compaction must also leave the false marker untouched");
      assert.deepEqual(reader.nextEventAfter(1), original, "compacted archive cursor must still deliver event 2");
    }
    const beforeRetrySequence = reader.latestSequence();
    const beforeRetryBytes = fs.readFileSync(liveFile, "utf8");
    let wholeHistoryReads = 0, directoryReads = 0;
    const read = fs.readFileSync, dirs = fs.readdirSync;
    fs.readFileSync = function(file, ...args) {
      if (String(file).endsWith(".jsonl")) wholeHistoryReads++;
      return read.call(this, file, ...args);
    };
    fs.readdirSync = function(...args) { directoryReads++; return dirs.apply(this, args); };
    let recovered;
    try {
      recovered = await reader.publish(packet);
      assert.deepEqual(await reader.publish({ ...packet, text: "retry again" }), recovered);
    } finally {
      fs.readFileSync = read;
      fs.readdirSync = dirs;
    }
    if (beforeLive) {
      assert.equal(recovered.sequence, 4);
      assert.notEqual(recovered.id, original.id);
    } else {
      assert.deepEqual(recovered, original);
      assert.equal(reader.latestSequence(), beforeRetrySequence, "key retry must not reserve a new sequence");
      assert.equal(fs.readFileSync(liveFile, "utf8"), beforeRetryBytes, "no live-tail replay after old compaction");
      assert.equal(wholeHistoryReads, 0);
      assert.equal(directoryReads, 0);
      assert.equal(readJSON(indexFile).committed, true);
    }
    assert.equal(fs.existsSync(intentFile), false);
    assert.deepEqual(readJSON(intentFile.replace(".pending.json", ".json")), recovered);
    assert.equal(fs.existsSync(path.join(path.dirname(archivedFile), "ABORTED.json")), false, "false marker must never manufacture an abort");
    const all = reader.read({ after: 0, limit: 500 }).filter(event => event.dedupeKey === packet.dedupeKey);
    assert.deepEqual(all, [recovered], "exactly one archived keyed publication");
    console.log(JSON.stringify({ scenario, oldCommit, crashSignal: crashed.signal, originalId: original.id, recoveredId: recovered.id, sequence: recovered.sequence, cursorSequencesAfterOldRecovery: cursor.map(event => event.sequence), dedupeEvents: all.length, originalDroppedByOldCompactor: !beforeLive, pendingSurvivedOldCompactor: !beforeLive, oldMarkerBeforeRetry: indexed.committed, beforeRetrySequence, afterRetrySequence: reader.latestSequence(), wholeHistoryReads, directoryReads, noAbort: true }));
  }
} finally {
  if (added) {
    const removed = spawnSync("git", ["worktree", "remove", "--force", oldTree], { cwd: repo, encoding: "utf8" });
    if (removed.status !== 0) throw new Error(removed.stderr || "scratch worktree cleanup failed");
  }
  fs.rmSync(scratch, { recursive: true, force: true });
}
