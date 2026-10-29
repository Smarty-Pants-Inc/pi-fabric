// Keep: retained-data safety, writer-exclusion trust boundary, crash/uncertain-CAS paths.
// Never catch target import failure or substitute a fake production implementation.
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { openRecoveryJournal } from "../src/recovery/recovery-journal.js";
import { fixture, deferred, ownerSnapshot, treeBytes, watchRetainedIO,
  type CreateBinding, type Fixture, type LockEntry } from "./helpers/portable-exclude-writer-fixture.js";
const target = new URL("../src/recovery/exclude-writer.ts", import.meta.url).href;
async function loadFactory(): Promise<CreateBinding> {
  const module = await import(/* @vite-ignore */ target);
  expect(typeof module.createPortablePreOpenBinding).toBe("function");
  return module.createPortablePreOpenBinding as CreateBinding;
}
const fixtures: Fixture[] = [];
afterEach(async () => { vi.restoreAllMocks(); for (const s of fixtures.splice(0)) await s.dispose(); });
async function setup() {
  const create = await loadFactory(); // Missing module is the honest RED, before any fixture work.
  const s = await fixture(); fixtures.push(s); return { s, create };
}
const noHistory = (s: Fixture) => expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
const exclude = (s: Fixture, create: CreateBinding) => s.bind(create).preOpen.excludeWriter({ sessionFile: s.sessionFile });

it("M2 target/export exists (FIRST RED; no fallback implementation)", async () => { await loadFactory(); });

// Linux-only real TransportHostLease reads /proc for its own identity. Other platforms
// still run the first import/export contract; skipped integration is NOT exclusion proof.
describe.skipIf(process.platform !== "linux")("portable pre-open composition (component decision contracts, NOT live B1/Main proof)", () => {
  it.each(["missing", "unreadable", "nonREQUIRED", "session", "path", "PVC", "CSI", "filesystem"])(
    "M2(a) FIRST RED: %s identity refuses before any retained read or mutation", async bad => {
      const { s, create } = await setup();
      if (bad === "missing") s.record = undefined;
      else if (bad === "unreadable") s.unreadable = true;
      else if (bad === "nonREQUIRED") s.record!.mode = "optional";
      else if (bad === "session") s.record!.sessionId = "another-session";
      else if (bad === "path") s.record!.sessionFile = path.join(s.root, "other.jsonl");
      else s.record!.volume[bad === "PVC" ? "pvcUid" : bad === "CSI" ? "csiHandle" : "filesystemUuid"] = "other-volume";
      const before = treeBytes(s.root), watch = watchRetainedIO(s);
      await expect(exclude(s, create)).rejects.toBeDefined();
      expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); watch.stop(); noHistory(s);
      expect(s.events.every(event => event === "identity:read")).toBe(true);
      expect(s.cas).toEqual([]); expect(s.acquired).toBe(0); expect(treeBytes(s.root)).toEqual(before);
    });

  it.each(["attachment", "PVC", "CSI", "filesystem"] as const)(
    "A1 component decision: mounted %s changed AFTER binding creation refuses at open", async bad => {
      const { s, create } = await setup(); const binding = s.bind(create);
      expect(s.events).not.toContain("mount:read-at-open");
      // Bootstrap, identity record and B1 receipt still agree with the OLD mount.
      // A creation-time self snapshot must not stand in for the live mount reader.
      if (bad === "attachment") s.mounted = { ...s.mounted, attachment: "attachment:replacement" };
      else s.mounted = { ...s.mounted, volume: { ...s.mounted.volume,
        [bad === "PVC" ? "pvcUid" : bad === "CSI" ? "csiHandle" : "filesystemUuid"]: "volume:replacement" } };
      const before = treeBytes(s.root), watch = watchRetainedIO(s);
      await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBeDefined();
      expect(s.events).toContain("mount:read-at-open");
      expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); watch.stop(); noHistory(s);
      expect(s.events).not.toContain("native:hold"); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
      expect(treeBytes(s.root)).toEqual(before);
    });

  it.each(["reader-invalidated", "current-check-invalidated"] as const)(
    "A2 component decision: same-Pod/same-attachment old receipt %s refuses", async bad => {
      const { s, create } = await setup(); const old = s.fence!;
      const read = s.input.b1.readCompletedForSelf;
      let invalidated = false, reads = 0;
      s.input.b1.readCompletedForSelf = async (...args) => {
        reads++; return invalidated && bad === "reader-invalidated" ? undefined : read(...args);
      };
      const binding = s.bind(create); expect(reads).toBe(0);
      // All receipt identity fields remain matching, as on a same-Pod restart.
      // The B1 reader/currentness decision, NOT those matching fields, is authoritative.
      invalidated = true;
      if (bad === "current-check-invalidated") s.revoke("b1");
      expect(old.receivingPodUid).toBe(s.input.bootstrap.self.podUid);
      expect(old.attachment).toBe(s.input.bootstrap.self.attachment);
      const before = treeBytes(s.root), watch = watchRetainedIO(s);
      await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBeDefined();
      expect(reads).toBeGreaterThan(0); expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); watch.stop(); noHistory(s);
      expect(s.events).not.toContain("native:hold"); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
      expect(treeBytes(s.root)).toEqual(before);
      // No claim that this fixture implements B1 authenticity, pod deletion or restart disabling.
    });

  it.each(["lease-absent", "harness-asserted", "confirmed-old-writer-stopped"])(
    "A11 component decision: %s evidence is never a B1-fence DTO", async kind => {
      const { s, create } = await setup(); Object.assign(s.fence!, { kind });
      const before = treeBytes(s.root), watch = watchRetainedIO(s);
      await expect(exclude(s, create)).rejects.toBeDefined();
      expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); watch.stop(); noHistory(s);
      expect(s.events).not.toContain("native:hold"); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
      expect(treeBytes(s.root)).toEqual(before);
    });

  it.each(["absent", "unreadable", "incomplete", "historical", "wrong-pod", "wrong-PVC", "wrong-CSI", "wrong-filesystem", "wrong-attachment", "zombie", "boot-change", "lease-expired"])(
    "M2(b) FIRST RED: %s is not a completed self/current-attachment B1 fence", async bad => {
      const { s, create } = await setup();
      if (bad === "absent") s.fence = undefined;
      else if (bad === "unreadable") s.input.b1.readCompletedForSelf = async () => { throw new Error("B1 unreadable"); };
      else if (bad === "wrong-pod" || bad === "historical") s.fence!.receivingPodUid = "pod:predecessor";
      else if (bad === "wrong-PVC") s.fence!.volume.pvcUid = "other";
      else if (bad === "wrong-CSI") s.fence!.volume.csiHandle = "other";
      else if (bad === "wrong-filesystem") s.fence!.volume.filesystemUuid = "other";
      else if (bad === "wrong-attachment") s.fence!.attachment = "attachment:historical";
      else {
        s.fence!.completed = false;
        // Decision-only negative hints. This is NOT a real zombie/physical fence test.
        Object.assign(s.fence!, { predecessorState: bad === "zombie" ? "Z" : "unknown", podDeleted: true });
        if (bad === "boot-change") {
          const receipt = JSON.parse(s.receiptBytes); receipt.bootId = "00000000-0000-4000-8000-000000000001";
          fs.writeFileSync(s.receiptFile, JSON.stringify(receipt));
        }
        // KV already absent, including the expired case: broker absence grants nothing.
        expect(s.kv.entries.size).toBe(0);
      }
      const before = treeBytes(s.root), watch = watchRetainedIO(s);
      await expect(exclude(s, create)).rejects.toBeDefined();
      expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); watch.stop(); noHistory(s);
      expect(s.events).not.toContain("native:hold"); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
      expect(treeBytes(s.root)).toEqual(before);
    });

  it("A3 component observer records sync/callback/promise/handle IO and rejected write attempts", async () => {
    const { s } = await setup(), watch = watchRetainedIO(s);
    try {
      fs.readFileSync(s.sessionFile);
      await new Promise<void>((resolve, reject) => fs.readFile(s.b5File, error => error ? reject(error) : resolve()));
      await fs.promises.readFile(s.b5File);
      const handle = await fs.promises.open(s.sessionFile, "r+");
      try { await handle.readFile(); await handle.write(Buffer.alloc(0)); } finally { await handle.close(); }
      const missing = path.join(s.b6, "missing-for-write-observation");
      expect(() => fs.chmodSync(missing, 0o600)).toThrow();
      expect(watch.reads).toContain(s.sessionFile); expect(watch.reads.filter(file => file === s.b5File).length).toBeGreaterThanOrEqual(2);
      expect(watch.writes).toContain(s.sessionFile); expect(watch.writes).toContain(missing);
    } finally { watch.stop(); }
  });

  it("A3 component decision: pending positive B1 proof permits no retained reads OR filesystem write attempts", async () => {
    const { s, create } = await setup(), gate = s.gate(), entered = deferred();
    const read = s.input.b1.readCompletedForSelf;
    s.input.b1.readCompletedForSelf = async (...args) => { entered.resolve(); await gate.promise; return read(...args); };
    const watch = watchRetainedIO(s), binding = s.bind(create);
    const pending = binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    try {
      await Promise.race([entered.promise, pending.then(() => { throw new Error("returned without positive B1 proof"); })]);
      expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); noHistory(s);
      expect(s.held).toBe(false); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
      gate.resolve(); await pending;
      expect(watch.writes.length).toBeGreaterThan(0); // Real generation/lease writes only after proof.
    } finally { gate.resolve(); await pending.catch(() => undefined); watch.stop(); }
  });

  it("M2(c) FIRST RED: failed native hold prevents gen/reclaim/receipt/lease/history mutation", async () => {
    const { s, create } = await setup(); s.holdFails = true;
    const before = treeBytes(s.root), watch = watchRetainedIO(s);
    await expect(exclude(s, create)).rejects.toBeDefined();
    expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]); watch.stop(); noHistory(s);
    expect(s.events.filter(event => event !== "mount:read-at-open")).toEqual(["identity:read", "b1:read", "native:hold"]);
    expect(s.cas).toEqual([]); expect(s.acquired).toBe(0); expect(treeBytes(s.root)).toEqual(before);
  });

  it("M2(d) FIRST RED: equal real custody.gen is incremented durably BEFORE native CAS", async () => {
    const { s, create } = await setup();
    let fileDurable = false, namespaceDurable = false;
    const realSync = fs.fsyncSync, realRead = fs.readFileSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      realSync(fd); const stat = fs.fstatSync(fd);
      if (stat.isFile() && realRead(`/proc/self/fd/${fd}`).equals(Buffer.from("8\n"))) fileDurable = true;
      if (stat.isDirectory() && stat.ino === fs.statSync(s.root).ino && fileDurable) namespaceDurable = true;
    });
    const cas = s.input.identityRecord.casHighWater;
    s.input.identityRecord.casHighWater = async (...args) => {
      expect(s.held).toBe(true); expect(fileDurable).toBe(true); expect(namespaceDurable).toBe(true);
      expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
      expect(s.acquired).toBe(0); noHistory(s); return cas(...args);
    };
    const result = await s.consume(s.bind(create));
    expect(s.cas).toEqual([{ before: 7, next: 8, disk: "8\n" }]); expect(s.record!.highWater).toBe(8);
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
    expect(result.native).toEqual(fs.readFileSync(s.sessionFile)); expect(result.b5).toEqual(fs.readFileSync(s.b5File));
    expect(Buffer.from(result.b6).toString()).toBe("B6 exact retained value");
    expect(s.historyOpens).toEqual({ native: 1, b5: 1, b6: 1 }); expect(s.acquired).toBe(1);
  });

  it("M2(d) reconciles h+1 before any further increment; retry never creates an unresolved h+2", async () => {
    const { s, create } = await setup(); fs.writeFileSync(s.generationFile, "8\n");
    await exclude(s, create);
    expect(s.cas[0]).toEqual({ before: 7, next: 8, disk: "8\n" });
    // Binding verdict A5: restart reconciles ONLY; there is NO second increment.
    expect(s.cas).toEqual([{ before: 7, next: 8, disk: "8\n" }]);
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n"); expect(s.record!.highWater).toBe(8);
    expect(s.acquired).toBe(1);
  });

  it.each(["6\n", "9\n", "", "not-a-number\n", "-1\n", "9007199254740992\n", "missing"])(
    "M2(d) clone/reverted h-1, h+2, malformed/missing gen %j never resets or admits", async bytes => {
      const { s, create } = await setup();
      if (bytes === "missing") fs.renameSync(s.generationFile, path.join(s.root, "saved.gen"));
      else fs.writeFileSync(s.generationFile, bytes);
      const before = treeBytes(s.root);
      await expect(exclude(s, create)).rejects.toBeDefined(); noHistory(s);
      expect(s.cas).toEqual([]); expect(s.record!.highWater).toBe(7); expect(s.acquired).toBe(0);
      expect(treeBytes(s.root)).toEqual(before);
    });

  it.each(["conflict", "unknown"] as const)("M2(d) %s CAS never decrements/restores gen or grants admission", async mode => {
    const { s, create } = await setup(); s.casMode = mode;
    const receipt = fs.readFileSync(s.receiptFile), binding = s.bind(create);
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBeDefined();
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n"); expect(s.record!.highWater).toBe(7);
    expect(fs.readFileSync(s.receiptFile)).toEqual(receipt); expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]); noHistory(s);
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBeDefined();
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
  });

  it("M2(d) unknown ACK that committed requires authoritative readback before lease admission", async () => {
    const { s, create } = await setup(); s.casMode = "unknown-committed";
    const read = s.input.identityRecord.readRequired, gate = s.gate(), entered = deferred();
    let reads = 0;
    // Install before factory creation: an implementation may legitimately capture this port.
    s.input.identityRecord.readRequired = async (...args) => { if (++reads > 1) { entered.resolve(); await gate.promise; } return read(...args); };
    const binding = s.bind(create), pending = binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    try {
      await Promise.race([entered.promise, pending.then(() => { throw new Error("admitted without ACK readback"); })]);
      expect(s.acquired).toBe(0); noHistory(s); expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
      gate.resolve(); await pending; expect(s.record!.highWater).toBe(8); expect(s.acquired).toBe(1);
    } finally { gate.resolve(); await pending.catch(() => undefined); }
  });

  it("M2(e) FIRST RED: only exact protocol2 receipt/protocol3 B6 locks reclaim; real ordinary openers still refuse foreign", async () => {
    const { s, create } = await setup();
    const old: ReturnType<typeof ownerSnapshot>[] = [];
    for (const entry of s.locks) old.push(await s.foreignLock(entry));
    await expect(acquireMeshCustodyLock(path.dirname(s.locks[0]!.directory), 0, { hostQualified: true, lockProtocol: 2 })).rejects.toBeDefined();
    await expect(openRecoveryJournal({ directory: s.b6, storeId: "store:retained", required: true })).rejects.toBeDefined();
    for (const before of old) expect(ownerSnapshot(before.directory).bytes).toEqual(before.bytes);
    const binding = s.bind(create); await s.consume(binding); expect(s.acquired).toBe(1);
    for (const before of old) {
      const evidence = findRetainedDirectory(s.root, before.dir.ino);
      expect(evidence).toBeDefined(); expect(evidence).not.toBe(before.directory);
      const retained = ownerSnapshot(evidence!);
      expect(retained.owner.ino).toBe(before.owner.ino); expect(retained.owner.dev).toBe(before.owner.dev);
      expect(retained.bytes).toEqual(before.bytes); expect(fs.readFileSync(path.join(evidence!, "retained-marker"), "utf8")).toBe("do not delete");
    }
    const receipt = fs.readFileSync(s.receiptFile), generation = fs.readFileSync(s.generationFile);
    await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }); // stale repeat cannot reclaim successor
    expect(fs.readFileSync(s.receiptFile)).toEqual(receipt); expect(fs.readFileSync(s.generationFile)).toEqual(generation); expect(s.acquired).toBe(1);
  });

  it.each(["unlisted", "shared", "protocol", "symlink"])("M2(e) %s root refuses without force, fallback, or retained-byte deletion", async bad => {
    const { s, create } = await setup(); const original = s.locks[1]!;
    const extra: LockEntry = { ...original, directory: path.join(s.root, "shared-mesh", "custody.lock") };
    await s.foreignLock(extra);
    if (bad === "unlisted") s.record!.locks = [{ ...original, directory: extra.directory }];
    if (bad === "shared") s.record!.locks = [{ ...extra, sessionPrivate: false }];
    if (bad === "protocol") s.record!.locks = [{ ...original, protocol: 2 }];
    if (bad === "symlink") { fs.symlinkSync(path.dirname(extra.directory), path.join(s.root, "alias")); s.record!.locks = [{ ...original, directory: path.join(s.root, "alias", "custody.lock") }]; }
    const bytes = treeBytes(path.dirname(extra.directory));
    await expect(exclude(s, create)).rejects.toBeDefined(); noHistory(s); expect(s.acquired).toBe(0);
    expect(treeBytes(path.dirname(extra.directory))).toEqual(bytes);
  });

  it.each([
    [0, "ownerless"], [0, "empty"], [0, "malformed"], [0, "invalid-utf8"],
    [1, "ownerless"], [1, "empty"], [1, "malformed"], [1, "invalid-utf8"],
  ] as const)("M2(e) foreign lock %i/%s refuses and preserves every supplied byte", async (index, bad) => {
    const { s, create } = await setup(), entry = s.locks[index]!;
    const original = await s.foreignLock(entry), owner = path.join(entry.directory, "owner");
    if (bad === "ownerless") fs.renameSync(owner, path.join(entry.directory, "retained-original-owner"));
    else {
      fs.writeFileSync(path.join(entry.directory, "retained-original-owner"), original.bytes);
      fs.writeFileSync(owner, bad === "empty" ? Buffer.alloc(0) : bad === "malformed"
        ? Buffer.from("not-a-custody-owner\n") : Buffer.from([0xff, 0xfe, 0x00, 0x0a]));
    }
    const before = treeBytes(path.dirname(entry.directory)), stat = fs.lstatSync(entry.directory);
    await expect(exclude(s, create)).rejects.toBeDefined(); noHistory(s);
    expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]);
    expect(treeBytes(path.dirname(entry.directory))).toEqual(before);
    expect(fs.lstatSync(entry.directory).ino).toBe(stat.ino);
    expect(fs.readFileSync(path.join(entry.directory, "retained-original-owner"))).toEqual(original.bytes);
  });

  it.each(["owner-inode", "owner-bytes", "directory-path"])("M2(e) last-check/rename %s race preserves retained evidence AND canonical successor", async race => {
    const { s, create } = await setup(), entry = s.locks[1]!;
    const old = await s.foreignLock(entry), realRename = fs.renameSync;
    let injected = false, successor: ReturnType<typeof ownerSnapshot> | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      let matches = false;
      try { const st = fs.lstatSync(from); matches = st.isDirectory() && st.ino === old.dir.ino && st.dev === old.dir.dev; } catch { /* other publication */ }
      if (!injected && matches) {
        injected = true;
        const owner = path.join(entry.directory, "owner");
        if (race === "owner-inode") { realRename(owner, path.join(s.root, "retained-original-owner")); fs.writeFileSync(owner, old.bytes); }
        if (race === "owner-bytes") fs.writeFileSync(owner, Buffer.from(old.bytes.toString().replace(/^./, "x")));
        if (race === "directory-path") {
          realRename(entry.directory, path.join(s.root, "retained-original-directory"));
          fs.mkdirSync(entry.directory); fs.writeFileSync(owner, old.bytes);
        }
        realRename(from, to);
        if (!fs.existsSync(entry.directory)) fs.mkdirSync(entry.directory);
        fs.writeFileSync(path.join(entry.directory, "owner"), old.bytes.toString().replace(/^[^\n]+/, randomUUID()));
        successor = ownerSnapshot(entry.directory); return;
      }
      realRename(from, to);
    });
    await expect(exclude(s, create)).rejects.toBeDefined();
    expect(injected).toBe(true); expect(s.acquired).toBe(0); noHistory(s);
    const current = ownerSnapshot(entry.directory);
    expect(current.dir.ino).toBe(successor!.dir.ino); expect(current.owner.ino).toBe(successor!.owner.ino); expect(current.bytes).toEqual(successor!.bytes);
    // Everything moved is retained; no restore over the new canonical owner.
    expect(findRetainedDirectory(s.root, old.dir.ino)).toBeDefined();
  });

  it("M2(f) FIRST RED: exact real startupSuccessor snapshot; lease held before return, borrowed without second acquire", async () => {
    const { s, create } = await setup(); const ack = s.gate(); s.kv.ack = ack.promise;
    const entered = deferred(), acquire = s.input.leaseOwner.acquireOnce;
    s.input.leaseOwner.acquireOnce = async args => { entered.resolve(); return acquire({ ...args, startupSuccessor: {
      async verifyPredecessor(snapshot, signal) {
        expect(fs.existsSync(`${s.receiptFile}.custody/custody.lock/owner`)).toBe(true);
        expect(fs.readFileSync(s.receiptFile, "utf8")).toBe(snapshot.bytes);
        expect(Object.isFrozen(snapshot)).toBe(true);
        await args.startupSuccessor.verifyPredecessor(snapshot, signal);
      },
    } }); };
    const binding = s.bind(create);
    let returned = false;
    const pending = binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(value => { returned = true; return value; });
    try {
      await Promise.race([entered.promise, pending.then(() => { throw new Error("returned without lease"); })]);
      expect(returned).toBe(false); noHistory(s); ack.resolve(); const guard = await pending;
      expect(Object.keys(guard)).toEqual(["assertCurrent"]); expect(Object.keys(binding.preOpen)).toEqual(["excludeWriter"]);
      expect(s.snapshots[0]!.bytes).toBe(s.receiptBytes); expect(s.snapshots[0]!.receipt!.incarnation).toBe("predecessor");
      const borrowed = binding.borrowLease(); expect(borrowed.lease.live()).toBe(true);
      expect(borrowed.tuple).toEqual({ ...s.record!.transport, localStateDir: s.state });
      expect(binding.borrowLease().lease).toBe(borrowed.lease); expect(s.acquired).toBe(1); expect(s.kv.writes).toEqual([0]);
    } finally { ack.resolve(); await pending.catch(() => undefined); }
  });

  it.each(["missing", "same-incarnation"])("M2(f) %s receipt refuses through real lease before new reservation/KV admission", async bad => {
    const { s, create } = await setup();
    if (bad === "missing") fs.renameSync(s.receiptFile, path.join(s.root, "retained-receipt"));
    else { const receipt = JSON.parse(s.receiptBytes); receipt.incarnation = "receiver"; fs.writeFileSync(s.receiptFile, JSON.stringify(receipt)); }
    const before = treeBytes(s.state);
    await expect(exclude(s, create)).rejects.toBeDefined(); expect(s.kv.writes).toEqual([]); noHistory(s);
    expect(treeBytes(s.state)).toEqual(before);
  });

  it.each(["native", "mount", "b1"] as const)("M2(g) component decision: %s currentness loss synchronously rechecks and latches old handle", async kind => {
    const { s, create } = await setup(); const guard = await exclude(s, create); guard.assertCurrent();
    s.loseObservation(kind); expect(() => guard.assertCurrent()).toThrow(); s.restoreObservation();
    expect(() => guard.assertCurrent()).toThrow(); noHistory(s);
  });

  it("M2(g) component decision: real lease lapse retains guard/native tuple; same-owner root recheck and rebind do not transfer again", async () => {
    const { s, create } = await setup(); const binding = s.bind(create), guard = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    const borrowed = binding.borrowLease(), tuple = s.guard.nativeTuple, gen = fs.readFileSync(s.generationFile);
    s.clock = 13_001; expect(borrowed.lease.live()).toBe(false); expect(borrowed.lease.recovering()).toBe(true);
    expect(() => guard.assertCurrent()).not.toThrow(); expect(s.held).toBe(true); expect(s.closed).toBe(0); expect(s.guard.nativeTuple).toBe(tuple);
    s.kv.entries.clear(); await s.renewOwnedLease(); expect(borrowed.lease.rootRecheck).toBe(true);
    expect(borrowed.lease.takeRecreated()).toBe(true); // existing directory admission must remain closed until actual root recheck
    expect(s.guard.nativeTuple).toBe(tuple); expect(() => guard.assertCurrent()).not.toThrow();
    s.confirmOwnedRootRecheck(); await s.renewOwnedLease();
    await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    expect(binding.borrowLease().lease).toBe(borrowed.lease); expect(s.acquired).toBe(1);
    expect(fs.readFileSync(s.generationFile)).toEqual(gen); expect(s.cas).toHaveLength(1);
    expect(tuple).toBe(s.nativeTuple); // fixture tuple, explicitly NOT a real Pi epoch proof
  });

  it("M2(g) concurrent same invocation joins one gen/lease; contradictory path refuses; close waits final native join", async () => {
    const { s, create } = await setup(); const binding = s.bind(create);
    const settled = await Promise.allSettled([binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }), binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })]);
    const unwrap = (result: (typeof settled)[number]) => { if (result.status === "rejected") throw result.reason; return result.value; };
    const first = unwrap(settled[0]), second = unwrap(settled[1]);
    expect(s.acquired).toBe(1); expect(s.cas).toHaveLength(1); expect(() => first.assertCurrent()).not.toThrow(); expect(() => second.assertCurrent()).not.toThrow();
    await expect(binding.preOpen.excludeWriter({ sessionFile: path.join(s.root, "another-session") })).rejects.toBeDefined();
    const lease = binding.borrowLease().lease, joined = s.gate(); let closed = false;
    const closing = binding.closeAfterNativeJoin(joined.promise).then(() => { closed = true; });
    try {
      await Promise.resolve(); expect(closed).toBe(false); expect(s.held).toBe(true); expect(lease.live()).toBe(true);
      joined.resolve(); await closing; expect(s.closed).toBe(1); expect(lease.live()).toBe(false); expect(() => first.assertCurrent()).toThrow();
      await binding.closeAfterNativeJoin(Promise.resolve()); expect(s.closed).toBe(1);
    } finally { joined.resolve(); await closing.catch(() => undefined); }
  });
});

function findRetainedDirectory(root: string, ino: number): string | undefined {
  for (const name of fs.readdirSync(root)) {
    const file = path.join(root, name), stat = fs.lstatSync(file);
    if (!stat.isDirectory() || stat.isSymbolicLink()) continue;
    if (stat.ino === ino) return file;
    const found = findRetainedDirectory(file, ino); if (found) return found;
  }
  return undefined;
}
