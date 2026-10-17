import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFileAtomic, syncDirectoryChain } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { EventLog } from "../src/mesh/event-log.js";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { prepareReceiptNamespace, receiptDirectoryBarrier } from "../src/mesh/receipt-namespace.js";

// Independent acceptance checks for #14179. Keep reason: data safety + real bridge/SQL trust boundary.
// Counts are successful real OS barriers, not spy invocations. Only explicitly faulted calls do not forward.
const roots: string[] = [];
const stores: MeshStore[] = [];
const scratch = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fsync-created-parents-")));
  roots.push(root);
  return root;
};
const chain = (directory: string): string[] => {
  const result: string[] = [];
  for (let current = path.resolve(directory); ; current = path.dirname(current)) {
    result.push(current);
    if (path.dirname(current) === current) return result;
  }
};
interface Barrier { file: string; directory: boolean; kind: "fsync" | "fdatasync"; held: boolean; stack: string }
const record = (meshRoot?: string, fault?: (file: string) => void) => {
  const descriptors = new Map<number, string>();
  const barriers: Barrier[] = [];
  const events: string[] = [];
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), data = fs.fdatasyncSync.bind(fs);
  const rename = fs.renameSync.bind(fs), remove = fs.rmSync.bind(fs);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    remove(file, options); events.push(`remove:${String(file)}`);
  });
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    const fd = open(file, flags, mode);
    descriptors.set(fd, String(file));
    if (!fs.fstatSync(fd).isDirectory()) events.push(`open:${String(file)}`);
    return fd;
  });
  const barrier = (fd: number, kind: "fsync" | "fdatasync") => {
    const file = descriptors.get(fd) ?? `unobserved-fd:${fd}`;
    fault?.(file);
    (kind === "fsync" ? sync : data)(fd);
    barriers.push({ file, directory: fs.fstatSync(fd).isDirectory(), kind,
      held: !!meshRoot && fs.existsSync(path.join(meshRoot, ".lock")), stack: new Error().stack ?? "" });
    events.push(`${kind}:${file}`);
  };
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => barrier(fd, "fsync"));
  vi.spyOn(fs, "fdatasyncSync").mockImplementation(fd => barrier(fd, "fdatasync"));
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    rename(from, to);
    events.push(`rename:${String(to)}`);
  });
  return { barriers, events };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  // Every deletion is below a freshly allocated private directory owned by this test.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const sender: MeshIdentity = { id: "session:fsync-fixture", name: "fsync-fixture", kind: "main" };
const fixture = async (extraDepth: number) => {
  const root = path.join(scratch(), ...Array.from({ length: extraDepth }, (_, i) => `nested-${i}`), "mesh");
  const archive = path.join(root, "archive");
  fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  const store = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite", canWakeResidents: () => false });
  stores.push(store);
  expect(store.stateBackend).toBe("sqlite");
  // Real initial history establishes the index group, BOOT, MESH and day chain outside measurement.
  await store.publish({ topic: "fixture.history", from: sender, text: "seed" });
  const side = new StoreBridgeSide(store, "fixture-peer");
  const now = Date.now();
  await side.mirror({ hosts: [{ record: { format: 1, id: sender.id, rootId: sender.id, identity: sender,
    startedAt: now, updatedAt: now, expiresAt: now + 600000 }, expiresAt: now + 600000 }], participants: [] });
  expect(side.holds(sender.id, sender)).toBe(true);
  return { root, archive, store, side };
};

describe.skipIf(process.platform === "win32")("#14179 created-parent durability acceptance", () => {
  it.each([0, 3])("real held-sender bridge publish removes repeated stable index ancestry (extra depth %s)", async depth => {
    const { root, archive, store, side } = await fixture(depth);
    // Observe the actual driver as well as the real withStateWriteFence boundary.
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    const exec = sqlite.DatabaseSync.prototype.exec;
    const sqlTransactions = new Set<object>();
    let sqlBegins = 0, appendInsideSql = false;
    vi.spyOn(sqlite.DatabaseSync.prototype, "exec").mockImplementation(function (this: object, sql) {
      exec.call(this as import("node:sqlite").DatabaseSync, sql);
      if (sql.trim() === "BEGIN IMMEDIATE") { sqlTransactions.add(this); sqlBegins++; }
      if (/^(ROLLBACK|COMMIT)$/.test(sql.trim())) sqlTransactions.delete(this);
    });
    const append = fs.appendFileSync.bind(fs);
    vi.spyOn(fs, "appendFileSync").mockImplementation(((file, ...args) => {
      if (String(file) === path.join(root, "events.jsonl")) appendInsideSql = sqlTransactions.size > 0;
      (append as (...args: unknown[]) => void)(file, ...args);
      measured.events.push(`append:${String(file)}`);
    }) as typeof fs.appendFileSync);
    const fence = vi.spyOn(store, "withStateWriteFence");
    const published = vi.spyOn(EventLog.prototype, "publish");
    const measured = record(root);
    const result = await side.publish({ topic: "fabric.control.command", kind: "steer", from: sender,
      to: "session:inert-target", text: "private scratch only", data: { bridge: { from: "wire", id: "one" },
        version: 1, commandId: "one", targetId: "actor:inert-target", operation: "steer", message: "test" } }, [sender.id]);
    const barriers = measured.barriers.slice();
    expect(result.sequence).toBe(2);
    expect(fence).toHaveBeenCalled(); expect(published).toHaveBeenCalledTimes(1);
    expect(sqlBegins).toBeGreaterThanOrEqual(1); expect(sqlTransactions.size).toBe(0); expect(appendInsideSql).toBe(true);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false); // Physical release, not callback return.
    const live = fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(live.at(-1)).toMatchObject({ sequence: 2, verification: "bridge", from: { verified: "bridge" }, data: { bridge: { from: "fixture-peer" } } });
    expect(new MeshArchive(archive, root).lookup(2)).toEqual(live.at(-1));
    expect(new MeshArchive(archive, root).head()?.sequence).toBe(2);
    const indexLeaf = path.join(archive, "sequence-index", "0");
    const index = barriers.filter(b => b.directory && b.stack.includes("writeIndex"));
    const held = barriers.filter(b => b.held);
    const heldDirs = held.filter(b => b.directory);
    const indexDepth = chain(indexLeaf).length, liveDepth = chain(root).length;
    const fileSyncs = held.filter(b => !b.directory && b.kind === "fsync");
    const dataSyncs = held.filter(b => b.kind === "fdatasync");
    // Non-negotiable file/leaf barriers: three index versions, intent, live file, receipt, segment.
    expect(fileSyncs).toHaveLength(6); expect(dataSyncs).toHaveLength(1);
    expect(fileSyncs.filter(b => b.file.startsWith(path.join(indexLeaf, "2.json.")))).toHaveLength(3);
    expect(fileSyncs.filter(b => b.file.includes("event-receipts"))).toHaveLength(2);
    expect(fileSyncs.filter(b => b.file === path.join(root, "events.jsonl"))).toHaveLength(1);
    expect(index.filter(b => b.file === indexLeaf)).toHaveLength(3);
    const order = measured.events;
    const indexRenames = order.flatMap((e, i) => e === `rename:${path.join(indexLeaf, "2.json")}` ? [i] : []);
    const intentRename = order.findIndex(e => e.startsWith("rename:") && e.endsWith(".pending.json"));
    const receiptRename = order.findIndex(e => e.startsWith(`rename:${path.join(root, "event-receipts")}`) && !e.endsWith(".pending.json"));
    const intentUnlink = order.findIndex(e => e.startsWith("remove:") && e.endsWith(".pending.json"));
    const liveAppend = order.indexOf(`append:${path.join(root, "events.jsonl")}`);
    expect(indexRenames).toHaveLength(3);
    expect(indexRenames[0]).toBeLessThan(intentRename);
    expect(intentRename).toBeLessThan(order.findIndex(e => e.startsWith("fdatasync:")));
    expect(indexRenames[1]).toBeLessThan(liveAppend);
    expect(liveAppend).toBeLessThan(indexRenames[2]!);
    expect(indexRenames[2]).toBeLessThan(receiptRename);
    expect(receiptRename).toBeLessThan(intentUnlink);
    for (const [i, renameAt] of indexRenames.entries()) {
      const nextBoundary = [intentRename, liveAppend, receiptRename][i]!;
      expect(order.slice(renameAt + 1, nextBoundary).indexOf(`fsync:${indexLeaf}`)).toBeGreaterThanOrEqual(0);
    }
    expect(heldDirs.filter(b => b.file === path.join(root, "event-receipts"))).toHaveLength(3);
    const segment = dataSyncs[0]!.file;
    for (const directory of chain(path.dirname(segment)).slice(0, 4)) expect(heldDirs.some(b => b.file === directory)).toBe(true);
    const accounting = { depth, indexDepth, liveDepth, actualHeldDirectories: heldDirs.length,
      oldFormula: 3 * indexDepth + liveDepth + 4 + 3,
      actualIndexDirectories: index.length, maximumIndexAfterOneFullGuard: indexDepth + 2,
      heldFileFsync: fileSyncs.length, heldFdatasync: dataSyncs.length,
      offLockDirectories: barriers.filter(b => b.directory && !b.held).length,
      barriers: barriers.map(({ stack, ...b }) => ({ ...b, operation: stack.includes("writeIndex") ? "archive-index" : "other" })), order };
    console.log("FSYNC_ACCOUNTING " + JSON.stringify(accounting));
    // Canonical request 1–3: allow FULL first unknown/unproven guard, never existence-as-receipt;
    // but not THREE complete rootward walks for one stable sequence group's three index updates.
    // The bound does not dictate a cache, a new API, or the eventual exact approved total.
    expect(index.length, "#14179: remove repeated unneeded stable ancestor barriers; first full guard + two fresh leaves allowed")
      .toBeLessThanOrEqual(indexDepth + 2);
    expect(heldDirs.length).toBeLessThanOrEqual(indexDepth + 2 + liveDepth + 4 + 3);
  });

  it("fresh directories: every new link barrier precedes file use and rename; external-parent failure prevents use", () => {
    const root = scratch(), outer = path.join(root, "new"), leaf = path.join(outer, "leaf"), target = path.join(leaf, "entry");
    const observed = record(undefined, file => { if (file === root) throw new Error("external-parent-fault"); });
    expect(() => writeFileAtomic(target, "no ack", { durable: true })).toThrow("external-parent-fault");
    expect(fs.statSync(leaf).isDirectory()).toBe(true);
    expect(observed.events.some(e => e.startsWith("open:") || e.startsWith("rename:"))).toBe(false);
    expect(fs.existsSync(target)).toBe(false);
    vi.restoreAllMocks();
    const next = path.join(root, "another", "leaf"), file = path.join(next, "entry");
    const success = record();
    writeFileAtomic(file, "accepted", { durable: true }); success.events.push("ack");
    const firstUse = success.events.findIndex(e => e.startsWith("open:"));
    for (const directory of [path.dirname(next), next, root]) {
      expect(success.events.indexOf(`fsync:${directory}`)).toBeGreaterThanOrEqual(0);
      expect(success.events.indexOf(`fsync:${directory}`)).toBeLessThan(firstUse);
    }
    const renamed = success.events.indexOf(`rename:${file}`);
    expect(success.barriers.filter(b => !b.directory && b.kind === "fsync")).toHaveLength(1);
    expect(success.events.findIndex(e => e.startsWith(`fsync:${file}.`))).toBeLessThan(renamed);
    expect(success.events.lastIndexOf(`fsync:${next}`)).toBeGreaterThan(renamed);
    expect(fs.readFileSync(file, "utf8")).toBe("accepted");
    console.log("FSYNC_FRESH_ORDER " + JSON.stringify(success.events));
  });

  it("actual SIGKILL after mkdir, fresh-process restart: existence never discharges an owed external-parent barrier", async () => {
    const root = scratch();
    const fixturePath = path.resolve("tests/fixtures/fsync-created-parent-crash.ts");
    const run = async (mode: string) => {
      // No detached/setsid: the child remains in the outer smarty-reap process group.
      const child = spawn(process.execPath, [fixturePath, mode, root], { stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "", stderr = "";
      child.stdout.on("data", bytes => { stdout += String(bytes); });
      child.stderr.on("data", bytes => { stderr += String(bytes); });
      const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
        child.once("error", reject); child.once("close", (code, signal) => resolve({ code, signal }));
      });
      // Event-driven close is normal; timer is only a finite deadline for a broken child.
      const deadline = setTimeout(() => { child.kill("SIGKILL"); }, 15000);
      try { return { ...await closed, stdout, stderr, pid: child.pid }; }
      finally { clearTimeout(deadline); }
    };
    // Process death alone does not emulate power loss. Pair the surviving entry/readback
    // with actual forwarded parent barriers, a fault refusal, and strict before-ack ordering.
    const crashed = await run("crash");
    expect(crashed, crashed.stderr).toMatchObject({ code: null, signal: "SIGKILL" });
    const trace = JSON.parse(fs.readFileSync(path.join(root, "crash-cut.json"), "utf8"));
    expect(trace).toMatchObject({ created: true, barriers: 0, fileUses: 0, cut: "after-mkdir-before-link-barrier" });
    if (process.platform === "linux") {
      const stat = fs.readFileSync("/proc/self/stat", "utf8");
      const parentGroup = Number(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[2]);
      expect(trace.processGroup).toBe(parentGroup);
    }
    expect(fs.statSync(path.join(root, "unprepared", "leaf")).isDirectory()).toBe(true);
    const restarted = await run("restart");
    expect(restarted.code, restarted.stderr).toBe(0);
    const result = JSON.parse(restarted.stdout);
    expect(restarted.pid).not.toBe(crashed.pid);
    expect(result.failed).toMatchObject({ acknowledged: false, error: "owed-external-parent" });
    expect(result.failed.events).not.toContain("ack");
    expect(result.accepted.acknowledged).toBe(true);
    if (process.platform === "linux") expect(result.processGroup).toBe(trace.processGroup);
    for (const directory of [root, path.join(root, "unprepared"), path.join(root, "unprepared", "leaf")]) {
      expect(result.accepted.events.indexOf(`fsync:${directory}`)).toBeGreaterThanOrEqual(0);
      expect(result.accepted.events.indexOf(`fsync:${directory}`)).toBeLessThan(result.accepted.events.indexOf("ack"));
    }
    expect(fs.readFileSync(path.join(root, "unprepared", "leaf", "entry"), "utf8")).toBe("accepted");
    console.log("FSYNC_CRASH_RESTART " + JSON.stringify({ crashed, restarted, trace }));
  }, 40000);

  it("cold and new sequence group: unknown creator's existing chain retries the full owed parent barrier", () => {
    const root = scratch(), dir = path.join(root, "archive");
    fs.mkdirSync(dir);
    const archive = new MeshArchive(dir, root);
    for (const sequence of [1, 1024]) {
      // Simulate an unknown creator: EEXIST is NOT proof, even for a brand new group.
      const group = path.join(dir, "sequence-index", String(Math.floor(sequence / 1024)));
      fs.mkdirSync(group, { recursive: true });
      const observed = record(undefined, file => { if (file === dir) throw new Error("unknown-group-link"); });
      expect(() => archive.reserveLookup(sequence)).toThrow("unknown-group-link");
      expect(observed.events).not.toContain("ack");
      vi.restoreAllMocks();
      const retry = record(); archive.reserveLookup(sequence); retry.events.push("ack");
      expect(archive.lookup(sequence)).toBeUndefined();
      expect(retry.events.indexOf(`fsync:${dir}`)).toBeLessThan(retry.events.indexOf("ack"));
      expect(retry.events).toContain(`fsync:${dir}`);
      vi.restoreAllMocks();
    }
  });

  it.each(["replacement", "ancestor-replacement", "symlink-retarget", "symlink-ABA"] as const)("checked preparation denies stale proof after %s", change => {
    const root = scratch(), physical = path.join(root, "physical"), aliasParent = path.join(root, "aliases");
    fs.mkdirSync(path.join(physical, "leaf"), { recursive: true }); fs.mkdirSync(aliasParent);
    const alias = path.join(aliasParent, "link"); fs.symlinkSync(physical, alias, "dir");
    const directory = path.join(alias, "leaf");
    const prepared = prepareReceiptNamespace(directory);
    expect(prepared).toBeDefined();
    if (change === "replacement") {
      fs.renameSync(path.join(physical, "leaf"), path.join(physical, "old")); fs.mkdirSync(path.join(physical, "leaf"));
    } else if (change === "ancestor-replacement") {
      fs.renameSync(physical, path.join(root, "old-physical")); fs.mkdirSync(path.join(physical, "leaf"), { recursive: true });
    } else {
      fs.renameSync(alias, path.join(aliasParent, "old-link"));
      if (change === "symlink-retarget") {
        const other = path.join(root, "other"); fs.mkdirSync(path.join(other, "leaf"), { recursive: true }); fs.symlinkSync(other, alias, "dir");
      } else fs.symlinkSync(physical, alias, "dir"); // Same endpoint, different alias identity.
    }
    record(undefined, file => { if (file === aliasParent) throw new Error("alias-parent-owed"); });
    expect(() => receiptDirectoryBarrier(prepared)(directory)).toThrow("alias-parent-owed");
  });

  it.each(["group-replacement", "ancestor-replacement", "alias-retarget", "alias-ABA"] as const)("real archive never reuses prior proof across %s", change => {
    const root = scratch(), physical = path.join(root, "physical"), aliases = path.join(root, "aliases");
    fs.mkdirSync(physical); fs.mkdirSync(aliases);
    const alias = path.join(aliases, "archive"); fs.symlinkSync(physical, alias, "dir");
    const archive = new MeshArchive(alias, root);
    archive.reserveLookup(1); // Successful prior barrier, same archive object and index group.
    let owedParent: string;
    if (change === "group-replacement") {
      const group = path.join(physical, "sequence-index", "0");
      fs.renameSync(group, path.join(physical, "old-group")); fs.mkdirSync(group);
      owedParent = path.dirname(group);
    } else if (change === "ancestor-replacement") {
      fs.renameSync(physical, path.join(root, "old-archive"));
      fs.mkdirSync(path.join(physical, "sequence-index", "0"), { recursive: true });
      owedParent = root;
    } else {
      fs.renameSync(alias, path.join(aliases, "old-link"));
      const endpoint = change === "alias-ABA" ? physical : path.join(root, "other");
      fs.mkdirSync(path.join(endpoint, "sequence-index", "0"), { recursive: true });
      fs.symlinkSync(endpoint, alias, "dir");
      owedParent = aliases;
    }
    let fail = true;
    const observed = record(undefined, file => { if (fail && file === owedParent) throw new Error("archive-replacement-link-owed"); });
    expect(() => archive.reserveLookup(2)).toThrow("archive-replacement-link-owed");
    fail = false; observed.events.length = 0;
    archive.reserveLookup(2); observed.events.push("ack");
    expect(observed.events).toContain(`fsync:${owedParent}`);
    expect(observed.events.indexOf(`fsync:${owedParent}`)).toBeLessThan(observed.events.indexOf("ack"));
    expect(archive.lookup(2)).toBeUndefined();
  });

  it("successful counterexample: checked stable ancestors avoid repeats but EVERY file and leaf still syncs", () => {
    const root = scratch(), directory = path.join(root, "receipts");
    const prepared = prepareReceiptNamespace(directory);
    expect(prepared).toBeDefined();
    const observed = record();
    for (let i = 0; i < 3; i++) writeFileAtomic(path.join(directory, `${i}.json`), String(i),
      { durable: true, syncDirectory: receiptDirectoryBarrier(prepared) });
    expect(observed.barriers.filter(b => b.directory).map(b => b.file)).toEqual([directory, directory, directory]);
    expect(observed.barriers.filter(b => !b.directory && b.kind === "fsync")).toHaveLength(3);
    expect(fs.readdirSync(directory).sort()).toEqual(["0.json", "1.json", "2.json"]);
    for (let i = 0; i < 3; i++) expect(fs.readFileSync(path.join(directory, `${i}.json`), "utf8")).toBe(String(i));
  });

  it.each(["file", "leaf"] as const)("stable prepared namespace still propagates each fresh %s barrier failure", cut => {
    const root = scratch(), directory = path.join(root, "receipts"), target = path.join(directory, "entry");
    const prepared = prepareReceiptNamespace(directory);
    expect(prepared).toBeDefined();
    let fail = true;
    const observed = record(undefined, file => {
      if (fail && (cut === "file" ? file.startsWith(`${target}.`) : file === directory)) throw new Error("fresh-barrier-fault");
    });
    expect(() => writeFileAtomic(target, "refused", { durable: true, syncDirectory: receiptDirectoryBarrier(prepared) })).toThrow("fresh-barrier-fault");
    expect(fs.existsSync(target)).toBe(cut === "leaf"); // Rename may be visible, but no acknowledgment is permitted.
    fail = false;
    observed.events.length = 0;
    writeFileAtomic(target, "accepted", { durable: true, syncDirectory: receiptDirectoryBarrier(prepared) });
    observed.events.push("ack");
    const fileBarrier = observed.events.findIndex(e => e.startsWith(`fsync:${target}.`));
    const leafBarrier = observed.events.indexOf(`fsync:${directory}`);
    expect(fileBarrier).toBeGreaterThanOrEqual(0); expect(leafBarrier).toBeGreaterThanOrEqual(0);
    expect(fileBarrier).toBeLessThan(observed.events.indexOf(`rename:${target}`));
    expect(leafBarrier).toBeLessThan(observed.events.indexOf("ack"));
    expect(fs.readFileSync(target, "utf8")).toBe("accepted");
  });

  it.each(["index-parent", "held-intent-leaf"] as const)("real bridge refuses acknowledgment and live append when %s fails", async cut => {
    const { root, archive, store, side } = await fixture(0);
    let fail = true;
    const observed = record(root, file => {
      if (fail && file === (cut === "index-parent" ? archive : path.join(root, "event-receipts")) && fs.existsSync(path.join(root, ".lock"))) {
        throw new Error("bridge-directory-fault");
      }
    });
    const input = { topic: "fabric.control.command", kind: "steer", from: sender, to: "session:inert-target",
      data: { bridge: { from: "wire", id: cut }, version: 1, commandId: cut,
        targetId: "actor:inert-target", operation: "steer", message: "test" } };
    await expect(side.publish(input, [sender.id])).rejects.toThrow("bridge-directory-fault");
    expect(observed.barriers.some(b => b.held)).toBe(true);
    expect(fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(1);
    expect(new MeshArchive(archive, root).head()?.sequence).toBe(1);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    fail = false;
    const accepted = await side.publish(input, [sender.id]);
    const repeated = await side.publish(input, [sender.id]);
    expect(repeated).toEqual(accepted);
    expect(store.read({ after: 1 }).filter(event => event.topic === input.topic)).toHaveLength(1);
  });

  it("no preparation: same existing directory must still take the full namespace barrier", () => {
    const root = scratch(), directory = path.join(root, "unknown"); fs.mkdirSync(directory);
    const observed = record();
    receiptDirectoryBarrier(undefined)(directory);
    expect(observed.barriers.map(b => b.file)).toEqual(chain(directory));
    syncDirectoryChain(directory);
    expect(observed.barriers.map(b => b.file)).toEqual([...chain(directory), ...chain(directory)]);
  });
});
