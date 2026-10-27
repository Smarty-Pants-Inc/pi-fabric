import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { prepareReceiptNamespace, receiptDirectoryBarrier } from "../src/mesh/receipt-namespace.js";
import * as receiptNamespace from "../src/mesh/receipt-namespace.js";

// Independent before-code checks, #14179. Keep reason: data safety / real bridge-SQL boundary.
// No power-loss simulation: real forwarded OS barriers plus deterministic namespace mutations/EIO.
const roots: string[] = [], stores: MeshStore[] = [];
const chain = (leaf: string, stop = path.parse(path.resolve(leaf)).root): string[] => {
  const result: string[] = [];
  for (let p = path.resolve(leaf); ; p = path.dirname(p)) {
    result.push(p);
    if (p === stop) return result;
    if (path.dirname(p) === p) throw new Error("stop is not an ancestor");
  }
};
const commonDirectory = (a: string, b: string) => chain(a).find(p => chain(b).includes(p))!;
const sorted = (values: string[]) => [...values].sort();
// Qualify the fixture independently of the production fast-path outcome. Unsupported
// Linux filesystems/proc capability owe full chains too; ext/XFS MUST exercise reuse.
const linuxPrefixCapable = (directory: string): boolean => {
  if (process.platform !== "linux") return false;
  const fd = fs.openSync(directory, fs.constants.O_RDONLY);
  try {
    const kind = fs.statfsSync(`/proc/self/fd/${fd}`, { bigint: true }).type;
    return (kind === 0xef53n || kind === 0x58465342n) &&
      [...fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8").matchAll(/^mnt_id:\s*(\d+)\s*$/gm)].length === 1;
  } catch (error) {
    if (["ENOENT", "ENOTDIR", "ENOSYS", "ENOTSUP", "EOPNOTSUPP", "EACCES"].includes(String((error as NodeJS.ErrnoException).code))) return false;
    throw error;
  } finally { fs.closeSync(fd); }
};
const sender: MeshIdentity = { id: "session:prefix-check", name: "prefix-check", kind: "main" };
const command = (key: string) => ({ topic: "fabric.control.command", kind: "steer", from: sender,
  to: "session:inert-target", text: "fixture", data: { bridge: { from: "wire", id: key }, version: 1,
    commandId: key, targetId: "actor:inert-target", operation: "steer", message: "fixture" } });
type Fixture = Awaited<ReturnType<typeof fixture>>;
async function fixture(layout: "sibling" | "inside" | "alias" = "sibling", warm = true) {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "prefix-check-"))); roots.push(base);
  const common = path.join(base, "common"), root = path.join(common, "mesh");
  const branch = path.join(common, "nonshared"), physical = path.join(branch, "archive");
  let archive = layout === "inside" ? path.join(root, "archive") : physical;
  fs.mkdirSync(root, { recursive: true }); fs.mkdirSync(archive, { recursive: true });
  if (layout === "alias") {
    fs.mkdirSync(path.join(common, "aliases"));
    archive = path.join(common, "aliases", "archive"); fs.symlinkSync(physical, archive, "dir");
  }
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  const store = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite", canWakeResidents: () => false });
  stores.push(store); expect(store.stateBackend).toBe("sqlite");
  const side = new StoreBridgeSide(store, "prefix-peer"), now = Date.now();
  await side.mirror({ hosts: [{ record: { format: 1, id: sender.id, rootId: sender.id, identity: sender,
    startedAt: now, updatedAt: now, expiresAt: now + 600000 }, expiresAt: now + 600000 }], participants: [] });
  expect(side.holds(sender.id, sender)).toBe(true);
  if (warm) {
    // Same topic: archive day/segment/bootstrap excluded from warm publication, not silently counted away.
    await store.publish({ topic: "fabric.control.command", from: sender, text: "seed" });
    expect(prepareReceiptNamespace(path.join(root, "event-receipts"))).toBeDefined();
  }
  const leaf = path.join(archive, "sequence-index", "0");
  return { base, common, root, branch, physical, archive, leaf, store, side, sequence: warm ? 2 : 1 };
}
interface Barrier { file: string; directory: boolean; held: boolean; phase: number; operation: string; kind: string }
function observe(f: Fixture, hooks: {
  renameIndex?: (phase: number) => void;
  sync?: (b: Barrier, fd: number) => void;
  afterSync?: (b: Barrier, fd: number) => void;
  close?: (file: string) => void;
  append?: (file: string) => void;
} = {}) {
  const descriptors = new Map<number, string>(), barriers: Barrier[] = [], events: string[] = [];
  const open = fs.openSync.bind(fs), close = fs.closeSync.bind(fs), sync = fs.fsyncSync.bind(fs);
  const data = fs.fdatasyncSync.bind(fs), rename = fs.renameSync.bind(fs), append = fs.appendFileSync.bind(fs);
  let phase = -1;
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    const fd = open(file, flags, mode); descriptors.set(fd, String(file));
    if (!fs.fstatSync(fd).isDirectory()) events.push(`open:${String(file)}`);
    return fd;
  });
  vi.spyOn(fs, "closeSync").mockImplementation(fd => {
    const file = descriptors.get(fd) ?? "unknown";
    close(fd); descriptors.delete(fd); hooks.close?.(file);
  });
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    rename(from, to); events.push(`rename:${String(to)}`);
    if (String(to) === path.join(f.leaf, `${f.sequence}.json`)) {
      const entry = JSON.parse(fs.readFileSync(to, "utf8"));
      phase = entry.absent ? 0 : entry.committed === false ? 1 : 2;
      hooks.renameIndex?.(phase);
    }
  });
  const barrier = (fd: number, kind: string) => {
    const stack = new Error().stack ?? "";
    const b: Barrier = { file: descriptors.get(fd) ?? "unknown", directory: fs.fstatSync(fd).isDirectory(),
      held: fs.existsSync(path.join(f.root, ".lock")), phase, kind,
      operation: stack.includes("writeIndex") ? "index" : stack.includes("confirmEventFile") ? "live" : "other" };
    hooks.sync?.(b, fd); (kind === "fsync" ? sync : data)(fd);
    barriers.push(b); events.push(`${kind}:${b.file}`); hooks.afterSync?.(b, fd);
  };
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => barrier(fd, "fsync"));
  vi.spyOn(fs, "fdatasyncSync").mockImplementation(fd => barrier(fd, "fdatasync"));
  vi.spyOn(fs, "appendFileSync").mockImplementation(((file, ...args) => {
    hooks.append?.(String(file));
    (append as (...args: unknown[]) => void)(file, ...args); events.push(`append:${String(file)}`);
  }) as typeof fs.appendFileSync);
  return { barriers, events };
}
const indexDirs = (observed: ReturnType<typeof observe>, phase: number) => observed.barriers
  .filter(b => b.directory && b.held && b.operation === "index" && b.phase === phase).map(b => b.file);
const liveDirs = (observed: ReturnType<typeof observe>) => observed.barriers
  .filter(b => b.directory && b.operation === "live").map(b => b.file);
const eio = () => Object.assign(new Error("owed-fsync-EIO"), { code: "EIO" });
const liveEvents = (f: Fixture) => fs.readFileSync(path.join(f.root, "events.jsonl"), "utf8").trim().split("\n")
  .map(line => JSON.parse(line));
async function retryExactlyOnce(f: Fixture, key: string) {
  const accepted = await f.side.publish(command(key), [sender.id]);
  const repeated = await f.side.publish(command(key), [sender.id]);
  expect(repeated).toEqual(accepted);
  const matches = liveEvents(f).filter(event => event.data?.commandId === key);
  expect(matches).toHaveLength(1);
  expect(new MeshArchive(f.archive, f.root).lookup(accepted.sequence)).toEqual(matches[0]);
  return matches[0];
}
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  // Only fresh fixture roots under the runner's inner allowed TMPDIR, never real meshes/worktrees.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform === "win32")("prepared archive prefix: independent acceptance", () => {
  it.each((["sibling", "inside"] as const).flatMap(layout =>
    (process.platform === "linux" ? ["native", "darwin-simulated"] as const : ["native"] as const)
      .map(gate => ({ layout, gate }))))("exact warm namespace sets ($layout, $gate)", async ({ layout, gate }) => {
    const f = await fixture(layout);
    const reduced = gate === "native" && linuxPrefixCapable(f.root);
    let gateCalls = 0;
    if (gate === "darwin-simulated") {
      const prepare = receiptNamespace.prepareKeyedPublishNamespace;
      const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
      vi.spyOn(receiptNamespace, "prepareKeyedPublishNamespace").mockImplementation(root => {
        // Only this synchronous source gate sees Darwin. Restore before backend,
        // bootstrap, host identity or async publication work; no foreign commands.
        Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
        try {
          const prepared = prepare(root);
          expect(prepared.receiptNamespace).toBeDefined();
          expect(prepared.prefixScope).toBeUndefined();
          gateCalls++;
          return prepared;
        } finally { Object.defineProperty(process, "platform", platform); }
      });
    }
    // Verify real driver transaction, not merely a mocked fence callback.
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    const exec = sqlite.DatabaseSync.prototype.exec, transactions = new Set<object>();
    let begins = 0, appendedInSql = false;
    vi.spyOn(sqlite.DatabaseSync.prototype, "exec").mockImplementation(function (this: object, sql) {
      exec.call(this as import("node:sqlite").DatabaseSync, sql);
      if (sql.trim() === "BEGIN IMMEDIATE") { transactions.add(this); begins++; }
      if (/^(ROLLBACK|COMMIT)$/.test(sql.trim())) transactions.delete(this);
    });
    const measured = observe(f, { append: file => {
      if (file === path.join(f.root, "events.jsonl")) appendedInSql = transactions.size > 0;
    } });
    const result = await f.side.publish(command("warm"), [sender.id]);
    expect(result.sequence).toBe(2); expect(begins).toBeGreaterThan(0); expect(appendedInSql).toBe(true);
    if (gate === "darwin-simulated") { expect(gateCalls).toBe(1); expect(process.platform).toBe("linux"); }
    expect(transactions.size).toBe(0); expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
    const common = commonDirectory(f.leaf, f.root);
    const expected = reduced ? chain(f.leaf, common) : chain(f.leaf);
    const expectedLive = reduced ? [f.root] : chain(f.root);
    const held = measured.barriers.filter(b => b.held), files = held.filter(b => !b.directory && b.kind === "fsync");
    expect(files).toHaveLength(6); expect(held.filter(b => b.kind === "fdatasync")).toHaveLength(1);
    expect(files.filter(b => b.file.startsWith(path.join(f.leaf, "2.json.")))).toHaveLength(3);
    expect(files.filter(b => b.file.includes("event-receipts"))).toHaveLength(2);
    expect(files.filter(b => b.file === path.join(f.root, "events.jsonl"))).toHaveLength(1);
    expect(held.filter(b => b.directory && b.file === path.join(f.root, "event-receipts"))).toHaveLength(3);
    expect(new MeshArchive(f.archive, f.root).lookup(2)).toEqual(liveEvents(f).at(-1));
    const renames = measured.events.flatMap((e, i) => e === `rename:${path.join(f.leaf, "2.json")}` ? [i] : []);
    const intent = measured.events.findIndex(e => e.startsWith("rename:") && e.endsWith(".pending.json"));
    const appendAt = measured.events.indexOf(`append:${path.join(f.root, "events.jsonl")}`);
    const receipt = measured.events.findIndex(e => e.startsWith(`rename:${path.join(f.root, "event-receipts")}`) && !e.endsWith(".pending.json"));
    expect(renames).toHaveLength(3);
    expect(renames[0]).toBeLessThan(intent); expect(intent).toBeLessThan(renames[1]!);
    expect(renames[1]).toBeLessThan(appendAt); expect(appendAt).toBeLessThan(renames[2]!);
    expect(renames[2]).toBeLessThan(receipt);
    const dataAt = measured.events.findIndex(e => e.startsWith("fdatasync:"));
    expect(intent).toBeLessThan(dataAt); expect(dataAt).toBeLessThan(renames[1]!);
    for (let p = 0; p < 3; p++) {
      const beforeRename = measured.events.slice(p ? renames[p - 1]! + 1 : 0, renames[p]);
      expect(beforeRename.filter(e => e.startsWith(`fsync:${path.join(f.leaf, "2.json.")}`))).toHaveLength(1);
    }
    for (let phase = 0; phase < 3; phase++) for (const directory of expected) {
      expect(indexDirs(measured, phase)).toContain(directory); // Safety remains meaningful on baseline.
    }
    const offLock = measured.barriers.filter(b => b.directory && !b.held).map(b => b.file);
    expect(offLock).toEqual(chain(path.join(f.root, "event-receipts")));
    const expectedHeld = 3 * expected.length + expectedLive.length + 3;
    console.log("PREFIX_ACCOUNTING " + JSON.stringify({ layout, gate, host: process.platform, reduced, common, expectedIndex: expected,
      fullIndex: chain(f.leaf), expectedHeld, baselineFormula: 3 * chain(f.leaf).length + chain(f.root).length + 3,
      actualHeld: held.filter(b => b.directory).length, actualIndex: [0, 1, 2].map(p => indexDirs(measured, p)),
      live: liveDirs(measured), offLock, offLockCount: offLock.length, files: files.length,
      data: held.filter(b => b.kind === "fdatasync").length }));
    // Linux eligible: omit ONLY ancestors above common. Darwin/other denial:
    // every old index chain at all three phases AND the complete live chain.
    for (let phase = 0; phase < 3; phase++) expect(sorted(indexDirs(measured, phase))).toEqual(sorted(expected));
    expect(liveDirs(measured)).toEqual(expectedLive);
    expect(held.filter(b => b.directory)).toHaveLength(expectedHeld);
  });

  it.each([0, 1, 2])("stable positive: all owed paths and live binding survive original phase %s", async phase => {
    const f = await fixture(), measured = observe(f);
    await retryExactlyOnce(f, `stable-${phase}`);
    for (const directory of chain(f.leaf, commonDirectory(f.leaf, f.root))) expect(indexDirs(measured, phase)).toContain(directory);
    expect(liveDirs(measured)).toContain(f.root);
  });

  it.each([0, 1, 2].flatMap(phase => ["archive", "ancestor"].map(move => ({ phase, move }))))(
    "same-inode nonshared $move ABA at phase $phase still owes its parent; EIO refuses ACK", async ({ phase, move }) => {
      const f = await fixture(), target = move === "archive" ? f.archive : f.branch;
      const owed = path.dirname(target), before = fs.statSync(target);
      let mutated = false, faults = 0;
      const measured = observe(f, { renameIndex: p => {
        if (p !== phase || mutated) return;
        mutated = true; fs.renameSync(target, `${target}-away`); fs.renameSync(`${target}-away`, target);
        expect(fs.statSync(target).ino).toBe(before.ino); // unchanged inode is deliberately NOT a receipt.
      }, sync: b => {
        if (mutated && faults === 0 && b.directory && b.operation === "index" && b.phase === phase && b.file === owed) {
          faults++; throw eio();
        }
      } });
      let acknowledged = false;
      await expect(f.side.publish(command("aba"), [sender.id]).then(r => { acknowledged = true; return r; })).rejects.toThrow("owed-fsync-EIO");
      expect(mutated).toBe(true); expect(faults).toBe(1); expect(acknowledged).toBe(false);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      const prior = liveEvents(f).find(event => event.data?.commandId === "aba");
      const recovered = await retryExactlyOnce(f, "aba");
      if (prior) expect(recovered.id).toBe(prior.id);
      console.log("PREFIX_ABA " + JSON.stringify({ phase, move, owed, faults, forwarded: measured.barriers.length }));
    });

  it("visible changed prepared receipt namespace denies prefix reuse on real single publish", async () => {
    const f = await fixture(), receipts = path.join(f.root, "event-receipts");
    const fence = f.store.withStateWriteFence.bind(f.store); let changed = false;
    vi.spyOn(f.store, "withStateWriteFence").mockImplementation(operation => {
      if (!changed) { changed = true; fs.renameSync(receipts, `${receipts}-old`); fs.mkdirSync(receipts); }
      return fence(operation);
    });
    const measured = observe(f); await f.side.publish(command("changed"), [sender.id]);
    expect(changed).toBe(true);
    for (const phase of [0, 1, 2]) expect(sorted(indexDirs(measured, phase))).toEqual(sorted(chain(f.leaf)));
    expect(liveDirs(measured)).toEqual(chain(f.root));
  });

  it("symlink archive fallback preserves physical AND alias namespace parent fences", async () => {
    const f = await fixture("alias"), measured = observe(f);
    await f.side.publish(command("alias"), [sender.id]);
    const expected = [...new Set([...chain(path.join(f.physical, "sequence-index", "0")), ...chain(path.dirname(f.archive))])];
    for (const phase of [0, 1, 2]) expect(sorted(indexDirs(measured, phase))).toEqual(sorted(expected));
  });

  it("batch borrows only its prepared prefix and keeps its after-release live fence", async () => {
    const f = await fixture(), measured = observe(f);
    const reduced = linuxPrefixCapable(f.root);
    await f.side.publishBatch([{ event: command("batch"), held: [sender.id] }]);
    const expected = reduced ? chain(f.leaf, commonDirectory(f.leaf, f.root)) : chain(f.leaf);
    for (const phase of [0, 1, 2]) expect(sorted(indexDirs(measured, phase))).toEqual(sorted(expected));
    // Archive-coupled keyed confirmation under lock, then unconditional #finishBatch live confirmation.
    expect(liveDirs(measured)).toEqual([...(reduced ? [f.root] : chain(f.root)), ...chain(f.root)]);
    expect(measured.barriers.filter(b => b.directory && !b.held && b.operation !== "live").map(b => b.file))
      .toEqual(chain(path.join(f.root, "event-receipts")));
  });

  it("cold unknown-creator index, direct rollback/recovery and generic atomic write remain full", async () => {
    const f = await fixture("sibling", false), archive = new MeshArchive(f.archive, f.root);
    fs.mkdirSync(f.leaf, { recursive: true }); // unknown creator, no preparation (existence is not a receipt).
    const measured = observe(f);
    archive.reserveLookup(1);
    expect(measured.barriers.filter(b => b.directory).map(b => b.file)).toEqual(chain(f.leaf));
    measured.barriers.length = 0;
    const event = { id: "direct", sequence: 1, topic: "direct", kind: "event" as const, from: sender,
      createdAt: Date.now(), dedupeKey: "direct" };
    const pending = archive.begin({ event, line: JSON.stringify(event) });
    archive.rollback(pending);
    expect(measured.barriers.filter(b => b.directory && b.operation === "index").map(b => b.file))
      .toEqual([...chain(f.leaf), ...chain(f.leaf)]);
    expect(archive.lookup(1)).toBeUndefined();
    measured.barriers.length = 0;
    writeFileAtomic(path.join(f.leaf, "generic"), "durable", { durable: true });
    expect(measured.barriers.filter(b => b.directory).map(b => b.file)).toEqual(chain(f.leaf));
    measured.barriers.length = 0;
    const receipts = path.join(f.root, "event-receipts"); fs.mkdirSync(receipts);
    receiptDirectoryBarrier(undefined)(receipts);
    expect(measured.barriers.filter(b => b.directory).map(b => b.file)).toEqual(chain(receipts));
    // Actual reboot-recovery promotion path, not a made-up optimized archive helper.
    archive.begin({ event, line: JSON.stringify(event) });
    fs.writeFileSync(path.join(f.archive, "BOOT"), "earlier-boot");
    const recovery = archive.recover(0, archive.prepareRecovery(0));
    expect(recovery.rebooted).toBe(true); expect(recovery.promote.map(e => e.event.id)).toEqual([event.id]);
    measured.barriers.length = 0;
    archive.recovered(recovery.promote.at(-1), recovery.promote);
    expect(measured.barriers.filter(b => b.directory && b.operation === "index").map(b => b.file)).toEqual(chain(f.leaf));
    expect(archive.lookup(1)).toEqual(event);
  });

  it("cold bootstrap is separately visible, and new directory links precede index file use", async () => {
    const f = await fixture("sibling", false), measured = observe(f);
    await f.side.publish(command("cold"), [sender.id]);
    expect(measured.barriers.filter(b => b.directory && !b.held).map(b => b.file))
      .toEqual(chain(path.join(f.root, "event-receipts")));
    // Full first unknown chain, including newly created group and its containing namespace.
    for (const directory of chain(f.leaf)) expect(indexDirs(measured, 0)).toContain(directory);
    const firstUse = measured.events.findIndex(e => e.startsWith(`open:${path.join(f.leaf, "1.json.")}`));
    expect(firstUse).toBeGreaterThan(0);
    for (const directory of [path.dirname(f.leaf), f.leaf, f.archive]) {
      const at = measured.events.indexOf(`fsync:${directory}`);
      expect(at).toBeGreaterThanOrEqual(0); expect(at).toBeLessThan(firstUse);
    }
    expect(measured.barriers.some(b => b.kind === "fdatasync")).toBe(true);
    console.log("PREFIX_COLD " + JSON.stringify({ barriers: measured.barriers, events: measured.events }));
  });

  it("off-lock preparation EIO refuses before live append, with a valid stable retry", async () => {
    const f = await fixture(); let faults = 0;
    observe(f, { sync: b => {
      if (!b.held && b.directory && b.file === f.root && faults === 0) { faults++; throw eio(); }
    } });
    await expect(f.side.publish(command("preparation"), [sender.id])).rejects.toThrow("owed-fsync-EIO");
    expect(faults).toBe(1); expect(liveEvents(f)).toHaveLength(1);
    expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
    await retryExactlyOnce(f, "preparation");
  });

  it("unkeyed durable publication keeps full live namespace fence", async () => {
    const f = await fixture(), measured = observe(f);
    await f.store.publish({ topic: "fabric.control.command", from: sender, text: "unkeyed", durable: true });
    expect(liveDirs(measured)).toEqual(chain(f.root));
    expect(measured.barriers.filter(b => b.operation === "index" && b.directory)).toHaveLength(0); // advisory unkeyed sidecar.
  });

  it("actual live replacement after file fsync refuses opened-inode binding, retry keeps id/exactonce", async () => {
    const f = await fixture(), live = path.join(f.root, "events.jsonl"); let replaced = false;
    observe(f, { afterSync: b => {
      if (b.file !== live || b.directory || replaced) return;
      replaced = true; const bytes = fs.readFileSync(live); fs.renameSync(live, `${live}-old`); fs.writeFileSync(live, bytes);
    } });
    await expect(f.side.publish(command("binding"), [sender.id])).rejects.toThrow(/inode changed/);
    expect(replaced).toBe(true);
    const prior = liveEvents(f).find(event => event.data?.commandId === "binding"); expect(prior).toBeDefined();
    expect((await retryExactlyOnce(f, "binding")).id).toBe(prior.id);
  });

  it.each(["index-file", "index-leaf", "live-file", "live-leaf", "close"] as const)(
    "real publication propagates %s EIO once, no ACK; valid retry reconciles", async cut => {
      const f = await fixture(); let faults = 0;
      observe(f, { sync: b => {
        const hit = cut === "index-file" ? !b.directory && b.operation === "index"
          : cut === "index-leaf" ? b.directory && b.operation === "index" && b.file === f.leaf
          : cut === "live-file" ? !b.directory && b.file === path.join(f.root, "events.jsonl")
          : cut === "live-leaf" ? b.directory && b.operation === "live" && b.file === f.root : false;
        if (hit && faults === 0) { faults++; throw eio(); }
      }, close: file => {
        if (cut === "close" && file.startsWith(path.join(f.leaf, "2.json.")) && faults === 0) { faults++; throw eio(); }
      } });
      let ack = false;
      await expect(f.side.publish(command(cut), [sender.id]).then(r => { ack = true; return r; })).rejects.toThrow("owed-fsync-EIO");
      expect(ack).toBe(false); expect(faults).toBe(1);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      await retryExactlyOnce(f, cut);
    });
});
