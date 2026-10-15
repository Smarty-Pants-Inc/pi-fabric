import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { controlBodyFingerprint, FABRIC_IDEMPOTENCY_KEY_REUSED, FabricIdempotencyKeyReusedError } from "../src/mesh/control-idempotency.js";
import { MeshStore } from "../src/mesh/store.js";

// fv2-coldpub design C: a keyed publish confirms the complete `event-receipts` namespace before
// `.lock`; inside it only fresh file and LEAF-directory barriers run, unless the namespace changed.
// Product receipt/intent/temp/PENDING.json deletions are RECORDED AND SKIPPED wherever they are (#4664):
// a skipped intent unlink models the conservative crash resurrection of that intent. Only owned
// lock-release cleanup inside the private root, and the teardown after restoreMocks, really delete.

const bases: string[] = [];
const from = { id: "session:receipt-ns", name: "receipt-ns", kind: "main" as const };
const removed: string[] = [];
const skipped: string[] = [];
let skippedTotal = 0;
const productDelete = (file: string) => path.basename(path.dirname(file)) === "event-receipts" ||
  path.basename(file) === "PENDING.json" || file.endsWith(".tmp");
/** base/real/mesh, reached through base/link -> real: the namespace has a symlink parent. */
const fixture = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "mesh-receipt-ns-")));
  bases.push(base);
  fs.mkdirSync(path.join(base, "real"));
  fs.symlinkSync(path.join(base, "real"), path.join(base, "link"));
  const root = path.join(base, "link", "mesh");
  const real = path.join(base, "real", "mesh");
  const rm = fs.rmSync.bind(fs);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    // Recorded by real parent path (the product reaches the root through base/link).
    let target = String(file);
    try { target = path.join(fs.realpathSync(path.dirname(target)), path.basename(target)); } catch { /* recorded as given */ }
    removed.push(target);
    if (productDelete(target)) { skipped.push(target); skippedTotal++; return; }
    if (!fs.realpathSync(path.dirname(String(file))).startsWith(base + path.sep)) throw new Error(`refused deletion outside fixture: ${String(file)}`);
    rm(file, options);
  });
  return { base, root, real, receipts: path.join(real, "event-receipts") };
};
const hash = (key: string) => createHash("sha256").update(key).digest("hex");
type Sync = { file: string; held: boolean; dir: boolean; appended: boolean };
const watch = (root: string) => {
  const syncs: Sync[] = [];
  let appended = false, fail: ((sync: Sync) => boolean) | undefined;
  const append = fs.appendFileSync.bind(fs);
  vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
    append(file, data, options);
    if (file === path.join(root, "events.jsonl")) appended = true;
  });
  const original = fs.fsyncSync.bind(fs);
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    const sync = { file: fs.readlinkSync(`/proc/self/fd/${fd}`), held: fs.existsSync(path.join(root, ".lock")), dir: fs.fstatSync(fd).isDirectory(), appended };
    syncs.push(sync);
    if (fail?.(sync)) { fail = undefined; throw Object.assign(new Error("injected EIO"), { code: "EIO" }); }
    original(fd);
  });
  return {
    syncs,
    failOnce: (predicate: (sync: Sync) => boolean) => { fail = predicate; },
    reset: () => { syncs.length = 0; appended = false; },
  };
};
/** Runs `mutate` once, synchronously, as the next `.lock` acquisition is attempted (after preparation). */
const beforeLock = (root: string, mutate: () => void) => {
  const mkdir = fs.mkdirSync.bind(fs);
  let done = false;
  vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
    if (!done && String(file) === path.join(root, ".lock")) { done = true; mutate(); }
    return mkdir(file, options);
  }) as typeof fs.mkdirSync);
};
const keyed = (key: string) => ({ topic: "mesh.receipt-ns", from, dedupeKey: key, text: key });
const once = (mesh: MeshStore, key: string) => mesh.read({ limit: 1000 }).filter(event => event.dedupeKey === key);

afterEach(() => {
  vi.restoreAllMocks();
  for (const base of bases.splice(0)) fs.rmSync(base, { recursive: true, force: true });
  removed.length = 0;
  skipped.length = 0;
});
afterAll(() => { console.log(`mesh-receipt-namespace: product deletions recorded and SKIPPED: ${skippedTotal}`); });

describe.skipIf(process.platform !== "linux")("keyed publish: prepared receipt namespace (fv2-coldpub C)", () => {
  it("cold and warm keyed publishes run only fresh file + leaf barriers under .lock; the ancestor chain is confirmed before it", async () => {
    const { root, real, receipts, base } = fixture();
    const sync = watch(root);
    const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    for (const key of ["cold", "warm"]) {
      sync.reset();
      const event = await mesh.publish(keyed(key));
      const held = sync.syncs.filter(entry => entry.held);
      expect(held.filter(entry => entry.dir).map(entry => entry.file)).toEqual([receipts, receipts, receipts]);
      expect(held.filter(entry => !entry.dir).map(entry => path.basename(entry.file))).toEqual([
        expect.stringMatching(new RegExp(`^${hash(key)}\\.pending\\.json\\.\\d+\\..+\\.tmp$`)),
        expect.stringMatching(new RegExp(`^${hash(key)}\\.json\\.\\d+\\..+\\.tmp$`)),
      ]);
      // Fresh intent barriers precede the live append; receipt + unlink barriers follow it.
      expect(held.map(entry => entry.appended)).toEqual([false, false, true, true, true]);
      const preflight = sync.syncs.filter(entry => !entry.held && entry.dir).map(entry => entry.file);
      for (const ancestor of [receipts, real, path.join(base, "real"), base, "/"]) expect(preflight).toContain(ancestor);
      expect(once(mesh, key)).toEqual([event]);
      expect(JSON.parse(fs.readFileSync(path.join(receipts, `${hash(key)}.json`), "utf8"))).toEqual(event);
      // The unlink was requested exactly once (recorded, skipped), then leaf-synced (held count above).
      expect(skipped.filter(file => file === path.join(receipts, `${hash(key)}.pending.json`))).toHaveLength(1);
    }
    expect(removed.filter(file => file.endsWith(".pending.json"))).toEqual(["cold", "warm"].map(key => path.join(receipts, `${hash(key)}.pending.json`)));
    // Resurrected intent + durable receipt: a rebooted retry returns the authoritative identity once.
    const receipt = JSON.parse(fs.readFileSync(path.join(receipts, `${hash("cold")}.json`), "utf8"));
    const rebooted = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    expect(await rebooted.publish(keyed("cold"))).toEqual(receipt);
    expect(once(rebooted, "cold")).toEqual([receipt]);
  });

  // Keep reason: data safety. A prepared receipt namespace must never acknowledge another body.
  it.each([false, true])("prepared namespace preserves fingerprinted retry identity and rejects changed input without appending (stamp=%s)", async stamp => {
    const { root, receipts } = fixture();
    const sync = watch(root);
    const options = { maxEventLogBytes: 230 * 1024 * 1024 };
    const mesh = new MeshStore(root, 1024, 100, options);
    const body = { message: "original", data: { value: 1 } };
    const data = (command: typeof body) => stamp ? (createdAt: number) => ({ ...command, requestedAt: createdAt }) : command;
    const input = { ...keyed(`fingerprint-${stamp}`), dedupeFingerprint: controlBodyFingerprint(body), data: data(body) };
    const event = await mesh.publish(input);
    expect(event.dedupeFingerprint).toBe(input.dedupeFingerprint);
    expect(sync.syncs.filter(entry => entry.held && entry.dir).map(entry => entry.file)).toEqual([receipts, receipts, receipts]);
    const livePath = path.join(root, "events.jsonl");
    const receiptPath = path.join(receipts, `${hash(input.dedupeKey)}.json`);
    const live = fs.readFileSync(livePath, "utf8"), receipt = fs.readFileSync(receiptPath, "utf8");
    expect(live).toBe(`${JSON.stringify(event)}\n`);
    expect(receipt).toBe(JSON.stringify(event));
    const sequence = mesh.latestSequence();
    const rebooted = new MeshStore(root, 1024, 100, options);
    expect(await rebooted.publish(input)).toEqual(event);
    const changed = { ...body, data: { value: 2 } };
    const refused = rebooted.publish({ ...input, data: data(changed), dedupeFingerprint: controlBodyFingerprint(changed) });
    await expect(refused).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
    await expect(refused).rejects.toMatchObject({ code: FABRIC_IDEMPOTENCY_KEY_REUSED, retryable: false, idempotencyKey: input.dedupeKey });
    expect(fs.readFileSync(livePath, "utf8")).toBe(live);
    expect(rebooted.latestSequence()).toBe(sequence);
    expect(fs.readFileSync(receiptPath, "utf8")).toBe(receipt);
    expect(once(rebooted, input.dedupeKey)).toEqual([event]);
  });

  it("a current-day archived keyed publish uses leaf barriers for receipt install + unlink; archive barriers remain", async () => {
    const { root, real, receipts, base } = fixture();
    const archiveBase = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "mesh-receipt-ns-archive-")));
    bases.push(archiveBase);
    const archive = path.join(archiveBase, "org", "mesh", "dev1", "fleet");
    fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
    const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    const sync = watch(root);
    const datasyncs: string[] = [];
    const datasync = fs.fdatasyncSync.bind(fs);
    vi.spyOn(fs, "fdatasyncSync").mockImplementation(fd => { datasyncs.push(fs.readlinkSync(`/proc/self/fd/${fd}`)); datasync(fd); });
    const event = await mesh.publish(keyed("archived"));
    const held = sync.syncs.filter(entry => entry.held);
    // Intent leaf, receipt leaf, unlink leaf: no receipt-namespace ancestor is fsynced under .lock.
    expect(held.filter(entry => entry.dir && entry.file === receipts)).toHaveLength(3);
    // (The live-log confirmation before the receipt still syncs the mesh root chain, unchanged.)
    const files = held.map(entry => entry.file);
    const intentTmp = files.findIndex(file => path.dirname(file) === receipts && file.includes(".pending.json."));
    const receiptTmp = files.findIndex(file => path.dirname(file) === receipts && /[a-f0-9]{64}\.json\.\d+\..+\.tmp$/.test(file));
    expect(files[intentTmp + 1]).toBe(receipts);
    expect(files[intentTmp + 2]).not.toBe(real);
    // Receipt install leaf, then the intent-unlink leaf: nothing else follows the receipt file fsync.
    expect(files.slice(receiptTmp + 1)).toEqual([receipts, receipts]);
    expect(receiptTmp).toBeGreaterThan(files.indexOf(path.join(real, "events.jsonl")));
    expect(held.filter(entry => !entry.dir && path.dirname(entry.file) === receipts).map(entry => path.basename(entry.file))).toEqual([
      expect.stringMatching(new RegExp(`^${hash("archived")}\\.pending\\.json\\.\\d+\\..+\\.tmp$`)),
      expect.stringMatching(new RegExp(`^${hash("archived")}\\.json\\.\\d+\\..+\\.tmp$`)),
    ]);
    // Archive durability is untouched: segment data barrier plus its fresh-day directory barriers.
    const today = new Date(event.createdAt).toISOString().slice(0, 10).replaceAll("-", "/");
    expect(datasyncs.some(file => file.startsWith(path.join(archive, today) + path.sep))).toBe(true);
    expect(held.some(entry => entry.dir && entry.file === path.join(archive, today))).toBe(true);
    expect(once(mesh, "archived")).toEqual([event]);
    expect(JSON.parse(fs.readFileSync(path.join(receipts, `${hash("archived")}.json`), "utf8"))).toEqual(event);
    expect(skipped).toContain(path.join(receipts, `${hash("archived")}.pending.json`));
    expect(skipped).toContain(path.join(archive, "PENDING.json"));
  });

  for (const [name, mutate] of [
    ["an ancestor symlink retargeted (same target)", ({ base }: ReturnType<typeof fixture>) => {
      fs.unlinkSync(path.join(base, "link"));
      fs.symlinkSync(path.join(base, "real"), path.join(base, "link"));
    }],
    ["the receipt directory recreated", ({ receipts }: ReturnType<typeof fixture>) => {
      fs.renameSync(receipts, `${receipts}.old`);
      fs.mkdirSync(receipts, { mode: 0o700 });
    }],
  ] as const) {
    it(`${name} between preparation and .lock falls back to the complete barrier`, async () => {
      const paths = fixture();
      const mesh = new MeshStore(paths.root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
      await mesh.publish(keyed("seed"));
      const sync = watch(paths.root);
      beforeLock(paths.root, () => mutate(paths));
      const event = await mesh.publish(keyed("moved"));
      const heldDirs = sync.syncs.filter(entry => entry.held && entry.dir).map(entry => entry.file);
      expect(heldDirs).toContain("/");
      expect(heldDirs).toContain(paths.base);
      expect(once(mesh, "moved")).toEqual([event]);
      expect(JSON.parse(fs.readFileSync(path.join(paths.receipts, `${hash("moved")}.json`), "utf8"))).toEqual(event);
    });
  }

  it("a failed preparation barrier appends nothing; a retry commits once", async () => {
    const { root } = fixture();
    const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    const sync = watch(root);
    sync.failOnce(entry => !entry.held && entry.dir);
    await expect(mesh.publish(keyed("prep"))).rejects.toThrow("injected EIO");
    expect(mesh.read({ limit: 100 })).toEqual([]);
    expect(sync.syncs.some(entry => entry.held)).toBe(false);
    const event = await mesh.publish(keyed("prep"));
    expect(once(mesh, "prep")).toEqual([event]);
  });

  it("a failed intent leaf barrier is not acknowledged and no live event is appended", async () => {
    const { root } = fixture();
    const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    const sync = watch(root);
    sync.failOnce(entry => entry.held && entry.dir && !entry.appended);
    await expect(mesh.publish(keyed("leaf"))).rejects.toThrow("injected EIO");
    expect(once(mesh, "leaf")).toEqual([]);
    const event = await mesh.publish(keyed("leaf"));
    expect(once(mesh, "leaf")).toEqual([event]);
  });

  it("post-live receipt-file fsync failure keeps the intent; a rebooted retry rejects a changed fingerprint and returns the exact identity", async () => {
    const { root, receipts } = fixture();
    const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    const body = { message: "original" };
    const input = { ...keyed("receipt"), data: body, dedupeFingerprint: controlBodyFingerprint(body) };
    const sync = watch(root);
    sync.failOnce(entry => entry.held && !entry.dir && entry.appended && /[a-f0-9]{64}\.json\.\d+\..+\.tmp$/.test(entry.file));
    await expect(mesh.publish(input)).rejects.toThrow("injected EIO");
    const [committed] = once(mesh, "receipt");
    expect(committed).toBeDefined();
    expect(committed?.dedupeFingerprint).toBe(input.dedupeFingerprint);
    const pendingPath = path.join(receipts, `${hash("receipt")}.pending.json`);
    expect(fs.existsSync(pendingPath)).toBe(true);
    expect(fs.existsSync(path.join(receipts, `${hash("receipt")}.json`))).toBe(false);
    const livePath = path.join(root, "events.jsonl");
    const live = fs.readFileSync(livePath, "utf8"), pending = fs.readFileSync(pendingPath, "utf8");
    const sequence = mesh.latestSequence();
    const rebooted = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    const changed = { message: "changed" };
    await expect(rebooted.publish({ ...input, data: changed, dedupeFingerprint: controlBodyFingerprint(changed) }))
      .rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
    expect(fs.readFileSync(livePath, "utf8")).toBe(live);
    expect(rebooted.latestSequence()).toBe(sequence);
    expect(fs.readFileSync(pendingPath, "utf8")).toBe(pending);
    expect(fs.existsSync(path.join(receipts, `${hash("receipt")}.json`))).toBe(false);
    expect(await rebooted.publish(input)).toEqual(committed);
    expect(fs.readFileSync(livePath, "utf8")).toBe(live);
    expect(once(rebooted, "receipt")).toEqual([committed]);
    expect(JSON.parse(fs.readFileSync(path.join(receipts, `${hash("receipt")}.json`), "utf8"))).toEqual(committed);
    expect(skipped).toContain(pendingPath);
  });

  it("an unlink leaf-barrier failure keeps the durable receipt authoritative; the retry cannot append again", async () => {
    const { root, receipts } = fixture();
    const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    const sync = watch(root);
    let postLiveLeaves = 0;
    sync.failOnce(entry => entry.held && entry.dir && entry.appended && ++postLiveLeaves === 2);
    await expect(mesh.publish(keyed("unlink"))).rejects.toThrow("injected EIO");
    const [committed] = once(mesh, "unlink");
    expect(JSON.parse(fs.readFileSync(path.join(receipts, `${hash("unlink")}.json`), "utf8"))).toEqual(committed);
    const rebooted = new MeshStore(root, 1024, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
    expect(await rebooted.publish(keyed("unlink"))).toEqual(committed);
    expect(once(rebooted, "unlink")).toEqual([committed]);
  });

  it("another writer commits between preparation and .lock: distinct keys stay strictly monotonic, a same key settles once", async () => {
    const { root, receipts } = fixture();
    const options = { maxEventLogBytes: 230 * 1024 * 1024 };
    const solo = new MeshStore(root, 1024, 100, options);
    const first = await solo.publish(keyed("solo"));
    // No other writer: the next keyed publish takes exactly the next sequence.
    expect((await solo.publish(keyed("solo-next"))).sequence).toBe(first.sequence + 1);
    const order: string[] = [];
    const mkdir = fs.mkdirSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, mode?: fs.MakeDirectoryOptions) => {
      if (String(file).endsWith("event-receipts")) order.push("prepare");
      if (String(file) === path.join(root, ".lock")) order.push("lock");
      return mkdir(file, mode);
    }) as typeof fs.mkdirSync);
    void receipts;
    const a = new MeshStore(root, 1024, 100, options), b = new MeshStore(root, 1024, 100, options);
    // B prepares and takes .lock first; A prepares while B holds it, so B commits between A's
    // preparation and A's lock. A must still reserve a strictly greater current sequence.
    const pendingRight = b.publish(keyed("right"));
    const pendingLeft = a.publish(keyed("left"));
    expect(order.slice(0, 3)).toEqual(["prepare", "lock", "prepare"]);
    const [right, left] = await Promise.all([pendingRight, pendingLeft]);
    expect(left.sequence).toBeGreaterThan(right.sequence);
    order.length = 0;
    const pendingSame = b.publish(keyed("same"));
    const pendingSameA = a.publish(keyed("same"));
    expect(order.slice(0, 3)).toEqual(["prepare", "lock", "prepare"]);
    const [same1, same2] = await Promise.all([pendingSame, pendingSameA]);
    expect(same2).toEqual(same1);
    const log = solo.read({ limit: 1000 });
    const sequences = log.map(event => event.sequence);
    expect(sequences).toEqual([...sequences].sort((x, y) => x - y));
    expect(new Set(sequences).size).toBe(sequences.length);
    for (const key of ["solo", "solo-next", "left", "right", "same"]) expect(once(solo, key)).toHaveLength(1);
  });
});
