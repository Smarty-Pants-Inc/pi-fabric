import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { openRecoveryJournal, type RecoveryJournal } from "../src/recovery/durability-adapter-journal.js";

// Keep: data safety/trust boundary and crash/fault paths unreachable cheaply in UI flows.
const roots: string[] = [];
const journals: RecoveryJournal[] = [];
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-journal-")); roots.push(root); return root; };
const open = async (directory: string, required = false, storeId = "store") => {
  const journal = await openRecoveryJournal({ directory, storeId, required }); journals.push(journal); return journal;
};
const options = () => ({ deadlineMs: Date.now() + 10_000 });
const rawNames = (root: string) => fs.readdirSync(root).filter(name => name.startsWith("raw-"));
afterEach(async () => { vi.restoreAllMocks(); for (const journal of journals.splice(0)) await journal.close(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

// Windows has no confirmed directory fsync in the shared namespace primitive; fail closed.
it.skipIf(process.platform !== "win32")("refuses unsupported namespace durability rather than returning a false receipt", async () => {
  await expect(open(temp())).rejects.toMatchObject({ reason: "store-unavailable" });
});
describe.skipIf(process.platform === "win32")("authoritative append-only recovery journal", () => {
  it("copies mutable input and all read views; concurrent retries retain one stable sequence", async () => {
    const root = temp(); const journal = await open(root);
    const input = Buffer.from("original");
    const pending = journal.append("key", input, options()); input.fill(0);
    expect(await pending).toEqual({ seq: 1, existing: false });
    const replies = await Promise.all(Array.from({ length: 12 }, () => journal.append("key", Buffer.from("original"), options())));
    expect(replies).toEqual(Array.from({ length: 12 }, () => ({ seq: 1, existing: true })));
    expect(rawNames(root)).toHaveLength(1);
    journal.get("key")!.bytes.fill(1); journal.entries()[0]!.bytes.fill(2);
    expect(Buffer.from(journal.get("key")!.bytes).toString()).toBe("original");
    await expect(journal.append("key", Buffer.from("changed"), options())).rejects.toMatchObject({ reason: "superseded" });
    expect(Buffer.from(journal.get("key")!.bytes).toString()).toBe("original");
    await journal.close();
    const recovered = await open(root, true);
    expect(await recovered.append("key", Buffer.from("original"), options())).toEqual({ seq: 1, existing: true });
    expect(await recovered.append("next", Buffer.from("next"), options())).toEqual({ seq: 2, existing: false });
  });

  it("REQUIRED refuses missing stores/identity and wrong store identity without inventing absence", async () => {
    const root = temp(); const missing = path.join(root, "missing");
    await expect(open(missing, true)).rejects.toMatchObject({ reason: "store-unavailable" });
    expect(fs.existsSync(missing)).toBe(false);
    const journal = await open(root); await journal.close();
    await expect(open(root, true, "another-store")).rejects.toMatchObject({ reason: "store-unavailable" });
    fs.renameSync(path.join(root, "journal.identity"), path.join(root, "retained-original-identity"));
    await expect(open(root, true)).rejects.toMatchObject({ reason: "store-unavailable" });
  });

  it.each(["file", "namespace"] as const)("a real %s fsync error fails the barrier and invalidates all cached authority until recovery", async target => {
    const root = temp(); const journal = await open(root);
    const real = fs.fsyncSync;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory() === (target === "namespace")) throw Object.assign(new Error("fsync EIO"), { code: "EIO" });
      return real(fd);
    });
    await expect(journal.append("pending", Buffer.from("pending"), options())).rejects.toMatchObject({ reason: "store-unavailable" });
    sync.mockRestore();
    expect(() => journal.get("absent")).toThrow(expect.objectContaining({ reason: "store-unavailable" }));
    await journal.close();
    const recovered = await open(root, true);
    expect(await recovered.append("pending", Buffer.from("pending"), options())).toEqual({ seq: 1, existing: true });
    expect(rawNames(root)).toHaveLength(1);
  });

  it("preserves an interrupted uncommitted raw segment and appends only to a NEW segment", async () => {
    const root = temp(); const journal = await open(root);
    const real = fs.writeSync;
    let partial: Buffer | undefined;
    const write = vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, bytes: Uint8Array, offset: number, length: number) => {
      const size = Math.max(1, Math.floor(length / 2));
      Reflect.apply(real, fs, [fd, bytes, offset, size]); partial = Buffer.from(bytes.subarray(offset, offset + size));
      throw Object.assign(new Error("interrupted write"), { code: "EIO" });
    }) as unknown as typeof fs.writeSync);
    await expect(journal.append("unfinished", Buffer.from("unfinished"), options())).rejects.toMatchObject({ reason: "store-unavailable" });
    write.mockRestore();
    const old = rawNames(root)[0]!;
    expect(fs.readFileSync(path.join(root, old))).toEqual(partial);
    await journal.close(); const recovered = await open(root, true);
    // All witnesses/complete sequences validate: this unwitnessed tail was never ACKed.
    expect(recovered.get("unfinished")).toBeUndefined();
    expect(await recovered.append("finished", Buffer.from("finished"), options())).toEqual({ seq: 1, existing: false });
    expect(rawNames(root)).toHaveLength(2);
    expect(fs.readFileSync(path.join(root, old))).toEqual(partial);
  });

  it("missing/wrong retained committed ranges are unavailable, preserving the original evidence", async () => {
    const root = temp(); const journal = await open(root);
    await journal.append("key", Buffer.from("exact evidence"), options()); await journal.close();
    const raw = rawNames(root)[0]!;
    fs.mkdirSync(path.join(root, "preserved"));
    fs.renameSync(path.join(root, raw), path.join(root, "preserved", raw));
    await expect(open(root, true)).rejects.toMatchObject({ reason: "store-unavailable" });
    expect(fs.readFileSync(path.join(root, "preserved", raw)).length).toBeGreaterThan(0);
    // A committed prefix replaced by a torn segment cannot be reclassified as uncommitted.
    fs.writeFileSync(path.join(root, raw), "{incomplete", { flag: "wx" });
    await expect(open(root, true)).rejects.toMatchObject({ reason: "store-unavailable" });
  });

  it("availability loss latches even if the exact retained file comes back; reopening is required", async () => {
    const root = temp(); const journal = await open(root);
    await journal.append("key", Buffer.from("exact"), options());
    const raw = rawNames(root)[0]!; const saved = path.join(root, "saved-range");
    fs.renameSync(path.join(root, raw), saved);
    expect(() => journal.get("absent")).toThrow(expect.objectContaining({ reason: "store-unavailable" }));
    fs.renameSync(saved, path.join(root, raw));
    expect(() => journal.entries()).toThrow(expect.objectContaining({ reason: "store-unavailable" }));
    await journal.close(); const recovered = await open(root, true);
    expect(recovered.get("key")?.seq).toBe(1);
  });

  it("checks original deadlines/abort before I/O and after commit without erasing a late committed append", async () => {
    const root = temp(); const journal = await open(root);
    await expect(journal.append("expired", Buffer.from("x"), { deadlineMs: 0 })).rejects.toMatchObject({ reason: "deadline" });
    const already = new AbortController(); already.abort();
    await expect(journal.append("aborted", Buffer.from("x"), { ...options(), signal: already.signal })).rejects.toMatchObject({ reason: "aborted" });
    expect(rawNames(root)).toHaveLength(0);
    const abort = new AbortController(); const real = fs.fsyncSync;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { real(fd); abort.abort(); });
    await expect(journal.append("late", Buffer.from("late"), { ...options(), signal: abort.signal })).rejects.toMatchObject({ reason: "aborted" });
    sync.mockRestore();
    expect(await journal.append("late", Buffer.from("late"), options())).toEqual({ seq: 1, existing: true });
    expect(rawNames(root)).toHaveLength(1);
  });

  it("foreign custody is not reclaimed from age, an unknown host or a PID assertion", async () => {
    const root = temp(); const lock = path.join(root, "custody.lock"); fs.mkdirSync(lock);
    const owner = `test-owner\n${process.pid}\n0\nunknown-incarnation\nuntrusted-machine\nunknown-boot\nunknown-namespace\n`;
    fs.writeFileSync(path.join(lock, "owner"), owner);
    await expect(open(root)).rejects.toMatchObject({ reason: "store-unavailable" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    expect(fs.existsSync(path.join(root, "journal.identity"))).toBe(false);
  });

  it("corrupt complete frames and unknown segment inventory never become authoritative absence", async () => {
    const root = temp(); const journal = await open(root); await journal.close();
    const raw = "raw-0000000000000001-11111111-1111-1111-1111-111111111111.jsonl";
    fs.writeFileSync(path.join(root, raw), '{"complete":"but-invalid"}\n', { flag: "wx" });
    await expect(open(root, true)).rejects.toMatchObject({ reason: "store-unavailable" });
    fs.renameSync(path.join(root, raw), path.join(root, "preserved-invalid-frame"));
    fs.writeFileSync(path.join(root, "raw-unknown"), "unknown raw evidence", { flag: "wx" });
    await expect(open(root, true)).rejects.toMatchObject({ reason: "store-unavailable" });
  });

  it("rejects a queued expired deadline and enforces one writer and closed-store failures", async () => {
    const root = temp(); const journal = await open(root);
    await expect(open(root)).rejects.toMatchObject({ reason: "store-unavailable" });
    const deadlineMs = Date.now() + 100;
    const real = fs.fsyncSync; const now = Date.now();
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { real(fd); vi.spyOn(Date, "now").mockReturnValue(now + 200); });
    const first = journal.append("first", Buffer.from("first"), { deadlineMs });
    const second = journal.append("queued", Buffer.from("queued"), { deadlineMs });
    await expect(first).rejects.toMatchObject({ reason: "deadline" });
    await expect(second).rejects.toMatchObject({ reason: "deadline" });
    sync.mockRestore(); vi.restoreAllMocks();
    expect(rawNames(root)).toHaveLength(1);
    expect(journal.get("first")?.seq).toBe(1);
    await journal.close();
    expect(() => journal.get("none")).toThrow(expect.objectContaining({ reason: "store-unavailable" }));
    await expect(journal.append("closed", Buffer.from("x"), options())).rejects.toMatchObject({ reason: "store-unavailable" });
  });
});
