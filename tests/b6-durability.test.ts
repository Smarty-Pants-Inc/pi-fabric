import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { openRecoveryDurabilityStore, type RecoveryDurabilityStore, type RecoveryDurabilityStoreOptions } from "../src/recovery/durability-adapter.js";
import { openRecoveryJournal } from "../src/recovery/durability-adapter-journal.js";
import type { RecoveryNativeEntryRegistrationRequest } from "../src/recovery/l7-interfaces.js";

// Keep: persistence data safety, trusted native receipt boundary, real subprocess crash.
const roots: string[] = [];
const stores: RecoveryDurabilityStore[] = [];
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-barrier-")); roots.push(root); return root; };
const sha = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
const binding = (directory: string) => ({ directory, storeId: "store", sessionId: "session", sessionFile: path.join(directory, "native-never-opened.jsonl"), verifyNativeEntry: async () => true });
const open = async (directory: string, extra: Partial<RecoveryDurabilityStoreOptions> = {}) => {
  const store = await openRecoveryDurabilityStore({ ...binding(directory), ...extra }); stores.push(store); return store;
};
const request = (recordKind: "control-seen" | "carrier" = "carrier") => ({ sessionId: "session", deliveryId: "delivery", recordKind, deadlineMs: Date.now() + 10_000 });
const native = (root: string, extra: Partial<RecoveryNativeEntryRegistrationRequest> = {}): RecoveryNativeEntryRegistrationRequest => {
  const bytes = Buffer.from('{ "type":"message", "id":"entry", "message":{"role":"user","content":"é"} }\n');
  return { ...request(), recordKind: "carrier", entryId: "entry", location: { file: binding(root).sessionFile, offset: 12, length: bytes.length - 1 }, bytes, bytesSha256: sha(bytes), flushReceipt: { kind: "file-fsync", file: binding(root).sessionFile, bytesThrough: 12 + bytes.length, namespaceDurable: true }, ...extra };
};
const records = (root: string) => fs.readdirSync(root).filter(name => name.startsWith("raw-"));
afterEach(async () => { vi.restoreAllMocks(); for (const store of stores.splice(0)) await store.close(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

// The shared namespace primitive explicitly refuses unsupported Windows durability.
describe.skipIf(process.platform === "win32")("recovery durability barrier", () => {
  it("never acknowledges staged bytes when the real journal fsync fails", async () => {
    const store = await openRecoveryDurabilityStore({
      directory: temp(), storeId: "store", sessionId: "session", sessionFile: "/native/session.jsonl",
      verifyNativeEntry: async () => true,
    });
    await store.stageControlSeen({ sessionId: "session", deliveryId: "delivery", bytes: new TextEncoder().encode("exact control bytes") });
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw Object.assign(new Error("durability failure"), { code: "EIO" }); });
    expect(await store.awaitDurableAppend({ sessionId: "session", deliveryId: "delivery", recordKind: "control-seen", deadlineMs: Date.now() + 5_000 }))
      .toEqual({ durable: false, reason: "store-unavailable" });
    sync.mockRestore();
    expect(await store.readDurableEntry({ sessionId: "session", deliveryId: "delivery", recordKind: "control-seen" }))
      .toEqual({ found: false, reason: "store-unavailable" });
    await store.close();
  });

  it("a real kernel fsync EBADF, not a fake adapter refusal, prevents barrier ACK", async () => {
    const root = temp(); const store = await open(root);
    await store.stageControlSeen({ sessionId: "session", deliveryId: "delivery", bytes: Buffer.from("kernel-fault-control") });
    // Keep a separate fixture fd occupied until the actual barrier reaches fsync, then
    // close it and invoke the real syscall on that now-invalid descriptor.
    const invalid = fs.openSync(path.join(root, "fsync-fault-fixture"), "wx");
    const real = fs.fsyncSync; let syscallError: unknown;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementationOnce(() => {
      fs.closeSync(invalid);
      try { real(invalid); } catch (error) { syscallError = error; throw error; }
    });
    expect(await store.awaitDurableAppend(request("control-seen"))).toEqual({ durable: false, reason: "store-unavailable" });
    expect(syscallError).toMatchObject({ code: "EBADF" }); sync.mockRestore();
    expect(await store.readDurableEntry(request("control-seen"))).toEqual({ found: false, reason: "store-unavailable" });
    await store.close(); const recovered = await open(root, { required: true });
    expect(await recovered.awaitDurableAppend(request("control-seen"))).toEqual({ durable: true, seq: 2 });
    expect(records(root)).toHaveLength(2);
  });

  it("registration is correlation only; barrier copies exact native bytes, dedupes and recovers", async () => {
    const root = temp(); const store = await open(root); const descriptor = native(root);
    const exact = Uint8Array.from(descriptor.bytes);
    const pending = store.registerNativeEntry(descriptor); descriptor.bytes.fill(0);
    expect(await pending).toEqual({ registered: true, existing: false });
    expect(await store.readDurableEntry(request())).toEqual({ found: false, reason: "not-found" });
    const replies = await Promise.all(Array.from({ length: 8 }, () => store.awaitDurableAppend(request())));
    expect(replies).toEqual(Array.from({ length: 8 }, () => ({ durable: true, seq: 2 })));
    expect(records(root)).toHaveLength(2); // session binding + one immutable delivery
    const read = await store.readDurableEntry(request()); expect(read).toEqual({ found: true, seq: 2, bytes: exact });
    if (read.found) read.bytes.fill(9);
    expect(await store.readDurableEntry(request())).toEqual({ found: true, seq: 2, bytes: exact });
    expect(fs.existsSync(binding(root).sessionFile)).toBe(false);
    await store.close(); const recovered = await open(root, { required: true });
    expect(await recovered.awaitDurableAppend(request())).toEqual({ durable: true, seq: 2 });
    expect(await recovered.readDurableEntry(request())).toEqual({ found: true, seq: 2, bytes: exact });
    expect(await recovered.registerNativeEntry(native(root))).toEqual({ registered: true, existing: true });
  });

  it("native provenance is mandatory; owned-prefix evidence cannot be certified from its structure/entry hash", async () => {
    const root = temp(); const calls: RecoveryNativeEntryRegistrationRequest[] = [];
    const store = await open(root, { verifyNativeEntry: async value => { calls.push(value); return value.flushReceipt.kind === "owned-journal-commit" && value.flushReceipt.sha256 === sha("trusted native prefix"); } });
    const descriptor = native(root, { flushReceipt: { kind: "owned-journal-commit", file: binding(root).sessionFile, bytes: 500, sha256: sha(native(root).bytes) } });
    expect(await store.registerNativeEntry(descriptor)).toEqual({ registered: false, reason: "conflict" });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: false, reason: "not-found" });
    expect(calls).toHaveLength(1);
    expect(await store.registerNativeEntry({ ...descriptor, flushReceipt: { ...descriptor.flushReceipt, kind: "owned-journal-commit", bytes: 500, sha256: sha("trusted native prefix") } })).toEqual({ registered: true, existing: false });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 2 });
    // A larger valid prefix may strengthen the same identity but cannot allocate a new seq.
    expect(await store.registerNativeEntry({ ...descriptor, flushReceipt: { kind: "owned-journal-commit", file: binding(root).sessionFile, bytes: 900, sha256: sha("trusted native prefix") } })).toEqual({ registered: true, existing: true });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 2 });
  });

  it.each(["session", "kind", "file", "offset", "length", "overflow", "lf", "hash", "entryId", "receipt-file", "coverage", "namespace", "owned-empty", "owned-hash", "reserved-id"] as const)("rejects frozen native contract violation: %s", async fault => {
    const root = temp(); const verify = vi.fn(async () => true); const store = await open(root, { verifyNativeEntry: verify });
    const original = native(root); let descriptor = original;
    if (fault === "session") descriptor = { ...original, sessionId: "other" };
    if (fault === "kind") descriptor = { ...original, recordKind: "control-seen" } as unknown as RecoveryNativeEntryRegistrationRequest;
    if (fault === "file") descriptor = { ...original, location: { ...original.location, file: path.join(root, "different") } };
    if (fault === "offset") descriptor = { ...original, location: { ...original.location, offset: -1 } };
    if (fault === "length") descriptor = { ...original, location: { ...original.location, length: original.bytes.length } };
    if (fault === "overflow") descriptor = { ...original, location: { ...original.location, offset: Number.MAX_SAFE_INTEGER } };
    if (fault === "lf") { const bytes = Uint8Array.from(original.bytes); bytes[bytes.length - 1] = 32; descriptor = { ...original, bytes, bytesSha256: sha(bytes) }; }
    if (fault === "hash") descriptor = { ...original, bytesSha256: original.bytesSha256.toUpperCase() };
    if (fault === "entryId") descriptor = { ...original, entryId: "different" };
    if (fault === "receipt-file") descriptor = { ...original, flushReceipt: { ...original.flushReceipt, file: path.join(root, "different") } };
    if (fault === "coverage") descriptor = { ...original, flushReceipt: { kind: "file-fsync", file: binding(root).sessionFile, bytesThrough: original.location.offset + original.location.length, namespaceDurable: true } };
    if (fault === "namespace") descriptor = { ...original, flushReceipt: { ...original.flushReceipt, namespaceDurable: false } } as unknown as RecoveryNativeEntryRegistrationRequest;
    if (fault === "owned-empty") descriptor = { ...original, flushReceipt: { kind: "owned-journal-commit", file: binding(root).sessionFile, bytes: 0, sha256: sha("prefix") } };
    if (fault === "owned-hash") descriptor = { ...original, flushReceipt: { kind: "owned-journal-commit", file: binding(root).sessionFile, bytes: 500, sha256: "bad" } };
    if (fault === "reserved-id") descriptor = { ...original, deliveryId: "pi-native-entry:v1:wrong" };
    expect(await store.registerNativeEntry(descriptor)).toEqual({ registered: false, reason: "conflict" });
    expect(verify).not.toHaveBeenCalled();
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: false, reason: "not-found" });
  });

  it.each(["entry", "location", "bytes"] as const)("a changed %s binding conflicts, supersedes reads/barrier, and preserves the first raw bytes", async fault => {
    const root = temp(); const store = await open(root); const first = native(root);
    expect(await store.registerNativeEntry(first)).toEqual({ registered: true, existing: false });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 2 });
    const retained = records(root).map(name => [name, fs.readFileSync(path.join(root, name))] as const);
    let next = native(root);
    if (fault === "entry") { const bytes = Buffer.from('{"id":"different"}\n'); next = { ...next, entryId: "different", bytes, bytesSha256: sha(bytes), location: { ...next.location, length: bytes.length - 1 } }; }
    if (fault === "location") next = { ...next, location: { ...next.location, offset: next.location.offset - 1 } };
    if (fault === "bytes") { const bytes = Buffer.from('{"id":"entry","message":"different"}\n'); next = { ...next, bytes, bytesSha256: sha(bytes), location: { ...next.location, length: bytes.length - 1 } }; }
    expect(await store.registerNativeEntry(next)).toEqual({ registered: false, reason: "conflict" });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: false, reason: "superseded" });
    expect(await store.readDurableEntry(request())).toEqual({ found: false, reason: "superseded" });
    for (const [name, bytes] of retained) expect(fs.readFileSync(path.join(root, name))).toEqual(bytes);
    await store.close(); const recovered = await open(root, { required: true });
    expect(await recovered.readDurableEntry(request())).toEqual({ found: true, seq: 2, bytes: Uint8Array.from(first.bytes) });
  });

  it("the same native entry cannot alias two deliveries; derived references check exact session/entry identity", async () => {
    const root = temp(); const store = await open(root); const descriptor = native(root);
    const deliveryId = "pi-native-entry:v1:" + sha(JSON.stringify(["session", "entry"]));
    expect(await store.registerNativeEntry({ ...descriptor, deliveryId })).toEqual({ registered: true, existing: false });
    expect(await store.registerNativeEntry(descriptor)).toEqual({ registered: false, reason: "conflict" });
    expect(await store.awaitDurableAppend({ ...request(), deliveryId })).toEqual({ durable: false, reason: "superseded" });
  });

  it("detaches mutable location/receipt/byte arrays before an asynchronous verifier and prevents verifier mutation", async () => {
    const root = temp(); let finish!: (accepted: boolean) => void;
    const store = await open(root, { verifyNativeEntry: async descriptor => { descriptor.bytes.fill(7); return await new Promise<boolean>(resolve => { finish = resolve; }); } });
    const descriptor = native(root); const expected = Uint8Array.from(descriptor.bytes);
    const pending = store.registerNativeEntry(descriptor);
    await Promise.resolve(); await Promise.resolve();
    descriptor.bytes.fill(0); (descriptor.location as { offset: number }).offset = 1000;
    (descriptor.flushReceipt as { file: string }).file = "changed";
    finish(true);
    expect(await pending).toEqual({ registered: true, existing: false });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 2 });
    expect(await store.readDurableEntry(request())).toEqual({ found: true, seq: 2, bytes: expected });
  });

  it("bounded native verification honors deadline/abort, never stages a late verifier, and does not restamp retries", async () => {
    const root = temp(); const signals: AbortSignal[] = []; const finish: Array<(value: boolean) => void> = [];
    const store = await open(root, { verifyNativeEntry: async (_descriptor, signal) => { signals.push(signal); return await new Promise<boolean>(resolve => { finish.push(resolve); }); } });
    expect(await store.registerNativeEntry(native(root, { deadlineMs: Date.now() + 20 }))).toEqual({ registered: false, reason: "deadline" });
    expect(signals[0]?.aborted).toBe(true); finish[0]!(true); await Promise.resolve();
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: false, reason: "not-found" });
    const abort = new AbortController(); const pending = store.registerNativeEntry(native(root, { signal: abort.signal }));
    await Promise.resolve(); await Promise.resolve(); abort.abort();
    expect(await pending).toEqual({ registered: false, reason: "aborted" }); finish[1]!(true);
    const expired = native(root, { deadlineMs: Date.now() - 1 });
    expect(await store.registerNativeEntry(expired)).toEqual({ registered: false, reason: "deadline" });
    expect(await store.registerNativeEntry(expired)).toEqual({ registered: false, reason: "deadline" });
    expect(signals).toHaveLength(2);
  });

  it("concurrent native registrations reconcile one immutable binding and never overwrite a winning commit", async () => {
    const root = temp(); const finish: Array<(value: boolean) => void> = [];
    const store = await open(root, { verifyNativeEntry: async () => await new Promise<boolean>(resolve => { finish.push(resolve); }) });
    const first = store.registerNativeEntry(native(root));
    const second = store.registerNativeEntry(native(root));
    await Promise.resolve(); await Promise.resolve(); finish[0]!(true); finish[1]!(true);
    expect(await Promise.all([first, second])).toEqual([{ registered: true, existing: false }, { registered: true, existing: true }]);
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 2 });
    const bytes = Buffer.from('{"id":"entry","different":true}\n');
    expect(await store.registerNativeEntry(native(root, { bytes, bytesSha256: sha(bytes), location: { ...native(root).location, length: bytes.length - 1 } }))).toEqual({ registered: false, reason: "conflict" });
    expect(records(root)).toHaveLength(2);
    expect(await store.readDurableEntry(request())).toEqual({ found: false, reason: "superseded" });
  });

  it("actual barrier refuses an aborted/expired reply while retaining late committed bytes and stable seq", async () => {
    const root = temp(); const store = await open(root);
    await store.stageControlSeen({ sessionId: "session", deliveryId: "delivery", bytes: Buffer.from("control") });
    const abort = new AbortController(); const real = fs.fsyncSync;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { real(fd); abort.abort(); });
    expect(await store.awaitDurableAppend({ ...request("control-seen"), signal: abort.signal })).toEqual({ durable: false, reason: "aborted" });
    sync.mockRestore();
    expect(await store.awaitDurableAppend(request("control-seen"))).toEqual({ durable: true, seq: 2 });
    const expired = { ...request("control-seen"), deadlineMs: 0 };
    expect(await store.awaitDurableAppend(expired)).toEqual({ durable: false, reason: "deadline" });
    expect(await store.awaitDurableAppend(expired)).toEqual({ durable: false, reason: "deadline" });
    expect(records(root)).toHaveLength(2);
    await store.close();
    expect(await store.readDurableEntry(request("control-seen"))).toEqual({ found: false, reason: "store-unavailable" });
    expect(await store.awaitDurableAppend(request("control-seen"))).toEqual({ durable: false, reason: "store-unavailable" });
  });

  it("native verifier rejection/unavailability cannot be mistaken for retained correlation", async () => {
    const root = temp(); const store = await open(root, { verifyNativeEntry: async () => { throw new Error("native feed unavailable"); } });
    expect(await store.registerNativeEntry(native(root))).toEqual({ registered: false, reason: "store-unavailable" });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: false, reason: "not-found" });
  });

  it("control staging is copied and immutable, separate from carriers and from commit", async () => {
    const root = temp(); const store = await open(root); const bytes = Buffer.from("store-owned serialized control\n"); const expected = Uint8Array.from(bytes);
    const staged = store.stageControlSeen({ sessionId: "session", deliveryId: "delivery", bytes }); bytes.fill(0);
    expect(await staged).toEqual({ staged: true, existing: false });
    expect(await store.readDurableEntry(request("control-seen"))).toEqual({ found: false, reason: "not-found" });
    expect(await store.awaitDurableAppend(request("control-seen"))).toEqual({ durable: true, seq: 2 });
    expect(await store.registerNativeEntry(native(root))).toEqual({ registered: true, existing: false });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 3 });
    expect(await store.readDurableEntry(request("control-seen"))).toEqual({ found: true, seq: 2, bytes: expected });
    await expect(store.stageControlSeen({ sessionId: "session", deliveryId: "delivery", bytes: Buffer.from("conflict") })).rejects.toMatchObject({ reason: "superseded" });
    expect(await store.awaitDurableAppend(request("control-seen"))).toEqual({ durable: false, reason: "superseded" });
    expect(await store.awaitDurableAppend(request())).toEqual({ durable: true, seq: 3 });
  });

  it("required missing/incomplete session recovery never supplies authoritative absence, and lost availability stays invalid", async () => {
    const root = temp(); await expect(open(path.join(root, "missing"), { required: true })).rejects.toMatchObject({ reason: "store-unavailable" });
    const journal = await openRecoveryJournal({ directory: root, storeId: "store" }); await journal.close();
    await expect(open(root, { required: true })).rejects.toMatchObject({ reason: "store-unavailable" });
    const store = await open(root);
    expect(await store.readDurableEntry(request())).toEqual({ found: false, reason: "not-found" });
    const name = records(root)[0]!; fs.renameSync(path.join(root, name), path.join(root, "preserved-binding"));
    expect(await store.readDurableEntry(request())).toEqual({ found: false, reason: "store-unavailable" });
    fs.renameSync(path.join(root, "preserved-binding"), path.join(root, name));
    expect(await store.readDurableEntry(request())).toEqual({ found: false, reason: "store-unavailable" });
    await store.close(); const recovered = await open(root, { required: true });
    expect(await recovered.readDurableEntry(request())).toEqual({ found: false, reason: "not-found" });
    expect(await recovered.readDurableEntry({ ...request(), sessionId: "different" })).toEqual({ found: false, reason: "superseded" });
    await recovered.close();
    await expect(open(root, { required: true, sessionFile: path.join(root, "different.jsonl") })).rejects.toMatchObject({ reason: "superseded" });
    await expect(open(root, { sessionFile: "relative.jsonl" })).rejects.toMatchObject({ reason: "store-unavailable" });
    await expect(openRecoveryDurabilityStore({ ...binding(root), verifyNativeEntry: undefined } as unknown as RecoveryDurabilityStoreOptions)).rejects.toMatchObject({ reason: "store-unavailable" });
  });

  it.each(["raw", "commit"] as const)("actual barrier process exits after %s fsync before replying; REQUIRED restart returns same seq/exact bytes", async mode => {
    const root = temp();
    const child = spawnSync("bun", [path.resolve("tests/b6-durability-crash.ts"), root, mode], { encoding: "utf8", timeout: 20_000, env: process.env });
    expect(child.error).toBeUndefined(); expect(child.status, child.stderr).toBe(73); expect(child.stdout).toBe("");
    const before = records(root).map(name => [name, fs.readFileSync(path.join(root, name))] as const);
    expect(before).toHaveLength(2);
    const store = await open(root, { storeId: "crash-store", required: true });
    expect(await store.readDurableEntry(request("control-seen"))).toEqual({ found: true, seq: 2, bytes: Uint8Array.from(Buffer.from("exact retained control\n")) });
    expect(await store.awaitDurableAppend(request("control-seen"))).toEqual({ durable: true, seq: 2 });
    expect(records(root)).toHaveLength(2);
    for (const [name, bytes] of before) expect(fs.readFileSync(path.join(root, name))).toEqual(bytes);
  }, 30_000);
});
