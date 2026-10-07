import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-payload-test-")); roots.push(root);
  const file = path.join(root, "actors.json"), id = "a".repeat(32);
  return { root, file, id, log: path.join(root, id, "registry", "messages.jsonl"), store: new ActorRegistryStore(root) };
};
const messages = (count: number) => Array.from({ length: count }, (_, i) => ({ id: `m-${i}`, source: "direct", direction: "in", createdAt: i, text: "x".repeat(1_100) }));
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("compact actor registry payloads (#3752, #4383)", () => {
  it("keeps a bounded filter journal soft, then archives it before substantive history or overflow", async () => {
    const { store, id, log, root } = setup();
    const skips = messages(30).map(message => ({ ...message, reason: "filtered: noise" }));
    const sync = vi.spyOn(fs, "fsyncSync");
    await store.withLock(() => store.write([{ id, messages: [] }]));
    sync.mockClear();
    await store.withLock(() => store.write([{ id, messages: skips, registryMessageAppend: skips }]));
    expect(sync).not.toHaveBeenCalled();
    expect(store.records()[0]!.messageHistory).toBeUndefined();
    expect(new ActorRegistryStore(root).messages(store.records()[0]!)).toEqual(skips);
    const next = { ...messages(31)[30]!, reason: undefined };
    await store.withLock(() => store.write([{ id, messages: [...skips, next], registryMessageAppend: [next] }]));
    expect(sync).toHaveBeenCalled();
    expect(store.messages(store.records()[0]!)).toEqual([...skips, next]);
    const archived = fs.readFileSync(log, "utf8").split("\n").filter(Boolean).flatMap(line => JSON.parse(line).messages);
    expect(archived).toEqual([...skips, JSON.parse(JSON.stringify(next))]);

    const other = "b".repeat(32), burst = messages(150).map(message => ({ ...message, reason: "filtered: noise" }));
    await store.withLock(() => store.write([...store.records(), { id: other, messages: burst.slice(-100), registryMessageAppend: burst }]));
    expect(store.messages(store.records().find(row => row.id === other)!)).toEqual(burst.slice(-100));
    const archive = path.join(root, other, "registry", "messages.jsonl");
    expect(JSON.parse(fs.readFileSync(archive, "utf8").trim()).messages).toEqual(burst);
  });

  it("recovers accepted history after a legacy owned-row save drops all unknown fields", async () => {
    const { store, id, file, root, log } = setup();
    const actor = { id, instructions: "i".repeat(20_000), messages: messages(100) };
    await store.withLock(() => store.write([actor]));
    const archive = fs.readFileSync(log, "utf8");
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ ...actor, messages: [] }] }));
    const fresh = new ActorRegistryStore(root);
    expect(fresh.messageCount(fresh.records()[0]!)).toBe(100);
    expect(fresh.messages(fresh.records()[0]!)).toEqual(actor.messages);
    expect(fresh.instructions(fresh.records()[0]!)).toBe(actor.instructions);
    await fresh.withLock(() => fresh.write(fresh.records()));
    expect(fresh.messages(fresh.records()[0]!)).toEqual(actor.messages);
    expect(fs.readFileSync(log, "utf8")).toBe(archive);
  });

  it("merges legacy inline additions without duplication or history reset", async () => {
    const { store, id, file, root } = setup();
    await store.withLock(() => store.write([{ id, messages: messages(99) }]));
    const inline = [messages(99)[98], messages(100)[99]];
    fs.writeFileSync(file, JSON.stringify({ actors: [{ id, messages: inline }] }));
    const fresh = new ActorRegistryStore(root);
    expect(fresh.messages(fresh.records()[0]!)).toEqual(messages(100));
    await fresh.withLock(() => fresh.write(fresh.records()));
    expect(new ActorRegistryStore(root).messages(fresh.records()[0]!)).toEqual(messages(100));
    await fresh.withLock(() => fresh.write([{ id, messages: [], registryMessageReset: true }]));
    expect(fresh.messages(fresh.records()[0]!)).toEqual([]);
    fs.writeFileSync(file, JSON.stringify({ actors: [{ id, messages: [] }] }));
    expect(new ActorRegistryStore(root).messages(fresh.records()[0]!)).toEqual([]);
  });

  it("does not recover orphan appends after an old save strips the selecting reference", async () => {
    const { store, id, file, root } = setup();
    await store.withLock(() => store.write([{ id, messages: messages(2) }]));
    const rename = fs.renameSync.bind(fs);
    let fail = true;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === file && fail) { fail = false; throw new Error("registry refused"); }
      rename(from, to);
    });
    await expect(store.withLock(() => store.write([{ id, messages: messages(3) }]))).rejects.toThrow("registry refused");
    fs.writeFileSync(file, JSON.stringify({ actors: [{ id, messages: [] }] }));
    expect(new ActorRegistryStore(root).messages(store.records()[0]!)).toEqual(messages(2));
  });

  it("rolls back registry and all checkpoints on a post-rename checkpoint failure", async () => {
    const { store, id, file, root } = setup();
    const other = "b".repeat(32);
    await store.withLock(() => store.write([{ id, messages: messages(2) }, { id: other, messages: messages(2) }]));
    const before = fs.readFileSync(file, "utf8");
    const rename = fs.renameSync.bind(fs);
    let fail = true;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to) === path.join(root, other, "registry", "messages-head.json") && fail) {
        fail = false; throw new Error("checkpoint barrier refused");
      }
    });
    await expect(store.withLock(() => store.write([{ id, messages: messages(3) }, { id: other, messages: messages(3) }]))).rejects.toThrow("checkpoint barrier refused");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    fs.writeFileSync(file, JSON.stringify({ actors: [{ id, messages: [] }, { id: other, messages: [] }] }));
    const fresh = new ActorRegistryStore(root);
    for (const row of fresh.records()) expect(fresh.messages(row)).toEqual(messages(2));
  });

  it("restores an inline registry if the first checkpoint publication fails", async () => {
    const { store, id, file, root } = setup();
    const actor = { id, messages: messages(2) }, before = JSON.stringify({ actors: [actor] });
    fs.writeFileSync(file, before);
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to).endsWith("messages-head.json")) throw new Error("first checkpoint refused");
      rename(from, to);
    });
    await expect(store.withLock(() => store.write([actor]))).rejects.toThrow("first checkpoint refused");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    expect(fs.existsSync(path.join(root, id, "registry", "messages-head.json"))).toBe(false);
    expect(new ActorRegistryStore(root).messages(store.records()[0]!)).toEqual(actor.messages);
  });

  it("fails closed on a corrupt checkpoint instead of saving an empty legacy stub", async () => {
    const { store, id, file, root } = setup();
    await store.withLock(() => store.write([{ id, messages: messages(2) }]));
    const stub = JSON.stringify({ actors: [{ id, messages: [] }] });
    fs.writeFileSync(file, stub);
    fs.writeFileSync(path.join(root, id, "registry", "messages-head.json"), "{broken");
    const fresh = new ActorRegistryStore(root);
    expect(() => fresh.messages(fresh.records()[0]!)).toThrow();
    await expect(fresh.withLock(() => fresh.write(fresh.records()))).rejects.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe(stub);
  });

  it("hydrates prior PR instruction sidecars back inline on the next save", async () => {
    const { store, id, root } = setup();
    const text = "i".repeat(20_000);
    const { createHash } = await import("node:crypto");
    const digest = createHash("sha256").update(text).digest("hex");
    const directory = path.join(root, id, "registry");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, `instructions-${digest}.txt`), text);
    await store.withLock(() => store.write([{ id, instructions: "old stub", instructionsFile: digest, messages: [] }]));
    expect(store.records()[0]!.instructions).toBe(text);
    expect(store.records()[0]!.instructionsFile).toBeUndefined();
  });

  it("migrates valid actors beside invalid legacy rows without a null-record regression", async () => {
    const { store, file, id } = setup();
    const actor = { id, messages: messages(2) };
    fs.writeFileSync(file, JSON.stringify({ actors: [null, 1, [], {}, actor] }));
    await store.withLock(() => store.write(store.records()));
    expect(store.messages(store.records().find(record => record.id === id)!)).toEqual(actor.messages);
  });

  it("archives ALL embedded legacy messages while keeping long instructions inline", async () => {
    const { store, file, id, log } = setup();
    const legacy = { id, name: "actor", instructions: "i".repeat(20_000), messages: messages(150), extra: { future: true }, status: "idle" };
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [legacy] }));
    await store.withLock(() => store.write([{ ...legacy, messages: legacy.messages.slice(-100), status: "running" }]));
    const record = store.records()[0]!;
    expect(fs.statSync(file).size).toBeLessThan(21_000);
    expect(record.messages).toEqual([]);
    expect(record.instructions).toBe(legacy.instructions);
    expect(record.instructionsFile).toBeUndefined();
    expect(record.extra).toEqual({ future: true });
    expect(store.instructions(record)).toBe(legacy.instructions);
    expect(store.messages(record)).toEqual(legacy.messages.slice(-100));
    const transactions = fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
    expect(transactions).toHaveLength(1);
    expect(transactions[0].messages).toEqual(legacy.messages); // No truncation during migration.
  });

  it("does not read/rewrite foreign history on status saves; one new message appends only its delta", async () => {
    const { root, store, file, id, log } = setup();
    const ring = messages(100), actor = { id, instructions: "i".repeat(20_000), messages: ring, status: "idle" };
    await store.withLock(() => store.write([actor]));
    const bytes = fs.statSync(log).size;
    const checkpoint = path.join(path.dirname(log), "messages-head.json");
    const inode = fs.statSync(checkpoint).ino;
    const fresh = new ActorRegistryStore(root);
    const read = vi.spyOn(fs, "readFileSync");
    await fresh.withLock(() => fresh.write(fresh.records().map(record => ({ ...record, status: "running" }))));
    expect(read.mock.calls.some(([file]) => String(file).endsWith("messages.jsonl") || String(file).endsWith(".txt"))).toBe(false);
    expect(fs.statSync(log).size).toBe(bytes);
    read.mockRestore();
    const next = messages(101).slice(-100);
    await store.withLock(() => store.write([{ ...actor, messages: next, status: "waiting" }]));
    expect(fs.statSync(log).size - bytes).toBeLessThan(1_500);
    expect(fs.statSync(checkpoint).ino).not.toBe(inode);
    expect(fs.readdirSync(path.dirname(log)).some(name => name.startsWith("instructions-"))).toBe(false);
    expect(new ActorRegistryStore(root).messages(store.records()[0]!)).toEqual(next);
    expect(fs.statSync(file).size).toBeLessThan(21_000);
  });

  it("archives an entire unsaved burst even when only the last 100 remain in memory", async () => {
    const { store, id, log } = setup();
    const burst = messages(250);
    await store.withLock(() => store.write([{ id, messages: burst.slice(-100), registryMessageAppend: burst }]));
    const record = store.records()[0]!;
    expect(record.registryMessageAppend).toBeUndefined();
    expect(store.messages(record)).toEqual(burst.slice(-100));
    expect(JSON.parse(fs.readFileSync(log, "utf8").trim()).messages).toEqual(burst);
  });

  it("archives a reset without resurrecting old messages and keeps old accepted heads readable", async () => {
    const { store, id, root, log } = setup();
    await store.withLock(() => store.write([{ id, messages: messages(2) }]));
    const accepted = store.records()[0]!;
    const oldBytes = fs.statSync(log).size;
    await store.withLock(() => store.write([{ id, messages: [] }]));
    expect(store.messages(store.records()[0]!)).toEqual([]);
    expect(new ActorRegistryStore(root).messages(accepted)).toEqual(messages(2));
    expect(fs.statSync(log).size).toBeGreaterThan(oldBytes);
    await store.withLock(() => store.write([{ id, messages: [messages(3)[2]] }]));
    expect(store.messages(store.records()[0]!)).toEqual([messages(3)[2]]);
  });

  it("a failed registry commit leaves only an orphan append, not a future accepted predecessor", async () => {
    const { store, id, file, log, root } = setup();
    const actor = { id, messages: messages(2) };
    await store.withLock(() => store.write([actor]));
    const before = fs.readFileSync(file, "utf8"), bytes = fs.statSync(log).size;
    const rename = fs.renameSync.bind(fs);
    let fail = true;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === file && fail) { fail = false; throw new Error("publish refused"); }
      rename(from, to);
    });
    await expect(store.withLock(() => store.write([{ id, messages: [...actor.messages, { id: "orphan" }] }]))).rejects.toThrow("publish refused");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    expect(fs.statSync(log).size).toBeGreaterThan(bytes);
    const next = [...actor.messages, { id: "accepted" }];
    await store.withLock(() => store.write([{ id, messages: next }]));
    expect(new ActorRegistryStore(root).messages(store.records()[0]!)).toEqual(next);
    expect(fs.readFileSync(log, "utf8")).toContain("orphan"); // Archived, never selected.
  });

  it("retries a failed payload barrier without publishing a guessed history or losing inline data", async () => {
    const { store, id, file, log } = setup();
    const actor = { id, messages: messages(3), status: "idle" };
    const inline = JSON.stringify({ actors: [actor] }); fs.writeFileSync(file, inline);
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), fds = new Map<number, string>();
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); fds.set(fd, String(file)); return fd; });
    let failed = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (!failed && fds.get(fd) === log) { failed = true; throw new Error("payload barrier unavailable"); }
      sync(fd);
    });
    await expect(store.withLock(() => store.write([actor]))).rejects.toThrow("payload barrier unavailable");
    expect(fs.readFileSync(file, "utf8")).toBe(inline);
    await store.withLock(() => store.write([actor]));
    expect(store.messages(store.records()[0]!)).toEqual(actor.messages);
  });

  it("reports truncated/corrupt references rather than converting them to empty histories", async () => {
    const { store, id, file, log, root } = setup();
    await store.withLock(() => store.write([{ id, messages: messages(2), instructions: "i".repeat(2_000) }]));
    const record = store.records()[0]!;
    // Match an old owned-row save: both selecting fields are gone, while the
    // checkpoint remains the only source of the accepted history.
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ ...record, messageHistory: undefined, instructionsFile: undefined, messages: [] }] }));
    fs.truncateSync(log, 1);
    const fresh = new ActorRegistryStore(root);
    expect(() => fresh.messages(fresh.records()[0]!)).toThrow("Truncated actor message history");
    expect(() => fresh.instructions({ ...record, instructionsFile: "../../escape" })).toThrow("Invalid actor instructions reference");
    const before = fs.readFileSync(file, "utf8");
    await expect(fresh.restoreInlineForDowngrade()).rejects.toThrow("Truncated actor message history");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it("restores checkpoint history together with legacy inline additions without duplication", async () => {
    const { store, id, file, log } = setup();
    await store.withLock(() => store.write([{ id, instructions: "legacy", messages: messages(110) }]));
    const archive = fs.readFileSync(log, "utf8");
    const inline = [messages(110)[109], messages(111)[110]];
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ id, instructions: "legacy", messages: inline }] }));
    await store.restoreInlineForDowngrade();
    expect(JSON.parse(fs.readFileSync(file, "utf8")).actors[0].messages).toEqual(messages(111).slice(-100));
    expect(fs.readFileSync(log, "utf8")).toBe(archive);
  });

  it("rejects a corrupt checkpoint without changing an old-owned registry row", async () => {
    const { store, id, file, root } = setup();
    await store.withLock(() => store.write([{ id, messages: messages(2) }]));
    const row = store.records()[0]!;
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ ...row, messageHistory: undefined, instructionsFile: undefined, messages: [] }] }));
    fs.writeFileSync(path.join(root, id, "registry", "messages-head.json"), "{corrupt");
    const before = fs.readFileSync(file, "utf8");
    await expect(store.restoreInlineForDowngrade()).rejects.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it("keeps old-loader-compatible stubs and explicitly restores inline records for downgrade", async () => {
    const { store, id, file, log } = setup();
    const actor = { id, name: "compatible", instructions: "i".repeat(20_000), createdAt: 1, messages: messages(110) };
    await store.withLock(() => store.write([actor]));
    const stub = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(stub.format).toBe(1);
    expect(stub.actors[0].instructions).toBe(actor.instructions);
    expect(stub.actors[0].instructionsFile).toBeUndefined();
    expect(Array.isArray(stub.actors[0].messages)).toBe(true);
    expect(stub.actors[0].messages).toEqual([]);
    const archive = fs.readFileSync(log, "utf8");
    // Simulate the actual old manager save, which drops both new selectors.
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ ...stub.actors[0], messageHistory: undefined, instructionsFile: undefined, messages: [] }] }));
    const oldOwnedReader = new ActorRegistryStore(path.dirname(file));
    expect(oldOwnedReader.messages(oldOwnedReader.records()[0]!)).toEqual(actor.messages.slice(-100));
    expect(await store.restoreInlineForDowngrade()).toBe(1);
    const restored = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(restored.actors[0].messages).toEqual(actor.messages.slice(-100));
    expect(store.records()[0]).toEqual({ ...actor, messages: actor.messages.slice(-100) });
    expect(fs.readFileSync(log, "utf8")).toBe(archive);
    await store.withLock(() => store.write(store.records()));
    expect(store.instructions(store.records()[0]!)).toBe(actor.instructions);
    expect(store.messages(store.records()[0]!)).toEqual(actor.messages.slice(-100));
  });
});
