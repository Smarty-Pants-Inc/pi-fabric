import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-callers-")); roots.push(directory); return directory; };
const identity = { id: "session:audit", name: "main", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });

// Observe real filesystem calls, including the namespace barrier after publication.
const observe = () => {
  const events: string[] = [];
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), asyncSync = fs.fsync.bind(fs), rename = fs.renameSync.bind(fs), link = fs.linkSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { events.push(`sync:${descriptors.get(fd)}`); sync(fd); });
  vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => { events.push(`sync:${descriptors.get(fd)}`); asyncSync(fd, callback); });
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { events.push(`rename:${to}`); rename(from, to); });
  vi.spyOn(fs, "linkSync").mockImplementation((from, to) => { events.push(`link:${to}`); link(from, to); });
  return events;
};
const expectPublished = (events: string[], file: string, kind = "rename") => {
  const publication = events.indexOf(`${kind}:${file}`);
  expect(publication).toBeGreaterThan(0);
  expect(events.slice(0, publication).some(event => event.startsWith(`sync:${file}.`))).toBe(true);
  if (process.platform !== "win32") expect(events.slice(publication + 1)).toContain(`sync:${path.dirname(file)}`);
};

describe("#2479 M durable callers", () => {
  it("syncs authoritative mesh revision state, not reconstructible reservations/read signals", async () => {
    const directory = root(), write = vi.spyOn(atomic, "writeFileAtomic"), events = observe();
    const mesh = new MeshStore(directory, 64 * 1024, 100);
    await mesh.put({ key: "resource/grant", value: { accepted: true }, identity });
    await mesh.publish({ topic: "audit", from: identity, text: "one" });
    const calls = write.mock.calls;
    expect(calls.find(([file]) => file === path.join(directory, "state.json"))?.[2]?.durable).not.toBe(true);
    expectPublished(events, path.join(directory, "state.durable.json"));
    const completion = JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8"));
    expect(completion.generation).toBe(mesh.get("resource/grant", { fresh: true })!.version);
    expect(calls.find(([file]) => file === path.join(directory, "sequence"))?.[2]?.durable).not.toBe(true);
    for (const [file, , options] of calls.filter(([file]) => file.includes("signal"))) expect(options?.durable, file).not.toBe(true);
  });

  it("syncs a compacted event log before publishing its durable generation", async () => {
    const directory = root(), events = observe();
    const mesh = new MeshStore(directory, 1024, 100, { maxEventLogBytes: 1100, retainedEventLogBytes: 300 });
    for (let n = 0; n < 6; n++) await mesh.publish({ topic: "audit", from: identity, text: "x".repeat(400) });
    const log = path.join(directory, "events.jsonl"), generation = path.join(directory, "generation");
    expectPublished(events, log);
    expectPublished(events, generation);
    const logRename = events.indexOf(`rename:${log}`), generationRename = events.indexOf(`rename:${generation}`);
    expect(logRename).toBeLessThan(generationRename);
    if (process.platform !== "win32") expect(events.slice(logRename + 1, generationRename)).toContain(`sync:${directory}`);
  });

});
