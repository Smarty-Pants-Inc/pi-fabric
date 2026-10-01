import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const NS = "residency/deliveries/";
const EMPTY = createHash("sha256").digest("base64");
const identity: MeshIdentity = { id: "writer", name: "writer", kind: "main" };
const roots: string[] = [];
const base = fileURLToPath(new URL("../.local/test-scratch/", import.meta.url));
const setup = async () => {
  fs.mkdirSync(base, { recursive: true });
  const root = fs.mkdtempSync(path.join(base, "namespace-watch-hint-"));
  roots.push(root);
  const writer = new MeshStore(root, 65536, 100);
  const reader = new MeshStore(root, 65536, 100, { readCacheMs: 2000 });
  await writer.put({ key: "topology/participants/a", value: 1, identity });
  const signalPath = path.join(root, "state.read-signal.json");
  const statePath = path.join(root, "state.json");
  const signalText = fs.readFileSync(signalPath, "utf8");
  return { root, writer, reader, signalPath, statePath, signalText };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("MeshStore namespaceWatchHint", () => {
  it("returns the empty digest without reading canonical payload or warming its cache", async () => {
    const { reader } = await setup();
    const reads = vi.spyOn(fs, "readFileSync");
    expect(reader.namespaceWatchHint(NS)).toBe(EMPTY);
    expect(reader.cachedStateStamp()).toBeUndefined();
    expect(reads).not.toHaveBeenCalled();
  });

  it("tracks namespace changes, not unrelated commits; deletion restores the empty digest", async () => {
    const { writer, reader } = await setup();
    const empty = reader.namespaceWatchHint(NS);
    await writer.put({ key: `${NS}root/a`, value: 1, identity });
    const first = reader.namespaceWatchHint(NS);
    expect(first).toMatch(/^[A-Za-z0-9+/]{43}=$/);
    expect(first).not.toBe(empty);
    await writer.put({ key: "topology/participants/a", value: 2, identity });
    expect(reader.namespaceWatchHint(NS)).toBe(first);
    await writer.put({ key: `${NS}root/a`, value: 1, identity });
    expect(reader.namespaceWatchHint(NS)).not.toBe(first);
    await writer.delete({ key: `${NS}root/a` });
    expect(reader.namespaceWatchHint(NS)).toBe(empty);
  });

  for (const namespace of ["", "residency/", "residency/deliveries", `${NS}root/`, `${NS}root`, "a//", "/a/", "a/b//", "a b/c/", "a/../x/"]) {
    it(`rejects non-exact namespace ${JSON.stringify(namespace)}`, async () => {
      const { reader } = await setup();
      expect(reader.namespaceWatchHint(namespace)).toBeUndefined();
    });
  }

  const corruptions: Array<[string, (signal: Record<string, unknown>) => unknown]> = [
    ["stale UUID", (s) => ({ ...s, generation: "00000000-0000-0000-0000-000000000000" })],
    ["wrong stamp", (s) => ({ ...s, stamp: "wrong" })],
    ["missing stamp", (s) => ({ generation: s.generation, namespaces: s.namespaces })],
    ["array namespaces", (s) => ({ ...s, namespaces: [] })],
    ["null namespaces", (s) => ({ ...s, namespaces: null })],
    ["numeric digest", (s) => ({ ...s, namespaces: { [NS]: 1 } })],
    ["short digest", (s) => ({ ...s, namespaces: { [NS]: "YQ==" } })],
    ["unpadded digest", (s) => ({ ...s, namespaces: { [NS]: EMPTY.slice(0, -1) } })],
    ["noncanonical pad bits", (s) => ({ ...s, namespaces: { [NS]: EMPTY.slice(0, -2) + "V=" } })],
    ["whitespace digest", (s) => ({ ...s, namespaces: { [NS]: EMPTY + "\n" } })],
    ["bad unrelated digest", (s) => ({ ...s, namespaces: { "other/namespace/": "bad" } })],
  ];
  for (const [name, corrupt] of corruptions) {
    it(`rejects ${name} even after same-UUID private memoization`, async () => {
      const { reader, writer, signalPath } = await setup();
      let now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now);
      reader.listAll(NS);
      await writer.put({ key: "topology/participants/a", value: 2, identity });
      now += 2001;
      reader.listAll(NS); // Populate the existing nonfresh signal memo for this UUID.
      expect(reader.namespaceWatchHint(NS)).toBe(EMPTY);
      const signalText = fs.readFileSync(signalPath, "utf8");
      fs.writeFileSync(signalPath, JSON.stringify(corrupt(JSON.parse(signalText))));
      expect(reader.namespaceWatchHint(NS)).toBeUndefined();
    });
  }

  for (const damage of ["missing", "truncated", "oversize", "directory"] as const) {
    it(`rejects ${damage} signal after successful observation`, async () => {
      const { reader, signalPath, signalText } = await setup();
      expect(reader.namespaceWatchHint(NS)).toBe(EMPTY);
      if (damage === "missing" || damage === "directory") fs.rmSync(signalPath);
      if (damage === "directory") fs.mkdirSync(signalPath);
      if (damage === "truncated") fs.writeFileSync(signalPath, signalText.slice(0, -1));
      if (damage === "oversize") fs.writeFileSync(signalPath, signalText + " ".repeat(128 * 1024));
      expect(reader.namespaceWatchHint(NS)).toBeUndefined();
    });
  }

  it("accepts bounded padding but never bypasses the bound through a same-UUID memo", async () => {
    const { reader, signalPath, signalText } = await setup();
    fs.writeFileSync(signalPath, signalText.padEnd(128 * 1024, " "));
    expect(reader.namespaceWatchHint(NS)).toBe(EMPTY);
    fs.appendFileSync(signalPath, " ");
    expect(reader.namespaceWatchHint(NS)).toBeUndefined();
  });

  it("rejects missing or markerless canonical state and stale copied-marker replacements", async () => {
    const { reader, statePath } = await setup();
    const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
    const replacement = `${statePath}.replacement`;
    fs.writeFileSync(replacement, JSON.stringify({ ...state, extra: true }));
    fs.renameSync(replacement, statePath);
    expect(reader.namespaceWatchHint(NS)).toBeUndefined();
    delete state.readGeneration;
    fs.writeFileSync(statePath, JSON.stringify(state));
    expect(reader.namespaceWatchHint(NS)).toBeUndefined();
    fs.rmSync(statePath);
    expect(reader.namespaceWatchHint(NS)).toBeUndefined();
  });

  it("does not refresh TTL or substitute a hint for fresh reads", async () => {
    const { writer, reader } = await setup();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const token = reader.stateToken();
    now += 1500;
    await writer.put({ key: `${NS}root/a`, value: 1, identity });
    expect(reader.namespaceWatchHint(NS)).not.toBe(EMPTY);
    expect(reader.stateToken()).toBe(token);
    now += 501;
    expect(reader.stateToken()).not.toBe(token);
    const reads = vi.spyOn(fs, "readFileSync");
    reader.namespaceWatchHint(NS);
    expect(reader.listAll(NS, { fresh: true })).toHaveLength(1);
    expect(reader.get(`${NS}root/a`, { fresh: true })?.value).toBe(1);
    const next = reader.stateToken();
    expect(reader.stateToken({ fresh: true })).not.toBe(next);
    expect(reads.mock.calls.filter(([file]) => String(file).endsWith("state.json"))).toHaveLength(3);
  });

  for (const fault of ["open EIO", "body short read", "body EIO", "replace after body"] as const) {
    it(`falls back on ${fault}`, async () => {
      const { reader, signalPath, statePath } = await setup();
      const open = fs.openSync;
      const read = fs.readSync;
      let signalDescriptor: number | undefined;
      let exercised = false;
      vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        if (String(file) === signalPath && fault === "open EIO") {
          exercised = true;
          throw new Error("injected EIO");
        }
        const descriptor = (open as (...args: unknown[]) => number)(file, ...args);
        if (String(file) === signalPath) signalDescriptor = descriptor;
        return descriptor;
      }) as typeof fs.openSync);
      vi.spyOn(fs, "readSync").mockImplementation(((descriptor: number, buffer: Buffer, offset: number, length: number, position: number) => {
        const count = read(descriptor, buffer, offset, length, position);
        if (descriptor === signalDescriptor && length > 64) {
          exercised = true;
          if (fault === "body EIO") throw new Error("injected EIO");
          if (fault === "body short read") return count - 1;
          if (fault === "replace after body") {
            fs.writeFileSync(`${statePath}.race`, fs.readFileSync(statePath));
            fs.renameSync(`${statePath}.race`, statePath);
          }
        }
        return count;
      }) as typeof fs.readSync);
      expect(reader.namespaceWatchHint(NS)).toBeUndefined();
      expect(exercised).toBe(true);
    });
  }

  it("rejects a canonical replacement during strict signal reading", async () => {
    const { reader, signalPath, statePath } = await setup();
    const open = fs.openSync;
    let raced = false;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (String(file) === signalPath && !raced) {
        raced = true;
        fs.writeFileSync(`${statePath}.race`, fs.readFileSync(statePath));
        fs.renameSync(`${statePath}.race`, statePath);
      }
      return (open as (...args: unknown[]) => number)(file, ...args);
    }) as typeof fs.openSync);
    expect(reader.namespaceWatchHint(NS)).toBeUndefined();
    expect(raced).toBe(true);
  });
});
