// Decision B guard (fake JetStream, recorded calls): a root with no file state bootstraps its fixed-root stream; a
// populated, staged, partial/malformed-marker or unreadable root refuses with its bytes unchanged; a race that makes file
// state appear during the async open refuses with nothing registered; an own-root marker opens without create.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { encodeMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { holdMeshFence } from "../src/mesh/fence-lock.js";
import { natsRollbackSelection } from "../src/mesh/mesh-backend-cli.js";
import {
  closeNatsStateBackend, decideNatsRootOpen, natsStreamName, openNatsStateBackend, preparedNatsStateBackend,
} from "../src/mesh/state-backend-nats.js";
import { fakeJetStream, type FakeJetStreamServer, type FakePort } from "./helpers/nats-jetstream-fake.js";

const roots: string[] = [];
const freshRoot = (create = true): string => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "nats-bootstrap-guard-"));
  roots.push(parent);
  const root = path.join(parent, "root");
  if (create) fs.mkdirSync(root, { mode: 0o700 });
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const parent of roots.splice(0)) {
    closeNatsStateBackend(path.join(parent, "root"));
    fs.rmSync(parent, { recursive: true, force: true });
  }
});

const openWith = (server: FakeJetStreamServer, root: string): { port: FakePort; opened: Promise<unknown> } => {
  const port = server.port();
  return { port, opened: openNatsStateBackend({ root, port, lockTimeoutMs: 2_000 }) };
};
const snapshot = (root: string): Record<string, string> =>
  Object.fromEntries(fs.readdirSync(root).filter(name => name.startsWith("state.")).sort().map(name => [name, fs.readFileSync(path.join(root, name), "base64")]));
const STATE = JSON.stringify({ format: 1, entries: { a: { value: 1 } } });
const SWITCH = "0123456789abcdef0123456789abcdef";

describe("decision B: fixed-root bootstrap only with no file state", () => {
  it("bootstraps an empty existing root's fixed-root stream and registers it", async () => {
    const root = freshRoot();
    const server = fakeJetStream();
    server.streamExists = false;
    const { port, opened } = openWith(server, root);
    const backend = await opened;
    expect(decideNatsRootOpen(root)).toMatchObject({ kind: "bootstrap", stream: natsStreamName(root) });
    expect(port.calls).toContain("ensureStream");
    expect(server.streams.has(natsStreamName(root))).toBe(true);
    expect(preparedNatsStateBackend(root)).toBe(backend);
  });

  it("an absent root bootstraps without creating the root", async () => {
    const root = freshRoot(false);
    const server = fakeJetStream();
    server.streamExists = false;
    await openWith(server, root).opened;
    expect(fs.lstatSync(root, { throwIfNoEntry: false })).toBeUndefined();
  });

  const refusals: [string, (root: string) => void][] = [
    ["a populated file root", root => fs.writeFileSync(path.join(root, "state.json"), STATE)],
    ["a malformed marker", root => fs.writeFileSync(path.join(root, "state.json"),
      JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 1, at: new Date().toISOString(), natsStream: "bad stream!" }))],
    ["a truncated (partial JSON) marker", root => {
      const marker = encodeMeshStateNatsMarker(natsStreamName(root), 1);
      fs.writeFileSync(path.join(root, "state.json"), marker.slice(0, marker.length - 7));
    }],
    ["a staged intent", root => fs.writeFileSync(path.join(root, "state.json.nats-intent"),
      JSON.stringify({ switchId: SWITCH, stream: `${natsStreamName(root)}_${SWITCH}` }))],
    ["a kept copy without a marker", root => fs.writeFileSync(path.join(root, "state.json.nats-cutover-1"), STATE)],
    ["another root's marker", root => fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateNatsMarker("FABRIC_MESH_OTHER", 1))],
  ];
  for (const [name, seed] of refusals) {
    it(`refuses ${name}: no create, no registration, bytes unchanged`, async () => {
      const root = freshRoot();
      seed(root);
      const before = snapshot(root);
      const server = fakeJetStream();
      server.streamExists = false;
      const { port, opened } = openWith(server, root);
      await expect(opened).rejects.toThrow();
      expect(port.calls).not.toContain("ensureStream");
      expect(port.calls).not.toContain("createStream");
      expect(server.streams.size).toBe(0);
      expect(preparedNatsStateBackend(root)).toBeUndefined();
      expect(snapshot(root)).toEqual(before);
    });
  }

  it("an unreadable root (EACCES) refuses with its own error, never bootstraps", async () => {
    const root = freshRoot();
    const readdir = fs.readdirSync;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (path.resolve(String(target)) === path.resolve(root)) throw Object.assign(new Error(`EACCES: permission denied, scandir '${root}'`), { code: "EACCES" });
      return (readdir as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    const server = fakeJetStream();
    server.streamExists = false;
    const { port, opened } = openWith(server, root);
    await expect(opened).rejects.toMatchObject({ code: "EACCES" });
    expect(port.calls).not.toContain("ensureStream");
    expect(preparedNatsStateBackend(root)).toBeUndefined();
  });

  it("refuses when file state appears during the async open: nothing registered, the file kept", async () => {
    const root = freshRoot();
    const server = fakeJetStream();
    server.streamExists = false;
    server.beforeConsume = () => { fs.writeFileSync(path.join(root, "state.json"), STATE); };
    const { opened } = openWith(server, root);
    await expect(opened).rejects.toThrow(/changed while its NATS stream was opened|no NATS moved marker/);
    expect(preparedNatsStateBackend(root)).toBeUndefined();
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(STATE);
  });

  it("an own-root marker opens its stream without create", async () => {
    const root = freshRoot();
    const stream = `${natsStreamName(root)}_${SWITCH}`;
    fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateNatsMarker(stream, 1), { mode: 0o600 });
    const server = fakeJetStream();
    const { port, opened } = openWith(server, root);
    const backend = await opened;
    expect(decideNatsRootOpen(root)).toMatchObject({ kind: "marker", stream });
    expect(port.calls).not.toContain("createStream");
    expect(preparedNatsStateBackend(root)).toBe(backend);
  });
});

// Record these root-write receivers and the exact .lock predicate used by the real multiprocess child.
const MUTATORS = ["mkdirSync", "mkdtempSync", "renameSync", "writeFileSync", "rmSync", "rmdirSync", "unlinkSync", "linkSync", "symlinkSync"] as const;
interface RootWrites { writes: string[]; lockAttempts: number }
const recordRootWrites = (root: string): RootWrites => {
  const record: RootWrites = { writes: [], lockAttempts: 0 };
  const isLock = (target: unknown) => path.basename(String(target)).startsWith(".lock") && path.dirname(path.resolve(String(target))) === path.resolve(root);
  const inRoot = (target: unknown): boolean => {
    if (typeof target !== "string" && !Buffer.isBuffer(target) && !(target instanceof URL)) return false;
    const resolved = path.resolve(String(target));
    return resolved === root || resolved.startsWith(`${root}${path.sep}`);
  };
  for (const name of MUTATORS) {
    const original = fs[name] as unknown as (...args: unknown[]) => unknown;
    vi.spyOn(fs, name).mockImplementation(((...args: unknown[]) => {
      if ((["mkdirSync", "mkdtempSync", "renameSync"] as string[]).includes(name) && (isLock(args[0]) || (name === "renameSync" && isLock(args[1])))) record.lockAttempts += 1;
      const hit = [args[0], name === "renameSync" || name === "linkSync" || name === "symlinkSync" ? args[1] : undefined].find(inRoot);
      if (hit !== undefined) record.writes.push(`${name} ${path.relative(root, path.resolve(String(hit))) || "."}`);
      return original.apply(fs, args);
    }) as never);
  }
  return record;
};
const lockWrites = (writes: string[]): string[] => writes.filter(write => /(^|\s)(\.lock|custody\.lock)/.test(write) || write.endsWith(" ."));
const identity = { id: "tester", name: "tester", kind: "agent" as const };
const markRoot = (root: string, stream = `${natsStreamName(root)}_${SWITCH}`): string => {
  fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateNatsMarker(stream, 1), { mode: 0o600 });
  return stream;
};

describe("steady-state marked-root admission takes custody but no mesh .lock (smarty-dev#6477 L2a)", () => {
  it("the recorder sees the fence's custody.lock and .lock receivers (control)", async () => {
    const root = freshRoot();
    markRoot(root);
    const recorded = recordRootWrites(root);
    await holdMeshFence(root, { lockTimeoutMs: 2_000 }, () => undefined);
    expect(recorded.lockAttempts).toBeGreaterThan(0);
    expect(recorded.writes.some(write => /custody\.lock/.test(write))).toBe(true);
    expect(recorded.writes.some(write => / \.lock/.test(write))).toBe(true);
  });

  it("a marked root opens under custody, then serves state without filesystem writes or mesh .lock", async () => {
    const root = freshRoot();
    const stream = markRoot(root);
    const before = snapshot(root);
    const recorded = recordRootWrites(root);
    const server = fakeJetStream();
    const { port, opened } = openWith(server, root);
    const backend = await opened as Awaited<ReturnType<typeof openNatsStateBackend>>;
    expect(recorded.lockAttempts).toBe(0);
    expect(recorded.writes.some(write => /custody\.lock/.test(write))).toBe(true);
    expect(snapshot(root)).toEqual(before);
    const admissionWrites = [...recorded.writes];
    expect(port.calls).not.toContain("createStream");
    expect(preparedNatsStateBackend(root)).toBe(backend);
    expect(backend.diagnostics().nats?.status).toBe("ready");
    await backend.put({ key: "steady/a", value: 1, identity });
    expect(backend.get("steady/a")?.value).toBe(1);
    expect(recorded).toEqual({ writes: admissionWrites, lockAttempts: 0 });
    expect(fs.readdirSync(root).filter(name => name.includes("lock"))).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    expect(decideNatsRootOpen(root)).toMatchObject({ kind: "marker", stream });
  });

  const changes: [string, (root: string) => void][] = [
    ["another own-root stream's marker", root => { markRoot(root, natsStreamName(root)); }],
    ["live file state", root => fs.writeFileSync(path.join(root, "state.json"), STATE)],
    ["a staged intent beside the marker", root => fs.writeFileSync(path.join(root, "state.json.nats-intent"),
      JSON.stringify({ switchId: SWITCH, stream: `${natsStreamName(root)}_${SWITCH}` }))],
  ];
  for (const [name, change] of changes) {
    it(`refuses a marker replaced by ${name} during the async open: nothing registered, no lock taken`, async () => {
      const root = freshRoot();
      markRoot(root);
      const recorded = recordRootWrites(root);
      const server = fakeJetStream();
      server.beforeConsume = () => { change(root); };
      const { opened } = openWith(server, root);
      await expect(opened).rejects.toThrow(/changed while its NATS stream was opened|no NATS moved marker|unfinished switch/);
      expect(preparedNatsStateBackend(root)).toBeUndefined();
      expect(recorded.lockAttempts).toBe(0);
      expect(lockWrites(recorded.writes)).toEqual([]);
    });
  }

  it("refuses a marked root whose stream is retired: nothing registered, no lock taken", async () => {
    const root = freshRoot();
    markRoot(root);
    const server = fakeJetStream();
    server.append(Buffer.from(JSON.stringify({ format: 1, type: "retire", epoch: 2 })));
    const recorded = recordRootWrites(root);
    const { port, opened } = openWith(server, root);
    await expect(opened).rejects.toThrow(/retired/);
    expect(port.calls).not.toContain("createStream");
    expect(preparedNatsStateBackend(root)).toBeUndefined();
    expect(recorded).toEqual({ writes: [], lockAttempts: 0 });
  });
});

describe("CLI rollback --stream selection (switch id only)", () => {
  it("maps a per-switch name to its id and the fixed-root name to no id", () => {
    const root = freshRoot();
    fs.writeFileSync(path.join(root, "state.json"), STATE); // a finished rollback: file state, no marker
    expect(natsRollbackSelection({ root, stream: `${natsStreamName(root)}_${SWITCH}` })).toEqual({ switchId: SWITCH });
    expect(natsRollbackSelection({ root, stream: natsStreamName(root) })).toEqual({});
    expect(natsRollbackSelection({ root })).toEqual({});
  });

  it("refuses a name that disagrees with the marker, and another root's name", () => {
    const root = freshRoot();
    fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateNatsMarker(`${natsStreamName(root)}_${SWITCH}`, 1));
    expect(() => natsRollbackSelection({ root, stream: natsStreamName(root) })).toThrow(/disagrees with the moved marker/);
    expect(() => natsRollbackSelection({ root, stream: "FABRIC_MESH_OTHER" })).toThrow(/not a NATS stream/);
    expect(natsRollbackSelection({ root, stream: `${natsStreamName(root)}_${SWITCH}` })).toEqual({ switchId: SWITCH });
  });
});
