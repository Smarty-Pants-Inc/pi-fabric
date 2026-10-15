// Decision B guard (fake JetStream, recorded calls): a root with no file state bootstraps its fixed-root stream; a
// populated, staged, partial/malformed-marker or unreadable root refuses with its bytes unchanged; a race that makes file
// state appear during the async open refuses with nothing registered; an own-root marker opens without create.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { encodeMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
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
