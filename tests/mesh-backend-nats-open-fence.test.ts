import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { encodeMeshStateNatsMarker, readMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { importMeshStateToNats, rollbackMeshStateFromNats } from "../src/mesh/backend-migration-nats.js";
import { meshSnapshotDigest } from "../src/mesh/backend-migration.js";
import { closeNatsStateBackend, openNatsStateBackend, preparedNatsStateBackend, type JetStreamPort, type NatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";

// rf2 P1: a NATS open must not register beside a rollback that holds the fence (custody), and a second open of a root
// must never close another opener's backend. Ordered by explicit barriers on the fake's port, never by timing.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) { closeNatsStateBackend(root); fs.rmSync(root, { recursive: true, force: true }); }
});

type Outcome<T> = { status: "fulfilled"; value: T } | { status: "rejected"; error: unknown };
const settle = <T>(promise: Promise<T>): Promise<Outcome<T>> =>
  promise.then(value => ({ status: "fulfilled" as const, value }), (error: unknown) => ({ status: "rejected" as const, error }));
const barrier = () => {
  let signal!: () => void; let release!: () => void;
  const entered = new Promise<void>(resolve => { signal = resolve; });
  const released = new Promise<void>(resolve => { release = resolve; });
  return { entered, released, signal, release };
};
const fileDigest = (root: string): string => meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false));

/** A file root with real state, imported onto a fresh fake server: a complete NATS marker. */
const markedRoot = async (label: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-nats-open-fence-${label}-`));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  try {
    await store.put({ key: "seed/a", value: { n: 1 }, identity });
    await store.put({ key: "seed/b", value: "two", identity });
  } finally { store.closeState(); }
  const digest = fileDigest(root);
  const server = fakeJetStream();
  const imported = await importMeshStateToNats(root, { port: server.port() });
  expect(imported.digest).toBe(digest);
  expect(readMeshStateNatsMarker(root)).toMatchObject({ natsStream: imported.stream, epoch: 1 });
  return { root, server, digest, imported };
};

describe("NATS open vs the migration fence and other openers (rf2 P1)", () => {
  it("an open while a rollback holds the fence refuses busy before registering; the rollback then completes", async () => {
    const { root, server, digest, imported } = await markedRoot("rollback");
    const marker = readMeshStateNatsMarker(root);
    const retire = barrier();
    const raw = server.port();
    // The rollback parks just before it publishes `retire`: it holds custody.lock and .lock and has its expected head.
    const port: JetStreamPort = { ...raw, publish: async (subject, data, options) => {
      if ((JSON.parse(Buffer.from(data).toString("utf8")) as { type?: string }).type === "retire") { retire.signal(); await retire.released; }
      return raw.publish(subject, data, options);
    } };
    const rollback = settle(rollbackMeshStateFromNats(root, { port }));
    let opened: NatsStateBackend | undefined;
    try {
      const reached = await Promise.race([retire.entered.then(() => true), rollback.then(() => false)]);
      expect(reached).toBe(true);
      const open = await settle(openNatsStateBackend({ root, port: server.port() }));
      const registered = preparedNatsStateBackend(root);
      const markerWhileHeld = readMeshStateNatsMarker(root);
      // d5: the open succeeded beside the rollback; a write through it moves the stream under the rollback's CAS.
      if (open.status === "fulfilled") {
        opened = open.value;
        await settle(open.value.put({ key: "racing/open", value: true, identity }));
      }
      retire.release();
      const back = await rollback;
      expect({ open: open.status, registered: registered !== undefined, rollback: back.status })
        .toEqual({ open: "rejected", registered: false, rollback: "fulfilled" });
      const error = (open as { error: unknown }).error;
      expect(error).toBeInstanceOf(MeshLockTimeoutError);
      expect(String((error as Error).message)).not.toMatch(/projector did not reach/);
      expect(markerWhileHeld).toEqual(marker);
      const result = (back as { value: Awaited<ReturnType<typeof rollbackMeshStateFromNats>> }).value;
      expect(result).toMatchObject({ stream: imported.stream, epoch: 2, digest, converged: false });
      expect(fileDigest(root)).toBe(digest);
      expect(readMeshStateNatsMarker(root)).toBeUndefined();
    } finally {
      retire.release();
      await rollback;
      opened?.close();
      closeNatsStateBackend(root);
    }
  }, 30_000);

  it("a second open never closes the first opener's backend; closing the first leaves the registered one", async () => {
    const { root, server } = await markedRoot("owners");
    const gated = () => {
      const gate = barrier();
      const raw = server.port();
      let first = true;
      const port: JetStreamPort = { ...raw, consume: async (...args) => {
        if (first) { first = false; gate.signal(); await gate.released; }
        return raw.consume(...args);
      } };
      return { gate, port };
    };
    const one = gated();
    const two = gated();
    const p1 = settle(openNatsStateBackend({ root, port: one.port }));
    const p2 = settle(openNatsStateBackend({ root, port: two.port }));
    const handles: NatsStateBackend[] = [];
    try {
      // Both openers have taken their initial root decision and wait at their projector start.
      expect(await Promise.race([one.gate.entered.then(() => true), p1.then(() => false)])).toBe(true);
      expect(await Promise.race([two.gate.entered.then(() => true), p2.then(() => false)])).toBe(true);
      one.gate.release();
      const o1 = await p1;
      expect(o1.status).toBe("fulfilled");
      const first = (o1 as { value: NatsStateBackend }).value;
      handles.push(first);
      two.gate.release();
      const o2 = await p2;
      expect(o2.status).toBe("fulfilled");
      const second = (o2 as { value: NatsStateBackend }).value;
      handles.push(second);
      expect(second).not.toBe(first);
      // The first opener still owns a working backend: the second open never closed it.
      expect(await settle(first.put({ key: "owner/first", value: 1, identity }))).toMatchObject({ status: "fulfilled" });
      expect(first.get("owner/first")?.value).toBe(1);
      expect(await settle(second.put({ key: "owner/second", value: 2, identity }))).toMatchObject({ status: "fulfilled" });
      expect(second.get("owner/second")?.value).toBe(2);
      // The last prepared open is the registered one (the same handle when openers share one).
      expect(preparedNatsStateBackend(root)).toBe(second);
      if (first !== second) {
        // The first owner's cleanup never closes or unregisters the second's backend.
        first.close();
        expect(preparedNatsStateBackend(root)).toBe(second);
        expect(await settle(second.put({ key: "owner/after", value: 3, identity }))).toMatchObject({ status: "fulfilled" });
        expect(second.get("owner/after")?.value).toBe(3);
        first.close(); // repeated stale-owner cleanup still cannot remove its successor
        expect(preparedNatsStateBackend(root)).toBe(second);
      }
      second.close();
      expect(preparedNatsStateBackend(root)).toBeUndefined();
    } finally {
      one.gate.release(); two.gate.release();
      for (const outcome of [await p1, await p2]) if (outcome.status === "fulfilled" && !handles.includes(outcome.value)) handles.push(outcome.value);
      for (const handle of handles) handle.close();
      closeNatsStateBackend(root);
    }
  }, 30_000);

  it("an importing stream keeps its inactive handle contract: no reads or writes", async () => {
    const { root, server } = await markedRoot("importing");
    server.append(Buffer.from(JSON.stringify({ format: 1, type: "import-begin", epoch: 2 })));
    const backend = await openNatsStateBackend({ root, port: server.port() });
    try {
      expect(backend.diagnostics().nats?.status).toBe("importing");
      expect(preparedNatsStateBackend(root)).toBe(backend);
      expect(() => backend.get("seed/a")).toThrow(/importing/);
      await expect(backend.put({ key: "blocked", value: true, identity })).rejects.toThrow(/importing/);
    } finally { backend.close(); }
  });

  it.each(["undelivered retire", "importing with undelivered retire", "same-stream marker replacement", "same-stream timestamp replacement", "head-read failure"] as const)(
    "admission refuses %s under custody and closes only its candidate", async change => {
      const { root, server, imported } = await markedRoot("admission");
      if (change === "importing with undelivered retire") server.append(Buffer.from(JSON.stringify({ format: 1, type: "import-begin", epoch: 2 })));
      const previous = await openNatsStateBackend({ root, port: server.port() });
      const raw = server.port();
      let changed = false;
      let candidateClosed = false;
      const port: JetStreamPort = { ...raw,
        lastSequence: async stream => {
          if (!changed && fs.existsSync(path.join(root, "custody.lock"))) {
            changed = true;
            if (change === "head-read failure") throw new Error("admission metadata unavailable");
            if (change === "same-stream marker replacement" || change === "same-stream timestamp replacement") {
              const marker = readMeshStateNatsMarker(root)!;
              fs.writeFileSync(path.join(root, "state.json"), change === "same-stream marker replacement"
                ? encodeMeshStateNatsMarker(imported.stream, 2, marker.at)
                : encodeMeshStateNatsMarker(imported.stream, marker.epoch, new Date(Date.parse(marker.at) + 1).toISOString()));
            } else {
              raw.pause();
              server.append(Buffer.from(JSON.stringify({ format: 1, type: "retire", epoch: change === "importing with undelivered retire" ? 3 : 2 })), `${imported.stream}.state`);
            }
          }
          return raw.lastSequence(stream);
        },
        close: async () => { candidateClosed = true; await raw.close(); },
      };
      try {
        await expect(openNatsStateBackend({ root, port })).rejects.toThrow(
          change.includes("undelivered retire") ? /admission projector/ : change === "head-read failure" ? /metadata unavailable/ : /changed while/);
        expect(changed).toBe(true);
        expect(candidateClosed).toBe(true);
        expect(preparedNatsStateBackend(root)).toBe(previous);
        expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);
      } finally { raw.resume(); previous.close(); }
    });

  it.each([false, true])("a pending admission head read excludes another open and releases custody (fail=%s)", async fail => {
    const { root, server } = await markedRoot("head-hold");
    const previous = await openNatsStateBackend({ root, port: server.port() });
    const head = barrier();
    const raw = server.port();
    let held = false;
    const port: JetStreamPort = { ...raw, lastSequence: async stream => {
      if (!held && fs.existsSync(path.join(root, "custody.lock"))) {
        held = true; head.signal(); await head.released;
        if (fail) throw new Error("held metadata read failed");
      }
      return raw.lastSequence(stream);
    } };
    const pending = settle(openNatsStateBackend({ root, port }));
    let candidate: NatsStateBackend | undefined;
    try {
      expect(await Promise.race([head.entered.then(() => true), pending.then(() => false)])).toBe(true);
      const competitor = await settle(openNatsStateBackend({ root, port: server.port() }));
      expect(competitor.status).toBe("rejected");
      expect((competitor as { error: unknown }).error).toBeInstanceOf(MeshLockTimeoutError);
      expect(preparedNatsStateBackend(root)).toBe(previous);
      expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(true);
      head.release();
      const outcome = await pending;
      expect(outcome.status).toBe(fail ? "rejected" : "fulfilled");
      if (outcome.status === "fulfilled") candidate = outcome.value;
      else expect(String(outcome.error)).toMatch(/held metadata read failed/);
      expect(preparedNatsStateBackend(root)).toBe(fail ? previous : candidate);
      expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);
      // A later NEW call succeeds; neither the busy competitor nor a failed candidate damaged the earlier owner.
      const later = await openNatsStateBackend({ root, port: server.port() });
      try { expect(await settle(previous.put({ key: "owner/still-live", value: true, identity }))).toMatchObject({ status: "fulfilled" }); }
      finally { later.close(); }
    } finally {
      head.release();
      const outcome = await pending;
      if (outcome.status === "fulfilled") outcome.value.close();
      previous.close();
    }
  });
});
