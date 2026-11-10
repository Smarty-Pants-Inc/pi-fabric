import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshSnapshotRetriesExhaustedError } from "../src/core/atomic-write.js";
import { BridgeOwnershipError, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const cleanups: Array<() => void> = [];
afterEach(() => { vi.restoreAllMocks(); for (const close of cleanups.splice(0).reverse()) close(); });
const key = (id: string) => `topology/participants/${createHash("sha256").update(id).digest("hex")}`;
const fixture = async (backend: "file" | "sqlite") => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-participant-lock-"));
  cleanups.push(() => fs.rmSync(base, { recursive: true, force: true }));
  const root = path.join(base, "mesh");
  const store = new MeshStore(root, 65_536, 1_000, { stateBackend: backend, lockProtocol: 1 });
  cleanups.push(() => store.closeState());
  const now = Date.now();
  const sender: MeshIdentity = { id: "session:far", name: "far", kind: "main", sessionId: "far" };
  const side = new StoreBridgeSide(store, "far");
  await side.mirror({ hosts: [{ record: { format: 1, id: sender.id, rootId: sender.id, identity: sender,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 }, expiresAt: now + 120_000 }], participants: [] }, now);
  const event: BridgePublish = { topic: "fleet.work.participant-lock", kind: "ask", from: sender,
    to: "session:local", text: "preserve sender", data: { bridge: { from: "far", id: "inbound:1" } } };
  return { base, root, store, side, sender, event };
};
const participant = (id: string, owner: MeshIdentity, rootId = id) => {
  const now = Date.now();
  return { key: key(id), version: 1, updatedAt: now, updatedBy: owner,
    value: { format: 1, id, kind: "root", rootId, ownerHostId: owner.id, ownerIdentityId: owner.id,
      name: id, label: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
      startedAt: now, updatedAt: now, controlProtocol: "v1" } };
};

// Real held publication -> EventLog admission/fence, both state backends. A synchronous slow
// directory listing blocks this JS thread, so a different process must prove canonical-lock progress.
// The child publishes a real event, not just an unlocked marker or a mocked custody callback.
describe.each(["file", "sqlite"] as const)("held participant snapshot (%s)", backend => {
  it.each(["single", "batch", "relay"] as const)("lets another publisher progress during slow listing: %s", async mode => {
    const f = await fixture(backend);
    // Ensure the scan has a real native participant directory, independent of mirrored SQLite rows.
    const native: MeshIdentity = { id: "session:unrelated", name: "unrelated", kind: "main" };
    writeParticipantFile(f.root, participant(native.id, native));
    const bundle = path.join(f.base, "publisher.mjs");
    buildSync({ stdin: { contents: 'export { MeshStore } from "./src/mesh/store.ts";', resolveDir: process.cwd() },
      bundle: true, platform: "node", format: "esm", outfile: bundle });
    const original = fs.readdirSync;
    let probed = false, scans = 0;
    vi.spyOn(fs, "readdirSync").mockImplementation(((directory: fs.PathLike, options?: unknown) => {
      if (String(directory) === path.join(f.root, "participants")) {
        scans++;
        expect(fs.existsSync(path.join(f.root, ".lock")), "participant listing must not own mesh .lock").toBe(false);
        if (!probed) {
          probed = true;
          const child = spawnSync(process.execPath, ["--input-type=module", "-e", `
            const { MeshStore } = await import(${JSON.stringify(pathToFileURL(bundle).href)});
            const store = new MeshStore(${JSON.stringify(f.root)}, 65536, 1000,
              { stateBackend: ${JSON.stringify(backend)}, lockProtocol: 1, lockTimeoutMs: 250 });
            try {
              await store.publish({ topic: "fleet.work.other-lock-user", kind: "ask",
                from: { id: "session:concurrent", name: "concurrent", kind: "main" }, to: "session:local", text: "progress" });
              process.stdout.write("PUBLISHED");
            } finally { store.closeState(); }
          `], { encoding: "utf8", timeout: 10_000, env: { ...process.env, PI_FABRIC_MESH_CHECKPOINT_LOG: "off" } });
          expect(child.error).toBeUndefined();
          expect(child.status, child.stderr).toBe(0);
          expect(child.stdout).toContain("PUBLISHED");
        }
      }
      return Reflect.apply(original, fs, options === undefined ? [directory] : [directory, options]);
    }) as typeof fs.readdirSync);
    const published = mode === "single" ? await f.side.publish(f.event, [f.sender.id])
      : mode === "batch" ? (await f.side.publishBatch([{ event: f.event, held: [f.sender.id] }]))[0]!
      : await f.side.recordRelay({ ...f.event, topic: "fabric.control.command" }, [f.sender.id]);
    expect(probed).toBe(true);
    expect(scans).toBeGreaterThan(0);
    const appended = f.store.read({ after: published.sequence - 1, limit: 1 })[0]!;
    expect(appended.from).toEqual({ ...f.sender, verified: "bridge" });
    if (mode !== "relay") expect(appended).toMatchObject({ text: "preserve sender", data: { bridge: { from: "far", id: "inbound:1" } } });
    else expect(appended.data).toBeUndefined(); // relay receipts never copy text/data
    expect(f.store.read({ topic: "fleet.work.other-lock-user", after: 0 })).toHaveLength(1);
  });

  it("refuses a legacy native alias claimed after preparation, without losing sender attribution", async () => {
    const f = await fixture(backend);
    const native: MeshIdentity = { id: "session:native", name: "native", kind: "main" };
    const now = Date.now();
    await f.store.put({ key: `topology/hosts/${createHash("sha256").update(native.id).digest("hex")}`, identity: native,
      value: { format: 1, id: native.id, rootId: native.id, identity: native, startedAt: now, updatedAt: now, expiresAt: now + 120_000 } });
    const original = f.store.publish.bind(f.store);
    let changed = false;
    vi.spyOn(f.store, "publish").mockImplementation(async input => {
      if (!changed) {
        changed = true;
        // Legacy state writers do not take participant-file custody. Fresh state ownership must
        // still reserve an alias whose rootId is the held sender, not just its primary file key.
        const claim = participant("session:native-alias", native, f.sender.id);
        await f.store.put({ key: claim.key, identity: native, value: claim.value });
      }
      return original(input);
    });
    await expect(f.side.publish(f.event, [f.sender.id])).rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(changed).toBe(true);
    expect(f.store.read({ topic: f.event.topic, after: 0 })).toEqual([]);
    expect(f.store.get(key("session:native-alias"), { fresh: true })?.updatedBy).toEqual(native);
  });

  it.skipIf(process.platform === "win32")("invalidates a raw file snapshot rather than rescanning under .lock", async () => {
    const f = await fixture(backend);
    const original = f.store.withStateWriteFence.bind(f.store);
    let changed = false;
    vi.spyOn(f.store, "withStateWriteFence").mockImplementation(operation => original(() => {
      if (!changed) {
        changed = true;
        writeParticipantFile(f.root, participant(f.sender.id, f.sender));
      }
      return operation();
    }));
    await expect(f.side.publish(f.event, [f.sender.id])).rejects.toBeInstanceOf(MeshSnapshotRetriesExhaustedError);
    expect(changed).toBe(true);
    expect(f.store.read({ topic: f.event.topic, after: 0 })).toEqual([]);
    await expect(f.side.publish(f.event, [f.sender.id])).rejects.toBeInstanceOf(BridgeOwnershipError);
  });
});
