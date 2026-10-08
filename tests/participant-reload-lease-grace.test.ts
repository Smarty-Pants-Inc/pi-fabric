import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MAIN_RELOAD_LEASE_MS, ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, readHostLeases } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#6729 (release gate 1): through an autoReload pin change, a Main's lease must not
// lapse while a slow reload (60-100 s on a CPU-starved host) tears down the old release and
// imports the new one. Nothing renews in between: the old heartbeat stops at teardown and the
// new one starts only at the next session_start. The explicit reload grace written at
// quiesce("reload") must outlast a slow reload, yet stay bounded so a Main that dies
// mid-reload still lapses.

const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
});

const sessionId = "6729aaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const identity: MeshIdentity = { id: `session:${sessionId}`, sessionId, name: "Main", kind: "main" };
const reader: MeshIdentity = { id: "session:reader", sessionId: "reader", name: "reader", kind: "main" };
const record = (): FabricParticipantRecord => ({
  format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  kind: "root", name: "Main", status: "idle", capabilities: ["steer", "followUp", "fabric"],
  runner: "pi", transport: "host", controlProtocol: "v1", sessionId, cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
});

const fixture = async (filesOnly: boolean) => {
  // A private temp mesh root, never the live fleet root.
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-reload-grace-"));
  roots.push(base);
  const meshRoot = path.join(base, "mesh");
  if (filesOnly) {
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({ key: LIVENESS_POLICY_KEY,
      value: { version: 1, participants: "files", hostLeases: "files" }, identity });
  }
  // The injected clock: every writer and reader in this process reads Date.now().
  const realNow = Date.now.bind(Date);
  let skew = 0;
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
  /** One Fabric release's directory for the Main; each has its own store, like a fresh runtime. */
  const release = () => {
    const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
    });
    directory.registerSource(() => [record()]);
    cleanup.push(() => directory.close());
    return directory;
  };
  // The directory reader of another process: its own store, fresh reads from disk, never started.
  const observer = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled: true, hostId: reader.id, rootId: reader.id, identity: reader, reapDeadHosts: false,
  });
  const seen = () => observer.get(identity.id, Date.now(), { fresh: true });
  const listed = () => observer.list({ kinds: ["root"], fresh: true }).map((participant) => participant.id);
  return { meshRoot, release, seen, listed, advance: (ms: number) => { skew += ms; } };
};

describe("Main lease through a slow autoReload (smarty-dev#6729)", () => {
  it.each([
    [60_000, false], [90_000, false], [150_000, false],
    [60_000, true], [90_000, true], [150_000, true],
  ])("a %i ms reload never shows the Main as gone (files-only policy: %s)", async (delayMs, filesOnly) => {
    const f = await fixture(filesOnly);
    const old = f.release();
    await old.start();
    expect(f.seen()).toMatchObject({ status: "idle", stale: false });

    // session_shutdown(reason: "reload"): quiesce("reload") publishes the grace, then teardown
    // closes the directory and stops its heartbeat. Nothing renews until the new release starts.
    await old.quiesce("reload");
    await old.close();
    for (let elapsed = 0; elapsed <= delayMs; elapsed += 5_000) {
      expect(f.seen(), `lapsed ${elapsed} ms into the reload`).toMatchObject({ status: "reloading", stale: false });
      expect(f.listed(), `missing from the directory ${elapsed} ms into the reload`).toContain(identity.id);
      if (elapsed < delayMs) f.advance(5_000);
    }

    // The new release's first heartbeat replaces the grace with an ordinary short lease.
    const fresh = f.release();
    await fresh.start();
    const seen = f.seen();
    expect(seen).toMatchObject({ status: "idle", stale: false });
    expect(seen?.reloadUntil).toBeUndefined();
    const lease = readHostLeases(f.meshRoot).get(identity.id)!;
    expect(lease.expiresAt - Date.now()).toBeLessThanOrEqual(30_000);
    expect(lease.session?.expiresAt ?? 0).toBeLessThanOrEqual(Date.now() + 30_000);
  }, 60_000);

  it.each([false, true])("a Main that dies mid-reload lapses once the bounded grace ends (files-only policy: %s)", async (filesOnly) => {
    // Long enough for a minute-plus reload on a loaded host, short enough to bound a dead Main.
    expect(MAIN_RELOAD_LEASE_MS).toBeGreaterThanOrEqual(120_000);
    expect(MAIN_RELOAD_LEASE_MS).toBeLessThanOrEqual(180_000);
    const f = await fixture(filesOnly);
    const old = f.release();
    await old.start();
    const quiescedAt = Date.now();
    await old.quiesce("reload");
    const reloadUntil = f.seen()!.reloadUntil!;
    expect(reloadUntil - quiescedAt).toBeGreaterThanOrEqual(MAIN_RELOAD_LEASE_MS);
    expect(reloadUntil - Date.now()).toBeLessThanOrEqual(MAIN_RELOAD_LEASE_MS);
    await old.close();
    // The process died: no new release ever starts.
    f.advance(reloadUntil - Date.now() - 1_000);
    expect(f.seen()).toMatchObject({ status: "reloading", stale: false });
    f.advance(reloadUntil - Date.now() + 1);
    expect(f.seen()).toBeUndefined();
    expect(f.listed()).not.toContain(identity.id);
    const lease = readHostLeases(f.meshRoot).get(identity.id);
    expect(lease === undefined || lease.expiresAt < Date.now()).toBe(true);
  }, 60_000);
});
