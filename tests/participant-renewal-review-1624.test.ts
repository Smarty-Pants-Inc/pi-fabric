// Keep: PR #1624's recorded lease-admission and presence-TTL failure paths.
// Reviewer probes for PR #1624: each compares base (fixed 5 s) against the jittered head.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUnavailableError, type FabricTransport } from "../src/transport/types.js";
import { hKey, kvWorld, pKey } from "./helpers/bridge-nats-kv.js";

const directories: ParticipantDirectory[] = [], stores: MeshStore[] = [], roots: string[] = [], views: KvParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(d => d.close()));
  await Promise.all(views.splice(0).map(v => v.close()));
  for (const s of stores.splice(0)) s.closeState();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const r of roots.splice(0)) fs.rmSync(r, { recursive: true, force: true });
});
function clock() {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const epoch = Date.now();
  let offset = 0;
  vi.spyOn(performance, "now").mockImplementation(() => Date.now() - epoch + offset);
  return { jumpWall(ms: number) { vi.setSystemTime(Date.now() + ms); offset -= ms; } };
}
function record(id: string): FabricParticipantRecord {
  return { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
    name: id, status: "idle", runner: "pi", transport: "host", capabilities: ["followUp"],
    controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
}
async function nats(id: string, random: () => number) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "rev1624-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000, { stateBackend: "file" });
  stores.push(mesh);
  const world = kvWorld(), listeners = new Set<() => void>();
  const base = world.transport("machine-a", id), leases = base.kv("leases"), presenceKv = base.kv("presence");
  const renewals: Array<{ at: number; ok: boolean }> = [];
  const ctl = { failNext: 0, delayNext: 0 };
  const transport: FabricTransport = { ...base,
    onVerifiedReconnect(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    kv(bucket) {
      if (bucket === "presence") return presenceKv;
      if (bucket !== "leases") return base.kv(bucket);
      return { ttlMs: leases.ttlMs, get: leases.get.bind(leases), delete: leases.delete.bind(leases), watch: leases.watch.bind(leases),
        async put(key, owner, value, revision, signal) {
          if (revision > 0) {
            if (ctl.failNext > 0) { ctl.failNext--; renewals.push({ at: Date.now(), ok: false }); throw new TransportUnavailableError("injected renewal failure"); }
            renewals.push({ at: Date.now(), ok: true });
            if (ctl.delayNext) { const d = ctl.delayNext; ctl.delayNext = 0; await new Promise<void>(r => setTimeout(r, d)); }
          }
          return leases.put(key, owner, value, revision, signal);
        } };
    },
  };
  const transportStateDir = path.join(root, "local");
  fs.mkdirSync(transportStateDir, { mode: 0o700 });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id,
    identity: { id, name: id, kind: "main" }, transport, transportStateDir, random, reapDeadHosts: false } as never);
  directories.push(directory);
  const rootRecord = record(id);
  directory.registerSource(() => [rootRecord]);
  await directory.start();
  return { directory, world, renewals, ctl, reconnect() { for (const l of listeners) l(); },
    leaseRow() { return world.leases.values.get(hKey("machine-a", id)); } };
}
async function watchAdmission(f: Awaited<ReturnType<typeof nats>>, ms: number, step = 250) {
  const lapses: number[] = [];
  const start = Date.now();
  for (let t = 0; t < ms; t += step) {
    await vi.advanceTimersByTimeAsync(step);
    if (!f.directory.canConsumeMesh()) lapses.push(Date.now() - start);
  }
  return lapses;
}

describe.skipIf(process.platform !== "linux")("review #1624 lease liveness", () => {
  it("R1: one fast-failed heartbeat renewal, then a 1.5 s retry, keeps admission", async () => {
    clock();
    const f = await nats("r1", () => 0.9999);
    const at = Date.now();
    // Let one renewal succeed, fail the next fast, then the retry takes 1.5 s.
    await vi.advanceTimersByTimeAsync(6_500);
    expect(f.renewals.length).toBe(1);
    f.ctl.failNext = 1;
    await vi.advanceTimersByTimeAsync(6_000);
    f.ctl.delayNext = 1_500;
    const lapses = await watchAdmission(f, 15_000);
    console.log("R1 renewals", JSON.stringify(f.renewals.map(r => ({ t: r.at - at, ok: r.ok }))), "lapses(ms after +12.5s)", JSON.stringify(lapses.slice(0, 4)), lapses.length);
    expect(lapses).toEqual([]);
  });

  it("R2: a failed (staggered) reconnect renewal still gets a retry before the 13 s cutoff", async () => {
    clock();
    const f = await nats("r2", () => 0.9999);
    const at = Date.now();
    await vi.advanceTimersByTimeAsync(9_000); // heartbeat renewed at ~6 s (head) or 5 s (base)
    f.ctl.failNext = 1; // the reconnect's renewal fails (JetStream not ready yet after reconnect)
    f.reconnect();
    const lapses = await watchAdmission(f, 16_000);
    console.log("R2 renewals", JSON.stringify(f.renewals.map(r => ({ t: r.at - at, ok: r.ok }))), "first lapse(ms after reconnect)", lapses[0], lapses.length);
    expect(lapses).toEqual([]);
  });

  it("R3: a reconnect while the presence watch is resyncing does not stop lease renewal", async () => {
    clock();
    const f = await nats("r3", () => 0.5);
    const at = Date.now();
    await vi.advanceTimersByTimeAsync(1_000);
    f.world.presence.gap(); // ordered-consumer restart: view not ready
    await vi.advanceTimersByTimeAsync(0); // the disconnect reset is delivered before the reconnect
    expect((f.directory.participantView as { ready?: boolean } | undefined)?.ready).toBe(false);
    f.reconnect();
    await vi.advanceTimersByTimeAsync(12_500);
    f.world.presence.resnapshot(); // watch ready again at 13.5 s
    await vi.advanceTimersByTimeAsync(500);
    console.log("R3 renewals", JSON.stringify(f.renewals.map(r => ({ t: r.at - at, ok: r.ok }))), "consume@14s", f.directory.canConsumeMesh());
    expect(f.renewals.filter(r => r.ok && r.at - at <= 13_000).length).toBeGreaterThan(0);
    expect(f.directory.canConsumeMesh()).toBe(true);
  });

  it("R4: after a host sleep (wall advances, monotonic does not) a reconnect renews promptly", async () => {
    const time = clock();
    const f = await nats("r4", () => 0.9999);
    await vi.advanceTimersByTimeAsync(1_000);
    time.jumpWall(120_000); // system sleep: broker TTL elapsed, local monotonic budget did not
    f.world.leases.expire(hKey("machine-a", "r4"));
    f.reconnect();
    await vi.advanceTimersByTimeAsync(1_000);
    console.log("R4 lease row present 1s after reconnect:", !!f.leaseRow(), "admitting:", f.directory.canConsumeMesh(), "renewals", f.renewals.length);
    expect(!!f.leaseRow()).toBe(true);
  });
});

describe("review #1624 presence TTL", () => {
  it("R5: two consecutive failed presence refreshes still leave a third attempt inside the 600 s TTL", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    vi.spyOn(performance, "now").mockImplementation(() => Date.now());
    const world = kvWorld(), abort = new AbortController();
    const view = new KvParticipantDirectory({ transport: world.transport("m", "session:p"),
      lease: { live: () => !abort.signal.aborted, signal: abort.signal }, identityId: "session:p",
      ownerIncarnation: "inc-1", rootId: "session:p", random: () => 0.9999, onError: () => undefined } as never);
    views.push(view);
    view.start();
    while (!view.ready) await view.whenChanged("ready", new AbortController().signal);
    const rec = { ...record("agent:x"), kind: "agent", rootId: "session:p", ownerHostId: "session:p", ownerIdentityId: "session:p", ownerIncarnation: "inc-1", transport: "process" } as FabricParticipantRecord;
    await view.publishBeforeLaunch(rec);
    const written = Date.now(), key = pKey("m", "agent:x"), rev = world.presence.values.get(key)!.revision;
    const put = world.presence.put.bind(world.presence);
    let fails = 2;
    const attempts: number[] = [];
    vi.spyOn(world.presence, "put").mockImplementation((...args) => {
      attempts.push(Date.now() - written);
      if (fails-- > 0) throw new TransportUnavailableError("injected presence failure");
      return put(...args);
    });
    await vi.advanceTimersByTimeAsync(700_000);
    const firstSuccess = attempts[2];
    console.log("R5 attempts(s)", JSON.stringify(attempts.map(a => a / 1000)));
    expect(world.presence.values.get(key)!.revision).toBeGreaterThan(rev);
    expect(firstSuccess).toBeLessThan(600_000);
  });
});
