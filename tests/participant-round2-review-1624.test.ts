// Keep: independent round-2 PR #1624 probes preserve the recorded lease/reconnect,
// host-sleep and recovery-spread failure paths; original reviewer bytes follow unchanged.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUnavailableError, type FabricTransport } from "../src/transport/types.js";
import { hKey, kvWorld } from "./helpers/bridge-nats-kv.js";

const directories: ParticipantDirectory[] = [], stores: MeshStore[] = [], roots: string[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(d => d.close()));
  for (const s of stores.splice(0)) s.closeState();
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const r of roots.splice(0)) fs.rmSync(r, { recursive: true, force: true });
});
function clock() {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const epoch = Date.now(); let offset = 0;
  vi.spyOn(performance, "now").mockImplementation(() => Date.now() - epoch + offset);
  return { jumpWall(ms: number) { vi.setSystemTime(Date.now() + ms); offset -= ms; } };
}
function record(id: string): FabricParticipantRecord {
  return { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id, name: id, status: "idle", runner: "pi",
    transport: "host", capabilities: ["followUp"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
}
async function nats(id: string, random: () => number) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "probe1624-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000, { stateBackend: "file" }); stores.push(mesh);
  const world = kvWorld(), listeners = new Set<() => void>();
  const base = world.transport("machine-a", id), leases = base.kv("leases"), presenceKv = base.kv("presence");
  const ops: Array<{ at: number; op: string; ok: boolean }> = [];
  const plan: Array<{ delay: number; fail?: boolean }> = [];
  const ctl = { fastFail: false };
  const transport: FabricTransport = { ...base,
    onVerifiedReconnect(l) { listeners.add(l); return () => { listeners.delete(l); }; },
    kv(bucket) {
      if (bucket === "presence") return presenceKv;
      if (bucket !== "leases") return base.kv(bucket);
      return { ttlMs: leases.ttlMs, delete: leases.delete.bind(leases), watch: leases.watch.bind(leases),
        async get(key, signal) {
          if (ctl.fastFail) { ops.push({ at: Date.now(), op: "get", ok: false }); throw new TransportUnavailableError("503 no responders"); }
          return leases.get(key, signal);
        },
        async put(key, owner, value, revision, signal) {
          if (ctl.fastFail) { ops.push({ at: Date.now(), op: "put", ok: false }); throw new TransportUnavailableError("503 no responders"); }
          if (revision > 0) {
            const step = plan.shift();
            if (step?.delay) await new Promise<void>(r => setTimeout(r, step.delay));
            ops.push({ at: Date.now(), op: "put", ok: !step?.fail });
            if (step?.fail) throw new TransportUnavailableError("injected renewal failure");
          }
          return leases.put(key, owner, value, revision, signal);
        } };
    },
  };
  const dir = path.join(root, "local"); fs.mkdirSync(dir, { mode: 0o700 });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity: { id, name: id, kind: "main" },
    transport, transportStateDir: dir, random, reapDeadHosts: false } as never);
  directories.push(directory);
  const rr = record(id); directory.registerSource(() => [rr]);
  await directory.start();
  return { directory, world, ops, plan, ctl, reconnect() { for (const l of listeners) l(); },
    leaseRow() { return world.leases.values.get(hKey("machine-a", id)); } };
}

describe.skipIf(process.platform !== "linux")("independent probes #1624", () => {
  it("PA: host sleep, then a real disconnect (view reset) and reconnect: broker-expired lease is discovered promptly", async () => {
    const time = clock();
    const f = await nats("pa", () => 0.9999);
    await vi.advanceTimersByTimeAsync(1_000);
    f.world.presence.gap();
    await vi.advanceTimersByTimeAsync(0);
    time.jumpWall(120_000);
    f.world.leases.expire(hKey("machine-a", "pa"));
    f.reconnect();
    const trace: string[] = [];
    let firstRow = -1;
    for (let t = 250; t <= 8_000; t += 250) {
      await vi.advanceTimersByTimeAsync(250);
      if (t === 500) f.world.presence.resnapshot();
      const row = !!f.leaseRow();
      if (row && firstRow < 0) firstRow = t;
      trace.push(t + ":" + (row ? "row" : "none") + "/" + (f.directory.canConsumeMesh() ? "admit" : "closed"));
    }
    console.log("PA trace", trace.join(" "));
    console.log("PA first lease row (ms after reconnect)", firstRow);
    expect(firstRow).toBeGreaterThanOrEqual(0);
    expect(firstRow).toBeLessThanOrEqual(1_000);
  });

  it("PB: a renewal at max jitter that times out (5 s), then a 2.5 s retry", async () => {
    clock();
    const f = await nats("pb", () => 0.9999);
    const at = Date.now();
    // first heartbeat renewal succeeds; the second hangs 5 s and fails; the retry takes 2.5 s
    f.plan.push({ delay: 0 }, { delay: 5_000, fail: true }, { delay: 2_500 });
    const lapses: number[] = [];
    for (let t = 0; t < 30_000; t += 250) {
      await vi.advanceTimersByTimeAsync(250);
      if (!f.directory.canConsumeMesh()) lapses.push(Date.now() - at);
    }
    console.log("PB ops", JSON.stringify(f.ops.map(o => ({ t: o.at - at, ok: o.ok }))), "lapses", lapses.length, lapses[0]);
    expect(lapses).toEqual([]);
  });

  it("PC: request rate during a 30 s fast-fail (JetStream not ready) window", async () => {
    clock();
    const f = await nats("pc", () => 0.5);
    await vi.advanceTimersByTimeAsync(7_000);
    const start = Date.now();
    f.ctl.fastFail = true;
    await vi.advanceTimersByTimeAsync(30_000);
    f.ctl.fastFail = false;
    const during = f.ops.filter(o => o.at >= start && !o.ok);
    const gaps = during.slice(1).map((o, i) => o.at - during[i]!.at);
    console.log("PC failed ops in 30s", during.length, "min gap", Math.min(...gaps), "gaps", JSON.stringify(gaps.slice(0, 40)));
    await vi.advanceTimersByTimeAsync(10_000);
    console.log("PC after restore canConsume", f.directory.canConsumeMesh());
    expect(during.length).toBeLessThan(1000);
  });
});

describe.skipIf(process.platform !== "linux")("independent probes #1624 (2)", () => {
  for (const rnd of [0.9999, 0.5]) it("PB2: renewal fails after 4.9 s, retry takes 2.5 s (random " + rnd + ")", async () => {
    clock();
    const f = await nats("pb2", () => rnd);
    const at = Date.now();
    f.plan.push({ delay: 0 }, { delay: 4_900, fail: true }, { delay: 2_500 });
    const lapses: number[] = [];
    for (let t = 0; t < 30_000; t += 100) {
      await vi.advanceTimersByTimeAsync(100);
      if (!f.directory.canConsumeMesh()) lapses.push(Date.now() - at);
    }
    console.log("PB2", rnd, "ops", JSON.stringify(f.ops.map(o => ({ t: o.at - at, ok: o.ok }))), "lapses", lapses.length, lapses[0]);
    expect(lapses).toEqual([]);
  });

  it("PD: renewal spread across 12 runtimes before vs after a shared 20 s fast-fail window", async () => {
    clock();
    const fleet = [] as Array<Awaited<ReturnType<typeof nats>>>;
    for (let i = 0; i < 12; i++) {
      let s = 1234567 + i * 7919;
      const rnd = () => { s = (s * 1103515245 + 12345) % 2147483648; return s / 2147483648; };
      fleet.push(await nats("pd" + i, rnd));
    }
    await vi.advanceTimersByTimeAsync(40_000);
    const mark = Date.now();
    await vi.advanceTimersByTimeAsync(7_000);
    const firstAfter = (f: typeof fleet[number], t: number) => f.ops.find(o => o.ok && o.at >= t)?.at ?? NaN;
    const before = fleet.map(f => firstAfter(f, mark) - mark);
    for (const f of fleet) f.ctl.fastFail = true;
    await vi.advanceTimersByTimeAsync(20_000);
    const restore = Date.now();
    for (const f of fleet) f.ctl.fastFail = false;
    await vi.advanceTimersByTimeAsync(7_000);
    const after = fleet.map(f => firstAfter(f, restore) - restore);
    const spread = (xs: number[]) => Math.max(...xs) - Math.min(...xs);
    console.log("PD first renewal after an arbitrary instant (ms):", JSON.stringify(before), "spread", spread(before));
    console.log("PD first renewal after the shared outage ends (ms):", JSON.stringify(after), "spread", spread(after));
    const failed = fleet.reduce((n, f) => n + f.ops.filter(o => !o.ok).length, 0);
    console.log("PD failed requests in 20 s across 12 runtimes:", failed);
    expect(spread(after)).toBeGreaterThan(0);
  });
});
