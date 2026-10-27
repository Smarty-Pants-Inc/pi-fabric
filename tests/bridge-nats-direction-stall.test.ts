import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";

// MeshBridge deliberately captures its health timers at module evaluation. Install fake timers
// BEFORE importing it, not after: otherwise the independent watchdog still waits real minutes.
vi.hoisted(() => { vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] }); });

import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { parseNatsLinkConfig } from "../src/mesh/bridge-nats-config.js";
import { startNatsLink, type NatsLinkHandle } from "../src/mesh/bridge-nats-link.js";
import { MeshStore } from "../src/mesh/store.js";
import { BRIDGE_MACHINE, FakePort, LINK, POD_MACHINE, settle } from "./helpers/bridge-nats-l2-fakes.js";

const roots: string[] = [];
const handles: NatsLinkHandle[] = [];
afterEach(async () => {
  for (const handle of handles.splice(0)) { await handle.stop(); await handle.closed; }
  vi.restoreAllMocks();
  vi.clearAllTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
afterAll(() => { vi.useRealTimers(); });

// Keep: the real config -> startNatsLink -> MeshBridge watchdog boundary. A source read that
// cannot advance is a failure path that takes minutes to exercise on a live link.
// Stated skip: nats-link refuses Windows (POSIX file modes and lock semantics).
describe.skipIf(process.platform === "win32")("nats-link direction stall configuration", () => {
  it.each([undefined, 300_000])("closes only after the parsed deadline (%s)", async directionStallAfterMs => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-stall-"));
    roots.push(root);
    const mesh = path.join(root, "mesh");
    const seed = new MeshStore(mesh, 256 * 1024, 500);
    await seed.prepareLock();
    seed.closeState();
    const config = parseNatsLinkConfig({ version: 1, name: "hub-a", link: LINK,
      file: { mesh, lockProtocol: 1 }, stateDir: path.join(root, "state"),
      ...(directionStallAfterMs === undefined ? {} : { directionStallAfterMs }),
      nats: { servers: ["tls://broker.example.test:4222"], machine: BRIDGE_MACHINE,
        credentials: path.join(root, "user.creds"), caFile: path.join(root, "ca.pem"),
        custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: [BRIDGE_MACHINE, POD_MACHINE],
        operatorMachine: "fabric-ops" } });
    const port = new FakePort();
    const logs: string[] = [];
    let mono = 0;
    vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
    // The file head advances, but its tail is wedged. The real independent watchdog must
    // still see the backlog, keep the opposite (idle) direction healthy, and close at the deadline.
    const tail = vi.spyOn(StoreBridgeSide.prototype, "tail").mockImplementation(async after => ({ events: [], through: after }));
    let store!: MeshStore;
    const handle = await startNatsLink(config, { signal: new AbortController().signal, log: line => logs.push(line),
      factories: { openPort: async (_config, options) => { store = options.fileStore; return port; },
        seatPins: () => ({ seats: new Map(), bound: new Map() }) } });
    handles.push(handle);
    await settle();
    await store.publish({ topic: "ops.owner", kind: "test-backlog", from: { id: "bridge:test", name: "test", kind: "main" } });
    await vi.advanceTimersByTimeAsync(1_000);
    await settle();
    expect(tail).toHaveBeenCalled();
    expect(store.latestSequence()).toBeGreaterThan(0);

    mono = 15_000; // First probe observes pending work; its independent deadline starts here.
    await vi.advanceTimersByTimeAsync(15_000);
    await settle();
    expect(port.closed).toBe(false);
    mono = 210_000; // 195 s stalled: beyond the unchanged 180 s default.
    await vi.advanceTimersByTimeAsync(15_000);
    await settle();
    expect(port.closed).toBe(directionStallAfterMs === undefined);
    if (directionStallAfterMs !== undefined) {
      expect(logs.some(line => line.includes("stall v1"))).toBe(false);
      mono = 315_001; // Just beyond the configured 300 s pending deadline.
      await vi.advanceTimersByTimeAsync(15_000);
      await settle();
      expect(port.closed).toBe(true);
    }
    expect(logs.some(line => /stall v1 .*direction=toRemote/.test(line))).toBe(true);
  });
});
