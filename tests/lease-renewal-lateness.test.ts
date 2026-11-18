import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLease } from "../src/topology/host-leases.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// #20777: advisory stall measurements must not become lease authority or extra timers.
it("records monotonic heartbeat lateness and ages out the five-minute maximum", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-renewal-lateness-"));
  roots.push(root);
  let monotonic = 1_000;
  vi.spyOn(performance, "now").mockImplementation(() => monotonic);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const timers = vi.spyOn(globalThis, "setTimeout");
  const identity = { id: "session:late", name: "late", kind: "main" as const };
  const directory = new ParticipantDirectory(new MeshStore(root, 65536, 1000), {
    enabled: true, identity, hostId: identity.id, rootId: identity.id,
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false, random: () => 0.5,
  });
  directories.push(directory);
  directory.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "late", status: "running", runner: "pi",
    transport: "host", capabilities: ["followUp"], controlProtocol: "v1", startedAt: 1, updatedAt: 1 }]);
  await directory.start();
  const initial = readHostLease(root, identity.id)!;
  expect(initial).toMatchObject({ renewalLateMs: 0, renewalMaxLateMs: 0 });
  const heartbeatIndex = timers.mock.calls.findIndex(([, delay]) => delay === 60_000);
  expect(heartbeatIndex).toBeGreaterThanOrEqual(0);
  const heartbeat = timers.mock.calls[heartbeatIndex]![0] as () => void;
  clearTimeout(timers.mock.results[heartbeatIndex]!.value);
  monotonic += 60_000 + 120_001;
  heartbeat();
  await directory.refresh();
  const late = readHostLease(root, identity.id)!;
  expect(late).toMatchObject({ renewalLateMs: 120_001, renewalMaxLateMs: 120_001 });
  expect(late.expiresAt - late.updatedAt).toBe(120_000);
  expect(warn).toHaveBeenCalledExactlyOnceWith(
    "[fabric] host lease renewal session:late ran 120001 ms late (TTL 120000 ms)",
  );
  // Serialization prunes the rolling maximum even without another heartbeat callback.
  monotonic += 300_001;
  await directory.refresh();
  expect(readHostLease(root, identity.id)).toMatchObject({ renewalLateMs: 120_001, renewalMaxLateMs: 0 });
});
