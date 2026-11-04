import childProcess from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { FABRIC_MESH_OUTAGE_ALARM_TOPIC, MeshStore } from "../src/mesh/store.js";
import { MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// Keep: smarty-dev#18131's real participant refusal/reap -> background queue -> owner-routed alarm path.
// Only the refusal publication fails; the alarm traverses the real MeshStore and retains attribution.
const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
const meshes: MeshStore[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const mesh of meshes.splice(0)) mesh.closeState();
  vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

it.each(["lock timeout", "FIFO expiry"])("bounds an actual participant refusal job after %s and routes its exhaustion alarm once to the configured owner", async failure => {
  vi.stubEnv("PI_FABRIC_MESH_BACKGROUND_GIVEUP_MS", "250");
  const to = "session:22222222-2222-4222-8222-222222222222";
  vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", to);
  vi.spyOn(childProcess, "execFile").mockImplementation(() => {
    const child = new childProcess.ChildProcess(); child.stdin = new PassThrough(); return child;
  }); // Never page the live fleet journal from a test.
  const errors = vi.spyOn(console, "error").mockImplementation(() => {});
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "participant-refusal-giveup-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend: "file" }); meshes.push(mesh);
  const from = { id: "session:local", name: "owner", kind: "main" as const };
  const bad = "session:bad";
  const key = "topology/participants/" + createHash("sha256").update(bad).digest("hex");
  await mesh.put({ identity: from, key, value: { format: 1, id: bad, remoteHost: "remote", sessionId: 42 } });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: from.id, rootId: from.id,
    identity: from, reapDeadHosts: false }); directories.push(directory);
  const publish = mesh.publish.bind(mesh);
  let refusalAttempts = 0;
  vi.spyOn(mesh, "publish").mockImplementation(input => {
    if (input.kind === "refused") {
      refusalAttempts++;
      return Promise.reject(failure === "FIFO expiry" ? new MeshLockQueueExpiredError(path.join(root, ".lock"), 1, 1)
        : new MeshLockTimeoutError(" held by incident-fixture", 1, 1));
    }
    return publish(input);
  });
  vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
  directory.list({ scope: "project", fresh: true });
  await vi.advanceTimersByTimeAsync(250);
  expect(refusalAttempts).toBe(3); // attempts at 0, 50 and 150 ms; no attempt at the deadline.
  await vi.advanceTimersByTimeAsync(10_000);
  expect(refusalAttempts).toBe(3);
  const giveups = errors.mock.calls.map(([line]) => String(line)).filter(line => line.includes("FABRIC_MESH_BACKGROUND_GIVEUP"));
  expect(giveups).toHaveLength(1);
  expect(giveups[0]).toContain('"label":"participant refusal/reap"');
  const events = mesh.read({ topic: FABRIC_MESH_OUTAGE_ALARM_TOPIC });
  expect(events).toEqual([expect.objectContaining({ to, from, kind: "mesh-outage",
    data: expect.objectContaining({ label: "participant refusal/reap", reason: "mesh background retry budget exhausted" }) })]);
});
