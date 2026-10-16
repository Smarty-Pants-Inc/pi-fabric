import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";
import * as wake from "../src/residency/wake.js";

// Keep reason: a data-safety/liveness boundary between committed publication,
// durable wake ACK and owner admission. No child process or periodic polling is needed.
const from = { id: "publisher", name: "publisher", kind: "main" as const };
const topic = "publication.admission.delivery";

describe("publication wake admission after intent commit", () => {
  it.each([false, true])("retains exactly one committed wake across the ACK wait (gate stays open: %s)", async staysOpen => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-publication-admission-"));
    let allowed = true;
    const mesh = new MeshStore(path.join(directory, "mesh"), 65_536, 100, {
      stateBackend: "file", canWakeResidents: () => allowed,
    });
    const rootId = "session:publication-admission";
    const root = residentRoot(mesh.root, rootId);
    const config = { rootId, residencyRoot: root, cwd: directory };
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({
      format: 1, rootId, hostId: "host:publication-admission",
      configJson: canonicalResidentWakeConfig(config),
      actors: [{ id: "actor:listener", name: "listener", topics: [topic] }],
    }));

    const launches: Array<{ id: string; sequence: number | undefined }> = [];
    const launch = vi.fn(async (file: string, saved: ResidentHostConfig) => {
      expect(file).toBe(path.join(root, "config.json"));
      expect(saved).toEqual(config);
      const intent = wake.readWakeJson<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(root));
      expect(intent).toBeDefined();
      launches.push({ id: intent!.id, sequence: intent!.sequence });
    });
    // Exercise the REAL MeshStore.publish -> dynamically imported dispatcher.
    // Preserve its live canWake argument; substitute only the native launch boundary.
    const dispatch = wake.wakeResidentActors;
    const publicationWakes = vi.spyOn(wake, "wakeResidentActors")
      .mockImplementation((store, events, _launch, recovery, ...rest) =>
        dispatch(store, events, launch, recovery, ...rest));
    const retry = wake.retryResidentWakes;
    const maintenance = vi.spyOn(wake, "retryResidentWakes")
      .mockImplementation((store, _launch, now, ...rest) => retry(store, launch, now, ...rest));

    let entered!: () => void;
    let release!: () => void;
    const ackEntered = new Promise<void>(resolve => { entered = resolve; });
    const ackReleased = new Promise<void>(resolve => { release = resolve; });
    let suspended = false;
    const exclusive = mesh.exclusive.bind(mesh);
    vi.spyOn(mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      // The pending read and intent-write custody both start WITHOUT this file.
      // Its first subsequent exclusive is onIntentWritten's ACK, not the earlier
      // intent lock/write wait covered by residency-wake-backoff.test.ts.
      if (!suspended && fs.existsSync(wake.residentWakeRequestPath(root))) {
        suspended = true;
        entered();
        await ackReleased;
      }
      return exclusive(operation, timeout);
    });

    let publishing: Promise<MeshEvent> | undefined;
    let returned = false;
    try {
      publishing = mesh.publish({ topic, from, text: "committed before admission closes" })
        .then(event => { returned = true; return event; });
      // Event-driven barrier, not a sleep/poll: fail if publication returns without
      // reaching the intended ACK boundary. Vitest bounds unexpected deadlocks.
      await Promise.race([ackEntered, publishing.then(() => {
        throw new Error("publication returned without suspending the intent ACK");
      })]);
      expect(publicationWakes).toHaveBeenCalledOnce();
      expect(returned).toBe(false);
      expect(launch).not.toHaveBeenCalled();
      const committed = mesh.read().filter(event => event.topic === topic);
      expect(committed).toHaveLength(1);
      const delivery = { id: committed[0]!.id, sequence: committed[0]!.sequence };
      expect(wake.readWakeJson(wake.residentWakeRequestPath(root))).toMatchObject(delivery);
      expect(residentUnacknowledgedDeliveries(mesh.root).get(root)).toEqual(delivery);
      // One cold root fits in the journal: product overflow unlink is never reached.
      expect(fs.existsSync(path.join(mesh.root, "wake-overflow"))).toBe(false);

      allowed = staysOpen;
      release();
      const published = await publishing;
      expect({ id: published.id, sequence: published.sequence }).toEqual(delivery);
      expect(mesh.read().filter(event => event.topic === topic)).toEqual([published]);
      expect(wake.readWakeJson(wake.residentWakeRequestPath(root))).toMatchObject(delivery);
      expect(launch).toHaveBeenCalledTimes(staysOpen ? 1 : 0);

      allowed = true;
      // Use the existing store maintenance entry point at a due time, without a
      // second publish. Accommodate either a retained index or a backoff receipt.
      const receipt = wake.readWakeJson<{ nextRetryAt?: number }>(path.join(root, "wake-failure.json"));
      const due = Math.max(Date.now(), receipt?.nextRetryAt ?? 0) + wake.RESIDENT_WAKE_RETRY_MIN_MS;
      vi.spyOn(Date, "now").mockReturnValue(due);
      const beforeRetry = launch.mock.calls.length;
      await mesh.retryResidentWakes();
      expect(maintenance).toHaveBeenCalledOnce();
      expect.soft(launch.mock.calls.length - beforeRetry, "reopened admission must retry the committed delivery, not lose its nudge")
        .toBe(staysOpen ? 0 : 1);
      expect.soft(launches, "the exact committed id AND sequence must launch once").toEqual([delivery]);
      expect(residentUnacknowledgedDeliveries(mesh.root).size).toBe(0);
      expect(fs.existsSync(path.join(root, "wake-failure.json"))).toBe(false);

      const afterRetry = launch.mock.calls.length;
      await mesh.retryResidentWakes();
      await mesh.retryResidentWakes();
      expect(launch.mock.calls.length - afterRetry, "additional maintenance must launch zero children").toBe(0);
      expect.soft(launch).toHaveBeenCalledOnce();
      expect(publicationWakes).toHaveBeenCalledOnce(); // No rescue publication.
      expect(mesh.read().filter(event => event.topic === topic)).toEqual([published]);
      expect(fs.existsSync(path.join(mesh.root, "wake-overflow"))).toBe(false);
    } finally {
      release();
      // Never remove a fixture while its publication is still using it.
      try { await publishing; }
      finally {
        vi.restoreAllMocks();
        mesh.closeState();
        fs.rmSync(directory, { recursive: true, force: true });
      }
    }
  }, 10_000);
});
