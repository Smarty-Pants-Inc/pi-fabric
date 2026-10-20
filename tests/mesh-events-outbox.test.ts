import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "session:outbox-test", name: "main", kind: "main", sessionId: "outbox-test" };

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("Mesh event publication outbox", () => {
  it("keeps the committed envelope across a failed forward and retries it after reopen", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-event-outbox-"));
    roots.push(root);
    const firstStore = new MeshStore(root, 64 * 1024, 100);
    let firstBinding: ReturnType<typeof firstStore.bindEventPublisher> | undefined;
    let reopened: MeshStore | undefined;
    let retryBinding: ReturnType<MeshStore["bindEventPublisher"]> | undefined;
    let releaseFirstAttempt!: () => void;
    const firstAttempt = new Promise<void>(resolve => { releaseFirstAttempt = resolve; });
    const attempts: unknown[] = [];
    try {
      firstBinding = firstStore.bindEventPublisher("nats:demo", async event => {
        attempts.push(event);
        releaseFirstAttempt();
        throw new Error("broker unavailable");
      });
      const local = await firstStore.publish({
        id: "stable-outbox-event",
        topic: "mail.inbound",
        kind: "message",
        from: identity,
        text: "accepted locally",
        data: { delivery: 17 },
      });
      await firstAttempt;
      await firstBinding.close();
      firstStore.closeState();

      reopened = new MeshStore(root, 64 * 1024, 100);
      const forwardedEvents: unknown[] = [];
      const forwarded = new Promise<unknown>(resolve => {
        retryBinding = reopened!.bindEventPublisher("nats:demo", async event => {
          forwardedEvents.push(event);
          resolve(event);
        });
      });
      const retried = await forwarded;
      await reopened.retryEventPublications();
      expect(attempts).toHaveLength(1);
      expect(forwardedEvents).toHaveLength(1);
      expect(retried).toMatchObject({
        id: "stable-outbox-event",
        topic: "mail.inbound",
        sequence: local.sequence,
        from: identity,
        text: "accepted locally",
        data: { delivery: 17 },
      });
    } finally {
      await retryBinding?.close();
      reopened?.closeState();
      await firstBinding?.close();
      firstStore.closeState();
    }
  });
});
