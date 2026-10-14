import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { importMeshStateToNats, type MeshNatsMigrationOptions } from "../src/mesh/backend-migration-nats.js";
import { MeshBackendRefusedError } from "../src/mesh/backend-migration.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";

const identity: MeshIdentity = { id: "retry-test", name: "retry-test", kind: "agent" };
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-nats-publish-retry-")); roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "retry/key", value: { exact: "bytes" }, identity }); store.closeState();
  const server = fakeJetStream();
  return { root, server, options: { port: server.port() } satisfies MeshNatsMigrationOptions };
};
const transient = () => Object.assign(new Error("lost acknowledgement"), { transient: true });

describe("migration-only NATS publish retries", () => {
  it("reconciles a commit whose acknowledgement was lost and commits exactly once", async () => {
    const { root, server, options } = await setup();
    let lost = false;
    server.beforePublish = () => { if (!lost) { lost = true; return "lose-ack"; } };
    const result = await importMeshStateToNats(root, options);
    expect(result.converged).toBe(false);
    expect(server.messages).toHaveLength(3); // begin, one batch, commit; no duplicate commit
    expect(server.messages[0]?.msgID).toBe(`${result.stream}-import-begin-1`);
  });

  it("retries an unknown but uncommitted publish with identical bytes, id and CAS", async () => {
    const { root, server, options } = await setup();
    const underlying = options.port!;
    const attempts: { bytes: Buffer; id: string; cas: number }[] = [];
    let failOnce = true;
    const port = { ...underlying, publish: async (subject: string, data: Uint8Array, args: { expectLastSequence: number; msgID: string }) => {
      attempts.push({ bytes: Buffer.from(data), id: args.msgID, cas: args.expectLastSequence });
      if (failOnce) { failOnce = false; throw transient(); }
      return underlying.publish(subject, data, args);
    } };
    await importMeshStateToNats(root, { port });
    expect(attempts.length).toBeGreaterThan(1);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(server.messages).toHaveLength(3);
  });

  it.each(["id", "bytes"] as const)("refuses ambiguous stored publish with mismatched %s", async mismatch => {
    const { root, server, options } = await setup();
    let corrupted = false;
    server.beforePublish = () => { if (!corrupted) { corrupted = true; return "lose-ack"; } };
    server.afterStore = seq => {
      const message = server.messages[seq - 1]!;
      if (corrupted && seq === 1) {
        if (mismatch === "id") message.msgID = "foreign-id";
        else message.data = Buffer.from("foreign-bytes");
      }
    };
    await expect(importMeshStateToNats(root, options)).rejects.toBeInstanceOf(MeshBackendRefusedError);
    expect(server.messages).toHaveLength(1);
  });

  it("bounds transient publish attempts at three", async () => {
    const { root, options } = await setup();
    const underlying = options.port!;
    let attempts = 0;
    const port = { ...underlying, publish: async () => { attempts++; throw transient(); } };
    await expect(importMeshStateToNats(root, { port })).rejects.toThrow("lost acknowledgement");
    expect(attempts).toBe(3);
  });

  it("continues to refuse an unrelated CAS conflict", async () => {
    const { root, server, options } = await setup();
    let raced = false;
    server.beforePublish = () => { if (!raced) { raced = true; server.append(Buffer.from("foreign")); } };
    await expect(importMeshStateToNats(root, options)).rejects.toBeInstanceOf(MeshBackendRefusedError);
    expect(server.messages).toHaveLength(1);
  });
});
