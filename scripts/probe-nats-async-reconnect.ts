import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import { NatsKvStateStore } from "../src/mesh/state-nats-kv.js";
import { openNatsMeshProvider } from "../src/mesh/state-async.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const deadline = async <T>(promise: Promise<T>, ms = 30_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Reconnect probe deadline spent")), ms); })]); }
  finally { clearTimeout(timer); }
};
export async function probeNatsAsyncReconnect(options: {
  servers: string[]; output: string; stopCluster(): Promise<void>; restartCluster(stream: string): Promise<void>;
}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-async-reconnect-"));
  const identity = { id: "reconnect-probe", name: "reconnect probe", kind: "main" as const };
  const context = {} as FabricInvocationContext, participants = { list: () => [] } as unknown as FabricParticipantSource;
  const local = new MeshStore(root, 128 * 1024, 100);
  const store = await NatsKvStateStore.open(root, { servers: options.servers[0]!, experimentalNatsKv: true, timeoutMs: 1_000 });
  const status = store.connectionStatus()[Symbol.asyncIterator]();
  const transport: unknown[] = [];
  const nextStatus = async (type: string) => {
    for (;;) { const next = await status.next(); assert.equal(next.done, false, "transport observer ended early"); transport.push(next.value); if (next.value.type === type) return next.value; }
  };
  let provider: Awaited<ReturnType<typeof openNatsMeshProvider>> | undefined;
  let reopened: NatsKvStateStore | undefined;
  try {
    provider = await openNatsMeshProvider(local, identity, participants, { backend: "nats-kv", nats: { servers: options.servers, experimentalNatsKv: true } });
    const key = "shared/restart";
    const first = await provider.invoke("put", { key, value: { saved: true }, ifVersion: 0 }, context) as MeshStateEntry;
    assert.deepEqual(await store.get(key), first); // independent reader on seed node 0, before stop
    const disconnected = deadline(nextStatus("disconnect")); disconnected.catch(() => {});
    await options.stopCluster(); await disconnected;
    let offlineError: { name: string; message: string } | undefined;
    try { await store.get(key); } catch (error) { const e = error as Error; offlineError = { name: e.name, message: e.message }; }
    assert.ok(offlineError, "offline reads must reject, never return cached success/absence");
    const reconnected = deadline(nextStatus("reconnect")); reconnected.catch(() => {});
    await options.restartCluster(`KV_${store.bucket}`); await reconnected;
    assert.deepEqual(await store.get(key), first, "same client must retain read-your-writes and revision after reconnect");
    assert.deepEqual(await provider.invoke("get", { key }, context), first, "provider's existing async handle must reconnect too");
    reopened = await NatsKvStateStore.open(root, { servers: options.servers, experimentalNatsKv: true });
    assert.deepEqual(await reopened.get(key), first, "reopened handle must observe persisted authority");
    await assert.rejects(reopened.put({ key, value: "stale", ifVersion: 0, identity }), /compare-and-swap/);
    const next = await reopened.put({ key, value: { saved: "after restart" }, ifVersion: first.version, identity });
    assert.ok(next.version > first.version); assert.deepEqual(await store.get(key), next);
    // Exercise the BUILT public export, not just source transpilation.
    const builtPath = path.resolve("dist/mesh.js"); assert.ok(fs.existsSync(builtPath), `Build first: ${builtPath}`);
    const built = await import(builtPath);
    for (const name of ["openNatsMeshProvider", "openAsyncMeshStateStore", "NatsKvStateStore", "MeshStore"]) assert.equal(typeof built[name], "function", `built public symbol ${name}`);
    const builtLocal = new built.MeshStore(root, 128 * 1024, 100);
    const builtProvider = await built.openNatsMeshProvider(builtLocal, identity, participants, { backend: "nats-kv", nats: { servers: options.servers, experimentalNatsKv: true } });
    try { assert.deepEqual(await builtProvider.invoke("get", { key }, context), next); }
    finally { await builtProvider.close(); builtLocal.closeState(); }
    fs.writeFileSync(path.join(options.output, "async-reconnect.json"), JSON.stringify({ result: "PASS", allThreeNodesRestarted: true, sameClientReconnected: true, offlineError, transport, before: first, after: next, reopened: await reopened.get(key), builtPublicProbe: "PASS", faultDomains: "one physical host only" }, null, 2) + "\n");
  } finally {
    await status.return?.(); await provider?.close(); await reopened?.close(); await store.close();
    local.closeState(); fs.rmSync(root, { recursive: true, force: true });
  }
}
