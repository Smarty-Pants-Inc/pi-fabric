import { openAsyncMeshStateStore } from "../src/mesh/state-async.ts";

const [kind, root, key, countText] = process.argv.slice(2);
if (!["file", "sqlite", "nats-kv"].includes(kind ?? "") || !root || !key) throw new Error("usage: bun scripts/state-cas-worker.ts <file|sqlite|nats-kv> <root> <key> <count>");
const count = Number(countText ?? 25);
const store = await openAsyncMeshStateStore(root, kind === "nats-kv"
  ? { backend: kind, nats: { servers: process.env.FABRIC_NATS_TEST_SERVERS!.split(","), experimentalNatsKv: true } }
  : { backend: kind as "file" | "sqlite" });
try {
  const start = new Promise<void>(resolve => process.stdin.once("data", () => { process.stdin.pause(); resolve(); }));
  console.log("READY");
  await start;
  let conflicts = 0;
  const revisions: number[] = [];
  for (let n = 0; n < count; n++) {
    let committed = false;
    for (let retry = 0; retry < 10_000; retry++) {
      const entry = await store.get(key);
      if (!entry) throw new Error("Counter disappeared");
      try {
        const next = await store.put({ key, value: Number(entry.value) + 1, ifVersion: entry.version,
          identity: { id: `worker-${process.pid}`, name: "CAS conformance", kind: "agent" } });
        revisions.push(next.version); committed = true; break;
      } catch (error) {
        if (!(error instanceof Error) || !/compare-and-swap failed/.test(error.message)) throw error;
        conflicts++;
      }
    }
    if (!committed) throw new Error("CAS retry budget spent");
  }
  console.log(JSON.stringify({ pid: process.pid, kind, count, conflicts, revisions }));
} finally { await store.close(); }
