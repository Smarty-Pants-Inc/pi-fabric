import { connect } from "@nats-io/transport-node";
import type { KV } from "@nats-io/kv";
import { NatsKvLeaseStore, newLeaseIncarnation } from "../../src/topology/nats-kv-leases.js";

// The parent kills this process with SIGKILL before acquire can return a handle.
const [servers, bucket, id, phase] = process.argv.slice(2) as [string, string, string, string];
const nc = await connect({ servers: servers.split(",") });
const store = await NatsKvLeaseStore.open(nc, { bucket, maxLeaseMs: 5_000 });
const kv = (store as unknown as { kv: KV }).kv;
const create = kv.create.bind(kv);
kv.create = async (...args) => {
  if (phase === "after") await create(...args);
  process.stdout.write(`PUBLISH_${phase.toUpperCase()}\n`);
  // Hold between protocol publish and adapter completion; no file/process lock exists.
  await new Promise<never>(() => undefined);
  throw new Error("unreachable");
};
const now = Date.now();
await store.acquire({ id, rootId: "crash-root", identityId: "crash-owner", startedAt: now,
  updatedAt: now, expiresAt: now + 400 }, newLeaseIncarnation());
throw new Error("Crash seam unexpectedly returned ownership");
