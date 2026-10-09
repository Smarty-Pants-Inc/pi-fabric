import { connect } from "@nats-io/transport-node";
import { NatsKvLeaseStore, newLeaseIncarnation } from "../../src/topology/nats-kv-leases.js";
import type { ProtectedWrite, WriteReply } from "./nats-lease-resource.js";

// A real process that retains its OLD token across SIGSTOP/expiry/SIGCONT.
// It deliberately attempts protected work WITHOUT consulting Date.now() or read().
const [servers, bucket, id, subject, resourceServers] = process.argv.slice(2) as [string, string, string, string, string];
const nc = await connect({ servers: servers.split(",") });
const resourceNc = await connect({ servers: resourceServers.split(",") });
try {
  // Parent owns the test-only always-sync server configs. Not user/lease data.
  const store = await NatsKvLeaseStore.open(nc, { bucket, maxLeaseMs: 10_000, syncAlwaysAttested: true });
  const now = Date.now();
  const owned = (await store.acquire({ id, rootId: "paused-root", identityId: "paused-A",
    startedAt: now, updatedAt: now, expiresAt: now + 5_000 }, newLeaseIncarnation()))!;
  if (!owned) throw new Error("Paused owner did not acquire");
  const write = async (writeId: string): Promise<WriteReply> =>
    (await resourceNc.request(subject, JSON.stringify({ owner: "paused-A", fence: owned.revision,
      writeId } satisfies ProtectedWrite), { timeout: 3_000 })).json<WriteReply>();
  if (!(await write("paused-A-first")).accepted) throw new Error("Initial protected write rejected");
  console.log(`PAUSE_READY ${JSON.stringify(owned)}`);
  // Parent SIGSTOPs before this ONE attempted write; after SIGCONT the overdue
  // timeout resumes. This is a test action, not lease acquisition/expiry polling.
  await new Promise<void>(resolve => setTimeout(resolve, 500));
  const stale = await write("paused-A-stale-after-successor");
  console.log(`STALE_WRITE ${JSON.stringify(stale)}`);
  if (stale.accepted) throw new Error("Stale owner landed a protected write after successor");
} finally { await Promise.all([nc.close(), resourceNc.close()]); }
