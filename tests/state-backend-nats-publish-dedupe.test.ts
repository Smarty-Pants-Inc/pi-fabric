// Real nats-server (NATS_BIN, official 2.14.7) R3 proof for fix A (smarty-dev#13112): after a committed publish, an
// identical same-ID retry with the SAME stale expected last sequence is a duplicate ACK at the original sequence (the
// clustered dedupe map answers before the expected-sequence check); a non-duplicate with a wrong expectation still
// fails the CAS. Owned cluster helper, puts only: no product delete or purge. Runs in the dedicated nats-* CI job.
import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";
import { afterEach, expect, it } from "vitest";
import { NatsWrongLastSequenceError } from "../src/mesh/state-backend-nats.js";
import { connectJetStream } from "../src/mesh/state-backend-nats-client.js";
import { startNatsElectionCluster, stopNatsElectionCluster, type NatsElectionCluster } from "./helpers/nats-election-cluster.js";
import { waitForNatsStreamReady } from "./helpers/nats-consumer-ready.js";

const binary = process.env.NATS_BIN;
let cluster: NatsElectionCluster | undefined;
afterEach(async () => { if (cluster) { const owned = cluster; cluster = undefined; await stopNatsElectionCluster(owned); } });

it.skipIf(!binary)("an identical same-ID retry after a committed R3 publish is a duplicate ACK at the original sequence", async () => {
  cluster = await startNatsElectionCluster(binary!);
  const stream = "NATSFLAKE_PUBLISH_DEDUPE";
  const subject = `${stream}.state`;
  const port = await connectJetStream([cluster.seedUrl], { replicas: 3 });
  const nc = await connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true });
  try {
    await port.ensureStream(stream, [subject], { maxMsgSize: 1024 * 1024, replicas: 3 });
    await waitForNatsStreamReady(cluster.seedUrl, stream);
    const manager = await jetstreamManager(nc);
    expect((await manager.streams.info(stream)).config.duplicate_window).toBeGreaterThan(5_000 * 1e6);
    const bytes = new TextEncoder().encode("batch-one");
    const first = await port.publish(subject, bytes, { expectLastSequence: 0, msgID: "stable-batch", timeoutMs: 1_000 });
    expect(first).toEqual({ seq: 1, duplicate: false });
    // The lost-ACK retry: same bytes, same id, same (now stale) expected last sequence.
    const retry = await port.publish(subject, bytes, { expectLastSequence: 0, msgID: "stable-batch", timeoutMs: 1_000 });
    expect(retry).toEqual({ seq: 1, duplicate: true });
    expect((await manager.streams.info(stream)).state).toMatchObject({ last_seq: 1, messages: 1 });
    // A different id with a wrong expectation is not a duplicate: the CAS still refuses it.
    await expect(port.publish(subject, bytes, { expectLastSequence: 0, msgID: "other-batch", timeoutMs: 1_000 }))
      .rejects.toBeInstanceOf(NatsWrongLastSequenceError);
    expect((await manager.streams.info(stream)).state).toMatchObject({ last_seq: 1, messages: 1 });
  } finally {
    await nc.drain();
    await port.close();
  }
}, 60_000);
