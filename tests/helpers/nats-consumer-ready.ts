import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";

/** Test setup only: CREATE/consume returning does not mean the broker has registered the first NEXT.
 * Observe the broker's pending pull before a contract test publishes its first write. The SDK's default
 * pull expires after 30s, so allow its next native pull too; never replace/reset the consumer or retry a write. */
export const waitForNatsConsumerReady = async (server: string, stream: string, name?: string): Promise<void> => {
  const nc = await connect({ servers: [server], ignoreClusterUpdates: true, timeout: 1_000 });
  try {
    const manager = await jetstreamManager(nc, { timeout: 1_000 });
    const deadline = Date.now() + 45_000;
    let last: unknown;
    while (Date.now() < deadline) {
      try {
        const consumers = name ? [await manager.consumers.info(stream, name)] : await manager.consumers.list(stream).next();
        last = consumers.map(info => ({ name: info.name, waiting: info.num_waiting }));
        if (consumers.length > 0 && consumers.every(info => info.num_waiting > 0)) return;
      } catch (error) { last = error instanceof Error ? error.message : String(error); }
      await new Promise<void>(resolve => setTimeout(resolve, 50));
    }
    throw new Error(`NATS first NEXT was not registered for ${stream}: ${JSON.stringify(last)}`);
  } finally { await nc.close(); }
};
