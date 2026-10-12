// The real nats-server for the StateBackend conformance runs (smarty-dev#7504 phase 2). The suites run their `nats`
// cases only when NATS_SERVER_URL and NATS_MONITOR_URL name a running server (JetStream with `sync_interval: always`,
// an http monitor port); otherwise each suite shows one skipped case that names the reason. NATS_SERVER_REQUIRED=1
// (the CI job) turns a missing server into a failure, so the job can never pass by skipping.
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import { openNatsStateBackend, type NatsStateBackend } from "../../src/mesh/state-backend-nats.js";

export const natsServerUrl = process.env.NATS_SERVER_URL?.trim() || undefined;
export const natsMonitorUrl = process.env.NATS_MONITOR_URL?.trim() || undefined;
export const natsConfigured = natsServerUrl !== undefined && natsMonitorUrl !== undefined;
export const natsSkipReason = "nats conformance skipped: set NATS_SERVER_URL and NATS_MONITOR_URL to a running nats-server (docs/mesh-backends.md)";
if (process.env.NATS_SERVER_REQUIRED === "1" && !natsConfigured) {
  throw new Error("NATS_SERVER_REQUIRED=1 but NATS_SERVER_URL or NATS_MONITOR_URL is unset");
}

/** `it` for a backend kind: the nats cases are skipped without a server. */
export const itFor = (kind: string) => it.skipIf(kind === "nats" && !natsConfigured);

/** One visible skipped case per suite that says why its nats cases did not run. */
export const reportNatsSkip = (): void => { if (!natsConfigured) it.skip(natsSkipReason, () => undefined); };

/** A fresh stream name per test: the conformance cases never share state on the server. */
export const natsTestStream = (): string => `FABRIC_CONF_${randomUUID().replaceAll("-", "")}`;

/** Prepares the root's NATS backend on its own stream, so `new MeshStore(root, …, { stateBackend: "nats" })` gets it. */
export const prepareNatsRoot = (root: string, options: { stream?: string; lockTimeoutMs?: number } = {}): Promise<NatsStateBackend> => {
  if (!natsConfigured) throw new Error(natsSkipReason);
  return openNatsStateBackend({ root, servers: [natsServerUrl!], monitoringUrl: natsMonitorUrl!, stream: options.stream ?? natsTestStream(),
    ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }) });
};
