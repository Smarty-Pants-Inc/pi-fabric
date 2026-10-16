// The real nats-server for the StateBackend conformance runs (smarty-dev#7504 phase 2). The suites run their `nats`
// cases only when NATS_SERVER_URL and NATS_MONITOR_URL name a running server (JetStream with `sync_interval: always`,
// an http monitor port); otherwise each suite shows one skipped case that names the reason. NATS_SERVER_REQUIRED=1
// (the CI job) turns a missing server into a failure, so the job can never pass by skipping.
//
// NATS_STATE_PROFILE=single|cluster (pi-fabric#11373) is a harness-only selector: it picks the stream replica count
// (1 or 3) and nothing else. NATS_SERVER_URL/NATS_MONITOR_URL stay the single seed connection inputs. At module
// evaluation (before any test, and so before every direct product call that bypasses prepareNatsRoot) the helper
// writes mesh.nats.replicas into this file's PRIVATE PI_CODING_AGENT_DIR/fabric.json, which scripts/test-temp.ts
// creates fresh per test file; spawned children inherit that env and so read the same config. Any existing config
// there is preserved; the path is validated as the private test fleet's agent dir before the write.
import fs, { existsSync, readFileSync, realpathSync, renameSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import path from "node:path";
import { encodeMeshStateNatsMarker } from "../../src/mesh/backend-fence.js";
import { connectJetStream } from "../../src/mesh/state-backend-nats-client.js";
import { it } from "vitest";
import { MAX_MESSAGE_BYTES, natsStreamName, openNatsStateBackend, type NatsStateBackend } from "../../src/mesh/state-backend-nats.js";

const profiles = { single: 1, cluster: 3 } as const;
const rawProfile = process.env.NATS_STATE_PROFILE;
if (rawProfile !== undefined && rawProfile !== "" && !Object.hasOwn(profiles, rawProfile)) {
  throw new Error(`NATS_STATE_PROFILE must be "single" or "cluster", got ${JSON.stringify(rawProfile)}`);
}
export const natsStateProfile = rawProfile ? rawProfile as keyof typeof profiles : undefined;
export const natsStateReplicas = natsStateProfile === undefined ? undefined : profiles[natsStateProfile];
if (process.env.NATS_SERVER_REQUIRED === "1" && natsStateProfile === undefined) {
  throw new Error("NATS_SERVER_REQUIRED=1 but NATS_STATE_PROFILE is unset (single|cluster)");
}

/** The private fixture agent dir: <tmpdir>/pi-fabric-test-fleet-XXXX/agent, never the real home profile. */
const privateAgentDir = (): string => {
  const raw = process.env.PI_CODING_AGENT_DIR?.trim();
  if (!raw || !path.isAbsolute(raw)) throw new Error("NATS_STATE_PROFILE: PI_CODING_AGENT_DIR is not an absolute private test path");
  const dir = realpathSync(raw);
  const fleet = path.dirname(dir);
  const temp = realpathSync(tmpdir());
  const home = realpathSync(homedir());
  if (path.basename(dir) !== "agent" || !path.basename(fleet).startsWith("pi-fabric-test-fleet-") || path.dirname(fleet) !== temp
    || dir === path.join(home, ".pi", "agent") || dir.startsWith(path.join(home, ".pi") + path.sep)) {
    throw new Error(`NATS_STATE_PROFILE: refusing to write config outside the private test fleet: ${dir}`);
  }
  return dir;
};

if (natsStateReplicas !== undefined) {
  const file = path.join(privateAgentDir(), "fabric.json");
  let config: Record<string, unknown> = {};
  if (existsSync(file)) {
    const parsed: unknown = JSON.parse(readFileSync(file, "utf8"));
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error(`${file} is not a JSON object`);
    config = parsed as Record<string, unknown>;
  }
  const objectAt = (value: unknown, name: string): Record<string, unknown> => {
    if (value === undefined) return {};
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error(`${file}: ${name} is not an object`);
    return value as Record<string, unknown>;
  };
  const mesh = objectAt(config.mesh, "mesh");
  const nats = objectAt(mesh.nats, "mesh.nats");
  const next = { ...config, mesh: { ...mesh, nats: { ...nats, replicas: natsStateReplicas } } };
  const staging = `${file}.${process.pid}.tmp`;
  writeFileSync(staging, `${JSON.stringify(next, null, 2)}\n`, { flag: "wx" });
  renameSync(staging, file);
}

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

/**
 * Makes `root` a NATS root for a test: the NATS moved marker over state.json (smarty-dev#9690: a root opens only the
 * stream its marker names). The stream defaults to the root's own (natsStreamName, pi-fabric#937: root-bound); a test of
 * the refusal may name another. A real migration does this through importMeshStateToNats.
 */
export const markNatsRoot = (root: string, stream?: string): void => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  // After mkdir: natsStreamName canonicalizes an existing root through its realpath.
  fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateNatsMarker(stream ?? natsStreamName(root), 1), { mode: 0o600 });
};

/** Prepares the root's NATS backend, so `new MeshStore(root, …, { stateBackend: "nats" })` gets it. Its stream is
 * derived from the root (pi-fabric#937): each test's fresh root has its own, so cases never share state on the server. */
export const prepareNatsRoot = async (root: string, options: { lockTimeoutMs?: number } = {}): Promise<NatsStateBackend> => {
  if (!natsConfigured) throw new Error(natsSkipReason);
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const stream = natsStreamName(root);
  // The root's open never creates a stream (smarty-dev#9690): a test creates it, as a migration would (the profile's
  // replicas from the private fabric.json written above).
  const port = await connectJetStream([natsServerUrl!]);
  try { await port.ensureStream(stream, [`${stream}.state`, `${stream}.snapshot`], { maxMsgSize: MAX_MESSAGE_BYTES }); } finally { await port.close(); }
  if (!fs.existsSync(path.join(root, "state.json"))) markNatsRoot(root, stream);
  return openNatsStateBackend({ root, servers: [natsServerUrl!], monitoringUrl: natsMonitorUrl!,
    ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }) });
};
