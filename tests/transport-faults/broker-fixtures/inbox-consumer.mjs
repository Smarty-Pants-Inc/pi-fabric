// Real Node-only carrier consumer. Its parent owns the fault and the close join.
// No control-plane imitation, no custody/claim journal, no NAK, no clean-close crash surrogate.
import { Buffer } from "node:buffer";
import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";

if (process.release.name !== "node" || "bun" in process.versions) {
  throw new Error("broker inbox consumer requires guarded Node, not Bun");
}
const root = fs.realpathSync(process.env.TMPDIR ?? "");
for (const name of ["VITE_CACHE_DIR", "XDG_CACHE_HOME", "npm_config_cache", "JITI_FS_CACHE"]) {
  const value = process.env[name];
  const relative = value ? path.relative(root, value) : "..";
  if (!value || !path.isAbsolute(value) || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new Error("broker consumer cache escapes its private TMPDIR");
  }
}
if (!process.env.NODE_OPTIONS?.includes("--import=") || !process.env.PI_FABRIC_TEST_LAUNCH_LOG) {
  throw new Error("broker consumer did not inherit its Node preload/launch guard");
}

// Finite last-resort lifetime even if parent assertion/setup fails before it can record ownership.
// Normal progress is driven solely by broker deliveries and IPC, never this timer.
const deadline = setTimeout(() => process.exit(124), 45_000);
const jiti = createJiti(import.meta.url, { fsCache: process.env.JITI_FS_CACHE });
const send = (receipt) => new Promise((resolve, reject) => {
  if (!process.send) { reject(new Error("parent IPC is absent")); return; }
  process.send(receipt, (error) => error ? reject(error) : resolve());
});
const source = (relative) => new URL(relative, import.meta.url).href;
let transport;
let mesh;
try {
  const { startTime } = await jiti.import(source("../../helpers/owned-processes.ts"));
  const started = startTime(process.pid);
  if (!started) throw new Error("Node consumer has no process birth receipt");
  await send({ type: "birth", pid: process.pid, started, node: process.versions.node, bun: "bun" in process.versions });

  // Always import the current worktree factory, not dist, a copied adapter, or a stub engine.
  const { openFabricTransport } = await jiti.import(source("../../../src/transport/index.ts"));
  const { openNatsConnection } = await jiti.import(source("../../../src/transport/nats/connection.ts"));
  const { DEFAULT_FABRIC_CONFIG } = await jiti.import(source("../../../src/config.ts"));
  const { MeshStore } = await jiti.import(source("../../../src/mesh/store.ts"));
  const servers = process.env.FABRIC_BROKER_URL;
  const credentials = process.env.FABRIC_BROKER_CREDS;
  const hostId = process.env.FABRIC_BROKER_HOST;
  if (!servers || !credentials || !hostId) throw new Error("Node consumer configuration is incomplete");
  const connection = await openNatsConnection({ servers, credentials, machine: "machine-b" });
  try {
    const info = connection.userInfo?.data;
    if (info?.user !== process.env.FABRIC_BROKER_USER ||
      !info.permissions?.publish?.allow?.includes("fabric.in.*.*.*.*.machine-b")) {
      throw new Error("Node consumer USER.INFO did not verify its signed machine principal");
    }
  } finally { await connection.close(); }

  const meshRoot = path.join(root, "mesh");
  mesh = new MeshStore(meshRoot, 1_048_576, 100, { stateBackend: "file" });
  transport = await openFabricTransport({
    config: {
      ...DEFAULT_FABRIC_CONFIG.mesh, root: meshRoot, transport: "nats",
      nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, servers, credentials, machine: "machine-b" },
    },
    hostId, mesh,
  });
  if (transport.kind !== "nats" || transport.self.hostId !== hostId || transport.self.machine !== "machine-b") {
    throw new Error("Node consumer factory did not open the requested current NATS carrier");
  }
  await send({ type: "ready" });
  let primed = false;
  const signal = new AbortController().signal;
  for await (const delivery of transport.inbox(signal)) {
    if (delivery.sourceMachine !== "machine-a" || delivery.verification !== "bridge") {
      throw new Error("Node consumer received an unauthenticated origin");
    }
    if (!primed) {
      if (delivery.payload.id !== "child-prime" || delivery.attempt !== 1) throw new Error("unexpected prime delivery");
      await delivery.ack();
      const drain = await transport.drainState();
      if (drain.pending !== 0 || drain.ackPending !== 0) throw new Error("prime did not double-ACK to drain 0/0");
      primed = true;
      await send({ type: "primed" });
      continue;
    }
    if (delivery.payload.id !== "held-before-os-death") throw new Error("unexpected held delivery");
    await send({
      type: "held", id: delivery.payload.id, sourceMachine: delivery.sourceMachine,
      verification: delivery.verification, attempt: delivery.attempt, sequence: delivery.sequence,
      payloadBase64: Buffer.from(JSON.stringify(delivery.payload), "utf8").toString("base64"),
    });
    // The parent must observe this receipt, SIGKILL this exact birth, and close-join it.
    // No final ACK, WPI, NAK, term, iterator abort, or graceful transport close happens here.
    await new Promise(() => {});
  }
  throw new Error("Node consumer inbox ended without the parent's OS-death injection");
} catch {
  // Deliberately omit exception text: a broker/auth error may include credential identities.
  await send({ type: "error" }).catch(() => undefined);
  process.exitCode = 1;
} finally {
  await transport?.close().catch(() => undefined);
  mesh?.closeState();
  clearTimeout(deadline);
  if (process.connected) process.disconnect();
}
