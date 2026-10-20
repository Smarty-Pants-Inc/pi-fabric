import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";
import { createJiti } from "jiti";

const options = JSON.parse(process.argv[2] ?? "{}");
const emit = (event, detail = {}) => fs.writeSync(1, JSON.stringify({ pid: process.pid, event, ...detail }) + "\n");
const holdPath = options.holdLockPath;
if (holdPath && options.holdLockMs > 0) {
  const rename = fs.renameSync;
  let held = false;
  fs.renameSync = (from, to, ...rest) => {
    const result = rename(from, to, ...rest);
    if (!held && path.resolve(String(to)) === path.resolve(holdPath) && String(from).includes(".pending-")) {
      held = true;
      emit("lock-held", { lockPath: path.resolve(holdPath) });
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, options.holdLockMs);
    }
    return result;
  };
}

const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
let mesh, transport, directory;
let closing = false;
const cleanup = async () => {
  if (closing) return;
  closing = true;
  try { await directory?.close(); } catch (error) { emit("cleanup-error", { message: String(error?.stack ?? error) }); }
  try { await transport?.close(); } catch (error) { emit("cleanup-error", { message: String(error?.stack ?? error) }); }
  try { mesh?.closeState(); } catch (error) { emit("cleanup-error", { message: String(error?.stack ?? error) }); }
};

try {
  const [{ MeshStore }, { ParticipantDirectory }, { openNatsTransport }] = await Promise.all([
    jiti.import("./src/mesh/store.ts"),
    jiti.import("./src/topology/participant-directory.ts"),
    jiti.import("./src/transport/nats/inbox.ts"),
  ]);
  mesh = new MeshStore(options.meshRoot, 65_536, 1_000);
  fs.mkdirSync(options.transportStateDir, { recursive: true, mode: 0o700 });
  transport = await openNatsTransport({ ...options.nats, hostId: options.hostId, name: `projection-worker-${options.index}` });
  const identity = { id: options.hostId, name: `Projection worker ${options.index}`, kind: "main", sessionId: options.hostId };
  directory = new ParticipantDirectory(mesh, {
    enabled: true,
    hostId: options.hostId,
    rootId: options.rootId,
    identity,
    transport,
    transportStateDir: options.transportStateDir,
    heartbeatMs: 60_000,
    leaseMs: 120_000,
  });
  const startedAt = Date.now();
  directory.registerSource(() => [{
    format: 1,
    id: options.hostId,
    kind: "root",
    rootId: options.rootId,
    ownerHostId: options.hostId,
    ownerIdentityId: identity.id,
    ownerIncarnation: directory.ownerIncarnation,
    name: identity.name,
    status: "idle",
    runner: "pi",
    transport: "host",
    capabilities: ["followUp"],
    controlProtocol: "v1",
    startedAt,
    updatedAt: Date.now(),
    sessionId: options.hostId,
  }]);
  await directory.start();
  emit("ready", { hostId: options.hostId });

  const lines = createInterface({ input: process.stdin });
  lines.on("line", async line => {
    let message;
    try { message = JSON.parse(line); } catch { return; }
    if (message?.command !== "close") return;
    await cleanup();
    emit("closed");
    process.exit(0);
  });
  process.once("SIGTERM", async () => {
    await cleanup();
    process.exit(0);
  });
} catch (error) {
  emit("failure", { message: String(error?.stack ?? error) });
  await cleanup();
  process.exit(1);
}
