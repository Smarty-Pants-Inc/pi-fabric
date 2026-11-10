// Keep reason: FC-AUDIT-001 caught a one-shot CLI inheriting the session runtime's
// infinite first-connect policy. Exercise the real factory, CLI process and TLS sockets.
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { openFabricTransport } from "../src/transport/index.js";
import { openBootstrapNatsConnection, openNatsConnection } from "../src/transport/nats/connection.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { createPathBHarness, PATH_B_POLICY_ENV, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const cliEntry = fileURLToPath(new URL("../src/cli/index.ts", import.meta.url));
const cliRuntime = process.versions.bun ? process.execPath : "bun";
type Closable = { close(): Promise<void> };
let broker: PathBHarness;
let sequence = 0;

function fabricOptions(relay: ReconnectRelay, signal: AbortSignal, retryFirstConnect?: boolean) {
  const root = path.join(broker.root, `caller-mesh-${++sequence}`);
  fs.mkdirSync(root, { mode: 0o700 });
  return {
    config: {
      ...structuredClone(DEFAULT_FABRIC_CONFIG.mesh),
      enabled: true, transport: "nats" as const, hostTransport: "nats" as const, root,
      nats: {
        ...structuredClone(DEFAULT_FABRIC_CONFIG.mesh.nats),
        ...broker.runtimeOptions(broker.users.machineA), servers: relay.url,
      },
    },
    hostId: `first-connect-caller-${sequence}`,
    mesh: new MeshStore(root, 64 * 1024, 1_000),
    signal,
    ...(retryFirstConnect === undefined ? {} : { retryFirstConnect }),
    readyFile: path.join(broker.root, `caller-ready-${sequence}`),
  };
}

async function expectFailFast(start: (signal: AbortSignal) => Promise<Closable>): Promise<void> {
  const owner = new AbortController();
  const guard = new Error("initial connect incorrectly waited for the operation deadline");
  const timer = setTimeout(() => owner.abort(guard), 3_000);
  const started = performance.now();
  let connected: Closable | undefined;
  try {
    const error = await start(owner.signal).then(value => { connected = value; return undefined; }, (cause: unknown) => cause);
    expect(error).toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE", message: "NATS connection failed" });
    expect(owner.signal.aborted, "failure must precede the overall-operation deadline").toBe(false);
    expect(performance.now() - started).toBeLessThan(3_000);
  } finally {
    clearTimeout(timer);
    owner.abort();
    await connected?.close();
  }
}

describe.skipIf(!binary || process.platform === "win32")("NATS first-connect policy belongs to the caller", () => {
  beforeAll(async () => { broker = await createPathBHarness({ binary: binary!, tls: {} }); }, 30_000);
  afterAll(async () => { await broker?.stop(); });

  it.each([
    ["shared transport, omitted", "fabric", undefined],
    ["shared transport, explicit false", "fabric", false],
    ["direct transport, omitted", "direct", undefined],
    ["direct transport, explicit false", "direct", false],
    ["direct connection", "connection", undefined],
    ["bootstrap/admin connection", "bootstrap", undefined],
  ] as const)("fails the initial socket attempt fast for %s", async (_label, kind, retryFirstConnect) => {
    const relay = await ReconnectRelay.create(broker);
    relay.enabled = false;
    try {
      await expectFailFast(signal => {
        if (kind === "fabric") return openFabricTransport(fabricOptions(relay, signal, retryFirstConnect));
        if (kind === "bootstrap") return openBootstrapNatsConnection({
          credentials: broker.users.bootstrap.credsPath, servers: relay.url, tls: { caFile: broker.tls!.caFile }, signal,
        });
        const options = { ...broker.runtimeOptions(broker.users.machineA), servers: relay.url, signal,
          ...(retryFirstConnect === undefined ? {} : { retryFirstConnect }) };
        if (kind === "connection") return openNatsConnection(options);
        return openNatsTransport({ ...options, hostId: "one-shot-direct" });
      });
      expect(relay.connectionCount).toBeGreaterThan(0);
      await relay.whenIdle();
      expect(relay.subjects).not.toContain("$SYS.REQ.USER.INFO");
    } finally { await relay.stop(); }
  }, 8_000);

  it.each(["fabric", "direct"] as const)("keeps explicit %s retry opt-in pending until its owner aborts", async kind => {
    const relay = await ReconnectRelay.create(broker);
    relay.enabled = false;
    const owner = new AbortController();
    const options = fabricOptions(relay, owner.signal, true);
    let state = "pending";
    let connected: Closable | undefined;
    const opening = (kind === "fabric" ? openFabricTransport(options) : openNatsTransport({
      ...broker.runtimeOptions(broker.users.machineA), servers: relay.url, hostId: options.hostId,
      retryFirstConnect: true, signal: owner.signal, readyFile: options.readyFile,
    })).then(value => { state = "connected"; connected = value; return undefined; }, (cause: unknown) => { state = "rejected"; return cause; });
    try {
      // Keep the endpoint down for 3s, then observe another native socket attempt. The
      // unchanged native delay handler can defer that attempt past the 3s boundary.
      await delay(3_000);
      const deadline = Date.now() + 5_000;
      while (relay.connectionCount < 2 && Date.now() < deadline) await delay(25);
      expect(relay.connectionCount).toBeGreaterThan(1);
      expect(state).toBe("pending");
      expect(fs.existsSync(options.readyFile)).toBe(false);
      expect(relay.subjects).not.toContain("$SYS.REQ.USER.INFO");
      const reason = new Error("caller canceled its explicit retry opt-in");
      owner.abort(reason);
      expect(await Promise.race([opening, delay(1_000).then(() => "abort did not settle")])).toBe(reason);
      await relay.whenIdle();
      expect(fs.existsSync(options.readyFile)).toBe(false);
    } finally {
      owner.abort();
      await opening;
      await connected?.close();
      await relay.stop();
    }
  }, 15_000);

  it("keeps an admin-style missing-CA open fail-fast without a TLS preflight", async () => {
    const relay = await ReconnectRelay.create(broker);
    try {
      await expectFailFast(signal => openBootstrapNatsConnection({
        credentials: broker.users.bootstrap.credsPath, servers: relay.url, signal,
      }));
      expect(relay.connectionCount).toBeGreaterThan(0);
      expect(relay.subjects).not.toContain("$SYS.REQ.USER.INFO");
      await relay.whenIdle();
    } finally { await relay.stop(); }
  }, 8_000);

  it("the actual one-shot send CLI reports initial failure, not its overall-operation timeout", async () => {
    const relay = await ReconnectRelay.create(broker);
    relay.enabled = false;
    const env: NodeJS.ProcessEnv = { ...process.env, ...PATH_B_POLICY_ENV,
      XDG_STATE_HOME: path.join(broker.root, "caller-cli-state") };
    for (const name of ["PI_FABRIC_MESH_NATS", "NODE_EXTRA_CA_CERTS", "NODE_TLS_REJECT_UNAUTHORIZED", "SSL_CERT_FILE", "SSL_CERT_DIR"]) delete env[name];
    const started = performance.now();
    try {
      const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }>((resolve, reject) => {
        const child = spawn(cliRuntime, [cliEntry, "nats", "send",
          "--to", "session:00000000-0000-4000-8000-000000000000", "--kind", "followUp", "--text", "must fail initial connect",
          "--creds", broker.users.machineA.credsPath, "--ca", broker.tls!.caFile,
          "--servers", relay.url, "--machine", "machine-a", "--name", "first-connect caller regression", "--timeout-ms", "12000",
        ], { cwd: repoRoot, env, stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "", stderr = "";
        child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
        child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
        child.once("error", reject);
        child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
      });
      expect(result.signal).toBeNull();
      expect(result.code, `${result.stdout}\n${result.stderr}`).toBe(1);
      expect(JSON.parse(result.stdout)).toEqual({ error: "NATS connection failed" });
      expect(performance.now() - started, "must not consume the 12s operation budget").toBeLessThan(6_000);
      expect(relay.connectionCount).toBeGreaterThan(0);
      expect(relay.subjects).not.toContain("$SYS.REQ.USER.INFO");
      await relay.whenIdle();
    } finally { await relay.stop(); }
  }, 20_000);
});
