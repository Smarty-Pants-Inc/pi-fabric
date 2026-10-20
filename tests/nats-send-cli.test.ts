// Expected-skip: the authenticated Path B broker needs nats-server; local runs use the pinned binary when installed.
// This test proves the real CLI process, NATS presence/lease, Fabric control acknowledgement and clean withdrawal.
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ConsumerInfo, JetStreamManager } from "@nats-io/jetstream";
import { PATH_B_OPERATOR_MACHINE } from "../src/mesh/nats-auth/permissions.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import type { FabricTransport, TransportKv } from "../src/transport/types.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, PATH_B_POLICY_ENV, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { transportDurableName, transportInboxStream } from "../src/transport/nats/subjects.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const builtNodeCli = process.env.PI_FABRIC_TEST_DIST_CLI === "1";
const cliEntry = fileURLToPath(new URL(builtNodeCli ? "../dist/cli/index.js" : "../src/cli/index.ts", import.meta.url));
const cliRuntime = builtNodeCli ? "node" : process.versions.bun ? process.execPath : "bun";
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const tokenFor = (id: string) => createHash("sha256").update(id).digest("hex").slice(0, 32);
const cliHostId = (machine: string, creds: string, name: string, stateHome: string): string => {
  const token = createHash("sha256").update(machine).update("\0").update(path.resolve(creds)).update("\0")
    .update(name).update("\0").update(path.resolve(stateHome)).digest().subarray(0, 16).toString("hex");
  return `runtime:${token}`;
};
const brokerSuite = describe.skipIf(!binary || process.platform !== "linux");

let broker: PathBHarness | undefined;

async function provision(fixture: PathBHarness): Promise<void> {
  const environment = fixture.adminEnv(fixture.users.bootstrap);
  const previous = new Map(Object.keys(environment).map((name) => [name, process.env[name]]));
  for (const [name, value] of Object.entries(environment)) process.env[name] = value;
  try {
    const { runTransportAdmin } = await import("../src/transport/nats/admin.js");
    expect(await runTransportAdmin(["provision", "--ca-file", fixture.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally {
    for (const [name, value] of previous) {
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
    }
  }
}

function runCli(args: string[], env: NodeJS.ProcessEnv): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(cliRuntime, [cliEntry, "nats", "send", ...args], { cwd: repoRoot, env, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

async function waitForConsumer(manager: JetStreamManager, hostId: string): Promise<ConsumerInfo> {
  const stream = transportInboxStream("machine-a"), durable = transportDurableName(hostId);
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try { return await manager.consumers.info(stream, durable); }
    catch { await sleep(50); }
  }
  throw new Error(`CLI inbox durable ${durable} was not created`);
}

async function waitForConsumerGone(manager: JetStreamManager, hostId: string): Promise<void> {
  const stream = transportInboxStream("machine-a"), durable = transportDurableName(hostId);
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try { await manager.consumers.info(stream, durable); }
    catch { return; }
    await sleep(50);
  }
  throw new Error(`CLI inbox durable ${durable} remained after shutdown`);
}

async function waitForGone(kv: TransportKv, key: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (!(await kv.get(key))) return;
    await sleep(50);
  }
  throw new Error("CLI-owned NATS record remained after process exit");
}

describe("pi-fabric nats send input", () => {
  it.each([
    ["both", ["--text", "one", "--text-file", "/unused/message.txt"]],
    ["neither", []],
  ] as const)("requires exactly one text source (%s)", async (_case, input) => {
    const env: NodeJS.ProcessEnv = { ...process.env };
    const result = await runCli([
      "--to", "session:00000000-0000-4000-8000-000000000000", "--kind", "followUp",
      "--creds", "/unused/agent.creds", "--ca", "/unused/ca.pem", "--servers", "tls://localhost:4222",
      "--machine", "machine-a", ...input,
    ], env);
    expect(result.code).toBe(1);
    expect(result.signal).toBeNull();
    expect(result.stderr).toBe("");
    expect(JSON.parse(result.stdout)).toMatchObject({ error: "provide exactly one of --text and --text-file" });
  });
});

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {}, org: "tristar" });
  await provision(broker);
}, 30_000);

afterAll(async () => { await broker?.stop(); });

brokerSuite("pi-fabric nats send over a real authenticated broker", () => {
  const scenarios = [
    { label: "followUp reply", kind: "followUp", inputFlag: "--text", awaitReplySeconds: 5, sendReply: true },
    { label: "steer", kind: "steer", inputFlag: "--text-file", awaitReplySeconds: undefined, sendReply: false },
    { label: "followUp reply timeout", kind: "followUp", inputFlag: "--text", awaitReplySeconds: 1, sendReply: false },
  ] as const;
  it.each(scenarios)("routes and withdraws a $label sender", async ({ kind, inputFlag, awaitReplySeconds, sendReply }) => {
    if (!broker) throw new Error("NATS fixture unavailable");
    const fixture = broker;
    const sessionId = randomUUID();
    const rootId = `session:${sessionId}`;
    const ownerIdentity: MeshIdentity = { id: rootId, name: "fixture Main", kind: "main", sessionId };
    const ownerMeshRoot = path.join(fixture.root, `owner-mesh-${sessionId}`);
    const ownerStateDir = path.join(fixture.root, `owner-transport-${sessionId}`);
    fs.mkdirSync(ownerMeshRoot, { mode: 0o700 });
    fs.mkdirSync(ownerStateDir, { mode: 0o700 });
    const ownerMesh = new MeshStore(ownerMeshRoot, 64 * 1024, 1_000);
    const ownerTransport: FabricTransport = await openNatsTransport({
      ...fixture.runtimeOptions(fixture.users.machineB), hostId: rootId, name: `fixture-${kind}`,
    });
    const ownerDirectory = new ParticipantDirectory(ownerMesh, {
      enabled: true, transport: ownerTransport, transportStateDir: ownerStateDir,
      hostId: rootId, rootId, identity: ownerIdentity,
    });
    let ownerPlane: FabricControlPlane | undefined;
    let inspector: Awaited<ReturnType<typeof fixture.connect>> | undefined;
    let replyDelivery: Promise<void> | undefined;
    const received: Array<{ operation: string; targetId: string; text: string | undefined; from: MeshIdentity; senderLive: boolean }> = [];
    try {
      await ownerDirectory.start();
      const record: FabricParticipantRecord = {
        format: 1, id: rootId, kind: "root", rootId,
        ownerHostId: rootId, ownerIdentityId: rootId,
        name: "fixture Main", status: "idle", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp", "fabric"], sessionId,
        startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
      };
      await ownerDirectory.publishBeforeLaunch(record);
      const view = ownerDirectory.participantView;
      if (!view) throw new Error("target NATS view is unavailable");
      const routeSignal = AbortSignal.timeout(5_000);
      while (!view.resolve(rootId)?.live) await view.whenChanged(rootId, routeSignal);
      const targetPlane = new FabricControlPlane(ownerMesh, ownerIdentity, {
        enabled: true, hostId: rootId, transport: ownerTransport, participants: view,
        ownerIncarnation: ownerDirectory.ownerIncarnation, controlIncarnationFence: "enforce",
        expectedNatsInboxStream: "FABRIC_IN_machine-b", relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
        canConsumeMesh: () => ownerDirectory.canConsumeMesh(), pollMs: 20, acknowledgementTimeoutMs: 5_000,
      });
      ownerPlane = targetPlane;
      ownerPlane.start((command, from) => {
        const senderRoute = view.resolve(from.id);
        received.push({ operation: command.operation, targetId: command.targetId, text: command.message,
          from, senderLive: senderRoute?.live === true });
        if (sendReply && command.operation === "followUp") {
          const senderId = from.id;
          const replyText = `FABRIC-RESULT ${sessionId}\nreply from fixture Main`;
          replyDelivery = new Promise<void>((resolve, reject) => {
            setTimeout(() => {
              const route = view.resolve(senderId);
              if (!route?.live) { reject(new Error("sender route disappeared before its reply")); return; }
              void targetPlane.request(route.ownerHostId, senderId, "followUp", {
                message: replyText, ownerIncarnation: route.ownerIncarnation,
              }, route.ownerIdentityId, { idempotencyKey: `reply-${sessionId}`, timeoutMs: 5_000 }).then(() => resolve(), reject);
            }, 100);
          });
        }
        return { accepted: true, messageId: command.commandId };
      });

      const message = `real NATS ${kind} from a non-Pi Node CLI`;
      const textFile = path.join(fixture.root, `message-${sessionId}.txt`);
      if (inputFlag === "--text-file") fs.writeFileSync(textFile, message, { mode: 0o600, flag: "wx" });
      const stateHome = path.join(fixture.root, `nats-send-state-${sessionId}`);
      const cliName = `test ${kind}`;
      const env: NodeJS.ProcessEnv = { ...process.env, ...PATH_B_POLICY_ENV,
        PI_FABRIC_MESH_TRANSPORT: "nats", PI_FABRIC_MESH_HOST_TRANSPORT: "nats",
        XDG_STATE_HOME: stateHome };
      delete env.PI_FABRIC_MESH_NATS;
      delete env.NODE_EXTRA_CA_CERTS;
      delete env.NODE_TLS_REJECT_UNAUTHORIZED;
      delete env.SSL_CERT_FILE;
      delete env.SSL_CERT_DIR;
      const port = new URL(fixture.serverUrl).port;
      const args = [
        "--to", rootId, "--kind", kind, inputFlag, inputFlag === "--text" ? message : textFile,
        "--creds", fixture.users.machineA.credsPath, "--ca", fixture.tls!.caFile,
        "--servers", `tls://localhost:${port}`, "--machine", "machine-a",
        "--org", "tristar", "--path-b-machines", "machine-a,machine-b",
        "--name", cliName, "--idempotency-key", `nats-send-${kind}-${sessionId}`, "--timeout-ms", "15000",
        ...(awaitReplySeconds === undefined ? [] : ["--await-reply-seconds", String(awaitReplySeconds)]),
      ];
      inspector = await fixture.connect(fixture.users.bootstrap);
      const admin = await openJetStream(inspector);
      const expectedSenderHostId = cliHostId("machine-a", fixture.users.machineA.credsPath, cliName, stateHome);
      const cliResult = runCli(args, env);
      const senderConsumer = await waitForConsumer(admin.manager, expectedSenderHostId);
      const cli = await cliResult;
      expect(senderConsumer.config.inactive_threshold).toBe(3_600 * 1_000_000_000);
      expect(cli.signal).toBeNull();
      expect(cli.code, `${cli.stdout}\n${cli.stderr}`).toBe(awaitReplySeconds === 1 && !sendReply ? 2 : 0);
      expect(cli.stderr).toBe("");
      const outputLines = cli.stdout.trim().split(/\r?\n/).map((line) => JSON.parse(line) as Record<string, unknown>);
      const output = outputLines[0]!;
      expect(output).toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
      expect(typeof output.messageId).toBe("string");
      expect(received).toHaveLength(1);
      expect(received[0]).toMatchObject({ operation: kind, targetId: rootId, text: message, senderLive: true });
      expect(received[0]!.from).toMatchObject({ name: `test ${kind}`, kind: "agent" });
      expect(received[0]!.from.id).toMatch(/^provider:cli:[0-9a-f]{32}$/);
      expect(received[0]!.from.sessionId).toMatch(/^[0-9a-f-]{36}$/);
      expect(output.senderId).toBe(received[0]!.from.id);
      if (awaitReplySeconds !== undefined) {
        expect(outputLines).toHaveLength(2);
        if (sendReply) {
          if (!replyDelivery) throw new Error("target did not schedule its followUp reply");
          await replyDelivery;
          expect(outputLines[1]).toMatchObject({
            reply: { from: { id: rootId, name: "fixture Main", kind: "main" },
              text: `FABRIC-RESULT ${sessionId}\nreply from fixture Main`, at: expect.any(Number) },
          });
        } else expect(outputLines[1]).toEqual({ reply: null });
      } else expect(outputLines).toHaveLength(1);

      const senderId = received[0]!.from.id;
      const senderHostId = `runtime:${senderId.slice("provider:cli:".length)}`;
      const presenceKv = ownerTransport.kv("presence");
      const leaseKv = ownerTransport.kv("leases");
      await waitForGone(presenceKv, `machine-a.p.${tokenFor(senderId)}`);
      await waitForGone(leaseKv, `machine-a.h.${tokenFor(senderHostId)}`);
      await waitForConsumerGone(admin.manager, senderHostId);
      expect(senderHostId).toBe(expectedSenderHostId);
      expect(view.resolve(senderId)).toBeUndefined();
      if (sendReply) {
        const retryArgs = [...args];
        const awaitIndex = retryArgs.indexOf("--await-reply-seconds");
        if (awaitIndex >= 0) retryArgs.splice(awaitIndex, 2);
        const retry = await runCli(retryArgs, env);
        expect(retry.signal).toBeNull();
        expect(retry.code).toBe(0);
        expect((JSON.parse(retry.stdout) as Record<string, unknown>).messageId).toBe(output.messageId);
        expect(received).toHaveLength(1);
        await waitForGone(presenceKv, `machine-a.p.${tokenFor(senderId)}`);
        await waitForGone(leaseKv, `machine-a.h.${tokenFor(senderHostId)}`);
        await waitForConsumerGone(admin.manager, senderHostId);
      }
    } finally {
      try { await inspector?.close(); } finally {
        try { await ownerPlane?.close(); } finally {
          try { await ownerDirectory.close(); } finally { await ownerTransport.close(); }
        }
      }
    }
  }, 45_000);
});
