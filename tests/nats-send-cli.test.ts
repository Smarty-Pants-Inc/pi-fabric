// Expected-skip: the authenticated Path B broker needs nats-server; local runs use the pinned binary when installed.
// This test proves the real CLI process, NATS presence/lease, Fabric control acknowledgement and clean withdrawal.
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { JetStreamApiError, type ConsumerInfo, type JetStreamManager } from "@nats-io/jetstream";
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
  const bytes = Buffer.from(createHash("sha256").update(machine).update("\0").update(path.resolve(creds)).update("\0")
    .update(name).update("\0").update(path.resolve(stateHome)).digest().subarray(0, 16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  return `runtime:${bytes.toString("hex")}`;
};
const UUID_PATTERN_FOR_TEST = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
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

function runCli(
  args: string[], env: NodeJS.ProcessEnv, stdin = "", onFirstLine?: (line: string) => void,
): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(cliRuntime, [cliEntry, "nats", "send", ...args], { cwd: repoRoot, env, stdio: ["pipe", "pipe", "pipe"] });
    child.stdin.end(stdin);
    let stdout = "", stderr = "", firstLineDelivered = false;
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
      stdout += chunk;
      if (onFirstLine && !firstLineDelivered) {
        const newline = stdout.indexOf("\n");
        if (newline >= 0) { firstLineDelivered = true; onFirstLine(stdout.slice(0, newline)); }
      }
    });
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
    catch (error) {
      if (error instanceof JetStreamApiError) {
        let code: number;
        try { code = error.apiError().err_code; } catch { code = error.code; }
        if (code === 10014) return;
      }
      throw error;
    }
    await sleep(50);
  }
  throw new Error(`CLI inbox durable ${durable} remained after shutdown`);
}

const managerWithInfoError = (error: unknown): JetStreamManager => ({
  consumers: { info: async () => { throw error; } },
} as unknown as JetStreamManager);

describe("CLI inbox durable cleanup", () => {
  it("accepts JetStream consumer-not-found as confirmed deletion", async () => {
    const missing = new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
    await expect(waitForConsumerGone(managerWithInfoError(missing), "runtime:test")).resolves.toBeUndefined();
  });

  it.each([
    ["JetStream permission failure", new JetStreamApiError({ code: 403, err_code: 10000, description: "permission denied" })],
    ["connection failure", new Error("connection closed")],
  ] as const)("rethrows %s instead of treating it as deletion", async (_name, error) => {
    await expect(waitForConsumerGone(managerWithInfoError(error), "runtime:test")).rejects.toBe(error);
  });
});

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
    expect(JSON.parse(result.stdout)).toMatchObject({ error: "provide exactly one of --text, --text-file, or --text -" });
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
    { label: "followUp immediate reply", kind: "followUp", inputMode: "argv", awaitReplySeconds: 5, sendReply: true, routeChange: false },
    { label: "followUp changed route reply", kind: "followUp", inputMode: "argv", awaitReplySeconds: 15, sendReply: false, routeChange: true },
    { label: "steer from text file", kind: "steer", inputMode: "file", awaitReplySeconds: undefined, sendReply: false, routeChange: false },
    { label: "followUp reply timeout", kind: "followUp", inputMode: "argv", awaitReplySeconds: 1, sendReply: false, routeChange: false },
    { label: "steer from stdin", kind: "steer", inputMode: "stdin", awaitReplySeconds: undefined, sendReply: false, routeChange: false },
  ] as const;
  it.each(scenarios)("routes and withdraws a $label sender", async ({ kind, inputMode, awaitReplySeconds, sendReply, routeChange }) => {
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
    let replacementPlane: FabricControlPlane | undefined;
    let replacementDirectory: ParticipantDirectory | undefined;
    let replacementTransport: FabricTransport | undefined;
    let replyDelivery: Promise<void> | undefined;
    const received: Array<{ operation: string; targetId: string; text: string | undefined; data: unknown; from: MeshIdentity; senderLive: boolean }> = [];
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
      const targetPlane = new FabricControlPlane(ownerMesh, ownerIdentity, {
        enabled: true, hostId: rootId, transport: ownerTransport, participants: view,
        ownerIncarnation: ownerDirectory.ownerIncarnation, controlIncarnationFence: "enforce",
        expectedNatsInboxStream: "FABRIC_IN_machine-b", relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
        canConsumeMesh: () => ownerDirectory.canConsumeMesh(), pollMs: 20, acknowledgementTimeoutMs: 5_000,
      });
      ownerPlane = targetPlane;
      ownerPlane.start(async (command, from) => {
        const senderRoute = view.resolve(from.id);
        received.push({ operation: command.operation, targetId: command.targetId, text: command.message,
          data: command.data, from, senderLive: senderRoute?.live === true });
        if (sendReply && command.operation === "followUp") {
          const senderId = from.id;
          const correlationId = command.data && typeof command.data === "object" && !Array.isArray(command.data)
            ? (command.data as { replyCorrelationId?: unknown }).replyCorrelationId : undefined;
          if (typeof correlationId !== "string") throw new Error("CLI did not publish a reply correlation id");
          const replyText = `FABRIC-RESULT ${sessionId}\nreply from fixture Main`;
          const deliverReply = async (inReplyTo: string, key: string, text: string): Promise<void> => {
            const route = view.resolve(senderId);
            if (!route?.live) throw new Error("sender route disappeared before its reply");
            await targetPlane.request(route.ownerHostId, senderId, "followUp", {
              message: text, data: { inReplyTo }, ownerIncarnation: route.ownerIncarnation,
            }, route.ownerIdentityId, { idempotencyKey: key, timeoutMs: 5_000 });
          };
          const unrelatedEnv: NodeJS.ProcessEnv = { ...env,
            XDG_STATE_HOME: path.join(fixture.root, `nats-send-unrelated-${sessionId}`) };
          const unrelated = await runCli([
            "--to", senderId, "--kind", "followUp", "--text", "unrelated sender message",
            "--creds", fixture.users.machineA.credsPath, "--ca", fixture.tls!.caFile,
            "--servers", `tls://localhost:${port}`, "--machine", "machine-a",
            "--org", "tristar", "--path-b-machines", "machine-a,machine-b",
            "--name", `unrelated ${sessionId}`, "--idempotency-key", `unrelated-${sessionId}`, "--timeout-ms", "5000",
          ], unrelatedEnv);
          expect(unrelated.signal).toBeNull();
          expect(unrelated.code, `${unrelated.stdout}\n${unrelated.stderr}`).toBe(1);
          expect(unrelated.stderr).toBe("");
          expect(JSON.parse(unrelated.stdout)).toMatchObject({
            error: expect.stringContaining("requested target with matching correlation"),
          });
          await expect(deliverReply("wrong-correlation", `wrong-reply-${sessionId}`, "wrong correlation reply"))
            .rejects.toThrow("requested target with matching correlation");
          // Withhold the original ACK until the CLI receives and acknowledges this real reply.
          replyDelivery = deliverReply(correlationId, `reply-${sessionId}`, replyText);
          await replyDelivery;
        }
        return { accepted: true, messageId: command.commandId };
      });

      const message = `real NATS ${kind} from a non-Pi Node CLI`;
      const textFile = path.join(fixture.root, `message-${sessionId}.txt`);
      if (inputMode === "file") fs.writeFileSync(textFile, message, { mode: 0o600, flag: "wx" });
      const textArgs = inputMode === "file" ? ["--text-file", textFile]
        : inputMode === "stdin" ? ["--text", "-"] : ["--text", message];
      const args = [
        "--to", rootId, "--kind", kind, ...textArgs,
        "--creds", fixture.users.machineA.credsPath, "--ca", fixture.tls!.caFile,
        "--servers", `tls://localhost:${port}`, "--machine", "machine-a",
        "--org", "tristar", "--path-b-machines", "machine-a,machine-b",
        "--name", cliName, "--idempotency-key", `nats-send-${kind}-${sessionId}`, "--timeout-ms", "15000",
        ...(awaitReplySeconds === undefined ? [] : ["--await-reply-seconds", String(awaitReplySeconds)]),
      ];
      inspector = await fixture.connect(fixture.users.bootstrap);
      const admin = await openJetStream(inspector);
      const expectedSenderHostId = cliHostId("machine-a", fixture.users.machineA.credsPath, cliName, stateHome);
      let senderConsumer: ConsumerInfo | undefined;
      let cli: Awaited<ReturnType<typeof runCli>>;
      let routeChangeElapsedMs: number | undefined;
      if (routeChange) {
        let resolveFirstLine!: (line: string) => void;
        const firstLine = new Promise<string>(resolve => { resolveFirstLine = resolve; });
        const cliPromise = runCli(args, env, "", resolveFirstLine);
        const consumerPromise = waitForConsumer(admin.manager, expectedSenderHostId);
        const acknowledgement = JSON.parse(await firstLine) as Record<string, unknown>;
        senderConsumer = await consumerPromise;
        expect(acknowledgement).toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
        const senderId = received[0]!.from.id;
        const correlationId = (received[0]!.data as { replyCorrelationId: string }).replyCorrelationId;
        const originalIncarnation = ownerDirectory.ownerIncarnation;
        await ownerPlane?.close();
        ownerPlane = undefined;
        await ownerDirectory.close();
        await ownerTransport.close();

        const replacementMeshRoot = path.join(fixture.root, `replacement-mesh-${sessionId}`);
        const replacementStateDir = path.join(fixture.root, `replacement-transport-${sessionId}`);
        fs.mkdirSync(replacementMeshRoot, { mode: 0o700 });
        fs.mkdirSync(replacementStateDir, { mode: 0o700 });
        const replacementMesh = new MeshStore(replacementMeshRoot, 64 * 1024, 1_000);
        replacementTransport = await openNatsTransport({
          ...fixture.runtimeOptions(fixture.users.machineB), hostId: rootId, name: `fixture-resumed-${kind}`,
        });
        replacementDirectory = new ParticipantDirectory(replacementMesh, {
          enabled: true, transport: replacementTransport, transportStateDir: replacementStateDir,
          hostId: rootId, rootId, identity: ownerIdentity,
        });
        await replacementDirectory.start();
        await replacementDirectory.publishBeforeLaunch({ ...record, startedAt: Date.now(), updatedAt: Date.now() });
        const replacementView = replacementDirectory.participantView;
        if (!replacementView) throw new Error("replacement NATS view is unavailable");
        const replacementSignal = AbortSignal.timeout(5_000);
        while (!replacementView.resolve(rootId)?.live ||
          replacementView.resolve(rootId)?.ownerIncarnation === originalIncarnation) {
          await replacementView.whenChanged(rootId, replacementSignal);
        }
        replacementPlane = new FabricControlPlane(replacementMesh, ownerIdentity, {
          enabled: true, hostId: rootId, transport: replacementTransport, participants: replacementView,
          ownerIncarnation: replacementDirectory.ownerIncarnation, controlIncarnationFence: "enforce",
          expectedNatsInboxStream: "FABRIC_IN_machine-b", relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
          canConsumeMesh: () => replacementDirectory?.canConsumeMesh() === true,
          pollMs: 20, acknowledgementTimeoutMs: 5_000,
        });
        replacementPlane.start(() => ({ accepted: false, error: "unexpected replacement control request" }));
        await sleep(250); // Let the sender's NATS presence watch observe the committed route change.
        const senderRoute = replacementView.resolve(senderId);
        if (!senderRoute?.live) throw new Error("sender route disappeared during target reconnect");
        const staleReplyStartedAt = Date.now();
        const delivery = replacementPlane.request(senderRoute.ownerHostId, senderId, "followUp", {
          message: "reply from resumed target", data: { inReplyTo: correlationId },
          ownerIncarnation: senderRoute.ownerIncarnation,
        }, senderRoute.ownerIdentityId, { idempotencyKey: `stale-route-${sessionId}`, timeoutMs: 5_000 });
        void delivery.catch(() => undefined);
        cli = await cliPromise;
        routeChangeElapsedMs = Date.now() - staleReplyStartedAt;
      } else {
        const cliPromise = runCli(args, env, inputMode === "stdin" ? message : "");
        senderConsumer = await waitForConsumer(admin.manager, expectedSenderHostId);
        cli = await cliPromise;
      }
      expect(senderConsumer?.config.inactive_threshold).toBe(3_600 * 1_000_000_000);
      expect(cli.signal).toBeNull();
      expect(cli.code, `${cli.stdout}\n${cli.stderr}`).toBe(routeChange ? 1 : awaitReplySeconds === 1 && !sendReply ? 2 : 0);
      expect(cli.stderr).toBe("");
      const outputLines = cli.stdout.trim().split(/\r?\n/).map((line) => JSON.parse(line) as Record<string, unknown>);
      const output = outputLines[0]!;
      expect(output).toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
      expect(typeof output.messageId).toBe("string");
      expect(received).toHaveLength(1);
      expect(received[0]).toMatchObject({ operation: kind, targetId: rootId, text: message, senderLive: true });
      if (sendReply || routeChange) expect(received[0]!.data).toEqual({ replyCorrelationId: expect.stringMatching(UUID_PATTERN_FOR_TEST) });
      expect(received[0]!.from).toMatchObject({ name: `test ${kind}`, kind: "agent" });
      expect(received[0]!.from.id).toMatch(/^provider:cli:[0-9a-f]{32}$/);
      expect(received[0]!.from.sessionId).toMatch(/^[0-9a-f-]{36}$/);
      expect(output.senderId).toBe(received[0]!.from.id);
      if (routeChange) {
        expect(outputLines).toHaveLength(2);
        expect(outputLines[1]).toEqual({ error: `reply from ${rootId} arrived on a changed route; not accepted` });
        expect(routeChangeElapsedMs).toBeLessThan(5_000);
      } else if (awaitReplySeconds !== undefined) {
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
      const monitorTransport = replacementTransport ?? ownerTransport;
      const presenceKv = monitorTransport.kv("presence");
      const leaseKv = monitorTransport.kv("leases");
      await waitForGone(presenceKv, `machine-a.p.${tokenFor(senderId)}`);
      await waitForGone(leaseKv, `machine-a.h.${tokenFor(senderHostId)}`);
      await waitForConsumerGone(admin.manager, senderHostId);
      expect(senderHostId).toBe(expectedSenderHostId);
      expect((replacementDirectory?.participantView ?? view).resolve(senderId)).toBeUndefined();
      if (sendReply) {
        const retry = await runCli(args, env);
        expect(retry.signal).toBeNull();
        expect(retry.code).toBe(2);
        const retryLines = retry.stdout.trim().split(/\r?\n/).map((line) => JSON.parse(line) as Record<string, unknown>);
        expect(retryLines[0]?.messageId).toBe(output.messageId);
        expect(retryLines[1]).toEqual({ reply: null });
        expect(received).toHaveLength(1);
        await waitForGone(presenceKv, `machine-a.p.${tokenFor(senderId)}`);
        await waitForGone(leaseKv, `machine-a.h.${tokenFor(senderHostId)}`);
        await waitForConsumerGone(admin.manager, senderHostId);
      }
    } finally {
      try { await inspector?.close(); } finally {
        try { await replacementPlane?.close(); } finally {
          try { await replacementDirectory?.close(); } finally {
            try { await replacementTransport?.close(); } finally {
              try { await ownerPlane?.close(); } finally {
                try { await ownerDirectory.close(); } finally { await ownerTransport.close(); }
              }
            }
          }
        }
      }
    }
  }, 45_000);
});
