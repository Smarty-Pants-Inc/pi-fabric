// Expected-skip: the authenticated Path B broker needs nats-server; local runs use the pinned binary when installed.
// The target roots publish placementRunId through the same PI_FABRIC_RUN_ID path as a sandbox child.
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PATH_B_OPERATOR_MACHINE } from "../src/mesh/nats-auth/permissions.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import type { FabricTransport, TransportKv } from "../src/transport/types.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { placementRunIdForParticipant } from "../src/topology/placement-run-id.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_POLICY_ENV, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const builtNodeCli = process.env.PI_FABRIC_TEST_DIST_CLI === "1";
const cliEntry = fileURLToPath(new URL(builtNodeCli ? "../dist/cli/index.js" : "../src/cli/index.ts", import.meta.url));
const cliRuntime = builtNodeCli ? "node" : process.versions.bun ? process.execPath : "bun";
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const tokenFor = (id: string) => createHash("sha256").update(id).digest("hex").slice(0, 32);
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
    const child = spawn(cliRuntime, [cliEntry, "nats", "dispatch", ...args], { cwd: repoRoot, env, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

async function waitForGone(kv: TransportKv, key: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (!(await kv.get(key))) return;
    await sleep(50);
  }
  throw new Error("dispatch-owned NATS record remained after process exit");
}

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {}, org: "tristar" });
  await provision(broker);
}, 30_000);

afterAll(async () => { await broker?.stop(); });

brokerSuite("pi-fabric nats dispatch by real placement-run presence", () => {
  it("finds three real Fabric roots and records their acknowledged replies", async () => {
    if (!broker) throw new Error("NATS fixture unavailable");
    const fixture = broker;
    const runIds = ["tristar-burst-one", "tristar-burst-two", "tristar-burst-three"];
    const received: Array<{ runId: string; targetId: string; senderId: string; message: string; senderLive: boolean }> = [];
    const replyDeliveries: Promise<unknown>[] = [];
    const owners: Array<{
      runId: string;
      id: string;
      sessionId: string;
      model: string;
      transport: FabricTransport;
      directory: ParticipantDirectory;
      plane: FabricControlPlane;
    }> = [];
    const priorRunId = process.env.PI_FABRIC_RUN_ID;
    try {
      for (let index = 0; index < runIds.length; index++) {
        const runId = runIds[index]!;
        const sessionId = randomUUID();
        const id = `session:${sessionId}`;
        const identity: MeshIdentity = { id, name: `TriStar child ${index + 1}`, kind: "main", sessionId };
        const meshRoot = path.join(fixture.root, `dispatch-owner-mesh-${index}-${sessionId}`);
        const transportStateDir = path.join(fixture.root, `dispatch-owner-transport-${index}-${sessionId}`);
        fs.mkdirSync(meshRoot, { mode: 0o700 });
        fs.mkdirSync(transportStateDir, { mode: 0o700 });
        const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
        const transport: FabricTransport = await openNatsTransport({
          ...fixture.runtimeOptions(fixture.users.machineB), hostId: id, name: `tristar-root-${index + 1}`,
        });
        const directory = new ParticipantDirectory(mesh, {
          enabled: true, transport, transportStateDir,
          hostId: id, rootId: id, identity,
        });
        process.env.PI_FABRIC_RUN_ID = runId;
        expect(placementRunIdForParticipant("root")).toBe(runId);
        await directory.start();
        const view = directory.participantView;
        if (!view) throw new Error("target NATS view is unavailable");
        try {
          const now = Date.now();
          const record: FabricParticipantRecord = {
            format: 1, id, kind: "root", rootId: id,
            ownerHostId: id, ownerIdentityId: id,
            name: identity.name, status: "running", runner: "pi", transport: "host", interactive: true,
            capabilities: ["steer", "followUp", "fabric"], sessionId, model: `test-model-${index + 1}`,
            cwd: repoRoot, startedAt: now, updatedAt: now, controlProtocol: "v1",
          };
          await directory.publishBeforeLaunch(record);
        } finally {
          if (priorRunId === undefined) delete process.env.PI_FABRIC_RUN_ID;
          else process.env.PI_FABRIC_RUN_ID = priorRunId;
        }
        const rawPresence = await transport.kv("presence").get<Record<string, unknown>>(`machine-b.p.${tokenFor(id)}`);
        expect(rawPresence?.value.placementRunId).toBe(runId);
        const published = directory.list({ scope: "project", kinds: ["root"], fresh: true }).find((participant) => participant.id === id);
        expect(published?.placementRunId).toBe(runId);
        const routeSignal = AbortSignal.timeout(10_000);
        while (!view.resolve(id)?.live) await view.whenChanged(id, routeSignal);
        const plane = new FabricControlPlane(mesh, identity, {
          enabled: true, hostId: id, transport, participants: view,
          ownerIncarnation: directory.ownerIncarnation, controlIncarnationFence: "enforce",
          expectedNatsInboxStream: "FABRIC_IN_machine-b", relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
          canConsumeMesh: () => directory.canConsumeMesh(), pollMs: 20, acknowledgementTimeoutMs: 10_000,
        });
        plane.start((command, from) => {
          if (command.operation !== "followUp" || command.targetId !== id) return { accepted: false, error: "unexpected dispatch target" };
          const senderRoute = view.resolve(from.id);
          if (!senderRoute?.live) return { accepted: false, error: "dispatch sender is not live" };
          received.push({ runId, targetId: command.targetId, senderId: from.id, message: command.message ?? "", senderLive: true });
          replyDeliveries.push(plane.request(senderRoute.ownerHostId, from.id, "followUp", {
            message: `FABRIC-RESULT ${runId}\nanswer-${runId}`, ownerIncarnation: senderRoute.ownerIncarnation,
          }, senderRoute.ownerIdentityId, { idempotencyKey: `result-${runId}`, timeoutMs: 10_000 }));
          return { accepted: true, messageId: command.commandId };
        });
        owners.push({ runId, id, sessionId, model: `test-model-${index + 1}`, transport, directory, plane });
      }
    } finally {
      if (priorRunId === undefined) delete process.env.PI_FABRIC_RUN_ID;
      else process.env.PI_FABRIC_RUN_ID = priorRunId;
    }

    try {
      const taskFile = path.join(fixture.root, "dispatch-task.txt");
      const outFile = path.join(fixture.root, "dispatch-results.jsonl");
      fs.writeFileSync(taskFile, "Complete the assigned task and return the result marker.", { mode: 0o600, flag: "wx" });
      const env: NodeJS.ProcessEnv = {
        ...process.env, ...PATH_B_POLICY_ENV,
        PI_FABRIC_MESH_TRANSPORT: "nats", PI_FABRIC_MESH_HOST_TRANSPORT: "nats",
        XDG_STATE_HOME: path.join(fixture.root, "dispatch-state"),
      };
      delete env.PI_FABRIC_MESH_NATS;
      delete env.PI_FABRIC_RUN_ID;
      delete env.NODE_EXTRA_CA_CERTS;
      delete env.NODE_TLS_REJECT_UNAUTHORIZED;
      delete env.SSL_CERT_FILE;
      delete env.SSL_CERT_DIR;
      const port = new URL(fixture.serverUrl).port;
      const outcomes = await Promise.all(owners.map((owner) => runCli([
        "--run-id", owner.runId, "--task-file", taskFile, "--out", outFile,
        "--creds", fixture.users.machineA.credsPath, "--ca", fixture.tls!.caFile,
        "--servers", `tls://localhost:${port}`, "--machine", "machine-a", "--org", "tristar",
        "--path-b-machines", "machine-a,machine-b", "--find-timeout-seconds", "30", "--await-reply-seconds", "30",
      ], env)));
      expect(outcomes.filter((outcome) => outcome.code !== 0).map((outcome) => ({ code: outcome.code, stderr: outcome.stderr, stdout: outcome.stdout })))
        .toEqual([]);
      expect(outcomes.every((outcome) => outcome.signal === null && outcome.stderr === "")).toBe(true);
      const records = fs.readFileSync(outFile, "utf8").trim().split(/\r?\n/).map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(records).toHaveLength(owners.length);
      const byRun = new Map(records.map((record) => [String(record.runId), record]));
      for (const [index, owner] of owners.entries()) {
        const record = byRun.get(owner.runId);
        if (!record) throw new Error(`missing dispatch record ${owner.runId}`);
        expect(record).toMatchObject({
          runId: owner.runId, childSession: owner.sessionId, model: owner.model,
          answer_ok: true,
          answer_text_sha256: createHash("sha256").update(`FABRIC-RESULT ${owner.runId}\nanswer-${owner.runId}`).digest("hex"),
        });
        for (const field of ["t_task_dispatched", "t_ack", "t_answer_received"] as const) {
          expect(record[field]).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z$/);
        }
        expect(record.model).toBe(`test-model-${index + 1}`);
      }
      expect(received).toHaveLength(owners.length);
      expect(new Set(received.map((item) => item.senderId)).size).toBe(owners.length);
      expect(received.every((item) => item.senderLive && item.message.includes(`agents.followUp(\"${item.senderId}\"`) &&
        item.message.includes(`FABRIC-RESULT ${item.runId}\\n<answer>`))).toBe(true);
      await Promise.all(replyDeliveries);

      const presenceKv = owners[0]!.transport.kv("presence");
      const leaseKv = owners[0]!.transport.kv("leases");
      for (const item of received) {
        const senderHostId = `runtime:${item.senderId.slice("provider:cli:".length)}`;
        await waitForGone(presenceKv, `machine-a.p.${tokenFor(item.senderId)}`);
        await waitForGone(leaseKv, `machine-a.h.${tokenFor(senderHostId)}`);
      }
    } finally {
      for (const owner of [...owners].reverse()) {
        try { await owner.plane.close(); } finally {
          try { await owner.directory.close(); } finally { await owner.transport.close(); }
        }
      }
    }
  }, 120_000);
});
