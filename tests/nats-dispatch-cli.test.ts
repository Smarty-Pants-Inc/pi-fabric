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
  broker = await createPathBHarness({ binary, tls: {}, org: "dispatch-fixture" });
  await provision(broker);
}, 30_000);

afterAll(async () => { await broker?.stop(); });

brokerSuite("pi-fabric nats dispatch by real placement-run presence", () => {
  it.each([
    {
      name: "finds three real Fabric roots and records their acknowledged replies",
      runIds: ["dispatch-fixture-1", "dispatch-fixture-2", "dispatch-fixture-3"],
      replyRunId: (runId: string) => runId,
      expectedCode: 0,
    },
    {
      name: "dedupes a timed-out dispatch retry and refuses changed content or unsafe bindings",
      runIds: ["dispatch-fixture-retry"],
      replyRunId: (_runId: string) => null,
      expectedCode: 2,
    },
    {
      name: "does not attribute a longer run ID reply to dispatch-fixture-1",
      runIds: ["dispatch-fixture-1"],
      replyRunId: (_runId: string) => "dispatch-fixture-10",
      expectedCode: 1,
    },
  ])("$name", async ({ runIds, replyRunId, expectedCode }) => {
    if (!broker) throw new Error("NATS fixture unavailable");
    const fixture = broker;
    const received: Array<{ runId: string; targetId: string; senderId: string; message: string; senderLive: boolean; replyCorrelationId: string }> = [];
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
        const identity: MeshIdentity = { id, name: `Generated fixture child ${index + 1}`, kind: "main", sessionId };
        const meshRoot = path.join(fixture.root, `dispatch-owner-mesh-${index}-${sessionId}`);
        const transportStateDir = path.join(fixture.root, `dispatch-owner-transport-${index}-${sessionId}`);
        fs.mkdirSync(meshRoot, { mode: 0o700 });
        fs.mkdirSync(transportStateDir, { mode: 0o700 });
        const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
        const transport: FabricTransport = await openNatsTransport({
          ...fixture.runtimeOptions(fixture.users.machineB), hostId: id, name: `dispatch-fixture-root-${index + 1}`,
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
          expect(command.data).toMatchObject({ replyCorrelationId: expect.any(String) });
          const replyCorrelationId = (command.data as { replyCorrelationId: string }).replyCorrelationId;
          received.push({ runId, targetId: command.targetId, senderId: from.id, message: command.message ?? "", senderLive: true, replyCorrelationId });
          const resultRunId = replyRunId(runId);
          if (resultRunId !== null) {
            replyDeliveries.push(plane.request(senderRoute.ownerHostId, from.id, "followUp", {
              message: `FABRIC-RESULT ${resultRunId}\nanswer-${runId}`,
              data: { inReplyTo: replyCorrelationId }, ownerIncarnation: senderRoute.ownerIncarnation,
            }, senderRoute.ownerIdentityId, { idempotencyKey: `result-${runId}`, timeoutMs: 10_000 }));
          }
          return { accepted: true, messageId: command.commandId };
        });
        owners.push({ runId, id, sessionId, model: `test-model-${index + 1}`, transport, directory, plane });
      }
    } finally {
      if (priorRunId === undefined) delete process.env.PI_FABRIC_RUN_ID;
      else process.env.PI_FABRIC_RUN_ID = priorRunId;
    }

    try {
      const caseRoot = fs.mkdtempSync(path.join(fixture.root, "dispatch-case-"));
      const taskFile = path.join(caseRoot, "dispatch-task.txt");
      const outFile = path.join(caseRoot, "dispatch-results.jsonl");
      fs.writeFileSync(taskFile, "Complete the assigned task and return the result marker.", { mode: 0o600, flag: "wx" });
      const env: NodeJS.ProcessEnv = {
        ...process.env, ...PATH_B_POLICY_ENV,
        PI_FABRIC_MESH_TRANSPORT: "nats", PI_FABRIC_MESH_HOST_TRANSPORT: "nats",
        XDG_STATE_HOME: path.join(caseRoot, "dispatch-state"),
      };
      delete env.PI_FABRIC_MESH_NATS;
      delete env.PI_FABRIC_RUN_ID;
      delete env.NODE_EXTRA_CA_CERTS;
      delete env.NODE_TLS_REJECT_UNAUTHORIZED;
      delete env.SSL_CERT_FILE;
      delete env.SSL_CERT_DIR;
      const port = new URL(fixture.serverUrl).port;
      const argsFor = (runId: string) => [
        "--run-id", runId, "--task-file", taskFile, "--out", outFile,
        "--creds", fixture.users.machineA.credsPath, "--ca", fixture.tls!.caFile,
        "--servers", `tls://localhost:${port}`, "--machine", "machine-a", "--org", "dispatch-fixture",
        "--path-b-machines", "machine-a,machine-b", "--find-timeout-seconds", "30", "--await-reply-seconds", expectedCode === 2 ? "1" : "30",
      ];
      const outcomes = await Promise.all(owners.map((owner) => runCli(argsFor(owner.runId), env)));
      expect(outcomes.filter((outcome) => outcome.code !== expectedCode).map((outcome) => ({ code: outcome.code, stderr: outcome.stderr, stdout: outcome.stdout })))
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
          answer_ok: expectedCode === 0,
          answer_text_sha256: expectedCode === 2 ? null : createHash("sha256")
            .update(`FABRIC-RESULT ${replyRunId(owner.runId)}\nanswer-${owner.runId}`).digest("hex"),
        });
        for (const field of ["t_task_dispatched", "t_ack", ...(expectedCode === 2 ? [] : ["t_answer_received"])] as const) {
          expect(record[field]).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z$/);
        }
        if (expectedCode === 2) {
          expect(record).toMatchObject({ t_answer_received: null, error: "timed out waiting for the child reply" });
        } else if (expectedCode === 1) {
          expect(record.error).toBe("reply did not start with the expected FABRIC-RESULT marker");
        } else expect(record.error).toBeUndefined();
        expect(record.model).toBe(`test-model-${index + 1}`);
      }
      expect(received).toHaveLength(owners.length);
      expect(new Set(received.map((item) => item.senderId)).size).toBe(owners.length);
      expect(received.every((item) => item.senderLive && item.message.includes(`id: \"${item.senderId}\"`) &&
        item.message.includes(`FABRIC-RESULT ${item.runId}\\n<answer>`) &&
        item.message.includes(`inReplyTo: \"${item.replyCorrelationId}\"`))).toBe(true);
      await Promise.all(replyDeliveries);

      if (expectedCode === 2) {
        const owner = owners[0]!;
        expect(owner.directory.participantView?.resolve(owner.id)?.live).toBe(true);
        const accepted = [...received];
        // Reuse the exact task, sender identity inputs, options, and live target after the acknowledged timeout.
        const retry = await runCli(argsFor(owner.runId), env);
        expect(retry).toMatchObject({ code: 2, signal: null, stderr: "" });
        const retriedRecords = fs.readFileSync(outFile, "utf8").trim().split(/\r?\n/)
          .map((line) => JSON.parse(line) as Record<string, unknown>);
        expect(retriedRecords).toHaveLength(2);
        expect(retriedRecords[1]).toMatchObject({
          runId: owner.runId, childSession: owner.sessionId, model: owner.model,
          answer_ok: false, answer_text_sha256: null, t_answer_received: null,
          error: "timed out waiting for the child reply",
        });
        expect(retriedRecords[1]!.t_task_dispatched).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z$/);
        expect(retriedRecords[1]!.t_ack).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z$/);
        expect(owner.directory.participantView?.resolve(owner.id)?.live).toBe(true);
        expect(received).toEqual(accepted);
        expect(received).toHaveLength(1);

        const bindingDir = path.join(env.XDG_STATE_HOME!, "pi-fabric", "nats-dispatch");
        const bindingFile = path.join(bindingDir, `${createHash("sha256").update(owner.runId).digest("hex")}.json`);
        const bindingBytes = fs.readFileSync(bindingFile, "utf8");
        expect(JSON.parse(bindingBytes)).toEqual({
          format: 1,
          contentSha256: createHash("sha256").update(JSON.stringify([fs.readFileSync(taskFile, "utf8"), owner.id])).digest("hex"),
        });
        expect(fs.readdirSync(bindingDir)).toEqual([path.basename(bindingFile)]);
        expect(fs.statSync(bindingDir).mode & 0o777).toBe(0o700);
        expect(fs.statSync(bindingFile).mode & 0o777).toBe(0o600);

        // A new process and receipt path must not hide a changed task behind the stable run-id key.
        fs.writeFileSync(taskFile, "Complete a different task instead of the previously accepted work.");
        const refusedOut = path.join(caseRoot, "changed-task-results.jsonl");
        const changedArgs = argsFor(owner.runId);
        changedArgs[changedArgs.indexOf("--out") + 1] = refusedOut;
        const changed = await runCli(changedArgs, env);
        expect(changed).toMatchObject({ code: 1, signal: null, stderr: "" });
        const refused = JSON.parse(changed.stdout) as Record<string, unknown>;
        expect(refused).toMatchObject({
          runId: owner.runId, error: "run id reused with different content",
          t_task_dispatched: null, t_ack: null, t_answer_received: null,
          answer_ok: false, answer_text_sha256: null,
        });
        expect(JSON.parse(fs.readFileSync(refusedOut, "utf8"))).toEqual(refused);
        expect(fs.readFileSync(bindingFile, "utf8")).toBe(bindingBytes);

        // Neither malformed state nor a symlink may reset or redirect the binding, even with a live target.
        const symlinkTarget = path.join(caseRoot, "binding-symlink-target.json");
        fs.writeFileSync(symlinkTarget, bindingBytes, { mode: 0o600 });
        for (const unsafe of ["malformed", "symlink"] as const) {
          fs.unlinkSync(bindingFile);
          if (unsafe === "malformed") fs.writeFileSync(bindingFile, "{", { mode: 0o600 });
          else fs.symlinkSync(symlinkTarget, bindingFile);
          const invalid = await runCli(changedArgs, env);
          expect(invalid).toMatchObject({ code: 1, signal: null, stderr: "" });
          expect(JSON.parse(invalid.stdout)).toMatchObject({
            error: "could not safely persist dispatch run binding", t_task_dispatched: null, t_ack: null,
          });
          expect(received).toEqual(accepted);
        }
        expect(fs.readFileSync(symlinkTarget, "utf8")).toBe(bindingBytes);
        expect(owner.directory.participantView?.resolve(owner.id)?.live).toBe(true);
        expect(received).toEqual(accepted);
        expect(received).toHaveLength(1);
      }

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
