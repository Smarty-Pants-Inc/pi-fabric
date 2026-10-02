import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { markUnresolvedWorker } from "../src/storage/retention.js";
import { cancellationError } from "../src/async-settlement.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { RESIDENT_REQUEST_RETENTION_MS } from "../src/residency/request-expiry.js";
import { readResidentRequestDecision, registerResidentCancellation, residentRoot, type ResidentCommand, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";

const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
  expect(predicate()).toBe(true);
};

it.each(["project", "session"] as const)("public actor stop retains its live writer's creation commitment and reconciliation IDs until checked writer exit, then collects once (%s scope)", async scope => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stopped-writer-retention-"));
  const rootId = "session:stopped-writer-retention";
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "stopped-writer-retention", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const host = new ResidentHost(config);
  let client: ResidencyClient | undefined;
  let control: FabricControlPlane | undefined;
  let scanTime: number | undefined;
  let scans = 0;
  const nativeSweep = ResidentRequestRetention.prototype.sweep;
  const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep").mockImplementation(function (this: ResidentRequestRetention, now, live, _budget, stoppedWritersGone) {
    // Advance only the retention clock: no fake worker/lease clocks or private maintenance calls.
    nativeSweep.call(this, scanTime ?? now, live, 10_000, stoppedWritersGone);
    if (scanTime !== undefined) scans++;
  });
  let due: ReturnType<typeof vi.spyOn> | undefined;
  const remove = vi.spyOn(fs, "rmSync");
  try {
    await host.start();
    client = new ResidencyClient({ config, mesh: host.mesh, participants: host.participants, mainAgent: { local: false } as FabricMainAgentTarget });
    control = new FabricControlPlane(host.mesh, { id: rootId, name: "main", kind: "main" },
      { enabled: true, hostId: rootId, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const request = { name: "live stopped writer", instructions: "Reply.", residency: "durable" as const, scope,
      transport: "process" as const, responseMode: "text" as const, delivery: "mailbox" as const };
    const create = vi.spyOn(host.actors, "create");
    const actor = await client.createActor(request, AbortSignal.timeout(5_000));
    const requestId = fs.readdirSync(path.join(config.residencyRoot, "decisions")).map(name => name.slice(0, -5))
      .find(id => readResidentRequestDecision(config.residencyRoot, id)?.operation === "createActor")!;
    const decisionPath = path.join(config.residencyRoot, "decisions", `${requestId}.json`);
    const ackPath = path.join(config.residencyRoot, "acknowledgements", `${requestId}.json`);
    const committed = fs.readFileSync(decisionPath, "utf8");
    const ack = JSON.parse(fs.readFileSync(ackPath, "utf8"));
    const command: ResidentCommand = { format: 3, requestId, rootId, operation: "createActor", request, createdAt: ack.completedAt };
    await control.request(client.hostId, actor.id, "followUp", { message: "HANG_WITH_PROGRESS" }, client.hostId);
    await waitFor(() => host.agents.listForUi().some(run => run.actorId === actor.id && "turns" in run && run.turns > 0));
    const writer = host.agents.listForUi().find(run => run.actorId === actor.id)!;
    expect(writer.status).toBe("running");
    expect(await control.request(client.hostId, actor.id, "stop", {}, client.hostId)).toMatchObject({ acknowledged: true });
    expect(await client.actorStatus(actor.id)).toMatchObject({ id: actor.id, status: "stopped" });
    expect(host.agents.status(writer.id)).toMatchObject({ status: "running", actorId: actor.id, turns: 3 });

    scanTime = ack.acknowledgedAt + RESIDENT_REQUEST_RETENTION_MS + 1;
    due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(true);
    await waitFor(() => scans > 0);
    due.mockReturnValue(false);
    expect(fs.readFileSync(decisionPath, "utf8")).toBe(committed);
    expect(JSON.parse(fs.readFileSync(ackPath, "utf8"))).toEqual(ack);
    const cancelled = new AbortController();
    registerResidentCancellation(cancelled.signal, config.residencyRoot, command);
    cancelled.abort();
    const outcome = cancellationError(cancelled.signal, new Error("outer invocation cancelled")) as Error & { residentOutcome: unknown };
    expect(outcome).toMatchObject({ code: "RESIDENT_REQUEST_EXPIRED", residentOutcome: {
      requestId, state: "committed", expired: true, operation: "createActor", entityKind: "actor", id: actor.id, ownerHostId: client.hostId,
    } });
    expect(outcome.message).toMatch(/do not replay or reassign/i);
    expect(Object.isFrozen(outcome.residentOutcome)).toBe(true);
    expect(await client.actorStatus(actor.id)).toMatchObject({ id: actor.id, status: "stopped", inFlightRun: { id: writer.id } });
    expect(create).toHaveBeenCalledTimes(1);
    expect(fs.readFileSync(decisionPath, "utf8")).toBe(committed);

    // Stop only the original fixture worker through the same public route; stop joins physical exit.
    expect(await control.request(client.hostId, writer.id, "stop", {}, client.hostId)).toMatchObject({ acknowledged: true });
    await waitFor(() => host.actors.inFlightCount() === 0);
    expect(host.agents.status(writer.id).status).toBe("stopped");
    const before = scans;
    scanTime = (scanTime ?? 0) + 60_001;
    due.mockReturnValue(true);
    await waitFor(() => scans > before && !fs.existsSync(decisionPath));
    due.mockReturnValue(false);
    expect(fs.existsSync(ackPath)).toBe(false);
    expect(remove.mock.calls.filter(([file]) => file === decisionPath)).toHaveLength(1);
    const collected = scans;
    scanTime = (scanTime ?? 0) + 60_001;
    due.mockReturnValue(true);
    await waitFor(() => scans > collected);
    due.mockReturnValue(false);
    expect(remove.mock.calls.filter(([file]) => file === decisionPath)).toHaveLength(1);
    expect(create).toHaveBeenCalledTimes(1);
  } finally {
    due?.mockRestore(); sweep.mockRestore(); remove.mockRestore();
    // Every run belongs to this isolated fixture. Join them even if setup failed
    // before the original writer ID was observed; actor close alone can await a detached run.
    for (const run of host.agents?.listForUi() ?? []) await host.agents.stop(run.id);
    await control?.close(); await client?.close(); await host.close();
    vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 20_000);

it("retention ownership uses checked orphan exit evidence, not terminal status, and fails closed on unknown trees", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-orphan-writer-retention-"));
  const runs = path.join(root, "runs");
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: runs });
  const record = (id: string, actorId: string, sessionId: string, transport = "process") => {
    const run = path.join(runs, id);
    fs.mkdirSync(run, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, actorId, transport, sessionId, status: "stopped" }), { mode: 0o600 });
    return run;
  };
  try {
    expect(manager.retentionReferences().size).toBe(0); // Proven absent run root, no writer.
    record("live", "live_actor", String(process.pid));
    let refs = manager.retentionReferences();
    expect(refs.has("live")).toBe(true);
    expect(refs.has("live_actor")).toBe(true); // A terminal file never overrides a live PID.
    record("live", "live_actor", "2147483647");
    expect(manager.retentionReferences().has("live_actor")).toBe(false);
    const unresolved = record("unresolved", "unresolved_actor", "2147483647");
    markUnresolvedWorker(unresolved, "worker exit unconfirmed");
    const nested = path.join(record("parent", "parent_actor", "2147483647"), "nested", "child");
    fs.mkdirSync(nested, { recursive: true });
    markUnresolvedWorker(nested, "nested worker exit unconfirmed");
    record("external", "external_actor", "pane", "tmux");
    record("missing_pid", "missing_pid_actor", "");
    refs = manager.retentionReferences();
    for (const id of ["unresolved_actor", "parent_actor", "external_actor", "missing_pid_actor"]) expect(refs.has(id)).toBe(true);
    fs.mkdirSync(path.join(runs, "unknown"));
    expect(manager.retentionReferences().has("*")).toBe(true);
    fs.rmSync(path.join(runs, "unknown"), { recursive: true });
    const clock = vi.spyOn(performance, "now").mockReturnValueOnce(0).mockReturnValue(10);
    try { expect(manager.retentionReferences().has("*")).toBe(true); } finally { clock.mockRestore(); }
  } finally {
    await manager.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
