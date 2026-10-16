import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { normalizeAgentRouterConfig } from "../src/agents/router-config.js";
import { recordSpawnRouterStart, recordSpawnRouterStartFailed, routeAgentCreation, type SpawnRouterAdmission, type SpawnRouterRequest } from "../src/agents/spawn-router.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const meshes = new Map<string, MeshStore>();
const root = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-spawn-router-")); roots.push(dir); return dir; };
const admissionFor = (dir: string): SpawnRouterAdmission => {
  let mesh = meshes.get(dir);
  if (!mesh) { mesh = new MeshStore(dir, 64 * 1024, 100, { stateBackend: "file" }); meshes.set(dir, mesh); }
  return { mesh, requesterSessionId: "router-parent", role: "task-agent",
    targetRootId: "session:router-parent", targetHostId: "session:router-parent" };
};
afterEach(() => { for (const mesh of meshes.values()) mesh.closeState(); meshes.clear(); for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 }); vi.restoreAllMocks(); vi.unstubAllEnvs(); });
const pick = { model: "provider/model-b", thinking: "high", reason: "normal.implementation", policyVersion: "1.0.0" };
const defaults = { model: "provider/model-a", thinking: "medium" as const };
const command = (dir: string, body = `process.stdout.write(${JSON.stringify(JSON.stringify(pick))});`): string[] => [
  process.execPath, "-e", `let input = ''; process.stdin.setEncoding('utf8'); process.stdin.on('data', c => input += c); process.stdin.on('end', () => {
    require('node:fs').writeFileSync(process.argv[1], input); ${body}
  });`, path.join(dir, "request.json"),
];
const options = (dir: string) => ({
  config: { command: command(dir), mode: "enforce" as const }, meshRoot: dir,
  kind: "spawn" as const, role: "task-agent", name: "implementation", cwd: dir, project: dir,
  task: "private task 🦉", parentId: "actor:parent", complexity: "normal" as const, defaults,
  explicit: false,
  validateModel: (model: string) => { if (model !== pick.model) throw new Error("private validation detail"); return model; },
});
const unverifiedStart = { launch: { runId: null, pid: null, processStartTime: null }, host: os.hostname(), verified: false, evidenceRef: null };
const decisions = (dir: string): Array<Record<string, any>> => {
  const entries = fs.readFileSync(path.join(dir, "router", "decisions.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
  for (const entry of entries) {
    const keys = entry.kind === "decision"
      ? ["ts", "decisionId", "kind", "spawnKind", "mode", "commandDigest", "decision", "pick", "requested", "actual", "fallbackReason", "latencyMs", "error"]
      : ["ts", "kind", "decisionId", "agentId", "sessionId", "launch", "host", "model", "thinking", "verified", "evidenceRef", ...(entry.kind === "start-failed" || entry.kind === "start-unknown" || entry.kind === "start-refused" ? ["error"] : [])];
    expect(Object.keys(entry).sort()).toEqual(keys.sort());
    if (entry.kind === "decision") {
      expect(entry.actual).toEqual(entry.requested);
      expect(entry.commandDigest).toMatch(/^[a-f0-9]{64}$/);
      expect(entry.fallbackReason).toBe(entry.decision === "default" ? entry.error : null);
    } else expect(Object.keys(entry.launch).sort()).toEqual(["pid", "processStartTime", "runId"]);
  }
  return entries;
};
const request = (dir: string): SpawnRouterRequest => JSON.parse(fs.readFileSync(path.join(dir, "request.json"), "utf8"));

describe("spawn router configuration", () => {
  it("is optional and defaults to off/1500/no task disclosure", () => {
    expect(normalizeFabricConfig({}).agents.router).toBeUndefined();
    expect(normalizeFabricConfig({ agents: { router: { command: [process.execPath, "a b"] } } }).agents.router)
      .toEqual({ command: [process.execPath, "a b"], mode: "off", timeoutMs: 1500, includeTask: false });
  });
  it.each([[0, 200], [199, 200], [5001, 5000], [NaN, 1500], [Infinity, 1500]])("bounds deadline %s to %s", (value, expected) => {
    expect(normalizeAgentRouterConfig({ command: [process.execPath], timeoutMs: value })?.timeoutMs).toBe(expected);
  });
  it.each(["router | shell", [], [""], ["router"], ["./router"], ["../router"], [process.execPath, 1], [process.execPath, "\0"]].map(command => ({ command })))("rejects malformed argv $command without shell repair", ({ command }) => {
    expect(normalizeAgentRouterConfig({ command, mode: "enforce" })?.command).toEqual([]);
  });
  it.each([true, false])("workspace cannot enable/override/disclose task (trusted=%s)", projectTrusted => {
    const cwd = root(); const agentDir = root(); fs.mkdirSync(path.join(cwd, ".pi"));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { router: { command: [process.execPath, "host-router"], mode: "shadow" } } }));
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ agents: { router: { command: [process.execPath, "workspace-router"], mode: "enforce", includeTask: true } } }));
    expect(loadFabricConfig({ cwd, agentDir, projectTrusted }).agents.router)
      .toMatchObject({ command: [process.execPath, "host-router"], mode: "shadow", includeTask: false });
    fs.unlinkSync(path.join(agentDir, "fabric.json"));
    expect(loadFabricConfig({ cwd, agentDir, projectTrusted }).agents.router).toBeUndefined();
  });
});

describe("native spawn router receipts", () => {
  const managerFor = async (dir: string, options: { mainAgentId?: string; hostId?: string } = {}) => {
    const { AgentManager } = await import("../src/agents/manager.js");
    const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
    return new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, ...defaults, budgetUsd: 0, maxConcurrent: 1,
      transport: "process", extensions: false, sessionExport: false }, {
      runRoot: path.join(dir, "runs"), meshRoot: dir,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      mainAgentId: "session:router-parent", hostId: "session:router-parent", identityId: "session:router-parent", ...options,
    });
  };
  type NativeRequest = Parameters<Awaited<ReturnType<typeof managerFor>>["spawn"]>[0] & { spawnRouterDecisionId?: string };
  const spawnAdmitted = (manager: Awaited<ReturnType<typeof managerFor>>, creation: NativeRequest,
    admission: SpawnRouterAdmission = admissionFor(manager.cwd)) => manager.spawn(creation,
      undefined, undefined, undefined, undefined, undefined, undefined, undefined, admission);
  const routeNative = (dir: string,
    creation: NonNullable<Parameters<typeof routeAgentCreation>[0]["admissionRequest"]>,
    overrides: Partial<Parameters<typeof routeAgentCreation>[0]> = {}) => routeAgentCreation({
      ...options(dir), admission: admissionFor(dir), admissionRequest: creation, admissionCwd: dir, ...overrides,
    });
  const expectRefused = (entry: Record<string, any>, decisionId: string,
    error: "decision-unknown" | "decision-replayed" | "decision-mismatch") => {
    expect(entry).toEqual({ ts: expect.any(String), kind: "start-refused", decisionId,
      agentId: null, sessionId: null, model: null, thinking: null, ...unverifiedStart, error });
  };

  it("refuses a consumed decision on a second real native spawn without changing either worker", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private native replay brief 🦉", name: "replay child", ...defaults };
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
      const first = await spawnAdmitted(manager, creation);
      expect(await manager.wait(first.id)).toMatchObject({ status: "completed", ...defaults });
      const second = await spawnAdmitted(manager, creation);
      expect(second.id).not.toBe(first.id);
      expect(await manager.wait(second.id)).toMatchObject({ status: "completed", ...defaults });
      const log = decisions(dir);
      expect(log).toHaveLength(3);
      expect(log[1]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: first.id });
      expectRefused(log[2]!, route.decisionId, "decision-replayed");
      expect(JSON.stringify(log)).not.toContain(admitted.task);
    } finally { await manager.close(); }
  });

  it.each([
    ["task", { task: "different private brief" }],
    ["name", { name: "different child" }],
    ["model", { model: pick.model }],
    ["thinking", { thinking: "low" as const }],
    ["cwd", { cwd: "other" }],
  ] as const)("refuses a different native %s body without burning the valid decision", async (_field, changed) => {
    const dir = root(); const manager = await managerFor(dir); fs.mkdirSync(path.join(dir, "other"));
    try {
      const admitted = { task: "private bound native brief", name: "bound child", ...defaults };
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
      const different = await spawnAdmitted(manager, { ...creation, ...changed });
      expect(await manager.wait(different.id)).toMatchObject({ status: "completed", ...changed,
        ...("cwd" in changed ? { cwd: fs.realpathSync(path.join(dir, changed.cwd)) } : {}) });
      expectRefused(decisions(dir)[1]!, route.decisionId, "decision-mismatch");
      const valid = await spawnAdmitted(manager, creation);
      expect((await manager.wait(valid.id)).status).toBe("completed");
      const log = decisions(dir);
      expect(log).toHaveLength(3);
      expect(log[2]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: valid.id, ...defaults });
      expect(log.filter(entry => entry.kind === "start")).toHaveLength(1);
    } finally { await manager.close(); }
  });

  it.each(["requesterSessionId", "role"] as const)(
    "refuses a different authenticated %s without burning the valid decision", async field => {
      const dir = root(); const manager = await managerFor(dir);
      try {
        const admitted = { task: "private caller-bound brief", ...defaults };
        const admission = admissionFor(dir);
        const route = (await routeNative(dir, admitted, { explicit: true }))!;
        const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
        const different = await spawnAdmitted(manager, creation, { ...admission, [field]: `${admission[field]}-other` });
        expect((await manager.wait(different.id)).status).toBe("completed");
        expectRefused(decisions(dir)[1]!, route.decisionId, "decision-mismatch");
        const valid = await spawnAdmitted(manager, creation, admission);
        expect((await manager.wait(valid.id)).status).toBe("completed");
        expect(decisions(dir)).toHaveLength(3);
        expect(decisions(dir)[2]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: valid.id });
      } finally { await manager.close(); }
    },
  );

  it.each(["targetRootId", "targetHostId"] as const)(
    "refuses a different native %s without burning the valid decision", async field => {
      const dir = root(); const admission = admissionFor(dir); const manager = await managerFor(dir);
      const otherManager = await managerFor(dir, field === "targetRootId"
        ? { mainAgentId: `${admission.targetRootId}-other` } : { hostId: `${admission.targetHostId}-other` });
      try {
        const admitted = { task: "private target-bound brief", ...defaults };
        const route = (await routeNative(dir, admitted, { explicit: true }))!;
        const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
        // Supplying the issued target cannot disguise the receiving manager's actual target.
        const different = await spawnAdmitted(otherManager, creation, admission);
        expect((await otherManager.wait(different.id)).status).toBe("completed");
        expectRefused(decisions(dir)[1]!, route.decisionId, "decision-mismatch");
        expect(admission.mesh.get(`decisions/spawn-router/admissions/${route.decisionId}`, { fresh: true })?.value)
          .toMatchObject({ state: "issued" });
        const valid = await spawnAdmitted(manager, creation, admission);
        expect((await manager.wait(valid.id)).status).toBe("completed");
        const log = decisions(dir);
        expect(log).toHaveLength(3);
        expect(log[2]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: valid.id });
      } finally { await Promise.all([manager.close(), otherManager.close()]); }
    },
  );

  it("caller-supplied target fields cannot override the native manager's configured target", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private supplied target counterexample", ...defaults };
      const admission = admissionFor(dir);
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const child = await spawnAdmitted(manager, { ...admitted, spawnRouterDecisionId: route.decisionId }, {
        ...admission, targetRootId: `${admission.targetRootId}-other`, targetHostId: `${admission.targetHostId}-other`,
      });
      expect((await manager.wait(child.id)).status).toBe("completed");
      const log = decisions(dir);
      expect(log).toHaveLength(2);
      expect(log[1]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: child.id });
    } finally { await manager.close(); }
  });

  it("invalid native admission remains a mismatch without burning the valid decision", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private invalid authority control", ...defaults };
      const admission = admissionFor(dir);
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
      const invalid = await spawnAdmitted(manager, creation, { ...admission, role: "" });
      expect((await manager.wait(invalid.id)).status).toBe("completed");
      expectRefused(decisions(dir)[1]!, route.decisionId, "decision-mismatch");
      const valid = await spawnAdmitted(manager, creation, admission);
      expect((await manager.wait(valid.id)).status).toBe("completed");
      const log = decisions(dir);
      expect(log).toHaveLength(3);
      expect(log[2]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: valid.id });
    } finally { await manager.close(); }
  });

  it.each([false, true])("an unknown ID is refused with native authority=%s while the real worker completes", async authenticated => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const unknown = "00000000-0000-4000-8000-000000000000";
      const creation = { task: "private unknown decision brief", ...defaults, spawnRouterDecisionId: unknown };
      const child = await (authenticated ? spawnAdmitted(manager, creation) : manager.spawn(creation));
      expect(await manager.wait(child.id)).toMatchObject({ status: "completed", ...defaults });
      const log = decisions(dir);
      expect(log).toHaveLength(1);
      expectRefused(log[0]!, unknown, "decision-unknown");
      expect(JSON.stringify(log)).not.toContain(creation.task);
    } finally { await manager.close(); }
  });

  it("request-body authority cannot substitute for the manager's native admission argument", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private forged body authority", ...defaults };
      const admission = admissionFor(dir);
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const creation = { ...admitted, spawnRouterDecisionId: route.decisionId, routerAdmission: admission };
      const forged = await manager.spawn(creation);
      expect((await manager.wait(forged.id)).status).toBe("completed");
      expectRefused(decisions(dir)[1]!, route.decisionId, "decision-unknown");
      const valid = await spawnAdmitted(manager, creation, admission);
      expect((await manager.wait(valid.id)).status).toBe("completed");
      expect(decisions(dir)[2]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: valid.id });
    } finally { await manager.close(); }
  });

  it("two native managers and separate mesh handles consume one persisted decision exactly once", async () => {
    const dir = root(); const firstManager = await managerFor(dir); const secondManager = await managerFor(dir);
    const secondMesh = new MeshStore(dir, 64 * 1024, 100, { stateBackend: "file" });
    try {
      const admitted = { task: "private concurrent native brief", ...defaults };
      const admission = admissionFor(dir);
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
      const children = await Promise.all([
        spawnAdmitted(firstManager, creation, admission),
        spawnAdmitted(secondManager, creation, { ...admission, mesh: secondMesh }),
      ]);
      expect(new Set(children.map(child => child.id)).size).toBe(2);
      const results = await Promise.all([firstManager.wait(children[0]!.id), secondManager.wait(children[1]!.id)]);
      expect(results.map(result => result.status)).toEqual(["completed", "completed"]);
      const log = decisions(dir);
      expect(log).toHaveLength(3);
      const starts = log.filter(entry => entry.kind === "start");
      expect(starts).toHaveLength(1);
      expect(starts[0]).toMatchObject({ decisionId: route.decisionId, ...defaults });
      expect(children.map(child => child.id)).toContain(starts[0]!.agentId);
      const refused = log.filter(entry => entry.kind === "start-refused");
      expect(refused).toHaveLength(1);
      expectRefused(refused[0]!, route.decisionId, "decision-replayed");
    } finally { await Promise.all([firstManager.close(), secondManager.close()]); secondMesh.closeState(); }
  });

  it("issuance and consumption survive closing and reopening the mesh handle", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private persisted admission", ...defaults };
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const key = `decisions/spawn-router/admissions/${route.decisionId}`;
      admissionFor(dir).mesh.closeState(); meshes.delete(dir);
      const admission = admissionFor(dir);
      expect(admission.mesh.get(key, { fresh: true })?.value).toMatchObject({ decisionId: route.decisionId, state: "issued" });
      const creation = { ...admitted, spawnRouterDecisionId: route.decisionId };
      const first = await spawnAdmitted(manager, creation, admission);
      expect((await manager.wait(first.id)).status).toBe("completed");
      admission.mesh.closeState(); meshes.delete(dir);
      const reopened = admissionFor(dir);
      expect(reopened.mesh.get(key, { fresh: true })?.value).toMatchObject({ decisionId: route.decisionId, state: "consumed" });
      const second = await spawnAdmitted(manager, creation, reopened);
      expect((await manager.wait(second.id)).status).toBe("completed");
      expectRefused(decisions(dir)[2]!, route.decisionId, "decision-replayed");
    } finally { await manager.close(); }
  });

  it("refuses an expired persisted issuance at its exact TTL boundary without blocking the native worker", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private expiring admission", ...defaults };
      const admission = admissionFor(dir);
      const route = (await routeNative(dir, admitted, { explicit: true }))!;
      const issued = admission.mesh.get(`decisions/spawn-router/admissions/${route.decisionId}`, { fresh: true })!.value as {
        issuedAt: number; expiresAt: number;
      };
      expect(issued.expiresAt - issued.issuedAt).toBe(10 * 60_000);
      const clock = vi.spyOn(Date, "now").mockReturnValue(issued.expiresAt);
      const child = await (async () => {
        try { return await spawnAdmitted(manager, { ...admitted, spawnRouterDecisionId: route.decisionId }, admission); }
        finally { clock.mockRestore(); }
      })();
      expect((await manager.wait(child.id)).status).toBe("completed");
      const log = decisions(dir);
      expect(log).toHaveLength(2);
      expectRefused(log[1]!, route.decisionId, "decision-unknown");
    } finally { await manager.close(); }
  });

  it.each([false, true])("no-ID native spawn is unchanged with native authority=%s", async authenticated => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const creation = { task: "plain no-ID control", extensions: false, ...defaults };
      const child = await (authenticated ? spawnAdmitted(manager, creation) : manager.spawn(creation));
      expect(await manager.wait(child.id)).toMatchObject({ status: "completed", ...defaults });
      expect(fs.existsSync(path.join(dir, "router"))).toBe(false);
    } finally { await manager.close(); }
  });

  it("the real provider attributes a relative child cwd resolved exactly once by the native manager", async () => {
    const dir = root(); const manager = await managerFor(dir); const childCwd = path.join(dir, "child");
    fs.mkdirSync(childCwd);
    const { AgentsProvider } = await import("../src/providers/agents-provider.js");
    const admission = admissionFor(dir);
    manager.config.router = { command: [], mode: "shadow" };
    const self = { id: admission.targetRootId, rootId: admission.targetRootId, sessionId: admission.requesterSessionId,
      ownerHostId: admission.targetHostId, ownerIdentityId: admission.targetRootId, role: admission.role, kind: "root" };
    const provider = new AgentsProvider(manager, { mesh: admission.mesh, identity: { ...self, kind: "main" } } as any,
      {} as any, { id: self.id, local: true } as any, { self: () => self, scheduleRefresh() {} } as any,
      undefined, {} as any);
    try {
      const creation = { task: "relative cwd control", cwd: "child", model: defaults.model, transport: "process", extensions: false };
      const child = await provider.invoke("spawn", creation, { cwd: dir,
        extensionContext: { modelRegistry: { getAvailable: () => [{ provider: "provider", id: "model-a" }] } }, update() {}, activity() {},
      } as any) as { id: string; cwd: string };
      expect(child.cwd).toBe(fs.realpathSync(childCwd));
      expect(await manager.wait(child.id)).toMatchObject({ status: "completed", cwd: fs.realpathSync(childCwd) });
      const log = decisions(dir);
      expect(log).toHaveLength(2);
      expect(log[1]).toMatchObject({ kind: "start", decisionId: log[0]!.decisionId, agentId: child.id });
    } finally { await manager.close(); }
  });

  it.each(["off", "shadow", "enforce", "explicit"] as const)("%s joins the real manager's applied binding to the real decision ledger", async mode => {
    const dir = root(); const opts = options(dir); const manager = await managerFor(dir);
    try {
      const admitted = { task: "native receipt", extensions: false };
      const route = await routeNative(dir, admitted, { explicit: mode === "explicit",
        config: { ...opts.config, mode: mode === "explicit" ? "enforce" : mode } });
      const creation = { ...admitted, ...route?.selected,
        ...(route ? { spawnRouterDecisionId: route.decisionId } : {}) };
      const child = await spawnAdmitted(manager, creation);
      await manager.wait(child.id);
      if (mode === "off") { expect(fs.existsSync(path.join(dir, "router"))).toBe(false); return; }
      const log = decisions(dir);
      const applied = mode === "enforce" ? { model: pick.model, thinking: pick.thinking } : defaults;
      expect(log).toHaveLength(2);
      expect(log[0]).toMatchObject({ decisionId: route!.decisionId, decision: mode === "explicit" ? "explicit" : mode === "enforce" ? "enforce" : "default" });
      expect(log[1]).toEqual({ ts: expect.any(String), kind: "start", decisionId: route!.decisionId,
        agentId: child.id, sessionId: null, ...applied, host: os.hostname(),
        launch: expect.objectContaining({ runId: child.id, pid: expect.any(Number) }),
        verified: expect.any(Boolean), evidenceRef: log[1]!.evidenceRef });
      expect([null, path.join("runs", child.id, "status.json")]).toContain(log[1]!.evidenceRef);
      if (log[1]!.verified) expect(log[1]!.evidenceRef).not.toBeNull();
      expect(JSON.stringify(log)).not.toContain(creation.task);
    } finally { await manager.close(); }
  });
  it.each([false, true])("verified=%s requires an observed native running status record", async recordFirst => {
    const dir = root(); const manager = await managerFor(dir);
    const { ProcessTransport } = await import("../src/agents/transports/process-transport.js");
    const original = ProcessTransport.prototype.launch;
    const release = path.join(dir, "boot-release");
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: InstanceType<typeof ProcessTransport>, launch) {
      const handle = await original.call(this, launch);
      if (recordFirst) {
        const file = launch.workerArguments[launch.workerArguments.indexOf("--status-file") + 1]!;
        await new Promise<void>((resolve, reject) => {
          const check = () => { if (fs.existsSync(file)) finish(); };
          const watcher = fs.watch(path.dirname(file), check);
          const timer = setTimeout(() => finish(new Error("native status not written")), 10_000);
          function finish(error?: Error) { clearTimeout(timer); watcher.close(); error ? reject(error) : resolve(); }
          watcher.on("error", finish); check();
        });
      }
      return handle;
    });
    try {
      const admitted = { task: recordFirst ? "HANG" : JSON.stringify({ fakeWorkerBootGate: release }) };
      const route = (await routeNative(dir, admitted))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const child = await spawnAdmitted(manager, creation);
      const start = decisions(dir)[1]!;
      expect(start).toMatchObject({ verified: recordFirst, launch: { runId: child.id, pid: expect.any(Number) },
        evidenceRef: recordFirst ? path.join("runs", child.id, "status.json") : null });
      expect(start.launch.processStartTime === null || typeof start.launch.processStartTime === "string").toBe(true);
      if (recordFirst) await manager.stop(child.id);
      else { fs.writeFileSync(release, "ready"); await manager.wait(child.id); }
    } finally { fs.writeFileSync(release, "ready"); await manager.close(); }
  });
  it("start-ledger failure cannot fail or change the native binding", async () => {
    const dir = root(); const manager = await managerFor(dir);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const admitted = { task: "receipt unavailable" };
      const route = (await routeNative(dir, admitted))!;
      const file = path.join(dir, "router", "decisions.jsonl");
      fs.renameSync(file, file + ".saved"); fs.mkdirSync(file);
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const child = await spawnAdmitted(manager, creation);
      expect(child).toMatchObject({ model: pick.model, thinking: pick.thinking });
      expect((await manager.wait(child.id)).status).toBe("completed");
      expect(warning).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
    } finally { await manager.close(); }
  });
  it("records the manager's clamped thinking, not the router's requested thinking", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "clamped receipt", thinkingBounds: { max: "low" as const } };
      const route = (await routeNative(dir, admitted))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const child = await spawnAdmitted(manager, creation); await manager.wait(child.id);
      expect(decisions(dir)[0]).toMatchObject({ requested: { model: pick.model, thinking: pick.thinking }, actual: { model: pick.model, thinking: pick.thinking } });
      expect(decisions(dir)[1]).toMatchObject({ kind: "start", decisionId: route.decisionId, model: pick.model, thinking: "low" });
    } finally { await manager.close(); }
  });
  it("uses an existing native Pi session header, never the process or parent session id", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const sessionFile = path.join(dir, "seed-session.jsonl");
      fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", id: "native-pi-session", cwd: dir, timestamp: new Date().toISOString(), version: 3 }) + "\n");
      const admitted = { task: "seeded receipt", sessionFile };
      const route = (await routeNative(dir, admitted))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const child = await spawnAdmitted(manager, creation); await manager.wait(child.id);
      expect(decisions(dir)[1]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: child.id, sessionId: "native-pi-session" });
    } finally { await manager.close(); }
  });
  it("a queued receipt waits for native launch before recording one start", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const first = await manager.spawn({ task: "HANG", extensions: false });
      const admitted = { task: "queued receipt" };
      const route = (await routeNative(dir, admitted))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const child = await spawnAdmitted(manager, creation);
      expect(child.status).toBe("queued");
      expect(decisions(dir)).toHaveLength(1);
      await manager.stop(first.id);
      await manager.wait(child.id);
      expect(decisions(dir)).toHaveLength(2);
      expect(decisions(dir)[1]).toMatchObject({ kind: "start", decisionId: route.decisionId, agentId: child.id, model: pick.model, thinking: pick.thinking });
    } finally { await manager.close(); }
  });
  it("cancelling a queued admission writes one failure, never a premature start", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      await manager.spawn({ task: "HANG", extensions: false });
      const admitted = { task: "cancelled queued receipt" };
      const route = (await routeNative(dir, admitted))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const child = await spawnAdmitted(manager, creation);
      expect(child.status).toBe("queued"); expect(decisions(dir)).toHaveLength(1);
      await manager.stop(child.id); await manager.wait(child.id);
      const log = decisions(dir);
      expect(log).toHaveLength(2);
      expect(log[1]).toMatchObject({ kind: "start-failed", decisionId: route.decisionId, verified: false, error: "start-failed" });
    } finally { await manager.close(); }
  });
  it("a real manager preparation failure writes exactly one fixed start-failed record", async () => {
    const dir = root(); const manager = await managerFor(dir);
    try {
      const admitted = { task: "private native failure text" };
      const route = (await routeNative(dir, admitted))!;
      vi.spyOn(manager, "prepareModelForAdmission").mockRejectedValue(new Error("private native failure text"));
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      await expect(spawnAdmitted(manager, creation)).rejects.toThrow("private native failure text");
      expect(decisions(dir)).toHaveLength(2);
      expect(decisions(dir)[1]).toEqual({ ts: expect.any(String), kind: "start-failed", decisionId: route.decisionId,
        agentId: null, sessionId: null, model: null, thinking: null, ...unverifiedStart, error: "start-failed" });
      expect(JSON.stringify(decisions(dir))).not.toContain(creation.task);
    } finally { await manager.close(); }
  });
  it("a real unknown native launch writes one privacy-safe start-unknown and retains custody", async () => {
    const dir = root(); const manager = await managerFor(dir);
    const { ProcessTransport } = await import("../src/agents/transports/process-transport.js");
    const original = ProcessTransport.prototype.launch;
    const launched: Array<{ stop(): Promise<void> }> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: InstanceType<typeof ProcessTransport>, launch) {
      const handle = await original.call(this, launch);
      launched.push(handle);
      throw Object.assign(new Error("private launch uncertainty"), { launchOutcome: "unknown" });
    });
    try {
      const admitted = { task: "private unknown native launch", ...defaults };
      const route = (await routeNative(dir, admitted))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const uncertain = await spawnAdmitted(manager, creation);
      expect(uncertain).toMatchObject({ status: "queued", heldPermits: 1 });
      expect(manager.heldPermits()).toBe(1);
      expect(manager.hasRunCustody(uncertain.id)).toBe(true);
      expect(manager.retentionCustodyVeto(uncertain.id)).toBe(true);
      // Unknown custody is queued, not a registered run; use the manager's configured run root.
      expect(fs.existsSync(path.join(dir, "runs", uncertain.id, "unresolved-worker.json"))).toBe(true);
      const waiting = await manager.spawn({ task: "second native launch", transport: "process" });
      expect(waiting.status).toBe("queued");
      expect(spy).toHaveBeenCalledOnce();
      expect(decisions(dir)[1]).toEqual({ ts: expect.any(String), kind: "start-unknown", decisionId: route.decisionId,
        agentId: null, sessionId: null, model: null, thinking: null, ...unverifiedStart, error: "start-unknown" });
      await recordSpawnRouterStart({ meshRoot: dir, decisionId: route.decisionId, agentId: "replay", sessionId: null,
        model: null, thinking: null, admission: admissionFor(dir), spawnKind: "spawn", request: creation, cwd: dir });
      const log = decisions(dir);
      expect(log.filter(entry => entry.kind === "start-unknown")).toHaveLength(1);
      expect(log.filter(entry => entry.kind === "start-failed" || entry.kind === "start")).toHaveLength(0);
      expect(log.at(-1)).toMatchObject({ kind: "start-refused", decisionId: route.decisionId, error: "decision-replayed" });
      await manager.stop(waiting.id);
      await expect(manager.stop(uncertain.id)).rejects.toThrow(/execution exit unconfirmed/);
    } finally {
      spy.mockRestore();
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
      for (const handle of launched) await handle.stop();
    }
  });

  it.each(["provider", "manager"] as const)("a real public provider %s preparation failure writes one start-failed and no replay refusal", async stage => {
    const dir = root(); const manager = await managerFor(dir);
    const { AgentsProvider } = await import("../src/providers/agents-provider.js");
    const admission = admissionFor(dir);
    manager.config.router = { command: [], mode: "shadow" };
    const self = { id: admission.targetRootId, rootId: admission.targetRootId, sessionId: admission.requesterSessionId,
      ownerHostId: admission.targetHostId, ownerIdentityId: admission.targetRootId, role: admission.role, kind: "root" };
    const provider = new AgentsProvider(manager, { mesh: admission.mesh, identity: { ...self, kind: "main" } } as any,
      {} as any, { id: self.id, local: true } as any, { self: () => self, scheduleRefresh() {} } as any,
      undefined, {} as any);
    const failure = new Error("private public spawn preparation failure");
    const spawn = vi.spyOn(manager, "spawn");
    const preparation = stage === "provider"
      ? vi.spyOn(manager, "resolveKernel").mockImplementation(() => { throw failure; })
      : vi.spyOn(manager, "prepareModelForAdmission").mockRejectedValue(failure);
    try {
      const creation = { task: "private public provider failure task", model: defaults.model, transport: "process", extensions: false };
      await expect(provider.invoke("spawn", creation, { cwd: dir,
        extensionContext: { modelRegistry: { getAvailable: () => [{ provider: "provider", id: "model-a" }] } }, update() {}, activity() {},
      } as any)).rejects.toBe(failure);
      expect(preparation).toHaveBeenCalledOnce();
      expect(spawn).toHaveBeenCalledTimes(stage === "manager" ? 1 : 0);
      const log = decisions(dir);
      expect(log).toHaveLength(2);
      expect(log[0]).toMatchObject({ kind: "decision", spawnKind: "spawn" });
      expect(log[1]).toEqual({ ts: expect.any(String), kind: "start-failed", decisionId: log[0]!.decisionId,
        agentId: null, sessionId: null, model: null, thinking: null, ...unverifiedStart, error: "start-failed" });
      expect(log.filter(entry => entry.kind === "start-failed")).toHaveLength(1);
      expect(log.filter(entry => entry.kind === "start-refused")).toHaveLength(0);
      expect(admission.mesh.get(`decisions/spawn-router/admissions/${log[0]!.decisionId}`, { fresh: true })?.value)
        .toMatchObject({ state: "consumed" });
      expect(JSON.stringify(log)).not.toContain(creation.task);
      expect(JSON.stringify(log)).not.toContain(failure.message);
    } finally { await manager.close(); }
  });
  it.each(["shadow", "enforce", "default", "explicit"] as const)("%s actor creation records its binding without inventing a Pi session id or pinning defaults", async mode => {
    const dir = root(); const manager = await managerFor(dir);
    const { ActorManager } = await import("../src/actors/manager.js");
    const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
    const mesh = admissionFor(dir).mesh;
    const actors = new ActorManager("router-parent", { id: "session:router-parent", name: "main", kind: "main", sessionId: "router-parent" },
      mesh, DEFAULT_FABRIC_CONFIG.mesh, manager, () => {}, { actorRoot: path.join(dir, "actors"), persistent: false, reapDeadSessionPresence: false });
    try {
      const opts = options(dir);
      const admitted = { name: "receipt actor", instructions: "private actor task" };
      const route = (await routeNative(dir, admitted, { kind: "actor", explicit: mode === "explicit",
        config: { ...opts.config, mode: mode === "shadow" ? "shadow" : "enforce", ...(mode === "default" ? { command: [] } : {}) } }))!;
      const creation = { ...admitted, ...route.selected, spawnRouterDecisionId: route.decisionId };
      const actor = await actors.create(creation, { routerAdmission: admissionFor(dir) });
      expect(decisions(dir)).toHaveLength(2);
      expect(decisions(dir)[1]).toEqual({ ts: expect.any(String), kind: "start", decisionId: route.decisionId, agentId: actor.id,
        sessionId: null, ...unverifiedStart, ...(mode === "enforce" ? { model: pick.model, thinking: pick.thinking } : defaults) });
      if (mode === "explicit") {
        const refused = (await routeNative(dir, admitted, { kind: "actor", explicit: true }))!;
        const duplicate = { ...creation, spawnRouterDecisionId: refused.decisionId };
        await expect(actors.create(duplicate, { routerAdmission: admissionFor(dir) })).rejects.toThrow();
        const log = decisions(dir);
        expect(log).toHaveLength(4);
        expect(log[3]).toMatchObject({ kind: "start-failed", decisionId: refused.decisionId, verified: false, error: "start-failed" });
      }
      if (mode !== "enforce") {
        expect(actors.definition(actor.id).model).toBeUndefined();
        expect(actors.definition(actor.id).thinking).toBeUndefined();
      }
      expect(JSON.stringify(decisions(dir))).not.toContain(creation.instructions);
    } finally { await actors.close(); await manager.close(); }
  });
});

describe("durable provider spawn router handoff receipts", () => {
  it.each(["spawn", "create"] as const)("real client %s startup failure stays before the publication callback", async action => {
    const dir = root();
    const { ResidencyClient } = await import("../src/residency/client.js");
    const identity = { id: "session:client-probe", name: "main", kind: "root", sessionId: "client-probe",
      rootId: "session:client-probe", ownerHostId: "session:client-probe", ownerIdentityId: "session:client-probe" };
    const client = new ResidencyClient({ config: { rootId: identity.id, sessionId: identity.sessionId,
      meshRoot: dir, residencyRoot: path.join(dir, "residency"), cwd: dir, mainName: "main", mainStartedAt: 1,
      agents: { runner: "pi", defaultTools: [] } }, mesh: {}, participants: { self: () => identity },
      mainAgent: { id: identity.id, local: true } } as any);
    const failure = new Error("private host startup failure");
    const startup = vi.spyOn(client, "ensureHost").mockRejectedValue(failure);
    const onDispatched = vi.fn();
    await expect(action === "spawn" ? client.spawnAgent({ task: "no dispatch", residency: "durable" }, undefined, onDispatched)
      : client.createActor({ name: "no dispatch", instructions: "no dispatch", residency: "durable" }, undefined, onDispatched)).rejects.toBe(failure);
    expect(startup).toHaveBeenCalledOnce(); expect(onDispatched).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(dir, "residency", "requests"))).toBe(false);
  });
  it("nested actor client rejects a missing host before publication custody transfers", async () => {
    const dir = root();
    const { ResidentActorClient } = await import("../src/residency/actor-client.js");
    const client = new ResidentActorClient(dir, "session:missing-host");
    const onDispatched = vi.fn();
    await expect(client.createActor({ name: "no dispatch", instructions: "no dispatch", residency: "durable" },
      undefined, undefined, onDispatched)).rejects.toThrow("Root resident host is not live");
    expect(onDispatched).not.toHaveBeenCalled();
    expect(fs.readdirSync(dir)).toEqual([]);
  });
  // Recorded public-provider boundary regression: no resident, worker, or native
  // manager is started. The receiving side still uses the real ledger writer.
  it.each((["spawn", "create"] as const).flatMap(action =>
    (["before", "rejected", "uncertain"] as const).map(stage => ({ action, stage })),
  ))("$action/$stage records exactly one outcome at the dispatch fence", async ({ action, stage }) => {
    const dir = root();
    const { AgentsProvider } = await import("../src/providers/agents-provider.js");
    const { AgentManager } = await import("../src/agents/manager.js");
    const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
    // The fake receiver cannot execute durable work, even in a confined runner.
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", "");
    const identity = { id: "session:receipt-probe", kind: "main", name: "main", sessionId: "receipt-probe" };
    const { residentHostId } = await import("../src/residency/protocol.js");
    const admission = { ...admissionFor(dir), requesterSessionId: identity.sessionId,
      targetRootId: identity.id, targetHostId: residentHostId(identity.id) };
    const config = { ...DEFAULT_FABRIC_CONFIG.agents };
    delete config.model;
    const manager = new AgentManager(dir, {
      ...config, thinking: "medium",
      router: { command: [], mode: "shadow", timeoutMs: 200, includeTask: false },
    }, { runRoot: path.join(dir, "runs"), meshRoot: dir });
    const failure = new Error(stage === "before" ? "local client preparation rejected before dispatch"
      : stage === "rejected" ? "native receiver rejected admission" : "response uncertain after native admission");
    const receive = vi.fn(async (creation: NonNullable<Parameters<typeof routeAgentCreation>[0]["admissionRequest"]> & { spawnRouterDecisionId?: string }, _signal?: AbortSignal, onDispatched?: () => void) => {
      const decisionId = creation.spawnRouterDecisionId;
      expect(decisionId).toEqual(expect.any(String));
      if (stage === "before") throw failure;
      expect(onDispatched).toEqual(expect.any(Function));
      onDispatched!();
      const attribution = { meshRoot: dir, decisionId: decisionId!, admission,
        spawnKind: action === "spawn" ? "spawn" as const : "actor" as const, request: creation, cwd: dir };
      if (stage === "rejected") await recordSpawnRouterStartFailed(attribution);
      else await recordSpawnRouterStart({ ...attribution, agentId: "native-record", sessionId: null,
        model: "provider/applied", thinking: "low" });
      throw failure;
    });
    const residency = { spawnAgent: receive, createActor: receive };
    const participants = { self: () => ({ ...identity, kind: "root", rootId: identity.id, ownerHostId: identity.id,
      ownerIdentityId: identity.id, cwd: dir, role: "task-agent" }), scheduleRefresh() {} };
    // As in the recorded probe, unused actor/lifecycle services are inert: no
    // timer-owning managers or disposal paths are needed for rejected calls.
    const provider = new AgentsProvider(manager, { mesh: admission.mesh, identity } as any, {} as any,
      { id: identity.id, local: true } as any, participants as any, undefined, {} as any, undefined, residency as any);
    const privateName = "private durable receipt name";
    const privateTask = "private durable receipt task 🦉";
    const args = action === "spawn" ? { name: privateName, task: privateTask, residency: "durable" }
      : { name: privateName, instructions: privateTask, residency: "durable" };
    await expect(provider.invoke(action, args, {
      cwd: dir, extensionContext: { modelRegistry: { getAvailable: () => [] } }, update() {}, activity() {},
    } as any)).rejects.toBe(failure);
    expect(receive).toHaveBeenCalledTimes(1);
    const log = decisions(dir);
    expect(log).toHaveLength(2);
    expect(log.filter(entry => entry.kind === "decision")).toHaveLength(1);
    expect(log.filter(entry => entry.kind !== "decision")).toHaveLength(1);
    const decisionId = log[0]!.decisionId;
    expect(decisionId).toMatch(/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/);
    expect(receive.mock.calls[0]![0].spawnRouterDecisionId).toBe(decisionId);
    expect(log[0]).toMatchObject({ kind: "decision", spawnKind: action === "spawn" ? "spawn" : "actor" });
    expect(log[1]).toEqual(stage === "uncertain"
      ? { ts: expect.any(String), kind: "start", decisionId, agentId: "native-record", sessionId: null,
        model: "provider/applied", thinking: "low", ...unverifiedStart }
      : { ts: expect.any(String), kind: "start-failed", decisionId, agentId: null, sessionId: null,
        model: null, thinking: null, ...unverifiedStart, error: "start-failed" });
    const ledger = fs.readFileSync(path.join(dir, "router", "decisions.jsonl"), "utf8");
    expect(ledger).not.toContain(privateName);
    expect(ledger).not.toContain(privateTask);
    expect(ledger).not.toContain(failure.message);
  });
});

describe("external spawn router behavior", () => {
  it("off starts no command and creates no ledger", async () => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, mode: "off" } })).toBeUndefined();
    expect(fs.existsSync(path.join(dir, "request.json"))).toBe(false);
    expect(fs.existsSync(path.join(dir, "router"))).toBe(false);
  });
  it.each(["node", "./router", "../router"])("runtime rejects non-absolute executable %s without PATH lookup", async executable => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: [executable, ...command(dir).slice(1)] } })).toMatchObject({ ...defaults, decisionId: expect.any(String) });
    expect(fs.existsSync(path.join(dir, "request.json"))).toBe(false);
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: "invalid-command" });
  });
  it.each([...new Set([process.platform, "win32"])])("passes only the minimal explicit environment, never credentials or loader hooks (%s)", async platform => {
    const dir = root(); const opts = options(dir);
    for (const name of ["SMARTY_AUTH", "GITHUB_TOKEN", "MY_TOKEN", "API_KEY", "PI_CODING_AGENT_DIR", "NODE_OPTIONS", "LD_PRELOAD", "COMSPEC", "PATHEXT"]) {
      vi.stubEnv(name, "must-not-reach-router");
    }
    vi.stubEnv("PATH", dir); vi.stubEnv("HOME", "/router-home"); vi.stubEnv("LANG", "C"); vi.stubEnv("TZ", "UTC");
    const systemRoot = Object.entries(process.env).find(([key]) => key.toLowerCase() === "systemroot")?.[1] ?? "C:\\Windows";
    const essentials = {
      HOMEDRIVE: "C:", HOMEPATH: "\\router-home", LOGONSERVER: "\\\\router-host", SYSTEMDRIVE: "C:",
      TEMP: dir, USERDOMAIN: "router-domain", USERNAME: "router-user", USERPROFILE: dir, WINDIR: systemRoot,
    };
    // Exercise case-insensitive Windows lookup without leaking these keys on POSIX.
    for (const [name, value] of Object.entries({ ...essentials, SYSTEMROOT: systemRoot })) {
      const keys = Object.keys(process.env).filter(key => key.toUpperCase() === name);
      for (const key of keys.length ? keys : [name.toLowerCase()]) vi.stubEnv(key, value);
    }
    const body = `require('node:fs').writeFileSync(process.argv[1] + '.env', JSON.stringify(process.env)); process.stdout.write(${JSON.stringify(JSON.stringify(pick))});`;
    const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    let routed: ReturnType<typeof routeAgentCreation>;
    try {
      // Inject only while the hook constructs env/spawn options; restore before
      // asynchronous process retirement so a Linux child is retired on Linux.
      Object.defineProperty(process, "platform", { ...descriptor, value: platform });
      routed = routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } });
    } finally { Object.defineProperty(process, "platform", descriptor); }
    expect(await routed).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    const platformEnv = platform === "win32" ? {
      ...essentials, SystemRoot: systemRoot,
      PATH: `${path.win32.join(systemRoot, "System32")};${systemRoot}`,
      COMSPEC: path.win32.join(systemRoot, "System32", "cmd.exe"),
    } : { PATH: "/usr/bin:/bin" };
    expect(JSON.parse(fs.readFileSync(path.join(dir, "request.json.env"), "utf8"))).toEqual({ ...platformEnv, HOME: "/router-home", LANG: "C", TZ: "UTC" });
  });
  it("shadow preserves default and logs validated pick beside actual", async () => {
    const dir = root(); const opts = options(dir);
    const result = await routeAgentCreation({ ...opts, config: { ...opts.config, mode: "shadow" } });
    expect(result).toEqual(expect.objectContaining({ ...defaults, decisionId: expect.any(String) }));
    expect(decisions(dir)).toEqual([expect.objectContaining({ pick, actual: defaults, error: null, mode: "shadow", decision: "default", decisionId: result!.decisionId })]);
  });
  it("enforce selects validated model/thinking and emits one private decision", async () => {
    const dir = root(); const opts = options(dir);
    const result = await routeAgentCreation(opts);
    expect(result).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    const req = request(dir);
    expect(req).toMatchObject({ kind: "spawn", role: "task-agent", name: "implementation", cwd: dir, project: dir,
      parentId: "actor:parent", requestedComplexity: "normal", defaults,
      taskDigest: createHash("sha256").update(opts.task).digest("hex"), taskLength: Buffer.byteLength(opts.task), host: os.hostname() });
    expect(req).not.toHaveProperty("task");
    const log = decisions(dir);
    expect(log).toHaveLength(1);
    expect(log[0]).toMatchObject({ actual: { model: pick.model, thinking: pick.thinking }, pick, error: null, decision: "enforce" });
    expect(log[0]!.commandDigest).toBe(createHash("sha256").update(JSON.stringify(opts.config.command)).digest("hex"));
    expect(log[0]).toMatchObject({ kind: "decision", spawnKind: "spawn", requested: { model: pick.model, thinking: pick.thinking } });
    expect(log[0]!.ts).toMatch(/^\d{4}-/);
    expect(log[0]!.latencyMs).toBeGreaterThanOrEqual(0);
    expect(JSON.stringify(log)).not.toContain(opts.task);
  });
  it.each(["simple", "normal", "complex", "delicate"] as const)("forwards %s complexity verbatim to router stdin", async complexity => {
    const dir = root();
    expect(await routeAgentCreation({ ...options(dir), complexity })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(request(dir).requestedComplexity).toBe(complexity);
  });
  it("full task reaches stdin only with includeTask true, never the ledger", async () => {
    const dir = root(); const opts = options(dir);
    await routeAgentCreation({ ...opts, config: { ...opts.config, includeTask: true } });
    expect(request(dir).task).toBe(opts.task);
    expect(JSON.stringify(decisions(dir))).not.toContain(opts.task);
  });
  it.each(["policy:task/normal", "policy:task/tier-1", `policy:task/${"a".repeat(32)}`, "capacity-hold", "taskclass:implementation", "taskclass:code-1", `taskclass:${"a".repeat(32)}`, "default", "unavailable", "normal.implementation"])("preserves structured router reason %s", async reason => {
    const dir = root(); const opts = options(dir);
    const body = `process.stdout.write(${JSON.stringify(JSON.stringify({ ...pick, reason }))});`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(decisions(dir)[0]).toMatchObject({ pick: { ...pick, reason }, error: null });
  });
  it.each(["normal.policy", "normal.implementation:secret-token", "normal.implementation\n", "policy", "policy:task/", `policy:task/${"a".repeat(33)}`, "policy:task/UPPERCASE", "taskclass:", "taskclass:secret words with spaces", `taskclass:${"a".repeat(33)}`, "taskclass:private_task", "taskclass:normal\r\n", "capacity-hold\u2028", "default\u2029", "a".repeat(100), "private task 🦉", "UPPERCASE", ""])("maps unrecognized reason %s to other", async reason => {
    const dir = root(); const opts = options(dir);
    const body = `process.stdout.write(${JSON.stringify(JSON.stringify({ ...pick, reason }))});`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, includeTask: true, command: command(dir, body) } })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(decisions(dir)[0]).toMatchObject({ pick: { reason: "other", policyVersion: pick.policyVersion }, error: null });
    expect(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8")).not.toContain(JSON.stringify(reason));
  });
  it.each(["v1", "v1-2026.10.09", "v12-release.1", `v1-${"a".repeat(32)}`, `v${"1".repeat(40)}`, "1.2", "1.2.3", `${"1".repeat(37)}.1.0`, "abcdef0", "a".repeat(40)])("preserves structured router version %s", async policyVersion => {
    const dir = root(); const opts = options(dir);
    const body = `process.stdout.write(${JSON.stringify(JSON.stringify({ ...pick, policyVersion }))});`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(decisions(dir)[0]).toMatchObject({ pick: { ...pick, policyVersion }, error: null });
  });
  it.each(["v", "v1-", `v1-${"a".repeat(33)}`, "v1-secret words with spaces", "v1-2026.10.09\n", "v1-UPPERCASE", "2026-10-v1", "abcdef", "a".repeat(41), "ABCDEF0123456789", "private task 🦉", "1", "1.2.3.4", "1.2.3-secret", "1.2\n", "abcdef0\r\n", "1.2\u2028", "1.2\u2029", " 1.2", "1.2 ", ""])("maps invalid version %s to unknown without truncation", async policyVersion => {
    const dir = root(); const opts = options(dir);
    const body = `process.stdout.write(${JSON.stringify(JSON.stringify({ ...pick, policyVersion }))});`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(decisions(dir)[0]).toMatchObject({ pick: { reason: pick.reason, policyVersion: "unknown" }, error: null });
    expect(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8")).not.toContain(JSON.stringify(policyVersion));
  });
  it.each((["shadow", "enforce"] as const).flatMap(mode => ["secret-token", "taskclass:secret words with spaces", `taskclass:${"a".repeat(33)}`].map(fragment => ({ mode, fragment }))))("never records task-secret fragment $fragment echoed through either metadata field in $mode mode", async ({ mode, fragment }) => {
    const dir = root(); const opts = { ...options(dir), task: `deploy ${fragment}` };
    const logs = [vi.spyOn(console, "log"), vi.spyOn(console, "warn"), vi.spyOn(console, "error")].map(spy => spy.mockImplementation(() => {}));
    const body = `const fragment = JSON.parse(input).task.slice('deploy '.length); process.stdout.write(JSON.stringify({ model: '${pick.model}', thinking: '${pick.thinking}', reason: fragment, policyVersion: fragment }));`;
    const result = await routeAgentCreation({ ...opts, config: { ...opts.config, mode, includeTask: true, command: command(dir, body) } });
    expect(result).toEqual(mode === "enforce" ? expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }) : expect.objectContaining({ ...defaults, decisionId: expect.any(String) }));
    expect(request(dir).task).toBe(opts.task);
    expect(decisions(dir)[0]).toMatchObject({ pick: { reason: "other", policyVersion: "unknown" }, error: null });
    expect(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8")).not.toContain(fragment);
    for (const log of logs) expect(JSON.stringify(log.mock.calls)).not.toContain(fragment);
  });
  it.each(["secret-task", "private task 🦉"])("redacts task text echoed through router metadata (%s)", async task => {
    const dir = root(); const opts = { ...options(dir), task };
    const body = `const task = JSON.parse(input).task; process.stdout.write(JSON.stringify({ model: '${pick.model}', thinking: '${pick.thinking}', reason: task, policyVersion: task }));`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, includeTask: true, command: command(dir, body) } })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(decisions(dir)[0]).toMatchObject({ pick: { reason: "other", policyVersion: "unknown" }, error: null });
    expect(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8")).not.toContain(task);
  });
  it("explicit model/thinking starts no router but logs the actual binding", async () => {
    const dir = root();
    const result = await routeAgentCreation({ ...options(dir), explicit: true });
    expect(result).toEqual(expect.objectContaining({ ...defaults, decisionId: expect.any(String) }));
    expect(fs.existsSync(path.join(dir, "request.json"))).toBe(false);
    expect(decisions(dir)).toEqual([expect.objectContaining({ decision: "explicit", pick: null, actual: defaults, error: null, decisionId: result!.decisionId })]);
  });
  it.each(["shadow", "enforce"] as const)("writes one applied start line joined by decisionId in %s mode", async mode => {
    const dir = root();
    const admission = admissionFor(dir);
    const admitted = { task: "applied start" };
    const result = await routeAgentCreation({ ...options(dir), config: { ...options(dir).config, mode },
      admission, admissionRequest: admitted });
    expect(result).toBeDefined();
    await recordSpawnRouterStart({ meshRoot: dir, decisionId: result!.decisionId, agentId: "agent-1", sessionId: null,
      model: result!.model ?? null, thinking: result!.thinking, admission, spawnKind: "spawn",
      request: { ...admitted, ...result!.selected }, cwd: dir });
    const log = decisions(dir);
    expect(log).toHaveLength(2);
    expect(log[1]).toEqual({ ts: expect.any(String), kind: "start", decisionId: result!.decisionId,
      agentId: "agent-1", sessionId: null, model: result!.model ?? null, thinking: result!.thinking, ...unverifiedStart });
    expect(log[1]!.model).toBe(mode === "enforce" ? pick.model : defaults.model);
  });
  it("writes an explicit applied start line and a fixed start failure", async () => {
    const dir = root();
    const admission = admissionFor(dir);
    const admitted = { name: "explicit actor", instructions: "private actor instructions" };
    const result = await routeAgentCreation({ ...options(dir), kind: "actor", explicit: true,
      admission, admissionRequest: admitted });
    await recordSpawnRouterStart({ meshRoot: dir, decisionId: result!.decisionId, agentId: "actor-1", sessionId: "pi-1",
      model: defaults.model, thinking: defaults.thinking, admission, spawnKind: "actor", request: admitted, cwd: dir });
    const log = decisions(dir);
    expect(log[1]).toMatchObject({ kind: "start", decisionId: result!.decisionId, agentId: "actor-1", sessionId: "pi-1",
      model: defaults.model, thinking: defaults.thinking });
    const failedDir = root();
    const failedAdmission = admissionFor(failedDir);
    const failedRequest = { task: "private failed request" };
    const failed = await routeAgentCreation({ ...options(failedDir), explicit: true,
      admission: failedAdmission, admissionRequest: failedRequest });
    await recordSpawnRouterStartFailed({ meshRoot: failedDir, decisionId: failed!.decisionId,
      admission: failedAdmission, spawnKind: "spawn", request: failedRequest, cwd: failedDir });
    expect(decisions(failedDir)[1]).toEqual({ ts: expect.any(String), kind: "start-failed", decisionId: failed!.decisionId,
      agentId: null, sessionId: null, model: null, thinking: null, ...unverifiedStart, error: "start-failed" });
  });
  it.each([
    ["process.stdout.write('not JSON');", "invalid-json"],
    ["process.stdout.write('[]');", "invalid-output"],
    ["process.stdout.write('{}');", "invalid-output"],
    ["process.stdout.write(JSON.stringify({ model: 'unknown', thinking: 'high' }));", "unknown-or-denied-model"],
    ["process.stdout.write(JSON.stringify({ model: 'provider/model-b', thinking: 'extreme' }));", "invalid-output"],
    ["process.stdout.write(JSON.stringify({ model: 'provider/model-b', thinking: 'high', reason: {} }));", "invalid-output"],
    ["process.stderr.write('secret task text'); process.exit(17);", "nonzero-exit"],
    ["process.stdout.write('x'.repeat(70000));", "output-too-large"],
  ])("falls back and logs a sanitized error for %s", async (body, error) => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body) } })).toMatchObject({ ...defaults, decisionId: expect.any(String) });
    expect(decisions(dir)).toEqual([expect.objectContaining({ actual: defaults, pick: null, error })]);
    expect(JSON.stringify(decisions(dir))).not.toContain("secret task text");
    expect(JSON.stringify(decisions(dir))).not.toContain("private validation detail");
  });
  it.each([[], ["/definitely/missing/spawn-router"]].map(argv => ({ argv })))("fails open on unavailable command $argv", async ({ argv }) => {
    const dir = root(); const opts = options(dir);
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: argv } })).toMatchObject({ ...defaults, decisionId: expect.any(String) });
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: argv.length ? "command-error" : "missing-command" });
  });
  it("times out, reaps the command and logs fallback before returning", async () => {
    const dir = root(); const opts = options(dir);
    const body = `require('node:fs').writeFileSync(process.argv[1] + '.pid', String(process.pid)); setInterval(() => {}, 1000);`;
    const before = Date.now();
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body), timeoutMs: 200 } })).toMatchObject({ ...defaults, decisionId: expect.any(String) });
    expect(Date.now() - before).toBeLessThan(5000);
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: "timeout" });
    const pid = Number(fs.readFileSync(path.join(dir, "request.json.pid"), "utf8"));
    await vi.waitFor(() => expect(() => process.kill(pid, 0)).toThrow());
  });
  it("timeout settles and retires a lingering grandchild with inherited pipes", async () => {
    const dir = root(); const opts = options(dir);
    const grandchild = `require('node:fs').writeFileSync(process.argv[1], 'started'); setTimeout(() => require('node:fs').writeFileSync(process.argv[1] + '.late', 'leaked'), 800);`;
    const body = `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(grandchild)}, process.argv[1] + '.grandchild'], { stdio: ['ignore', 'inherit', 'inherit'] }); setInterval(() => {}, 1000);`;
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body), timeoutMs: 400 } })).toMatchObject({ ...defaults, decisionId: expect.any(String) });
    expect(decisions(dir)[0]).toMatchObject({ error: "timeout", actual: defaults });
    expect(fs.existsSync(path.join(dir, "request.json.grandchild"))).toBe(true);
    await new Promise(resolve => setTimeout(resolve, 900));
    expect(fs.existsSync(path.join(dir, "request.json.grandchild.late"))).toBe(false);
  });
  it("argv metacharacters are forwarded literally, never interpreted by a shell", async () => {
    const dir = root(); const opts = options(dir); const literal = `space ; $(touch ${path.join(dir, "shell-leak")}) | &`;
    const argv = [...command(dir, `require('node:fs').writeFileSync(process.argv[1] + '.arg', process.argv[2]); process.stdout.write(${JSON.stringify(JSON.stringify(pick))});`), literal];
    expect(await routeAgentCreation({ ...opts, config: { ...opts.config, command: argv } })).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(fs.readFileSync(path.join(dir, "request.json.arg"), "utf8")).toBe(literal);
    expect(fs.existsSync(path.join(dir, "shell-leak"))).toBe(false);
  });

  it("abort retires a running router and falls back", async () => {
    const dir = root(); const opts = options(dir); const controller = new AbortController();
    const body = `require('node:fs').writeFileSync(process.argv[1] + '.pid', String(process.pid)); setInterval(() => {}, 1000);`;
    const pending = routeAgentCreation({ ...opts, config: { ...opts.config, command: command(dir, body), timeoutMs: 5000 }, signal: controller.signal });
    await vi.waitFor(() => expect(fs.existsSync(path.join(dir, "request.json.pid"))).toBe(true), { timeout: 5000 });
    const pid = Number(fs.readFileSync(path.join(dir, "request.json.pid"), "utf8"));
    controller.abort(); await expect(pending).resolves.toMatchObject({ ...defaults, decisionId: expect.any(String) });
    expect(decisions(dir)[0]).toMatchObject({ actual: defaults, error: "aborted" });
    expect(() => process.kill(pid, 0)).toThrow();
  });
  it("a logging failure cannot fail or change an enforce selection", async () => {
    const dir = root(); fs.writeFileSync(path.join(dir, "router"), "not a directory");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
  });
  it.skipIf(process.platform === "win32")("creates a private directory and regular ledger with 0700/0600 modes", async () => {
    const dir = root(); await routeAgentCreation({ ...options(dir), explicit: true });
    const directory = fs.lstatSync(path.join(dir, "router"));
    const ledger = fs.lstatSync(path.join(dir, "router/decisions.jsonl"));
    expect(directory.isDirectory()).toBe(true); expect(directory.uid).toBe(process.getuid!());
    expect(directory.mode & 0o777).toBe(0o700);
    expect(ledger.isFile()).toBe(true); expect(ledger.mode & 0o777).toBe(0o600);
  });
  it.skipIf(!process.getuid)("refuses a directory owned by another uid without failing selection", async () => {
    const dir = root(); fs.mkdirSync(path.join(dir, "router"), { mode: 0o700 });
    const uid = process.getuid!(); vi.spyOn(process, "getuid").mockReturnValue(uid + 1);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(fs.existsSync(path.join(dir, "router/decisions.jsonl"))).toBe(false);
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
  });
  it.skipIf(process.platform === "win32")("refuses a non-private existing router directory", async () => {
    const dir = root(); fs.mkdirSync(path.join(dir, "router")); fs.chmodSync(path.join(dir, "router"), 0o755);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(fs.existsSync(path.join(dir, "router/decisions.jsonl"))).toBe(false);
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
  });
  it.skipIf(process.platform === "win32").each(["directory", "ledger"])("refuses a %s symlink without changing the enforce selection", async target => {
    const dir = root(); const victim = root();
    const sentinel = path.join(victim, "decisions.jsonl"); fs.writeFileSync(sentinel, "untouched");
    if (target === "directory") fs.symlinkSync(victim, path.join(dir, "router"), "dir");
    else {
      fs.mkdirSync(path.join(dir, "router"), { mode: 0o700 });
      fs.symlinkSync(sentinel, path.join(dir, "router/decisions.jsonl"));
    }
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(fs.readFileSync(sentinel, "utf8")).toBe("untouched");
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
    expect(fs.readdirSync(victim)).toEqual(["decisions.jsonl"]);
  });
  it("refuses a non-regular ledger without failing selection", async () => {
    const dir = root(); fs.mkdirSync(path.join(dir, "router/decisions.jsonl"), { recursive: true, mode: 0o700 });
    fs.chmodSync(path.join(dir, "router"), 0o700);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(await routeAgentCreation(options(dir))).toEqual(expect.objectContaining({ model: pick.model, thinking: pick.thinking, decisionId: expect.any(String) }));
    expect(warn).toHaveBeenCalledWith("[pi-fabric] spawn router decision log unavailable");
    expect(fs.lstatSync(path.join(dir, "router/decisions.jsonl")).isDirectory()).toBe(true);
  });
  it("rotates at 8 MiB, serializes concurrent appends, and keeps one archive", async () => {
    const dir = root(); const router = path.join(dir, "router"); fs.mkdirSync(router, { mode: 0o700 });
    const file = path.join(router, "decisions.jsonl"); const limit = 8 * 1024 * 1024;
    const seed = "x".repeat(limit - 16);
    fs.writeFileSync(file, seed, { mode: 0o600 }); fs.writeFileSync(file + ".1", "obsolete archive");
    await Promise.all(Array.from({ length: 5 }, (_, n) => routeAgentCreation({ ...options(dir), task: `task-${n}`, explicit: true })));
    expect(decisions(dir)).toHaveLength(5);
    expect(fs.statSync(file).size).toBeLessThanOrEqual(limit);
    expect(fs.readFileSync(file + ".1", "utf8") === seed).toBe(true);
    fs.writeFileSync(file, "y".repeat(limit));
    await routeAgentCreation({ ...options(dir), explicit: true });
    expect(decisions(dir)).toHaveLength(1); expect(fs.statSync(file + ".1").size).toBe(limit);
    expect(fs.readFileSync(file + ".1", "utf8").startsWith("y")).toBe(true);
    expect(fs.readdirSync(router).sort()).toEqual(["decisions.jsonl", "decisions.jsonl.1"]);
  });
  it("concurrent spawns each append one complete JSONL record", async () => {
    const dir = root();
    await Promise.all(Array.from({ length: 5 }, (_, n) => routeAgentCreation({ ...options(dir), task: `task-${n}` })));
    expect(decisions(dir)).toHaveLength(5);
    expect(new Set(decisions(dir).map(d => d.decisionId)).size).toBe(5);
  });
  it("owner-only actor instruction files expose only the supplied digest and unknown length", async () => {
    const dir = root(); const { task: _task, ...opts } = options(dir); const taskDigest = "a".repeat(64);
    await routeAgentCreation({ ...opts, kind: "actor", taskDigest, config: { ...opts.config, includeTask: true } });
    expect(request(dir)).toMatchObject({ kind: "actor", taskDigest, taskLength: null });
    expect(request(dir)).not.toHaveProperty("task");
  });
});
