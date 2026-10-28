// Keep reason: the runners-lead/fork-lead misdelivery is an exact-address trust boundary.
// Public provider + real guest execution; no model, network, manager cleanup, or live mesh.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MainAgentController } from "../src/main-agent.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const RUNNERS = "session:11111111-1111-4111-8111-111111111111";
const FORK = "session:22222222-2222-4222-8222-222222222222";
const SELF = "session:33333333-3333-4333-8333-333333333333";
const tempTrees: string[] = [];
const providers: AgentsProvider[] = [];
const incomingKeys = [
  "PI_FABRIC_SPAWNER_ID", "PI_FABRIC_SPAWNER_KIND", "PI_FABRIC_SPAWNER_RUN", "PI_FABRIC_SPAWNER_SESSION_ID",
  "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_LINEAGE", "PI_FABRIC_DEPTH",
  "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_AGENT_RUN_DIR", "PI_FABRIC_AGENT_NAME",
  "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID", "PI_FABRIC_PROJECT", "SMARTY_LEAD_SESSION", "SMARTY_ROLE",
];
beforeEach(() => { for (const key of incomingKeys) vi.stubEnv(key, undefined); });
afterEach(async () => {
  for (const provider of providers.splice(0)) await provider.close();
  for (const root of tempTrees.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});
const lane = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-project-agent-"));
  tempTrees.push(root);
  return root;
};
const marker = (cwd: string, id = FORK) => {
  fs.mkdirSync(path.join(cwd, ".local"), { recursive: true });
  fs.writeFileSync(path.join(cwd, ".local", "lead"), `${id}\n`);
};
const root = (id: string, cwd: string, fields: Partial<FabricParticipantInfo> = {}): FabricParticipantInfo => ({
  format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "fixture:1",
  kind: "root", name: id === FORK ? "fork-lead" : "runners-lead", runner: "pi", transport: "host",
  status: "idle", interactive: true, capabilities: ["steer", "followUp"], controlProtocol: "v1",
  role: "project-agent", project: cwd, cwd, startedAt: 1, updatedAt: 2, local: false, stale: false, ...fields,
});
const setup = (cwd: string, members: FabricParticipantInfo[], callerKind = "main") => {
  const main = new MainAgentController({} as any, SELF, true, cwd);
  const provider = new AgentsProvider({ cwd } as any, { identity: { id: SELF, kind: callerKind } } as any,
    {} as any, main, { sessions: () => members, list: () => members } as any, undefined, {} as any,
    undefined, undefined, false);
  providers.push(provider);
  const context = { cwd, extensionContext: { cwd, sessionManager: { getSessionId: () => SELF.slice(8) } },
    update() {}, activity() {} } as unknown as FabricInvocationContext;
  return { provider, context, invoke: (invocationCwd = cwd) => provider.invoke("projectAgent", {}, { ...context, cwd: invocationCwd }) };
};
const bind = (id = RUNNERS, kind = "main") => {
  vi.stubEnv("PI_FABRIC_SPAWNER_ID", id);
  vi.stubEnv("PI_FABRIC_SPAWNER_KIND", kind);
};
const unresolved = { name: "FabricProjectAgentUnresolvedError", code: "FABRIC_PROJECT_AGENT_UNRESOLVED" };

describe("projectAgent uses only the actual parent binding", () => {
  it.each([1, 2])("no binding never elects any of %s tempting native roots", async count => {
    const cwd = lane();
    const members = [root(FORK, cwd), root(RUNNERS, cwd)].slice(0, count);
    await expect(setup(cwd, members).invoke()).rejects.toMatchObject(unresolved);
  });

  it("uses the bound Main before a misleading marker, repository, role or invocation cwd", async () => {
    const cwd = lane(), foreign = lane();
    marker(cwd); marker(foreign);
    bind();
    vi.stubEnv("SMARTY_LEAD_SESSION", FORK);
    const runners = root(RUNNERS, "/other-host/runners", { remoteHost: "other-host", role: "area-lead", repository: "forge/Other/Repo" });
    const f = setup(cwd, [root(FORK, cwd, { startedAt: 99 }), runners]);
    await expect(f.invoke(foreign)).resolves.toMatchObject({ id: RUNNERS });
    await expect(f.provider.invoke("spawner", {}, f.context)).resolves.toEqual({ id: RUNNERS, kind: "main" });
    vi.stubEnv("PI_FABRIC_SPAWNER_ID", FORK);
    vi.stubEnv("PI_FABRIC_SPAWNER_KIND", "actor");
    await expect(f.invoke()).resolves.toMatchObject({ id: RUNNERS });
    const route = vi.spyOn(f.provider, "routeMessage").mockResolvedValue({ queued: true } as any);
    await f.provider.reportMainProviderError("terminal error");
    expect(route).toHaveBeenCalledExactlyOnceWith(RUNNERS, "terminal error", undefined, "followUp");
  });

  it("discovers a root lane marker created after provider construction, not invocation cwd or mutable env", async () => {
    const cwd = lane(), foreign = lane();
    marker(foreign);
    const f = setup(cwd, [root(FORK, foreign), root(RUNNERS, "/lead")]);
    marker(cwd, RUNNERS);
    vi.stubEnv("SMARTY_LEAD_SESSION", FORK);
    vi.stubEnv("PI_FABRIC_PROJECT", foreign);
    bind(FORK);
    await expect(f.invoke(foreign)).resolves.toMatchObject({ id: RUNNERS });
    marker(cwd, FORK);
    await expect(f.invoke()).resolves.toMatchObject({ id: FORK });
  });

  it.each(["garbage", "", "session:fake"])("a malformed lane marker is unresolved, never an election: %j", async contents => {
    const cwd = lane(); marker(cwd, contents);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = setup(cwd, [root(FORK, cwd)]);
    expect(warn).not.toHaveBeenCalled(); // marker discovery is not a provider startup dependency
    await expect(f.invoke()).rejects.toMatchObject(unresolved);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]![0]).not.toContain("garbage");
  });

  it("a bound Main is independent of malformed launcher hints", async () => {
    const cwd = lane(); marker(cwd, "malformed"); bind();
    vi.stubEnv("SMARTY_LEAD_SESSION", "malformed");
    await expect(setup(cwd, [root(RUNNERS, cwd), root(FORK, cwd)]).invoke()).resolves.toMatchObject({ id: RUNNERS });
  });

  it.each(["absent", "stale", "bridge-stale", "noninteractive", "no-steer", "no-followUp"])("refuses the exact %s bound target without marker fallback", async scenario => {
    const cwd = lane(); marker(cwd); bind();
    const fields: Partial<FabricParticipantInfo> = scenario === "stale" ? { stale: true }
      : scenario === "bridge-stale" ? { bridgeStale: true }
      : scenario === "noninteractive" ? { interactive: false }
      : scenario === "no-steer" ? { capabilities: ["followUp"] }
      : scenario === "no-followUp" ? { capabilities: ["steer"] } : {};
    const members = [root(FORK, cwd), ...(scenario === "absent" ? [] : [root(RUNNERS, cwd, fields)])];
    await expect(setup(cwd, members).invoke()).rejects.toMatchObject(unresolved);
  });

  it.each([
    { PI_FABRIC_SPAWNER_ID: RUNNERS },
    { PI_FABRIC_SPAWNER_KIND: "main" },
    { PI_FABRIC_SPAWNER_ID: RUNNERS, PI_FABRIC_SPAWNER_KIND: "unsupported" },
    { PI_FABRIC_SPAWNER_ID: "malformed", PI_FABRIC_SPAWNER_KIND: "main" },
    { PI_FABRIC_SPAWNER_RUN: "parent" },
    { PI_FABRIC_SPAWNER_SESSION_ID: RUNNERS.slice(8) },
    { PI_FABRIC_PARENT_RUN: "task" },
    { PI_FABRIC_TASK_PROCESS_CHILD: "1", PI_FABRIC_PARENT_RUN: "task", PI_FABRIC_MAIN_AGENT_ID: RUNNERS },
    { PI_FABRIC_ACTOR_ID: "actor:parent" },
    { PI_FABRIC_LINEAGE: "[]" },
    { PI_FABRIC_DEPTH: "1" },
    { PI_FABRIC_AGENT_RUN_DIR: "/owned/test-run" },
    { PI_FABRIC_AGENT_NAME: "task" },
    { SMARTY_ROLE: "task-agent@fixture" },
  ])("expected or malformed incoming binding cannot borrow a valid lane marker: %j", async environment => {
    const cwd = lane(); marker(cwd);
    for (const [key, value] of Object.entries(environment)) vi.stubEnv(key, value);
    const f = setup(cwd, [root(FORK, cwd), root(RUNNERS, cwd)]);
    // Removing evidence later cannot turn a task into a root lane.
    for (const key of Object.keys(environment)) vi.stubEnv(key, undefined);
    await expect(f.invoke()).rejects.toMatchObject(unresolved);
  });

  it("a malformed Main binding cannot masquerade as a root even in structural directory fixtures", async () => {
    const cwd = lane(); marker(cwd); bind("not-a-native-session");
    await expect(setup(cwd, [root("not-a-native-session", cwd), root(FORK, cwd)]).invoke()).rejects.toMatchObject(unresolved);
  });

  it.each(["agent", "actor"])("a %s spawner cannot be converted into a root via native session id or lineage Main", async kind => {
    const cwd = lane(); marker(cwd);
    bind("nonroot-parent", kind);
    vi.stubEnv("PI_FABRIC_SPAWNER_SESSION_ID", RUNNERS.slice(8));
    vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", RUNNERS);
    await expect(setup(cwd, [root(RUNNERS, cwd), root(FORK, cwd)], "agent").invoke()).rejects.toMatchObject(unresolved);
  });

  it.each(["agent", "actor"])("host-bound %s identity with missing env cannot fall through to the lane marker", async kind => {
    const cwd = lane(); marker(cwd);
    await expect(setup(cwd, [root(FORK, cwd)], kind).invoke()).rejects.toMatchObject(unresolved);
  });

  it.each(["file", "directory", "unversioned"])("the actual %s lane never borrows an enclosing/common-checkout marker", async layout => {
    const outer = lane(), cwd = path.join(outer, "lane");
    fs.mkdirSync(cwd); marker(outer);
    fs.mkdirSync(path.join(outer, ".git"));
    if (layout === "file") fs.writeFileSync(path.join(cwd, ".git"), "gitdir: ../.git/worktrees/lane\n");
    if (layout === "directory") fs.mkdirSync(path.join(cwd, ".git"));
    await expect(setup(cwd, [root(FORK, outer)]).invoke(outer)).rejects.toMatchObject(unresolved);
  });

  it("keeps exact-address terminal error reporting separate from projectAgent (no election)", async () => {
    const cwd = lane(); marker(cwd, RUNNERS);
    const f = setup(cwd, [root(FORK, cwd)]);
    const route = vi.spyOn(f.provider, "routeMessage").mockResolvedValue({ queued: true } as any);
    await expect(f.invoke()).rejects.toMatchObject(unresolved);
    await f.provider.reportMainProviderError("terminal error");
    expect(route).toHaveBeenCalledExactlyOnceWith(RUNNERS, "terminal error", undefined, "followUp");
    const unbound = setup(lane(), [root(FORK, cwd)]);
    const noRoute = vi.spyOn(unbound.provider, "routeMessage");
    await expect(unbound.provider.reportMainProviderError("terminal error")).resolves.toBeUndefined();
    expect(noRoute).not.toHaveBeenCalled();
  });

  it("preserves unresolved name/code through FabricExecutionService's real TypeScript guest boundary", async () => {
    const cwd = lane();
    const { provider, context } = setup(cwd, [root(FORK, cwd)]);
    const registry = new ActionRegistry(); registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.kernel = "typescript"; config.executor.runtime = "quickjs";
    const service = new FabricExecutionService(registry, config);
    try {
      const result = await service.execute({
        code: `try { await agents.projectAgent(); return { unexpected: true }; }
          catch (error) { const failure = error as Error & { code?: string };
            return { isError: error instanceof Error, name: failure.name, code: failure.code }; }`,
        context: { ...context.extensionContext, cwd, mode: "rpc" } as any,
        signal: undefined, parentToolCallId: "project-agent-unresolved", onPartial() {},
      });
      expect(result.success, result.error).toBe(true);
      expect(result.value).toEqual({ isError: true, ...unresolved });
    } finally { await registry.close(); }
  });
});
