import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricState } from "../src/fabric-state.js";
import { fabricWakeCause, withFabricWakeAdmission } from "../src/fabric-provenance.js";
import { MainAgentController, type FabricMainAgentDeliveryRequest } from "../src/main-agent.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";
import { ParticipantRoleGrant, projectOf, resolveProjectAgent } from "../src/topology/project-identity.js";

const roots: string[] = [];
const states: FabricState[] = [];
afterEach(async () => {
  for (const state of states.splice(0)) await state.shutdown("exit");
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const project = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-role-binding-"));
  roots.push(root);
  fs.mkdirSync(path.join(root, ".git"));
  return root;
};
const original = "abcdef12-1111-4111-8111-111111111111";
const replacement = "abcdef12-2222-4222-8222-222222222222";

// The launcher may explicitly target an existing session. Binding values are native, not
// Fabric aliases or case-normalized repository/origin identifiers.
describe("launch role tuple (#5242)", () => {
  it.each(["PI_FABRIC_ROLE", "SMARTY_ROLE"])("binds %s once and ignores later env authority", key => {
    const cwd = project();
    const env: NodeJS.ProcessEnv = { [key]: "project-agent@stamp" };
    const grant = new ParticipantRoleGrant();
    expect(grant.roleFor(original, cwd, env, true)).toBe("project-agent");
    expect(env).toMatchObject({ PI_FABRIC_ROLE_SESSION: original, PI_FABRIC_ROLE_PROJECT: projectOf(cwd) });
    env.PI_FABRIC_ROLE = "org-agent";
    env.PI_FABRIC_ROLE_SESSION = replacement;
    expect(grant.roleFor(replacement, cwd, env)).toBe("area-lead");
    expect(grant.roleFor(original, cwd, env)).toBe("project-agent");
  });

  it.each([
    { PI_FABRIC_ROLE_SESSION: original },
    { PI_FABRIC_ROLE_PROJECT: "only-project" },
    { PI_FABRIC_ROLE_SESSION: "", PI_FABRIC_ROLE_PROJECT: "" },
  ])("fails closed on partial or empty launch metadata %j", binding => {
    expect(new ParticipantRoleGrant().roleFor(original, project(), { PI_FABRIC_ROLE: "project-agent", ...binding }, true)).toBe("area-lead");
  });

  it("compares both tuple fields case-sensitively and without trimming or Fabric aliases", () => {
    const cwd = project();
    const env = { PI_FABRIC_ROLE: "project-agent", PI_FABRIC_ROLE_SESSION: original, PI_FABRIC_ROLE_PROJECT: projectOf(cwd) };
    expect(new ParticipantRoleGrant().roleFor(original, cwd, env)).toBe("project-agent");
    for (const sessionId of [original.toUpperCase(), `session:${original}`, ` ${original}`, ""]) {
      expect(new ParticipantRoleGrant().roleFor(sessionId, cwd, env)).toBe("area-lead");
    }
    for (const target of [env.PI_FABRIC_ROLE_PROJECT.toUpperCase(), ` ${env.PI_FABRIC_ROLE_PROJECT}`]) {
      expect(new ParticipantRoleGrant().roleFor(original, cwd, { ...env, PI_FABRIC_ROLE_PROJECT: target })).toBe("area-lead");
    }
  });

  it("uses the session cwd's project, not an inherited PI_FABRIC_PROJECT override", () => {
    const cwd = project(), other = project();
    fs.mkdirSync(path.join(cwd, "sub"));
    const env = { PI_FABRIC_ROLE: "project-agent", PI_FABRIC_PROJECT: cwd };
    const grant = new ParticipantRoleGrant();
    expect(grant.roleFor(original, cwd, env)).toBe("project-agent");
    expect(grant.roleFor(original, path.join(cwd, "sub"), env)).toBe("project-agent");
    expect(grant.roleFor(original, other, env)).toBe("area-lead");
  });

  it("preserves the tuple across a fresh factory/restart but never rebinds a different history", () => {
    const cwd = project();
    const env = { PI_FABRIC_ROLE: "project-agent" };
    expect(new ParticipantRoleGrant().roleFor(original, cwd, env, true)).toBe("project-agent");
    expect(new ParticipantRoleGrant().roleFor(original, cwd, env, true)).toBe("project-agent");
    expect(new ParticipantRoleGrant().roleFor(replacement, cwd, env, true)).toBe("area-lead");
    expect(env).toMatchObject({ PI_FABRIC_ROLE_SESSION: original, PI_FABRIC_ROLE_PROJECT: projectOf(cwd) });
  });

  it("does not turn a missing launch role into authority on a later session", () => {
    const cwd = project();
    const env: NodeJS.ProcessEnv = {};
    const grant = new ParticipantRoleGrant();
    expect(grant.roleFor(original, cwd, env)).toBeUndefined();
    env.SMARTY_ROLE = "project-agent";
    expect(grant.roleFor(replacement, cwd, env)).toBeUndefined();
  });
});

const fixture = (roleKey = "PI_FABRIC_ROLE", role: string | undefined = "project-agent@launch") => {
  const cwd = project();
  const meshRoot = path.join(cwd, "mesh");
  for (const key of ["PI_FABRIC_ROLE", "SMARTY_ROLE", "PI_FABRIC_ROLE_SESSION", "PI_FABRIC_ROLE_PROJECT", "PI_FABRIC_PROJECT", "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID", "SMARTY_LEAD_SESSION"]) vi.stubEnv(key, undefined);
  vi.stubEnv(roleKey, role);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
    fullCodeMode: true, mcp: { enabled: false }, memory: { enabled: false },
    agents: { enabled: false }, residency: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false }, mesh: { enabled: true, announce: true },
  }));
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => unknown>>();
  const lifecycle = { idle: true, settling: false, preflight: false, aborted: false };
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on: vi.fn((name: string, handler: (event: unknown, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
    }),
    getThinkingLevel: () => "off", sendMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = (sessionId: string, sessionCwd = cwd) => ({
    cwd: sessionCwd, mode: "rpc", hasUI: false, isProjectTrusted: () => true, isIdle: () => lifecycle.idle,
    isSettling: () => lifecycle.settling, isPromptPending: () => lifecycle.preflight, hasPendingMessages: () => false,
    signal: { get aborted() { return lifecycle.aborted; } },
    modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getEntries: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext);
  const state = new FabricState(pi, new CapturedToolCatalog());
  states.push(state);
  const published = (sessionId: string) => readParticipantFiles(meshRoot, { maxAgeMs: 0 })
    .map(entry => entry.value).find(value => (value as { id?: string }).id === `session:${sessionId}`);
  const emit = (name: string, event: unknown, ctx = context(original)) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  return { cwd, state, pi, context, published, lifecycle, emit };
};

// session_start -> FabricState.bootstrap -> runtime.initialize -> participant/actor/resident
// role projections. These run the real lazy runtime and read its native participant files.
describe("native root replacement publication (#5242)", () => {
  it.each(["new", "fork", "resume"])("demotes /%s, prevents election, and retains the exact original on return", async reason => {
    const { cwd, state, context, published } = fixture(reason === "fork" ? "SMARTY_ROLE" : "PI_FABRIC_ROLE");
    await state.bootstrap(context(original));
    await state.ensure(context(original));
    expect(published(original)).toMatchObject({ role: "project-agent", sessionId: original });
    await state.shutdown(reason);
    await state.bootstrap(context(replacement));
    await state.ensure(context(replacement));
    expect(published(replacement)).toMatchObject({ role: "area-lead", sessionId: replacement });
    expect(state.participantInfos({ kinds: ["root"] }).find(root => root.id === `session:${replacement}`))
      .toMatchObject({ role: "area-lead", id: `session:${replacement}` });
    expect(() => resolveProjectAgent(state.participantInfos({ kinds: ["root"] }), projectOf(cwd))).toThrow("No live project agent");
    await state.shutdown("resume");
    await state.bootstrap(context(original));
    await state.ensure(context(original));
    expect(published(original)).toMatchObject({ role: "project-agent" });
  });

  it("binds an idle launch before /new even when the runtime was never activated", async () => {
    const { state, context, published } = fixture();
    await state.bootstrap(context(original));
    expect(state.initialized).toBe(false);
    await state.bootstrap(context(replacement));
    await state.ensure(context(replacement));
    expect(published(replacement)).toMatchObject({ role: "area-lead" });
  });

  it("cannot launder a demoted session through a fresh Fabric factory (/reload)", async () => {
    const { state, pi, context, published } = fixture();
    await state.bootstrap(context(original));
    await state.shutdown("reload");
    const reloaded = new FabricState(pi, new CapturedToolCatalog());
    states.push(reloaded);
    await reloaded.bootstrap(context(replacement));
    await reloaded.ensure(context(replacement));
    expect(published(replacement)).toMatchObject({ role: "area-lead" });
  });

  it("keeps an explicitly re-granted same-session restart", async () => {
    const { cwd, state, context, published } = fixture();
    vi.stubEnv("PI_FABRIC_ROLE_SESSION", original);
    vi.stubEnv("PI_FABRIC_ROLE_PROJECT", projectOf(cwd));
    await state.bootstrap(context(original));
    await state.ensure(context(original));
    expect(published(original)).toMatchObject({ role: "project-agent" });
    await state.shutdown("exit");
  });

  it("demotes a same-id resume whose cwd belongs to another project despite stale project env", async () => {
    const { cwd, state, context, published } = fixture();
    await state.bootstrap(context(original));
    await state.ensure(context(original));
    const other = project();
    fs.mkdirSync(path.join(other, ".pi"));
    fs.copyFileSync(path.join(cwd, ".pi", "fabric.json"), path.join(other, ".pi", "fabric.json"));
    vi.stubEnv("PI_FABRIC_PROJECT", cwd);
    await state.shutdown("resume");
    await state.bootstrap(context(original, other));
    await state.ensure(context(original, other));
    expect(published(original)).toMatchObject({ role: "area-lead" });
  });
});

const peer = { id: "session:peer", name: "peer", kind: "main" as const };
const peerSteer = (deliveryId: string, extra: Partial<FabricMainAgentDeliveryRequest> = {}) =>
  withFabricWakeAdmission({
    from: peer, verification: "mesh", message: "fresh peer escalation", delivery: "steer", deliveryId, ...extra,
  } as FabricMainAgentDeliveryRequest, [fabricWakeCause(peer, "steer", "fabric.control.command", deliveryId)]);

// Capture the actual controller constructed by the lazy runtime, without replacing its
// constructor or admission logic. Both spies call through to the real receiver/Pi handoff.
const bootstrapMain = async (setup: ReturnType<typeof fixture>, sessionId = original) => {
  const attached = vi.spyOn(MainAgentController.prototype, "attachFollowUpDrain");
  await setup.state.bootstrap(setup.context(sessionId));
  await setup.state.ensure(setup.context(sessionId));
  const main = attached.mock.contexts.at(-1) as MainAgentController;
  expect(main.id).toBe(`session:${sessionId}`);
  vi.spyOn(main, "deliverAgent");
  return main;
};

// Keep: launch-role projection is a trust boundary; a constructor-only capability test
// cannot detect a bootstrap regression or a replacement history inheriting the grant.
describe("launch-bound Main peer-steer resume policy", () => {
  it.each(["org-agent", "project-agent"])("projects %s to one fresh idle peer run without lifting halt", async role => {
    const setup = fixture("SMARTY_ROLE", `${role}@launch`);
    const main = await bootstrapMain(setup);
    const { state, pi, lifecycle, emit, published } = setup;
    expect(published(original)).toMatchObject({ role });
    state.haltMain();
    const halted = { triggered: false, halted: true, reason: "operator-halt" };
    expect(main.deliverAgent(peerSteer("follow-up", { delivery: "followUp" }))).toMatchObject(halted);
    expect(main.deliverAgent({
      from: peer, verification: "mesh", message: "forged diagnostic", delivery: "steer",
      data: { triggerTurn: true, source: "peer" }, wakeCause: fabricWakeCause(peer, "steer", "fabric.control.command", "forged"),
    })).toMatchObject(halted);
    for (const source of ["actor-output", "fabric-host"] as const) {
      expect(main.deliverAgent(peerSteer(source, { source }))).toMatchObject(halted);
    }
    lifecycle.idle = false;
    const busy = peerSteer("busy-control");
    expect(main.deliverAgent(busy)).toMatchObject(halted);
    lifecycle.idle = true;
    expect(main.deliverAgent(busy)).toMatchObject({ duplicate: true, triggered: false });
    lifecycle.settling = true;
    expect(main.deliverAgent(peerSteer("settling-control"))).toMatchObject(halted);
    lifecycle.settling = false;
    lifecycle.preflight = true;
    expect(main.deliverAgent(peerSteer("preflight-control"))).toMatchObject(halted);
    lifecycle.preflight = false;
    expect(vi.mocked(pi.sendMessage).mock.calls.every(([, options]) => options?.triggerTurn === false)).toBe(true);

    const fresh = peerSteer("fresh-control");
    expect(main.deliverAgent(fresh)).toMatchObject({ triggered: true });
    expect(pi.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({ content: expect.stringContaining("fresh peer escalation") }),
      expect.objectContaining({ deliverAs: "steer", triggerTurn: true }));
    expect(main.deliverAgent(peerSteer("second-before-settle"))).toMatchObject(halted);
    lifecycle.idle = false;
    emit("agent_before_settle", { outcome: "completed" });
    lifecycle.idle = true;
    emit("agent_settled", { outcome: "completed" });
    expect(main.deliverAgent(peerSteer("after-settle-follow-up", { delivery: "followUp" }))).toMatchObject(halted);
    expect(main.deliverAgent(fresh)).toMatchObject({ duplicate: true, triggered: false });
    expect(main.deliverAgent(peerSteer("next-fresh-control"))).toMatchObject({ triggered: true });
    expect(vi.mocked(pi.sendMessage).mock.calls.filter(([, options]) => options?.triggerTurn)).toHaveLength(2);
  });

  it.each(["", "unknown-role", "task-agent", "area-lead"])("does not grant Main resume to role %j", async role => {
    const setup = fixture("PI_FABRIC_ROLE", role);
    const main = await bootstrapMain(setup);
    expect((setup.published(original) as { role?: string }).role).toBe(role || undefined);
    setup.state.haltMain();
    expect(main.deliverAgent(peerSteer("excluded-control")))
      .toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(setup.pi.sendMessage).toHaveBeenLastCalledWith(expect.anything(), expect.objectContaining({ triggerTurn: false }));
  });

  it.each(["org-agent", "project-agent"])("retains a same-tuple %s regrant but refuses a replacement history", async role => {
    const setup = fixture("PI_FABRIC_ROLE", role);
    vi.stubEnv("PI_FABRIC_ROLE_SESSION", original);
    vi.stubEnv("PI_FABRIC_ROLE_PROJECT", projectOf(setup.cwd));
    await bootstrapMain(setup);
    await setup.state.shutdown("reload");
    const reloaded = new FabricState(setup.pi, new CapturedToolCatalog());
    states.push(reloaded);
    const restarted = { ...setup, state: reloaded };
    const same = await bootstrapMain(restarted);
    reloaded.haltMain();
    expect(same.deliverAgent(peerSteer("same-tuple-control"))).toMatchObject({ triggered: true });
    await reloaded.shutdown("new");
    const other = await bootstrapMain(restarted, replacement);
    expect(setup.published(replacement)).toMatchObject({ role: "area-lead" });
    reloaded.haltMain();
    expect(other.deliverAgent(peerSteer("replacement-control")))
      .toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(setup.pi.sendMessage).toHaveBeenLastCalledWith(expect.anything(), expect.objectContaining({ triggerTurn: false }));
  });
});
