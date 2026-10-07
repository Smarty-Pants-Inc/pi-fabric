import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricState } from "../src/fabric-state.js";
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

const fixture = (roleKey = "PI_FABRIC_ROLE") => {
  const cwd = project();
  const meshRoot = path.join(cwd, "mesh");
  for (const key of ["PI_FABRIC_ROLE", "SMARTY_ROLE", "PI_FABRIC_ROLE_SESSION", "PI_FABRIC_ROLE_PROJECT", "PI_FABRIC_PROJECT", "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID", "SMARTY_LEAD_SESSION"]) vi.stubEnv(key, undefined);
  vi.stubEnv(roleKey, "project-agent@launch");
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
    fullCodeMode: true, mcp: { enabled: false }, memory: { enabled: false },
    agents: { enabled: false }, residency: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false }, mesh: { enabled: true, announce: true },
  }));
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) }, on: vi.fn(), getThinkingLevel: () => "off", sendMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = (sessionId: string, sessionCwd = cwd) => ({
    cwd: sessionCwd, mode: "rpc", hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getEntries: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext);
  const state = new FabricState(pi, new CapturedToolCatalog());
  states.push(state);
  const published = (sessionId: string) => readParticipantFiles(meshRoot, { maxAgeMs: 0 })
    .map(entry => entry.value).find(value => (value as { id?: string }).id === `session:${sessionId}`);
  return { cwd, state, pi, context, published };
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
