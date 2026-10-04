import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { assertNativeRolePair, assertNativeRoleTools, assertNativeRoleParticipant, snapshotNativeRoleBinding } from "../src/agents/native-role-binding.js";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { installNativeRoleBinding, nativeRoleBinding, registerNativeRoleAttester, attestNativeRoleParticipant } from "../src/agents/native-role-binding.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const tools = ["read", "grep", "find", "ls", "bash", "write"];
const binding = (role = "review-agent") => snapshotNativeRoleBinding({ role, model: "cliproxyapi/gpt-6.1-sol", thinking: "max", tools });
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const manager = (preparePiModel?: (model: string | undefined) => Promise<string | void>) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-role-")); roots.push(root);
  const result = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0,
    defaultTools: ["read"], model: "provider/global", thinking: "medium" }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    ...(preparePiModel ? { preparePiModel } : {}),
  });
  managers.push(result); return result;
};

describe("native role contract", () => {
  it("ignores public binding/name/environment impostors during normalization", () => {
    vi.stubEnv("PI_FABRIC_ACTOR_NAME", "review-agent"); vi.stubEnv("SMARTY_ROLE", "security-agent");
    const request = normalizeAgentRunRequest({ task: "review as security-agent", name: "review-agent", nativeRoleBinding: binding() },
      { runner: "pi", timeoutMs: 1000 });
    expect(request).not.toHaveProperty("nativeRoleBinding");
    expect(request.tools).toBeUndefined(); expect(request.model).toBeUndefined(); expect(request.thinking).toBeUndefined();
  });
  it("freezes a detached native set and compares sets, not order/multiplicity or fabric_exec", () => {
    const source = { role: "review-agent", model: "cliproxyapi/gpt-6.1-sol", thinking: "max", tools: [...tools] };
    const frozen = snapshotNativeRoleBinding(source); source.tools.pop(); source.model = "provider/changed";
    expect(frozen).toEqual(binding()); expect(Object.isFrozen(frozen)).toBe(true); expect(Object.isFrozen(frozen.tools)).toBe(true);
    expect(() => assertNativeRoleTools(frozen, [...tools].reverse().concat("bash", "fabric_exec"), "requested")).not.toThrow();
  });
  it.each([undefined, {}, { ...binding(), role: "Main" }, { ...binding(), model: "alias" },
    { ...binding(), thinking: "turbo" }, { ...binding(), tools: tools.filter(tool => tool !== "bash") },
    { ...binding(), tools: [...tools, "fabric_exec"] }])("refuses invalid host authority %j", value => {
    expect(() => snapshotNativeRoleBinding(value)).toThrow("NATIVE_ROLE_BINDING_MISMATCH");
  });
  it.each([{ actual: tools.filter(tool => tool !== "bash") }, { actual: [...tools, "edit"] }, { actual: ["fabric_exec"] }])("refuses a differing native set %j", ({ actual }) => {
    expect(() => assertNativeRoleTools(binding(), actual, "delivered")).toThrow("native tools must be exactly");
  });
  it.each([["other/model", "max"], ["cliproxyapi/gpt-6.1-sol", "medium"], [undefined, undefined]])("refuses pair mismatch %s/%s", (model, thinking) => {
    expect(() => assertNativeRolePair(binding(), model, thinking)).toThrow("NATIVE_ROLE_BINDING_MISMATCH");
  });
  it.each([{ id: "parent-actor" }, { kind: "actor" }, { model: "provider/wrong" }, { thinking: "medium" }, { thinking: undefined }, { stale: true }])("refuses contradictory self metadata %j", patch => {
    const participant = { id: "child", kind: "agent", model: binding().model, thinking: "max", stale: false, ...patch } as FabricParticipantInfo;
    expect(() => assertNativeRoleParticipant(binding(), participant, "child", "agent")).toThrow("NATIVE_ROLE_BINDING_MISMATCH");
  });
});

describe("native activation authority over supplied host interfaces", () => {
  const hosts = () => {
    const listeners = new Map<string, Set<(data: unknown) => void>>();
    const wrapper = (): ExtensionAPI["events"] => ({
      emit(channel, data) { for (const listener of listeners.get(channel) ?? []) listener(data); },
      on(channel, handler) {
        const set = listeners.get(channel) ?? new Set(); listeners.set(channel, set); set.add(handler);
        return () => { set.delete(handler); };
      },
    });
    return { hook: wrapper(), runtime: wrapper() };
  };
  it("shares frozen native authority across distinct hook/runtime host wrappers, not module-global identity", async () => {
    const { hook, runtime } = hosts();
    expect(nativeRoleBinding(runtime)).toBeUndefined();
    installNativeRoleBinding(hook, binding());
    expect(nativeRoleBinding(runtime)).toEqual(binding());
    expect(Object.isFrozen(nativeRoleBinding(runtime))).toBe(true);
    const self = { id: "child", kind: "agent", model: binding().model, thinking: "max", stale: false } as FabricParticipantInfo;
    registerNativeRoleAttester(runtime, async () => self);
    expect(await attestNativeRoleParticipant(hook, binding().model, "max")).toBe(self);
    expect(() => installNativeRoleBinding(runtime, binding())).toThrow("already installed");
  });
  it("fails closed if the native metadata adapter is absent", async () => {
    const { hook } = hosts(); installNativeRoleBinding(hook, binding());
    await expect(attestNativeRoleParticipant(hook, binding().model, "max")).rejects.toThrow("attester is unavailable");
  });
  it("does not silently choose one of two native authority claims", () => {
    const { hook, runtime } = hosts(); installNativeRoleBinding(hook, binding());
    runtime.on("fabric.native-role.activation-query", query => {
      (query as { accept(value: unknown): void }).accept({ binding: binding("security-agent") });
    });
    expect(() => nativeRoleBinding(runtime)).toThrow("multiple native activation authorities");
  });
});

describe("manager native role admission", () => {
  it.each(["review-agent", "security-agent"])("%s omissions inherit role tools/model/effort rather than weakened global defaults", async role => {
    const agents = manager();
    const result = await agents.run({ task: "REPORT_FABRIC_SURFACE", nativeRoleBinding: binding(role) });
    expect(result).toMatchObject({ status: "completed", model: binding().model, thinking: "max" });
    expect((result as typeof result & { tools: string[] }).tools.filter(tool => tool !== "fabric_exec")).toEqual(tools);
  });
  it.each(["review-agent", "security-agent"])("%s fails after a restrictive inherited ceiling, never widening it", async role => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", JSON.stringify(tools.filter(tool => tool !== "bash")));
    const agents = manager();
    await expect(agents.spawn({ task: "must not launch", nativeRoleBinding: binding(role), tools })).rejects.toThrow("after inherited ceiling");
    expect(agents.list()).toEqual([]);
  });
  it.each([{ tools: [...tools, "edit"] }, { tools: tools.filter(tool => tool !== "bash") },
    { model: "provider/global" }, { thinking: "medium" as const }, { runner: "claude" as const }, { extensions: false }])("rejects explicit mismatch before launch: %j", async patch => {
    const agents = manager();
    await expect(agents.spawn({ task: "must not launch", nativeRoleBinding: binding(), ...patch })).rejects.toThrow("NATIVE_ROLE_BINDING_MISMATCH");
    expect(agents.list()).toEqual([]);
  });
  it("rejects the final prepared model rather than silently rebinding", async () => {
    const prepare = vi.fn(async () => "provider/prepared-other"); const agents = manager(prepare);
    await expect(agents.spawn({ task: "must not launch", nativeRoleBinding: binding() })).rejects.toThrow("provider/prepared-other");
    expect(prepare).toHaveBeenCalled(); expect(agents.list()).toEqual([]);
  });
  it("keeps ordinary global defaults and ceiling narrowing behavior", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", '["read"]'); const agents = manager();
    const result = await agents.run({ task: "REPORT_FABRIC_SURFACE", name: "security-agent", tools: ["read", "bash"] });
    expect(result).toMatchObject({ status: "completed", model: "provider/global", thinking: "medium" });
    expect((result as typeof result & { tools: string[] }).tools.filter(tool => tool !== "fabric_exec")).toEqual(["read"]);
  });
});
