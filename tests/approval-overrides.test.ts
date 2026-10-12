import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApprovalController, FabricSessionApprovals } from "../src/core/approval-controller.js";
import { actionApprovalOverride, approvalActionOverridesValue } from "../src/core/approval-overrides.js";
import type { ResolvedFabricAction } from "../src/core/action-registry.js";
import { normalizeFabricConfig, type FabricApprovalConfig } from "../src/config.js";

const action = (ref: string, risk: ResolvedFabricAction["risk"] = "write"): ResolvedFabricAction => {
  const [provider, ...rest] = ref.split(".");
  return { ref, provider: provider!, name: rest.join("."), description: "Demo", inputSchema: {}, risk };
};

const policy = (actions: FabricApprovalConfig["actions"], mode: FabricApprovalConfig["write"] = "allow"): FabricApprovalConfig => ({
  read: "allow", write: mode, execute: mode, network: mode, agent: mode,
  ...(actions ? { actions } : {}),
});

const uiContext = (choice: string) => ({
  hasUI: true,
  mode: "tui",
  ui: { custom: vi.fn(async () => choice), notify: vi.fn() },
} as unknown as ExtensionContext);

afterEach(() => { vi.unstubAllEnvs(); });

describe("approvals.actions validation", () => {
  it("accepts exact refs and provider wildcards", () => {
    expect(approvalActionOverridesValue({
      "delegate.dispatch": "deny",
      "delegate.*": "ask",
      "mcp.github.search_issues": "allow",
    })).toEqual({ "delegate.dispatch": "deny", "delegate.*": "ask", "mcp.github.search_issues": "allow" });
    expect(approvalActionOverridesValue(undefined)).toBeUndefined();
    expect(approvalActionOverridesValue({})).toBeUndefined();
  });

  it.each([
    ["*", "deny"],
    ["delegate", "deny"],
    ["delegate.dis*", "deny"],
    ["*.dispatch", "deny"],
    ["mcp.github.*", "deny"],
    ["Delegate.dispatch", "deny"],
    ["delegate. dispatch", "deny"],
    ["delegate.dispatch", "auto"],
    ["delegate.dispatch", true],
  ])("rejects %s → %s", (key, mode) => {
    expect(() => approvalActionOverridesValue({ [key]: mode })).toThrow(/approvals\.actions/);
  });

  it("rejects non-object and oversized maps", () => {
    expect(() => approvalActionOverridesValue([])).toThrow("approvals.actions must be an object");
    const many = Object.fromEntries(Array.from({ length: 257 }, (_, index) => [`p${index}.a`, "allow"]));
    expect(() => approvalActionOverridesValue(many)).toThrow("at most 256");
  });

  it("normalizes into config and fails closed on invalid keys", () => {
    expect(normalizeFabricConfig({}).approvals.actions).toBeUndefined();
    expect(normalizeFabricConfig({ approvals: { actions: { "pi.bash": "deny" } } }).approvals.actions)
      .toEqual({ "pi.bash": "deny" });
    expect(() => normalizeFabricConfig({ approvals: { actions: { "pi.b*": "deny" } } })).toThrow();
  });

  it("resolves exact before wildcard", () => {
    const overrides = { "delegate.*": "ask", "delegate.status": "allow" } as const;
    expect(actionApprovalOverride(overrides, "delegate.status")).toBe("allow");
    expect(actionApprovalOverride(overrides, "delegate.dispatch")).toBe("ask");
    expect(actionApprovalOverride(overrides, "other.dispatch")).toBeUndefined();
    expect(actionApprovalOverride(undefined, "delegate.dispatch")).toBeUndefined();
    expect(actionApprovalOverride({ "toString.*": "deny" }, "constructor")).toBeUndefined();
  });
});

describe("ApprovalController per-action overrides", () => {
  it("lets exact beat wildcard beat the risk-class mode", async () => {
    const config = policy({ "delegate.*": "deny", "delegate.status": "allow" }, "ask");
    const controller = new ApprovalController(config, { hasUI: false } as ExtensionContext);
    await expect(controller.approve(action("delegate.status"))).resolves.toBeUndefined();
    await expect(controller.approve(action("delegate.dispatch"))).rejects.toThrow("approvals.actions policy");
    // No override: the risk-class ask still applies (and fails closed headless).
    await expect(controller.approve(action("other.write"))).rejects.toThrow("no interactive UI");
  });

  it("asks for an overridden action even when its risk class is allowed", async () => {
    const context = uiContext("allow-once");
    const controller = new ApprovalController(policy({ "delegate.dispatch": "ask" }), context);
    await controller.approve(action("delegate.dispatch"));
    await controller.approve(action("delegate.status"));
    expect(context.ui.custom).toHaveBeenCalledTimes(1);
    await expect(new ApprovalController(policy({ "delegate.dispatch": "ask" }), { hasUI: false } as ExtensionContext)
      .approve(action("delegate.dispatch"))).rejects.toThrow("no interactive UI");
  });

  it("keeps an action deny absolute over session and inherited risk grants", async () => {
    const session = new FabricSessionApprovals();
    session.approvedRisks.add("write");
    vi.stubEnv("PI_FABRIC_GRANTED_RISKS", "write");
    const controller = new ApprovalController(policy({ "pi.bash": "deny" }), uiContext("allow-once"), session);
    await expect(controller.approve(action("pi.bash"))).rejects.toThrow("pi.bash is denied");
    await expect(controller.approve(action("pi.write"))).resolves.toBeUndefined();
  });

  it("lets an action allow lift a denied risk class", async () => {
    const controller = new ApprovalController(policy({ "mcp.docs.search": "allow" }, "deny"), { hasUI: false } as ExtensionContext);
    await expect(controller.approve(action("mcp.docs.search", "network"))).resolves.toBeUndefined();
    await expect(controller.approve(action("mcp.docs.fetch", "network"))).rejects.toThrow("denied by the Fabric network policy");
  });
});
