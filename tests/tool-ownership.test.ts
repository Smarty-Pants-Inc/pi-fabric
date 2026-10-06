import { describe, expect, it, vi } from "vitest";
import {
  createToolOwnershipReassertion,
  fabricToolLoadout,
  fabricModelContext,
  FabricToolOwnership,
} from "../src/core/tool-ownership.js";

const hostWith = (initial: string[]) => {
  let active = [...initial];
  const setActiveTools = vi.fn((names: string[]) => {
    active = [...names];
  });
  return {
    host: {
      getActiveTools: () => [...active],
      setActiveTools,
    },
    active: () => active,
    setActiveTools,
  };
};

describe("FabricToolOwnership", () => {
  it("keeps direct tools callable while ensuring the orchestrator stays active", () => {
    const state = hostWith(["read", "bash", "custom_tool"]);
    const ownership = new FabricToolOwnership(state.host);
    expect(ownership.apply(true)).toBe(true);
    expect(state.active()).toEqual(["read", "bash", "custom_tool", "fabric_exec"]);
    expect(ownership.apply(true)).toBe(false);
    expect(state.setActiveTools).toHaveBeenCalledOnce();
    expect(ownership.release()).toBe(false);
    expect(ownership.apply(false)).toBe(false);
    expect(state.active()).toContain("read");
  });

  it("re-activates Fabric without undoing another extension's selection", () => {
    const state = hostWith(["fabric_exec"]);
    const ownership = new FabricToolOwnership(state.host);
    state.host.setActiveTools(["read", "late_mcp"]);
    ownership.apply(true, new Set(["late_mcp"]));
    expect(state.active()).toEqual(["read", "late_mcp", "fabric_exec"]);
  });

  it("does not alter tools in orchestration-only mode", () => {
    const state = hostWith(["read"]);
    expect(new FabricToolOwnership(state.host).apply(false)).toBe(false);
    expect(state.setActiveTools).not.toHaveBeenCalled();
  });

  it("hides every other declaration, including historical, deferred and native orchestrators", () => {
    const names = ["fabric_exec", "read", "codemode", "tool_search", "late_mcp", "deferred", "withdrawn"];
    const registered = names.map((name) => ({ name }));
    const loadout = { registered, declared: registered.slice(0, 4) } as unknown as Parameters<typeof fabricToolLoadout>[0];
    expect(fabricToolLoadout(loadout, true)?.hiddenDeclarations).toEqual(names.slice(1));
    expect(fabricToolLoadout(loadout, false)).toBeUndefined();
  });
});

// Pi 1.0 preserves active tools for nested invocation and gates model declarations
// instead. Retain the original fleet visibility regressions at that boundary.
const fleetModelTools = (state: ReturnType<typeof hostWith>, exclusive: boolean, foreground: string[] = []) => {
  const loadout = { registered: state.active().map(name => ({ name })) } as unknown as Parameters<typeof fabricToolLoadout>[0];
  const hidden = new Set(fabricToolLoadout(loadout, exclusive, foreground)?.hiddenDeclarations ?? []);
  return state.active().filter(name => !hidden.has(name));
};
describe("fleet full-code visibility on native loadouts", () => {
  it("gives Fabric exclusive ownership of active Pi core tools", () => {
    const state = hostWith(["read", "bash", "grep", "custom_tool"]); const ownership = new FabricToolOwnership(state.host);
    ownership.apply(true);
    expect(fleetModelTools(state, true, ["custom_tool"])).toEqual(["custom_tool", "fabric_exec"]);
    expect(state.active()).toContain("read"); // nested tools remain callable, not model-declared
  });
  it("restores only the native core tools that were active before full mode", () => {
    const state = hostWith(["read", "find", "custom_tool"]); const ownership = new FabricToolOwnership(state.host);
    ownership.apply(true); expect(fleetModelTools(state, true, ["custom_tool"])).toEqual(["custom_tool", "fabric_exec"]);
    ownership.release(); expect(fleetModelTools(state, false)).toEqual(["read", "find", "custom_tool", "fabric_exec"]);
    expect(state.active()).not.toContain("bash");
  });
  it("removes core tools re-enabled while full mode remains active", () => {
    const state = hostWith(["read", "fabric_exec"]); const ownership = new FabricToolOwnership(state.host);
    ownership.apply(true); state.host.setActiveTools(["fabric_exec", "read", "ls"]); ownership.apply(true);
    expect(fleetModelTools(state, true)).toEqual(["fabric_exec"]);
  });
  it("does not alter native tools in orchestration-only mode", () => {
    const state = hostWith(["read", "bash", "fabric_exec"]); new FabricToolOwnership(state.host).apply(false);
    expect(fleetModelTools(state, false)).toEqual(["read", "bash", "fabric_exec"]); expect(state.setActiveTools).not.toHaveBeenCalled();
  });
  it("hides captured extension tools from the active set in full code mode", () => {
    const state = hostWith(["read", "ask_user_question", "deploy_release"]); const ownership = new FabricToolOwnership(state.host);
    ownership.apply(true, new Set(["ask_user_question", "deploy_release"]));
    expect(fleetModelTools(state, true)).toEqual(["fabric_exec"]);
    expect(state.active()).toContain("ask_user_question"); // active registry is not the model loadout on Pi 1.0
  });
  it("rehides extension tools that a refresh re-activated while full mode stays active", () => {
    const state = hostWith(["fabric_exec"]); const ownership = new FabricToolOwnership(state.host);
    state.host.setActiveTools(["fabric_exec", "ask_user_question"]); ownership.apply(true, new Set(["ask_user_question"]));
    expect(fleetModelTools(state, true)).toEqual(["fabric_exec"]);
  });
  it("re-exposes extension tools removed from the hidden set while full mode stays active", () => {
    const state = hostWith(["read", "ask_user_question", "deploy_release"]); new FabricToolOwnership(state.host).apply(true);
    expect(fleetModelTools(state, true)).toEqual(["fabric_exec"]);
    // New foreground policy is explicit; no silent re-exposure from catalog drift.
    expect(fleetModelTools(state, true, ["ask_user_question"])).toEqual(["ask_user_question", "fabric_exec"]);
    expect(fleetModelTools(state, true)).not.toContain("deploy_release");
  });
  it("restores hidden extension tools when full code mode is released", () => {
    const state = hostWith(["read", "ask_user_question"]); const ownership = new FabricToolOwnership(state.host);
    ownership.apply(true); expect(fleetModelTools(state, true)).toEqual(["fabric_exec"]);
    ownership.release(); expect(fleetModelTools(state, false)).toEqual(["read", "ask_user_question", "fabric_exec"]);
  });
});

describe("fabricModelContext", () => {
  it("removes historical additions/removals without changing messages or prompt sections", () => {
    const tool = { name: "fabric_exec", description: "Fabric", parameters: { type: "object", properties: {} } };
    const messages = [
      { role: "system", content: "system", sections: { guidelines: "keep" }, toolsAdded: [{ ...tool, name: "read" }], timestamp: 1 },
      { role: "user", content: "request", timestamp: 2 },
      { role: "system", content: "delta", toolsAdded: [{ ...tool, name: "late" }], toolsRemoved: [{ name: "fabric_exec" }], timestamp: 3 },
    ] as Parameters<typeof fabricModelContext>[0];
    const result = fabricModelContext(messages, tool);
    expect(result[0]).toMatchObject({ content: "system", sections: { guidelines: "keep" }, toolsAdded: [tool] });
    expect(result[1]).toBe(messages[1]);
    expect(result[2]).toEqual({ role: "system", content: "delta", timestamp: 3 });
    expect(messages[2]).toHaveProperty("toolsRemoved");
  });
});

describe("createToolOwnershipReassertion", () => {
  it("no-ops scheduled reassertions that run before the host is ready", async () => {
    // Registry rebuilds fire during extension load, before session_start
    // initializes Fabric state; the deferred reassertion must not read config.
    let ready = false;
    const apply = vi.fn();
    const { schedule } = createToolOwnershipReassertion({
      ready: () => ready,
      active: () => true,
      hiddenNames: () => new Set(["ask_user_question"]),
      apply,
    });

    schedule();
    await Promise.resolve();
    expect(apply).not.toHaveBeenCalled();

    ready = true;
    schedule();
    await Promise.resolve();
    expect(apply).toHaveBeenCalledOnce();
    expect(apply).toHaveBeenCalledWith(new Set(["ask_user_question"]));
  });

  it("dedupes simultaneous schedules and skips reassertion while inactive", async () => {
    let active = false;
    const apply = vi.fn();
    const { reassert, schedule } = createToolOwnershipReassertion({
      ready: () => true,
      active: () => active,
      hiddenNames: () => new Set(["deploy_release"]),
      apply,
    });

    schedule();
    schedule();
    await Promise.resolve();
    expect(apply).not.toHaveBeenCalled();

    active = true;
    reassert();
    expect(apply).toHaveBeenCalledOnce();
  });
});
