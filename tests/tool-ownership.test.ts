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

// Every fork active-set scenario is retained at Pi 0.99's model-declaration boundary.
// Tools remain active/callable; hidden declarations own model visibility instead.
describe("fork ownership scenarios through native loadouts", () => {
  const hidden = (names: string[], full = true, visible = new Set(["fabric_exec"])) =>
    fabricToolLoadout({ registered: names.map(name => ({ name })) } as never, full, visible)?.hiddenDeclarations ?? [];
  it("gives Fabric exclusive ownership of active Pi core tools", () => {
    const names = ["read", "bash", "grep", "custom_tool", "fabric_exec"];
    expect(hidden(names, true, new Set(["fabric_exec", "custom_tool"]))).toEqual(["read", "bash", "grep"]);
  });
  it("restores only the native core tools that were active before full mode", () => {
    const state = hostWith(["read", "find", "custom_tool"]);
    const owner = new FabricToolOwnership(state.host); owner.apply(true); owner.apply(false);
    expect(state.active()).toEqual(["read", "find", "custom_tool", "fabric_exec"]);
    expect(state.active()).not.toContain("bash");
    expect(hidden(state.active(), false)).toEqual([]);
  });
  it("removes core tools re-enabled while full mode remains active", () => {
    expect(hidden(["fabric_exec", "read", "ls"])).toEqual(["read", "ls"]);
  });
  it("does not alter native tools in orchestration-only mode", () => {
    expect(hidden(["read", "bash", "fabric_exec"], false)).toEqual([]);
  });
  it("hides captured extension tools from the model in full code mode", () => {
    expect(hidden(["read", "ask_user_question", "deploy_release", "fabric_exec"]))
      .toEqual(["read", "ask_user_question", "deploy_release"]);
  });
  it("rehides extension tools that a refresh re-activated while full mode stays active", () => {
    expect(hidden(["fabric_exec", "ask_user_question"])).toEqual(["ask_user_question"]);
  });
  it("re-exposes extension tools removed from the hidden set while full mode stays active", () => {
    const names = ["read", "ask_user_question", "deploy_release", "fabric_exec"];
    expect(hidden(names, true, new Set(["fabric_exec", "ask_user_question"]))).toEqual(["read", "deploy_release"]);
    const fabric = { name: "fabric_exec", description: "Fabric", parameters: { type: "object" } };
    const ask = { ...fabric, name: "ask_user_question" };
    expect(fabricModelContext([{ role: "system", content: "", timestamp: 1 }] as never, fabric, [ask])[0])
      .toMatchObject({ toolsAdded: [fabric, ask] });
  });
  it("restores hidden extension tools when full code mode is released", () => {
    const names = ["read", "ask_user_question", "fabric_exec"];
    expect(hidden(names)).toEqual(["read", "ask_user_question"]);
    expect(hidden(names, false)).toEqual([]);
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
