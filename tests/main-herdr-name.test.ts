import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { FabricTellNameTargetError } from "../src/providers/agents-message-router.js";
import { herdrPaneId, readHerdrAgentName } from "../src/topology/herdr-name.js";
import { rootParticipantName } from "../src/topology/participant-name.js";
import { claimMainName, mainNameBindingKey, readMainNameBinding, MAIN_NAME_REBINDING_PREFIX } from "../src/topology/main-name-binding.js";

// smarty-dev#6758: an unnamed Main takes its Herdr agent name; name:<x> selects one live Main.

const temp = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-herdr-name-"));

/** A stub `herdr` that answers `herdr agent get <pane>` from a pane -> stdout script. */
const stubHerdr = (dir: string, body: string): string => {
  const file = path.join(dir, "herdr");
  fs.writeFileSync(file, `#!/bin/sh\necho "$@" >> "${path.join(dir, "calls")}"\n${body}\n`, { mode: 0o755 });
  return file;
};
const agentJson = (name: unknown) => JSON.stringify({ id: "cli:agent:get", result: { type: "agent_info", agent: { name, pane_id: "x" } } });

afterEach(() => { vi.unstubAllEnvs(); });

describe.skipIf(process.platform === "win32")("readHerdrAgentName", () => {
  it("reads result.agent.name for HERDR_PANE_ID through HERDR_BIN_PATH", async () => {
    const dir = temp();
    const bin = stubHerdr(dir, `echo '${agentJson("lane-a")}'`);
    expect(await readHerdrAgentName({ HERDR_PANE_ID: "w3Q:p1", HERDR_BIN_PATH: bin })).toBe("lane-a");
    expect(fs.readFileSync(path.join(dir, "calls"), "utf8")).toBe("agent get w3Q:p1\n");
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it.each([
    ["an invalid name", `echo '${agentJson("bad/name")}'`],
    ["a non-string name", `echo '${agentJson(42)}'`],
    ["malformed JSON", "echo not-json"],
    ["a failing command", "exit 3"],
  ])("keeps main on %s", async (_label, body) => {
    const dir = temp();
    const bin = stubHerdr(dir, body);
    expect(await readHerdrAgentName({ HERDR_PANE_ID: "w3Q:p1", HERDR_BIN_PATH: bin })).toBeUndefined();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("keeps main when herdr is slow, within the timeout", async () => {
    const dir = temp();
    const bin = stubHerdr(dir, `sleep 5; echo '${agentJson("late")}'`);
    const started = Date.now();
    expect(await readHerdrAgentName({ HERDR_PANE_ID: "w3Q:p1", HERDR_BIN_PATH: bin }, 200)).toBeUndefined();
    expect(Date.now() - started).toBeLessThan(3000);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("keeps main when herdr is missing or no pane is set, and never runs a command without a pane", async () => {
    const dir = temp();
    expect(await readHerdrAgentName({ HERDR_PANE_ID: "w3Q:p1", HERDR_BIN_PATH: path.join(dir, "absent") })).toBeUndefined();
    const bin = stubHerdr(dir, `echo '${agentJson("lane-a")}'`);
    expect(await readHerdrAgentName({ HERDR_BIN_PATH: bin })).toBeUndefined();
    expect(await readHerdrAgentName({ HERDR_PANE_ID: "bad pane;rm", HERDR_BIN_PATH: bin })).toBeUndefined();
    expect(fs.existsSync(path.join(dir, "calls"))).toBe(false);
    expect(herdrPaneId({ HERDR_PANE_ID: " w3Q:p1 " })).toBe("w3Q:p1");
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("prefers a valid Pi session name, then the Herdr name, then main", () => {
    expect(rootParticipantName("pi-name", "lane-a")).toBe("pi-name");
    expect(rootParticipantName(undefined, "lane-a")).toBe("lane-a");
    expect(rootParticipantName("bad/name", " lane-a ")).toBe("lane-a");
    expect(rootParticipantName(undefined, "bad/name")).toBe("main");
    expect(rootParticipantName()).toBe("main");
  });
});

const main = (cwd: string, sessionId: string, sessionName?: string) => {
  const sendMessage = vi.fn();
  const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() },
    getThinkingLevel: () => "off", getSessionName: () => sessionName, sendMessage,
  } as unknown as ExtensionAPI;
  const context = { cwd, mode: "rpc", hasUI: false, isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => ({ ok: true }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(cwd, "unused-extension.mjs"), worker: path.join(cwd, "unused-worker.mjs"),
    residentHost: path.join(cwd, "unused-resident.mjs"), skills: cwd,
  } });
  const invoke = (ref: string, args: Record<string, unknown> = {}) => runtime.registry.invoke(ref, args, {
    cwd, signal: undefined, parentToolCallId: "herdr-name-probe", nestedToolCallId: ref,
    extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
  });
  return { runtime, context, invoke, sendMessage, id: `session:${sessionId}` };
};

describe.skipIf(process.platform === "win32")("Mains named by Herdr", () => {
  it("publishes Herdr names and panes in agents.sessions and routes name:<x> only to one live Main", async () => {
    const root = temp();
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("HERDR_BIN_PATH", stubHerdr(root, [
      'case "$3" in',
      `  w1:p1) echo '${agentJson("lane-a")}' ;;`,
      `  w2:p1) echo '${agentJson("lane-b")}' ;;`,
      `  w3:p1|w4:p1) echo '${agentJson("lane-dup")}' ;;`,
      `  w5:p1) echo '${agentJson("lane-e")}' ;;`,
      "  *) exit 1 ;;",
      "esac",
    ].join("\n")));
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const a = main(root, "aaaaaaaa-0000-4000-8000-000000000001");
    const b = main(root, "bbbbbbbb-0000-4000-8000-000000000002");
    const dup1 = main(root, "cccccccc-0000-4000-8000-000000000003");
    const dup2 = main(root, "dddddddd-0000-4000-8000-000000000004");
    const named = main(root, "eeeeeeee-0000-4000-8000-000000000005", "pi-named");
    const all = [a, b, dup1, dup2, named];
    try {
      for (const [m, pane] of [[a, "w1:p1"], [b, "w2:p1"], [dup1, "w3:p1"], [dup2, "w4:p1"], [named, "w5:p1"]] as const) {
        vi.stubEnv("HERDR_PANE_ID", pane);
        await m.runtime.initialize(m.context, config);
        if (m === dup1) await vi.waitFor(() => expect(readMainNameBinding(a.runtime.mesh, "lane-dup")?.sessionId)
          .toBe(dup1.id.slice("session:".length)), { timeout: 8000 });
      }
      await vi.waitFor(async () => expect(await a.invoke("agents.sessions")).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: a.id, name: "lane-a", herdrPane: "w1:p1" }),
        expect.objectContaining({ id: b.id, name: "lane-b", herdrPane: "w2:p1" }),
        expect.objectContaining({ id: dup1.id, name: "lane-dup", herdrPane: "w3:p1" }),
        expect.objectContaining({ id: dup2.id, name: "lane-dup", herdrPane: "w4:p1", nameBinding: "unbound" }),
        // A valid Pi session name wins over the Herdr name.
        expect.objectContaining({ id: named.id, name: "pi-named", herdrPane: "w5:p1" }),
      ])), { timeout: 8000, interval: 100 });

      for (const action of ["followUp", "steer"] as const) {
        await expect(a.invoke(`agents.${action}`, { id: "name:lane-b", message: `to lane-b ${action}` }))
          .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
        expect(b.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
          customType: "pi-fabric-agent-message", content: expect.stringContaining(`to lane-b ${action}`),
          details: expect.objectContaining({ from: expect.objectContaining({ id: a.id }), delivery: action }),
        }), expect.objectContaining({ deliverAs: action }));
      }
      // A Main can address itself by its own Herdr name.
      await expect(a.invoke("agents.followUp", { id: "name:lane-a", message: "self" }))
        .resolves.toMatchObject({ routed: "main", queued: true });

      // tell remains actor-oriented: reject even a resolvable Main name before routing.
      const tellCommandsBefore = a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
      const deliveriesBefore = all.map(m => m.sendMessage.mock.calls.length);
      for (const target of [{ id: "name:lane-b" }, { to: "name:lane-b" },
        { id: " name:lane-a " }, { id: "name:nobody" }, { id: "name:org" }]) {
        const error = await a.invoke("agents.tell", { ...target, message: "never via tell" }).catch(error => error);
        expect(error).toBeInstanceOf(FabricTellNameTargetError);
        expect(error).toMatchObject({ code: "FABRIC_TELL_NAME_TARGET_UNSUPPORTED",
          message: expect.stringContaining("agents.steer or agents.followUp") });
      }
      expect(a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(tellCommandsBefore);
      expect(all.map(m => m.sendMessage.mock.calls.length)).toEqual(deliveriesBefore);

      const commandsBefore = a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
      const ambiguous = await a.invoke("agents.followUp", { id: "name:lane-dup", message: "never guess" })
        .catch((error: unknown) => error) as Error & { code?: string };
      expect(ambiguous.message).toContain("Ambiguous Fabric Main name: name:lane-dup");
      for (const text of [dup1.id, dup2.id, "pane w3:p1", "pane w4:p1"]) expect(ambiguous.message).toContain(text);
      const absent = await a.invoke("agents.steer", { id: "name:nobody", message: "never guess" })
        .catch((error: unknown) => error) as Error;
      expect(absent.message).toContain("No live Fabric Main is named nobody");
      // The Herdr name of a Pi-named Main is not a selector.
      await expect(a.invoke("agents.followUp", { id: "name:lane-e", message: "x" })).rejects.toThrow("No live Fabric Main");
      expect(a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(commandsBefore);
      for (const m of [dup1, dup2, named]) expect(m.sendMessage).not.toHaveBeenCalled();

      // Existing exact targets are unchanged.
      await expect(a.invoke("agents.followUp", { id: dup1.id, message: "exact id" }))
        .resolves.toMatchObject({ routed: "mesh", acknowledged: true });

      // Removing presence is NOT positive process death (these test Mains share a pid).
      await dup1.runtime.shutdown();
      const beforeSquat = a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
      await expect(a.invoke("agents.followUp", { id: "name:lane-dup", message: "squat during absence" }))
        .rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_ABSENT", boundSessionId: dup1.id });
      expect(a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(beforeSquat);
      expect(dup2.sendMessage).not.toHaveBeenCalled();
      // A readable start-time mismatch is the PID-reuse death witness, not lease expiry.
      const old = readMainNameBinding(a.runtime.mesh, "lane-dup")!;
      const newcomerIdentity = { id: dup2.id, name: "main", kind: "main" as const };
      await a.runtime.mesh.put({ key: mainNameBindingKey("lane-dup"), identity: newcomerIdentity,
        value: { ...old, owner: { ...old.owner, processStartedAt: "0" } } });
      const newcomer = (await a.invoke("agents.sessions") as import("../src/topology/types.js").FabricParticipantInfo[])
        .find((s) => s.id === dup2.id)!;
      await claimMainName(a.runtime.mesh, newcomerIdentity, newcomer);
      expect(a.runtime.mesh.listAll(MAIN_NAME_REBINDING_PREFIX)).toHaveLength(1);
      await expect(a.invoke("agents.followUp", { id: "name:lane-dup", message: "only live" }))
        .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
      expect(dup2.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
        content: expect.stringContaining("only live") }), expect.anything());
    } finally {
      for (const m of all.reverse()) await m.runtime.shutdown();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 40_000);

  it.each([[false, "reload"], [false, "exit"], [true, "reload"], [true, "exit"]] as const)(
    "refuses a same-named newcomer during %s/%s, keeps it unbound in sessions, and forbids principal selectors", async (files, reason) => {
      const root = temp();
      for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
      vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
      vi.stubEnv("HERDR_PANE_ID", undefined);
      const meshRoot = path.join(root, "mesh");
      const config = normalizeFabricConfig({ fullCodeMode: false,
        mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
        agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
        mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
        prewalk: { enabled: false, alwaysRearm: false },
      });
      const lead = main(root, "aaaaaaaa-0000-4000-8000-000000000001", "lead");
      const newcomer = main(root, "bbbbbbbb-0000-4000-8000-000000000002", "lead");
      const sender = main(root, "cccccccc-0000-4000-8000-000000000003", "sender");
      const principal = main(root, "dddddddd-0000-4000-8000-000000000004", "org");
      try {
        if (files) {
          const { MeshStore } = await import("../src/mesh/store.js");
          const { LIVENESS_POLICY_KEY } = await import("../src/topology/host-leases.js");
          await new MeshStore(meshRoot, 64 * 1024, 1000).put({ key: LIVENESS_POLICY_KEY,
            identity: { id: lead.id, name: "main", kind: "main" },
            value: { version: 1, participants: "files", hostLeases: "files" } });
        }
        await lead.runtime.initialize(lead.context, config);
        await sender.runtime.initialize(sender.context, config);
        await lead.runtime.shutdown(reason);
        await newcomer.runtime.initialize(newcomer.context, config);
        expect(await sender.invoke("agents.sessions")).toEqual(expect.arrayContaining([
          expect.objectContaining({ id: newcomer.id, name: "lead", nameBinding: "unbound" }),
        ]));
        const before = sender.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
        for (const action of ["steer", "followUp"] as const) {
          await expect(sender.invoke(`agents.${action}`, { id: "name:lead", message: "never to newcomer" }))
            .rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_ABSENT", boundSessionId: lead.id });
        }
        expect(sender.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(before);
        expect(newcomer.sendMessage).not.toHaveBeenCalled();
        expect(lead.sendMessage).not.toHaveBeenCalled();
        await principal.runtime.initialize(principal.context, config);
        const principalBefore = sender.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
        await expect(sender.invoke("agents.steer", { id: "name:org", message: "not by name" }))
          .rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_PRINCIPAL" });
        expect(sender.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(principalBefore);
        expect(principal.sendMessage).not.toHaveBeenCalled();
        await expect(sender.invoke("agents.steer", { id: principal.id, message: "exact principal session" }))
          .resolves.toMatchObject({ acknowledged: true });
        expect(principal.sendMessage).toHaveBeenCalledOnce();
      } finally {
        await principal.runtime.shutdown(); await newcomer.runtime.shutdown(); await sender.runtime.shutdown();
        await lead.runtime.shutdown();
        fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      }
    }, 30_000);

  it("keeps main for a Main whose Herdr lookup fails and for a print-mode root", async () => {
    const root = temp();
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("HERDR_BIN_PATH", stubHerdr(root, `echo '${agentJson("lane-x")}'`));
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const printed = main(root, "ffffffff-0000-4000-8000-000000000006");
    (printed.context as { mode: string }).mode = "print";
    const failing = main(root, "abababab-0000-4000-8000-000000000007");
    try {
      vi.stubEnv("HERDR_PANE_ID", "w6:p1");
      await printed.runtime.initialize(printed.context, config);
      vi.stubEnv("HERDR_BIN_PATH", path.join(root, "absent-herdr"));
      await failing.runtime.initialize(failing.context, config);
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(fs.existsSync(path.join(root, "calls"))).toBe(false);
      const sessions = await failing.invoke("agents.sessions") as Array<{ id: string; name: string }>;
      expect(sessions.find((s) => s.id === failing.id)).toMatchObject({ name: "main", herdrPane: "w6:p1" });
    } finally {
      await failing.runtime.shutdown(); await printed.runtime.shutdown();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);
});
