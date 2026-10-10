import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { herdrPaneId, readHerdrAgentName } from "../src/topology/herdr-name.js";
import { rootParticipantName } from "../src/topology/participant-name.js";

// smarty-dev#6758: Herdr names and panes are Main display metadata, not new routing targets.

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
    const bin = stubHerdr(dir, "exec sleep 5");
    const started = Date.now();
    expect(await readHerdrAgentName({ HERDR_PANE_ID: "w3Q:p1", HERDR_BIN_PATH: bin })).toBeUndefined();
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
  it("publishes display names and panes, reads Herdr once per start/reload, and rejects name-prefixed targets", async () => {
    const root = temp();
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const herdrBody = (bName: string) => [
      'case "$3" in',
      `  w1:p1) echo '${agentJson("lane-a")}' ;;`,
      `  w2:p1) echo '${agentJson(bName)}' ;;`,
      `  w3:p1|w4:p1) echo '${agentJson("lane-dup")}' ;;`,
      `  w5:p1) echo '${agentJson("lane-e")}' ;;`,
      "  *) exit 1 ;;",
      "esac",
    ].join("\n");
    vi.stubEnv("HERDR_BIN_PATH", stubHerdr(root, herdrBody("lane-b")));
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
    const calls = () => fs.readFileSync(path.join(root, "calls"), "utf8").trim().split("\n");
    try {
      for (const [m, pane] of [[a, "w1:p1"], [b, "w2:p1"], [dup1, "w3:p1"], [dup2, "w4:p1"], [named, "w5:p1"]] as const) {
        vi.stubEnv("HERDR_PANE_ID", pane);
        await m.runtime.initialize(m.context, config);
      }
      await vi.waitFor(async () => expect(await a.invoke("agents.sessions")).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: a.id, name: "lane-a", herdrPane: "w1:p1" }),
        expect.objectContaining({ id: b.id, name: "lane-b", herdrPane: "w2:p1" }),
        expect.objectContaining({ id: dup1.id, name: "lane-dup", herdrPane: "w3:p1" }),
        expect.objectContaining({ id: dup2.id, name: "lane-dup", herdrPane: "w4:p1" }),
        // A valid Pi session name wins over the Herdr name.
        expect.objectContaining({ id: named.id, name: "pi-named", herdrPane: "w5:p1" }),
      ])), { timeout: 8000, interval: 100 });
      expect(calls().sort()).toEqual([1, 2, 3, 4, 5].map(n => `agent get w${n}:p1`));

      // A Herdr rename is not polled by directory reads/heartbeats; reload reads once again.
      stubHerdr(root, herdrBody("lane-renamed"));
      await b.invoke("agents.sessions");
      expect(await a.invoke("agents.sessions")).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: b.id, name: "lane-b", herdrPane: "w2:p1" }),
      ]));
      expect(calls()).toHaveLength(5);
      await b.runtime.shutdown("reload");
      vi.stubEnv("HERDR_PANE_ID", "w2:p1");
      await b.runtime.initialize(b.context, config);
      await vi.waitFor(async () => expect(await a.invoke("agents.sessions")).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: b.id, name: "lane-renamed", herdrPane: "w2:p1" }),
      ])), { timeout: 8000, interval: 100 });
      expect(calls()).toHaveLength(6);
      expect(calls().filter(call => call === "agent get w2:p1")).toHaveLength(2);

      // Use the real public provider/router: name:x is an ordinary unknown target,
      // even if its suffix is an existing, duplicate, or principal display name.
      const commandsBefore = a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
      for (const action of ["followUp", "steer", "tell"] as const) {
        for (const id of ["name:x", "name:lane-renamed", "name:lane-a", "name:lane-dup", "name:org"]) {
          const error = await a.invoke(`agents.${action}`, { id, message: "never by name prefix" }).catch(error => error) as Error & { code?: string };
          expect(error).toBeInstanceOf(Error);
          expect(error.name).toBe("Error");
          expect(error.message).toBe(`Unknown Fabric participant: ${id} (no record on this mesh root: the session has ended, has not joined yet, or uses another mesh root)`);
          expect(error.code).toBeUndefined();
        }
      }
      expect(a.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(commandsBefore);
      for (const m of all) expect(m.sendMessage).not.toHaveBeenCalled();

      // Exact session targets retain ordinary delivery despite duplicate display labels.
      await expect(a.invoke("agents.followUp", { id: dup1.id, message: "exact session still works" }))
        .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
      expect(dup1.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
        content: expect.stringContaining("exact session still works"),
      }), expect.objectContaining({ deliverAs: "followUp" }));
      expect(dup2.sendMessage).not.toHaveBeenCalled();
    } finally {
      for (const m of all.reverse()) await m.runtime.shutdown();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 40_000);

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
