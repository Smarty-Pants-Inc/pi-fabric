import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { resolveFabricIdentity } from "../src/host-compatibility.js";
import * as herdrModule from "../src/topology/herdr-name.js";
import { HERDR_NAME_RETRY_MS, herdrNameLookup, herdrPaneId, readHerdrAgentName } from "../src/topology/herdr-name.js";
import { rootParticipantName, validName } from "../src/topology/participant-name.js";

// smarty-dev#6758: Herdr names and panes are Main display metadata, not new routing targets.

const temp = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-herdr-name-"));

/** A stub `herdr` that answers `herdr agent get <pane>` from a pane -> stdout script. */
const stubHerdr = (dir: string, body: string): string => {
  const file = path.join(dir, "herdr");
  fs.writeFileSync(file, `#!/bin/sh\necho "$@" >> "${path.join(dir, "calls")}"\n${body}\n`, { mode: 0o755 });
  return file;
};
const agentJson = (name: unknown) => JSON.stringify({ id: "cli:agent:get", result: { type: "agent_info", agent: { name, pane_id: "x" } } });

// scripts/test-temp.ts scrubs the fleet's SMARTY_AGENT per file; tests that need one stub it.
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
});

// Pure name ordering: platform-independent, so outside the win32 skip.
describe("rootParticipantName", () => {
  it("prefers a valid Pi session name, then the Herdr name, then main", () => {
    expect(rootParticipantName("pi-name", "lane-a")).toBe("pi-name");
    expect(rootParticipantName(undefined, "lane-a")).toBe("lane-a");
    expect(rootParticipantName("bad/name", " lane-a ")).toBe("lane-a");
    expect(rootParticipantName(undefined, "bad/name")).toBe("main");
    expect(rootParticipantName()).toBe("main");
  });

  it("orders session, then SMARTY_AGENT, then Herdr, then main, validating each", () => {
    expect(rootParticipantName("pi-name", "lane-a", "agent-a")).toBe("pi-name");
    expect(rootParticipantName(undefined, "lane-a", "agent-a")).toBe("agent-a");
    expect(rootParticipantName("bad/name", "lane-a", " agent-a ")).toBe("agent-a");
    expect(rootParticipantName(undefined, "lane-a", "bad/agent")).toBe("lane-a");
    expect(rootParticipantName(undefined, undefined, "agent-a")).toBe("agent-a");
    expect(rootParticipantName(undefined, "bad/name", "")).toBe("main");
    // Counterexample for the runtime retry predicate: an explicit name literally "main" is a
    // valid name, so "display name is main" alone cannot mean "unnamed, retry Herdr".
    expect(rootParticipantName("main", undefined, undefined)).toBe("main");
    expect(rootParticipantName(undefined, undefined, "main")).toBe("main");
    expect(validName("main")).toBe("main");
  });
});

describe("herdrNameLookup bounded retry", () => {
  // smarty-dev#11077: one-shot timers at absolute offsets after start (not refresh pokes).
  // Fake both Date and setTimeout so offsets are exact and pending timers are countable.
  const START = 1_000;
  const OFFSETS = [5_000, 10_000, 20_000, 35_000, 55_000, 300_000];
  beforeEach(() => { vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"], now: START }); });
  afterEach(() => { vi.useRealTimers(); });
  /** A Herdr that names the pane only from `availableAt` ms after start (never when undefined). */
  const harness = (availableAt?: number) => {
    let alive = true;
    let eligible = true;
    const names: Array<[string, number]> = [];
    const readAt: number[] = [];
    const read = vi.fn(async () => {
      const elapsed = Date.now() - START;
      readAt.push(elapsed);
      return availableAt !== undefined && elapsed >= availableAt ? "lane-late" : undefined;
    });
    const lookup = herdrNameLookup({ read, live: () => alive, eligible: () => eligible,
      onName: (name) => { names.push([name, Date.now() - START]); } });
    return { lookup, read, readAt, names,
      advance: (ms: number) => vi.advanceTimersByTimeAsync(ms),
      retire: () => { alive = false; },
      setEligible: (value: boolean) => { eligible = value; } };
  };

  it("retries at absolute offsets 5, 10, 20, 35, 55s and 5min after start, then leaves no timer", async () => {
    expect(HERDR_NAME_RETRY_MS).toEqual(OFFSETS);
    const h = harness();
    h.lookup.start();
    await h.advance(0);
    expect(h.readAt).toEqual([0]);
    for (const offset of OFFSETS) {
      await h.advance(offset - 1 - (Date.now() - START));
      const before = h.read.mock.calls.length;
      await h.advance(1);
      expect(h.read).toHaveBeenCalledTimes(before + 1);
    }
    expect(h.readAt).toEqual([0, ...OFFSETS]);
    expect(vi.getTimerCount()).toBe(0);
    await h.advance(24 * 3_600_000);
    expect(h.read).toHaveBeenCalledTimes(OFFSETS.length + 1);
    expect(h.names).toEqual([]);
  });

  it.each([
    [25_000, 35_000],
    [50_000, 55_000],
  ])("publishes a name available at %ims at the %ims retry, before 60s, then stops", async (availableAt, publishedAt) => {
    const h = harness(availableAt);
    h.lookup.start();
    await h.advance(59_999);
    expect(h.names).toEqual([["lane-late", publishedAt]]);
    expect(h.readAt).toEqual([0, ...OFFSETS.filter(offset => offset <= publishedAt)]);
    expect(vi.getTimerCount()).toBe(0);
    await h.advance(24 * 3_600_000);
    expect(h.read).toHaveBeenCalledTimes(h.readAt.length);
    expect(h.names).toHaveLength(1);
  });

  it("does not recover a name that appears only after the final 5min retry", async () => {
    const h = harness(300_001);
    h.lookup.start();
    await h.advance(24 * 3_600_000);
    expect(h.readAt).toEqual([0, ...OFFSETS]);
    expect(h.names).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("publishes an initial name once without arming a timer", async () => {
    const h = harness(0);
    h.lookup.start();
    await h.advance(0);
    expect(h.names).toEqual([["lane-late", 0]]);
    expect(vi.getTimerCount()).toBe(0);
    await h.advance(3_600_000);
    expect(h.read).toHaveBeenCalledTimes(1);
  });

  it("never reads while the Main is otherwise named", async () => {
    const named = harness(25_000);
    named.lookup.start();
    await named.advance(0);
    named.setEligible(false); // e.g. a Pi session name arrived
    await named.advance(24 * 3_600_000);
    expect(named.read).toHaveBeenCalledTimes(1);
    expect(named.names).toEqual([]);
  });

  it("cancels every one-shot on stop and drops an in-flight result even while the lease is live", async () => {
    let resolve!: (name: string) => void;
    const onName = vi.fn();
    const read = vi.fn(() => new Promise<string>(answer => { resolve = answer; }));
    const lookup = herdrNameLookup({ read, live: () => true, eligible: () => true, onName });
    lookup.start();
    lookup.start();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(read).toHaveBeenCalledTimes(1); // no overlapping reads while the initial lookup waits
    lookup.stop();
    expect(vi.getTimerCount()).toBe(0);
    resolve("late-name");
    await vi.advanceTimersByTimeAsync(24 * 3_600_000);
    expect(onName).not.toHaveBeenCalled();
    expect(read).toHaveBeenCalledTimes(1);
  });

  it("stops reading and leaves no timer after retirement before, during or between reads", async () => {
    const between = harness(25_000);
    between.lookup.start();
    await between.advance(12_000);
    expect(between.readAt).toEqual([0, 5_000, 10_000]);
    between.retire();
    await between.advance(24 * 3_600_000);
    expect(between.read).toHaveBeenCalledTimes(3);
    expect(between.names).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);

    const during = harness(0);
    during.lookup.start(); // read in flight
    during.retire();
    await during.advance(24 * 3_600_000);
    expect(during.names).toEqual([]);
    expect(during.read).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);

    const dead = harness(0);
    dead.retire();
    dead.lookup.start();
    await dead.advance(24 * 3_600_000);
    expect(dead.read).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
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

// smarty-dev#11077: a Main's inbox accepts fleet.work.* sent to its published name (real mesh/router).
const sendWork = (via: ReturnType<typeof main>, to: string) => via.runtime.mesh.publish({ topic: "fleet.work.herdr-name",
  kind: "ask", to, from: { id: "review-actor", name: "review-actor", kind: "actor" }, text: `hello ${to}` });
/** The `to` targets of a Main's next inbox batch; only bypasses the inbox's existing 60s steer grace. */
const inboxTargets = async (m: ReturnType<typeof main>) => {
  const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
  try {
    const batch = await m.runtime.nextRootInbox({ holdsBatch: () => true, holdsSteer: () => false });
    return batch?.events.map(event => event.to).sort();
  } finally { clock.mockRestore(); }
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
      // A Herdr-named Main receives work sent to its Herdr name; a Pi-named Main does not take its Herdr name.
      for (const to of ["lane-a", "lane-e", "pi-named"]) await sendWork(b, to);
      expect(await inboxTargets(a)).toEqual(["lane-a"]);
      expect(await inboxTargets(named)).toEqual(["pi-named"]);

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

  it("retries the Herdr read with the pane and binary captured at start, not the current process env", async () => {
    // smarty-dev#11077: root A's first read fails; another root later changes the process's
    // HERDR_PANE_ID/HERDR_BIN_PATH. A's 5s one-shot retry must still ask about A's own pane.
    // Before initialize, so the lookup's timer is faked; shouldAdvanceTime keeps the runtime's
    // other timers moving with real time.
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"], now: Date.now(), shouldAdvanceTime: true });
    const root = temp();
    const dirA = path.join(root, "a");
    const dirB = path.join(root, "b");
    fs.mkdirSync(dirA); fs.mkdirSync(dirB);
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const failed = path.join(dirA, "failed-once");
    const binA = stubHerdr(dirA, `[ -e "${failed}" ] || { touch "${failed}"; exit 1; }\n[ "$3" = w1:p1 ] && echo '${agentJson("lane-a")}'`);
    const binB = stubHerdr(dirB, `echo '${agentJson("lane-b")}'`);
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const a = main(root, "18181818-0000-4000-8000-00000000000e");
    const callsA = () => fs.existsSync(path.join(dirA, "calls"))
      ? fs.readFileSync(path.join(dirA, "calls"), "utf8").trim().split("\n") : [];
    // Spy through the real implementation only to await the first read before advancing the clock.
    const read = vi.spyOn(herdrModule, "readHerdrAgentName");
    try {
      vi.stubEnv("HERDR_PANE_ID", "w1:p1");
      vi.stubEnv("HERDR_BIN_PATH", binA);
      await a.runtime.initialize(a.context, config);
      expect(read).toHaveBeenCalledTimes(1);
      expect(await read.mock.results[0]!.value).toBeUndefined();
      expect(fs.existsSync(failed)).toBe(true);
      for (let i = 0; i < 10; i++) await Promise.resolve(); // the lookup schedules its backoff
      // The process env now belongs to another root (B).
      vi.stubEnv("HERDR_PANE_ID", "w2:p1");
      vi.stubEnv("HERDR_BIN_PATH", binB);
      await vi.advanceTimersByTimeAsync(5_000); // the 5s one-shot retry fires
      await vi.waitFor(async () => expect(await a.invoke("agents.sessions")).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: a.id, name: "lane-a", herdrPane: "w1:p1" }),
      ])), { timeout: 8000, interval: 100 });
      expect(callsA()).toEqual(["agent get w1:p1", "agent get w1:p1"]);
      expect(read).toHaveBeenCalledTimes(2);
      expect(read.mock.calls[1]![0]).toMatchObject({ HERDR_PANE_ID: "w1:p1", HERDR_BIN_PATH: binA });
      expect(fs.existsSync(path.join(dirB, "calls"))).toBe(false);
    } finally {
      read.mockRestore();
      await a.runtime.shutdown();
      vi.useRealTimers();
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
      // The unnamed fallback "main" is not an inbox alias; its exact id still is.
      for (const to of ["main", failing.id]) await sendWork(printed, to);
      expect(await inboxTargets(failing)).toEqual([failing.id]);
    } finally {
      await failing.runtime.shutdown(); await printed.runtime.shutdown();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);

  it("names an interactive Main from SMARTY_AGENT before Herdr, after its session name, never in print/JSON mode or a child", async () => {
    const root = temp();
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("HERDR_BIN_PATH", stubHerdr(root, `echo '${agentJson("lane-h")}'`));
    vi.stubEnv("SMARTY_AGENT", "agent-s");
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const agent = main(root, "12121212-0000-4000-8000-000000000008");
    const named = main(root, "13131313-0000-4000-8000-000000000009", "pi-named");
    const printed = main(root, "14141414-0000-4000-8000-00000000000a");
    (printed.context as { mode: string }).mode = "print";
    const json = main(root, "15151515-0000-4000-8000-00000000000b");
    (json.context as { mode: string }).mode = "json";
    // Interactive (rpc) children inherit SMARTY_AGENT and HERDR_PANE_ID, but resolveFabricIdentity
    // makes them actor/agent participants, so they are not a local Main and own no root record.
    const actorChild = main(root, "16161616-0000-4000-8000-00000000000c");
    const agentChild = main(root, "17171717-0000-4000-8000-00000000000d");
    const actorEnv = { PI_FABRIC_MAIN_AGENT_ID: agent.id, PI_FABRIC_ACTOR_ID: "actor:gate", PI_FABRIC_PARENT_RUN: "c".repeat(32) };
    const agentEnv = { PI_FABRIC_MAIN_AGENT_ID: agent.id, PI_FABRIC_PARENT_RUN: "d".repeat(32) };
    expect(resolveFabricIdentity("16161616-0000-4000-8000-00000000000c", { ...actorEnv, SMARTY_AGENT: "agent-s" }).identity.kind).toBe("actor");
    expect(resolveFabricIdentity("17171717-0000-4000-8000-00000000000d", { ...agentEnv, SMARTY_AGENT: "agent-s" }).identity.kind).toBe("agent");
    const all = [agent, named, printed, json, actorChild, agentChild];
    const calls = () => fs.existsSync(path.join(root, "calls"))
      ? fs.readFileSync(path.join(root, "calls"), "utf8").trim().split("\n") : [];
    try {
      vi.stubEnv("HERDR_PANE_ID", "w8:p1");
      for (const m of [agent, named]) await m.runtime.initialize(m.context, config);
      // Gated roots get their own pane, so any Herdr call they made would be visible.
      vi.stubEnv("HERDR_PANE_ID", "w9:p1");
      for (const m of [printed, json]) await m.runtime.initialize(m.context, config);
      for (const [m, env] of [[actorChild, actorEnv], [agentChild, agentEnv]] as const) {
        for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value);
        await m.runtime.initialize(m.context, config);
        for (const key of Object.keys(env)) vi.stubEnv(key, undefined);
      }
      await vi.waitFor(async () => expect(await agent.invoke("agents.sessions")).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: agent.id, name: "agent-s" }),
        expect.objectContaining({ id: named.id, name: "pi-named" }),
        expect.objectContaining({ id: printed.id, name: "main" }),
        expect.objectContaining({ id: json.id, name: "main" }),
      ])), { timeout: 8000, interval: 100 });
      const sessions = await agent.invoke("agents.sessions") as Array<{ id: string; name: string }>;
      // Only the interactive Main claims the launcher name; no child owns a root record.
      expect(sessions.filter(s => s.name === "agent-s").map(s => s.id)).toEqual([agent.id]);
      expect(sessions.map(s => s.id)).not.toContain(actorChild.id);
      expect(sessions.map(s => s.id)).not.toContain(agentChild.id);
      expect(sessions.some(s => s.name === "lane-h")).toBe(false);
      // The launcher-named Main receives work sent to SMARTY_AGENT; when the Pi name wins,
      // neither the launcher nor the Herdr name is an alias.
      for (const to of ["agent-s", "lane-h", "pi-named"]) await sendWork(printed, to);
      expect(await inboxTargets(agent)).toEqual(["agent-s"]);
      expect(await inboxTargets(named)).toEqual(["pi-named"]);
      // No print/JSON root or child ever asked Herdr.
      expect(calls().filter(call => call.includes("w9:p1"))).toEqual([]);
    } finally {
      for (const m of all.reverse()) await m.runtime.shutdown();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);
});
