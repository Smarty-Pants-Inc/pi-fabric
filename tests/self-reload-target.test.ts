import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installSelfReload, reloadTargetUiHold as productionUiHold, SELF_RELOAD_COMMAND } from "../src/lifecycle/self-reload.js";

// A producer uses only the public bus contract, never the controller's private state.
const REQUEST = "pi-fabric:reload-target:v1";
const RESULT = `${REQUEST}:result`;
type Reply = { requestId: string; owner: string; resource: string; accepted: boolean; reason: string; target: string | null };
type Handler = (event: any, context: any) => unknown;
let root: string;
let serial = 0;
let shutdown: (() => void) | undefined;
const originalArgv = [...process.argv];
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-reload-target-"));
  for (const name of ["PI_FABRIC_NO_AUTO_RELOAD", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID"]) vi.stubEnv(name, undefined);
});
afterEach(() => {
  shutdown?.(); shutdown = undefined;
  vi.useRealTimers(); vi.unstubAllEnvs();
  process.argv = [...originalArgv];
  fs.rmSync(root, { recursive: true, force: true });
});
const entry = (name: string, packageName = "smarty-code") => {
  const dir = path.join(root, name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: packageName, pi: { extensions: ["index.js"] } }));
  fs.writeFileSync(path.join(dir, "index.js"), "export default () => {};\n");
  return path.join(dir, "index.js");
};
const setup = (options: { uiQuery?: boolean; hostQuery?: boolean; configured?: boolean; packages?: boolean; turnProvenance?: boolean; beforeStart?: (loaded: string) => void } = {}) => {
  const fabric = entry("fabric", "pi-fabric");
  const loaded = entry("code-old");
  const next = entry("code-new");
  const settingsPath = path.join(root, "settings.json");
  const activate = (target: string, extras: unknown[] = []) => {
    fs.writeFileSync(settingsPath, JSON.stringify(options.packages
      ? { packages: [path.dirname(fabric), path.dirname(target), ...extras] }
      : { packages: [path.dirname(fabric)], extensions: [target, ...extras] }));
  };
  activate(loaded);
  options.beforeStart?.(loaded);
  const handlers = new Map<string, Handler[]>();
  const bus = new Map<string, Set<(data: any) => void>>();
  const commands = new Map<string, { handler: (args: string, context: any) => Promise<void> }>();
  const sent: string[] = [];
  const replies: Reply[] = [];
  const pi = {
    ...(options.turnProvenance ? { hostCapabilities: { turnProvenance: 1 } } : {}),
    on: (name: string, handler: Handler) => handlers.set(name, [...handlers.get(name) ?? [], handler]),
    events: {
      on: (name: string, handler: (data: any) => void) => {
        const listeners = bus.get(name) ?? new Set(); listeners.add(handler); bus.set(name, listeners);
        return () => { listeners.delete(handler); };
      },
      emit: (name: string, data: unknown) => { for (const handler of bus.get(name) ?? []) handler(data); },
    },
    registerCommand: (name: string, command: any) => commands.set(name, command),
    sendUserMessage: vi.fn((text: string, _options?: unknown) => { sent.push(text); }),
  };
  pi.events.on(RESULT, data => replies.push(data));
  const uiState = { hold: undefined as "dialog" | "custom" | "editor" | undefined, throws: false };
  const held: Array<{ reason: string; heldForMs: number; target: string }> = [];
  const state = { busy: 0, halted: false, idle: true, pending: false, prompt: false, settling: false, dialog: false, editor: "", compacting: false };
  let sessionId = `reload-target-${++serial}`;
  const context = {
    mode: "tui", hasUI: true,
    ui: { notify: vi.fn(), setStatus: vi.fn(), getEditorText: () => state.editor,
      ...(options.hostQuery ? { holdState() { if (uiState.throws) throw new Error("host query failed"); return uiState.hold; } } : {}) },
    isIdle: () => state.idle && !state.compacting,
    hasPendingMessages: () => state.pending,
    isPromptPending: () => state.prompt, isSettling: () => state.settling,
    sessionManager: { getSessionId: () => sessionId }, reload: vi.fn(async () => {}),
  };
  // Existing seam cases model a host query; hostQuery cases use the production Pi adapter.
  const deps = {
    moduleUrl: pathToFileURL(fabric).href, settingsPath,
    busy: () => state.busy, halted: () => state.halted,
    autoReloadConfigured: () => options.configured ?? true,
    heldNoticeMs: 10_000, publishHeld: (data: { reason: string; heldForMs: number; target: string }) => held.push(data),
    ...(options.uiQuery === false ? {} : { reloadTargetUiHold: options.hostQuery ? productionUiHold : () => state.dialog ? "ui-dialog-active" : undefined }),
  };
  const controller = installSelfReload(pi as never, deps);
  const emit = (name: string, event: unknown = {}) => { for (const handler of handlers.get(name) ?? []) handler(event, context); };
  controller.sessionStart("startup", context as never);
  shutdown = () => emit("session_shutdown");
  const request = async (configured = loaded, owner = "smarty-code", resource = loaded, runtimeLoaded = loaded) => {
    const requestId = `request-${++serial}`;
    expect(bus.get(REQUEST)?.size, "Fabric registers the public request listener").toBeGreaterThan(0);
    const resultPromise = new Promise<Reply>(resolve => {
      const off = pi.events.on(RESULT, data => {
        if (data.requestId === requestId) { off(); resolve(data as Reply); }
      });
    });
    pi.events.emit(REQUEST, { requestId, owner, resource, loaded: runtimeLoaded, configured, reason: "test installation" });
    return await resultPromise;
  };
  const bind = async () => expect(await request()).toMatchObject({ accepted: true, reason: "bound-unchanged", target: loaded });
  const execute = async (text = sent.at(-1) ?? `/${SELF_RELOAD_COMMAND} auto`) => {
    const [command, ...args] = text.slice(1).split(" ");
    await commands.get(command!)!.handler(args.join(" "), context);
  };
  return { pi, context, state, uiState, held, loaded, next, fabric, settingsPath, activate, request, bind, execute, emit, sent, replies, controller,
    replaceSessionWithoutStart: () => { sessionId = `replaced-${++serial}`; },
    switchSession: () => { sessionId = `switched-${++serial}`; controller.sessionStart("switch", context as never); } };
};

describe("merged Pi holdState adapter", () => {
  it("keeps absent holdState fail-closed but admits a supported clear host", async () => {
    const s = setup({ hostQuery: true }); await s.bind(); s.activate(s.next);
    const holdState = s.context.ui.holdState!; delete s.context.ui.holdState;
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "unsupported-host:global-dialog/editor-hold-query" });
    s.context.ui.holdState = holdState;
    expect(await s.request(s.next)).toMatchObject({ accepted: true, reason: "pending" });
    s.emit("agent_settled"); await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(1);
  });
  it.each(["dialog", "custom", "editor"] as const)("resource %s retains pending target, reports once, then reloads once after clear", async hold => {
    vi.useFakeTimers(); const s = setup({ hostQuery: true }); await s.bind(); s.activate(s.next); s.uiState.hold = hold;
    const result = await s.request(s.next);
    expect(result).toMatchObject({ accepted: true, reason: "pending" });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(s.sent).toEqual([]); expect(s.context.reload).not.toHaveBeenCalled();
    expect(s.replies.filter(reply => reply.requestId === result.requestId)).toEqual([result]);
    expect(s.held).toEqual([{ reason: `ui-hold:${hold}`, target: s.next, heldForMs: expect.any(Number) }]);
    expect(s.context.ui.notify).toHaveBeenCalledTimes(1);
    s.uiState.hold = undefined; await vi.advanceTimersByTimeAsync(6_000);
    expect(s.sent).toHaveLength(1);
    s.uiState.hold = hold; await s.execute(); expect(s.context.reload).not.toHaveBeenCalled();
    s.uiState.hold = undefined; await vi.advanceTimersByTimeAsync(6_000); await s.execute();
    const count = s.sent.length; await vi.advanceTimersByTimeAsync(20_000);
    expect(s.sent).toHaveLength(count); expect(s.context.reload).toHaveBeenCalledTimes(1);
  });
  it.each(["dialog", "custom", "editor"] as const)("Fabric %s holds scheduling and native execution, then reloads once", async hold => {
    vi.useFakeTimers(); const s = setup({ hostQuery: true });
    const next = path.dirname(entry("fabric-held-next", "pi-fabric"));
    const previous = fs.statSync(s.settingsPath).mtimeMs;
    fs.writeFileSync(s.settingsPath, JSON.stringify({ packages: [next], extensions: [s.loaded] }));
    fs.utimesSync(s.settingsPath, new Date(previous + 1000), new Date(previous + 1000));
    s.uiState.hold = hold; s.emit("agent_settled"); await vi.advanceTimersByTimeAsync(30_000);
    expect(s.sent).toEqual([]); expect(s.held).toEqual([{ reason: `ui-hold:${hold}`, target: next, heldForMs: expect.any(Number) }]);
    expect(s.context.ui.notify).toHaveBeenCalledTimes(1);
    s.uiState.hold = undefined; await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(1);
    s.uiState.hold = hold; await s.execute(); expect(s.context.reload).not.toHaveBeenCalled();
    s.uiState.hold = undefined; s.uiState.throws = true; await vi.advanceTimersByTimeAsync(6_000);
    expect(s.sent).toHaveLength(1); expect(s.context.reload).not.toHaveBeenCalled();
    s.uiState.throws = false; await vi.advanceTimersByTimeAsync(6_000); await s.execute();
    expect(s.context.reload).toHaveBeenCalledTimes(1); const count = s.sent.length;
    await vi.advanceTimersByTimeAsync(20_000); expect(s.sent).toHaveLength(count);
  });
  it.each(["advertisement", "execution"])("query throw at %s refuses the resource without consuming a reload attempt", async phase => {
    vi.useFakeTimers(); const s = setup({ hostQuery: true }); await s.bind(); s.activate(s.next);
    if (phase === "execution") { expect(await s.request(s.next)).toMatchObject({ accepted: true }); s.emit("agent_settled"); }
    s.uiState.throws = true;
    if (phase === "advertisement") expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "unsupported-host:ui-hold-query-failed" });
    else { await s.execute(); expect(s.replies.at(-1)).toMatchObject({ accepted: false, reason: "unsupported-host:ui-hold-query-failed" }); }
    expect(s.context.reload).not.toHaveBeenCalled();
    s.uiState.throws = false; expect(await s.request(s.next)).toMatchObject({ accepted: true });
    s.emit("agent_settled"); await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(1);
  });
});

describe("reload-target v1 public producer/consumer counterexamples", () => {
  it.each([false, true])("preserves pinned resource commands and provenance compatibility (capable=%s)", async turnProvenance => {
    const s = setup({ turnProvenance }); await s.bind();
    s.activate(s.next); await s.request(s.next);
    s.emit("agent_settled", { outcome: "completed" });
    s.emit("agent_settled", { outcome: "completed" });
    expect(s.pi.sendUserMessage).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(/^\/fabric-release-reload auto resource-\d+$/),
      { expandPromptTemplates: true, ...(turnProvenance ? { provenance: {
        v: 1, channel: "fabric", sender: { id: `session:${s.context.sessionManager.getSessionId()}`,
          name: "main", kind: "main", verified: "mesh" }, via: "followUp",
      } } : {}) },
    );
    await s.execute();
    expect(s.context.reload).toHaveBeenCalledTimes(1);
  });

  it.each([false, true])("changed Code-only target reloads once at a safe settle (packages=%s)", async packages => {
    const s = setup({ packages }); await s.bind();
    expect(s.sent).toEqual([]);
    s.activate(s.next);
    expect(await s.request(s.next)).toMatchObject({ accepted: true, reason: "pending", target: s.next });
    expect(s.sent).toEqual([]); // advertising a target never injects a command
    s.emit("agent_settled", { outcome: "completed" });
    expect(s.sent).toHaveLength(1);
    await s.execute(); await s.execute();
    s.emit("agent_settled", { outcome: "completed" });
    expect(s.context.reload).toHaveBeenCalledTimes(1);
    expect(s.sent).toHaveLength(1);
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "already-attempted" });
    // Fabric stayed loaded from the same package: reporting must name the changed resource.
    const done = s.controller.sessionStart("reload", s.context as never);
    expect(done).toMatchObject({ owner: "smarty-code", resource: s.loaded, target: s.next, old: "index.js", new: "index.js" });
  });

  it("busy children/actors/shell/Jev then idle adopts without another Main turn", async () => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next);
    s.state.busy = 4; s.emit("agent_settled", { outcome: "completed" });
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toEqual([]);
    s.state.busy = 0; await vi.advanceTimersByTimeAsync(6_000);
    expect(s.sent).toHaveLength(1); await s.execute();
    expect(s.context.reload).toHaveBeenCalledTimes(1);
  });

  it.each(["aborted", "error", "escape"])("explicit %s halt survives extension followups and timers", async outcome => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next);
    if (outcome === "escape") s.state.halted = true;
    s.emit("agent_settled", { outcome: outcome === "escape" ? "completed" : outcome });
    s.emit("input", { source: "extension" }); await s.request(s.next);
    s.emit("agent_settled", { outcome: "completed" });
    await vi.advanceTimersByTimeAsync(20_000); await s.execute();
    expect(s.sent).toEqual([]); expect(s.context.reload).not.toHaveBeenCalled();
    s.state.halted = false; s.emit("input", { source: "interactive" });
    s.emit("agent_settled", { outcome: "completed" }); await s.execute();
    expect(s.context.reload).toHaveBeenCalledTimes(1);
  });

  it.each(["prompt", "dialog", "editor", "compacting", "pending", "settling", "busy", "idle"])("intervening %s blocks native execution", async hold => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next);
    s.emit("agent_settled", { outcome: "completed" }); expect(s.sent).toHaveLength(1);
    if (hold === "editor") s.state.editor = "unfinished draft";
    else if (hold === "busy") s.state.busy = 1;
    else if (hold === "idle") s.state.idle = false;
    else s.state[hold as "prompt" | "dialog" | "compacting" | "pending" | "settling"] = true;
    await s.execute(); expect(s.context.reload).not.toHaveBeenCalled();
  });

  it.each(["rollback", "removed", "missing", "ambiguous"])("intervening target %s blocks native execution", async change => {
    const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next);
    s.emit("agent_settled", { outcome: "completed" }); expect(s.sent).toHaveLength(1);
    if (change === "rollback") s.activate(s.loaded);
    if (change === "removed") fs.writeFileSync(s.settingsPath, JSON.stringify({ packages: [path.dirname(s.fabric)], extensions: [] }));
    if (change === "missing") fs.rmSync(s.next);
    if (change === "ambiguous") s.activate(s.next, [s.next]);
    await s.execute(); expect(s.context.reload).not.toHaveBeenCalled();
    expect(s.replies.at(-1)).toMatchObject({ accepted: false, reason: expect.stringMatching(/reverted|removed|missing|ambiguous/) });
  });

  it.each(["-e", "--extension", "--extension=", "outside"])("explicit %s/outside-profile never follows", async source => {
    const s = setup({ beforeStart: loaded => {
      if (source === "--extension=") process.argv.push(`--extension=${loaded}`);
      else if (source !== "outside") process.argv.push(source, loaded);
    } });
    const outside = entry("outside");
    expect(await s.request(source === "outside" ? outside : s.loaded, "smarty-code", source === "outside" ? outside : s.loaded, source === "outside" ? outside : s.loaded)).toMatchObject({ accepted: false, reason: expect.stringMatching(/explicit|outside-profile/) });
    s.activate(s.next); expect(await s.request(s.next)).toMatchObject({ accepted: false });
    s.emit("agent_settled", { outcome: "completed" }); await s.execute();
    expect(s.sent).toEqual([]); expect(s.context.reload).not.toHaveBeenCalled();
  });
});

describe("reload-target v1 adversarial admission", () => {
  it("idle pointer changes use the existing retry without a new Main turn", async () => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next);
    expect(s.sent).toEqual([]); await vi.advanceTimersByTimeAsync(6_000);
    expect(s.sent).toHaveLength(1); await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(1);
  });
  it("global UI holds keep the idle retry blocked until they clear", async () => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); s.state.dialog = true; await s.request(s.next);
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toEqual([]);
    s.state.dialog = false; s.state.editor = "draft";
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toEqual([]);
    s.state.editor = ""; await vi.advanceTimersByTimeAsync(6_000);
    expect(s.sent).toHaveLength(1); await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(1);
  });
  it("a queued target never adopts a newer target or refuses its requestId", async () => {
    const s = setup(); await s.bind(); s.activate(s.next); const first = await s.request(s.next); s.emit("agent_settled");
    const stale = s.sent[0]!; const third = entry("code-third"); s.activate(third); const newer = await s.request(third);
    await s.execute(stale); expect(s.context.reload).not.toHaveBeenCalled();
    expect(s.replies.at(-1)).toMatchObject({ requestId: first.requestId, accepted: false, reason: "target-changed", target: third });
    expect(s.replies.filter(reply => reply.requestId === newer.requestId)).toEqual([newer]);
    s.emit("agent_settled"); await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(1);
  });
  it.each(["fabric", "resource"])("a request for a different resource never drops a superseded queued %s idle retry", async first => {
    vi.useFakeTimers(); const s = setup(); await s.bind();
    const fabricNext = path.dirname(entry("fabric-next", "pi-fabric"));
    const fabricThird = path.dirname(entry("fabric-third", "pi-fabric"));
    const codeThird = entry("code-third");
    const profile = (fabric: string, code: string) => {
      const previous = fs.statSync(s.settingsPath).mtimeMs;
      fs.writeFileSync(s.settingsPath, JSON.stringify({ packages: [fabric], extensions: [code] }));
      fs.utimesSync(s.settingsPath, new Date(previous + 1000), new Date(previous + 1000));
    };
    if (first === "fabric") {
      profile(fabricNext, s.loaded); s.emit("agent_settled");
      profile(fabricNext, s.next); await s.request(s.next);
    } else {
      profile(path.dirname(s.fabric), s.next); await s.request(s.next); s.emit("agent_settled");
      profile(fabricNext, s.next); await s.request(s.next);
    }
    const stale = s.sent[0]!;
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(1);
    profile(first === "fabric" ? fabricThird : fabricNext, first === "fabric" ? s.next : codeThird);
    // No new advertisement, input, turn or settle after the pinned target is superseded.
    await s.execute(stale); expect(s.context.reload).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(2);
    await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(1);
    if (first === "fabric") {
      await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(3);
      await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(2);
    }
    // Unadvertised Code-third is not adopted; the pending Fabric target still survives.
    await vi.advanceTimersByTimeAsync(20_000);
    expect(s.context.reload).toHaveBeenCalledTimes(first === "fabric" ? 2 : 1);
  });

  it.each(["fabric", "resource"])("a superseded queued %s preserves both resources' idle retries", async first => {
    vi.useFakeTimers(); const s = setup(); await s.bind();
    const fabricNext = path.dirname(entry("fabric-next", "pi-fabric"));
    const fabricThird = path.dirname(entry("fabric-third", "pi-fabric"));
    const codeThird = entry("code-third");
    const profile = (fabric: string, code: string) => {
      const previous = fs.statSync(s.settingsPath).mtimeMs;
      fs.writeFileSync(s.settingsPath, JSON.stringify({ packages: [fabric], extensions: [code] }));
      fs.utimesSync(s.settingsPath, new Date(previous + 1000), new Date(previous + 1000));
    };
    if (first === "fabric") {
      profile(fabricNext, s.loaded); s.emit("agent_settled");
      profile(fabricNext, s.next); await s.request(s.next);
    } else {
      profile(path.dirname(s.fabric), s.next); await s.request(s.next); s.emit("agent_settled");
      profile(fabricNext, s.next); await s.request(s.next);
    }
    expect(s.sent).toHaveLength(1); const stale = s.sent[0]!;
    // A retry tick while the command is queued must not abandon either pending target.
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(1);
    profile(first === "fabric" ? fabricThird : fabricNext, codeThird);
    await s.request(codeThird);
    await s.execute(stale); expect(s.context.reload).not.toHaveBeenCalled();
    // The modeled reload does not replace the runtime: this also checks that an already
    // attempted candidate cannot starve the other resource, including after a native failure.
    s.context.reload.mockRejectedValueOnce(new Error("native failure"));
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(2);
    await expect(s.execute()).rejects.toThrow("native failure");
    await vi.advanceTimersByTimeAsync(6_000); expect(s.sent).toHaveLength(3);
    await s.execute(); expect(s.context.reload).toHaveBeenCalledTimes(2);
    expect(await s.request(codeThird)).toMatchObject({ accepted: false, reason: "already-attempted" });
    await vi.advanceTimersByTimeAsync(20_000); s.emit("agent_settled"); await s.execute();
    expect(s.sent).toHaveLength(3); expect(s.context.reload).toHaveBeenCalledTimes(2);
  });

  it("a native session change before lifecycle cleanup cannot reuse the previous binding", async () => {
    const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next); s.emit("agent_settled");
    s.replaceSessionWithoutStart(); await s.execute();
    expect(s.context.reload).not.toHaveBeenCalled();
    expect(s.replies.at(-1)).toMatchObject({ accepted: false, reason: "session-changed" });
  });
  it("a failed native reload consumes the exact-target attempt", async () => {
    const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next); s.emit("agent_settled");
    s.context.reload.mockRejectedValueOnce(new Error("native failure"));
    await expect(s.execute()).rejects.toThrow("native failure");
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "already-attempted" });
    s.emit("agent_settled"); expect(s.sent).toHaveLength(1);
  });
  it("profile exclusions and empty package manifests are never treated as selected resources", async () => {
    const s = setup(); s.activate(s.loaded, [`-${s.loaded}`]);
    expect(await s.request()).toMatchObject({ accepted: false, reason: "outside-profile" });
    const p = setup({ packages: true }); await p.bind();
    fs.writeFileSync(path.join(path.dirname(p.next), "package.json"), JSON.stringify({ name: "smarty-code", pi: { extensions: [] } }));
    p.activate(p.next); expect(await p.request(p.next)).toMatchObject({ accepted: false, reason: "missing-or-removed-profile-target" });
  });
  it("independently rereads a rollback even if the settings mtime is unchanged", async () => {
    const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next); s.emit("agent_settled");
    const at = fs.statSync(s.settingsPath); s.activate(s.loaded); fs.utimesSync(s.settingsPath, at.atime, at.mtime);
    await s.execute(); expect(s.context.reload).not.toHaveBeenCalled();
    expect(s.replies.at(-1)).toMatchObject({ accepted: false, reason: "target-reverted" });
  });
  it("refuses package identity replacement and missing profile files", async () => {
    const s = setup({ packages: true }); await s.bind(); s.activate(s.next);
    fs.writeFileSync(path.join(path.dirname(s.next), "package.json"), JSON.stringify({ name: "other-extension", pi: { extensions: ["index.js"] } }));
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "removed-profile-entry" });
    fs.rmSync(s.settingsPath); expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "profile-unreadable" });
  });
  it("cleans an armed busy retry on session switch and shutdown", async () => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); s.state.busy = 1; await s.request(s.next); s.emit("agent_settled");
    s.switchSession(); s.state.busy = 0; await vi.advanceTimersByTimeAsync(20_000); expect(s.sent).toEqual([]);
    s.activate(s.loaded); await s.bind(); s.activate(s.next); s.state.busy = 1; await s.request(s.next); s.emit("agent_settled");
    s.emit("session_shutdown"); s.state.busy = 0; await vi.advanceTimersByTimeAsync(20_000); expect(s.sent).toEqual([]);
  });
});

describe("reload-target v1 validation and lifecycle", () => {
  it("binds before installation; configured is never authority and owner is only a label", async () => {
    const s = setup(); s.activate(s.next);
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "outside-profile" });
    s.activate(s.loaded); await s.bind(); s.activate(s.next);
    const rogue = entry("rogue");
    expect(await s.request(rogue)).toMatchObject({ accepted: false, reason: "configured-mismatch", target: s.next });
    expect(await s.request(s.next, "another-label")).toMatchObject({ accepted: true, owner: "another-label", target: s.next });
  });
  it("correlates shared-topic replies by requestId, not owner", async () => {
    const s = setup(); const a = await s.request(); const b = await s.request(s.loaded, a.owner);
    expect(a.requestId).not.toBe(b.requestId);
    expect(a).toMatchObject({ resource: s.loaded, owner: "smarty-code" });
    expect(b).toMatchObject({ resource: s.loaded, owner: "smarty-code" });
  });
  it("rejects noncanonical, missing, malformed and duplicate profile targets", async () => {
    const s = setup();
    const link = path.join(root, "code-link.js"); fs.symlinkSync(s.loaded, link);
    expect(await s.request(link, "code", link, link)).toMatchObject({ accepted: false, reason: "noncanonical-path" });
    expect(await s.request(path.join(root, "missing.js"))).toMatchObject({ accepted: false, reason: "missing-path" });
    s.activate(s.loaded, [s.loaded]); expect(await s.request()).toMatchObject({ accepted: false, reason: "ambiguous-profile-entry" });
    s.pi.events.emit(REQUEST, { requestId: "malformed", owner: "code", resource: s.loaded });
    expect(s.replies.at(-1)).toMatchObject({ requestId: "malformed", accepted: false, reason: "invalid-request" });
  });
  it("refuses unsafe list reorder/removal and package filters", async () => {
    const s = setup(); await s.bind();
    s.activate(s.next, [entry("unrelated")]);
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "ambiguous-profile-change" });
    const p = setup({ packages: true }); await p.bind();
    fs.writeFileSync(p.settingsPath, JSON.stringify({ packages: [path.dirname(p.fabric), { source: path.dirname(p.next), extensions: [] }] }));
    expect(await p.request(p.next)).toMatchObject({ accepted: false, reason: "removed-profile-entry" });
  });
  it.each(["isPromptPending", "isSettling", "getEditorText", "global-ui-query"])("fails closed when the host lacks %s", async capability => {
    const s = setup({ uiQuery: capability !== "global-ui-query" }); await s.bind();
    if (capability === "getEditorText") delete (s.context.ui as any).getEditorText;
    else if (capability !== "global-ui-query") delete (s.context as any)[capability];
    s.activate(s.next);
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: expect.stringContaining("unsupported-host:") });
    s.emit("agent_settled", { outcome: "completed" }); await s.execute();
    expect(s.context.reload).not.toHaveBeenCalled();
  });
  it("rechecks host capability loss at command execution", async () => {
    const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next); s.emit("agent_settled");
    delete (s.context as any).isPromptPending;
    await s.execute(); expect(s.context.reload).not.toHaveBeenCalled();
    expect(s.replies.at(-1)).toMatchObject({ accepted: false, reason: "unsupported-host:isPromptPending" });
  });
  it.each(["setting", "environment"])("preserves the %s opt-out", async source => {
    const s = setup({ configured: source !== "setting" }); await s.bind();
    if (source === "environment") vi.stubEnv("PI_FABRIC_NO_AUTO_RELOAD", "1");
    s.activate(s.next); expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "auto-reload-disabled" });
    s.emit("agent_settled"); expect(s.sent).toEqual([]);
  });
  it("cleans pending bindings/timers/queued commands on session switch and shutdown", async () => {
    vi.useFakeTimers(); const s = setup(); await s.bind(); s.activate(s.next); await s.request(s.next); s.emit("agent_settled");
    const stale = s.sent[0]!; s.switchSession(); await s.execute(stale);
    expect(s.context.reload).not.toHaveBeenCalled();
    expect(await s.request(s.next)).toMatchObject({ accepted: false, reason: "outside-profile" });
    s.emit("session_shutdown"); const count = s.replies.length;
    s.pi.events.emit(REQUEST, { requestId: "after-shutdown", owner: "code", resource: s.loaded, loaded: s.loaded, configured: s.next, reason: "ignored" });
    await vi.advanceTimersByTimeAsync(20_000);
    expect(s.replies).toHaveLength(count); expect(s.context.reload).not.toHaveBeenCalled();
  });
});
