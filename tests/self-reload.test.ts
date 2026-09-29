import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  AUTO_RELOAD_OPT_OUT_ENV,
  SELF_RELOAD_COMMAND,
  activeFabricRoot,
  installSelfReload,
  loadedFabricRoot,
} from "../src/lifecycle/self-reload.js";

// smarty-dev#2160: a Main loaded from one Fabric release reloads itself onto the release the
// profile settings.json activates, only when no task agent or actor run is in flight.

let dir: string;
const release = (name: string, packageName = "pi-fabric"): string => {
  const root = path.join(dir, "releases", name);
  fs.mkdirSync(path.join(root, "dist"), { recursive: true });
  fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ name: packageName }));
  return root;
};
const settingsPath = () => path.join(dir, "settings.json");
const activate = (...packages: unknown[]): void => {
  fs.writeFileSync(settingsPath(), JSON.stringify({ packages }));
  // Two writes inside one mtime tick must still be seen.
  const later = new Date(Date.now() + Math.floor(Math.random() * 1e6) + 1000);
  fs.utimesSync(settingsPath(), later, later);
};

type Handler = (event: unknown, context: unknown) => unknown;
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const commands = new Map<string, { handler: (args: string, context: unknown) => Promise<void> }>();
  const sent: string[] = [];
  const pi = {
    on: (name: string, handler: Handler) => handlers.set(name, [...(handlers.get(name) ?? []), handler]),
    registerCommand: (name: string, command: { handler: (args: string, context: unknown) => Promise<void> }) =>
      commands.set(name, command),
    sendUserMessage: (text: string) => sent.push(text),
  };
  const emit = (name: string, context: unknown) => handlers.get(name)?.forEach((handler) => handler({}, context));
  return { pi, emit, commands, sent };
};

const fakeContext = (
  sessionId: string,
  state: { idle: boolean; pending: boolean; promptPending?: boolean; settling?: boolean },
) => {
  const notices: string[] = [];
  const reload = vi.fn(async () => {});
  return {
    notices,
    reload,
    mode: "tui",
    hasUI: true,
    ui: { notify: (message: string) => notices.push(message) },
    isIdle: () => state.idle,
    hasPendingMessages: () => state.pending,
    isPromptPending: () => state.promptPending ?? false,
    isSettling: () => state.settling ?? false,
    sessionManager: { getSessionId: () => sessionId },
  };
};

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-self-reload-"));
  delete process.env[AUTO_RELOAD_OPT_OUT_ENV];
  delete process.env.PI_FABRIC_ACTOR_ID;
  delete process.env.PI_FABRIC_PARENT_RUN;
});
afterEach(() => {
  vi.useRealTimers();
  delete process.env[AUTO_RELOAD_OPT_OUT_ENV];
  fs.rmSync(dir, { recursive: true, force: true });
});

describe("release detection", () => {
  it("finds the loaded package root and the one active pi-fabric package entry", () => {
    const old = release("aaa");
    const next = release("bbb");
    const other = release("inbox", "smarty-inbox");
    expect(loadedFabricRoot(pathToFileURL(path.join(old, "dist", "index.js")).href)).toBe(fs.realpathSync(old));
    activate(next, { source: other }, "npm:pi-fabric");
    expect(activeFabricRoot(settingsPath())).toBe(fs.realpathSync(next));
    activate(next, old);
    expect(activeFabricRoot(settingsPath())).toBeUndefined(); // ambiguous: never guess
  });
});

describe("installSelfReload", () => {
  const setup = (options: { busy?: () => number; configured?: boolean } = {}) => {
    const old = release("aaa");
    const next = release("bbb");
    activate(old);
    const { pi, emit, commands, sent } = fakePi();
    const selfReload = installSelfReload(pi as never, {
      busy: options.busy ?? (() => 0),
      autoReloadConfigured: () => options.configured ?? true,
      moduleUrl: pathToFileURL(path.join(old, "dist", "index.js")).href,
      settingsPath: settingsPath(),
    });
    return { old, next, emit, commands, sent, selfReload };
  };

  it("reloads at settle onto the newly active release and reports old -> new once", async () => {
    const { next, emit, commands, sent, selfReload } = setup();
    const state = { idle: true, pending: false };
    const context = fakeContext("s-reload", state);
    expect(selfReload.sessionStart("startup", context as never)).toBeUndefined();
    emit("agent_settled", context);
    expect(sent).toEqual([]); // nothing newer is active

    activate(next);
    emit("agent_settled", context);
    expect(sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(1);

    // The new runtime (loaded from the new release) reports the swap once.
    const fresh = fakePi();
    const reloaded = installSelfReload(fresh.pi as never, {
      busy: () => 0,
      autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(next, "dist", "index.js")).href,
      settingsPath: settingsPath(),
    });
    expect(reloaded.sessionStart("reload", context as never)).toEqual({ old: "aaa", new: "bbb" });
    expect(reloaded.sessionStart("reload", context as never)).toBeUndefined();
    fresh.emit("agent_settled", context);
    expect(fresh.sent).toEqual([]);
  });

  it("waits for task agents and actor runs, then reloads while the Main stays idle", async () => {
    vi.useFakeTimers();
    let busy = 1;
    const { next, emit, sent, selfReload } = setup({ busy: () => busy });
    const context = fakeContext("s-busy", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context);
    expect(sent).toEqual([]);
    await vi.advanceTimersByTimeAsync(6_000);
    expect(sent).toEqual([]);
    busy = 0; // the last child finished with no new Main turn
    await vi.advanceTimersByTimeAsync(6_000);
    expect(sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(sent).toHaveLength(1);
  });

  it("never reloads under a prompt in preflight or during settle handlers (review/astra on #158)", async () => {
    vi.useFakeTimers();
    let busy = 1;
    const { next, emit, commands, sent, selfReload } = setup({ busy: () => busy });
    // Pi keeps isIdle() true while a user's prompt is in preflight.
    const state = { idle: true, pending: false, promptPending: false, settling: false };
    const context = fakeContext("s-preflight", state);
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context);
    busy = 0; // an actor run ended with no Main turn: only the retry tick can reload
    state.promptPending = true;
    await vi.advanceTimersByTimeAsync(6_000);
    expect(sent).toEqual([]);
    state.promptPending = false;
    state.settling = true;
    await vi.advanceTimersByTimeAsync(6_000);
    expect(sent).toEqual([]);
    // The command re-checks both, too.
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    state.settling = false;
    state.promptPending = true;
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).not.toHaveBeenCalled();
    // The counterexample: once the prompt has started or settled, the tick reloads.
    state.promptPending = false;
    await vi.advanceTimersByTimeAsync(6_000);
    expect(sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(1);
  });

  it("a deferred command that finds the Main busy keeps checking (review/astra round 2 note)", async () => {
    vi.useFakeTimers();
    let busy = 0;
    const { next, emit, commands, sent, selfReload } = setup({ busy: () => busy });
    const context = fakeContext("s-late-busy", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context);
    expect(sent).toHaveLength(1);
    busy = 1; // an actor run started between the settle and the deferred command
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).not.toHaveBeenCalled();
    busy = 0; // it ended with no Main turn
    await vi.advanceTimersByTimeAsync(6_000);
    expect(sent).toHaveLength(2);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(1);
  });

  it("the command defers when a turn or a queued message arrived meanwhile", async () => {
    const { next, commands, selfReload } = setup();
    const state = { idle: true, pending: true };
    const context = fakeContext("s-pending", state);
    selfReload.sessionStart("startup", context as never);
    activate(next);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    state.pending = false;
    state.idle = false;
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).not.toHaveBeenCalled();
  });

  it("an opted-out Main only says a newer release is active", async () => {
    process.env[AUTO_RELOAD_OPT_OUT_ENV] = "1";
    const { next, emit, sent, selfReload } = setup();
    const context = fakeContext("s-optout", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("turn_end", context);
    emit("turn_end", context);
    emit("agent_settled", context);
    expect(sent).toEqual([]);
    expect(context.notices).toEqual(["Newer Fabric active: bbb (auto-reload off; /reload loads it)"]);
  });

  it("the fabric.json setting also opts out", () => {
    const { next, emit, sent, selfReload } = setup({ configured: false });
    const context = fakeContext("s-config", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context);
    expect(sent).toEqual([]);
  });

  it("task agents, actors and a Fabric loaded from outside the profile never chase it", () => {
    const { next, emit, sent, selfReload } = setup();
    process.env.PI_FABRIC_PARENT_RUN = "run-1";
    const child = fakeContext("s-child", { idle: true, pending: false });
    selfReload.sessionStart("startup", child as never);
    activate(next);
    emit("agent_settled", child);
    expect(sent).toEqual([]);
    delete process.env.PI_FABRIC_PARENT_RUN;

    // Loaded from a dev checkout while the profile already activates another release.
    const dev = fakePi();
    const devReload = installSelfReload(dev.pi as never, {
      busy: () => 0,
      autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(release("dev"), "dist", "index.js")).href,
      settingsPath: settingsPath(),
    });
    const main = fakeContext("s-dev", { idle: true, pending: false });
    devReload.sessionStart("startup", main as never);
    dev.emit("agent_settled", main);
    expect(dev.sent).toEqual([]);
    // An install then swaps the profile to another release: a dev-path load still never follows
    // (a reload would load the dev path again and report dev -> dev; review/astra on #158).
    activate(release("ccc"));
    dev.emit("turn_end", main);
    dev.emit("agent_settled", main);
    expect(dev.sent).toEqual([]);
    expect(main.notices).toEqual([]);
  });
});
