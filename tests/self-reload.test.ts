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
  reloadTargetUiHold,
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
    sendUserMessage: vi.fn((text: string, _options?: unknown) => sent.push(text)),
  };
  const emit = (name: string, context: unknown, event: unknown = {}) =>
    handlers.get(name)?.forEach((handler) => handler(event, context));
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
  const setup = (options: { busy?: () => number; configured?: boolean; halted?: () => boolean; turnProvenance?: boolean } = {}) => {
    const old = release("aaa");
    const next = release("bbb");
    activate(old);
    const { pi, emit, commands, sent } = fakePi();
    if (options.turnProvenance) Object.assign(pi, { hostCapabilities: { turnProvenance: 1 } });
    const selfReload = installSelfReload(pi as never, {
      busy: options.busy ?? (() => 0),
      selfReloadConcurrency: () => 0, // legacy behavior; admission is covered separately
      autoReloadConfigured: () => options.configured ?? true,
      moduleUrl: pathToFileURL(path.join(old, "dist", "index.js")).href,
      settingsPath: settingsPath(),
      reloadTargetUiHold, // Production adapter: this fake old host intentionally lacks holdState.
      ...(options.halted ? { halted: options.halted } : {}),
    });
    return { old, next, pi, emit, commands, sent, selfReload };
  };

  it.each([false, true])("keeps participant-free Fabric reload commands untokenized and unclaimed (capable=%s)", async turnProvenance => {
    const { next, pi, emit, commands, selfReload } = setup({ turnProvenance });
    const context = fakeContext(`s-fabric-provenance-${turnProvenance}`, { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context);
    emit("agent_settled", context);
    expect(pi.sendUserMessage).toHaveBeenCalledExactlyOnceWith(`/${SELF_RELOAD_COMMAND} auto`,
      { expandPromptTemplates: true });
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(1);
    emit("session_shutdown", context);
  });

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
      selfReloadConcurrency: () => 0, // legacy behavior; admission is covered separately
      autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(next, "dist", "index.js")).href,
      settingsPath: settingsPath(),
    });
    expect(reloaded.sessionStart("reload", context as never)).toEqual({ old: "aaa", new: "bbb" });
    expect(reloaded.sessionStart("reload", context as never)).toBeUndefined();
    fresh.emit("agent_settled", context);
    expect(fresh.sent).toEqual([]);
  });

  it("rechecks a Fabric rollback at command execution even when mtime did not change", async () => {
    const { old, next, emit, commands, sent, selfReload } = setup();
    const context = fakeContext("s-fabric-rollback", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next); emit("agent_settled", context); expect(sent).toHaveLength(1);
    const stat = fs.statSync(settingsPath());
    activate(old); fs.utimesSync(settingsPath(), stat.atime, stat.mtime);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).not.toHaveBeenCalled();
  });

  it("superseded queued Fabric keeps its idle retry without another Main turn", async () => {
    vi.useFakeTimers();
    const { next, emit, commands, sent, selfReload } = setup();
    const third = release("ccc");
    const context = fakeContext("s-fabric-superseded", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next); emit("agent_settled", context); expect(sent).toHaveLength(1);
    activate(third);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).not.toHaveBeenCalled(); // never retarget the queued command
    await vi.advanceTimersByTimeAsync(6_000); // no input, agent turn or second settle
    expect(sent).toHaveLength(2);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(sent).toHaveLength(2); // the exact third target is consumed once
    emit("session_shutdown", context);
  });

  it("attempts each exact Fabric target once even across intervening failed targets", async () => {
    const { next, emit, commands, sent, selfReload } = setup();
    const third = release("ccc");
    const context = fakeContext("s-fabric-exact-attempt", { idle: true, pending: false });
    context.reload.mockRejectedValue(new Error("native failure"));
    selfReload.sessionStart("startup", context as never);
    for (const target of [next, third]) {
      activate(target); emit("agent_settled", context);
      await expect(commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context)).rejects.toThrow("native failure");
    }
    activate(next); emit("agent_settled", context);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(sent).toHaveLength(2); expect(context.reload).toHaveBeenCalledTimes(2);
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

  it("reports a reload held by background work once per target (smarty-dev#2216)", async () => {
    vi.useFakeTimers();
    const old = release("aaa");
    const next = release("bbb");
    activate(old);
    const { pi, emit, sent } = fakePi();
    const published: Array<{ reason: string; heldForMs: number; target: string }> = [];
    const selfReload = installSelfReload(pi as never, {
      busy: () => 1, // a dev server that never ends
      selfReloadConcurrency: () => 0, // legacy behavior; admission is covered separately
      autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(old, "dist", "index.js")).href,
      settingsPath: settingsPath(),
      heldNoticeMs: 60_000,
      publishHeld: (data) => published.push(data),
    });
    const context = fakeContext("s-held", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context);
    await vi.advanceTimersByTimeAsync(50_000);
    expect(context.notices).toEqual([]);
    expect(published).toEqual([]);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(context.notices).toHaveLength(1);
    expect(context.notices[0]).toMatch(/^Fabric reload to bbb held \d+ min: 1 task agent/);
    expect(context.notices[0]).not.toContain("\n");
    expect(published).toEqual([{ reason: expect.stringContaining("still running"), heldForMs: expect.any(Number), target: fs.realpathSync(next) }]);
    expect(published[0]!.heldForMs).toBeGreaterThanOrEqual(60_000);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    emit("agent_settled", context);
    expect(context.notices).toHaveLength(1);
    expect(published).toHaveLength(1);
    expect(sent).toEqual([]);
  });

  it("reports once per continuous background hold, not once per target (smarty-dev#2216)", async () => {
    vi.useFakeTimers();
    const old = release("aaa"), next = release("bbb");
    activate(old);
    const { pi, emit, sent } = fakePi();
    let busy = 1;
    const published: unknown[] = [];
    const selfReload = installSelfReload(pi as never, {
      busy: () => busy, autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(old, "dist", "index.js")).href,
      settingsPath: settingsPath(), heldNoticeMs: 60_000,
      publishHeld: data => { published.push(data); },
    });
    const state = { idle: true, pending: false };
    const context = fakeContext("s-continuous-held", state);
    selfReload.sessionStart("startup", context as never);
    activate(next); emit("agent_settled", context);
    await vi.advanceTimersByTimeAsync(65_000);
    expect(context.notices).toHaveLength(1); expect(published).toHaveLength(1);
    busy = 0; state.pending = true; // input holds the target while background work clears
    await vi.advanceTimersByTimeAsync(5_000);
    busy = 1;
    await vi.advanceTimersByTimeAsync(55_000);
    expect(context.notices).toHaveLength(1); expect(published).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(context.notices).toHaveLength(2); expect(published).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(context.notices).toHaveLength(2); expect(published).toHaveLength(2);
    expect(context.notices.every(line => !line.includes("\n"))).toBe(true);
    expect(sent).toEqual([]);
    emit("session_shutdown", context);
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

  it("an Escape halt holds the automatic reload until the user's next input (review/astra on #158)", async () => {
    vi.useFakeTimers();
    let halted = true;
    let busy = 1;
    const { next, emit, commands, sent, selfReload } = setup({ busy: () => busy, halted: () => halted });
    const context = fakeContext("s-halted", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    emit("agent_settled", context, { outcome: "completed" });
    busy = 0;
    await vi.advanceTimersByTimeAsync(20_000); // no retry tick requests it either
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(sent).toEqual([]);
    expect(context.reload).not.toHaveBeenCalled();
    // An extension's prompt does not resume; the user's input does (and lifts the actor halt).
    emit("input", context, { source: "extension" });
    halted = false;
    emit("input", context, { source: "interactive" });
    emit("agent_settled", context, { outcome: "completed" });
    expect(sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
    await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(1);
  });

  it("an aborted or failed settle holds the automatic reload until the user's next input", async () => {
    vi.useFakeTimers();
    let busy = 1;
    const { next, emit, commands, sent, selfReload } = setup({ busy: () => busy });
    const context = fakeContext("s-aborted", { idle: true, pending: false });
    selfReload.sessionStart("startup", context as never);
    activate(next);
    for (const outcome of ["aborted", "error"]) {
      emit("agent_settled", context, { outcome });
      busy = 0;
      await vi.advanceTimersByTimeAsync(20_000);
      await commands.get(SELF_RELOAD_COMMAND)!.handler("auto", context);
      emit("input", context, { source: "extension" });
      emit("agent_settled", context, { outcome: "completed" }); // e.g. Fabric's inbox follow-up
      expect(sent).toEqual([]);
      expect(context.reload).not.toHaveBeenCalled();
      busy = 1;
    }
    busy = 0;
    emit("input", context, { source: "interactive" });
    emit("agent_settled", context, { outcome: "completed" });
    expect(sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
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
      selfReloadConcurrency: () => 0, // legacy behavior; admission is covered separately
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

describe("restore to an older release (smarty-dev#3324)", () => {
  // Each runtime is a fresh module instance loaded from one release; the process-global attempt
  // memory and handoff survive Pi's native reload exactly like production.
  const runtime = (root: string) => {
    const fake = fakePi();
    const controller = installSelfReload(fake.pi as never, {
      busy: () => 0, selfReloadConcurrency: () => 0, autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(root, "dist", "index.js")).href, settingsPath: settingsPath(),
      reloadTargetUiHold,
    });
    return { ...fake, controller, command: fake.commands.get(SELF_RELOAD_COMMAND)! };
  };
  // A Main that already self-reloaded onto `a` once (the earlier activation of the release that is
  // later restored), then onto `b`.
  const history = async (session: string) => {
    const older = release("c00"), a = release("dd65"), b = release("ab85");
    activate(older);
    const context = fakeContext(session, { idle: true, pending: false });
    let current = runtime(older);
    current.controller.sessionStart("startup", context as never);
    for (const [from, to] of [[older, a], [a, b]] as const) {
      activate(to); current.emit("agent_settled", context);
      expect(current.sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
      await current.command.handler("auto", context);
      current.emit("session_shutdown", context);
      current = runtime(to);
      expect(current.controller.sessionStart("reload", context as never))
        .toEqual({ old: path.basename(from), new: path.basename(to) });
    }
    expect(context.reload).toHaveBeenCalledTimes(2);
    return { a, b, context, current };
  };

  const legacyState = (session: string, targets: string[], handoff: { old: string; target: string; owner?: string; resource?: string }) => {
    const globals = globalThis as Record<symbol, unknown>;
    const attempts = globals[Symbol.for("pi-fabric.self-reload.attempts")] as Map<string, Set<string>>;
    attempts.set(session, new Set(targets));
    (globals[Symbol.for("pi-fabric.self-reload.fabric-attempts")] as Map<string, Set<string>>).delete(session);
    ((globals[Symbol.for("pi-fabric.self-reload")] ??= new Map()) as Map<string, unknown>).set(session, handoff);
    return attempts;
  };

  it("classifies inherited package roots and pruned release roots, never resource entrypoints", () => {
    const a = release("dd65"), b = release("ab85"), unrelated = release("code", "smarty-code");
    const alias = path.join(dir, "fabric-alias"); fs.symlinkSync(a, alias, "junction");
    const pruned = path.join(dir, "releases", "c00cccc");
    const nested = path.join(pruned, "dist", "index.js");
    const resource = path.join(dir, "releases", "failed.js");
    const missingResource = path.join(dir, "releases", "failed.ts");
    fs.writeFileSync(resource, "export default () => {};\n");
    activate(b);
    const current = runtime(b), session = "s-legacy-roots";
    const attempts = legacyState(session, [a, alias, b, pruned, unrelated, nested, resource, missingResource], { old: a, target: b });
    const context = fakeContext(session, { idle: true, pending: false });
    expect(current.controller.sessionStart("reload", context as never)).toEqual({ old: "dd65", new: "ab85" });
    expect(attempts.get(session)).toEqual(new Set([unrelated, nested, resource, missingResource]));
    current.emit("session_shutdown", context);
  });

  it.each(["startup", "wrong-root", "resource"])("does not migrate legacy guards without confirmed Fabric takeover (%s)", mode => {
    const a = release("dd65"), b = release("ab85");
    activate(b);
    const current = runtime(b), session = `s-legacy-unconfirmed-${mode}`;
    const resource = path.join(a, "dist", "index.js");
    const targets = [a, b, resource];
    const attempts = legacyState(session, targets, { old: a, target: mode === "wrong-root" ? a : b,
      ...(mode === "resource" ? { resource, owner: "test-resource" } : {}) });
    const context = fakeContext(session, { idle: true, pending: false });
    current.controller.sessionStart(mode === "startup" ? "startup" : "reload", context as never);
    expect(attempts.get(session)).toEqual(new Set(targets));
    current.emit("session_shutdown", context);
  });

  it("does not infer pruned release roots from a directory outside the active profile", () => {
    const a = release("dd65"), b = release("ab85");
    const pruned = path.join(dir, "releases", "c00cccc");
    activate(a);
    const current = runtime(b), session = "s-legacy-outside";
    const attempts = legacyState(session, [a, b, pruned], { old: a, target: b });
    const context = fakeContext(session, { idle: true, pending: false });
    current.controller.sessionStart("reload", context as never);
    expect(attempts.get(session)).toEqual(new Set([pruned]));
    current.emit("session_shutdown", context);
  });

  it("a runtime on the activated release self-reloads back onto the restored older one once idle", async () => {
    const { a, b, context, current } = await history("s-restore-auto");
    activate(a); // --restore
    current.emit("agent_settled", context);
    expect(current.sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
    await current.command.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(3);
    current.emit("session_shutdown", context);
    const restored = runtime(a);
    expect(restored.controller.sessionStart("reload", context as never)).toEqual({ old: "ab85", new: "dd65" });
    // A later re-activation of the same release is followed again.
    activate(b); restored.emit("agent_settled", context);
    expect(restored.sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
    await restored.command.handler("auto", context);
    expect(context.reload).toHaveBeenCalledTimes(4);
    restored.emit("session_shutdown", context);
  });

  it("the command loads the restored older release on demand and says where it moves", async () => {
    const { a, context, current } = await history("s-restore-command");
    activate(a);
    await current.command.handler("", context);
    expect(context.reload).toHaveBeenCalledTimes(3);
    expect(context.notices.at(-1)).toBe("Reloading Fabric ab85 -> dd65 (the release the Pi profile activates; it may be older).");
    current.emit("session_shutdown", context);
  });

  it("a runtime loaded from outside the profile still never follows a restore", async () => {
    const a = release("dd65"), b = release("ab85");
    activate(b);
    const dev = runtime(release("dev"));
    const context = fakeContext("s-restore-dev", { idle: true, pending: false });
    dev.controller.sessionStart("startup", context as never);
    activate(a); dev.emit("agent_settled", context);
    await dev.command.handler("", context);
    expect(dev.sent).toEqual([]);
    expect(context.reload).not.toHaveBeenCalled();
    dev.emit("session_shutdown", context);
  });
});
