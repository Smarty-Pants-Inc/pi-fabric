import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { activeFabricRoot, loadedFabricRoot, releaseLabel, resolveAgentDir, SELF_RELOAD_COMMAND } from "../core/agent-dir.js";
// Preserve the self-reload module's public selector API for callers and tests.
export { activeFabricRoot, loadedFabricRoot, releaseLabel, SELF_RELOAD_COMMAND } from "../core/agent-dir.js";

// smarty-dev#2160: a Main loaded from one Fabric release reloads itself when the Pi profile's
// settings.json activates another. The installer swaps the one `packages` entry that is Fabric
// (bin/smarty-install swap_package); Fabric finds that entry by its package name, not a path
// pattern, so any profile works.

export const AUTO_RELOAD_OPT_OUT_ENV = "PI_FABRIC_NO_AUTO_RELOAD";
export const RELOADED_TOPIC = "ops.fabric.reloaded";
export const SELF_RELOAD_STATUS = "fabric-reload";
export const RELOAD_HELD_TOPIC = "ops.fabric.reload_held";
/** A reload held this long by background work is reported once per target (smarty-dev#2216). */
export const RELOAD_HELD_NOTICE_MS = 10 * 60_000;
const RETRY_MS = 5_000;

/**
 * Watches the profile settings.json for a different active Fabric release. A turn end costs one
 * stat; the file is read only when its mtime moves. Only a runtime loaded from the release the
 * profile activated at load follows it: one loaded from elsewhere (-e, a project package) never does.
 */
export class ActiveReleaseWatch {
  readonly #settingsPath: string;
  readonly #loaded: string;
  #mtimeMs = -1;
  #disabled = false;
  #target: string | undefined;
  #active: string | undefined;

  constructor(loaded: string, settingsPath = path.join(resolveAgentDir(), "settings.json")) {
    this.#loaded = loaded;
    this.#settingsPath = settingsPath;
    this.#read();
    // Loaded from outside the profile (pi -e, a project package): a reload would load the same
    // outside path again, so this runtime never follows the profile (review/astra on pi-fabric#158).
    this.#disabled = this.#active !== loaded;
    this.#target = undefined;
  }

  get loaded(): string { return this.#loaded; }

  /** The newly active release root to reload onto, or undefined. */
  check(): string | undefined {
    if (this.#disabled) return undefined;
    let mtimeMs: number;
    try { mtimeMs = fs.statSync(this.#settingsPath).mtimeMs; } catch { return this.#target; }
    if (mtimeMs !== this.#mtimeMs) this.#read(mtimeMs);
    return this.#target;
  }

  #read(mtimeMs?: number): void {
    try { this.#mtimeMs = mtimeMs ?? fs.statSync(this.#settingsPath).mtimeMs; } catch { this.#mtimeMs = -1; }
    const active = (this.#active = activeFabricRoot(this.#settingsPath));
    this.#target = active && active !== this.#loaded ? active : undefined;
  }
}

// ponytail: a process-global handoff, like stopped-runs' (smarty-dev#1882). A reload replaces this
// module but keeps the process and the session id; the next runtime reports what it replaced.
const HANDOFF = Symbol.for("pi-fabric.self-reload");
interface SelfReloadHandoff { old: string; target: string; reported?: boolean }
const handoffs = (): Map<string, SelfReloadHandoff> =>
  ((globalThis as Record<symbol, unknown>)[HANDOFF] ??= new Map<string, SelfReloadHandoff>()) as Map<string, SelfReloadHandoff>;

export const rememberSelfReload = (sessionId: string, old: string, target: string): void => {
  handoffs().set(sessionId, { old, target });
};

/** Whether this process already tried to reload this session onto the target (no retry loop). */
export const attemptedSelfReload = (sessionId: string, target: string): boolean =>
  handoffs().get(sessionId)?.target === target;

/** The finished self-reload for the new runtime, once. */
export const takeSelfReload = (sessionId: string, reason: string): SelfReloadHandoff | undefined => {
  const handoff = handoffs().get(sessionId);
  if (reason !== "reload" || !handoff || handoff.reported) return undefined;
  handoff.reported = true;
  return handoff;
};

export const autoReloadOptedOut = (configured: boolean): boolean =>
  !configured || /^(1|true|yes)$/i.test(process.env[AUTO_RELOAD_OPT_OUT_ENV]?.trim() ?? "");

/** A top-level Main in an interactive or RPC Pi: not a task agent, an actor or a one-shot `pi -p`. */
export const selfReloadEligible = (mode: string, environment: NodeJS.ProcessEnv = process.env): boolean =>
  (mode === "tui" || mode === "rpc") && !environment.PI_FABRIC_ACTOR_ID?.trim() && !environment.PI_FABRIC_PARENT_RUN?.trim();

const hostSettling = (context: ExtensionContext): boolean =>
  (context as { isSettling?: () => boolean }).isSettling?.() ?? false;
const promptPending = (context: ExtensionContext): boolean =>
  (context as { isPromptPending?: () => boolean }).isPromptPending?.() ?? false;

export interface SelfReloadDeps {
  /** Task agents this Main started that are still running, plus in-flight runs of actors it hosts. */
  busy(): number;
  /** fabric.json autoReload. */
  autoReloadConfigured(): boolean;
  moduleUrl: string;
  settingsPath?: string;
  /** Best-effort mesh publish of RELOAD_HELD_TOPIC; the runtime skips it when the mesh is off. */
  publishHeld?(data: { reason: string; heldForMs: number; target: string }): void;
  /** Test override of RELOAD_HELD_NOTICE_MS. */
  heldNoticeMs?: number;
  /** The user's Escape stop-the-world halt of actors and Jev observers is in force. */
  halted?(): boolean;
}

/**
 * smarty-dev#2160: the turn end stats settings.json; the run's settle requests the reload through
 * Pi's own command context (ctx.reload is command-only, and Pi refuses it while streaming). The
 * command re-checks safety when it runs, so a turn that started meanwhile only defers it.
 */
export const installSelfReload = (pi: ExtensionAPI, deps: SelfReloadDeps) => {
  let watch: ActiveReleaseWatch | undefined;
  let noticed: string | undefined;
  let retry: ReturnType<typeof setInterval> | undefined;
  let held: { target: string; since: number; reported: boolean } | undefined;
  // An explicit stop (Escape, a failed run) holds the automatic reload until the user speaks again:
  // a reload would re-arm the halted actors and the inbox wake (review/astra on pi-fabric#158).
  let stopped = false;
  const userHalted = (): boolean => stopped || (deps.halted?.() ?? false);
  /** An unbounded job (a dev server, tail -f) can hold the reload forever: say so once. */
  const noteHeld = (context: ExtensionContext, target: string, busy: number): void => {
    const now = Date.now();
    if (held?.target !== target) held = { target, since: now, reported: false };
    const heldForMs = now - held.since;
    if (held.reported || heldForMs < (deps.heldNoticeMs ?? RELOAD_HELD_NOTICE_MS)) return;
    held.reported = true;
    const reason = `${busy} task agent(s), actor run(s), shell job(s) or Jev run(s) still running`;
    if (context.hasUI) {
      context.ui.notify(`Fabric reload to ${releaseLabel(target)} held ${Math.round(heldForMs / 60_000)} min: ${reason} (stop them or /reload)`, "warning");
    }
    deps.publishHeld?.({ reason, heldForMs, target });
  };
  // Pi keeps isIdle() true while a prompt is in preflight and while agent_settled handlers run; a
  // reload then would pull the runner out from under that prompt (review/astra on pi-fabric#158).
  // Pi runs this extension command before it marks a preflight, so the command's own prompt does not count.
  const hostIdle = (context: ExtensionContext): boolean =>
    context.isIdle() && !hostSettling(context) && !promptPending(context);
  const safe = (context: ExtensionContext): boolean =>
    deps.busy() === 0 && hostIdle(context) && !context.hasPendingMessages();
  const stopRetry = (): void => {
    if (retry) clearInterval(retry);
    retry = undefined;
  };
  /** Request the reload now if it is safe; false while the Main or its children are busy. */
  const request = (context: ExtensionContext): boolean => {
    const target = watch?.check();
    if (!target || autoReloadOptedOut(deps.autoReloadConfigured())) return true;
    if (attemptedSelfReload(context.sessionManager.getSessionId(), target)) return true;
    if (userHalted()) return true; // the user's next input settles a run that re-checks
    const busy = deps.busy();
    if (busy > 0) noteHeld(context, target, busy);
    if (busy > 0 || context.hasPendingMessages()) return false;
    pi.sendUserMessage(`/${SELF_RELOAD_COMMAND} auto`, { expandPromptTemplates: true });
    return true;
  };

  pi.on("turn_end", (_event, context) => {
    const target = watch?.check();
    if (!target || !autoReloadOptedOut(deps.autoReloadConfigured()) || noticed === target) return;
    noticed = target;
    if (context.hasUI) {
      context.ui.notify(`Newer Fabric active: ${releaseLabel(target)} (auto-reload off; /reload loads it)`, "info");
    }
  });

  pi.on("agent_settled", (event, context) => {
    stopRetry();
    const outcome = (event as { outcome?: string }).outcome;
    if (outcome === "aborted" || outcome === "error") stopped = true;
    // Pi defers a prompt sent while agent_settled handlers run until they finish.
    if (!request(context)) armRetry(context);
  });

  /**
   * Busy: the last task agent or actor run can end with no new turn (an actor bound to
   * agent_settled starts right now), so re-check while the Main stays idle.
   */
  const armRetry = (context: ExtensionContext): void => {
    stopRetry();
    retry = setInterval(() => {
      try {
        if (!context.isIdle()) return stopRetry(); // a new run settles and re-checks
        if (!hostIdle(context)) return; // a prompt is starting: its settle re-checks, or the next tick
        if (request(context)) stopRetry();
      } catch {
        stopRetry(); // stale context: the session was replaced or reloaded
      }
    }, RETRY_MS);
    retry.unref?.();
  };

  pi.on("session_shutdown", () => stopRetry());
  let statusShown = false;
  pi.on("input", (event, context) => {
    // Only the user resumes: an extension's own prompt (Fabric's inbox follow-up) does not.
    if ((event as { source?: string }).source !== "extension") stopped = false;
    if (!statusShown) return;
    statusShown = false;
    if (context.hasUI) context.ui.setStatus(SELF_RELOAD_STATUS, undefined);
  });

  pi.registerCommand(SELF_RELOAD_COMMAND, {
    description: "Reload onto the Fabric release the Pi profile activates (Fabric runs it after a run)",
    handler: async (args, context) => {
      const auto = args.trim() === "auto";
      const target = watch?.check();
      const say = (message: string) => { if (!auto && context.hasUI) context.ui.notify(message, "info"); };
      if (!watch || !target) return say("No newer Fabric release is active.");
      if (auto && userHalted()) return;
      if (!safe(context)) {
        // A run that started after the settle can end with no Main turn: keep checking (round 2 note).
        if (auto) armRetry(context);
        return say(autoReloadOptedOut(deps.autoReloadConfigured())
          ? "Fabric is busy (task agents, actor runs, shell jobs or Jev runs); run this again when they finish."
          : "Fabric reloads after its task agents, actor runs, shell jobs and Jev runs finish.");
      }
      const sessionId = context.sessionManager.getSessionId();
      if (auto && attemptedSelfReload(sessionId, target)) return;
      rememberSelfReload(sessionId, watch.loaded, target);
      await context.reload();
      // The old runtime ends here: nothing after reload may touch it.
    },
  });

  return {
    /** Arm the watch for a new runtime; returns the finished self-reload this runtime completes. */
    sessionStart(reason: string, context: ExtensionContext): { old: string; new: string } | undefined {
      stopRetry();
      noticed = undefined;
      held = undefined;
      stopped = false;
      const loaded = loadedFabricRoot(deps.moduleUrl);
      if (!loaded || !selfReloadEligible(context.mode)) {
        watch = undefined;
        return undefined;
      }
      // A session switch in this runtime keeps the watch (and its profile check) taken at load.
      if (watch?.loaded !== loaded) watch = new ActiveReleaseWatch(loaded, deps.settingsPath);
      const done = takeSelfReload(context.sessionManager.getSessionId(), reason);
      statusShown = done !== undefined; // index.ts shows the footer notice for a finished self-reload
      return done ? { old: releaseLabel(done.old), new: releaseLabel(loaded) } : undefined;
    },
  };
};
