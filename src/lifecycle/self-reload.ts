import fs from "node:fs";
import { fabricHostIdentity, sendFabricUserMessage } from "../fabric-provenance.js";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { resolveAgentDir } from "../core/agent-dir.js";
import type { ResourceBinding } from "./reload-target-profile.js";

// smarty-dev#2160: a Main loaded from one Fabric release reloads itself when the Pi profile's
// settings.json activates another. The installer swaps the one `packages` entry that is Fabric
// (bin/smarty-install swap_package); Fabric finds that entry by its package name, not a path
// pattern, so any profile works.

export const AUTO_RELOAD_OPT_OUT_ENV = "PI_FABRIC_NO_AUTO_RELOAD";
export const SELF_RELOAD_COMMAND = "fabric-release-reload";
export const RELOADED_TOPIC = "ops.fabric.reloaded";
export const SELF_RELOAD_STATUS = "fabric-reload";
export const RELOAD_HELD_TOPIC = "ops.fabric.reload_held";
export const RELOAD_TARGET_TOPIC = "pi-fabric:reload-target:v1";
export const RELOAD_TARGET_RESULT_TOPIC = `${RELOAD_TARGET_TOPIC}:result`;

export interface ReloadTargetRequest {
  requestId: string;
  /** Attribution only; this label confers no authority. */
  owner: string;
  /** The canonical entrypoint bound before the profile pointer changed. */
  resource: string;
  loaded: string;
  configured: string;
  reason: string;
}
export interface ReloadTargetResult {
  requestId: string;
  owner: string;
  resource: string;
  accepted: boolean;
  reason: string;
  target: string | null;
}
/** A reload held this long by background work is reported once per target (smarty-dev#2216). */
export const RELOAD_HELD_NOTICE_MS = 10 * 60_000;
const PACKAGE_NAME = "pi-fabric";
const RETRY_MS = 5_000;

const packageName = (root: string): string | undefined => {
  try {
    const parsed = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")) as { name?: unknown };
    return typeof parsed.name === "string" ? parsed.name : undefined;
  } catch {
    return undefined;
  }
};

const real = (value: string): string => {
  try { return fs.realpathSync(value); } catch { return path.resolve(value); }
};

/** The Fabric package root this code loaded from: the nearest pi-fabric package.json above it. */
export const loadedFabricRoot = (moduleUrl: string): string | undefined => {
  let directory: string;
  try { directory = path.dirname(fileURLToPath(moduleUrl)); } catch { return undefined; }
  for (;;) {
    if (packageName(directory) === PACKAGE_NAME) return real(directory);
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
};

/** The local Fabric package the profile's settings.json activates, if exactly one. */
export const activeFabricRoot = (settingsPath: string): string | undefined => {
  let packages: unknown;
  try {
    packages = (JSON.parse(fs.readFileSync(settingsPath, "utf8")) as { packages?: unknown }).packages;
  } catch {
    return undefined;
  }
  if (!Array.isArray(packages)) return undefined;
  const base = path.dirname(settingsPath);
  const roots = packages.flatMap((entry) => {
    const source = typeof entry === "string" ? entry : (entry as { source?: unknown } | null)?.source;
    if (typeof source !== "string" || /^(npm|git|https?):/.test(source)) return [];
    const expanded = source.startsWith("~/") ? path.join(process.env.HOME ?? "", source.slice(2)) : source;
    const root = path.resolve(base, expanded);
    return packageName(root) === PACKAGE_NAME ? [real(root)] : [];
  });
  return roots.length === 1 ? roots[0] : undefined;
};

export const releaseLabel = (root: string): string => path.basename(root);

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

interface ReloadCandidate {
  kind: "fabric" | "resource";
  loaded: string;
  target: string;
  owner?: string;
  resource?: string;
  requestId?: string;
}
/** Capture explicit extension arguments once; an explicit copy must never follow a profile slot. */
const explicitExtensions = (): string[] => {
  const sources: string[] = [];
  for (let i = 2; i < process.argv.length; i++) {
    const arg = process.argv[i]!;
    if (arg === "-e" || arg === "--extension") sources.push(process.argv[++i] ?? "");
    else if (arg.startsWith("--extension=")) sources.push(arg.slice("--extension=".length));
  }
  return sources;
};
// ponytail: a process-global handoff, like stopped-runs' (smarty-dev#1882). A reload replaces this
// module but keeps the process and the session id; the next runtime reports what it replaced.
const HANDOFF = Symbol.for("pi-fabric.self-reload");
const ATTEMPTS = Symbol.for("pi-fabric.self-reload.attempts");
interface SelfReloadHandoff { old: string; target: string; owner?: string; resource?: string; reported?: boolean }
const handoffs = (): Map<string, SelfReloadHandoff> =>
  ((globalThis as Record<symbol, unknown>)[HANDOFF] ??= new Map<string, SelfReloadHandoff>()) as Map<string, SelfReloadHandoff>;

export const rememberSelfReload = (sessionId: string, old: string, target: string): void => {
  handoffs().set(sessionId, { old, target });
};

/** Whether this process already tried to reload this session onto the target (no retry loop). */
export const attemptedSelfReload = (sessionId: string, target: string): boolean =>
  handoffs().get(sessionId)?.target === target
  || (((globalThis as Record<symbol, unknown>)[ATTEMPTS] as Map<string, Set<string>> | undefined)?.get(sessionId)?.has(target) ?? false);

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
  /**
   * Supported host adapter for GLOBAL dialog, custom UI and external-editor holds.
   * Undefined means clear; an unsupported-host:* reason fails closed. The pinned Pi host has
   * no such query. Never substitute a caller's own UI state for the global host state.
   */
  reloadTargetUiHold?(context: ExtensionContext): string | undefined;
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
  let contextNow: ExtensionContext | undefined;
  let generation = 0;
  let profileHelpers: typeof import("./reload-target-profile.js") | undefined;
  let profileLoad: Promise<typeof import("./reload-target-profile.js")> | undefined;
  let sessionId: string | undefined;
  let scheduled: { candidate: ReloadCandidate; token: string } | undefined;
  const commandSequenceKey = Symbol.for("pi-fabric.self-reload.command-sequence");
  const settingsPath = deps.settingsPath ?? path.join(resolveAgentDir(), "settings.json");
  const explicit = explicitExtensions();
  const bindings = new Map<string, ResourceBinding>();
  // Fabric and public resources share one pending set; a queued command owns no retry.
  const pending = new Map<string, ReloadCandidate>();
  const pendingKey = (candidate: ReloadCandidate): string =>
    candidate.kind === "fabric" ? "fabric" : `resource:${candidate.resource!}`;
  // Keep exact-target attempts through native reloads, even if a different target was tried later.
  const attempts = ((globalThis as Record<symbol, unknown>)[ATTEMPTS] ??= new Map<string, Set<string>>()) as Map<string, Set<string>>;
  const attempted = (id: string, candidate: ReloadCandidate): boolean => attemptedSelfReload(id, candidate.target);
  // Escape and failed/aborted runs hold reload until user input, never extension followups.
  let stopped = false;
  const userHalted = (): boolean => stopped || (deps.halted?.() ?? false);
  const reply = (request: Pick<ReloadTargetRequest, "requestId" | "owner" | "resource">, accepted: boolean, reason: string, target?: string): void => {
    pi.events?.emit(RELOAD_TARGET_RESULT_TOPIC, { requestId: request.requestId, owner: request.owner, resource: request.resource,
      accepted, reason, target: target ?? null } satisfies ReloadTargetResult);
  };
  const targetFor = (binding: ResourceBinding): { target?: string; reason?: string } =>
    profileHelpers?.targetFor(binding, settingsPath) ?? { reason: "profile-validation-unavailable" };
  const hostLimit = (context: ExtensionContext): string | undefined => {
    const host = context as ExtensionContext & { isSettling?: () => boolean; isPromptPending?: () => boolean };
    if (typeof host.isPromptPending !== "function") return "unsupported-host:isPromptPending";
    if (typeof host.isSettling !== "function") return "unsupported-host:isSettling";
    if (typeof context.ui?.getEditorText !== "function") return "unsupported-host:getEditorText";
    if (!deps.reloadTargetUiHold) return "unsupported-host:global-dialog/editor-hold-query";
    try {
      const hold = deps.reloadTargetUiHold(context);
      return hold?.startsWith("unsupported-host:") ? hold : undefined;
    } catch { return "unsupported-host:ui-hold-query-failed"; }
  };
  const resourceHold = (context: ExtensionContext): string | undefined => {
    const limit = hostLimit(context);
    if (limit) return limit;
    try {
      const text = context.ui.getEditorText();
      if (typeof text !== "string") return "unsupported-host:getEditorText-result";
      if (text.length > 0) return "editor-not-empty";
      return deps.reloadTargetUiHold!(context);
    } catch { return "unsupported-host:ui-hold-query-failed"; }
  };
  const invalidate = (candidate: ReloadCandidate, reason: string, target?: string): void => {
    const key = pendingKey(candidate);
    if (pending.get(key)?.target === candidate.target) pending.delete(key);
    if (candidate.kind !== "resource") return;
    // A newer advertisement for this resource may have arrived after the command was queued.
    // Refuse the queued request by its original ID, not the newer request's correlation fields.
    reply({ requestId: candidate.requestId!, owner: candidate.owner!, resource: candidate.resource! }, false, reason, target);
  };
  const recheck = (candidate: ReloadCandidate): boolean => {
    if (candidate.kind === "fabric") {
      if (activeFabricRoot(settingsPath) === candidate.target) return true;
      invalidate(candidate, "target-changed");
      return false;
    }
    const binding = bindings.get(candidate.resource!);
    const active = binding ? targetFor(binding) : { reason: "unbound-resource" };
    const reason = active.reason ?? (active.target === candidate.loaded ? "target-reverted"
      : active.target !== candidate.target ? "target-changed" : undefined);
    if (reason) { invalidate(candidate, reason, active.target); return false; }
    return true;
  };
  const candidateNow = (): ReloadCandidate | undefined => {
    const target = watch?.check();
    if (target && watch) pending.set("fabric", { kind: "fabric", loaded: watch.loaded, target });
    else pending.delete("fabric");
    for (const [key, candidate] of pending) {
      // Consumed targets must not starve another resource after a failed native reload.
      if (sessionId && attempted(sessionId, candidate)) { pending.delete(key); continue; }
      if (recheck(candidate)) return candidate;
    }
    return undefined;
  };
  const receive = async (value: unknown): Promise<void> => {
    const data = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
    const correlation = { requestId: typeof data.requestId === "string" ? data.requestId : "",
      owner: typeof data.owner === "string" ? data.owner : "", resource: typeof data.resource === "string" ? data.resource : "" };
    if (!["requestId", "owner", "resource", "loaded", "configured", "reason"].every(key => typeof data[key] === "string")
      || !correlation.requestId || !correlation.owner) return reply(correlation, false, "invalid-request");
    const request = data as unknown as ReloadTargetRequest;
    const context = contextNow;
    if (!context || !sessionId) return reply(request, false, "no-active-session");
    // The optional profile parser is loaded only on the first public request, never at idle startup.
    const receivedGeneration = generation;
    let helpers: typeof import("./reload-target-profile.js");
    try {
      helpers = await (profileLoad ??= import("./reload-target-profile.js").then(module => (profileHelpers = module)));
    } catch { return reply(request, false, "profile-validation-unavailable"); }
    // Native reload/session replacement can invalidate an in-flight first-use import.
    if (receivedGeneration !== generation || context !== contextNow) return reply(request, false, "session-changed");
    for (const field of [request.resource, request.loaded, request.configured]) {
      if (!fs.existsSync(field)) return reply(request, false, "missing-path");
      if (!helpers.canonicalEntrypoint(field)) return reply(request, false, "noncanonical-path");
    }
    if (request.resource !== request.loaded) return reply(request, false, "resource-loaded-mismatch");
    if (helpers.explicitResource(explicit, request.loaded)) return reply(request, false, "explicit-extension");
    let binding = bindings.get(request.resource);
    if (!binding) {
      const profile = helpers.readResourceProfile(settingsPath);
      if (!profile) return reply(request, false, "profile-unreadable");
      const matching = helpers.profileSlots(profile, path.dirname(settingsPath)).filter(slot => slot.target === request.loaded);
      if (!matching.length) return reply(request, false, "outside-profile");
      if (matching.length !== 1) return reply(request, false, "ambiguous-profile-entry");
      binding = { loaded: request.loaded, slot: matching[0]!, entries: helpers.serializedEntries(profile, matching[0]!.section) };
      // Registration must occur before the pointer moves, with both paths agreeing.
      if (request.configured !== request.loaded) return reply(request, false, "configured-mismatch", request.loaded);
      bindings.set(request.resource, binding);
    }
    if (binding.loaded !== request.loaded) return reply(request, false, "resource-loaded-mismatch");
    const active = targetFor(binding);
    if (active.reason || !active.target) return reply(request, false, active.reason ?? "missing-profile-target");
    if (request.configured !== active.target) return reply(request, false, "configured-mismatch", active.target);
    if (active.target === binding.loaded) {
      const wasPending = pending.delete(`resource:${request.resource}`);
      return reply(request, !wasPending, wasPending ? "target-reverted" : "bound-unchanged", active.target);
    }
    if (autoReloadOptedOut(deps.autoReloadConfigured())) return reply(request, false, "auto-reload-disabled", active.target);
    const limit = hostLimit(context);
    if (limit) return reply(request, false, limit, active.target);
    if (attempts.get(sessionId)?.has(active.target)) return reply(request, false, "already-attempted", active.target);
    pending.set(`resource:${request.resource}`, { ...request, kind: "resource", target: active.target });
    reply(request, true, "pending", active.target);
    // Reuse the idle retry for a changed pointer with no subsequent Main turn. Advertising
    // never clears the halt or injects a command synchronously.
    if (context.isIdle() && !userHalted()) armRetry(context);
  };
  let unsubscribe: (() => void) | undefined = pi.events?.on(RELOAD_TARGET_TOPIC, receive);
  /** An unbounded job can hold reload forever: report it once per target. */
  const noteHeld = (context: ExtensionContext, target: string, busy: number): void => {
    const now = Date.now();
    if (held?.target !== target) held = { target, since: now, reported: false };
    const heldForMs = now - held.since;
    if (held.reported || heldForMs < (deps.heldNoticeMs ?? RELOAD_HELD_NOTICE_MS)) return;
    held.reported = true;
    const reason = `${busy} task agent(s), actor run(s), shell job(s) or Jev run(s) still running`;
    if (context.hasUI) context.ui.notify(`Fabric reload to ${releaseLabel(target)} held ${Math.round(heldForMs / 60_000)} min: ${reason} (stop them or /reload)`, "warning");
    deps.publishHeld?.({ reason, heldForMs, target });
  };
  const hostIdle = (context: ExtensionContext): boolean =>
    context.isIdle() && !hostSettling(context) && !promptPending(context);
  const safe = (context: ExtensionContext, candidate: ReloadCandidate): boolean =>
    deps.busy() === 0 && hostIdle(context) && !context.hasPendingMessages()
    && (candidate.kind === "fabric" || !resourceHold(context));
  const stopRetry = (): void => {
    if (retry) clearInterval(retry);
    retry = undefined;
  };
  /** One scheduler and one native command for Fabric changes and bound resource changes. */
  const request = (context: ExtensionContext): boolean => {
    const candidate = candidateNow();
    if (!candidate || autoReloadOptedOut(deps.autoReloadConfigured())) return true;
    if (attempted(context.sessionManager.getSessionId(), candidate)) return true;
    if (userHalted()) return true;
    if (candidate.kind === "resource") {
      const hold = resourceHold(context);
      if (hold?.startsWith("unsupported-host:")) { invalidate(candidate, hold, candidate.target); return false; }
      if (hold || promptPending(context) || !context.isIdle()) return false;
    }
    const busy = deps.busy();
    if (busy > 0) noteHeld(context, candidate.target, busy);
    if (busy > 0 || context.hasPendingMessages()) return false;
    if (scheduled) return false; // keep the shared idle retry until the queued command executes
    const globals = globalThis as Record<symbol, unknown>;
    const sequence = Number(globals[commandSequenceKey] ?? 0) + 1;
    globals[commandSequenceKey] = sequence;
    const token = candidate.kind === "resource" ? `resource-${sequence}` : "";
    scheduled = { candidate, token };
    sendFabricUserMessage(pi, `/${SELF_RELOAD_COMMAND} auto${token ? ` ${token}` : ""}`, () => fabricHostIdentity(context.sessionManager.getSessionId()), "followUp", { expandPromptTemplates: true }, "mesh");
    return true;
  };

  pi.on("turn_end", (_event, context) => {
    const target = watch?.check();
    if (!target || !autoReloadOptedOut(deps.autoReloadConfigured()) || noticed === target) return;
    noticed = target;
    if (context.hasUI) context.ui.notify(`Newer Fabric active: ${releaseLabel(target)} (auto-reload off; /reload loads it)`, "info");
  });
  pi.on("agent_settled", (event, context) => {
    stopRetry();
    const outcome = (event as { outcome?: string }).outcome;
    if (outcome === "aborted" || outcome === "error") stopped = true;
    // Pi defers this command until all settle handlers finish. Final admission is in the command.
    if (!request(context)) armRetry(context);
  });
  const armRetry = (context: ExtensionContext): void => {
    stopRetry();
    const id = sessionId;
    retry = setInterval(() => {
      try {
        if (id !== sessionId || !contextNow || !context.isIdle()) return stopRetry();
        if (!hostIdle(context)) return;
        if (request(context)) stopRetry();
      } catch { stopRetry(); }
    }, RETRY_MS);
    retry.unref?.();
  };
  pi.on("session_shutdown", () => {
    generation++;
    stopRetry(); bindings.clear(); pending.clear(); scheduled = undefined; contextNow = undefined;
    unsubscribe?.(); unsubscribe = undefined;
  });
  let statusShown = false;
  pi.on("input", (event, context) => {
    if ((event as { source?: string }).source !== "extension") stopped = false;
    if (!statusShown) return;
    statusShown = false;
    if (context.hasUI) context.ui.setStatus(SELF_RELOAD_STATUS, undefined);
  });

  pi.registerCommand(SELF_RELOAD_COMMAND, {
    description: "Reload onto the Fabric release or bound extension target the Pi profile activates",
    handler: async (args, context) => {
      const [mode, token = ""] = args.trim().split(/\s+/);
      const auto = mode === "auto";
      // A queued resource command is pinned to this session/target; never retarget a stale command.
      if (token && scheduled?.token !== token) return;
      const candidate = scheduled?.candidate ?? candidateNow();
      scheduled = undefined;
      // Consuming a command never consumes other pending targets. In particular, a stale
      // pinned target or a failed reload must leave an idle path to the current target.
      // Native session_start/shutdown clears this timer after a successful reload.
      if (auto && contextNow && context.sessionManager.getSessionId() === sessionId) armRetry(context);
      const say = (message: string) => { if (!auto && context.hasUI) context.ui.notify(message, "info"); };
      if (!candidate || !recheck(candidate)) return say("No newer Fabric release or bound extension target is active.");
      if (auto && (userHalted() || autoReloadOptedOut(deps.autoReloadConfigured()))) return;
      if (candidate.kind === "resource") {
        if (autoReloadOptedOut(deps.autoReloadConfigured())) return;
        const limit = hostLimit(context);
        if (limit) { invalidate(candidate, limit, candidate.target); return say(limit); }
      }
      if (!safe(context, candidate)) {
        if (auto) armRetry(context);
        return say("Fabric reload waits for Main, child work, input and UI holds to clear.");
      }
      const id = context.sessionManager.getSessionId();
      if (candidate.kind === "resource" && id !== sessionId) { invalidate(candidate, "session-changed"); return; }
      if (auto && attempted(id, candidate)) return;
      // No await between the final profile/safety checks and invoking the native reload.
      if (!recheck(candidate)) return;
      const tried = attempts.get(id) ?? new Set<string>(); tried.add(candidate.target); attempts.set(id, tried);
      if (candidate.kind === "resource") {
        pending.delete(pendingKey(candidate));
        handoffs().set(id, { old: candidate.loaded, target: candidate.target, owner: candidate.owner!, resource: candidate.resource! });
      } else rememberSelfReload(id, candidate.loaded, candidate.target);
      await context.reload();
    },
  });

  return {
    /** Arm the watch and clear session-scoped resource proofs; report a finished native reload once. */
    sessionStart(reason: string, context: ExtensionContext): { old: string; new: string; owner?: string; resource?: string; target?: string } | undefined {
      generation++;
      stopRetry(); bindings.clear(); pending.clear(); scheduled = undefined;
      noticed = undefined; held = undefined; stopped = false;
      sessionId = context.sessionManager.getSessionId();
      contextNow = selfReloadEligible(context.mode) ? context : undefined;
      if (!unsubscribe) unsubscribe = pi.events?.on(RELOAD_TARGET_TOPIC, receive);
      const loaded = loadedFabricRoot(deps.moduleUrl);
      if (!loaded || !contextNow) { watch = undefined; return undefined; }
      // Session switches keep the Fabric watch's original profile eligibility proof.
      if (watch?.loaded !== loaded) watch = new ActiveReleaseWatch(loaded, deps.settingsPath);
      const done = takeSelfReload(sessionId, reason);
      statusShown = done !== undefined;
      if (!done) return undefined;
      return done.resource ? { old: releaseLabel(done.old), new: releaseLabel(done.target), owner: done.owner!, resource: done.resource, target: done.target }
        : { old: releaseLabel(done.old), new: releaseLabel(loaded) };
    },
  };
};
