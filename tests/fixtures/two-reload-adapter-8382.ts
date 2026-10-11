// TEST-ONLY Pi extension for tests/main-two-reload-e2e.test.ts (smarty-dev#8382). It is never
// bundled or shipped; the test loads it next to the compiled Fabric with `-e`, and it refuses to
// load without the test's report directory.
//
// Why it exists: Pi's RPC mode has no native reload command (`{"type":"reload"}` answers
// "Unknown command: reload"), and the interactive built-in /reload is not reachable over RPC.
// This adapter registers /reload as an extension command whose handler calls the command
// ctx's reload(): the same in-process AgentSession.reload() an interactive /reload runs
// (session_shutdown "reload" -> invalidate -> fresh extension generation -> session_start
// "reload"). An RPC `prompt` record carrying "/reload" runs it.
//
// It also records each extension generation's lifecycle, in one process-wide sequence (`seq`) that
// orders overlapping reloads, and keeps a census of the participant heartbeat timers Fabric's
// ParticipantDirectory.start() arms, which the test reads through the /e2e-census command. A
// session replacement does not need it: Pi's RPC protocol has a native `new_session` command.
// Nothing here calls a model or provider.
import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// `generation` is the newest extension generation loaded when the timer was armed. Sequential reloads and
// replacements make that the arming generation; when two generations load concurrently it is an upper bound.
interface TrackedTimer { generation: number; createdAt: number; delay: number; timer: NodeJS.Timeout }
interface Census { generation: number; seq: number; reloads: number; tracked: TrackedTimer[] }

// Extension generations share one process and globalThis; module state does not survive reload.
const CENSUS = Symbol.for("pi-fabric.test.two-reload-8382.v2");
// The bundle keeps class and method names (tests/reload-poll-guard.test.ts relies on them too).
const HEARTBEAT_FRAME = /\bParticipantDirectory\w*\.start\b/;

const census = (): Census => {
  const holder = globalThis as unknown as Record<symbol, Census | undefined>;
  const existing = holder[CENSUS];
  if (existing) return existing;
  const state: Census = { generation: 0, seq: 0, reloads: 0, tracked: [] };
  holder[CENSUS] = state;
  // Installed once per process, before any generation's Fabric activates. A cleared Node
  // timer reports _destroyed, however it was cleared (clearInterval or clearTimeout).
  const original = globalThis.setInterval;
  globalThis.setInterval = ((...args: Parameters<typeof setInterval>) => {
    const timer = original(...args);
    if (HEARTBEAT_FRAME.test(new Error().stack ?? "")) {
      state.tracked.push({ generation: state.generation, createdAt: Date.now(), delay: Number(args[1]), timer });
    }
    return timer;
  }) as unknown as typeof setInterval;
  return state;
};

export default function twoReloadAdapter(pi: ExtensionAPI): void {
  const dir = process.env.FABRIC_8382_REPORT_DIR;
  if (!dir) throw new Error("two-reload-adapter-8382 is test-only: FABRIC_8382_REPORT_DIR is not set");
  const state = census();
  const generation = ++state.generation;
  const record = (event: Record<string, unknown>): void =>
    fs.appendFileSync(path.join(dir, "adapter.jsonl"), `${JSON.stringify({ generation, seq: ++state.seq, at: Date.now(), ...event })}\n`);
  record({ type: "factory", pid: process.pid });
  pi.on("session_start", (event, ctx) => {
    record({ type: "session_start", reason: event.reason, sessionId: ctx.sessionManager.getSessionId(),
      sessionFile: ctx.sessionManager.getSessionFile() });
  });
  pi.on("session_shutdown", (event) => {
    record({ type: "session_shutdown", reason: event.reason });
  });
  pi.registerCommand("reload", {
    description: "TEST-ONLY (smarty-dev#8382): in-process reload over RPC, which has no native reload command",
    handler: async (_args, ctx) => {
      const call = ++state.reloads;
      record({ type: "reload", call });
      await ctx.reload();
      // This generation is stale from here on: never touch ctx or pi after reload(). The record
      // only appends to the report file, which orders this reload's end against an overlapping one.
      record({ type: "reload_done", call });
    },
  });
  pi.registerCommand("e2e-census", {
    description: "TEST-ONLY (smarty-dev#8382): write the participant heartbeat timer census",
    handler: async () => {
      const timers = state.tracked.map(({ generation: owner, createdAt, delay, timer }) => ({
        generation: owner, createdAt, delay, live: !(timer as NodeJS.Timeout & { _destroyed?: boolean })._destroyed,
      }));
      fs.writeFileSync(path.join(dir, "census.json"), JSON.stringify({ generation, at: Date.now(), timers }));
    },
  });
}
