import path from "node:path";
import { callMainDrain, mainDrainSocket, MainDrainClientError } from "../main-drain/client.js";
import type { MainDrainBindingRequest } from "../main-drain/binding.js";
import type { MainDrainReply } from "../main-drain/protocol.js";
import { evaluateDrain, scanCompletions } from "../seat-drain/evaluate.js";
import { inventorySessionPending } from "./session-pending.js";

const USAGE = `Usage:
  pi-fabric session drain <session-uuid | session:uuid> --mesh <meshRoot> --status | --probe
  pi-fabric session drain <session-uuid | session:uuid> --mesh <meshRoot> --exit --pod-uid <uid>
      --run <32 hex> --run-sha256 <64 hex> --operation-id <32 hex> --generation <32 hex> [--deadline-s <1-600>]

Owner-only D3 seat drain of THIS host's live Main (smarty-dev#15310), through its local socket
<meshRoot>/main-drain/ (mode 0700, the Main's own OS user). It is never reachable from the mesh.
  --status  observe only;  --probe  the dry-run ingress probes
  --exit    raw-Pod resume-drain, inside the Pod: bind to the pod UID, drain (bound wait), run the
            same zero and children checks as the host seam, then ask the Main for a clean exit
The host seat's drain and undo run through the pinned seam (fabric-seat-drain), which proves the
seat lock. Nothing is deleted or acknowledged. Output is one JSON line; exit 0 only when the op completed.`;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const DEFAULT_DEADLINE_S = 540;
const POLL_MS = 1_000;
class UsageError extends Error {}

interface CliIo {
  stdout: Pick<NodeJS.WritableStream, "write">;
  stderr: Pick<NodeJS.WritableStream, "write">;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  uid?: number;
}

const VALUE_FLAGS = new Set(["--mesh", "--pod-uid", "--run", "--run-sha256", "--operation-id", "--generation", "--deadline-s"]);
const MODE_FLAGS = new Set(["--status", "--probe", "--exit", "--help"]);

export const runSessionDrainCli = async (
  argv: string[], io: CliIo = { stdout: process.stdout, stderr: process.stderr },
): Promise<number> => {
  try {
    const positionals: string[] = [];
    const values = new Map<string, string>();
    const modes = new Set<string>();
    for (let index = 0; index < argv.length; index++) {
      const arg = argv[index]!;
      if (!arg.startsWith("-")) { positionals.push(arg); continue; }
      if (VALUE_FLAGS.has(arg) && !values.has(arg)) {
        const value = argv[++index];
        if (!value || value.startsWith("-")) throw new UsageError(`${arg} requires a value`);
        values.set(arg, value);
      } else if (MODE_FLAGS.has(arg) && !modes.has(arg)) modes.add(arg);
      else throw new UsageError("Unknown or repeated option");
    }
    if (modes.has("--help")) { io.stdout.write(`${USAGE}\n`); return 0; }
    const sessionId = positionals[0]?.replace(/^session:/, "");
    if (positionals.length !== 1 || !sessionId || !UUID.test(sessionId)) throw new UsageError("Expected exactly one canonical session UUID or session:UUID");
    const mesh = values.get("--mesh");
    if (!mesh || !path.isAbsolute(mesh)) throw new UsageError("--mesh is required and must be absolute; there is no environment default");
    if (modes.size !== 1) throw new UsageError("Choose exactly one of --status, --probe, --exit");
    const mode = [...modes][0]!;
    const exitValues = ["--pod-uid", "--run", "--run-sha256", "--operation-id", "--generation"];
    if (mode !== "--exit" && (exitValues.some(flag => values.has(flag)) || values.has("--deadline-s"))) throw new UsageError("Binding options belong to --exit");
    const uid = io.uid ?? process.geteuid?.();
    if (uid === undefined) throw new MainDrainClientError("MAIN_DRAIN_UNSUPPORTED_PLATFORM");
    const socket = mainDrainSocket(mesh, sessionId, uid);
    const emit = (value: Record<string, unknown>, ok: boolean): number => {
      io.stdout.write(`${JSON.stringify({ format: 1, sessionId, ...value })}\n`);
      return ok ? 0 : 1;
    };
    if (mode === "--status" || mode === "--probe") {
      const op = mode === "--status" ? "status" : "probe";
      return emit({ op, reply: await callMainDrain(socket, sessionId, op, null) }, true);
    }
    const podUid = values.get("--pod-uid") ?? "";
    const run = values.get("--run") ?? "";
    const runSha256 = values.get("--run-sha256") ?? "";
    const operationId = values.get("--operation-id") ?? "";
    const generation = values.get("--generation") ?? "";
    if (!UUID.test(podUid) || !HEX32.test(run) || !HEX64.test(runSha256) || !HEX32.test(operationId) || !HEX32.test(generation)) {
      throw new UsageError("--exit needs --pod-uid <uid> --run <32 hex> --run-sha256 <64 hex> --operation-id <32 hex> --generation <32 hex>");
    }
    const deadlineText = values.get("--deadline-s") ?? String(DEFAULT_DEADLINE_S);
    const deadlineS = Number(deadlineText);
    if (!/^[0-9]{1,3}$/.test(deadlineText) || deadlineS < 1 || deadlineS > 600) throw new UsageError("--deadline-s must be 1-600");
    const binding: MainDrainBindingRequest = { runDir: null, run, runSha256, phase: "resume-drain", operationId, generation, lockFd: null, podUid };
    const now = io.now ?? Date.now;
    const sleep = io.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
    const deadline = now() + deadlineS * 1000;
    // M9: the SAME zero and children checks as the host seam, measured inside the Pod.
    const check = (reply: MainDrainReply) => evaluateDrain({ sessionId, meshRoot: mesh, reply,
      inventory: (() => { try { return inventorySessionPending(mesh, sessionId); } catch { return undefined; } })(),
      completions: scanCompletions(mesh, `session:${sessionId}`) });
    let reply = await callMainDrain(socket, sessionId, "drain", binding);
    let evaluation = check(reply);
    let previous = false;
    while (!(evaluation.drained && previous) && !evaluation.haltedWithHeld && now() + POLL_MS < deadline) {
      previous = evaluation.drained;
      await sleep(POLL_MS);
      // r2 N2: the raw Pod's session actors are stopped only as the last act, exactly as on the host.
      if (reply.native.inputsFenced && evaluation.reasons.length > 0 && evaluation.reasons.every(reason => reason.startsWith("children:live-actor:"))) {
        await callMainDrain(socket, sessionId, "stop-actors", binding);
      }
      reply = await callMainDrain(socket, sessionId, "drain", binding);
      evaluation = check(reply);
    }
    if (!evaluation.drained) {
      // Not drained: the Pod's Main undoes itself here (release, reopen); the seam reports refused.
      const undo = await callMainDrain(socket, sessionId, "undo", binding).catch(() => undefined);
      return emit({ op: "exit", drained: false, exiting: false,
        inputsFenced: !undo || undo.undo?.restartRequired === true ||
          (undo.native.capability && !(undo.native.fenceApi && undo.native.piInputsFenced === false)),
        reasons: evaluation.reasons, observed: evaluation.observed, reply, undo: undo?.undo ?? null }, false);
    }
    const exit = await callMainDrain(socket, sessionId, "exit", binding);
    return emit({ op: "exit", drained: true, exiting: exit.exiting, inputsFenced: evaluation.observed.inputsFenced && exit.native.inputsFenced,
      exitRefused: exit.exitRefused, observed: evaluation.observed, reply: exit }, exit.exiting);
  } catch (error) {
    const usage = error instanceof UsageError;
    const code = error instanceof MainDrainClientError ? error.code : "MAIN_DRAIN_UNAVAILABLE";
    io[usage ? "stderr" : "stdout"].write(`${JSON.stringify({ format: 1, error: usage ? "usage" : code,
      ...(usage ? { message: (error as Error).message, usage: USAGE } : {}) })}\n`);
    return usage ? 2 : 1;
  }
};
