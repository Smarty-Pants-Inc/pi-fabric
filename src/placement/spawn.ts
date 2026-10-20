import { execFileSync } from "node:child_process";
import path from "node:path";
import type { PlacementBinding, PlacementClient, PlacementIntent, PlacementOutcome } from "./client.js";

export const PLACEMENT_DELAY_BUDGET_MS = 120_000;

/** Refusal raised at the trusted spawn boundary; server detail never escapes. */
export class PlacementAdmissionError extends Error {
  readonly code: string;
  constructor(readonly outcome: PlacementOutcome) {
    const code = "code" in outcome ? outcome.code : "LOCAL_PROTOCOL_ERROR";
    super(`Placement ${outcome.status}: ${code}`);
    this.name = "PlacementAdmissionError";
    this.code = code;
  }
}

export interface PlacementIntentInput {
  runId: string;
  kind: "process" | "remote";
  parentSession: string | undefined;
  org: string;
  role: string;
  complexity: PlacementIntent["complexity"];
  model: string | undefined;
  thinking: string | undefined;
  needs: readonly string[];
  projectRoot: string;
  cwd: string;
  timeoutMs: number;
}

const localRefusal = (code: "LOCAL_INTENT_INVALID"): PlacementAdmissionError =>
  new PlacementAdmissionError({ status: "refused", code });

const repositoryUrl = (projectRoot: string): string => {
  const origin = execFileSync("git", ["-C", projectRoot, "config", "--get", "remote.origin.url"], {
    encoding: "utf8", timeout: 2_000, stdio: ["ignore", "pipe", "ignore"],
  }).trim();
  const httpsOrigin = origin.replace(/^git@([^:]+):/i, "https://$1/").replace(/^ssh:\/\/(?:[^@/]+@)?/i, "https://");
  const url = new URL(httpsOrigin);
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) throw new Error();
  const pathname = url.pathname.replace(/\.git$/i, "");
  if (!/^\/[A-Za-z0-9_-][A-Za-z0-9_.-]*\/[A-Za-z0-9_-][A-Za-z0-9_.-]*$/.test(pathname)) throw new Error();
  return `${url.origin}${pathname}`;
};

/** Build the exact canonical intent from manager-owned identity and normalized worker state. */
export const makePlacementIntent = (input: PlacementIntentInput): PlacementIntent => {
  try {
    const root = path.resolve(input.projectRoot);
    const cwd = path.resolve(input.cwd);
    const relative = path.relative(root, cwd);
    if (!input.org || !input.role || !input.parentSession || !input.model || !input.thinking ||
      !["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(input.thinking)
      || input.timeoutMs < 1_000 || !Number.isFinite(input.timeoutMs)
      || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) throw new Error();
    const seconds = Math.ceil(input.timeoutMs / 1000);
    if (!Number.isSafeInteger(seconds) || seconds < 1 || seconds > 604_800) throw new Error();
    return {
      v: 1,
      runId: input.runId,
      kind: input.kind,
      org: input.org,
      parentSession: input.parentSession,
      role: input.role,
      complexity: input.complexity,
      model: input.model,
      thinking: input.thinking as PlacementIntent["thinking"],
      needs: [...new Set(input.needs)].sort(),
      repository: repositoryUrl(root),
      cwd: relative === "" ? "." : relative.split(path.sep).join("/"),
      expectedSeconds: seconds,
      timeoutSeconds: seconds,
      deadline: null,
    };
  } catch {
    throw localRefusal("LOCAL_INTENT_INVALID");
  }
};

const refused = (code: "LOCAL_TIMEOUT"): PlacementOutcome => ({ status: "refused", code });

/** Submit once, then watch/read the same idempotent operation within the 120-second budget. */
export const awaitPlacementAdmission = async (
  client: PlacementClient,
  intent: PlacementIntent,
  signal?: AbortSignal,
  budgetMs = PLACEMENT_DELAY_BUDGET_MS,
): Promise<PlacementOutcome> => {
  const deadline = Date.now() + budgetMs;
  let outcome = await client.requestSpawn(intent);
  while (outcome.status === "delayed") {
    if (signal?.aborted || Date.now() >= deadline) return refused("LOCAL_TIMEOUT");
    let watchEnded = false;
    for await (const event of client.watchSpawn(intent.runId)) {
      if (signal?.aborted || Date.now() >= deadline) return refused("LOCAL_TIMEOUT");
      if (event.status === "admitted") return event;
      if (event.status === "delayed") { outcome = event; continue; }
      if (event.code === "LOCAL_WATCH_ENDED") { watchEnded = true; break; }
      return event;
    }
    if (!watchEnded) {
      // A bounded client timeout or network/auth/protocol error is a refusal, never a retry signal.
      return outcome.status === "delayed" ? refused("LOCAL_TIMEOUT") : outcome;
    }
    if (signal?.aborted || Date.now() >= deadline) return refused("LOCAL_TIMEOUT");
    outcome = await client.getSpawn(intent.runId);
  }
  return outcome;
};

/** Ensure the full binding agrees with the admitted spawn response before any worker is created. */
export const placementBindingMatches = (
  outcome: PlacementOutcome,
  binding: PlacementBinding,
): boolean => {
  if (outcome.status !== "admitted" || binding.status !== "admitted"
    || outcome.host !== binding.host || outcome.claimUID !== binding.claimUID) return false;
  if (outcome.claimRef === null || binding.claimRef === null) return outcome.claimRef === binding.claimRef;
  return outcome.claimRef.namespace === binding.claimRef.namespace
    && outcome.claimRef.name === binding.claimRef.name
    && outcome.claimRef.uid === binding.claimRef.uid;
};
