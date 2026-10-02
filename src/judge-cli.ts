import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { JevClient, JevCredentials } from "./jev/client.js";
import { DEFAULT_JEV_CONFIG } from "./jev/config.js";
import { JEV_TYPESAFE_ROUTE, resolveJevModelRoute } from "./jev/routes.js";
import { resolveAgentDir } from "./core/agent-dir.js";
import { MAX_INPUT_BYTES, object, parseJudgeRequest, recommendation, type JudgeEnvelope } from "./judge/contract.js";
import { judge, type JudgePolicy } from "./judge/runtime.js";

interface TrustedConfig { policy: JudgePolicy; ledger?: string; piBinary: string; jevModel?: string; fixtureEndpoint?: string }
function config(file: string | undefined): TrustedConfig {
  if (!file) throw new Error("invalid_config");
  const stat = fs.statSync(file);
  if (!stat.isFile() || stat.size > 8192) throw new Error("invalid_config");
  const v = JSON.parse(fs.readFileSync(file, "utf8")) as TrustedConfig;
  if (!v || typeof v !== "object" || Object.keys(v).some(key => !["policy", "ledger", "piBinary", "jevModel", "fixtureEndpoint"].includes(key))) throw new Error("invalid_config");
  if (typeof v.piBinary !== "string" || !path.isAbsolute(v.piBinary) || v.piBinary.length > 4096) throw new Error("invalid_config");
  if (v.ledger !== undefined && (typeof v.ledger !== "string" || !path.isAbsolute(v.ledger))) throw new Error("invalid_config");
  const policy = object(v.policy, v.policy?.protectionKnownClear === undefined ? ["version", "role", "pin"] : ["version", "role", "pin", "protectionKnownClear"]);
  if (typeof policy.version !== "string" || policy.version.length < 1 || policy.version.length > 128 || policy.role !== "Sol" || policy.protectionKnownClear !== undefined && typeof policy.protectionKnownClear !== "boolean") throw new Error("invalid_config");
  object(policy.pin, ["model", "effort"]);
  if (v.fixtureEndpoint !== undefined) {
    const url = new URL(v.fixtureEndpoint);
    if (url.protocol !== "http:" || !["127.0.0.1", "[::1]"].includes(url.hostname) || url.username || url.password || url.search || url.hash) throw new Error("invalid_config");
  }
  return v;
}
const terminal = (reasonCode: string): JudgeEnvelope => ({ verdict: "unknown", confidence: null, confidenceProvenance: "unavailable", evidenceLinks: [], nextAction: recommendation("unknown", ""), decisionId: "", cost: { usd: null, basis: "unknown", tokens: 0, evaluations: 0, agents: 0 }, status: "unknown", reasonCode });
/** One bounded document; nothing but the terminal envelope is written to stdout. */
export async function main(): Promise<number> {
  const controller = new AbortController();
  const cancel = () => controller.abort(new Error("cancelled"));
  process.on("SIGINT", cancel); process.on("SIGTERM", cancel);
  let client: JevClient | undefined;
  let envelope: JudgeEnvelope;
  try {
    const chunks: Buffer[] = []; let size = 0;
    const inputTimer = setTimeout(() => { controller.abort(new Error("input_timeout")); process.stdin.destroy(new Error("input_timeout")); }, 5000);
    try {
      for await (const chunk of process.stdin) {
        size += Buffer.byteLength(chunk);
        if (size > MAX_INPUT_BYTES) { process.stdin.destroy(); throw new Error("invalid_input"); }
        chunks.push(Buffer.from(chunk));
      }
    } finally { clearTimeout(inputTimer); }
    let request; try { request = parseJudgeRequest(JSON.parse(Buffer.concat(chunks).toString("utf8"))); } catch { throw new Error("invalid_input"); }
    let trusted: TrustedConfig;
    try { trusted = config(process.env.FABRIC_JUDGE_CONFIG); } catch { throw new Error("invalid_config"); }
    const target = resolveJevModelRoute(trusted.jevModel ?? "jev-latest");
    client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: target.model, requestTimeoutMs: 2500, maxRequestBytes: 65536 }, fetch,
      trusted.fixtureEndpoint ? new JevCredentials([], { TYPESAFE_API_KEY: "local-fixture-not-a-secret" }) : undefined,
      trusted.fixtureEndpoint ? { ...JEV_TYPESAFE_ROUTE, endpoint: trusted.fixtureEndpoint } : target.route);
    const workerPath = fileURLToPath(new URL("./worker.js", import.meta.url));
    envelope = await judge(request, { policy: trusted.policy, ledger: trusted.ledger ?? path.join(resolveAgentDir(), "fabric", "model-routing.jsonl"),
      evaluate: (input, signal) => client!.evaluate(input, signal),
      agent: async (input, limits, signal) => {
        const { runJudgmentAgent } = await import("./judge/agent.js");
        return runJudgmentAgent(input, limits, signal, { piBinary: trusted.piBinary, workerPath });
      },
    }, controller.signal);
  } catch (error) {
    const reason = error instanceof Error ? error.message : "";
    envelope = terminal(controller.signal.aborted ? "cancelled" : ["invalid_input", "invalid_policy", "invalid_config"].includes(reason) ? reason : "invalid_config");
  } finally {
    client?.close(); process.off("SIGINT", cancel); process.off("SIGTERM", cancel);
  }
  process.stdout.write(`${JSON.stringify(envelope)}\n`);
  return envelope.status === "completed" ? 0 : 2;
}
