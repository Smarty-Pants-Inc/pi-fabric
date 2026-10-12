// smarty-dev#10560 usage anchor: small public-hook probes of the production activation-window hook in a
// disposable child, the same pattern as the window suite's "guards the final JSON boundary" probes.
// Decision C: these are NOT E2E. The named full-worker E2E plan and its pass condition are in
// tests/worker-activation-fit.test.ts (native createAgentSession + Responses wire; the AgentManager -> worker
// -> pinned Pi CLI process path is excluded there and declared). Each probe drives session_start,
// before_provider_headers, a wrapped provider dispatch with its awaited onPayload, and turn_end.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const GUARD_MS = 120_000;
const W = 8_000; // tokens; 28,000 chars at the host's 3.5 chars/token
const roots: string[] = [];
afterEach(() => { for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });

type Model = { provider: string; id: string; api: string; contextWindow: number };
type Step = { op: "dispatch"; model?: Partial<Model>; payload: Record<string, unknown>; replace?: Record<string, unknown> }
  | { op: "report"; model?: Partial<Model>; usage?: Record<string, number>; stopReason?: string; tools?: boolean };

const M: Model = { provider: "p", id: "m", api: "openai-responses", contextWindow: W };
// 40,000 opaque chars: 11.4k tokens by characters, above the window, but billed by provider usage.
const C = "gAAAAA" + Array.from({ length: 39_994 }, (_, i) => "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[(i * 7919) % 64]).join("");
const base = (extra: Record<string, unknown> = {}, user = "SMALL") => ({ model: "m", input: [
  { role: "user", content: user }, { type: "reasoning", id: "rs_1", summary: [], encrypted_content: C }], ...extra });
const usage = (total: number) => ({ input: total - 100, output: 100, cacheRead: 0, cacheWrite: 0, totalTokens: total });
const report = (total: number, rest: Partial<Extract<Step, { op: "report" }>> = {}): Step => ({ op: "report", usage: usage(total), ...rest });
const x = (chars: number) => "x".repeat(chars);

function run(steps: Step[]) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-usage-"));
  roots.push(dir);
  const sent = path.join(dir, "sent.jsonl");
  const hook = fs.realpathSync(path.resolve("src/worker/activation-window.ts"));
  fs.writeFileSync(path.join(dir, "steps.json"), JSON.stringify({ steps, M }));
  const child = path.join(dir, "probe.mjs");
  fs.writeFileSync(child, `
    import fs from 'node:fs';
    import hook from ${JSON.stringify(pathToFileURL(hook).href)};
    process.env.PI_FABRIC_ACTIVATION_WORKER_PID = String(process.ppid);
    process.env.PI_FABRIC_ACTIVATION_NONCE = 'usage-nonce';
    process.env.PI_FABRIC_PARENT_RUN = 'usage-run';
    process.env.PI_FABRIC_ACTIVATION_HOOK = ${JSON.stringify(hook)};
    const {steps, M} = JSON.parse(fs.readFileSync(${JSON.stringify(path.join(dir, "steps.json"))}, 'utf8'));
    const handlers = new Map();
    await hook({on(name, fn) {handlers.set(name, fn)}});
    async function dispatch(model, context, options) {
      const payload = await options.onPayload(context.payload, model);
      fs.appendFileSync(${JSON.stringify(sent)}, JSON.stringify(payload) + '\\n');
    }
    const provider = {stream: dispatch, streamSimple: dispatch};
    const models = steps.map(s => ({...M, ...s.model}));
    const ctx = {mode: 'rpc', model: M, sessionManager: {getBranch() {return []}},
      modelRegistry: {getAll() {return models}, getProvider() {return provider}}};
    await handlers.get('session_start')({}, ctx);
    for (const step of steps) {
      const model = {...M, ...step.model};
      if (step.op === 'dispatch') {
        await handlers.get('before_provider_headers')({}, ctx);
        // The callback (after the native before_provider_request chain) may replace the payload.
        await provider.streamSimple(model, {messages: [], payload: step.payload},
          {onPayload: async payload => step.replace ?? undefined});
      } else {
        await handlers.get('turn_end')({type: 'turn_end', toolResults: [], message: {role: 'assistant', content: [],
          provider: model.provider, model: model.id, api: model.api, stopReason: step.stopReason ?? 'stop', usage: step.usage, timestamp: 1}});
      }
    }
  `);
  const result = spawnSync(process.execPath, [child], { encoding: "utf8", timeout: GUARD_MS });
  const frames = result.stdout.split("\n").filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return undefined; } });
  return { status: result.status, stderr: result.stderr, refusal: frames.find(f => f?.type === "fabric_activation_window_refusal"),
    sent: fs.existsSync(sent) ? fs.readFileSync(sent, "utf8").split("\n").filter(Boolean) : [] };
}

const admits = (r: ReturnType<typeof run>, n: number) => {
  expect(r.status, r.stderr).toBe(0);
  expect(r.refusal).toBeUndefined();
  expect(r.sent).toHaveLength(n);
};
const refuses = (r: ReturnType<typeof run>, n: number, contextWindow = W) => {
  expect(r.status, r.stderr).toBe(78);
  expect(r.refusal).toMatchObject({ code: "context-overflow", runId: "usage-run", nonce: "usage-nonce", contextWindow });
  expect(r.sent).toHaveLength(n); // nothing oversized dispatched
};

describe("activation window provider-usage anchor (smarty-dev#10560)", () => {
  it("dispatches legitimate unchanged opaque ciphertext byte-exact, through an identity callback too", () => {
    const r = run([{ op: "dispatch", payload: base() }, report(3_000), { op: "dispatch", payload: base(), replace: base() }]);
    admits(r, 2);
    for (const body of r.sent) expect(body).toBe(JSON.stringify(base()));
    expect(JSON.parse(r.sent[1]!).input[1].encrypted_content).toBe(C);
  }, GUARD_MS);

  it("refuses a late callback expansion of an unknown field over the reported usage, admits a fitting one", () => {
    // Without the anchor, the 20k-char field alone (~5.8k tokens) would be admitted under 8k.
    refuses(run([{ op: "dispatch", payload: base() }, report(3_000),
      { op: "dispatch", payload: base(), replace: base({ unknown: x(20_000) }) }]), 1);
    admits(run([{ op: "dispatch", payload: base() }, report(3_000),
      { op: "dispatch", payload: base(), replace: base({ unknown: x(10_000) }) }]), 2);
  }, GUARD_MS);

  it("refuses a same-length opaque-to-visible replacement and ciphertext outside a direct reasoning item", () => {
    const visible = { ...base(), input: [{ role: "user", content: "SMALL" }, { type: "reasoning", id: "rs_1", summary: [{ type: "summary_text", text: C }], encrypted_content: "" }] };
    refuses(run([{ op: "dispatch", payload: base() }, report(3_000), { op: "dispatch", payload: base(), replace: visible }]), 1);
    const misplaced = { ...base(), input: [{ role: "user", content: "SMALL" }, { type: "function_call_output", call_id: "c", output: "ok", encrypted_content: C }] };
    refuses(run([{ op: "dispatch", payload: misplaced }]), 0);
    // A non-qualified API gets no opaque exclusion at all.
    refuses(run([{ op: "dispatch", model: { api: "openai-completions" }, payload: base() }]), 0);
  }, GUARD_MS);

  it("refuses a native old-prefix rewrite before the wrapped dispatch when its growth is too large", () => {
    refuses(run([{ op: "dispatch", payload: base() }, report(3_000), { op: "dispatch", payload: base({}, x(20_000)) }]), 1);
    admits(run([{ op: "dispatch", payload: base() }, report(3_000), { op: "dispatch", payload: base({}, x(10_000)) }]), 2);
  }, GUARD_MS);

  it("never honors another model's usage, and resets on a model change", () => {
    const grow = { op: "dispatch", payload: base({ more: x(3_000) }) } as const;
    refuses(run([{ op: "dispatch", payload: base() }, report(7_500), grow]), 1); // same model: 7,500 + ~860
    for (const model of [{ id: "m2" }, { provider: "q" }, { api: "azure-openai-responses" }]) {
      admits(run([{ op: "dispatch", payload: base() }, report(7_500), { ...grow, model }]), 2);
      // The report must name the dispatched model; a different response model is not trusted.
      admits(run([{ op: "dispatch", payload: base() }, report(7_500, { model }), grow]), 2);
    }
  }, GUARD_MS);

  it("applies a valid report to a same-model window change, refusing a shrink and allowing a larger window", () => {
    refuses(run([{ op: "dispatch", payload: base() }, report(3_000),
      { op: "dispatch", model: { contextWindow: 3_500 }, payload: base({ more: x(2_000) }) }]), 1, 3_500);
    admits(run([{ op: "dispatch", payload: base() }, report(3_000),
      { op: "dispatch", model: { contextWindow: 16_000 }, payload: base({ more: x(20_000) }) }]), 2);
  }, GUARD_MS);

  it("falls back to the wire estimate for invalid, zero, error, aborted or ambiguous reports, without poisoning", () => {
    const grow: Step = { op: "dispatch", payload: base({ more: x(3_000) }) };
    for (const bad of [report(7_500, { stopReason: "error" }), report(7_500, { stopReason: "aborted" }), report(0),
      report(7_500, { usage: { ...usage(7_500), input: -1 } }), report(7_500, { usage: { output: 7_500 } as never }),
      { op: "report" } as Step]) {
      admits(run([{ op: "dispatch", payload: base() }, bad, grow]), 2);
      // An invalid report after a valid one leaves the next dispatch on its own (fallback) estimate.
      admits(run([{ op: "dispatch", payload: base() }, report(7_500), { op: "dispatch", payload: base() }, bad, grow]), 3);
    }
    // Two admissions in one turn (a side request) make the pairing ambiguous: the report is ignored.
    admits(run([{ op: "dispatch", payload: base() }, { op: "dispatch", payload: base() }, report(7_500), grow]), 3);
  }, GUARD_MS);

  it("keeps a no-tool answer's report for a later continuation request", () => {
    // turn_end without tool results (queued/settle continuation) still records the same-model report.
    refuses(run([{ op: "dispatch", payload: base() }, report(7_500, { tools: false }), { op: "dispatch", payload: base({ more: x(3_000) }) }]), 1);
  }, GUARD_MS);
});
