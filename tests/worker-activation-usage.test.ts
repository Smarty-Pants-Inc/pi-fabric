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
// Calibrated for both hosts: Pi 1.1 (3.5 chars/token) and Pi 0.87 (4 chars/token).
const W = 8_000; // tokens; 28,000 chars at 3.5, 32,000 at 4
const roots: string[] = [];
afterEach(() => { for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });

type Model = { provider: string; id: string; api: string; contextWindow: number };
// mutate: the awaited callback edits payload.input[at] in place (creating it when absent) and returns undefined.
type Step = { op: "dispatch"; model?: Partial<Model>; payload: Record<string, unknown>; replace?: Record<string, unknown>;
  mutate?: { at: number; set: Record<string, unknown> } }
  | { op: "report"; model?: Partial<Model>; usage?: Record<string, number>; stopReason?: string; tools?: boolean; content?: unknown[] };

const M: Model = { provider: "p", id: "m", api: "openai-responses", contextWindow: W };
// 40,000 opaque chars: 11.4k (3.5) / 10k (4) tokens by characters, above the window, but billed by provider usage.
const C = "gAAAAA" + Array.from({ length: 39_994 }, (_, i) => "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[(i * 7919) % 64]).join("");
const C2 = "gAAAAB" + C.slice(6); // another 40,000-char ciphertext
const R = { type: "reasoning", id: "rs_1", summary: [], encrypted_content: C };
const R2 = { type: "reasoning", id: "rs_2", summary: [], encrypted_content: C2 };
const U = { role: "user", content: "SMALL" };
const base = (extra: Record<string, unknown> = {}, user = "SMALL") => ({ model: "m", input: [
  { role: "user", content: user }, R], ...extra });
const usage = (total: number) => ({ input: total - 100, output: 100, cacheRead: 0, cacheWrite: 0, totalTokens: total });
const report = (total: number, rest: Partial<Extract<Step, { op: "report" }>> = {}): Step => ({ op: "report", usage: usage(total), ...rest });
const x = (chars: number) => "x".repeat(chars);
// A native Responses thinking block: thinkingSignature is the JSON reasoning output item (shared adapter).
const sig = (item: unknown) => ({ type: "thinking", thinking: "", thinkingSignature: typeof item === "string" ? item : JSON.stringify(item) });
// A first ordinary request carries no opaque reasoning; its valid same-model report (whose response emitted R,
// already counted in its usage) anchors the next request that replays R. seed dispatches 2 (small, then base).
const small = (extra: Record<string, unknown> = {}) => ({ model: "m", input: [{ role: "user", content: "SMALL" }], ...extra });
const seed = (): Step[] => [{ op: "dispatch", payload: small() }, report(3_000, { content: [sig(R)] }), { op: "dispatch", payload: base() }];

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
        await provider.streamSimple(model, {messages: [], payload: step.payload}, {onPayload: async payload => {
          if (step.mutate) Object.assign(payload.input[step.mutate.at] ??= {}, step.mutate.set);
          return step.replace ?? undefined;
        }});
      } else {
        await handlers.get('turn_end')({type: 'turn_end', toolResults: [], message: {role: 'assistant', content: step.content ?? [],
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
  it("dispatches legitimate unchanged opaque ciphertext byte-exact after a valid report, through an identity callback too", () => {
    // Ordinary first request (no opaque) fits; its report anchors the opaque replay; one more valid
    // no-tool report anchors the unchanged replay through an identity callback.
    const r = run([...seed(), report(3_000), { op: "dispatch", payload: base(), replace: base() }]);
    admits(r, 3);
    expect(r.sent[0]).toBe(JSON.stringify(small()));
    for (const body of r.sent.slice(1)) expect(body).toBe(JSON.stringify(base()));
    expect(JSON.parse(r.sent[2]!).input[1].encrypted_content).toBe(C);
  }, GUARD_MS);

  it("refuses unanchored opaque ciphertext by its full encoded estimate (no report, or the audited ambiguous sequence)", () => {
    refuses(run([{ op: "dispatch", payload: base() }]), 0);
    // Audit R1 sequence: two same-model opaque admissions, an (ambiguous) report, then growth.
    refuses(run([{ op: "dispatch", payload: base() }, { op: "dispatch", payload: base() }, report(7_500),
      { op: "dispatch", payload: base({ more: x(3_000) }) }]), 0);
  }, GUARD_MS);

  it("refuses a late callback expansion of an unknown field over the reported usage, admits a fitting one", () => {
    // Without the anchor's growth accounting, the 24k-char field alone (6k-6.9k tokens) would fit under 8k.
    refuses(run([...seed(), report(3_000), { op: "dispatch", payload: base(), replace: base({ unknown: x(24_000) }) }]), 2);
    admits(run([...seed(), report(3_000), { op: "dispatch", payload: base(), replace: base({ unknown: x(10_000) }) }]), 3);
  }, GUARD_MS);

  it("refuses a same-length opaque-to-visible replacement and ciphertext outside a direct reasoning item", () => {
    const visible = { ...base(), input: [{ role: "user", content: "SMALL" }, { type: "reasoning", id: "rs_1", summary: [{ type: "summary_text", text: C }], encrypted_content: "" }] };
    refuses(run([...seed(), report(3_000), { op: "dispatch", payload: base(), replace: visible }]), 2);
    const misplaced = { ...base(), input: [{ role: "user", content: "SMALL" }, { type: "function_call_output", call_id: "c", output: "ok", encrypted_content: C }] };
    refuses(run([{ op: "dispatch", payload: misplaced }]), 0);
    refuses(run([{ op: "dispatch", payload: small() }, report(3_000), { op: "dispatch", payload: misplaced }]), 1);
    // A non-qualified API gets no opaque exclusion at all, even when anchored.
    refuses(run([{ op: "dispatch", model: { api: "openai-completions" }, payload: base() }]), 0);
    refuses(run([{ op: "dispatch", model: { api: "openai-completions" }, payload: small() }, report(3_000, { model: { api: "openai-completions" } }),
      { op: "dispatch", model: { api: "openai-completions" }, payload: base() }]), 1);
  }, GUARD_MS);

  it("refuses a native old-prefix rewrite before the wrapped dispatch when its growth is too large", () => {
    refuses(run([...seed(), report(3_000), { op: "dispatch", payload: base({}, x(24_000)) }]), 2);
    admits(run([...seed(), report(3_000), { op: "dispatch", payload: base({}, x(10_000)) }]), 3);
  }, GUARD_MS);

  it("never honors another model's usage: a model change or foreign report falls back to the full encoded estimate", () => {
    const grow = { op: "dispatch", payload: base({ more: x(3_000) }) } as const;
    const visible = { op: "dispatch", payload: small({ more: x(3_000) }) } as const;
    refuses(run([...seed(), report(7_500), grow]), 2); // same model: 7,500 + 750-860 growth
    for (const model of [{ id: "m2" }, { provider: "q" }, { api: "azure-openai-responses" }]) {
      // Safe conservative fallback: the other model's dispatch counts its opaque ciphertext and refuses.
      refuses(run([...seed(), report(3_000), { ...grow, model }]), 2);
      admits(run([...seed(), report(7_500), { ...visible, model }]), 3); // visible small request still fits
      // The report must name the dispatched model; a different response model is not trusted.
      refuses(run([...seed(), report(3_000, { model }), grow]), 2);
      admits(run([...seed(), report(7_500, { model }), visible]), 3);
    }
  }, GUARD_MS);

  it("applies a valid report to a same-model window change, refusing a shrink and allowing a larger window", () => {
    refuses(run([...seed(), report(3_000), { op: "dispatch", model: { contextWindow: 3_500 }, payload: base({ more: x(4_000) }) }]), 2, 3_500);
    admits(run([...seed(), report(3_000), { op: "dispatch", model: { contextWindow: 16_000 }, payload: base({ more: x(24_000) }) }]), 3);
  }, GUARD_MS);

  it("refuses opaque history after invalid, zero, error, aborted or ambiguous reports; visible small requests still fit", () => {
    const grow: Step = { op: "dispatch", payload: base({ more: x(3_000) }) };
    const visible: Step = { op: "dispatch", payload: small({ more: x(3_000) }) };
    for (const bad of [report(3_000, { stopReason: "error" }), report(3_000, { stopReason: "aborted" }), report(0),
      report(3_000, { usage: { ...usage(3_000), input: -1 } }), report(3_000, { usage: { output: 3_000 } as never }),
      { op: "report" } as Step]) {
      refuses(run([...seed(), bad, grow]), 2);
      admits(run([...seed(), bad, visible]), 3);
      // An invalid report after a valid one is not rescued by the earlier anchor.
      refuses(run([...seed(), report(3_000), { op: "dispatch", payload: base() }, bad, grow]), 3);
      admits(run([...seed(), report(3_000), { op: "dispatch", payload: base() }, bad, visible]), 4);
    }
    // Two admissions in one turn (a side request) make the pairing ambiguous: the report is ignored.
    refuses(run([{ op: "dispatch", payload: small() }, { op: "dispatch", payload: small() }, report(3_000), grow]), 2);
    admits(run([{ op: "dispatch", payload: small() }, { op: "dispatch", payload: small() }, report(3_000), visible]), 3);
  }, GUARD_MS);

  it("R3: charges in full opaque reasoning the trusted report does not cover", () => {
    // Audit R3 counterexample: valid 3,000 report with an explicitly empty output inventory, then the awaited
    // callback replaces the request with 40,000 new ciphertext chars (full estimate ~11.4k > 8k).
    const r = run([{ op: "dispatch", payload: small() }, report(3_000, { content: [] }), { op: "dispatch", payload: small(), replace: base() }]);
    refuses(r, 1);
    expect(r.refusal.tokens).toBeGreaterThan(W);
    // An earlier transform (provider-built payload) adding uncovered ciphertext.
    refuses(run([{ op: "dispatch", payload: small() }, report(3_000, { content: [] }), { op: "dispatch", payload: base() }]), 1);
    const after = [...seed(), report(3_000, { content: [] })];
    for (const step of [
      { replace: { model: "m", input: [U, { ...R, encrypted_content: C2 }] } }, // replaced ciphertext, same id
      { mutate: { at: 1, set: { encrypted_content: C2 } } }, // in-place ciphertext mutation
      { mutate: { at: 1, set: { id: "rs_9" } } }, // re-identified covered ciphertext
      { replace: { model: "m", input: [U, R, R] } }, // duplicated covered item (one covered occurrence)
      { mutate: { at: 2, set: R } }, // in-place appended copy
      // The same changes made before the callback (earlier transform / native payload), identity callback.
      { payload: { model: "m", input: [U, { ...R, id: "rs_9" }] } },
      { payload: { model: "m", input: [U, R, R] } },
      { payload: { model: "m", input: [U, { ...R, encrypted_content: C2 }] } },
    ]) refuses(run([...after, { op: "dispatch", payload: base(), ...step }]), 2);
    // Two output parts naming the same native item cover one occurrence, not two replayed copies.
    refuses(run([{ op: "dispatch", payload: small() }, report(3_000, { content: [sig(R), sig(R)] }),
      { op: "dispatch", payload: { model: "m", input: [U, R, R] } }]), 1);
    // Malformed or unqualified signatures, or a non-thinking block, create no coverage.
    for (const block of [sig("not json"), sig("null"), sig({ type: "reasoning", id: "rs_1" }), sig({ ...R, type: "message" }),
      { type: "text", text: "", thinkingSignature: JSON.stringify(R) }]) {
      refuses(run([{ op: "dispatch", payload: small() }, report(3_000, { content: [block] }), { op: "dispatch", payload: base() }]), 1);
    }
    // An untrusted report's output inventory is ignored.
    for (const bad of [report(3_000, { stopReason: "error", content: [sig(R)] }), report(0, { content: [sig(R)] }),
      report(3_000, { model: { id: "m2" }, content: [sig(R)] })]) {
      refuses(run([{ op: "dispatch", payload: small() }, bad, { op: "dispatch", payload: base() }]), 1);
    }
  }, GUARD_MS);

  it("R4: exempts opaque reasoning only under a string id; missing, null or non-string ids count in full", () => {
    // Decision C: a hook probe, NOT E2E. Planned pinned-CLI full-worker test: tests/worker-activation-window.test.ts
    // "native activation window" > "R4 counts non-string-id opaque reasoning in full" with
    // PI_FABRIC_ACTIVATION_TEST_PI_BINARY and AgentManager.run({inferenceContext: "activation", transport: "process"})
    // against a responses server whose turn-1 reasoning item has a missing/null/number/object/boolean id and
    // 40,000 ciphertext chars (8k window): pass = CONTEXT_OVERFLOW with a refusal frame, no oversized turn-2
    // body served; the string-id control completes with its replay served byte-exact (as in this suite).
    const as = (id: unknown) => { const { id: _, ...rest } = R; return id === undefined ? rest : { ...rest, id }; };
    const wide = { contextWindow: 16_000 }; // same model key: admits the unanchored ~10-11.4k full estimate
    for (const id of [undefined, null, 7, { id: "rs_1" }, true]) {
      const item = as(id);
      // Report output: trustworthy same-model report whose response inventory names this item.
      const out = run([{ op: "dispatch", payload: small() }, report(3_000, { content: [sig(item)] }),
        { op: "dispatch", payload: { model: "m", input: [U, item] } }]);
      refuses(out, 1);
      expect(out.refusal.tokens).toBeGreaterThan(W);
      // Direct wire: the earlier admitted input inventory, then a trustworthy report with no output reasoning.
      refuses(run([{ op: "dispatch", model: wide, payload: { model: "m", input: [U, item] } }, report(3_000, { content: [] }),
        { op: "dispatch", payload: { model: "m", input: [U, item] } }]), 1);
    }
    // Exact alias: response/wire item with a missing id, then the awaited callback sets id null.
    refuses(run([{ op: "dispatch", payload: small() }, report(3_000, { content: [sig(as(undefined))] }),
      { op: "dispatch", payload: { model: "m", input: [U, as(undefined)] }, mutate: { at: 1, set: { id: null } } }]), 1);
    refuses(run([{ op: "dispatch", model: wide, payload: { model: "m", input: [U, as(undefined)] } }, report(3_000, { content: [] }),
      { op: "dispatch", payload: { model: "m", input: [U, as(undefined)] }, mutate: { at: 1, set: { id: null } } }]), 1);
    // String-id direct-wire control through the same window change.
    admits(run([{ op: "dispatch", model: wide, payload: base() }, report(3_000, { content: [] }), { op: "dispatch", payload: base() }]), 2);
  }, GUARD_MS);

  it("admits fresh provider reasoning and its byte-exact replay on all three Responses APIs", () => {
    for (const api of ["openai-responses", "azure-openai-responses", "openai-codex-responses"]) {
      const r = run([{ op: "dispatch", model: { api }, payload: small() }, report(3_000, { model: { api }, content: [sig(R)] }),
        { op: "dispatch", model: { api }, payload: base() }]);
      admits(r, 2);
      expect(r.sent[1]).toBe(JSON.stringify(base()));
    }
    // Multi-turn history: R covered by the dispatched input, R2 by the next response's output; identity callback.
    const both = { model: "m", input: [U, R, U, R2] };
    const r = run([...seed(), report(3_000, { content: [sig(R2)] }), { op: "dispatch", payload: both },
      report(3_000, { content: [] }), { op: "dispatch", payload: both, replace: both }]);
    admits(r, 4);
    for (const body of r.sent.slice(2)) expect(body).toBe(JSON.stringify(both));
  }, GUARD_MS);

  it("keeps a no-tool answer's report for a later continuation request", () => {
    // turn_end without tool results (queued/settle continuation) still records the same-model report.
    refuses(run([...seed(), report(7_500, { tools: false }), { op: "dispatch", payload: base({ more: x(3_000) }) }]), 2);
    admits(run([...seed(), report(3_000, { tools: false }), { op: "dispatch", payload: base() }]), 3);
  }, GUARD_MS);
});
