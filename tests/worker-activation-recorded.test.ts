// smarty-dev#10560 recorded-composition witness: the production activation-window hook driven through its public
// admission hooks (session_start, before_provider_headers, the hook-wrapped provider dispatch with its awaited
// onPayload, and turn_end usage reports) over the PERSISTED sanitized journal of incident session 2aefdb94.
// Every dispatched payload is the native Pi openai-responses payload built from the persisted entries.
//
// Disclosure (what this witness is and is not):
// - The live failure refused at 275,729 tokens. Rebuilt from the persisted journal, the old installed hook
//   (Fabric 8d0af4b0 dist, Pi 1.1.0 227172bf) refuses the final request at 314,053 (the old raw-formula rebuild
//   at e0cdecda measured 308,082). The difference is unrecorded in-memory preflight state; no input was tuned.
//   Driven through this recorded series, the old hook already refuses request->799 at 273,720 (measured).
// - A resimulated old-hook compaction lifecycle diverges at turn_end 636-639 (it drafts a compaction there; the
//   real run compacted only after 647-650). It is NOT used: every compaction here is the real persisted one,
//   including 647-650. The resimulated prefix through 635 matched the journal, as prior fidelity evidence only.
// - The data is structure-only: real lengths, usages, ids and references; all text is filler.
// - Provenance (verifiable on Ryzen 1, where the private source stays): SOURCE.sha256 is the output of the
//   sanitizer (sha256 966d6fecec008d9acc2752af836d6449c9f055d06a4a4c63d465784dfcc74e29) run on the recorded
//   session of actor 39d0eee958694fbdbbc7f21dc5eea5e0 (run 2aefdb94), session.jsonl.20261012T214524629Z.bak,
//   10,839,524 bytes, sha256 992f6b929643ca31dd18ffb1da3104eb48e93c4c42ac962f75717c406b699020. Re-derive with
//   ~/.local/state/smarty/org/fabric-store-replay-2aefdb94/verify.sh (smarty-dev#10560).
//
// Decision C: this is NOT E2E and does not claim the exact live session's failure end to end. Excluded: the
// context hook projection and verifySystem's system witness (dispatch context.messages is [], as in
// tests/worker-activation-usage.test.ts; the persisted compactions were never authorized by this hook
// instance, and only that path accepts a re-stamped checkpoint head), the AgentManager -> worker -> pinned Pi CLI transport (launch binding,
// readiness ACK) and the native compaction chain (turn_end drafting: usage reports carry toolResults: [],
// so the hook records each real report without resimulating a compaction). Planned named E2E:
// tests/worker-activation-window.test.ts "native activation window" > "fits the recorded 2aefdb94 activation"
// (target 2026-10-20, owner fabric-store, smarty-dev#10049)
// with PI_FABRIC_ACTIVATION_TEST_PI_BINARY and AgentManager.run({inferenceContext: "activation",
// transport: "process"}) over this recorded persisted state: pass = the old hook's typed CONTEXT_OVERFLOW
// refusal, the new hook's completion with the final request served byte-exact, and a true visible overflow
// still a typed refusal with no oversized body served (tests/worker-activation-fit.test.ts keeps that control).
//
// FABRIC_RECORDED_TEST_HOOK (read-only, optional) pins another hook file, e.g. the old installed dist, to run
// this suite as an external RED regression. The default is the production src hook. (Not PI_FABRIC_*: the
// Vitest config scrubs PI_FABRIC_* variables outside its test-control allowlist.)
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
// @ts-expect-error untyped JS fixture generator
import { buildReplay } from "./fixtures/activation-recorded-2aefdb94.mjs";

const GUARD_MS = 300_000;
const SOURCE = { entries: 809, bytes: 10_704_519, sha256: "947b252e9eb056520e91ddf4ab545fab1310983cbcb2f51ca7ec8284a35ef8df" };
const WINDOW = 272_000;
const ROOT = 538; // every activation compaction's firstKeptEntryId (model_change); system 544, user 545
const roots: string[] = [];
afterEach(() => { for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });

let replay: string | undefined;
const journal = (): string => {
  replay ??= buildReplay(JSON.parse(fs.readFileSync(path.resolve("tests/fixtures/activation-recorded-2aefdb94.json"), "utf8"))) as string;
  return replay;
};
const unwrap = (e: Record<string, any>) => e.type === "custom" && e.customType === "fabric-activation-context" ? e.data.entry : e;

// The child replays the persisted entries ROOT..hi (first parentId null: the isolated activation session),
// native buildSessionContext -> convertToLlm -> native openai-responses streamSimple (reasoning: the recorded
// thinking level, max_output_tokens: model.maxTokens), whose built payload is captured in onPayload before any
// client call (fetch is a throwing stub). That exact payload is then dispatched through the hook-wrapped
// provider (usage-suite admission pattern: context.messages [] so verifySystem's journal witness is excluded).
const CHILD = `
import fs from 'node:fs';
import { pathToFileURL } from 'node:url';
const [file, outDir, hookPath, mode] = process.argv.slice(1);
const { buildSessionContext, convertToLlm } = await import('@earendil-works/pi-coding-agent');
const responses = await import('@earendil-works/pi-ai/api/openai-responses');
process.env.PI_FABRIC_ACTIVATION_WORKER_PID = String(process.ppid);
process.env.PI_FABRIC_ACTIVATION_NONCE = 'recorded-nonce';
process.env.PI_FABRIC_PARENT_RUN = 'recorded-run';
process.env.PI_FABRIC_ACTIVATION_HOOK = hookPath;
const { default: hook } = await import(pathToFileURL(hookPath).href);
const L = fs.readFileSync(file, 'utf8').split('\\n').filter(Boolean).map(l => JSON.parse(l));
const unwrap = e => e.type === 'custom' && e.customType === 'fabric-activation-context' ? e.data.entry : e;
const span = hi => { const es = L.slice(${ROOT}, hi + 1).map(e => structuredClone(unwrap(e))); es[0].parentId = null; return es; };
const model = { id: 'gpt-6-luna', name: 'gpt-6-luna', api: 'openai-responses', provider: 'cliproxyapi',
  baseUrl: 'http://127.0.0.1:9/v1', reasoning: true, input: ['text'],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: ${WINDOW}, maxTokens: 128000 };
let sent;
async function wire(model, context, options) { sent = JSON.stringify(await options.onPayload(JSON.parse(context.payload), model)); }
const provider = { stream: wire, streamSimple: wire };
const handlers = new Map();
await hook({ on(name, fn) { handlers.set(name, fn); } });
const ctx = { mode: 'rpc', model, sessionManager: { getBranch() { return []; } },
  modelRegistry: { getAll() { return [model]; }, getProvider() { return provider; } } };
const log = r => fs.appendFileSync(outDir + '/log.jsonl', JSON.stringify(r) + '\\n');
await handlers.get('session_start')({ type: 'session_start' }, ctx);
async function dispatch(label, hi) {
  const es = span(hi);
  let thinking;
  for (const e of es) if (e.type === 'thinking_level_change') thinking = e.thinkingLevel;
  const messages = buildSessionContext(es, es.at(-1).id).messages;
  let native;
  sent = undefined;
  const s = responses.streamSimple(model, { messages: convertToLlm(messages) }, { apiKey: 'recorded-dummy', reasoning: thinking,
    maxRetries: 0, fetch: async () => { throw new Error('recorded replay: network forbidden'); },
    onPayload: async p => { native = JSON.stringify(p); throw new Error('recorded replay: payload captured'); } });
  for await (const _ of s) { /* drain the captured-payload error */ }
  if (native === undefined) throw new Error('no native payload for ' + label);
  log({ label: 'payload', of: label, chars: native.length });
  await handlers.get('before_provider_headers')({ type: 'before_provider_headers' }, ctx);
  await provider.streamSimple(model, { messages: [], payload: native }, { onPayload: async () => undefined });
  const rec = { label, hi, chars: native?.length, full: native && Math.ceil(native.length / 3.5), sent: sent !== undefined,
    exact: sent === native, items: native && JSON.parse(native).input.length };
  if (label === 'final') { fs.writeFileSync(outDir + '/native.json', native ?? ''); fs.writeFileSync(outDir + '/sent.json', sent ?? ''); }
  log(rec);
}
if (mode === 'single') await dispatch('final', L.length - 1);
else {
  for (let a = ${ROOT}; a < L.length; a++) {
    const e = unwrap(L[a]);
    if (e.type !== 'message' || e.message.role !== 'assistant') continue;
    await dispatch('request->' + a, a - 1);
    // The recorded response's actual usage and output inventory; toolResults [] records the report only.
    await handlers.get('turn_end')({ type: 'turn_end', toolResults: [], message: e.message });
    log({ label: 'report', a, usage: e.message.usage, stopReason: e.message.stopReason });
  }
  await dispatch('final', L.length - 1);
}
`;

function run(mode: "series" | "single") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recorded-"));
  roots.push(dir);
  const file = path.join(dir, "replay.jsonl");
  fs.writeFileSync(file, journal());
  const hook = fs.realpathSync(process.env.FABRIC_RECORDED_TEST_HOOK || path.resolve("src/worker/activation-window.ts"));
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", CHILD, file, dir, hook, mode],
    { encoding: "utf8", timeout: GUARD_MS, cwd: process.cwd(), maxBuffer: 64 * 1024 * 1024 });
  const frames = result.stdout.split("\n").filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return undefined; } });
  const log = fs.existsSync(path.join(dir, "log.jsonl"))
    ? fs.readFileSync(path.join(dir, "log.jsonl"), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
  const read = (name: string) => fs.existsSync(path.join(dir, name)) ? fs.readFileSync(path.join(dir, name), "utf8") : undefined;
  const out = { status: result.status, stderr: result.stderr, refusal: frames.find(f => f?.type === "fabric_activation_window_refusal"),
    payloads: log.filter(r => r.label === "payload"),
    requests: log.filter(r => r.label !== "report" && r.label !== "payload"), reports: log.filter(r => r.label === "report"), native: read("native.json"), sent: read("sent.json") };
  const evidence = process.env.FS_ACTFIT_RECORDED_EVIDENCE;
  if (evidence) {
    const prior = fs.existsSync(evidence) ? JSON.parse(fs.readFileSync(evidence, "utf8")) : {};
    fs.writeFileSync(evidence, JSON.stringify({ ...prior, [mode]: { status: out.status, refusal: out.refusal, hook,
      stderr: out.stderr.split("\n").filter(Boolean).slice(-3), requests: out.requests, reports: out.reports.slice(-3) } }, null, 1));
  }
  return out;
}

describe("activation window admits the recorded 2aefdb94 composition (smarty-dev#10560)", () => {
  it("rebuilds the persisted v2 journal byte-exact: entries 0..808 with the real compactions 647-650", () => {
    const text = journal();
    expect(Buffer.byteLength(text)).toBe(SOURCE.bytes);
    expect(createHash("sha256").update(text).digest("hex")).toBe(SOURCE.sha256);
    const L = text.split("\n").filter(Boolean).map(line => JSON.parse(line));
    expect(L).toHaveLength(SOURCE.entries);
    const root = unwrap(L[ROOT]);
    expect(root.type).toBe("model_change");
    expect(unwrap(L[544]).message.role).toBe("system");
    expect(unwrap(L[545]).message.role).toBe("user");
    // The real compaction drafted after turn 647..650 (31 persisted entries at 651..681).
    const real = L.slice(651, 682).map(unwrap);
    expect(real.map(e => e.type)).toEqual(["compaction", ...Array(30).fill("context_edit")]);
    expect(unwrap(L[647]).message.role).toBe("assistant");
    expect(unwrap(L[682]).message.role).toBe("assistant");
    for (const e of L.slice(ROOT).map(unwrap)) if (e.type === "compaction") expect(e.firstKeptEntryId).toBe(root.id);
    const last = unwrap(L[807]).message;
    expect([last.role, last.provider, last.model, last.api, last.usage.totalTokens]).toEqual(["assistant", "cliproxyapi", "gpt-6-luna", "openai-responses", 120_700]);
    expect(unwrap(L[808]).message.role).toBe("toolResult");
  }, GUARD_MS);

  it("admits every recorded request and dispatches the final request byte-exact, anchored by the recorded 807 usage", () => {
    const r = run("series");
    expect(r.status, r.stderr).toBe(0);
    expect(r.refusal).toBeUndefined();
    const final = r.requests.at(-1);
    expect(final.label).toBe("final");
    expect(r.requests.every(q => q.sent && q.exact), JSON.stringify(r.requests.filter(q => !q.exact))).toBe(true);
    expect(r.reports.at(-1)).toMatchObject({ a: 807, usage: { totalTokens: 120_700 } });
    // Measured native composition (Pi 1.1): the final payload is 1,099,184 chars, 314,053 tokens by characters,
    // above the window: only the provider-usage anchor (recorded 807 usage 120,700) admits it.
    expect([final.chars, final.full, final.items]).toEqual([1_099_184, 314_053, 345]);
    expect(final.full).toBeGreaterThan(WINDOW);
    // The real run's last three requests were answered although their full estimates exceed the window.
    expect(r.requests.filter(q => [798, 802, 806].includes(q.hi)).map(q => q.full)).toEqual([273_720, 279_850, 281_472]);
    expect(r.requests).toHaveLength(51); // 50 recorded requests (548..807) + the final request after 808
    expect(r.sent).toBe(r.native);
    expect(JSON.parse(r.sent!).input.filter((i: { type?: string }) => i.type === "reasoning").length).toBeGreaterThan(0);
  }, GUARD_MS);

  it("still refuses the same final request unanchored, by its full encoded estimate, with nothing dispatched", () => {
    const r = run("single");
    expect(r.status, r.stderr).toBe(78);
    expect(r.refusal).toMatchObject({ code: "context-overflow", runId: "recorded-run", nonce: "recorded-nonce", contextWindow: WINDOW });
    expect(r.sent ?? "").toBe("");
    expect(r.requests).toHaveLength(0); // the guard exits before the dispatch record
    const [payload] = r.payloads;
    expect(r.payloads).toHaveLength(1);
    expect(r.refusal.tokens).toBe(Math.ceil(payload.chars / 3.5));
    expect([payload.chars, r.refusal.tokens]).toEqual([1_099_184, 314_053]);
  }, GUARD_MS);
});
