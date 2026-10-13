// smarty-dev#10560 MEASURE BEFORE FIX. A real activation-window extension registered into native
// createAgentSession, a native SessionManager, the native read tool and the native openai-responses
// (or openai-completions) provider against an offline loopback HTTP/SSE server. Every native serialized
// provider payload is measured in a read-only before_provider_request observer registered after the
// hook (the hook admits after the complete chain), including a payload the hook then refuses; the
// server cross-checks the dispatched wire body. Sizes are a transparent synthetic model of the real
// run's composition (see the scenario builders), not the literal incident transcript.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const WINDOW = 272_000;
const RESERVE = 16_384; // documented Pi default; the extension context exposes no effective reserve.
const ADMIT = WINDOW - RESERVE;
const GUARD_MS = 240_000;

// Provider usage reported by one response.completed (Responses API semantics: input includes cached).
type Usage = { input: number; cacheRead: number; output: number };
// reasoning: per item thinkingSignature (JSON.stringify(reasoning item)) lengths; summary: per item text.
type Turn = { decision: number; results: number[]; reasoning?: number[]; summary?: number[]; usage?: Usage };
type Scenario = { api: "openai-completions" | "openai-responses"; turns: Turn[]; final?: Turn; systemPad: number;
  systemTarget?: number; custom?: number; user?: number; argsChars?: number };
type Payload = { req: number; tokens: number; tool: number; assistant: number; user: number; system: number; schema: number;
  reasoning: number; toolCall: number; other: number; encryptedChars: number; contentEstimate: number; usagePrev?: number;
  detailsOnWire: boolean; toolMessages: number; excerpts: number; fullLatest: boolean; compactions: number;
  compactionTokensBefore?: number; floor: number; allExcerpt: number; dispatched?: number };
type Llm = { tokens: number; assistant: number; toolResult: number; user: number; other: number; excerpts: number };
type Boundary = { turn: number; prevReq: number; hookNextEstimate: number; resultChars: number[]; before: Llm;
  drafts?: number; edits?: number; includeLatest?: boolean; after?: Llm };
type Totals = { reasoningBlocks: number; signatureChars: number; encryptedChars: number; thinkingChars: number;
  toolResults: number; toolTextParts: number; toolTextChars: number; detailsChars: number; toolCallArgsChars: number;
  customChars: number; userChars: number; usage: number[]; systemChars: number; schemaChars: number };
type Evidence = { payloads: Payload[]; boundaries: Boundary[]; errors?: string[]; totals?: Totals; served: number;
  refusal?: { type: string; code: string; tokens: number; contextWindow: number }; status: number | null; stderr: string };

// ---- Real-run composition (lead real run; provider usage and session metadata measured read-only) ----
const REAL = { usageFinal: { input: 1_312 + 91_648, cacheRead: 91_648, output: 4_058 }, usageFinalTotal: 97_018, usagePeak: 143_324,
  reasoningItems: 112, signatureChars: 755_244, thinkingChars: 8_368, toolTextChars: 604_856, toolCallArgsChars: 21_064,
  custom: 20_082, user: 2_166, mainCharPreflight: 274_359, systemSections: 82_540 };
// The real actor rendered an 82,540-char system.sections. Pi's native rendered system prompt differs by host
// version, so an unprompted native probe session sizes the DISCLOSED pad from its actual systemPrompt.
// The activation's rendered system is exactly the recorded size (asserted).
const LARGE_FILE = 60_000; // native read truncates to ~51.2k delivered chars with its 50KB notice (parent probe: 51,273)
const LARGE_AT = new Set([3, 11, 19, 27, 35, 43, 51, 62]); // the final batch carries one large read
const LARGE_DELIVERED = 51_189; // measured delivered size of a LARGE_FILE read (asserted against the run)
const split = (total: number, n: number) => Array.from({ length: n }, (_, i) => Math.floor(total / n) + (i < total % n ? 1 : 0));

// 49 tool turns (turns 1-14 read two files: 63 results), 112 reasoning items (3 on turns 1-14, 2 after),
// 755,244 aggregate parseable thinkingSignature chars, 8,368 summary chars. Usage rises linearly to the
// real peak 143,324 at response 40, then falls to the real final 97,018 (the real run compacted natively);
// it never approaches the window. System: the real rendered size (disclosed above), not invented overhead.
const realRun = (): Scenario => {
  const counts = Array.from({ length: 49 }, (_, i) => (i < 14 ? 3 : 2));
  const sigs = split(REAL.signatureChars, REAL.reasoningItems), sums = split(REAL.thinkingChars, REAL.reasoningItems);
  const small = split(REAL.toolTextChars - LARGE_AT.size * LARGE_DELIVERED, 63 - LARGE_AT.size);
  let n = 0, k = 0, r = 0;
  const usageAt = (resp: number): Usage => {
    if (resp === 50) return REAL.usageFinal;
    const total = resp <= 40 ? Math.round(12_000 + (REAL.usagePeak - 12_000) * (resp - 1) / 39)
      : Math.round(REAL.usagePeak - (REAL.usagePeak - REAL.usageFinalTotal) * (resp - 40) / 10);
    const output = 2_000;
    return { input: total - output, cacheRead: Math.floor((total - output) * 0.95), output };
  };
  const turns = counts.map((c, i) => ({ decision: 0,
    results: Array.from({ length: i < 14 ? 2 : 1 }, () => (LARGE_AT.has(n++) ? LARGE_FILE : small[k++]!)),
    reasoning: sigs.slice(r, r + c), summary: sums.slice(r, (r += c)), usage: usageAt(i + 1) }));
  return { api: "openai-responses", turns, final: { decision: 15, results: [], usage: usageAt(50) },
    systemPad: 0, systemTarget: REAL.systemSections,
    custom: REAL.custom, user: REAL.user, argsChars: REAL.toolCallArgsChars };
};

// True provider-usage overflow: honest usage (input = dispatched body chars / 3.5) climbs near 272k on
// irreducible visible assistant text (50k chars per turn, tiny reads) until the next turn's content pushes
// the request over the window. Must refuse typed (exit 78) and never dispatch the oversized request.
const trueOverflow = (): Scenario => ({ api: "openai-responses", systemPad: 0, user: 200, turns: Array.from({ length: 24 },
  () => ({ decision: 50_000, results: [300], reasoning: [600], summary: [60] })) });

// Char-fallback counterexample without provider usage (SSE usage 0): 49 tool turns, 63 results (40 of
// 4,000 chars, 23 of 300). Assistant text grows linearly (turn i writes 760*i chars) with a DISCLOSED
// 40k-char appended system prompt: assistant history alone outgrows the window, so it must refuse (exit 78).
const incident = (): Scenario => {
  let big = 0;
  return { api: "openai-completions", systemPad: 40_000, turns: Array.from({ length: 49 }, (_, i) => ({
    decision: 760 * (i + 1),
    results: Array.from({ length: i < 14 ? 2 : 1 }, () => (big++ < 40 ? 4_000 : 300)),
  })) };
};
// Counterexample: small decisions, 30k-char reads; history must compact while the latest stays full.
const fitting = (): Scenario => ({ api: "openai-completions", systemPad: 0,
  turns: Array.from({ length: 30 }, () => ({ decision: 200, results: [30_000] })) });

const roots: string[] = [];
afterEach(() => { for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

function drive({ api, turns, final, systemPad, systemTarget = 0, custom = 0, user = 0, argsChars }: Scenario): Evidence {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actfit-"));
  roots.push(dir);
  fs.symlinkSync(path.resolve("node_modules"), path.join(dir, "node_modules"), "junction");
  const hook = fs.realpathSync(path.resolve("src/worker/activation-window.ts"));
  const evidence = path.join(dir, "evidence.json");
  const results = turns.reduce((a, t) => a + t.results.length, 0);
  // Long read paths model the real tool-call argument volume: JSON.stringify({path}) = 11 + path length.
  let files = dir;
  if (argsChars) {
    const pathLen = Math.round(argsChars / results) - 11 - "/r-10-0.txt".length - dir.length - 2;
    const a = Math.min(200, pathLen - 1);
    files = path.join(dir, "a".repeat(a), "b".repeat(Math.max(1, pathLen - a - 1)));
  }
  fs.mkdirSync(files, { recursive: true });
  fs.writeFileSync(path.join(dir, "scenario.json"), JSON.stringify({ turns, final, files }));
  for (const [i, turn] of turns.entries()) for (const [j, size] of turn.results.entries()) {
    const line = "y".repeat(99) + "\n";
    const head = `RESULT_${i + 1}_${j}`.padEnd(15) + "\n"; // fixed width: constant read truncation size
    fs.writeFileSync(path.join(files, `r-${i + 1}-${j}.txt`), (head + line.repeat(Math.ceil(size / 100))).slice(0, size));
  }
  const prompt = ("EXACT_ACTIVATION_DIRECTIONS " + "u".repeat(user)).slice(0, Math.max(user, 27));
  const child = path.join(dir, "drive.mjs");
  fs.writeFileSync(child, `
    import fs from 'node:fs';
    import http from 'node:http';
    import { estimateTextTokens } from '@earendil-works/pi-ai/utils/estimate';
    import { convertToLlm, createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from '@earendil-works/pi-coding-agent';
    import { registerHooks } from 'node:module';
    // Node type stripping does not remap the source tree's .js specifiers to .ts.
    registerHooks({resolve(specifier, context, next) {
      try { return next(specifier, context); } catch (error) {
        if (!specifier.endsWith('.js') || !context.parentURL?.endsWith('.ts')) throw error;
        return next(specifier.slice(0, -3) + '.ts', context);
      }
    }});
    const {default: hook} = await import(${JSON.stringify(pathToFileURL(hook).href)});
    process.env.PI_FABRIC_ACTIVATION_WORKER_PID = String(process.ppid);
    process.env.PI_FABRIC_ACTIVATION_NONCE = 'fit-nonce';
    process.env.PI_FABRIC_PARENT_RUN = 'fit-run';
    process.env.PI_FABRIC_ACTIVATION_HOOK = ${JSON.stringify(hook)};
    const API = ${JSON.stringify(api)};
    const root = ${JSON.stringify(dir)};
    const {turns, final, files} = JSON.parse(fs.readFileSync(root + '/scenario.json', 'utf8'));
    const out = {payloads: [], boundaries: [], served: 0};
    const flush = () => fs.writeFileSync(${JSON.stringify(evidence)}, JSON.stringify(out));
    let session, lastWire = 0, lastUsage;
    const est = value => value === undefined ? 0 : estimateTextTokens(JSON.stringify(value));
    const COMPACTED = 'Compacted tool output';
    const text = c => typeof c === 'string' ? c : (c ?? []).map(p => p.text ?? '').join('\\n');
    // Wire items, normalized over both APIs.
    const items = p => p.messages ?? p.input;
    const kind = m => m.role === 'tool' || m.type === 'function_call_output' ? 'tool'
      : m.type === 'reasoning' ? 'reasoning' : m.type === 'function_call' ? 'toolCall'
      : m.role === 'system' || m.role === 'developer' ? 'system' : m.role === 'user' ? 'user' : 'assistant';
    const body = m => m.type === 'function_call_output' ? m.output : text(m.content);
    const put = (m, t) => m.type === 'function_call_output' ? {...m, output: t}
      : {...m, content: typeof m.content === 'string' ? t : [{type: 'text', text: t}]};
    const blankEnc = p => JSON.parse(JSON.stringify(p, (k, v) => k === 'encrypted_content' ? '' : v));
    // Native serialized final payload, measured after the complete before_provider_request chain
    // (this observer is registered last) and immediately before the hook's onPayload admission.
    const measure = payload => {
      const msgs = items(payload);
      const by = k => msgs.filter(m => kind(m) === k);
      const tokens = estimateTextTokens(JSON.stringify(payload));
      const part = {tool: est(by('tool')), assistant: est(by('assistant')), user: est(by('user')), system: est(by('system')),
        schema: est(payload.tools), reasoning: est(by('reasoning')), toolCall: est(by('toolCall'))};
      const tools = by('tool');
      const turn = API === 'openai-responses' ? by('toolCall').filter(m => m.call_id.endsWith('-0')).length : by('assistant').length;
      const latest = turns[turn - 1];
      const id = (JSON.stringify(msgs).match(/audit journal entry ([0-9a-z-]+)\\]/) ?? [])[1] ?? 'xxxxxxxx';
      const marker = '[Compacted tool output; full result in audit journal entry ' + id + ']';
      // Floor: every tool output (latest included) reduced to its bare marker; everything else intact.
      const floorMsgs = msgs.map(m => kind(m) === 'tool' ? put(m, marker) : m);
      // Main's deepest reduction: every tool output as a 1,024-char excerpt plus marker.
      const allExcerptMsgs = msgs.map(m => kind(m) !== 'tool' || body(m).includes(COMPACTED) ? m
        : put(m, body(m).slice(0, 1024) + '\\n' + marker));
      const wrap = list => payload.messages ? {...payload, messages: list} : {...payload, input: list};
      const compactions = session.sessionManager.getBranch().filter(e => e.type === 'compaction');
      const json = JSON.stringify(payload);
      const row = {req: out.payloads.length + 1, tokens, ...part,
        other: tokens - Object.values(part).reduce((a, b) => a + b, 0),
        encryptedChars: by('reasoning').reduce((a, m) => a + (m.encrypted_content?.length ?? 0), 0),
        contentEstimate: estimateTextTokens(JSON.stringify(blankEnc(payload))), usagePrev: lastUsage,
        detailsOnWire: json.includes('"truncation"') || json.includes('"details"'),
        toolMessages: tools.length, excerpts: tools.filter(m => body(m).includes(COMPACTED)).length,
        fullLatest: !latest || tools.slice(-latest.results.length).every(m => !body(m).includes(COMPACTED)),
        compactions: compactions.length, compactionTokensBefore: compactions.at(-1)?.tokensBefore,
        floor: estimateTextTokens(JSON.stringify(wrap(floorMsgs))), allExcerpt: estimateTextTokens(JSON.stringify(wrap(allExcerptMsgs)))};
      out.payloads.push(row);
      lastWire = tokens;
      flush();
    };
    // Opaque encrypted reasoning: deterministic base64-alphabet text sized so JSON.stringify(item)
    // (the native thinkingSignature) has the requested length.
    const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    let seed = 7;
    const opaque = n => { let s = 'gAAAAA'; while (s.length < n) { seed = (seed * 1103515245 + 12345) % 2147483648; s += B64[seed % 64]; } return s.slice(0, n); };
    const reasoningItem = (n, k, sig, sum) => {
      const item = {id: 'rs_' + n + '_' + k, type: 'reasoning', summary: [{type: 'summary_text', text: ('SUMMARY_' + n + '_' + k + ' ' + 't'.repeat(sum)).slice(0, sum)}], encrypted_content: ''};
      item.encrypted_content = opaque(sig - JSON.stringify(item).length);
      if (JSON.stringify(item).length !== sig) throw new Error('signature sizing');
      return item;
    };
    const readArgs = (n, j) => JSON.stringify({path: files + '/r-' + n + '-' + j + '.txt'});
    const server = http.createServer((request, response) => {
      let raw = '';
      request.on('data', chunk => { raw += chunk; });
      request.on('end', () => {
        const payload = JSON.parse(raw);
        out.served++;
        const row = out.payloads.at(-1);
        if (row) row.dispatched = estimateTextTokens(raw);
        flush();
        const turn = out.served - 1;
        const next = turns[turn] ?? final;
        const n = turn + 1;
        response.writeHead(200, {'Content-Type': 'text/event-stream'});
        if (API === 'openai-completions') {
          const chunk = (delta, finish_reason = null) => response.write('data: ' + JSON.stringify({id: 'fit', object: 'chat.completion.chunk',
            created: 1, model: 'offline', choices: [{index: 0, delta, finish_reason}]}) + '\\n\\n');
          if (turns[turn]) {
            chunk({role: 'assistant', content: ('DECISION_' + n + ' ' + 'd'.repeat(next.decision)).slice(0, next.decision)});
            chunk({tool_calls: next.results.map((_, j) => ({index: j, id: 'read-' + n + '-' + j, type: 'function',
              function: {name: 'read', arguments: readArgs(n, j)}}))});
            chunk({}, 'tool_calls');
          } else {
            chunk({role: 'assistant', content: 'ACTIVATION_DONE'});
            chunk({}, 'stop');
          }
          response.end('data: [DONE]\\n\\n');
          return;
        }
        const output = [];
        for (const [k, sig] of (next?.reasoning ?? []).entries()) output.push(reasoningItem(n, k, sig, next.summary[k]));
        if (!turns[turn] || next.decision) output.push({id: 'msg_' + n, type: 'message', role: 'assistant', status: 'completed',
          content: [{type: 'output_text', annotations: [], text: turns[turn] ? ('DECISION_' + n + ' ' + 'd'.repeat(next.decision)).slice(0, next.decision) : 'ACTIVATION_DONE'}]});
        if (turns[turn]) for (const j of next.results.keys()) output.push({id: 'fc_' + n + '_' + j, type: 'function_call', status: 'completed',
          call_id: 'call_' + n + '-' + j, name: 'read', arguments: readArgs(n, j)});
        const send = event => response.write('event: ' + event.type + '\\ndata: ' + JSON.stringify(event) + '\\n\\n');
        for (const [i, item] of output.entries()) send({type: 'response.output_item.done', output_index: i, item});
        // Provider-reported usage: the scenario's (real-run model) or honest (dispatched body / 3.5).
        const total = next?.usage ? next.usage.input + next.usage.output : estimateTextTokens(raw) + 500;
        const usage = next?.usage ?? {input: total - 500, cacheRead: Math.floor((total - 500) * 0.9), output: 500};
        lastUsage = total;
        send({type: 'response.completed', response: {id: 'resp_' + n, status: 'completed', output: [], usage: {
          input_tokens: usage.input, input_tokens_details: {cached_tokens: usage.cacheRead}, output_tokens: usage.output,
          output_tokens_details: {reasoning_tokens: 0}, total_tokens: total}}});
        response.end();
      });
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    fs.writeFileSync(root + '/models.json', JSON.stringify({providers: {'fit-test': {baseUrl: 'http://127.0.0.1:' + server.address().port + '/v1',
      apiKey: 'offline-only', api: API, models: [{id: 'offline', name: 'offline', reasoning: API === 'openai-responses', input: ['text'],
      contextWindow: ${WINDOW}, maxTokens: ${RESERVE}, cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0}}]}}}));
    const modelRuntime = await ModelRuntime.create({modelsPath: root + '/models.json', refreshOnCreate: false, authPath: root + '/unused-auth.json'});
    const llm = msgs => {
      const by = role => est(msgs.filter(m => m.role === role));
      const tokens = est(msgs), assistant = by('assistant'), toolResult = by('toolResult'), user = by('user');
      return {tokens, assistant, toolResult, user, other: tokens - assistant - toolResult - user,
        excerpts: msgs.filter(m => m.role === 'toolResult' && text(m.content).includes(COMPACTED)).length};
    };
    // BEFORE: registered ahead of the hook, so its boundary context has no activation drafts yet.
    // The activation's custom context message is delivered natively with the prompt.
    const before = pi => (${custom} && pi.on('before_agent_start', () => ({message: {customType: 'fabric-activation-context',
      content: 'CUSTOM_CONTEXT ' + 'c'.repeat(${custom} - 15), display: false}})), pi.on('turn_end', event => {
      if (!event.toolResults.length) return;
      out.boundaries.push({turn: out.boundaries.length + 1, prevReq: out.payloads.length,
        hookNextEstimate: lastWire + est(convertToLlm([event.message, ...event.toolResults])),
        resultChars: event.toolResults.map(r => text(r.content).length), before: llm(event.context.llmMessages)});
      flush();
    }));
    // AFTER: registered behind the hook; the same boundary's preview context includes its drafts.
    let lastPayload;
    const after = pi => {
      pi.on('turn_end', event => {
        if (!event.toolResults.length) return;
        const b = out.boundaries.at(-1);
        b.drafts = event.entries.length;
        b.edits = event.entries.filter(e => e.type === 'context_edit').length;
        b.includeLatest = event.entries.some(e => e.type === 'compaction' && e.details?.protectedEntryIds?.length === 0);
        b.after = llm(event.context.llmMessages);
        flush();
      });
      pi.on('before_provider_request', event => { lastPayload = event.payload; measure(event.payload); });
    };
    const resources = (pad, extensionFactories) => ({cwd: root, agentDir: root, noSkills: true, noPromptTemplates: true, noThemes: true,
      noContextFiles: true, appendSystemPrompt: pad === undefined ? [] : [${JSON.stringify("SYSTEM_PAD ")} + 's'.repeat(pad)], extensionFactories});
    const open = async (loader, sessionManager) => (await createAgentSession({cwd: root, agentDir: root, modelRuntime,
      model: modelRuntime.getModel('fit-test', 'offline'), resourceLoader: loader, tools: ['read'], sessionManager,
      settingsManager: SettingsManager.inMemory({compaction: {enabled: false}, retry: {enabled: false}})})).session;
    let pad = ${systemPad} || undefined;
    if (${systemTarget}) {
      // DISCLOSED pad sized from this host's actual natively rendered system prompt (an unbound, unprompted
      // probe session with an empty pad), so the rendered system is exactly the target on every host version.
      const probeLoader = new DefaultResourceLoader(resources(0, []));
      await probeLoader.reload();
      const probe = await open(probeLoader, SessionManager.inMemory(root));
      pad = ${systemTarget} - probe.systemPrompt.length;
      probe.dispose();
      if (pad < 0) throw new Error('system target below the native system prompt');
    }
    const loader = new DefaultResourceLoader(resources(pad,
      [{name: 'fit-before', factory: before}, {name: 'activation-window', factory: hook}, {name: 'fit-after', factory: after}]));
    await loader.reload();
    const sessionManager = SessionManager.inMemory(root);
    session = await open(loader, sessionManager);
    const errors = [];
    await session.bindExtensions({mode: 'rpc', onError: error => errors.push(String(error.error ?? error))});
    // Session-side composition (raw append-only messages, not the projected wire).
    const totals = () => {
      const msgs = sessionManager.getBranch().filter(e => e.type === 'message' || e.type === 'custom_message')
        .map(e => e.type === 'message' ? e.message : {role: 'custom', content: e.content});
      const blocks = msgs.filter(m => m.role === 'assistant').flatMap(m => m.content);
      const thinking = blocks.filter(b => b.type === 'thinking');
      const results = msgs.filter(m => m.role === 'toolResult');
      const parts = results.flatMap(m => m.content).filter(p => p.type === 'text');
      const sys = lastPayload ? items(lastPayload).filter(m => kind(m) === 'system') : [];
      return {reasoningBlocks: thinking.length, signatureChars: thinking.reduce((a, b) => a + (b.thinkingSignature?.length ?? 0), 0),
        encryptedChars: thinking.reduce((a, b) => a + (b.thinkingSignature ? JSON.parse(b.thinkingSignature).encrypted_content.length : 0), 0),
        thinkingChars: thinking.reduce((a, b) => a + b.thinking.length, 0), toolResults: results.length, toolTextParts: parts.length,
        toolTextChars: parts.reduce((a, p) => a + p.text.length, 0),
        detailsChars: results.reduce((a, m) => a + (m.details === undefined ? 0 : JSON.stringify(m.details).length), 0),
        toolCallArgsChars: blocks.filter(b => b.type === 'toolCall').reduce((a, b) => a + JSON.stringify(b.arguments).length, 0),
        customChars: msgs.filter(m => m.role === 'custom').reduce((a, m) => a + text(m.content).length, 0),
        userChars: msgs.filter(m => m.role === 'user').reduce((a, m) => a + text(m.content).length, 0),
        usage: msgs.filter(m => m.role === 'assistant').map(m => m.usage?.totalTokens ?? 0),
        systemChars: sys.reduce((a, m) => a + text(m.content).length, 0), schemaChars: JSON.stringify(lastPayload?.tools ?? []).length};
    };
    process.on('exit', () => { try { out.totals = totals(); flush(); } catch {} });
    try {
      await session.prompt(${JSON.stringify(prompt)});
    } finally {
      out.errors = errors; out.totals = totals(); flush();
      session.dispose(); server.closeAllConnections(); server.close();
    }
  `);
  const result = spawnSync(process.execPath, [child], { encoding: "utf8", timeout: GUARD_MS, maxBuffer: 64 << 20 });
  const refusal = result.stdout.split("\n").filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return undefined; } })
    .find(frame => frame?.type === "fabric_activation_window_refusal");
  const data = fs.existsSync(evidence) ? JSON.parse(fs.readFileSync(evidence, "utf8")) : { payloads: [], boundaries: [], served: 0 };
  return { ...data, refusal: refusal && { type: refusal.type, code: refusal.code, tokens: refusal.tokens, contextWindow: refusal.contextWindow },
    status: result.status, stderr: result.stderr.slice(-2_000) };
}

// Per-compaction evidence, written only when FS_ACTFIT_EVIDENCE names a file. A compaction's BEFORE
// and AFTER are the same turn_end boundary context without and with the hook's drafts; prevPayload
// is the last native payload before that turn's tools, nextPayload the next native payload.
function report(name: string, scenario: Scenario, e: Evidence) {
  const { turns } = scenario;
  const compactions = e.boundaries.filter(b => b.drafts).map(b => ({
    turn: b.turn, drafts: b.drafts, edits: b.edits, includeLatest: b.includeLatest, hookNextEstimate: b.hookNextEstimate,
    before: b.before, after: b.after, prevPayload: e.payloads[b.prevReq - 1], nextPayload: e.payloads[b.prevReq],
  }));
  const last = e.payloads.at(-1);
  const delivered = e.boundaries.flatMap(b => b.resultChars);
  const summary = { name, api: scenario.api, window: WINDOW, reserve: RESERVE, admit: ADMIT, turns: turns.length,
    results: turns.reduce((n, t) => n + t.results.length, 0), deliveredToolChars: delivered.reduce((a, b) => a + b, 0),
    largestResults: [...delivered].sort((a, b) => b - a).slice(0, 10),
    assistantChars: turns.reduce((n, t) => n + t.decision, 0), systemPad: scenario.systemPad, custom: scenario.custom, user: scenario.user,
    payloads: e.payloads.length, served: e.served, status: e.status, refusal: e.refusal, totals: e.totals,
    compactions, last, peak: Math.max(0, ...e.payloads.map(p => p.tokens)), maxFloor: Math.max(0, ...e.payloads.map(p => p.floor)),
    maxAllExcerpt: Math.max(0, ...e.payloads.map(p => p.allExcerpt)),
    stderr: e.status === 0 ? undefined : e.stderr.split("\n").filter(Boolean).slice(-2) };
  const file = process.env.FS_ACTFIT_EVIDENCE;
  if (file) {
    const prior = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : {};
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify({ ...prior, [name]: { ...summary, series: e.payloads, boundaries: e.boundaries } }, null, 1));
  }
  return summary;
}

// Classification (Decision C): every test here is native-session E2E in a child process (native
// createAgentSession, SessionManager, read tool, openai-responses/-completions HTTP/SSE wire and the
// production hook). Transparently excluded: the AgentManager -> worker -> pinned Pi CLI process path.
// Planned named full-worker E2E pass condition: tests/worker-activation-window.test.ts "native activation
// window" > "fits the real-run composition by provider usage" with PI_FABRIC_ACTIVATION_TEST_PI_BINARY,
// AgentManager.run({inferenceContext: "activation", transport: "process"}) and this realRun responses server:
// status completed, no refusal frame, all 50 requests served, final usage 97,018; trueOverflow yields
// CONTEXT_OVERFLOW with no oversized body served; incident yields CONTEXT_OVERFLOW; fitting keeps its latest batch full.
describe("activation window fits a long native activation (smarty-dev#10560)", () => {
  it("admits the real-run composition whose provider usage peaks at 143,324 (usage-anchored regression)", () => {
    const scenario = realRun();
    const e = drive(scenario);
    const s = report("realRun", scenario, e);
    const t = e.totals!;
    // Composition is the real run's (session side, before projection).
    expect(t.reasoningBlocks, JSON.stringify(t)).toBe(REAL.reasoningItems);
    expect(t.signatureChars).toBe(REAL.signatureChars);
    expect(t.thinkingChars).toBeGreaterThanOrEqual(REAL.thinkingChars); // + the native "\n\n" summary-part joins: none (done-only)
    expect(t.customChars).toBe(REAL.custom);
    expect(t.userChars).toBe(REAL.user);
    expect(t.toolResults).toBe(63);
    expect(Math.abs(t.toolTextChars - REAL.toolTextChars) / REAL.toolTextChars, JSON.stringify(s.largestResults)).toBeLessThan(0.005);
    expect(Math.abs(t.toolCallArgsChars - REAL.toolCallArgsChars) / REAL.toolCallArgsChars).toBeLessThan(0.02);
    expect(t.detailsChars).toBeGreaterThan(0);
    expect(t.systemChars).toBe(REAL.systemSections);
    expect(Math.max(...t.usage)).toBeLessThanOrEqual(REAL.usagePeak);
    // Wire: reasoning replayed as parsed items; read details never sent; dispatched body == observed payload.
    for (const p of e.payloads) {
      expect(p.detailsOnWire).toBe(false);
      if (p.dispatched !== undefined) expect(p.dispatched).toBe(p.tokens);
    }
    // MAIN RED: the char preflight counts opaque encrypted reasoning and refuses although the provider reports <= 143,324.
    expect(e.refusal, JSON.stringify({ refusal: s.refusal, last: s.last, usage: t.usage.slice(-3) })).toBeUndefined();
    expect(e.status, e.stderr).toBe(0);
    expect(e.served).toBe(50);
    expect(s.compactions).toHaveLength(0); // no false compaction: usage never approaches the window
    expect(t.usage.at(-1)).toBe(REAL.usageFinalTotal);
    expect(Math.max(...t.usage)).toBe(REAL.usagePeak);
  }, GUARD_MS + 30_000);

  it("refuses typed with exit 78 and dispatches nothing oversized when provider usage plus new content exceeds the window", () => {
    const scenario = trueOverflow();
    const e = drive(scenario);
    const s = report("trueOverflow", scenario, e);
    expect(e.refusal, JSON.stringify(s)).toMatchObject({ type: "fabric_activation_window_refusal", code: "context-overflow",
      contextWindow: WINDOW });
    expect(e.status, e.stderr).toBe(78);
    // Provider usage reported for the last served request was near the window, and the refused request was never served.
    expect(e.totals!.usage.at(-1)).toBeGreaterThan(WINDOW - 20_000);
    expect(e.served).toBe(e.payloads.length - 1);
    for (const p of e.payloads) if (p.dispatched !== undefined) expect(p.dispatched).toBeLessThanOrEqual(WINDOW);
    expect(e.payloads.at(-1)!.dispatched).toBeUndefined();
    expect(e.payloads.at(-1)!.floor).toBeGreaterThan(WINDOW); // irreducible: visible assistant text
  }, GUARD_MS + 30_000);

  it("refuses typed with exit 78 when assistant history alone outgrows the window (char fallback, no provider usage)", () => {
    const scenario = incident();
    const e = drive(scenario);
    const s = report("incident", scenario, e);
    expect(e.refusal, JSON.stringify(s)).toMatchObject({ type: "fabric_activation_window_refusal", code: "context-overflow",
      contextWindow: WINDOW });
    expect(e.refusal!.tokens).toBeGreaterThan(ADMIT);
    expect(e.status, e.stderr).toBe(78);
    expect(e.totals!.usage.every(u => u === 0)).toBe(true); // SSE reports no usage
    expect(e.payloads.at(-1)!.floor, JSON.stringify(s.last)).toBeGreaterThan(ADMIT);
  }, GUARD_MS + 30_000);

  it("keeps the latest batch full when compacting older outputs is enough (counterexample)", () => {
    const scenario = fitting();
    const e = drive(scenario);
    const s = report("fitting", scenario, e);
    expect(e.refusal, JSON.stringify(s)).toBeUndefined();
    expect(e.status, e.stderr).toBe(0);
    expect(s.compactions.length, JSON.stringify(s)).toBeGreaterThan(0);
    for (const c of s.compactions) expect(c.nextPayload?.fullLatest, JSON.stringify(c)).toBe(true);
    expect(s.peak).toBeLessThan(ADMIT);
  }, GUARD_MS + 30_000);
});
