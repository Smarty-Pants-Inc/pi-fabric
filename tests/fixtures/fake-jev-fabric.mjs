#!/usr/bin/env node
// Minimal stand-in for the jev-fabric CLI contract Fabric uses (start, status,
// stop, follow, list, --version) with the same JSON shapes. Jobs run under a
// detached worker so they outlive the caller, like the native supervisor.
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const self = fileURLToPath(import.meta.url);
const argv = process.argv.slice(2);
if (argv[0] === "--") argv.shift();
const home = process.env.JEV_FABRIC_HOME ?? path.resolve(".jev-fabric-native");
const dir = (id) => path.join(home, id);
const print = (value) => process.stdout.write(JSON.stringify(value) + "\n");
const fail = (message, code = 1) => { process.stderr.write(message + "\n"); process.exit(code); };
const read = (file) => { try { return fs.readFileSync(file, "utf8"); } catch { return undefined; } };
const meta = (id) => JSON.parse(read(path.join(dir(id), "meta.json")) ?? "{}");
const receipt = (id) => { const text = read(path.join(dir(id), "receipt.json")); return text ? JSON.parse(text) : undefined; };
const state = (id) => {
  if (!fs.existsSync(dir(id))) fail(`unknown job: ${id}`, 22);
  const label = meta(id).label;
  return receipt(id) ?? { schemaVersion: 1, id, state: "running", ...(label ? { label } : {}) };
};
const events = (id) => (read(path.join(dir(id), "events.jsonl")) ?? "").split("\n").filter(Boolean).map((line) => JSON.parse(line));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const option = (name) => { const i = argv.indexOf(name); if (i < 0) return undefined; const [, value] = argv.splice(i, 2); return value; };

const command = argv.shift();
if (command === "--version") { process.stdout.write("0.5.0-native (fake)\n"); process.exit(0); }
if (command === "capabilities") {
  print({ version: "0.5.0-native (fake)", protocol: 2, store: 1, platform: `${process.platform}-${process.arch}`,
    features: (process.env.FAKE_JEV_FABRIC_FEATURES ?? "follow,list,label,start-24h").split(",").filter(Boolean) });
  process.exit(0);
}
if (command === "start") {
  const timeoutMs = Number(option("--timeout-ms") ?? 3600000);
  const label = option("--label");
  const input = option("--input");
  const cwd = option("--cwd");
  if (argv[0] === "--") argv.shift();
  const id = randomUUID().replaceAll("-", "");
  fs.mkdirSync(dir(id), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(dir(id), "meta.json"), JSON.stringify({ label, startedAt: Date.now(), input: input === "pipe" }));
  if (input === "pipe") fs.writeFileSync(path.join(dir(id), "input"), "");
  const worker = spawn(process.execPath, [self, "--", "__worker", id, String(timeoutMs), ...argv], { detached: true, stdio: "ignore", ...(cwd ? { cwd } : {}) });
  worker.unref();
  print({ id });
  process.exit(0);
}
if (command === "__worker") {
  const [id, timeoutMs, ...child] = argv;
  let sequence = 0;
  const event = (type, data) => fs.appendFileSync(path.join(dir(id), "events.jsonl"), JSON.stringify({ sequence: ++sequence, type, data }) + "\n");
  event("job.started", { id });
  const offsets = { stdout: 0, stderr: 0 };
  // Own process group, like the native supervisor, so stop reaches descendants.
  const interactive = meta(id).input === true;
  const proc = spawn(child[0], child.slice(1), { stdio: [interactive ? "pipe" : "ignore", "pipe", "pipe"], detached: true });
  if (interactive) {
    // Drain the private input queue each tick, like the native worker.
    let consumed = 0; let ended = false;
    proc.stdin.on("error", () => {});
    const feed = setInterval(() => {
      const queue = fs.readFileSync(path.join(dir(id), "input"));
      if (queue.length > consumed) { proc.stdin.write(queue.subarray(consumed)); consumed = queue.length; }
      if (!ended && fs.existsSync(path.join(dir(id), "input.closed"))) { ended = true; proc.stdin.end(); }
    }, 25);
    proc.on("close", () => clearInterval(feed));
  }
  const kill = () => { try { process.kill(-proc.pid, "SIGTERM"); } catch { proc.kill("SIGTERM"); } };
  let stopped = false; let timedOut = false;
  for (const stream of ["stdout", "stderr"]) proc[stream].on("data", (chunk) => {
    event("process.output", { stream, offset: offsets[stream], bytes: chunk.length, omittedBytes: 0, text: chunk.toString("utf8") });
    offsets[stream] += chunk.length;
  });
  const poll = setInterval(() => { if (fs.existsSync(path.join(dir(id), "stop"))) { stopped = true; kill(); } }, 25);
  const deadline = setTimeout(() => { timedOut = true; kill(); }, Number(timeoutMs));
  proc.on("close", (code) => {
    clearInterval(poll); clearTimeout(deadline);
    event("job.finished", { receiptAvailable: true });
    const label = meta(id).label;
    const state = timedOut ? "timed_out" : stopped ? "cancelled" : code === 0 ? "exited" : "failed";
    fs.writeFileSync(path.join(dir(id), "receipt.json"), JSON.stringify({ id, schemaVersion: 1, state, exitCode: code, timedOut, cancelled: stopped, ...(label ? { label } : {}) }));
  });
} else if (command === "status") print(state(argv[0]));
else if (command === "stop") {
  const id = argv[0];
  state(id);
  fs.writeFileSync(path.join(dir(id), "stop"), "");
  for (let i = 0; i < 200 && !receipt(id); i++) await sleep(25);
  print(state(id));
} else if (command === "follow") {
  const timeoutMs = Number(option("--timeout-ms") ?? 30000);
  const [id, after = "0"] = argv;
  state(id);
  let next = Number(after);
  const until = Date.now() + timeoutMs;
  for (;;) {
    const done = receipt(id);
    for (const item of events(id)) if (item.sequence > next) { print(item); next = item.sequence; }
    if (done) { print({ type: "follow.end", reason: "finished", next, receipt: done }); break; }
    if (Date.now() >= until) { print({ type: "follow.end", reason: "timeout", next, receipt: state(id) }); break; }
    await sleep(25);
  }
} else if (command === "list") {
  const ids = fs.existsSync(home) ? fs.readdirSync(home).filter((name) => fs.statSync(dir(name)).isDirectory()) : [];
  print({ jobs: ids.map((id) => { const s = state(id); const m = meta(id); return { id, state: s.state, ...(m.label ? { label: m.label } : {}), ...(m.startedAt ? { startedAt: m.startedAt } : {}) }; }), truncated: false });
} else if (command === "serve") {
  // Protocol 2 subset: requests are handled concurrently and answered by id.
  const timeoutMs = Number(option("--timeout-ms") ?? 3600000);
  print({ ready: { protocol: 2, version: "0.5.0-native (fake)", store: 1, timeoutMs, maxEvaluations: 0, maxTokens: 0,
    features: ["follow", "list", "label", "sessions", "serve-concurrent", "read", "durable-input"] } });
  const children = new Map();
  let serial = 0;
  const receiptOf = (c) => c.receipt ?? { id: c.id, lifetime: c.lifetime, state: "running", ...(c.label ? { label: c.label } : {}) };
  const settle = (c) => { for (const wake of c.waiters.splice(0)) wake(); };
  const within = (c, until, ready) => new Promise((resolve) => {
    if (ready() || Date.now() >= until) return resolve();
    const timer = setTimeout(done, until - Date.now());
    function done() { clearTimeout(timer); const i = c.waiters.indexOf(check); if (i >= 0) c.waiters.splice(i, 1); resolve(); }
    function check() { if (ready()) done(); else c.waiters.push(check); }
    c.waiters.push(check);
  });
  const child = (id) => { const c = children.get(id); if (!c) throw Object.assign(new Error(`unknown job: ${id}`), { code: 22 }); return c; };
  // Durable jobs live in the store, so any connection (or the CLI) can reach them.
  const durable = (id) => { if (children.has(id) || !fs.existsSync(dir(id))) return undefined; return id; };
  const stored = (id, stream) => Buffer.from(events(id).filter((e) => e.type === "process.output" && e.data.stream === stream).map((e) => e.data.text).join(""));
  const storeHandlers = {
    write: (r, id) => { if (receipt(id) || fs.existsSync(path.join(dir(id), "input.closed"))) throw new Error("job has finished"); fs.appendFileSync(path.join(dir(id), "input"), r.text); return { id, written: Buffer.byteLength(r.text), closed: false }; },
    closeInput: (r, id) => { fs.writeFileSync(path.join(dir(id), "input.closed"), ""); return { id, written: 0, closed: true }; },
    read: async (r, id) => {
      const stream = r.stream ?? "stdout"; const offset = r.offset ?? 0; const until = Date.now() + (r.waitMs ?? 0);
      while (r.waitMs && stored(id, stream).length <= offset && !receipt(id) && Date.now() < until) await sleep(25);
      const data = stored(id, stream); const slice = data.subarray(offset, offset + (r.max ?? 65536));
      return { id, stream, offset, bytes: slice.length, omittedBytes: 0, ...(r.encoding === "base64" ? { data: slice.toString("base64") } : { text: slice.toString("utf8") }),
        next: offset + slice.length, eof: Boolean(receipt(id)) && offset + slice.length >= data.length, state: state(id).state };
    },
    events: (r, id) => { const retained = events(id).filter(event => event.sequence > (r.after ?? 0)); return { events: retained, next: retained.at(-1)?.sequence ?? (r.after ?? 0) }; },
    status: (r, id) => ({ ...state(id), lifetime: "durable" }),
    wait: async (r, id) => { const until = Date.now() + (r.timeoutMs ?? 30000); while (!receipt(id) && Date.now() < until) await sleep(25); return { ...state(id), lifetime: "durable" }; },
  };
  const handlers = {
    spawn: (r) => open(r, "session"),
    start: (r) => {
      const args = ["--", "start", ...(r.timeoutMs ? ["--timeout-ms", String(r.timeoutMs)] : []), ...(r.label ? ["--label", r.label] : []),
        ...(r.input === "pipe" ? ["--input", "pipe"] : []), ...(r.cwd ? ["--cwd", r.cwd] : []), "--", ...r.argv];
      const out = spawnSync(process.execPath, [self, ...args], { encoding: "utf8" });
      if (out.status !== 0) throw new Error(out.stderr.trim() || "start failed");
      return { ...JSON.parse(out.stdout), lifetime: "durable", state: "running" };
    },
    write: (r) => { const c = child(r.job); if (c.receipt || c.inputClosed) throw new Error("stdin is closed"); c.proc.stdin.write(r.text); return { id: c.id, written: Buffer.byteLength(r.text), closed: false }; },
    closeInput: (r) => { const c = child(r.job); c.inputClosed = true; c.proc.stdin.end(); return { id: c.id, written: 0, closed: true }; },
    read: async (r) => {
      const c = child(r.job); const stream = r.stream ?? "stdout"; const offset = r.offset ?? 0;
      if (r.waitMs) await within(c, Date.now() + r.waitMs, () => c.out[stream].length > offset || c.receipt);
      const slice = c.out[stream].subarray(offset, offset + (r.max ?? 65536));
      return { id: c.id, stream, offset, bytes: slice.length, omittedBytes: 0,
        ...(r.encoding === "base64" ? { data: slice.toString("base64") } : { text: slice.toString("utf8") }),
        next: offset + slice.length, eof: Boolean(c.receipt) && offset + slice.length >= c.out[stream].length, state: receiptOf(c).state };
    },
    status: (r) => receiptOf(child(r.job)),
    wait: async (r) => { const c = child(r.job); await within(c, Date.now() + (r.timeoutMs ?? 30000), () => c.receipt); return receiptOf(c); },
    stop: async (r) => { const c = child(r.job); if (!c.receipt) { c.stopped = true; try { process.kill(-c.proc.pid, "SIGTERM"); } catch {} } await within(c, Date.now() + 5000, () => c.receipt); return receiptOf(c); },
    list: () => ({ jobs: [...children.values()].map(receiptOf), truncated: false }),
    // Canned typed answers; the model string says which provider and whether a
    // per-request credential arrived, never the credential itself.
    jev: (r) => {
      const answers = Object.fromEntries(Object.entries(r.request.questions).map(([key, q]) => {
        if (q.type === "noul") return [key, { type: "noul", noul: 0.9 }];
        const keys = q.type === "choice" ? Object.keys(q.criteria) : q.criteria.map((_, i) => String(i));
        const probabilities = Object.fromEntries(keys.map((k, i) => [k, i === 0 ? 1 : 0]));
        return [key, q.type === "choice" ? { type: "choice", choice: keys[0], confidence: 1, probabilities } : { type: "score", score: 0, confidence: 1, probabilities }];
      }));
      return { model: `fake:${r.provider ?? "env"}:${r.credential ? "credential" : "none"}:${r.request.model}`, answers, usage: { input_tokens: 1, output_tokens: 1 } };
    },
  };
  function open(r, lifetime) {
    const id = `${lifetime === "session" ? "s-" : ""}${(++serial).toString(16).padStart(8, "0")}`;
    const proc = spawn(r.argv[0], r.argv.slice(1), { cwd: r.cwd, stdio: ["pipe", "pipe", "pipe"], detached: true });
    const c = { id, lifetime, label: r.label, proc, out: { stdout: Buffer.alloc(0), stderr: Buffer.alloc(0) }, waiters: [] };
    for (const stream of ["stdout", "stderr"]) proc[stream].on("data", (chunk) => { c.out[stream] = Buffer.concat([c.out[stream], chunk]); settle(c); });
    proc.stdin.on("error", () => {});
    const deadline = setTimeout(() => { c.stopped = true; try { process.kill(-proc.pid, "SIGTERM"); } catch {} }, r.timeoutMs ?? 3600000);
    proc.on("close", (code) => {
      clearTimeout(deadline);
      c.receipt = { id, lifetime, state: c.stopped ? "cancelled" : code === 0 ? "exited" : "failed", exitCode: code, ...(c.label ? { label: c.label } : {}) };
      settle(c);
    });
    children.set(id, c);
    return { id, lifetime, state: "running", ...(r.label ? { label: r.label } : {}) };
  }
  let pending = "";
  process.stdin.on("data", (chunk) => {
    pending += chunk;
    let index;
    while ((index = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, index); pending = pending.slice(index + 1);
      if (!line.trim()) continue;
      const request = JSON.parse(line);
      Promise.resolve().then(() => {
        const stored = typeof request.job === "string" ? durable(request.job) : undefined;
        if (stored && storeHandlers[request.op]) return storeHandlers[request.op](request, stored);
        const handler = handlers[request.op];
        if (!handler) throw Object.assign(new Error(`unknown op: ${request.op}`), { code: 2 });
        return handler(request);
      }).then((result) => print({ id: request.id, ok: true, result }),
        (error) => print({ id: request.id, ok: false, error: { code: error.code ?? 1, message: error.message } }));
    }
  });
  // End of input ends the connection and its session children; durable ones stay.
  process.stdin.on("end", () => {
    for (const c of children.values()) if (c.lifetime === "session" && !c.receipt) { try { process.kill(-c.proc.pid, "SIGKILL"); } catch {} }
    setTimeout(() => process.exit(0), 50);
  });
} else fail(`unknown command: ${command}`, 2);
