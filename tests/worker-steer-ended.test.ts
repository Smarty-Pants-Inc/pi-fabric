import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readFinalAnswerReceipt, taskSteerFile } from "../src/worker/terminal-answer.js";
import { readTerminalControl } from "../src/worker/terminal-controls.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";

// Same process-transport + built real-worker path as worker-e2e. Only the model
// child is deterministic; fixtures are retained under the (scoped) TMPDIR (no
// real deletes in this proof). Controls live in the protected central layout.
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});
const rows = (file: string): Array<Record<string, any>> => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const observed = async (root: string, condition: () => boolean): Promise<void> => {
  if (condition()) return;
  await new Promise<void>((resolve, reject) => {
    const finish = (error?: Error) => { clearTimeout(timer); watcher.close(); error ? reject(error) : resolve(); };
    const watcher = fs.watch(root, { recursive: true }, () => { if (condition()) finish(); });
    const timer = setTimeout(() => finish(new Error("E-steer-final observation timed out")), 20_000);
    if (condition()) finish();
  });
};
// Protected controls live in the central task-control root, OUTSIDE s.root, so a
// recursive fs.watch on s.root cannot observe control-state transitions. Wait on
// those (and on hook-driven events) with a bounded deadline instead.
const until = async (condition: () => boolean, timeoutMs = 20_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("Steer-ended proof deadline exceeded");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
  sender: { id: "session:steer-ended-sender", name: "Original sender", kind: "main" as const, verified: "mesh" as const } };
/** Fault/interleave cases need NODE_OPTIONS --import hooks to observe the real
 * worker's held FILE fds, so they run the real source through Node/jiti (Bun's
 * direct .ts launcher ignores NODE_OPTIONS). Default cases use the built worker. */
const setup = (options: { sourceWorker?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-steer-ended-"));
  const workspace = path.join(root, "workspace"); fs.mkdirSync(workspace);
  const profile = path.join(root, "profile"); fs.mkdirSync(profile);
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ retry: { enabled: false } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("STEER_ENDED_ROOT", root);
  const sourceRoot = path.resolve(process.env.STEER_ENDED_SOURCE_ROOT ?? ".");
  const helper = path.join(root, "terminal-boundary.mjs");
  // Both revisions run their own production child boundary/delivery gates. Only
  // the SDK session is fake; never manufacture a discard in the fixture.
  buildSync({ stdin: { contents: [
    `export { installTerminalAnswerBoundary } from ${JSON.stringify(path.join(sourceRoot, "src/worker/terminal-boundary.ts"))};`,
    `export { default as principalDelivery } from ${JSON.stringify(path.join(sourceRoot, "src/worker/principal-delivery.ts"))};`,
    `export { readTerminalControl } from ${JSON.stringify(path.join(sourceRoot, "src/worker/terminal-controls.ts"))};`,
    // The interleave hook resolves the protected control layout through the real helper.
    `export { saveFinalAnswerReceipt, taskControlDirectory, taskSteerFile } from ${JSON.stringify(path.join(sourceRoot, "src/worker/terminal-answer.ts"))};`,
  ].join("\n"), resolveDir: sourceRoot, sourcefile: "steer-ended-production-gates.ts" },
    outfile: helper, bundle: true, platform: "node", format: "esm" });
  vi.stubEnv("STEER_ENDED_HELPER", pathToFileURL(helper).href);
  const sdk = path.join(root, "sdk");
  for (const file of ["cli.js", "main.js", "cli/args.js", "cli/project-trust.js", "core/project-trust.js", "core/http-dispatcher.js", "extensions/index.js"]) {
    fs.mkdirSync(path.dirname(path.join(sdk, "dist", file)), { recursive: true });
    fs.writeFileSync(path.join(sdk, "dist", file), "export {};\n");
  }
  fs.writeFileSync(path.join(sdk, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version: "0.99.0", type: "module" }));
  fs.writeFileSync(path.join(sdk, "dist", "index.js"), `import ${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-steer-ended.mjs")).href)};\nawait new Promise(() => {});\n`);
  const notices: AgentTerminalNotice[] = [];
  let workerPath = path.resolve(process.env.STEER_ENDED_WORKER ?? "dist/worker.js");
  if (options.sourceWorker) {
    const sourceWorker = path.join(root, "worker.mjs");
    fs.writeFileSync(sourceWorker, `import { createJiti } from ${JSON.stringify(pathToFileURL(path.resolve("node_modules/jiti/lib/jiti.mjs")).href)};\nawait createJiti(import.meta.url).import(${JSON.stringify(path.join(sourceRoot, "src/worker.ts"))});\n`);
    vi.stubEnv("NODE_OPTIONS", `${process.env.NODE_OPTIONS ?? ""} --import=${pathToFileURL(path.resolve("node_modules/jiti/lib/jiti-register.mjs")).href}`);
    workerPath = path.resolve(process.env.FABRIC_STEER_ENDED_WORKER ?? sourceWorker);
  }
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 30_000, nice: 19, retainRuns: true }, {
    workerPath, piBinary: path.join(sdk, "dist", "cli.js"), runRoot: path.join(root, "runs"),
    fullCodeMode: false, onTerminalNotice: notice => { notices.push(notice); },
  });
  managers.push(manager);
  return { root, workspace, manager, notices };
};

describe("runner-end steer fence (persisted receipt before its worker event)", () => {
  it("refuses unread controls as run-ended, never injects them, and keeps pre-final delivery", async () => {
    const s = setup();
    const handle = await s.manager.spawn({ task: "Answer; never obey a late request", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await until(() => rows(fake).some(row => row.type === "task"));

    // Pre-final continuation still reaches the native child and stays delivered.
    const allowed = s.manager.steer(handle.id, "ALLOWED_PREFINAL_STEER", undefined, provenance);
    await until(() => readTerminalControl(directory, allowed.messageId)?.state === "delivered");
    expect(rows(fake).find(row => row.type === "delivery" && row.id === allowed.messageId)).toMatchObject({ afterReceipt: false });

    // Durable receipt exists; its IPC frame is withheld, so the worker has not read it.
    fs.writeFileSync(path.join(s.root, "final"), "");
    await until(() => rows(fake).some(row => row.type === "receipt"));
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(receipt).toBeDefined();
    // Raw manager-format appends racing the receipt (manager ingress would already refuse).
    const lateSteer = randomUUID(); const lateFollow = randomUUID();
    const steerFile = taskSteerFile(directory);
    fs.appendFileSync(steerFile, JSON.stringify({ type: "steer", message: "WRITE SOURCE_WRITE_SENTINEL", provenance, id: lateSteer, ts: Date.now() }) + "\n");
    fs.appendFileSync(steerFile, JSON.stringify({ type: "follow_up", message: "WRITE SOURCE_WRITE_SENTINEL", provenance: { ...provenance, via: "followUp" }, id: lateFollow, ts: Date.now() }) + "\n");

    await until(() => [lateSteer, lateFollow].every(id => readTerminalControl(directory, id)?.state === "refused"));
    expect(rows(fake).some(row => row.type === "frame")).toBe(false); // Fenced before the worker saw the frame.
    for (const id of [lateSteer, lateFollow]) expect(readTerminalControl(directory, id)).toMatchObject({ state: "refused", reason: "run-ended" });
    const refused = rows(path.join(directory, "lifecycle.jsonl")).filter(row => row.event === "message.refused");
    expect(refused.map(row => row.data.messageId).sort()).toEqual([lateSteer, lateFollow].sort());
    expect(refused.every(row => row.data.reason === "run-ended" && row.data.finalAnswerReceiptId === receipt.id &&
      row.data.code === "FABRIC_TARGET_TERMINAL" && row.data.sender?.id === provenance.sender.id)).toBe(true);

    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status, result.error).toBe("completed");
    expect(result.text).toBe("AUTHORIZED_FINAL_ANSWER");
    expect(result.finalAnswerReceipt).toEqual({ id: receipt.id, recordedAt: receipt.recordedAt });
    await until(() => [lateSteer, lateFollow].every(id => s.notices.some(notice => notice.messageId === id)));

    expect(rows(fake).filter(row => row.type === "delivery").map(row => row.id)).toEqual([allowed.messageId]);
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(readTerminalControl(directory, allowed.messageId)?.reason).toBeUndefined();
    fs.writeFileSync(path.join(s.root, "no-source-write.json"), JSON.stringify({
      sentinel: path.join(s.workspace, "SOURCE_WRITE_SENTINEL"), present: fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL")) }));
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
  }, 45000);

  it.each(["read", "inject"] as const)("%s interleaving: receipt between poll preflight and injection is refused with original sender evidence", async mode => {
    const s = setup({ sourceWorker: true });
    const hook = pathToFileURL(path.resolve("tests/fixtures/steer-final-interleave.mjs")).href;
    vi.stubEnv("NODE_OPTIONS", `${process.env.NODE_OPTIONS ?? ""} --import=${hook}`);
    const handle = await s.manager.spawn({ task: "deterministic publication race", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    await until(() => rows(path.join(s.root, "fake.jsonl")).some(row => row.type === "task"));
    const id = randomUUID();
    const entry = { id, type: "steer", message: "WRITE SOURCE_WRITE_SENTINEL", provenance, ts: Date.now() };
    // Arm before the next poll. 'read' records a size snapshot of zero, appends,
    // then blocks the real worker until the other process publishes its receipt.
    fs.writeFileSync(taskSteerFile(directory), "");
    fs.writeFileSync(path.join(s.root, "interleave.json"), JSON.stringify({ mode, entry }));
    const messageId = mode === "read" ? id : s.manager.steer(handle.id, entry.message, undefined, provenance).messageId;
    await until(() => readTerminalControl(directory, messageId)?.state === "refused");
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(readTerminalControl(directory, messageId)).toMatchObject({ state: "refused", reason: "run-ended", provenance });
    const evidence = rows(path.join(s.root, "interleave-log.jsonl"));
    expect(evidence.some(row => row.event === "receipt-before-injection")).toBe(true);
    if (mode === "read") expect(evidence[0]).toMatchObject({ event: "stat-before-append", bytes: 0 });
    expect(rows(path.join(s.root, "fake.jsonl")).some(row => row.type === "delivery")).toBe(false);
    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: receipt.text });
    await until(() => s.notices.some(notice => notice.messageId === messageId));
    expect(s.notices.find(notice => notice.messageId === messageId)).toMatchObject({ sender: provenance.sender, finalAnswerReceiptId: receipt.id });
    await s.manager.close();
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
    console.log(`INTERLEAVING: PASS mode=${mode} receiptFirst=true injected=false refused=${messageId} childExited=true root=${s.root}`);
  }, 45000);

  it.each(process.getuid ? ["oversized", "growing", "controls", "owner"] as const : ["oversized", "growing", "controls"] as const)("fails the live journal closed for %s without allowing later input", async fault => {
    const s = setup({ sourceWorker: true });
    const hook = path.join(s.root, "journal-bound-hook.mjs");
    fs.writeFileSync(hook, `import fs from 'node:fs';
if(process.argv.includes('--steer-file')){
 const root=${JSON.stringify(s.root)}, fault=${JSON.stringify(fault)}, descriptors=new Map(), open=fs.openSync, stat=fs.fstatSync, read=fs.readSync;
 fs.openSync=(file,...args)=>{const fd=open(file,...args);descriptors.delete(fd);const actual=fs.realpathSync(process.platform==='linux'?'/proc/self/fd/'+fd:String(file));if(actual.endsWith('/steer.jsonl'))descriptors.set(fd,actual);return fd;};
 fs.fstatSync=(fd,...args)=>{const value=stat(fd,...args);if(fault==='owner'&&descriptors.has(fd)&&fs.existsSync(root+'/armed'))value.uid=typeof value.uid==='bigint'?value.uid+1n:value.uid+1;return value;};
 fs.readSync=(fd,...args)=>{if(fault==='growing'&&descriptors.has(fd)&&fs.existsSync(root+'/armed')&&!fs.existsSync(root+'/grew')){fs.appendFileSync(descriptors.get(fd),'x'.repeat(1048577));fs.writeFileSync(root+'/grew','');}return read(fd,...args);};
}
`);
    vi.stubEnv("NODE_OPTIONS", `${process.env.NODE_OPTIONS ?? ""} --import=${pathToFileURL(hook).href}`);
    const handle = await s.manager.spawn({ task: "bounded live journal", model: "steer-ended/fake", thinking: "off", transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id), file = taskSteerFile(directory), fake = path.join(s.root, "fake.jsonl");
    await until(() => rows(fake).some(row => row.type === "task"));
    const id = randomUUID(), entry = JSON.stringify({ id, type: "steer", message: "NO_OVERLIMIT_DELIVERY", provenance }) + "\n";
    fs.writeFileSync(path.join(s.root, "armed"), "");
    fs.writeFileSync(file, fault === "oversized" ? entry + "x".repeat(1048577) : fault === "controls"
      ? Array.from({ length: 257 }, () => JSON.stringify({ type: "set_steering_mode", mode: "all" }) + "\n").join("") : entry);
    await until(() => rows(path.join(directory, "events.jsonl")).some(row => row.type === "fabric_steer_journal_invalid"));
    if (fault === "growing") expect(fs.existsSync(path.join(s.root, "grew"))).toBe(true);
    expect(rows(fake).some(row => row.type === "delivery")).toBe(false);
    // Shrinking/replacing the bad journal cannot reset the lifetime fence.
    fs.writeFileSync(file, entry); fs.writeFileSync(path.join(s.root, "final"), "");
    await until(() => rows(fake).some(row => row.type === "receipt"));
    fs.writeFileSync(path.join(s.root, "release"), "");
    expect((await s.manager.wait(handle.id, { timeoutMs: 30000 })).status).toBe("completed");
    await s.manager.close();
    expect(rows(fake).some(row => row.type === "delivery")).toBe(false);
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
  }, 45000);

  // The former PR-only direct-spawn counterexample (unfenced fixture obeys a
  // post-receipt delivery) cannot run against the main fixture: it now routes
  // every delivery through the production child gate, which is the fence. Sink
  // liveness is proven by the "prefinal native action" counterexample below
  // (same drainNative sentinel path, PREFINAL_WRITE_SENTINEL is written).
});

describe("E-steer-final: real worker final-answer fence", () => {
  it("delivers before final, refuses a post-final steer with an undelivered receipt, and starts no new turn", async () => {
    const s = setup();
    const handle = await s.manager.spawn({ task: "Return the authorized answer", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await observed(s.root, () => rows(fake).some(row => row.type === "task"));
    const allowed = s.manager.steer(handle.id, "ALLOWED_PREFINAL_STEER", undefined, provenance);
    await until(() => readTerminalControl(directory, allowed.messageId)?.state === "delivered");
    await observed(s.root, () => rows(fake).some(row => row.type === "delivery" && row.id === allowed.messageId));
    expect(rows(fake).find(row => row.type === "delivery" && row.id === allowed.messageId)).toMatchObject({ afterReceipt: false });

    fs.writeFileSync(path.join(s.root, "final"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "receipt"));
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    const journal = taskSteerFile(directory); // Protected central layout.
    const before = fs.readFileSync(journal, "utf8");
    expect(() => s.manager.steer(handle.id, "WRITE SOURCE_WRITE_SENTINEL", undefined, provenance)).toThrow(expect.objectContaining({
      name: "FabricTargetTerminalError", code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", deliveryStatus: "undelivered",
      runState: "final-answer-pending", finalAnswerReceiptId: receipt.id,
      message: expect.stringContaining("undelivered: task already finished"),
    }));
    expect(fs.readFileSync(journal, "utf8")).toBe(before);
    expect(rows(fake).some(row => row.type === "frame")).toBe(false);

    // An admission racing the pre-check is represented by its original journal
    // row. The worker must refuse it BEFORE seeing the held final IPC frame.
    const late = randomUUID();
    fs.appendFileSync(journal, JSON.stringify({ type: "steer", message: "WRITE SOURCE_WRITE_SENTINEL", provenance, id: late, ts: Date.now() }) + "\n");
    await until(() => readTerminalControl(directory, late)?.state === "refused");
    expect(rows(fake).some(row => row.type === "frame")).toBe(false);
    const refusal = rows(path.join(directory, "lifecycle.jsonl")).find(row => row.event === "message.refused" && row.data.messageId === late);
    expect(refusal?.data).toMatchObject({ code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt.id, sender: provenance.sender });

    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30_000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: "AUTHORIZED_FINAL_ANSWER", finalAnswerReceipt: { id: receipt.id } });
    await s.manager.close(); // Join the owned native process tree, not just the answer.
    expect(s.notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, finalAnswerReceiptId: receipt.id,
      messageId: late, delivery: "steer", sender: provenance.sender }]);
    expect(rows(fake).filter(row => row.type === "task")).toHaveLength(1);
    expect(rows(fake).filter(row => row.type === "delivery").map(row => row.id)).toEqual([allowed.messageId]);
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
    expect(() => s.manager.steer(handle.id, "late after native close")).toThrow(expect.objectContaining({ deliveryStatus: "undelivered", runState: "completed" }));
    console.log(`E-steer-final: PASS worker=dist/worker.js root=${s.root} prefinal=delivered postfinal=undelivered turns=1 sourceUnchanged=true`);
  }, 45_000);

  it("keeps a control written to child stdin pending until the post-receipt child discard ACK", async () => {
    const s = setup();
    fs.writeFileSync(path.join(s.root, "hold-controls"), "");
    const handle = await s.manager.spawn({ task: "Return the authorized answer", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await observed(s.root, () => rows(fake).some(row => row.type === "task"));
    const raced = s.manager.steer(handle.id, "WRITE SOURCE_WRITE_SENTINEL", undefined, provenance);
    // Pipe capture is positive proof of an actual write, not journal admission.
    await observed(s.root, () => rows(fake).some(row => row.type === "captured" && row.id === raced.messageId));
    expect(rows(fake).find(row => row.type === "captured" && row.id === raced.messageId)).toMatchObject({
      command: "prompt", message: `/fabric-delivery ${raced.messageId}`, afterReceipt: false,
    });
    expect(readFinalAnswerReceipt(directory, handle.id)).toBeUndefined();
    expect(readTerminalControl(directory, raced.messageId)?.state).toBe("queued");
    expect(rows(fake).some(row => row.type === "consume" && row.id === raced.messageId)).toBe(false);

    fs.writeFileSync(path.join(s.root, "final"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "receipt"));
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30_000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: "AUTHORIZED_FINAL_ANSWER", finalAnswerReceipt: { id: receipt.id } });
    // Host has observed/frozen the final receipt, but the frame is STILL held.
    // Refusal here would be the bug: forwarding does not certify consumption or
    // cancellation, even though the final-answer lock ordered the stdin write.
    expect(readTerminalControl(directory, raced.messageId)?.state).toBe("queued");
    expect(s.notices).toEqual([]);
    expect(rows(path.join(directory, "lifecycle.jsonl")).some(row => row.event === "message.refused" && row.data.messageId === raced.messageId)).toBe(false);
    expect(rows(fake).some(row => row.type === "consume" && row.id === raced.messageId)).toBe(false);

    fs.writeFileSync(path.join(s.root, "consume-controls"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "ack-observed" && row.id === raced.messageId));
    expect(rows(fake).find(row => row.type === "consume" && row.id === raced.messageId)).toMatchObject({ afterReceipt: true });
    expect(rows(fake).find(row => row.type === "ack-observed" && row.id === raced.messageId)).toMatchObject({ state: "refused", afterReceipt: true });
    expect(readTerminalControl(directory, raced.messageId)?.state).toBe("refused");
    await s.manager.close();
    expect(s.notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, finalAnswerReceiptId: receipt.id,
      messageId: raced.messageId, delivery: "steer", sender: provenance.sender }]);
    expect(rows(fake).filter(row => row.type === "task")).toHaveLength(1);
    expect(rows(fake).some(row => ["delivery", "action"].includes(row.type) && row.id === raced.messageId)).toBe(false);
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
    const events = rows(fake);
    expect(events.findIndex(row => row.type === "captured" && row.id === raced.messageId)).toBeLessThan(events.findIndex(row => row.type === "receipt"));
    expect(events.findIndex(row => row.type === "receipt")).toBeLessThan(events.findIndex(row => row.type === "consume" && row.id === raced.messageId));
    console.log(`E-steer-final forwarded-race: PASS root=${s.root} capturedBeforeReceipt=true consumedAfterReceipt=true childDiscardAck=true senderNoticeAfterAck=true action=false`);
  }, 45_000);

  it("uses the production child gate to ACK a held /fabric-delivery after receipt, not merely throw a terminal error", async () => {
    const s = setup();
    fs.writeFileSync(path.join(s.root, "hold-controls"), "");
    // Consume directly from the fixture's final-event hook, before releasing
    // IPC to the host. This isolates the production CHILD fence regression from
    // the separate host pending-control check above.
    fs.writeFileSync(path.join(s.root, "consume-on-final"), "");
    const handle = await s.manager.spawn({ task: "Return the authorized answer", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await observed(s.root, () => rows(fake).some(row => row.type === "task"));
    const raced = s.manager.steer(handle.id, "WRITE SOURCE_WRITE_SENTINEL", undefined, provenance);
    await observed(s.root, () => rows(fake).some(row => row.type === "captured" && row.id === raced.messageId));
    expect(readTerminalControl(directory, raced.messageId)?.state).toBe("queued");
    fs.writeFileSync(path.join(s.root, "final"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "ack-observed" && row.id === raced.messageId));
    const events = rows(fake);
    expect(events.find(row => row.type === "captured" && row.id === raced.messageId)).toMatchObject({ afterReceipt: false });
    expect(events.find(row => row.type === "consume" && row.id === raced.messageId)).toMatchObject({ afterReceipt: true });
    expect(events.some(row => row.type === "frame")).toBe(false);
    // A terminal RPC error is allowed ONLY with a durable child discard ACK.
    // The base wrapper throws the same error but leaves this registry queued.
    for (const error of events.filter(row => row.type === "command-error" && row.id === raced.messageId)) {
      expect(error).toMatchObject({ code: "FABRIC_TARGET_TERMINAL", afterReceipt: true });
    }
    expect(events.find(row => row.type === "ack-observed" && row.id === raced.messageId)).toMatchObject({ state: "refused", afterReceipt: true });
    expect(events.some(row => ["delivery", "action"].includes(row.type) && row.id === raced.messageId)).toBe(false);
    expect(fs.existsSync(path.join(s.workspace, "SOURCE_WRITE_SENTINEL"))).toBe(false);
    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30_000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: "AUTHORIZED_FINAL_ANSWER" });
    await s.manager.close();
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(s.notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, finalAnswerReceiptId: receipt.id,
      messageId: raced.messageId, delivery: "steer", sender: provenance.sender }]);
    console.log(`E-steer-final child-gate: PASS root=${s.root} productionDiscardAck=true independentFixtureDiscard=false`);
  }, 45_000);

  it("preserves a prefinal native action as delivered when host final observation precedes the held control response", async () => {
    const s = setup();
    fs.writeFileSync(path.join(s.root, "hold-responses"), "");
    const handle = await s.manager.spawn({ task: "Return the authorized answer", model: "steer-ended/fake", thinking: "off",
      transport: "process", extensions: false, tools: [], cwd: s.workspace });
    const directory = path.join(s.root, "runs", handle.id);
    const fake = path.join(s.root, "fake.jsonl");
    await observed(s.root, () => rows(fake).some(row => row.type === "task"));
    const allowed = s.manager.steer(handle.id, "WRITE PREFINAL_WRITE_SENTINEL", undefined, provenance);
    await observed(s.root, () => rows(fake).some(row => row.type === "response-held" && row.id === allowed.messageId));
    expect(rows(fake).find(row => row.type === "action" && row.id === allowed.messageId)).toMatchObject({ afterReceipt: false });
    expect(fs.readFileSync(path.join(s.workspace, "PREFINAL_WRITE_SENTINEL"), "utf8")).toBe("WRITE PREFINAL_WRITE_SENTINEL");
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    fs.writeFileSync(path.join(s.root, "final"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "receipt"));
    fs.writeFileSync(path.join(s.root, "release"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30_000 });
    expect(result).toMatchObject({ status: "completed", turns: 1, text: "AUTHORIZED_FINAL_ANSWER" });
    expect(rows(fake).some(row => row.type === "response-released" && row.id === allowed.messageId)).toBe(false);
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(s.notices).toEqual([]);
    fs.writeFileSync(path.join(s.root, "release-responses"), "");
    await observed(s.root, () => rows(fake).some(row => row.type === "response-released" && row.id === allowed.messageId));
    await s.manager.close();
    expect(s.notices).toEqual([]); // NEVER undelivered + child action.
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(rows(path.join(directory, "lifecycle.jsonl")).some(row => row.event === "message.refused" && row.data.messageId === allowed.messageId)).toBe(false);
    expect(rows(fake).filter(row => row.type === "action" && row.id === allowed.messageId)).toHaveLength(1);
    expect(rows(fake).filter(row => row.type === "task")).toHaveLength(1);
    console.log(`E-steer-final prefinal-counterexample: PASS root=${s.root} nativeActionBeforeReceipt=true hostFinalBeforeControlResponse=true delivered=true undeliveredNotice=false`);
  }, 45_000);
});
