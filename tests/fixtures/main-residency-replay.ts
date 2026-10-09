// Component/integration coverage: a fresh Main controller plus the actual residency drainer.
// The Pi send/session boundary is synthetic; real CLI evidence is recorded separately.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { MainAgentController } from "../../src/main-agent.js";
import { ResidencyClient } from "../../src/residency/client.js";
import { MeshStore } from "../../src/mesh/store.js";
import { writeFileAtomic } from "../../src/core/atomic-write.js";
import type { ResidentHostConfig } from "../../src/residency/protocol.js";

const [configPath, key, mode, phase] = process.argv.slice(2) as [string, string, string, string];
const config = JSON.parse(fs.readFileSync(configPath, "utf8")) as ResidentHostConfig;
const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
const journal = path.join(config.meshRoot, "main-followups", "root.json");
fs.mkdirSync(path.dirname(journal), { recursive: true }); // Fail AFTER rename, never during mkdir.
const source = mesh.get(key)!;
const deliveryId = `resident:${config.rootId}:${(source.value as { id: string }).id}`;
const handlers = new Map<string, Array<(event: unknown, ctx: unknown) => void>>();
const sent: Array<{ details: unknown }> = [];
const entries: unknown[] = [];
const ctx = { isIdle: () => false, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } };
const main = new MainAgentController({
  on: (name: string, fn: (event: unknown, ctx: unknown) => void) => {
    handlers.set(name, [...(handlers.get(name) ?? []), fn]);
  },
  sendMessage: (message: { details: unknown }) => sent.push(message),
} as never, config.rootId, true, config.cwd, config.sessionId);
const emit = (name: string, event: unknown) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); };
const descriptors = new Map<number, string>();
const open = fs.openSync.bind(fs);
const sync = fs.fsyncSync.bind(fs);
let fail = true;
const events: string[] = [];
fs.openSync = ((file, flags, permissions) => {
  const fd = open(file, flags, permissions); descriptors.set(fd, String(file)); return fd;
}) as typeof fs.openSync;
fs.fsyncSync = (fd) => {
  if (descriptors.get(fd) === config.meshRoot) {
    events.push(fail ? "barrier-failed" : "barrier-confirmed");
    if (fail) throw new Error("post-rename replay barrier unavailable");
  }
  sync(fd);
};
const attempts: Array<{ acknowledged: boolean; error?: string }> = [];
const deliver = main.deliverAgent.bind(main);
main.deliverAgent = (request) => {
  try { const result = deliver(request); attempts.push({ acknowledged: true }); return result; }
  catch (error) { attempts.push({ acknowledged: false, error: String(error) }); throw error; }
};
const deletes: string[] = [];
const remove = mesh.delete.bind(mesh);
mesh.delete = async (request) => {
  if (request.key === key) { deletes.push(request.key); events.push("source-delete"); }
  return remove(request);
};
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("replay fixture timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
if (phase === "prepare" && mode === "consumed") {
  assert.throws(() => writeFileAtomic(`${journal}.delivered`, JSON.stringify({ version: 1, ids: [deliveryId] }), { durable: true }), /post-rename/);
  assert.ok(fs.readFileSync(`${journal}.delivered`, "utf8").includes(deliveryId));
  console.log(JSON.stringify({ pid: process.pid, sourceSurvives: !!mesh.get(key), visibleReceipt: true, events }));
} else {
  main.attachFollowUpDrain(ctx as never, 60_000, journal);
  const client = new ResidencyClient({ config, mesh, participants: {} as never, mainAgent: main });
  try {
    client.start();
    await waitFor(() => attempts.length >= 1 || attempts.some((attempt) => attempt.acknowledged));
    // A barrier failure retains the source but does not own an idle retry tick.
    // Exercise a second refusal through the public explicit recovery boundary.
    client.retryDeliveries();
    await waitFor(() => attempts.length >= 2 || attempts.some((attempt) => attempt.acknowledged));
    const refused = { sourceSurvives: !!mesh.get(key), acknowledgments: attempts.filter((a) => a.acknowledged).length, deletes: deletes.length };
    assert.ok(refused.sourceSurvives, "failed replay must retain resident source");
    assert.equal(refused.acknowledgments, 0, "failed replay must not acknowledge");
    assert.equal(refused.deletes, 0);
    assert.equal(sent.length, 0);
    if (phase === "prepare") {
      assert.ok(fs.readFileSync(journal, "utf8").includes(deliveryId), "rename left payload visible");
      console.log(JSON.stringify({ pid: process.pid, refused, attempts, events, visibleJournal: true }));
    } else {
      events.length = 0;
      fail = false;
      client.retryDeliveries(); // Repair admission, not elapsed time, owns replay.
      await waitFor(() => !mesh.get(key));
      assert.equal(attempts.filter((a) => a.acknowledged).length, 1);
      assert.equal(deletes.length, 1);
      assert.ok(events.indexOf("barrier-confirmed") >= 0);
      assert.ok(events.indexOf("source-delete") > events.indexOf("barrier-confirmed"));
      emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
      for (const message of sent) entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: message.details });
      emit("agent_settled", { outcome: "completed" });
      emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
      assert.equal(sent.length, mode === "consumed" ? 0 : 1);
      console.log(JSON.stringify({ pid: process.pid, refused, recovered: { acknowledgments: 1, deletes: 1, delivered: sent.length }, events }));
    }
  } finally { await client.close(); }
}
// Deliberately no graceful Main close in prepare: model an abrupt process exit,
// keeping the failed rename's visible journal/receipt rather than rewriting it.
