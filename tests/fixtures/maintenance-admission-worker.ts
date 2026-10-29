// Candidate-bundled Node fixture. The parent serially excludes each previous process by
// awaiting its actual SIGKILL/exit. Native/B6 plan references are boundary fixtures only.
import fs from "node:fs";
import path from "node:path";
import { bindInputAdmission, initializeNewRecoverySession, readRecoverySession, recoveryHash, type InputAdmissionMainAgent } from "../../src/recovery/input-admission.js";
import { createRecoveryMaintenanceAdapter, type MaintenanceCompletion, type MaintenanceRequest, type MaintenanceResult, type NativePlanRef } from "../../src/recovery/maintenance-admission.js";
const [root, stage, mode, owner] = process.argv.slice(2) as [string, string, string, string];
const api = createRecoveryMaintenanceAdapter();
const sessionId = "retained-session";
const epoch = { processIncarnationId: `process-${owner}-${process.pid}`, bindingId: `binding-${owner}-${process.pid}`, epoch: 0 };
const mainAgent: InputAdmissionMainAgent = { id: `session:${sessionId}`, local: true, sessionId, deliverAgent: () => { throw new Error("No driver prompt"); } };
const recovery = { epoch, podId: `pod-${owner}`, bootId: epoch.processIncarnationId,
  historyComplete: true, assertCurrent: () => {}, reconsiderInput: () => {} };
// Only this explicit first-ever fixture mode owns authoritative NEW intent.
// All recovery/lookup/adoption modes bind the retained ledger without initializing.
if (mode === "crash") initializeNewRecoverySession({ mode: "NEW", root, sessionId, recovery, assertNew() {} });
bindInputAdmission({ root, sessionId, mainAgent, recovery });
const take = <T>(r: MaintenanceResult<T>): T => { if (!r.ok) throw new Error(`${r.code}: ${r.message}`); return r.value; };
const key = { sessionId, requestId: "retained-maintenance" };
const requestFile = path.join(root, "request.json"), planFile = path.join(root, "native-plan.json"), callsFile = path.join(root, "summary-attempts.jsonl");
const durable = (file: string, bytes: string) => { const fd = fs.openSync(file, "w"); try { fs.writeSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); } };
const die = (): never => { process.kill(process.pid, "SIGKILL"); throw new Error("SIGKILL failed"); };
const plan = (summary: boolean): NativePlanRef => {
  if (summary && !fs.existsSync(planFile)) durable(planFile, JSON.stringify({ entryId: "native-prepare", summary: "NATIVE ONLY SUMMARY", parentId: "branch" }) + "\n");
  const bytes = summary ? fs.readFileSync(planFile, "utf8") : JSON.stringify({ entryId: "control-prepare", outcome: "failed", errorCode: "RECOVERY_MAINTENANCE_INTERRUPTED" }) + "\n";
  return { entryId: summary ? "native-prepare" : "control-prepare", bytesSha256: recoveryHash(bytes), planSha256: recoveryHash(`plan:${bytes}`) };
};
const completion = (p: NativePlanRef, summary: boolean): MaintenanceCompletion => ({ outcome: summary ? "completed" : "failed",
  ...(!summary ? { errorCode: "RECOVERY_MAINTENANCE_INTERRUPTED" } : {}), checkpointEntryId: "checkpoint",
  entries: [{ entryId: p.entryId, bytesSha256: p.bytesSha256, seq: 1 }, { entryId: "checkpoint", bytesSha256: recoveryHash("checkpoint\n"), seq: 2 }] });
if (mode === "crash") {
  const now = Date.now();
  const request: MaintenanceRequest = { version: 1, admissionKind: "maintenance", sessionId, requestId: key.requestId, epoch,
    operation: { kind: "compact", reason: "manual" }, baseLeafId: "branch", baseHistorySha256: recoveryHash("history"), viewSha256: recoveryHash("view"), issuedAtMs: now, deadlineMs: now + 60_000 };
  durable(requestFile, JSON.stringify(request));
  const admitted = take(await api.admitMaintenance(request));
  if (stage === "ADMITTED") die();
  const begun = take(await api.beginMaintenance(key, epoch, admitted.revision));
  if (stage === "ACTIVE" || stage === "ADOPTED") die();
  await api.withMaintenanceAttempt(begun.token, async () => {
    api.assertMaintenanceGate(begun.token, "compact-summary", request.viewSha256);
    fs.appendFileSync(callsFile, JSON.stringify({ owner, incarnation: epoch.processIncarnationId }) + "\n");
    take(await api.prepareMaintenance(begun.token, plan(true)));
  });
  if (stage === "PREPARED") die();
  take(await api.reconcileAfterJoin(key, epoch, { deadlineMs: Date.now() + 30_000, joined: Promise.resolve(),
    settle: async (_r, prepare) => { const p = plan(true); take(await prepare(p)); return completion(p, true); }, reconcile: async () => die(),
  }));
} else if (mode === "adopt-crash") {
  const snapshot = take(await api.read(key)); take(await api.adoptMaintenance(key, epoch, snapshot.revision)); die();
} else if (mode === "recover") {
  const snapshot = take(await api.read(key)); take(await api.adoptMaintenance(key, epoch, snapshot.revision));
  take(await api.reconcileAfterJoin(key, epoch, { deadlineMs: Date.now() + 30_000, joined: Promise.resolve(),
    settle: async (record, prepare) => { const summary = !!record.plan; const p = plan(summary); take(await prepare(p)); return completion(p, summary); }, reconcile: async () => {},
  }));
}
if (mode === "recover" || mode === "lookup") {
  const state = readRecoverySession(sessionId), record = state.maintenance[0]!.record;
  const attempts = fs.existsSync(callsFile) ? fs.readFileSync(callsFile, "utf8").trim().split("\n").map(line => JSON.parse(line) as { owner: string }) : [];
  process.stdout.write(JSON.stringify({ record, counts: { records: state.maintenance.length, nativePlans: Number(fs.existsSync(planFile)), provider: attempts.filter(a => a.owner === owner).length },
    deadline: (JSON.parse(fs.readFileSync(requestFile, "utf8")) as MaintenanceRequest).deadlineMs }) + "\n");
}
