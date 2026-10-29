// Candidate-bundled B5-only process. Parent awaits native SIGKILL exit before reopening.
// The sibling identity file is a TEST authority, not factory/M2/B6 implementation.
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../../src/core/atomic-write.js";
import { admitBoundScriptedInput, bindInputAdmission, initializeNewRecoverySession, readRecoverySession,
  RecoveryAdmissionError, recoveryHash, type InputAdmissionMainAgent } from "../../src/recovery/input-admission.js";
const [root, trace, fact, stage, mode] = process.argv.slice(2) as [string, string, string, string, string, string];
const sessionId = "seal-session", file = path.join(root, "input-admission", "v1", "session", `${recoveryHash(sessionId)}.json`);
const epoch = { processIncarnationId: `process-${process.pid}`, bindingId: `binding-${process.pid}`, epoch: 0 };
const recovery = { epoch, podId: "fixture-pod", bootId: epoch.processIncarnationId, historyComplete: true,
  assertCurrent() {}, reconsiderInput() {} };
const identity = () => JSON.parse(fs.readFileSync(fact, "utf8")) as { mode: string; sessionId: string };
const assertNew = () => {
  const current = identity();
  if (current.mode !== "NEW" || current.sessionId !== sessionId) throw new RecoveryAdmissionError("RECOVERY_CONFLICT", "Fixture authority does not positively assert NEW");
};
const recordAndDie = (extra: object = {}): never => {
  const fd = fs.openSync(trace, "a");
  try { fs.writeSync(fd, JSON.stringify({ stage, ack: false, required: identity().mode === "REQUIRED", ...extra }) + "\n"); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
  process.kill(process.pid, "SIGKILL"); throw new Error("SIGKILL failed");
};
const publishFixtureRequired = () => writeJsonAtomic(fact, { mode: "REQUIRED", sessionId }, { durable: true });
const seal = () => initializeNewRecoverySession({ mode: "NEW", root, sessionId, recovery, assertNew });
if (mode === "crash") {
  let published = false;
  const rename = fs.renameSync;
  fs.renameSync = (...args) => {
    if (String(args[1]) === file && stage === "before-publish") {
      // Atomic writer has already written/fsynced/closed the staged candidate file.
      recordAndDie({ stagedSha256: recoveryHash(fs.readFileSync(args[0], "utf8")), targetPresent: fs.existsSync(file) });
    }
    rename(...args);
    if (String(args[1]) === file) {
      published = true;
      if (stage === "after-rename") recordAndDie({ namespaceBarrierAcknowledged: false });
    }
  };
  initializeNewRecoverySession({ mode: "NEW", root, sessionId, recovery, assertNew() {
    assertNew();
    // The post-write trusted check is reached only after writeJsonAtomic's complete
    // file/namespace barrier and second namespace identity walk have returned.
    if (published && stage === "after-durable") recordAndDie({ namespaceBarrierAcknowledged: true });
  } });
  if (stage === "after-required") { publishFixtureRequired(); recordAndDie({ namespaceBarrierAcknowledged: true }); }
  throw new Error("Unreached seal cut");
}
if (mode === "finish-new") { seal(); publishFixtureRequired(); }
if (mode === "retry-new") {
  try { seal(); throw new Error("Unexpected second initialization"); }
  catch (error) { if (!(error instanceof RecoveryAdmissionError)) throw error; process.stdout.write(JSON.stringify({ code: error.code }) + "\n"); }
} else {
  let nativeDeliveries = 0;
  const mainAgent: InputAdmissionMainAgent = { id: `session:${sessionId}`, sessionId, local: true,
    deliverAgent(): never { nativeDeliveries++; throw new Error("Seal fixture must never deliver native work"); } };
  bindInputAdmission({ root, sessionId, mainAgent, recovery });
  try {
    const state = readRecoverySession(sessionId);
    if (mode === "admit") {
      // This is only a B5 fixture publication boundary; no B6 or manifest is claimed.
      publishFixtureRequired();
      const ack = admitBoundScriptedInput({ sessionId, request: { from: { id: "driver", name: "Driver", kind: "main" },
        message: "one legitimate input", delivery: "followUp", data: { scriptedInput: { version: 1, inputId: "original", sessionId, deadlineAt: Date.now() + 120_000 } } } });
      process.stdout.write(JSON.stringify({ ack, state: readRecoverySession(sessionId), nativeDeliveries }) + "\n");
    } else process.stdout.write(JSON.stringify(state) + "\n");
  } catch (error) {
    if (!(error instanceof RecoveryAdmissionError)) throw error;
    process.stdout.write(JSON.stringify({ code: error.code }) + "\n");
  }
}
