// Native helpers for resident-attempt containment probes. Held helpers have a
// finite natural lifetime; tests may terminate only their recorded, birth-checked PID.
import fs from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(import.meta.url);
const [mode, ...args] = process.argv.slice(2);
const procFields = (pid) => {
  const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
};
const sessionId = (pid) => Number(procFields(pid)[3]);
const startTime = (pid) => procFields(pid)[19];
const cgroup = (pid = process.pid) => fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").split("\n").find(row => row.startsWith("0::"))?.slice(3);
const writeJson = (file, value) => fs.writeFileSync(file, JSON.stringify(value));
const startHeld = (receiptPath, durationMs) => {
  const child = spawn(process.execPath, [
    scriptPath, "hold", receiptPath, String(durationMs), String(process.pid), String(sessionId(process.pid)), String(startTime(process.pid)),
  ], { detached: true, stdio: "ignore" });
  child.once("error", error => { console.error(error); process.exitCode = 1; });
  child.once("spawn", () => child.unref());
};

// ProcessTransport worker mode: record the actual cgroup of the launched worker.
if (mode === "--containment-probe") {
  writeJson(args[0], { pid: process.pid, processStartTime: startTime(process.pid), cgroup: cgroup() });
} else if (mode === "hold") {
  const [receiptPath, durationText = "12000", parentPidText = "", parentSessionText = "", parentStartText = ""] = args;
  if (!receiptPath) throw new Error("hold mode requires a receipt path");
  const durationMs = Number(durationText);
  if (!Number.isSafeInteger(durationMs) || durationMs < 1 || durationMs > 30_000) throw new Error("invalid hold duration");
  writeJson(receiptPath, { pid: process.pid, parentPid: Number(parentPidText) || undefined,
    parentSession: Number(parentSessionText) || undefined, parentStartTime: parentStartText || undefined,
    processStartTime: startTime(process.pid), startedAt: Date.now() });
  setTimeout(() => process.exit(0), durationMs).unref();
  setInterval(() => {}, 1_000);
} else if (process.env.PI_RESCOPE_HELPER_MODE) {
  const helperMode = process.env.PI_RESCOPE_HELPER_MODE;
  const receiptPath = process.env.PI_RESCOPE_HELPER_RECEIPT;
  const configPath = process.env.PI_FABRIC_RESIDENT_CONFIG;
  let config;
  try { config = configPath ? JSON.parse(fs.readFileSync(configPath, "utf8")) : undefined; }
  catch (error) { if (helperMode === "serve") throw error; }
  if (!receiptPath) throw new Error("PI_RESCOPE_HELPER_RECEIPT is required");
  if (helperMode === "clean") {
    writeJson(receiptPath, { pid: process.pid, processStartTime: startTime(process.pid), cgroup: cgroup() });
  } else if (helperMode === "setsid-parent") {
    startHeld(receiptPath, Number(process.env.PI_RESCOPE_HELPER_HOLD_MS ?? "4200"));
  } else if (helperMode === "nested-process-transport") {
    const deadline = setTimeout(() => process.exit(124), 12_000);
    const progressPath = `${receiptPath}.progress`;
    try {
      writeJson(progressPath, { stage: "entered", pid: process.pid, cgroup: cgroup() });
      const moduleUrl = process.env.PI_RESCOPE_PROCESS_TRANSPORT_URL;
      if (!moduleUrl) throw new Error("PI_RESCOPE_PROCESS_TRANSPORT_URL is required");
      writeJson(progressPath, { stage: "importing", moduleUrl });
      const { ProcessTransport } = await import(moduleUrl);
      const childReceipt = `${receiptPath}.nested`;
      writeJson(progressPath, { stage: "launching-child" });
      const handle = await new ProcessTransport("rescope-outside.slice").launch({
        id: `attempt-child-${process.pid}`,
        name: "attempt-child",
        cwd: process.cwd(),
        workerPath: scriptPath,
        workerArguments: ["--containment-probe", childReceipt],
      });
      writeJson(progressPath, { stage: "child-launched", sessionId: handle.sessionId });
      await handle.waitForClose?.();
      writeJson(progressPath, { stage: "child-closed" });
      clearTimeout(deadline);
      writeJson(receiptPath, { pid: process.pid, processStartTime: startTime(process.pid), cgroup: cgroup(), nested: JSON.parse(fs.readFileSync(childReceipt, "utf8")) });
    } catch (error) {
      clearTimeout(deadline);
      writeJson(progressPath, { stage: "error", error: error instanceof Error ? error.stack ?? error.message : String(error) });
      writeJson(receiptPath, { error: error instanceof Error ? error.stack ?? error.message : String(error) });
      process.exitCode = 1;
    }
  } else if (helperMode === "serve") {
    if (!config?.residencyRoot) throw new Error("resident config is missing residencyRoot");
    const ownerPath = `${config.residencyRoot}/owner.json`;
    const owner = { format: 1, hostId: `resident:test-${process.pid}`, pid: process.pid,
      processStartTime: startTime(process.pid), token: `test-${process.pid}`, startedAt: Date.now(), readyAt: Date.now(),
      ...(process.env.PI_FABRIC_RESIDENT_ATTEMPT_ID && process.env.PI_FABRIC_RESIDENT_ATTEMPT_CGROUP
        ? { launchAttestation: { attemptId: process.env.PI_FABRIC_RESIDENT_ATTEMPT_ID,
          scope: process.env.PI_FABRIC_RESIDENT_ATTEMPT_CGROUP, launchedInScope: true } } : {}) };
    writeJson(ownerPath, owner);
    writeJson(receiptPath, { pid: process.pid, processStartTime: startTime(process.pid), cgroup: cgroup(), owner });
    process.on("SIGTERM", () => process.exit(0));
    process.on("SIGINT", () => process.exit(0));
    setInterval(() => {}, 1_000);
  } else {
    throw new Error(`unknown resident helper mode: ${helperMode}`);
  }
} else {
  // CLI mode is used for the unscoped parent/session counterprobe.
  const [receiptPath, durationText = "12000"] = args;
  if (mode !== "setsid-parent" || !receiptPath) throw new Error(`unknown helper mode: ${String(mode)}`);
  const durationMs = Number(durationText);
  if (!Number.isSafeInteger(durationMs) || durationMs < 1 || durationMs > 30_000) throw new Error("invalid hold duration");
  startHeld(receiptPath, durationMs);
}
