// Synthetic N7 callback only, not smarty-inbox application/idempotency proof.
import fs from "node:fs";
const pins = JSON.parse(fs.readFileSync(process.env.FABRIC_NATIVE_OWNER_PINS, "utf8"));
fs.appendFileSync(pins.observations, JSON.stringify({ event: "module-loaded", pid: process.pid,
  resident: !!process.env.PI_FABRIC_RESIDENT_CONFIG }) + "\n", { mode: 0o600 });
export const handlerKey = "native-residency.test.v1";
export const handle = async (request, ctx) => {
  if (request.message !== handlerKey) throw new Error("Unsupported native fixture message");
  fs.appendFileSync(pins.callbacks, JSON.stringify({ actorId: ctx.actorId, pid: process.pid,
    sender: ctx.sender.id, sourceMachine: ctx.sourceMachine, commandId: ctx.commandId }) + "\n", { mode: 0o600 });
  // Fixture modes support the parent's separate native custody/stall proof, not a timer change.
  if (request.data?.mode === "stall") {
    // N5 worst phase: align to the REAL lease just before the unchanged 5s renewal.
    // Waits below are expiry-boundary scheduling, not replacement heartbeat intervals.
    const leaseFile = request.data.phaseLeaseFile;
    if (typeof leaseFile !== "string" || !leaseFile.startsWith(pins.meshRoot + "/")) throw new Error("Fixture lease pin missing");
    const deadline = Date.now() + 30_000;
    let leaseAgeMs;
    while (Date.now() < deadline) {
      const lease = JSON.parse(fs.readFileSync(leaseFile, "utf8"));
      await new Promise(resolve => setTimeout(resolve, Math.max(0, lease.updatedAt + 4_800 - Date.now())));
      const current = JSON.parse(fs.readFileSync(leaseFile, "utf8"));
      leaseAgeMs = Date.now() - current.updatedAt;
      if (leaseAgeMs >= 4_700 && leaseAgeMs < 4_950) break;
      if (current.updatedAt === lease.updatedAt && leaseAgeMs >= 4_950) throw new Error("Missed native fixture stall phase");
    }
    if (!(leaseAgeMs >= 4_700 && leaseAgeMs < 4_950)) throw new Error("Native fixture stall phase unavailable");
    fs.appendFileSync(pins.observations, JSON.stringify({ event: "stall-enter", at: Date.now(), leaseAgeMs }) + "\n", { mode: 0o600 });
    const until = Date.now() + 11_000;
    while (Date.now() < until) { /* actual event-loop stall: freshness loss is EXPECTED */ }
    fs.appendFileSync(pins.observations, JSON.stringify({ event: "stall-settled", at: Date.now() }) + "\n", { mode: 0o600 });
  }
  if (request.data?.mode === "async-wait") {
    await new Promise((resolve, reject) => {
      const abort = () => { clearTimeout(timer); reject(new Error("Fixture wait aborted")); };
      const timer = setTimeout(() => { ctx.signal.removeEventListener("abort", abort); resolve(); }, 11_000);
      ctx.signal.addEventListener("abort", abort, { once: true });
      if (ctx.signal.aborted) abort();
    });
  }
  if (request.data?.mode === "ignore-abort") {
    // Parent's real-promise close/handover case: timeout/abort is NOT settlement.
    fs.appendFileSync(pins.observations, JSON.stringify({ event: "unsettled-enter", at: Date.now() }) + "\n", { mode: 0o600 });
    await new Promise(resolve => setTimeout(resolve, 11_000));
    fs.appendFileSync(pins.observations, JSON.stringify({ event: "unsettled-settled", at: Date.now(), aborted: ctx.signal.aborted }) + "\n", { mode: 0o600 });
  }
  return { data: { echo: request.data, sender: ctx.sender.id, actorId: ctx.actorId } };
};
