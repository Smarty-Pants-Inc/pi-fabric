// Safe transport fixture only: Node JS, no Fabric worker, no actor execution,
// no product deletion, no signals. Every path is supplied inside test TMPDIR.
import fs from "node:fs";
import path from "node:path";

const [root, hostId, mode = "success"] = process.argv.slice(2);
if (!root || !hostId) throw new Error("fixture requires a test-owned root and host ID");
const requests = path.join(root, "requests");
const responses = path.join(root, "responses");
const decisions = path.join(root, "decisions");
for (const directory of [requests, responses, decisions]) fs.mkdirSync(directory, { recursive: true });
const emit = value => process.stdout.write(`${JSON.stringify(value)}\n`);
const write = (file, value) => {
  const temporary = `${file}.fixture-tmp`;
  fs.writeFileSync(temporary, JSON.stringify(value));
  fs.renameSync(temporary, file);
};
const seen = new Set();
const status = { protocol: "slot-admission-v1", enforcement: "unenforced",
  reason: "existing-authority", gap: "smarty-dev#7991", fixtureHost: hostId };
function readRequests() {
  for (const file of fs.readdirSync(requests)) {
    if (!file.endsWith(".json") || seen.has(file)) continue;
    const command = JSON.parse(fs.readFileSync(path.join(requests, file), "utf8"));
    seen.add(file);
    emit({ event: "picked", command });
    const response = { format: 1, requestId: command.requestId, ok: true, completedAt: Date.now() };
    if (command.operation === "slotStatus") {
      write(path.join(responses, file), { ...response, slotStatus: status });
      continue;
    }
    if (mode === "cancel") continue; // Deliberately no decision file: absence is NOT rejection.
    if (mode === "forbidden") {
      write(path.join(responses, file), { ...response, ok: false, error: "Only the actual owning Main can create this slot",
        errorCode: "RESIDENT_ACTOR_FORBIDDEN" });
      continue;
    }
    if (mode === "refusal") {
      write(path.join(responses, file), { ...response, ok: false, error: "claim changed before admission",
        errorCode: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", slotError: { reason: "stale",
          claimKey: command.request.slotClaim?.key, claimVersion: command.request.slotClaim?.ifVersion } });
      continue;
    }
    const actorId = "fixture-original-actor";
    write(path.join(decisions, file), { requestId: command.requestId, state: "committed", operation: "createActor",
      id: actorId, ownerHostId: hostId });
    if (mode === "uncertain") {
      write(path.join(responses, file), { ...response, ok: false, error: "state sync deadline exceeded",
        errorCode: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN", slotError: { actorId,
          claimKey: command.request.slotClaim?.key, claimVersion: command.request.slotClaim?.ifVersion,
          requestKey: command.idempotencyKey } });
      continue;
    }
    write(path.join(responses, file), { ...response, actor: { id: actorId, name: command.request.name,
      residency: "durable", topics: command.request.topics ?? [] } });
  }
}
const watcher = fs.watch(requests, readRequests);
write(path.join(root, "owner.json"), { format: 1, pid: process.pid, hostId, token: "node-fixture",
  commands: ["createActor", "slotStatus"], requestFence: 1, creationIdempotency: 1,
  fabricExtensionPath: "unused-node-fixture", startedAt: Date.now(), readyAt: Date.now() });
const shutdown = () => { watcher.close(); clearTimeout(deadline); process.stdin.pause(); };
const deadline = setTimeout(() => { process.exitCode = 2; shutdown(); }, 10_000);
process.stdin.resume();
process.stdin.on("data", shutdown);
process.stdin.on("end", shutdown);
emit({ event: "ready" });
