import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { currentBinding } from "../src/main-drain/binding.js";
import { assertMainInboxOpen, mainInboxDrainGate } from "../src/main-drain/gate.js";
import { drainSlot } from "../src/main-drain/slot.js";
import { main } from "../src/seat-drain/seam.js";
import { createSeat, installedSeam, ROOT, runSeam, SID, type Seat } from "./seat-drain-fixture.js";

// D3 drain seam (smarty-dev#15310 prep item 13): DRAIN-RESULT.md "Tests to write", challenge r1/r2 (N1-N9) and
// DRAIN-PHASE-CONTRACT.md rev 4, against a synthetic sealed RUN, a synthetic mesh and a live drain channel.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
afterEach(async () => { for (const seat of seats.splice(0)) await seat.close(); });
const seat = async (options: Parameters<typeof createSeat>[0] = {}): Promise<Seat> => { const created = await createSeat(options); seats.push(created); return created; };
const sha = (text: string): string => createHash("sha256").update(text).digest("hex");
const ENVELOPE = ["observed", "owner", "phase", "result", "run", "schema"];
const DRAIN_KEYS = ["admitted", "childrenTerminal", "inputAdmission", "inputsFenced", "queued", "sessionId"];

/** The seam in-process, with a real held seat lock and a short, injectable clock. */
const inProcess = async (target: Seat, phase: string, inputs: Record<string, unknown> = {}, deps: Parameters<typeof main>[2] = {},
  request?: string) => {
  let stdout = ""; let stderr = "";
  const code = await main(["--run-dir", target.runDir, "--phase", phase], {
    stdin: () => Buffer.from(request ?? target.request(phase, inputs)),
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(target.lockFd()) },
  }, { deadlineMs: 3_000, undoBudgetMs: 3_000, ...deps });
  return { code, stdout, stderr, receipt: stdout ? JSON.parse(stdout) as Record<string, any> : undefined };
};
const outcome = (target: Seat, name = "fabric-v2-drain-outcome.json") => JSON.parse(fs.readFileSync(path.join(target.runDir, name), "utf8"));
const actorRow = (id: string, status: string, extra: Record<string, unknown> = {}) => ({ id, name: `actor-${id.slice(0, 4)}`, rootId: ROOT, status, ...extra });
const storeReceipt = (target: Seat, phase: string, text: string): string => {
  fs.mkdirSync(path.join(target.runDir, "receipts"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(target.runDir, "receipts", `${phase}.json`), text, { mode: 0o600 });
  return sha(text);
};
const undoInputs = (attempt: number, drainReceiptSha256: string | null) => ({ attempt, drainReceiptSha256 });

describe.skipIf(!linux)("fabric-seat-drain, the real installed artifact run as A runs it", () => {
  it("drains an idle Main to exactly the six passing keys, writes evidence once and changes no mesh file", async () => {
    const target = await seat();
    const exe = await installedSeam(target.base);
    const before = target.meshSnapshot();
    const result = await runSeam(target, exe, "drain", target.request("drain"));
    expect(result.stderr).toBe("");
    expect(result.code).toBe(0);
    const receipt = JSON.parse(result.stdout);
    expect(Object.keys(receipt).sort()).toEqual(ENVELOPE);
    expect(receipt).toEqual({ schema: "d3-seam-receipt-v1", run: target.run, phase: "drain", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, inputAdmission: 1, inputsFenced: true, queued: 0, admitted: 0, childrenTerminal: true } });
    expect(Object.keys(receipt.observed).sort()).toEqual(DRAIN_KEYS);
    expect(target.native.state.fenced).toBe(true);
    for (const name of ["fabric-v2-drain-intent.json", "fabric-v2-drain-outcome.json"]) {
      expect(fs.statSync(path.join(target.runDir, name)).mode & 0o7777).toBe(0o600);
    }
    expect(outcome(target).result).toBe("pass");
    expect(outcome(target, "fabric-v2-drain-intent.json").measured.interpreter.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(target.meshSnapshot()).toBe(before);
  }, 60_000);

  it("on today's Pi (no extension fence API) refuses fence-unavailable at once and leaves v1 admitting", async () => {
    const target = await seat({ fenceApi: false });
    const exe = await installedSeam(target.base);
    const result = await runSeam(target, exe, "drain", target.request("drain"));
    expect(result).toMatchObject({ code: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toEqual({ schema: "d3-seam-receipt-v1", run: target.run, phase: "drain", owner: "fabric-v2",
      result: "refused", observed: { sessionId: SID, reason: "fence-unavailable", inputsFenced: false } });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
    expect(currentBinding(ROOT)).toBeUndefined();
    expect(outcome(target)).toMatchObject({ result: "refused", reason: "fence-unavailable", undo: { admissionRestored: true } });
  }, 60_000);

  it("without the seat lock prints NO receipt, exits 2, writes nothing under RUN and never contacts the Main", async () => {
    const target = await seat();
    const exe = await installedSeam(target.base);
    const before = fs.readdirSync(target.runDir).sort();
    const result = await runSeam(target, exe, "drain", target.request("drain"), true);
    expect(result).toMatchObject({ code: 2, stdout: "" });
    expect(result.stderr).toMatch(/^GAP: lock-not-held \[owner: fabric-v2\]\n$/);
    expect(fs.readdirSync(target.runDir).sort()).toEqual(before);
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  }, 60_000);

  it("the uninstalled bundle refuses to run: the interpreter pin is written only at install", async () => {
    const target = await seat();
    const exe = await installedSeam(target.base);
    const raw = path.join(target.base, "raw.cjs");
    fs.writeFileSync(raw, fs.readFileSync(exe, "utf8").replace(/^#!.*\n/, "").replace(/"[0-9a-f]{64}"/, '"__FABRIC_SEAT_DRAIN_INTERPRETER_SHA256__"'));
    const result = spawnSync("node", [raw, "--run-dir", target.runDir, "--phase", "drain"], { input: target.request("drain"), encoding: "utf8",
      env: { PATH: process.env.PATH, LANG: "C", LC_ALL: "C" } });
    expect(result.status).toBe(2);
    expect(result.stderr).toBe("GAP: seam-not-installed [owner: fabric-v2]\n");
  }, 60_000);
});

describe.skipIf(!linux)("drain counts (in-process seam, real lock and channel)", () => {
  it("an absent follow-up journal counts 0", async () => {
    const target = await seat();
    expect((await inProcess(target, "drain")).receipt).toMatchObject({ result: "pass", observed: { queued: 0, admitted: 0 } });
  });

  it("a held follow-up gives queued > 0; it is waited for, never deleted, and the deadline returns the counts", async () => {
    const target = await seat();
    target.writeJournal([{ id: "held-1" }]);
    const before = target.meshSnapshot();
    const started = Date.now();
    const result = await inProcess(target, "drain", {}, { deadlineMs: 2_500 });
    expect(Date.now() - started).toBeLessThan(15_000);
    expect(result.receipt).toMatchObject({ result: "refused", observed: { sessionId: SID, reason: "pending-timeout", inputsFenced: false } });
    expect(outcome(target).wouldBeObserved.queued).toBeGreaterThan(0);
    expect(target.meshSnapshot()).toBe(before);
  });

  it("a handed:true follow-up gives admitted > 0", async () => {
    const target = await seat();
    target.writeJournal([{ id: "handed-1", handed: true }]);
    await inProcess(target, "drain", {}, { deadlineMs: 2_000 });
    expect(outcome(target).wouldBeObserved.admitted).toBeGreaterThan(0);
  });

  it("an unfinished reroute of this root counts in queued", async () => {
    const target = await seat();
    const routes = path.join(target.meshRoot, "main-followups", "routes");
    fs.mkdirSync(routes, { recursive: true });
    fs.writeFileSync(path.join(routes, `${"a".repeat(64)}.json`), JSON.stringify({ oldRoot: ROOT, messageId: "m-1" }));
    await inProcess(target, "drain", {}, { deadlineMs: 2_000 });
    expect(outcome(target)).toMatchObject({ reason: "pending-timeout", measured: { unfinishedRoutes: 1 } });
  });

  it("a corrupt journal is not drained: inventory-incomplete", async () => {
    const target = await seat();
    fs.writeFileSync(path.join(target.meshRoot, "main-followups", `${SID}.json`), "{not json", { mode: 0o600 });
    expect((await inProcess(target, "drain", {}, { deadlineMs: 2_000 })).receipt?.observed.reason).toBe("inventory-incomplete");
  });

  it("a halted Main holding a follow-up is reported at once, not after the deadline", async () => {
    const target = await seat();
    target.fabric.held = 1; target.fabric.halted = true;
    const started = Date.now();
    const result = await inProcess(target, "drain", {}, { deadlineMs: 600_000 });
    expect(Date.now() - started).toBeLessThan(15_000);
    expect(result.receipt?.observed.reason).toBe("pending-timeout");
  });

  it("a server that is not the inbox owner (owner.json pid differs) is never trusted: main-unreachable", async () => {
    const target = await seat();
    const owner = path.join(target.meshRoot, "main-followups", `${SID}.owner.json`);
    fs.writeFileSync(owner, JSON.stringify({ ...JSON.parse(fs.readFileSync(owner, "utf8")), pid: 1 }));
    const result = await inProcess(target, "drain", {}, { deadlineMs: 2_000 });
    expect(result.receipt?.observed).toEqual({ sessionId: SID, reason: "main-unreachable", inputsFenced: false });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
  });
});

describe.skipIf(!linux)("children (B2/B3/B4, r2 N1/N2)", () => {
  it("a live session actor that is not quiesced keeps childrenTerminal false and is never stopped", async () => {
    const target = await seat();
    const id = "a".repeat(32);
    target.writeActors([actorRow(id, "idle")]);
    target.actors.listOwned = () => [{ id, name: "live", status: "idle", rootId: ROOT }];
    target.actors.drainQuiescence = () => ["queued"];
    target.actors.drainWorker = () => null;
    expect((await inProcess(target, "drain", {}, { deadlineMs: 2_000 })).receipt?.observed.reason).toBe("children-live");
    expect(target.actors.stops).toEqual([]);
  });

  it("a stopped session actor whose worker never launched is terminal", async () => {
    const target = await seat();
    const id = "b".repeat(32);
    target.writeActors([actorRow(id, "stopped", { stopReason: "explicit" })]);
    target.actors.listOwned = () => [{ id, name: "done", status: "stopped", rootId: ROOT }];
    target.actors.drainQuiescence = () => ["status-stopped"];
    target.actors.drainWorker = () => null;
    expect((await inProcess(target, "drain")).receipt).toMatchObject({ result: "pass", observed: { childrenTerminal: true } });
  });

  it("a stopped actor whose recorded worker still runs (pid + start time) is NOT terminal", async () => {
    const target = await seat();
    const id = "c".repeat(32);
    const start = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8").split(") ")[1]!.split(" ")[19]!;
    target.writeActors([actorRow(id, "stopped")]);
    target.actors.listOwned = () => [{ id, name: "ghost", status: "stopped", rootId: ROOT }];
    target.actors.drainQuiescence = () => [];
    target.actors.drainWorker = () => ({ runId: "run-1", pid: process.pid, startTime: start });
    expect((await inProcess(target, "drain", {}, { deadlineMs: 2_000 })).receipt?.observed.reason).toBe("children-live");
    expect(outcome(target).reasons).toContain(`children:worker-alive:${id}`);
  });

  it("N2: a quiesced session actor is stopped only as the LAST act (after the fence), and its spec is recorded", async () => {
    const target = await seat();
    const id = "d".repeat(32);
    const row = { status: "idle" };
    const fencedAtStop: boolean[] = [];
    target.actors.listOwned = () => [{ id, name: "quiet", status: row.status, rootId: ROOT }];
    target.actors.drainQuiescence = () => row.status === "idle" ? [] : ["status-stopped"];
    target.actors.drainWorker = () => null;
    target.actors.stop = async () => {
      fencedAtStop.push(target.native.state.fenced);
      target.actors.stops.push(id); row.status = "stopped"; target.writeActors([actorRow(id, "stopped", { stopReason: "explicit" })]); return {};
    };
    target.writeActors([actorRow(id, "idle")]);
    const result = await inProcess(target, "drain", {}, { deadlineMs: 8_000 });
    expect(target.actors.stops).toEqual([id]);
    expect(fencedAtStop).toEqual([true]);
    expect(result.receipt).toMatchObject({ result: "pass", observed: { childrenTerminal: true } });
    expect(outcome(target).stoppedActors).toEqual([expect.objectContaining({ id, spec: expect.objectContaining({ id, name: "quiet" }) })]);
  });

  it("N2: a quiesced actor beside a Main that stays busy is NOT stopped; the refusal leaves it active", async () => {
    const target = await seat();
    const id = "9".repeat(32);
    target.native.state.idle = false;
    target.writeActors([actorRow(id, "idle")]);
    target.actors.listOwned = () => [{ id, name: "keep", status: "idle", rootId: ROOT }];
    target.actors.drainQuiescence = () => [];
    target.actors.drainWorker = () => null;
    const result = await inProcess(target, "drain", {}, { deadlineMs: 2_500 });
    // DRAIN-DESIGN.r1 §1.10's order: children-live before pending-timeout.
    expect(result.receipt?.observed).toEqual({ sessionId: SID, reason: "children-live", inputsFenced: false });
    expect(target.actors.stops).toEqual([]);
  });

  it("a durable actor refuses durable-actor-present BEFORE begin: no intent, no gate, no stop", async () => {
    const target = await seat();
    const id = "e".repeat(32);
    target.writeActors([actorRow(id, "idle", { residency: "durable" })]);
    const result = await inProcess(target, "drain", {}, { deadlineMs: 600_000 });
    expect(result.receipt?.observed).toEqual({ sessionId: SID, reason: "durable-actor-present", inputsFenced: false });
    expect(fs.existsSync(path.join(target.runDir, "fabric-v2-drain-intent.json"))).toBe(false);
    expect(currentBinding(ROOT)).toBeUndefined();
  });

  it("a running agent run or an unconsumed completion keeps childrenTerminal false", async () => {
    const target = await seat();
    target.agents.push({ id: "f".repeat(32), status: "running" });
    expect((await inProcess(target, "drain", {}, { deadlineMs: 2_000 })).receipt?.observed.reason).toBe("children-live");
    const other = await seat();
    fs.mkdirSync(path.join(other.meshRoot, "agent-completions"), { recursive: true });
    fs.writeFileSync(path.join(other.meshRoot, "agent-completions", "x.json"), JSON.stringify({ format: 1, recipient: { rootId: ROOT } }));
    expect((await inProcess(other, "drain", {}, { deadlineMs: 2_000 })).receipt?.observed.reason).toBe("children-live");
  });

  it("N1/N8: the drain's own child completion passes the gate before the fence; outside input is refused into the ledger", async () => {
    const target = await seat();
    const runId = "7".repeat(32);
    target.agents.push({ id: runId, status: "running" });
    let first = true;
    const verdicts: string[] = [];
    const sleep = async (ms: number): Promise<void> => {
      if (first) {
        first = false;
        try { assertMainInboxOpen(ROOT, { path: "completion.journal", from: { id: runId, kind: "agent" }, deliveryId: `agent-completion:${runId}` }); verdicts.push("child-admitted"); }
        catch { verdicts.push("child-refused"); }
        try { assertMainInboxOpen(ROOT, { path: "fabric-inbox", from: { id: "session:00000000-0000-4000-8000-0000000000cc", kind: "main" }, deliveryId: "cmd-1", text: "hello" }); verdicts.push("outside-admitted"); }
        catch (error) { verdicts.push((error as { code?: string }).code ?? "error"); }
        target.agents[0]!.status = "completed";
      }
      await new Promise(resolve => setTimeout(resolve, Math.min(ms, 50)));
    };
    const result = await inProcess(target, "drain", {}, { deadlineMs: 8_000, sleep });
    expect(verdicts).toEqual(["child-admitted", "FABRIC_MAIN_DRAINING"]);
    expect(result.receipt?.result).toBe("pass");
    expect(outcome(target).admitted).toEqual([expect.objectContaining({ id: `agent-completion:${runId}`, path: "completion.journal" })]);
    expect(outcome(target).ledger).toEqual([expect.objectContaining({ id: "cmd-1", fromId: "session:00000000-0000-4000-8000-0000000000cc",
      textSha256: sha("hello") })]);
  });
});

describe.skipIf(!linux)("evidence, binding and undo (contract rev 4)", () => {
  it("writes its evidence once: a second drain on the same RUN is UNKNOWN (exit 2, no receipt)", async () => {
    const target = await seat();
    expect((await inProcess(target, "drain")).code).toBe(0);
    const again = await inProcess(target, "drain");
    expect(again).toMatchObject({ code: 2, stdout: "" });
    expect(again.stderr).toBe("GAP: evidence-write-failed [owner: fabric-v2]\n");
  });

  it("a request binding failure (runSha256, or a missing operationId) is UNKNOWN: no receipt, nothing written", async () => {
    const target = await seat();
    const wrong = await inProcess(target, "drain", {}, {}, target.request("drain").replace(target.runSha256, "f".repeat(64)));
    expect(wrong).toMatchObject({ code: 2, stdout: "", stderr: "GAP: seat-run-sha256-mismatch [owner: fabric-v2]\n" });
    const request = JSON.parse(target.request("drain"));
    delete request.inputs.operationId;
    const missing = await inProcess(target, "drain", {}, {}, JSON.stringify(request));
    expect(missing).toMatchObject({ code: 2, stdout: "", stderr: "GAP: operation-id-invalid [owner: fabric-v2]\n" });
    expect(fs.existsSync(path.join(target.runDir, "fabric-v2-drain-intent.json"))).toBe(false);
  });

  it("drain-undo with A's refused receipt restores admission; idempotent per attempt; a wrong sha is UNKNOWN", async () => {
    const target = await seat();
    target.writeJournal([{ id: "held-1" }]);
    const refused = await inProcess(target, "drain", {}, { deadlineMs: 1_500 });
    const digest = storeReceipt(target, "drain", refused.stdout);
    const first = await inProcess(target, "drain-undo", undoInputs(1, digest));
    expect(first.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: target.run, phase: "drain-undo", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, inputsFenced: false, admissionRestored: true } });
    expect((await inProcess(target, "drain-undo", undoInputs(1, digest))).stdout).toBe(first.stdout);
    expect((await inProcess(target, "drain-undo", undoInputs(2, digest))).receipt?.result).toBe("pass");
    expect(await inProcess(target, "drain-undo", undoInputs(3, "0".repeat(64)))).toMatchObject({ code: 2, stdout: "" });
  });

  it("N6 iii: a PASS that A rejected is undone while receipts/fence.json is absent, never after", async () => {
    const target = await seat();
    const passed = await inProcess(target, "drain");
    expect(target.native.state.fenced).toBe(true);
    const digest = storeReceipt(target, "drain", passed.stdout);
    const undo = await inProcess(target, "drain-undo", undoInputs(1, digest));
    expect(undo.receipt).toMatchObject({ result: "pass", observed: { inputsFenced: false, admissionRestored: true } });
    expect(target.native.state).toMatchObject({ fenced: false, releases: 1 });
    storeReceipt(target, "fence", "{}");
    expect(await inProcess(target, "drain-undo", undoInputs(2, digest))).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: drain-pass-after-fence [owner: fabric-v2]\n" });
  });

  it("the operator path (null, no stored receipt) releases through the kept handle across a reload", async () => {
    const target = await seat();
    expect((await inProcess(target, "drain")).receipt?.result).toBe("pass");
    expect(target.native.state.fenced).toBe(true);
    // A reload: this generation's listener closes; a new generation with a new ctx object listens again.
    // Pi keeps the fence, and the process-global slot keeps its original handle.
    await target.server.close();
    const { MainDrainControl } = await import("../src/main-drain/control.js");
    const { MainDrainServer } = await import("../src/main-drain/server.js");
    const reloaded = new MainDrainServer(new MainDrainControl({ ...target.host, context: () => target.native.ctx() }));
    expect(await reloaded.start()).toBe("listening");
    target.server = reloaded;
    const undo = await inProcess(target, "drain-undo", undoInputs(1, null));
    expect(undo.receipt?.observed).toEqual({ sessionId: SID, inputsFenced: false, admissionRestored: true });
    expect(target.native.state).toMatchObject({ fenced: false, releases: 1 });
    expect(mainInboxDrainGate(ROOT)).toBeUndefined();
    storeReceipt(target, "drain", "{}");
    expect(await inProcess(target, "drain-undo", undoInputs(2, null))).toMatchObject({ code: 2, stdout: "" });
  });

  it("a LOST fence handle is refused {inputsFenced:true, admissionRestored:false}: restart required", async () => {
    const target = await seat();
    expect((await inProcess(target, "drain")).receipt?.result).toBe("pass");
    delete drainSlot(ROOT).fence;
    const undo = await inProcess(target, "drain-undo", undoInputs(1, null));
    expect(undo.receipt).toMatchObject({ result: "refused", observed: { sessionId: SID, inputsFenced: true, admissionRestored: false } });
  });

  it("N6 ii: another operation's fence is UNKNOWN (exit 2) and stays held", async () => {
    const target = await seat();
    expect((await inProcess(target, "drain")).receipt?.result).toBe("pass");
    drainSlot(ROOT).binding!.generation = "f".repeat(32);
    expect(await inProcess(target, "drain-undo", undoInputs(1, null))).toMatchObject({ code: 2, stdout: "" });
    expect(target.native.state.fenced).toBe(true);
  });

  it("N3: a drain refused before its intent passes drain-undo without contacting the Main", async () => {
    const target = await seat();
    target.writeActors([actorRow("e".repeat(32), "idle", { residency: "durable" })]);
    const refused = await inProcess(target, "drain");
    const digest = storeReceipt(target, "drain", refused.stdout);
    await target.server.close();
    const undo = await inProcess(target, "drain-undo", undoInputs(1, digest));
    expect(undo.receipt).toMatchObject({ result: "pass", observed: { sessionId: SID, inputsFenced: false, admissionRestored: true } });
    expect(outcome(target, "fabric-v2-drain-undo-1-outcome.json")).toMatchObject({ neverFenced: true, basis: "measured" });
  });

  it("drain-recheck: exact keys; N4 counts only explicit/directive stops in the drain's dispositions as stopped", async () => {
    const target = await seat();
    const done = "1".repeat(32);
    target.writeActors([actorRow(done, "stopped", { stopReason: "explicit" })]);
    target.actors.listOwned = () => [{ id: done, name: "done", status: "stopped", rootId: ROOT }];
    target.actors.drainQuiescence = () => ["status-stopped"];
    target.actors.drainWorker = () => null;
    expect((await inProcess(target, "drain")).receipt?.result).toBe("pass");
    // v1 crash-restarted between drain and fence: a new actor, retired as host-exit, revives on v2.
    target.writeActors([actorRow(done, "stopped", { stopReason: "explicit" }), actorRow("2".repeat(32), "stopped", { stopReason: "host-exit" })]);
    const result = await inProcess(target, "drain-recheck");
    expect(Object.keys(result.receipt!.observed).sort()).toEqual(["actorsNotStopped", "complete", "sessionId", "totals"]);
    expect(Object.keys(result.receipt!.observed.totals).sort()).toEqual(
      ["actors", "childCompletionEntries", "followUps", "inFlight", "pendingDeliveryOutcomes", "queued", "settledWindows"]);
    expect(result.receipt).toMatchObject({ result: "refused", observed: { sessionId: SID, complete: true, actorsNotStopped: 1, totals: { actors: 2, followUps: 0 } } });
  });

  it("drain-recheck passes truthfully only with the passing values", async () => {
    const target = await seat();
    expect((await inProcess(target, "drain")).receipt?.result).toBe("pass");
    const result = await inProcess(target, "drain-recheck");
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: target.run, phase: "drain-recheck", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, complete: true, totals: { actors: 0, followUps: 0, queued: 0, inFlight: 0, settledWindows: 0,
        pendingDeliveryOutcomes: 0, childCompletionEntries: 0 }, actorsNotStopped: 0 } });
  });

  it("resume-drain without a pinned kube packet is UNKNOWN (no receipt); the kubectl path is in seat-drain-resume.test.ts", async () => {
    const target = await seat();
    const result = await inProcess(target, "resume-drain", { pod: { name: "raw", uid: "00000000-0000-4000-8000-0000000000bb" } });
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: kube-unpinned [owner: fabric-v2]\n" });
  });
});
