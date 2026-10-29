import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { main } from "../src/seat-drain/seam.js";
import { createSeat, installedSeam, runSeam, SID, type Seat } from "./seat-drain-fixture.js";

// Keep reason (unit + installed integration): recorded real incident R1 B06 a6.
// RUN1-a9 archive sha256: 37ac3322b609d2ddf319b66aa98eb3eca84a25aa0411c6c9556223a1955122a9.
// Inspected locally with A's seat_cutover.main.py: OFF has no seams.fence and no receipts/fence.json;
// A writes done/fence.observed after done/drain, then calls the installed drain-recheck seam.
// All paths, identities, units, request/receipt hashes and observations below are synthetic. No archive is
// loaded at test time. These checks must be RED on the original fence-owner-unpinned implementation.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const seats: Seat[] = [];
afterEach(async () => { for (const target of seats.splice(0)) await target.close(); });
const seat = async (options: Parameters<typeof createSeat>[0] = { fenceSeam: null }): Promise<Seat> => {
  const target = await createSeat(options);
  seats.push(target);
  return target;
};
const sha = (bytes: string): string => createHash("sha256").update(bytes).digest("hex");
type RecordRow = Record<string, unknown>;
type Result = { code: number | null; stdout: string; stderr: string };
const append = (target: Seat, ...records: RecordRow[]): void => {
  fs.appendFileSync(path.join(target.runDir, "seat-journal.jsonl"), records.map(record => `${JSON.stringify(record)}\n`).join(""));
};
const storeReceipt = (target: Seat, phase: string, text: string): string => {
  fs.mkdirSync(path.join(target.runDir, "receipts"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(target.runDir, "receipts", `${phase}.json`), text, { mode: 0o600 });
  return sha(text);
};
const inProcess = async (target: Seat, phase: string, inputs: RecordRow = {}): Promise<Result> => {
  let stdout = ""; let stderr = "";
  const code = await main(["--run-dir", target.runDir, "--phase", phase], {
    stdin: () => Buffer.from(target.request(phase, inputs)),
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(target.lockFd()) },
  }, { deadlineMs: 3_000, undoBudgetMs: 3_000, sleep: async () => {} });
  return { code, stdout, stderr };
};
const offObserved = (changes: RecordRow = {}): RecordRow => ({
  mode: "off", writerGone: true, tombstoneSha256: "a1".repeat(32), stoppedUnit: "org-main-123456.scope", ...changes,
});
const offDone = (changes: RecordRow = {}): RecordRow => ({ event: "done", step: "fence", observed: offObserved(changes) });
const drainDone = (): RecordRow => ({ event: "done", step: "drain" });
const recordDrain = (target: Seat, text: string, done = true): string => {
  const digest = storeReceipt(target, "drain", text);
  append(target, { event: "step", step: "drain" }, { event: "drain-operation", operationId: target.operationId },
    { event: "seam-intent", phase: "drain", seam: "drain", requestSha256: sha(target.request("drain")) },
    { event: "seam", phase: "drain", seam: "drain", sha256: digest }, ...(done ? [drainDone()] : []));
  return digest;
};
const preparedDrain = async (target: Seat, done = true): Promise<string> => {
  const drained = await inProcess(target, "drain");
  expect(drained).toMatchObject({ code: 0, stderr: "" });
  expect(JSON.parse(drained.stdout)).toMatchObject({ result: "pass", observed: { sessionId: SID, inputsFenced: true } });
  return recordDrain(target, drained.stdout, done);
};
const fenceOff = (target: Seat, changes: RecordRow = {}): void => {
  append(target, { event: "writer-stop-intent", unit: "org-main-123456.scope" },
    { event: "writer-stop-result", returncode: 0, unit: "org-main-123456.scope" },
    { event: "step", step: "fence" }, offDone(changes));
};
const expectedRecheck = (target: Seat) => ({
  schema: "d3-seam-receipt-v1", run: target.run, phase: "drain-recheck", owner: "fabric-v2", result: "pass",
  observed: { sessionId: SID, complete: true, totals: { actors: 0, followUps: 0, queued: 0, inFlight: 0,
    settledWindows: 0, pendingDeliveryOutcomes: 0, childCompletionEntries: 0 }, actorsNotStopped: 0 },
});
const expectRecheckPass = (target: Seat, result: Result): void => {
  expect(result).toMatchObject({ code: 0, stderr: "" });
  expect(JSON.parse(result.stdout)).toEqual(expectedRecheck(target));
  const evidence = JSON.parse(fs.readFileSync(path.join(target.runDir, "fabric-v2-drain-recheck-outcome.json"), "utf8"));
  expect(evidence).toMatchObject({ result: "pass", operationId: target.operationId, observed: expectedRecheck(target).observed });
};
const expectGap = (target: Seat, result: Result, reason?: string): void => {
  expect(result).toMatchObject({ code: 2, stdout: "" });
  if (reason) expect(result.stderr).toBe(`GAP: ${reason} [owner: fabric-v2]\n`);
  else expect(result.stderr).toMatch(/^GAP: [^\n]+ \[owner: fabric-v2\]\n$/);
  // Bad fence lineage is rejected before recount evidence, not reported as a successful drain-recheck.
  expect(fs.existsSync(path.join(target.runDir, "fabric-v2-drain-recheck-intent.json"))).toBe(false);
  expect(fs.existsSync(path.join(target.runDir, "fabric-v2-drain-recheck-outcome.json"))).toBe(false);
};
const onReceipt = (target: Seat, changes: RecordRow = {}): string => JSON.stringify({
  schema: "d3-seam-receipt-v1", run: target.run, phase: "fence", owner: "synthetic-fence-owner", result: "pass",
  observed: { tombstone: true }, ...changes,
});

/** Anonymized RUN1-a9's complete journal sequence, including unrelated preflight/hold and trailing recheck intent. */
const recordRun1A9 = (target: Seat, drainText: string): void => {
  append(target, { event: "fence-mode", mode: "off" },
    { event: "step", step: "preflight" },
    { event: "seam-intent", phase: "preflight", seam: "preflight", requestSha256: "3".repeat(64) },
    { event: "seam", phase: "preflight", seam: "preflight", sha256: "4".repeat(64) },
    { event: "seam-intent", phase: "host-probe", seam: "hostProbe", scriptSha256: "5".repeat(64) },
    { event: "seam", phase: "host-probe", seam: "hostProbe", scriptSha256: "5".repeat(64), sha256: "6".repeat(64) },
    { event: "v1-identity", cgroupPath: "/synthetic/org.slice/org-main-123456.scope", hostProbeSha256: "6".repeat(64),
      pid: 123456, startTime: 12345678 },
    { event: "route-preflight", revision: 1, route: { to: "v1" }, routeDigest: "7".repeat(64) },
    { event: "done", step: "preflight", reservation: { label: "synthetic", name: "synthetic-reservation",
      requests: { cpu: "0.5", memory: "1073741824" }, uid: "00000000-0000-4000-8000-0000000000aa" } },
    { event: "step", step: "hold" },
    { event: "seam-intent", phase: "hold", seam: "hold", requestSha256: "8".repeat(64) },
    { event: "seam", phase: "hold", seam: "hold", sha256: "9".repeat(64) },
    { event: "done", step: "hold" });
  recordDrain(target, drainText);
  fenceOff(target);
  append(target, { event: "step", step: "drain-recheck" },
    { event: "seam-intent", phase: "drain-recheck", seam: "drain", requestSha256: sha(target.request("drain-recheck")) });
};

describe.skipIf(!linux)("R1 B06 a6: fence-OFF through A's real installed drain seam", () => {
  it("accepts absent seams.fence plus a valid LAST done/fence observation after done/drain", async () => {
    const target = await seat();
    const exe = await installedSeam(target.base);
    const drained = await runSeam(target, exe, "drain", target.request("drain"));
    expect(drained).toMatchObject({ code: 0, stderr: "" });
    expect(JSON.parse(drained.stdout)).toMatchObject({ result: "pass" });
    recordDrain(target, drained.stdout);
    fenceOff(target);
    await target.server.close(); // Recheck must not need the stopped v1 Main's channel.
    const packet = JSON.parse(fs.readFileSync(path.join(target.runDir, "seat-run.json"), "utf8"));
    expect(packet.seams).not.toHaveProperty("fence");
    expect(fs.existsSync(path.join(target.runDir, "receipts", "fence.json"))).toBe(false);
    const before = target.meshSnapshot();
    expectRecheckPass(target, await runSeam(target, exe, "drain-recheck", target.request("drain-recheck")));
    expect(target.meshSnapshot()).toBe(before);
  }, 60_000);

  it("replays the anonymized RUN1-a9 record shape, including later NON-fence drain-recheck activity", async () => {
    const target = await seat();
    const exe = await installedSeam(target.base);
    const drained = await runSeam(target, exe, "drain", target.request("drain"));
    expect(drained).toMatchObject({ code: 0, stderr: "" });
    expect(JSON.parse(drained.stdout)).toMatchObject({ result: "pass" });
    recordRun1A9(target, drained.stdout);
    await target.server.close();
    expectRecheckPass(target, await runSeam(target, exe, "drain-recheck", target.request("drain-recheck")));
    expect(fs.existsSync(path.join(target.runDir, "receipts", "fence.json"))).toBe(false);
  }, 60_000);
});

describe.skipIf(!linux)("R1 B06 a6: fence-OFF observation validation and journal order", () => {
  it("a fence-mode:off marker is not a done/fence observation: fence-off-observation-missing", async () => {
    const target = await seat();
    await preparedDrain(target);
    append(target, { event: "fence-mode", mode: "off" });
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-off-observation-missing");
  });

  it("no pin and neither kind of fence evidence is fence-off-observation-missing", async () => {
    const target = await seat();
    await preparedDrain(target);
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-off-observation-missing");
  });

  it.each([
    ["writerGone false", { writerGone: false }],
    ["writerGone string", { writerGone: "true" }],
    ["missing writerGone", { writerGone: undefined }],
    ["short sha", { tombstoneSha256: "a".repeat(63) }],
    ["long sha", { tombstoneSha256: "a".repeat(65) }],
    ["nonhex sha", { tombstoneSha256: "g".repeat(64) }],
    ["nonstring sha", { tombstoneSha256: 123 }],
    ["empty unit", { stoppedUnit: "" }],
    ["nonstring unit", { stoppedUnit: 123 }],
  ] satisfies Array<[string, RecordRow]>)("rejects %s as fence-off-observation-invalid", async (_label, changes) => {
    const target = await seat();
    await preparedDrain(target);
    fenceOff(target, changes);
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-off-observation-invalid");
  });

  it("an OFF done/fence BEFORE done/drain is invalid (file presence is not ordering proof)", async () => {
    const target = await seat();
    await preparedDrain(target, false);
    fenceOff(target);
    append(target, drainDone());
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-off-observation-invalid");
  });

  it("without done/drain, even a real passing drain outcome and valid OFF observation are invalid lineage", async () => {
    const target = await seat();
    await preparedDrain(target, false);
    fenceOff(target);
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-off-observation-invalid");
  });

  it.each([
    ["step fence", { event: "step", step: "fence" }],
    ["seam-intent fence", { event: "seam-intent", phase: "fence", seam: "fence", requestSha256: "b".repeat(64) }],
    ["seam fence", { event: "seam", phase: "fence", seam: "fence", sha256: "c".repeat(64) }],
    ["reconcile fence", { event: "reconcile", step: "fence" }],
    ["failed fence", { event: "failed", phase: "fence" }],
    ["retry fence", { event: "retry", seam: "fence" }],
    ["newer nonoff done/fence", { event: "done", step: "fence", observed: offObserved({ mode: "on" }) }],
    ["newer malformed done/fence", { event: "done", step: "fence", observed: offObserved({ writerGone: false }) }],
  ] satisfies Array<[string, RecordRow]>)("never falls back to an older OFF observation after %s", async (_label, activity) => {
    const target = await seat();
    await preparedDrain(target);
    fenceOff(target);
    append(target, activity);
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-off-observation-invalid");
  });

  it("a passing drain BEFORE OFF fence can be rechecked, but cannot be undone after that fence", async () => {
    const target = await seat();
    const drainReceiptSha256 = await preparedDrain(target);
    fenceOff(target);
    expectRecheckPass(target, await inProcess(target, "drain-recheck"));
    // There is still NO fence receipt file: undo must consult journal ordering, not only that filename.
    expect(fs.existsSync(path.join(target.runDir, "receipts", "fence.json"))).toBe(false);
    const result = await inProcess(target, "drain-undo", { attempt: 1, drainReceiptSha256 });
    expect(result).toEqual({ code: 2, stdout: "", stderr: "GAP: drain-pass-after-fence [owner: fabric-v2]\n" });
    expect(target.native.state).toMatchObject({ fenced: true, releases: 0 });
    expect(fs.existsSync(path.join(target.runDir, "fabric-v2-drain-undo-1-intent.json"))).toBe(false);
  });

  it.each([false, true])("blocks undo of a stored PASS with OFF fence evidence (done/drain after fence: %j)", async drainDoneAfterFence => {
    const target = await seat();
    const drainReceiptSha256 = await preparedDrain(target, !drainDoneAfterFence);
    fenceOff(target);
    if (drainDoneAfterFence) append(target, drainDone());
    // Both orders must block undo. Only the drain-before-fence order is valid for recheck.
    // In particular, a newer done/drain cannot erase the earlier writer stop / OFF fence.
    expect(fs.existsSync(path.join(target.runDir, "receipts", "fence.json"))).toBe(false);
    const result = await inProcess(target, "drain-undo", { attempt: 1, drainReceiptSha256 });
    expect(result).toEqual({ code: 2, stdout: "", stderr: "GAP: drain-pass-after-fence [owner: fabric-v2]\n" });
    expect(target.native.state).toMatchObject({ fenced: true, releases: 0 });
    expect(fs.existsSync(path.join(target.runDir, "fabric-v2-drain-undo-1-intent.json"))).toBe(false);
  });

  it("counterexample: a pass still BEFORE any fence observation remains undoable, even in fence-mode:off", async () => {
    const target = await seat();
    append(target, { event: "fence-mode", mode: "off" });
    const drainReceiptSha256 = await preparedDrain(target);
    const result = await inProcess(target, "drain-undo", { attempt: 1, drainReceiptSha256 });
    expect(result).toMatchObject({ code: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toMatchObject({ result: "pass", observed: { inputsFenced: false, admissionRestored: true } });
    expect(target.native.state).toMatchObject({ fenced: false, releases: 1 });
  });
});

describe.skipIf(!linux)("R1 B06 a6: fence mode is selected by pin PRESENCE, not a usable owner string", () => {
  it("a pinned fence seam cannot use an OFF journal observation instead of its ON receipt", async () => {
    const target = await seat({}); // Existing fixture default is the pinned fence-ON seam.
    await preparedDrain(target);
    fenceOff(target);
    expectGap(target, await inProcess(target, "drain-recheck"));
  });

  it("a pinned ON receipt plus an OFF observation is contradictory, not permission to pick either branch", async () => {
    const target = await seat({});
    await preparedDrain(target);
    storeReceipt(target, "fence", onReceipt(target));
    fenceOff(target);
    expectGap(target, await inProcess(target, "drain-recheck"));
  });

  it.each(["", null, 123])("an existing fence pin with invalid owner %j never falls back to OFF", async owner => {
    const target = await seat({ fenceSeam: { owner, path: "/synthetic/fence", sha256: "2".repeat(64) } });
    await preparedDrain(target);
    fenceOff(target);
    expectGap(target, await inProcess(target, "drain-recheck"));
  });

  it("keeps existing fence-ON receipt behavior: correct owner/run/pass works without OFF journal evidence", async () => {
    const target = await seat({});
    await preparedDrain(target);
    const digest = storeReceipt(target, "fence", onReceipt(target));
    append(target, { event: "step", step: "fence" }, { event: "seam", phase: "fence", seam: "fence", sha256: digest },
      { event: "done", step: "fence" });
    expectRecheckPass(target, await inProcess(target, "drain-recheck"));
  });

  it.each([
    ["wrong owner", { owner: "another-fence-owner" }],
    ["wrong run", { run: "f".repeat(32) }],
    ["refused result", { result: "refused" }],
  ] satisfies Array<[string, RecordRow]>)("keeps fence-ON %s receipt rejection", async (_label, changes) => {
    const target = await seat({});
    await preparedDrain(target);
    storeReceipt(target, "fence", onReceipt(target, changes));
    expectGap(target, await inProcess(target, "drain-recheck"), "fence-receipt-invalid");
  });
});
