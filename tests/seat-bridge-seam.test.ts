import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import net from "node:net";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { procStartTime } from "../src/main-drain/binding.js";
import * as mainBinding from "../src/main-drain/binding.js";
import * as mainClient from "../src/main-drain/client.js";
import type { MainDrainReply } from "../src/main-drain/protocol.js";
import { readBridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { journalBranch } from "../src/seat-drain/journal-branch.js";
import * as kube from "../src/seat-bridge/kube.js";
import { SeamGap } from "../src/seat-drain/run.js";
import * as runBoundary from "../src/seat-drain/run.js";
import { redactNatsError } from "../src/mesh/state-backend-nats.js";
import { main, type BridgeDeps } from "../src/seat-bridge/seam.js";
import { podJournalArgv, podOwnerArgv } from "../src/seat-bridge/pod-journal.js";
import { createSeat, SID, type Seat } from "./seat-drain-fixture.js";

// The D3 bridge seam (principal-seat move): synthetic kubectl/systemctl/link fixtures plus the explicitly labeled
// real-a19 captured-byte replay below. Neither fixture accesses a live cluster, broker or principal filesystem.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const sha = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");
const seats: Seat[] = [];
afterEach(async () => { for (const seat of seats.splice(0)) await seat.close(); });
const RAW = { name: "org-d3synth-raw", uid: "00000000-0000-4000-8000-0000000000aa" };
const MANAGED = { name: "org-d3synth-0", uid: "00000000-0000-4000-8000-0000000000bb" };
const CA = Buffer.from("synthetic CA bytes");
const SERVER = "https://synthetic.invalid:6443";
const LEAD = "session:00000000-0000-4000-8000-0000000000c3";
const POD_JOURNAL = "/var/lib/fabric/sessions/synthetic.jsonl";
const INCARNATION = "00000000-0000-4000-8000-0000000001d1";
const V1_INCARNATION = "00000000-0000-4000-8000-0000000001c1";
const ORG = { productOwner: { name: "Delta" }, principals: [
  { name: "Delta", orgInstance: { herdrAgent: "org-delta", herdrWorkspaceLabel: "Delta", fabricSession: SID } },
  { name: "Echo", orgInstance: { herdrAgent: "org-echo", herdrWorkspaceLabel: "Echo", fabricSession: "00000000-0000-4000-8000-0000000000e5" } },
] };
const pins = readBridgeNatsSeatPins(ORG);
const seatFileEntry = { kind: "root", id: `session:${SID}`, principal: "Delta", send: ["followUp"], receive: ["followUp", "cancel"] };
const leadEntry = { kind: "root", id: LEAD, send: ["followUp"] };

const header = JSON.stringify({ type: "session", version: 3, id: SID, timestamp: "2026-10-25T00:00:00.000Z", cwd: "/synthetic" });
const entry = (id: string, parentId: string | null, extra: Record<string, unknown> = {}) =>
  JSON.stringify({ type: "message", id, parentId, timestamp: "2026-10-25T00:00:01.000Z", ...extra });
/** A sealed journal whose branch already has a thinking_level_change: Pi appends nothing on resume. */
const SEALED = `${header}\n${entry("e1", null)}\n${JSON.stringify({ type: "thinking_level_change", id: "t1", parentId: "e1",
  timestamp: "2026-10-25T00:00:01.500Z", thinkingLevel: "medium" })}\n${entry("e2", "t1")}\n`;
/** A sealed journal (an older session) whose branch has none: Pi appends exactly one on resume. */
const SEALED_BARE = `${header}\n${entry("e1", null)}\n${entry("e2", "e1")}\n`;
const THINKING = `${JSON.stringify({ type: "thinking_level_change", id: "e3", parentId: "e2", timestamp: "2026-10-25T00:00:02.000Z", thinkingLevel: "high" })}\n`;

interface World {
  seat: Seat; kube: string; marker: string; config: string; calls: () => Array<{ argv: string[] }>; journalFile: string;
  /** The sealed source journal text A's final seal describes. */
  sealed: string;
  status: { ok: boolean; problems: string[]; allow: { file: Array<Record<string, unknown>>; nats: Array<Record<string, unknown>> } };
  receipt: Record<string, unknown>; roots: Array<Record<string, unknown>>; holderStuck: boolean;
  /** What `nats-link status` reads once the unit was restarted (default: the same as before). */
  statusAfter?: World["status"];
  /** The link's identity gate row for the seat (`nats-link dry-run`), or null for no row. */
  gate: Record<string, unknown> | null;
  /** Every config object the link readers were handed (#1609 review P0-1: the post-write config). */
  seenConfigs: Array<{ allow: unknown }>;
}

const world = async (options: { podJournal?: string; machines?: string[]; allow?: Record<string, unknown>; sealed?: string } = {}):
  Promise<World> => {
  const sealed = options.sealed ?? SEALED;
  const seat = await createSeat({ kube: { kubeconfig: "/placeholder", server: SERVER, caSha256: sha(CA) } });
  seats.push(seat);
  const kube = path.join(seat.base, "kube");
  fs.mkdirSync(kube, { mode: 0o700 });
  // Re-pin the sealed inputs with the real kubeconfig path.
  const inputsFile = path.join(seat.runDir, "seat-inputs.json");
  const inputs = JSON.parse(fs.readFileSync(inputsFile, "utf8"));
  inputs.kube.kubeconfig = path.join(kube, "config");
  fs.chmodSync(inputsFile, 0o600); fs.writeFileSync(inputsFile, JSON.stringify(inputs)); fs.chmodSync(inputsFile, 0o400);
  const journalPath = path.join(seat.runDir, "seat-journal.jsonl");
  const records = fs.readFileSync(journalPath, "utf8").trim().split("\n").map(line => JSON.parse(line));
  records[1].sha256 = sha(fs.readFileSync(inputsFile));
  // The host's sealed source journal holds exactly the sealed text, and A's final seal of it is journaled once. Its
  // header names a synthetic cwd, so the seat's mesh root comes from the agent directory's Fabric config.
  const sourceJournal = JSON.parse(fs.readFileSync(path.join(seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
  fs.writeFileSync(sourceJournal, sealed);
  fs.writeFileSync(path.join(path.dirname(path.dirname(path.dirname(sourceJournal))), "fabric.json"),
    JSON.stringify({ mesh: { root: seat.meshRoot } }), { mode: 0o600 });
  const seal = Buffer.from(`${JSON.stringify(finalSealFor(seat, sourceJournal, sealed))}\n`);
  fs.mkdirSync(path.join(seat.runDir, "receipts"), { mode: 0o700 });
  fs.writeFileSync(path.join(seat.runDir, "receipts", "final-seal.json"), seal, { mode: 0o400 });
  records.push({ event: "final-seal", sha256: sha(seal) });
  fs.writeFileSync(journalPath, records.map(record => `${JSON.stringify(record)}\n`).join(""));
  fs.writeFileSync(path.join(kube, "config"), "synthetic", { mode: 0o600 });
  // The Pod's own journal, read by the REAL journal reader to make the in-Pod CLI's line.
  const journalFile = path.join(seat.base, "pod-journal.jsonl");
  fs.writeFileSync(journalFile, options.podJournal ?? sealed);
  const kubectl = path.join(kube, "kubectl");
  fs.copyFileSync(path.join("tests", "seat-bridge-kubectl.py"), kubectl);
  fs.chmodSync(kubectl, 0o700);
  // The synthetic link config: hub file mesh = the seat's mesh, the seat's own Path B machine listed. Its seat pins are
  // the synthetic org.json beside it (the pins the in-process runs inject), never a host's real org.json.
  const linkDir = path.join(seat.base, "link");
  fs.mkdirSync(linkDir, { mode: 0o700 });
  const orgJson = path.join(linkDir, "org.json");
  fs.writeFileSync(orgJson, JSON.stringify(ORG), { mode: 0o600 });
  const config = path.join(linkDir, "v2.json");
  fs.writeFileSync(config, `${JSON.stringify({ version: 1, name: "hub-a", link: "v2", file: { mesh: seat.meshRoot, lockProtocol: 2 },
    orgJson, stateDir: "/synthetic/link-state", nats: { servers: ["tls://nats.example.test:4222"], machine: "hub-a-bridge",
      credentials: "/synthetic/creds", caFile: "/synthetic/ca.pem", custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
      pathBMachines: options.machines ?? ["hub-a-bridge", "sandbox1", "sp-org-d3synth"], operatorMachine: "fabric-ops" },
    allow: options.allow ?? { file: [leadEntry, seatFileEntry] } }, null, 2)}\n`, { mode: 0o600 });
  const marker = path.join(seat.base, "systemctl-calls");
  const systemctl = path.join(seat.base, "systemctl");
  fs.writeFileSync(systemctl, `#!/bin/sh\necho "$@" >> ${marker}\n`, { mode: 0o700 });
  const w: World = {
    seat, kube, marker, config, journalFile, sealed,
    calls: () => fs.existsSync(path.join(kube, "kube-calls.jsonl"))
      ? fs.readFileSync(path.join(kube, "kube-calls.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)) : [],
    status: { ok: true, problems: [], allow: { file: [{ identity: LEAD, live: true }, { identity: `session:${SID}`, live: true }], nats: [] } },
    receipt: {}, roots: [], holderStuck: false, gate: null, seenConfigs: [],
  };
  return w;
};

/** A's final seal: exactly seven keys (A at 7d2e09af+, rule T-FIX3); the leaf is the last line's id. */
const finalSealFor = (seat: Seat, journal?: string, sealed = SEALED) => ({ schema: "d3-seat-final-seal-v1", run: seat.run,
  journal: journal ?? JSON.parse(fs.readFileSync(path.join(seat.runDir, "seat-run.json"), "utf8")).source.journal as string,
  journalSha256: sha(sealed), journalBytes: Buffer.byteLength(sealed),
  leafId: (JSON.parse(sealed.trim().split("\n").at(-1)!) as { id: string }).id,
  sealHasThinkingEntry: sealed.split("\n").some(line => line.includes('"type":"thinking_level_change"')) });

/** Re-seal the run with a change (and every pin that names its sha256), as a different A packet would. */
const resealRun = (w: World, mutate: (run: Record<string, unknown>) => void): void => {
  const runFile = path.join(w.seat.runDir, "seat-run.json");
  const value = JSON.parse(fs.readFileSync(runFile, "utf8")) as Record<string, unknown>;
  mutate(value);
  const bytes = Buffer.from(JSON.stringify(value));
  fs.chmodSync(runFile, 0o600); fs.writeFileSync(runFile, bytes); fs.chmodSync(runFile, 0o400);
  const runSha256 = sha(bytes);
  const inputsFile = path.join(w.seat.runDir, "seat-inputs.json");
  const inputs = JSON.parse(fs.readFileSync(inputsFile, "utf8"));
  inputs.runSha256 = runSha256;
  fs.chmodSync(inputsFile, 0o600); fs.writeFileSync(inputsFile, JSON.stringify(inputs)); fs.chmodSync(inputsFile, 0o400);
  const journalPath = path.join(w.seat.runDir, "seat-journal.jsonl");
  const records = fs.readFileSync(journalPath, "utf8").trim().split("\n").map(line => JSON.parse(line));
  records[0].runSha256 = runSha256;
  records[1].sha256 = sha(fs.readFileSync(inputsFile));
  fs.writeFileSync(journalPath, records.map(record => `${JSON.stringify(record)}\n`).join(""));
  (w.seat as { runSha256: string }).runSha256 = runSha256;
};

const setKube = (w: World, pod: { name: string; uid: string }, identity: { line: string; code?: number; stderr?: string; changeUid?: string; restart?: Record<string, unknown> },
  podExtra: Record<string, unknown> = {}) => {
  const ownerLine = JSON.stringify({ rootId: `session:${SID}`, ownerHostId: `session:${SID}`, ownerIdentityId: `session:${SID}`,
    ownerIncarnation: INCARNATION, machine: "sp-org-d3synth", pid: 7, processStartedAt: "123", projectionKey: "sp-org-d3synth.p.synthetic", projectionRevision: 1 });
  fs.writeFileSync(path.join(w.kube, "kube-state.json"), JSON.stringify({ server: SERVER, caB64: CA.toString("base64"), cliPresent: true,
    image0112: true, journalFile: w.journalFile, readOverride: identity.line !== "READ_REAL_JOURNAL",
    ownerLine: identity.line === "READ_REAL_JOURNAL" ? ownerLine : identity.line,
    pod: { name: pod.name, uid: pod.uid, meshRoot: "/var/lib/fabric/mesh", running: true, terminated: null, args: ["--session", POD_JOURNAL],
      natsMachine: "sp-org-d3synth", ...podExtra },
    exec: { line: "" }, identity }));
};

/** Default runs the exact Python reader over the fixture's real file. Overrides exercise malformed byte reports. */
const identityLine = (w: World, options: { journal?: unknown } = {}) => {
  if (Object.keys(options).length === 0) return "READ_REAL_JOURNAL";
  const journal = (options.journal ?? journalBranch(w.journalFile, "e2", Buffer.byteLength(w.sealed))) as Record<string, any>;
  if (journal.error) return JSON.stringify({ error: "journal-suffix-invalid" });
  return JSON.stringify({ size: journal.journalBytes, sha256: journal.journalSha256, prefixSha256: journal.sealedSha256,
    suffixBase64: journal.suffixBase64, mtimeNs: "123" });
};

const request = (seat: Seat, phase: string, inputs: Record<string, unknown>) => JSON.stringify({ schema: "d3-seam-request-v1", run: seat.run,
  runSha256: seat.runSha256, phase, principal: "d3synth", sessionId: SID, inputs });

const run = async (w: World, phase: string, given: Record<string, unknown> | ((w: World) => Record<string, unknown>),
  extra: Partial<BridgeDeps> = {}, stdin?: Buffer) => {
  const inputs = typeof given === "function" ? given(w) : given;
  let stdout = ""; let stderr = "";
  let t = 0;
  const deps: BridgeDeps = {
    kubectl: path.join(w.kube, "kubectl"), systemctl: path.join(w.seat.base, "systemctl"),
    seatPins: () => pins, now: () => t, sleep: async ms => { t += ms; },
    holder: () => ({ pid: !w.holderStuck && fs.existsSync(w.marker) ? 101 + fs.readFileSync(w.marker, "utf8").split("\n").length : 100,
      startTime: "1", purpose: "start" }),
    status: async config => { w.seenConfigs.push({ allow: structuredClone(config.allow) });
      return structuredClone(w.statusAfter && fs.existsSync(w.marker) ? w.statusAfter : w.status); },
    receipt: async config => { w.seenConfigs.push({ allow: structuredClone(config.allow) }); return structuredClone(w.receipt); },
    dryRun: async config => { w.seenConfigs.push({ allow: structuredClone(config.allow) }); return { rows: w.gate ? [structuredClone(w.gate)] : [] }; },
    roots: async () => structuredClone(phase === "publish" && w.roots.length === 0 ? [{ id: `session:${SID}`, machine: "sp-org-d3synth",
      ownerHostId: `session:${SID}`, ownerIdentityId: `session:${SID}`, ownerIncarnation: INCARNATION, live: true }] : w.roots) as never,
    onError: error => { if (!(error instanceof Error && error.name === "SeamGap")) throw error; },
    ...extra,
  };
  const code = await main(["--run-dir", w.seat.runDir, "--phase", phase], {
    stdin: () => stdin ?? Buffer.from(request(w.seat, phase, inputs)), stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(w.seat.lockFd()) },
  }, { linkConfig: w.config, linkUnit: "smarty-nats-link.service" }, deps);
  return { code, stdout, stderr, receipt: stdout ? JSON.parse(stdout) as Record<string, any> : undefined };
};

const resumeInputs = (w: World) => ({ pod: RAW, finalSeal: finalSealFor(w.seat, undefined, w.sealed) });
const evidenceOf = (w: World, name: string) => JSON.parse(fs.readFileSync(path.join(w.seat.runDir, name), "utf8"));

describe.skipIf(!linux)("bridge resume: a read-only check of the resumed Pi in the raw Pod", () => {
  it("b6 a20 failed-exec replay: empty stdout + Forbidden stderr records the actual cause", async () => {
    // a20's byte shape is not the bug: the same exact reader succeeds with size == sealed.
    const padding = 76414 - Buffer.byteLength(SEALED);
    const sealed = SEALED.replace('"cwd":"/synthetic"', `"cwd":"/synthetic","padding":"${"x".repeat(padding - 13)}"`);
    expect(Buffer.byteLength(sealed)).toBe(76414);
    const w = await world({ sealed });
    const forbidden = 'error: unable to upgrade connection: pods "principal-d3synth-owner" is forbidden: User "system:serviceaccount:d3-rehearsal:d3r-executor" cannot create resource "pods/exec" in API group "" in the namespace "d3-rehearsal"';
    setKube(w, RAW, { line: "", code: 1, stderr: forbidden });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    const outcome = evidenceOf(w, "fabric-v2-bridge-resume-outcome.json");
    expect(outcome).toMatchObject({ reason: "pod-journal-unreadable", errorClass: "KubectlExecError",
      measured: { execCode: 1, execStdout: "", execStderr: forbidden, journal: null } });
    expect(JSON.parse(outcome.errorMessage)).toEqual({ execCode: 1, stdout: "", stderr: forbidden });
    expect(outcome.asserted.podLine).toBe("");
  });

  it("passes with no append: the true leaf, the file's sha256, an empty suffix, [] appended", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    const result = await run(w, "resume", resumeInputs);
    expect(result).toMatchObject({ code: 0, stderr: "" });
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: w.seat.run, phase: "resume", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, journal: POD_JOURNAL, leafId: "e2", podUid: RAW.uid, appendedSinceSeal: [], journalSha256: sha(SEALED), suffixBase64: "" } });
    const exec = w.calls().find(call => call.argv.includes("/usr/bin/python3"))!;
    expect(exec.argv.slice(exec.argv.indexOf("--"))).toEqual(["--", ...podJournalArgv(POD_JOURNAL, Buffer.byteLength(SEALED))]);
  });

  it.each([false, true])("b5 image CLI regression a19/a20: startup waiting=%s passes with seven byte-derived fields, no session CLI", async waiting => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: !waiting });
    let t = 0;
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep: async ms => {
      t += ms; setKube(w, RAW, { line: identityLine(w) });
    } });
    const outcome = evidenceOf(w, "fabric-v2-bridge-resume-outcome.json");
    expect(result, JSON.stringify(outcome)).toMatchObject({ code: 0, stderr: "", receipt: { result: "pass" } });
    expect(Object.keys(result.receipt!.observed).sort()).toEqual([
      "sessionId", "journal", "leafId", "podUid", "appendedSinceSeal", "journalSha256", "suffixBase64"].sort());
    expect(w.calls().some(call => call.argv.includes("session"))).toBe(false);
    expect(outcome).toMatchObject({ measured: { before: { piRestartCount: 0, piStartedAt: "2026-10-31T00:00:00Z",
      piContainerId: "containerd://synthetic-pi", pvcWriters: ["pi"] }, after: { piRestartCount: 0 } } });
  });

  it.each([
    { restartCount: 1 }, { startedAt: "2026-10-31T00:01:00Z" }, { containerID: "containerd://replacement" },
  ])("b5: Pod restart during journal read %j refuses and records its outcome", async restart => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w), restart });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 0, receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ reason: "pod-restarted-during-read",
      errorClass: null, errorMessage: null });
  });

  it("b5: the saved a19 args shape has BOTH --session-dir and --session; only the explicit file is read", async () => {
    const w = await world();
    const journal = "/var/lib/smarty/state/pi/sessions/synthetic/known.jsonl";
    setKube(w, RAW, { line: identityLine(w) }, { args: ["--mode", "rpc", "--model", "synthetic/model", "--thinking", "xhigh",
      "--session-dir", "/var/lib/smarty/state/pi/sessions", "--session", journal] });
    const result = await run(w, "resume", resumeInputs);
    expect(result.receipt).toMatchObject({ result: "pass", observed: { journal } });
    const exec = w.calls().find(call => call.argv.includes("/usr/bin/python3"))!;
    expect(exec.argv.at(-2)).toBe(journal);
  });

  it.each(["extraContainers", "initContainers", "ephemeralContainers"])("b5: another writable PVC container in %s refuses before exec", async field => {
    const w = await world();
    const writer = { name: "other", restartPolicy: "Always", volumeMounts: [{ name: "state", mountPath: "/var/lib/fabric" }] };
    setKube(w, RAW, { line: identityLine(w) }, { [field]: [writer] });
    expect((await run(w, "resume", resumeInputs)).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json").reason).toBe("pod-pvc-writers-unproved");
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it("b5: RESUME does not require meshRoot, owner incarnation or a Main-drain PID", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { meshRoot: "" });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 0, receipt: { result: "pass" } });
  });

  it("reports exactly the one thinking_level_change Pi appends on resume, its bytes and the true leaf", async () => {
    const w = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + THINKING });
    setKube(w, RAW, { line: identityLine(w) });
    const result = await run(w, "resume", resumeInputs);
    expect(result.receipt).toMatchObject({ result: "pass", observed: { leafId: "e3", journalSha256: sha(SEALED_BARE + THINKING),
      suffixBase64: Buffer.from(THINKING).toString("base64"),
      appendedSinceSeal: [{ id: "e3", parentId: "e2", type: "thinking_level_change", thinkingLevel: "high" }] } });
  });

  it.each([
    { phaseCap: 5_000, elapsedMs: 2_000, observations: 1 },
    { phaseCap: 600_000, elapsedMs: 400_000, observations: 2 },
  ])("a missing thinking append records its outcome before the $phaseCap ms phase cap", async ({ phaseCap, elapsedMs, observations }) => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "resume", resumeInputs, { deadlineMs: phaseCap, now: () => t, sleep: async ms => {
      // Late timer wakes consume phase time too; cover the full default cap without hundreds of subprocess reads.
      t += phaseCap === 600_000 ? 200_000 : ms;
    } });
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
    const outcome = evidenceOf(w, "fabric-v2-bridge-resume-outcome.json");
    expect(outcome).toMatchObject({ result: "unknown", gap: "resume-thinking-append-missing",
      errorClass: "SeamGap", errorMessage: "resume-thinking-append-missing", elapsedMs,
      measured: { observations, finalSeal: { sealHasThinkingEntry: false }, journal: { appendedSinceSeal: [], suffixBytes: 0 } } });
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(observations);
    expect(t).toBe(elapsedMs);
    expect(t).toBeLessThan(phaseCap);
  });

  it.each([150_000, 388_000])("Pi's first append after %i ms passes, including near the last full-read budget", async appendAt => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep: async () => {
      t = Math.min(t + 30_000, appendAt); // Delayed scheduler wakes inside the original phase clock.
      if (t === appendAt) { fs.appendFileSync(w.journalFile, THINKING); setKube(w, RAW, { line: identityLine(w) }); }
    } });
    expect(result).toMatchObject({ code: 0, stderr: "" });
    expect(result.receipt).toMatchObject({ result: "pass", observed: { leafId: "e3",
      appendedSinceSeal: [{ id: "e3", parentId: "e2", type: "thinking_level_change", thinkingLevel: "high" }] } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ elapsedMs: appendAt,
      measured: { observations: Math.ceil(appendAt / 30_000) + 1 } });
  });

  it("a seal WITH a thinking entry needs one observation and no wait", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    expect((await run(w, "resume", resumeInputs, { sleep: async () => { throw new Error("unexpected append wait"); } })).receipt)
      .toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ elapsedMs: 0, measured: { observations: 1 } });
  });

  // A19 (B08 attempt 19): A called resume while the pinned Pod's pi was still waiting, one second before startup.
  it("resume startup regression: a19 sealed-with-thinking Pod starts after three polls and executes identity once", async () => {
    const w = await world({ sealed: SEALED });
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    let t = 0;
    let polls = 0;
    const sleep = vi.fn(async (ms: number) => {
      t += ms;
      if (++polls === 3) setKube(w, RAW, { line: identityLine(w) });
    });
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep });
    const outcome = evidenceOf(w, "fabric-v2-bridge-resume-outcome.json");
    // Keep the actual a19 receipt, stderr and outcome in the RED assertion, not just a mock-call failure.
    expect(result, JSON.stringify({ stdout: result.stdout, stderr: result.stderr, outcome }, null, 2))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "pass", observed: { leafId: "e2", appendedSinceSeal: [] } } });
    expect(sleep).toHaveBeenCalledTimes(3);
    expect(t).toBeGreaterThan(0);
    expect(t).toBeLessThanOrEqual(180_000);
    expect(outcome).toMatchObject({ result: "pass", elapsedMs: t, measured: {
      observations: 4, waitedMs: t, finalSeal: { sealHasThinkingEntry: true } } });
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(1);
  });

  it.each([840_000, 5_000])("resume startup regression: neverstarts respects the 180 s startup and %i ms phase caps", async phaseCap => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    let t = 0;
    const reads: Array<{ at: number; budget: number }> = [];
    const actualRead = kube.readPod;
    vi.spyOn(kube, "readPod").mockImplementation(async (access, name, budget) => {
      reads.push({ at: t, budget });
      return actualRead(access, name, budget);
    });
    const sleep = vi.fn(async (ms: number) => { t += phaseCap === 840_000 ? 30_000 : ms; });
    const result = await run(w, "resume", resumeInputs, { deadlineMs: phaseCap, now: () => t, sleep });
    expect(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(t).toBeGreaterThan(0);
    expect(t).toBeLessThanOrEqual(Math.min(180_000, phaseCap));
    if (phaseCap === 840_000) expect(t).toBe(180_000);
    expect(sleep).toHaveBeenCalled();
    expect(reads.every(read => read.at < phaseCap && read.budget >= 1_000 && read.budget <= Math.min(30_000, phaseCap - read.at)))
      .toBe(true);
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "pod-not-running",
      elapsedMs: t, measured: { observations: expect.any(Number), waitedMs: t } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json").measured.observations).toBeGreaterThan(1);
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it.each([0, 23])("resume startup regression: already terminated with exit %i refuses without sleeping or exec", async terminated => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated });
    const sleep = vi.fn(async () => { throw new Error("terminated pi must not be polled"); });
    expect(await run(w, "resume", resumeInputs, { sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "pod-not-running",
      elapsedMs: 0, measured: { observations: 1, waitedMs: 0 } });
    expect(sleep).not.toHaveBeenCalled();
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it("resume startup regression: already running preserves one observation and zero startup wait", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    const sleep = vi.fn(async () => { throw new Error("already running pi must not wait"); });
    expect(await run(w, "resume", resumeInputs, { sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "pass" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 0,
      measured: { observations: 1, waitedMs: 0 } });
    expect(sleep).not.toHaveBeenCalled();
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(1);
  });

  it.each([
    { fault: "uid", gap: "pod-uid-mismatch" },
    { fault: "node", gap: "pod-node-mismatch" },
  ])("resume startup regression: $fault replacement while waiting is a GAP with an outcome, never exec", async ({ fault, gap }) => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    let t = 0;
    const sleep = vi.fn(async (ms: number) => {
      t += ms;
      if (fault === "uid") setKube(w, { ...RAW, uid: "00000000-0000-4000-8000-0000000000ff" }, { line: identityLine(w) }, { running: false });
      else setKube(w, RAW, { line: identityLine(w) }, { running: false, nodeName: "replacement-node" });
    });
    expect(await run(w, "resume", resumeInputs, { now: () => t, sleep }))
      .toMatchObject({ code: 2, stdout: "", stderr: `GAP: ${gap} [owner: fabric-v2]\n` });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: gap,
      elapsedMs: t, intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-intent.json"))) });
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it("resume startup regression: termination during waiting stops after that observation", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    let t = 0;
    const sleep = vi.fn(async (ms: number) => {
      t += ms;
      setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: 0 });
    });
    expect(await run(w, "resume", resumeInputs, { now: () => t, sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "pod-not-running",
      elapsedMs: t, measured: { observations: 2, waitedMs: t } });
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it.each([false, true])("resume startup regression: unreadable Pod duringWait=%s is not blindly retried", async duringWait => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    const makeUnreadable = () => {
      const file = path.join(w.kube, "kube-state.json");
      const state = JSON.parse(fs.readFileSync(file, "utf8"));
      state.pod = null;
      fs.writeFileSync(file, JSON.stringify(state));
    };
    if (!duringWait) makeUnreadable();
    let t = 0;
    const sleep = vi.fn(async (ms: number) => { t += ms; makeUnreadable(); });
    expect(await run(w, "resume", resumeInputs, { now: () => t, sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "pod-unreadable",
      elapsedMs: t, measured: { observations: duringWait ? 2 : 1, waitedMs: t } });
    expect(sleep).toHaveBeenCalledTimes(duringWait ? 1 : 0);
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
  });

  it("resume startup regression: unreadable identity after startup is refused without another wait or exec", async () => {
    const w = await world();
    setKube(w, RAW, { line: "not json", code: 1 }, { running: false, terminated: null });
    let t = 0;
    const sleep = vi.fn(async (ms: number) => { t += ms; setKube(w, RAW, { line: "not json", code: 1 }); });
    expect(await run(w, "resume", resumeInputs, { now: () => t, sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "pod-journal-unreadable",
      elapsedMs: t, measured: { observations: 2, waitedMs: t } });
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(1);
  });

  it("resume startup regression: a slow read consumes only the short original phase budget and retains its GAP outcome", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    const phaseCap = 3_500;
    let t = 0;
    const budgets: number[] = [];
    const actualRead = kube.readPod;
    vi.spyOn(kube, "readPod").mockImplementation(async (access, name, budget) => {
      expect(t).toBeLessThan(phaseCap);
      budgets.push(budget);
      const value = await actualRead(access, name, budget);
      t += budget;
      return value;
    });
    const sleep = vi.fn(async (ms: number) => { t += ms; });
    expect(await run(w, "resume", resumeInputs, { deadlineMs: phaseCap, now: () => t, sleep }))
      .toMatchObject({ code: 2, stdout: "", stderr: "GAP: phase-deadline [owner: fabric-v2]\n" });
    expect(budgets).toEqual([phaseCap]);
    expect(t).toBe(phaseCap);
    expect(sleep).not.toHaveBeenCalled();
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "phase-deadline",
      elapsedMs: phaseCap, intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-intent.json"))) });
  });

  it("resume startup regression: a generic sleep throw after INTENT attempts one refused outcome and keeps exit 2 seam-internal", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) }, { running: false, terminated: null });
    const error = new Error("synthetic pi startup timer failure");
    const onError = vi.fn();
    const open = vi.spyOn(fs, "openSync");
    const outcomeFile = path.join(w.seat.runDir, "fabric-v2-bridge-resume-outcome.json");
    expect(await run(w, "resume", resumeInputs, { sleep: async () => { throw error; }, onError }))
      .toMatchObject({ code: 2, stdout: "", stderr: "GAP: seam-internal [owner: fabric-v2]\n" });
    expect(onError).toHaveBeenCalledWith(error);
    expect(open.mock.calls.filter(([file, flags]) => file === outcomeFile && typeof flags === "number" && (flags & fs.constants.O_EXCL) !== 0))
      .toHaveLength(1);
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "seam-internal",
      errorClass: "Error", errorMessage: error.message,
      intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-intent.json"))) });
  });

  // b5 deliberately has no Main-drain memory identity: A pins this path and re-derives the bytes.
  it.each([{ args: [] }, { args: ["--session", "relative.jsonl"] }, { args: ["--session"] },
    { args: ["--session", POD_JOURNAL, "--session", POD_JOURNAL] },
    { args: ["--session-dir", "/var/lib/smarty/state/pi/sessions"] }])(
    "b5: invalid rendered session args $args refuse without reading", async ({ args }) => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) }, { args });
    const sleep = vi.fn(async () => { throw new Error("invalid path must not wait"); });
    expect(await run(w, "resume", resumeInputs, { sleep })).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ reason: "pod-session-arg-unknown" });
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
    expect(sleep).not.toHaveBeenCalled();
  });

  it("b5: rendered session path changing during an append observation stops polling", async () => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    const actualRead = kube.readPod;
    let reads = 0;
    vi.spyOn(kube, "readPod").mockImplementation(async (...args) => {
      const pod = await actualRead(...args);
      if (++reads === 4 && pod) pod.piArgs = ["--session", "/changed.jsonl"];
      return pod;
    });
    let t = 0;
    expect(await run(w, "resume", resumeInputs, { now: () => t, sleep: async ms => { t += ms; } }))
      .toMatchObject({ code: 0, receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ reason: "pod-spec-changed", elapsedMs: 2_000 });
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(2);
  });

  it.each([
    { field: "journalBytes", value: Buffer.byteLength(SEALED_BARE) + 1, gap: "journal-suffix-inconsistent" },
    { field: "journalSha256", value: sha("not the sealed journal"), gap: "journal-suffix-inconsistent" },
    { field: "suffixBase64", value: "Cg==", gap: "journal-suffix-inconsistent" },
  ])("F1: an unchanged-journal claim with incoherent $field is an immediate GAP", async ({ field, value, gap }) => {
    const w = await world({ sealed: SEALED_BARE });
    const journal = { ...journalBranch(w.journalFile, "e2", Buffer.byteLength(SEALED_BARE)), [field]: value };
    setKube(w, RAW, { line: identityLine(w, { journal }) });
    let t = 0;
    const sleep = vi.fn(async (ms: number) => { t += ms; throw new Error("F1: unexpected append wait for incoherent journal"); });
    expect(await run(w, "resume", resumeInputs, { now: () => t, sleep })).toMatchObject({ code: 2, stdout: "",
      stderr: `GAP: ${gap} [owner: fabric-v2]\n` });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown", gap, elapsedMs: 0,
      measured: { observations: 1 } });
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(1);
    expect(sleep).not.toHaveBeenCalled();
    expect(t).toBe(0);
  });

  it("an unreadable identity during append polling is not misclassified as a missing append", async () => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep: async ms => {
      t += ms;
      setKube(w, RAW, { line: "unreadable", code: 1 });
    } });
    expect(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused",
      reason: "pod-journal-unreadable", elapsedMs: 2_000, measured: { observations: 2 } });
  });

  it.each([200_000, 600_000])("slow per-read calls preserve the %i ms cap and distinguish initial identity from missing append", async phaseCap => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const budgets: number[] = [];
    const actualReadPod = kube.readPod;
    const actualKubectl = kube.kubectl;
    vi.spyOn(kube, "readPod").mockImplementation(async (access, name, timeout) => {
      const value = await actualReadPod(access, name, timeout);
      budgets.push(timeout); t += timeout;
      return value;
    });
    vi.spyOn(kube, "kubectl").mockImplementation(async (access, args, timeout) => {
      const value = await actualKubectl(access, args, timeout);
      budgets.push(timeout); t += timeout;
      return value;
    });
    const result = await run(w, "resume", resumeInputs, { deadlineMs: phaseCap, now: () => t, sleep: async ms => { t += ms; } });
    // No CLI executable probe: a complete fixed-script read costs 180 s rather than b4's 210 s.
    const observations = phaseCap === 200_000 ? 1 : 3;
    // The final short poll wake is counted even when there is no full-read budget left.
    const elapsed = observations * (180_000 + 2_000);
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
    expect(budgets).toEqual(Array.from({ length: observations }, () => [30_000, 120_000, 30_000]).flat());
    expect(t).toBe(elapsed);
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown",
      gap: "resume-thinking-append-missing", elapsedMs: elapsed, measured: { observations } });
    expect(t).toBeLessThan(phaseCap);
  });

  it("keeps Pi's missing-append wait at 600 s under the default 840 s phase ceiling", async () => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep: async () => { t += 180_000; } });
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown",
      gap: "resume-thinking-append-missing", elapsedMs: 540_000, measured: { observations: 3 } });
    expect(t).toBe(540_000);
    expect(t).toBeLessThan(600_000);
  });

  it("caps an oversized phase budget at 840 s, with the final read consuming the remaining time", async () => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const budgets: number[] = [];
    const actualVerify = kube.verifyKubeconfig;
    const actualReadPod = kube.readPod;
    vi.spyOn(kube, "verifyKubeconfig").mockImplementation(async (access, timeout) => {
      await actualVerify(access, timeout);
      budgets.push(timeout); t += 780_000;
    });
    vi.spyOn(kube, "readPod").mockImplementation(async (access, name, timeout) => {
      const value = await actualReadPod(access, name, timeout);
      budgets.push(timeout); t += timeout;
      return value;
    });
    const result = await run(w, "resume", resumeInputs, { deadlineMs: 900_000, now: () => t, sleep: async ms => { t += ms; } });
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: phase-deadline [owner: fabric-v2]\n" });
    expect(t).toBe(840_000);
    expect(budgets).toEqual([30_000, 30_000, 30_000]);
    expect(w.calls()).toHaveLength(4);
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "phase-deadline",
      errorClass: "SeamGap", errorMessage: "phase-deadline", elapsedMs: 840_000,
      intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-intent.json"))) });
  });

  /** A thinking_level_change line as Pi writes it (5 keys). */
  const thinkingLine = (id: string, parentId: string) => `${JSON.stringify({ type: "thinking_level_change", id, parentId,
    timestamp: "2026-10-25T00:00:02.000Z", thinkingLevel: "high" })}\n`;
  /** A CLI report that calls `tail` a clean branch from the sealed leaf (what a wrong or hostile reader could say). */
  const cleanReport = (sealed: string, tail: string, appended: Array<{ id: string; parentId: string }>) => ({
    leafId: appended.at(-1)!.id, entries: 9, sealOnBranch: true, truncated: false,
    appendedSinceSeal: appended.map(item => ({ ...item, type: "thinking_level_change", thinkingLevel: "high" })),
    journalSha256: sha(sealed + tail), journalBytes: Buffer.byteLength(sealed + tail), sealedSha256: sha(sealed),
    suffixBytes: Buffer.byteLength(tail), suffixBase64: Buffer.from(tail).toString("base64") });
  const reused = { code: 2, stdout: "", stderr: "GAP: resume-tail-id-reused [owner: fabric-v2]\n" };

  it("A70: an appended entry that REUSES a sealed id is never a resume, whatever the Pod's reader says", async () => {
    // An off-branch sealed id reused by the tail: no cycle, so the in-image reader itself reports a clean branch.
    const fork = `${header}\n${entry("e1", null)}\n${entry("b1", "e1")}\n${entry("e2", "e1")}\n`;
    const off = await world({ sealed: fork, podJournal: fork + thinkingLine("b1", "e2") });
    expect(journalBranch(off.journalFile, "e2", Buffer.byteLength(fork))).toMatchObject({ sealOnBranch: true, leafId: "b1",
      appendedSinceSeal: [{ id: "b1", parentId: "e2" }] });
    setKube(off, RAW, { line: identityLine(off) });
    expect(await run(off, "resume", resumeInputs)).toMatchObject(reused);
    expect(evidenceOf(off, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown", gap: "resume-tail-id-reused",
      errorClass: "SeamGap", errorMessage: "resume-tail-id-reused" });
    // An on-branch sealed id: a cycle under later-id-wins. The real reader refuses the branch, and the CLI says so.
    const tail = thinkingLine("e1", "e2");
    const cycle = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + tail });
    expect(() => journalBranch(cycle.journalFile, "e2", Buffer.byteLength(SEALED_BARE)))
      .toThrow(expect.objectContaining({ code: "JOURNAL_BRANCH_BROKEN" }));
    setKube(cycle, RAW, { line: identityLine(cycle) });
    expect(await run(cycle, "resume", resumeInputs)).toMatchObject(reused);
    // The same tail with a reader that hides the cycle: the seam's own check (the HOST's sealed ids) refuses it.
    const hidden = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + tail });
    setKube(hidden, RAW, { line: identityLine(hidden, { journal: cleanReport(SEALED_BARE, tail, [{ id: "e1", parentId: "e2" }]) }) });
    expect(await run(hidden, "resume", resumeInputs)).toMatchObject(reused);
  });

  it("A70: a duplicate id within the tail is refused; so is a sealed copy that is not the seal's bytes", async () => {
    const tail = thinkingLine("a1b2c3d4", "e2") + thinkingLine("a1b2c3d4", "a1b2c3d4");
    // The real reader: a self-parent is a cycle.
    const real = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + tail });
    expect(() => journalBranch(real.journalFile, "e2", Buffer.byteLength(SEALED_BARE)))
      .toThrow(expect.objectContaining({ code: "JOURNAL_BRANCH_BROKEN" }));
    // A reader that reports both entries as a clean chain: the seam's own tail check refuses the repeat.
    const hidden = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + tail });
    setKube(hidden, RAW, { line: identityLine(hidden, { journal: cleanReport(SEALED_BARE, tail,
      [{ id: "a1b2c3d4", parentId: "e2" }, { id: "a1b2c3d4", parentId: "a1b2c3d4" }]) }) });
    expect(await run(hidden, "resume", resumeInputs)).toMatchObject(reused);
    // The sealed ids come from the host's sealed copy only while it is exactly the sealed bytes.
    const changed = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + thinkingLine("a1b2c3d4", "e2") });
    const source = JSON.parse(fs.readFileSync(path.join(changed.seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
    fs.appendFileSync(source, `${entry("e9", "e2")}\n`);
    setKube(changed, RAW, { line: identityLine(changed) });
    expect(await run(changed, "resume", resumeInputs)).toMatchObject({ code: 2, stderr: "GAP: journal-prefix-mismatch [owner: fabric-v2]\n" });
  });

  it("A70: a tail entry whose parent is a sealed entry other than the sealed leaf is not Pi's resume append", async () => {
    // Pi appends with parentId = its current leaf, which on load is the last entry: the sealed leaf.
    const w = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + thinkingLine("a1b2c3d4", "e1") });
    setKube(w, RAW, { line: identityLine(w) });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stderr: "GAP: branch-diverged [owner: fabric-v2]\n" });
    // The counterexample: Pi's real append (new id, parent = the sealed leaf) passes.
    const ok = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + thinkingLine("a1b2c3d4", "e2") });
    setKube(ok, RAW, { line: identityLine(ok) });
    expect((await run(ok, "resume", resumeInputs)).receipt).toMatchObject({ result: "pass", observed: { leafId: "a1b2c3d4" } });
  });

  it("a diverged branch (the sealed leaf is no ancestor of the current leaf) is UNKNOWN: no receipt", async () => {
    const w = await world({ podJournal: `${SEALED}${entry("e3", "e1")}\n` });
    setKube(w, RAW, { line: identityLine(w) });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stdout: "", stderr: "GAP: branch-diverged [owner: fabric-v2]\n" });
  });

  it("a journal whose sealed bytes changed, or that grew by more than 64 KiB, is UNKNOWN: no receipt", async () => {
    const changed = await world({ podJournal: SEALED.replace("e1\"", "e1\",\"x\":1") });
    setKube(changed, RAW, { line: identityLine(changed) });
    expect(await run(changed, "resume", resumeInputs)).toMatchObject({ code: 2, stdout: "", stderr: "GAP: journal-prefix-mismatch [owner: fabric-v2]\n" });
    const big = await world({ podJournal: `${SEALED}${entry("e3", "e2", { text: "x".repeat(70 * 1024) })}\n` });
    setKube(big, RAW, { line: identityLine(big) });
    expect(await run(big, "resume", resumeInputs)).toMatchObject({ code: 2, stdout: "", stderr: "GAP: journal-suffix-too-large [owner: fabric-v2]\n" });
  });

  it("does not accept CLI-style appended/leaf assertions in the closed byte-report schema", async () => {
    const w = await world({ podJournal: SEALED + THINKING });
    const report = JSON.parse(identityLine(w, { journal: journalBranch(w.journalFile, "e2", Buffer.byteLength(SEALED)) }));
    setKube(w, RAW, { line: JSON.stringify({ ...report, appendedSinceSeal: [], leafId: "e2" }) });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 0, receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json").reason).toBe("pod-journal-unreadable");
  });

  it("reports the rendered path and byte-derived leaf, without pretending to verify Main-drain memory", async () => {
    const file = await world();
    const rendered = "/var/lib/fabric/sessions/other-rendered.jsonl";
    setKube(file, RAW, { line: identityLine(file) }, { args: ["--session", rendered] });
    expect((await run(file, "resume", resumeInputs)).receipt).toMatchObject({ result: "pass", observed: { journal: rendered, leafId: "e2" } });
    const exec = file.calls().find(call => call.argv.includes("/usr/bin/python3"))!;
    expect(exec.argv.at(-2)).toBe(rendered);
    const leaf = await world({ podJournal: SEALED + THINKING });
    setKube(leaf, RAW, { line: identityLine(leaf) });
    expect((await run(leaf, "resume", resumeInputs)).receipt).toMatchObject({ result: "pass", observed: { leafId: "e3" } });
  });

  it("checks A's final seal from RUN itself: a request seal that differs, or an unjournaled seal file, is UNKNOWN", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    // A's real input is its whole 7-key seal: a 6-key seal without sealHasThinkingEntry is refused.
    const { sealHasThinkingEntry: _drop, ...sixKeys } = finalSealFor(w.seat);
    expect(await run(w, "resume", { pod: RAW, finalSeal: sixKeys })).toMatchObject({ code: 2, stderr: "GAP: final-seal-invalid [owner: fabric-v2]\n" });
    expect(await run(w, "resume", { pod: RAW, finalSeal: { ...finalSealFor(w.seat), sealHasThinkingEntry: !finalSealFor(w.seat).sealHasThinkingEntry } }))
      .toMatchObject({ code: 2, stderr: "GAP: final-seal-mismatch [owner: fabric-v2]\n" });
    expect(await run(w, "resume", { pod: RAW, finalSeal: { ...finalSealFor(w.seat), leafId: "e1" } }))
      .toMatchObject({ code: 2, stderr: "GAP: final-seal-mismatch [owner: fabric-v2]\n" });
    const file = path.join(w.seat.runDir, "receipts", "final-seal.json");
    fs.chmodSync(file, 0o600); fs.appendFileSync(file, " ");
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stderr: "GAP: final-seal-unjournaled [owner: fabric-v2]\n" });
  });

  it("#1609 review (seal P2): sealHasThinkingEntry is true or false only, in the request and in RUN's own seal", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    const good = finalSealFor(w.seat);
    const { sealHasThinkingEntry: _drop, ...missing } = good;
    const bad: Array<[string, Record<string, unknown>]> = [["missing", missing], ["a string", { ...good, sealHasThinkingEntry: "false" }],
      ["a number", { ...good, sealHasThinkingEntry: 0 }], ["null", { ...good, sealHasThinkingEntry: null }]];
    const invalid = { code: 2, stdout: "", stderr: "GAP: final-seal-invalid [owner: fabric-v2]\n" };
    // The request's copy: each bad type refuses before any evidence or Pod read.
    for (const [, seal] of bad) expect(await run(w, "resume", { pod: RAW, finalSeal: seal })).toMatchObject(invalid);
    // RUN's own seal file (journaled, so only its shape can refuse it), with the request still the good seal.
    const sealFile = path.join(w.seat.runDir, "receipts", "final-seal.json");
    const journalPath = path.join(w.seat.runDir, "seat-journal.jsonl");
    const original = fs.readFileSync(sealFile);
    const rewrite = (bytes: Buffer) => {
      fs.chmodSync(sealFile, 0o600); fs.writeFileSync(sealFile, bytes); fs.chmodSync(sealFile, 0o400);
      fs.writeFileSync(journalPath, fs.readFileSync(journalPath, "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>)
        .map(record => record.event === "final-seal" ? { ...record, sha256: sha(bytes) } : record).map(record => `${JSON.stringify(record)}\n`).join(""));
    };
    for (const [, seal] of bad) {
      rewrite(Buffer.from(`${JSON.stringify(seal)}\n`));
      expect(await run(w, "resume", { pod: RAW, finalSeal: good })).toMatchObject(invalid);
    }
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-intent.json"))).toBe(false);
    expect(w.calls()).toEqual([]);
    // The counterexample: both copies valid (true here) passes.
    rewrite(original);
    expect((await run(w, "resume", resumeInputs)).receipt).toMatchObject({ result: "pass" });
  });

  it("a Pod that is not the pin is UNKNOWN; a phase runs once per RUN", async () => {
    const other = await world();
    setKube(other, { ...RAW, uid: "00000000-0000-4000-8000-0000000000ff" }, { line: identityLine(other) });
    expect(await run(other, "resume", resumeInputs)).toMatchObject({ code: 2, stderr: "GAP: pod-uid-mismatch [owner: fabric-v2]\n" });
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    expect((await run(w, "resume", resumeInputs)).code).toBe(0);
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stdout: "", stderr: "GAP: evidence-write-failed [owner: fabric-v2]\n" });
  });
});

// The contract with A's OWN side (#1609): A's final-seal producer, A's request encoder and A's receipt checks and resume
// consumer, run from A's published code. A's code is not in this repository: D3_A_RESUME_CONSUMER names an external
// driver (`<driver> <D3_A_CODE> seal|request|consume`, one JSON object on stdin), D3_A_CODE A's code directory.
// Without both it is skipped; the lane runs it against A's published commit and records the result on the PR.
const A_CONSUMER = process.env.D3_A_RESUME_CONSUMER;
const A_CODE = process.env.D3_A_CODE;
describe.skipIf(!linux || !A_CONSUMER || !A_CODE)("the resume contract against A's own seal, request and consumer (external)", () => {
  const a = async (command: string, input: unknown): Promise<Buffer> => {
    const { spawnSync } = await import("node:child_process");
    const result = spawnSync("python3", [A_CONSUMER!, A_CODE!, command], { input: JSON.stringify(input) });
    if (result.status !== 0) throw new Error(`A's ${command}: ${result.stderr.toString()}`);
    return result.stdout;
  };
  const ARGS = ["--session", POD_JOURNAL, "--thinking", "high"];
  /** Pi's own startup append (session-manager: exactly these five fields, an 8-hex id, a millisecond timestamp). */
  const piThinking = (id: string, parentId: string, thinkingLevel: string) =>
    `${JSON.stringify({ type: "thinking_level_change", id, parentId, timestamp: "2026-10-25T00:00:03.123Z", thinkingLevel })}\n`;

  /** Seal the host journal with A's producer, resume through the seam with A's request bytes, consume with A. */
  const contract = async (sealedEntries: string, appended: string) => {
    const w = await world();
    const source = JSON.parse(fs.readFileSync(path.join(w.seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
    const sealed = Buffer.from(`${fs.readFileSync(source, "utf8").split("\n")[0]}\n${sealedEntries}`);
    fs.writeFileSync(source, sealed);
    fs.writeFileSync(w.journalFile, Buffer.concat([sealed, Buffer.from(appended)]));
    const sealBytes = await a("seal", { journal: source, sessionId: SID, run: w.seat.run });
    const sealFile = path.join(w.seat.runDir, "receipts", "final-seal.json");
    fs.chmodSync(sealFile, 0o600); fs.writeFileSync(sealFile, sealBytes); fs.chmodSync(sealFile, 0o400);
    const journalPath = path.join(w.seat.runDir, "seat-journal.jsonl");
    fs.writeFileSync(journalPath, fs.readFileSync(journalPath, "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>)
      .map(record => record.event === "final-seal" ? { ...record, sha256: sha(sealBytes) } : record).map(record => `${JSON.stringify(record)}\n`).join(""));
    const final = JSON.parse(sealBytes.toString("utf8")) as { leafId: string; journalBytes: number; sealHasThinkingEntry: boolean };
    setKube(w, RAW, { line: identityLine(w, { journal: journalBranch(w.journalFile, final.leafId, final.journalBytes) }) }, { args: ARGS });
    const requestBytes = await a("request", { run: w.seat.run, runSha256: w.seat.runSha256, principal: "d3synth", sessionId: SID, pod: RAW,
      finalSeal: final });
    const seam = await run(w, "resume", {}, {}, requestBytes);
    const consumed = JSON.parse((await a("consume", { code: seam.code, stdout: seam.stdout, stderr: seam.stderr, run: w.seat.run, sessionId: SID,
      pod: RAW, final, args: ARGS })).toString("utf8")) as { ok: boolean; gap?: string; identity?: Record<string, unknown> };
    return { final, seam, consumed };
  };

  it("a sealed branch WITH a thinking entry and no append: the seam passes, and A accepts it with the sealed leaf", async () => {
    const { final, seam, consumed } = await contract(`${entry("e1", null)}\n${piThinking("e2", "e1", "medium")}${entry("e3", "e2")}\n`, "");
    expect(final.sealHasThinkingEntry).toBe(true);
    expect(seam.receipt).toMatchObject({ result: "pass", observed: { leafId: "e3", appendedSinceSeal: [], suffixBase64: "" } });
    expect(consumed).toMatchObject({ ok: true, identity: { sealLeafId: "e3", leafId: "e3", appendBytes: 0,
      thinkingLevelPolicy: "sealed-branch-has-thinking-entry" } });
  });

  it("a sealed branch WITHOUT one and Pi's one thinking_level_change: the seam passes, and A accepts the new leaf", async () => {
    const append = piThinking("a1b2c3d4", "e2", "high");
    const { final, seam, consumed } = await contract(`${entry("e1", null)}\n${entry("e2", "e1")}\n`, append);
    expect(final.sealHasThinkingEntry).toBe(false);
    expect(seam.receipt).toMatchObject({ result: "pass", observed: { leafId: "a1b2c3d4", suffixBase64: Buffer.from(append).toString("base64"),
      appendedSinceSeal: [{ id: "a1b2c3d4", parentId: "e2", type: "thinking_level_change", thinkingLevel: "high" }] } });
    expect(consumed).toMatchObject({ ok: true, identity: { sealLeafId: "e2", leafId: "a1b2c3d4", appendBytes: Buffer.byteLength(append),
      effectiveThinkingLevel: "high" } });
  });

  it("no thinking entry and NO append is not a completed resume: BOTH sides refuse it", async () => {
    const { seam, consumed } = await contract(`${entry("e1", null)}\n${entry("e2", "e1")}\n`, "");
    expect(seam).toMatchObject({ code: 2, stdout: "", stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
    expect(consumed).toEqual({ ok: false, gap: "seam-refused-or-unknown:resume [owner: fabric-v2]" });
  });
});

// The contract with the REAL pinned Pi (#1609 ruling): Pi opens an existing session whose branch has no
// thinking_level_change and appends exactly one, before any prompt. D3_PINNED_PI names the pinned Pi package's CLI
// (`<package>/dist/bundle/cli.js`); without it this is skipped. A Pi upgrade that changes the append fails here loudly.
const PINNED_PI = process.env.D3_PINNED_PI;
describe.skipIf(!linux || !PINNED_PI)("the resume contract against the REAL pinned Pi (external)", () => {
  /** A sealed older session, as Pi writes it: a header and one user message, no thinking_level_change. */
  const sealedFor = (project: string) => `${JSON.stringify({ type: "session", version: 3, id: SID, timestamp: "2026-10-25T00:00:00.000Z",
    cwd: project })}\n${JSON.stringify({ type: "message", id: "e1", parentId: null, timestamp: "2026-10-25T00:00:01.000Z",
    message: { role: "user", content: [{ type: "text", text: "hello" }], timestamp: 1792886401000 } })}\n`;
  const scene = () => {
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "real-pi-")));
    const dirs = { home: path.join(base, "home"), agent: path.join(base, "agent"), project: path.join(base, "project") };
    for (const dir of Object.values(dirs)) fs.mkdirSync(dir, { mode: 0o700 });
    return { ...dirs, sealed: sealedFor(dirs.project) };
  };
  /** The Pod's Pi starting on the session: the real CLI, isolated and offline, RPC mode, no prompt, stdin closed. */
  const piResume = async (file: string, dirs: { home: string; agent: string; project: string }) => {
    const { spawnSync } = await import("node:child_process");
    const result = spawnSync(process.execPath, [PINNED_PI!, "--session", file, "--mode", "rpc", "--thinking", "high"], {
      cwd: dirs.project, input: "", timeout: 60_000, encoding: "utf8",
      env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: dirs.home, PI_CODING_AGENT_DIR: dirs.agent, PI_OFFLINE: "1",
        PI_SKIP_VERSION_CHECK: "1", ...(process.env.NODE_OPTIONS ? { NODE_OPTIONS: process.env.NODE_OPTIONS } : {}) } });
    if (result.status !== 0) throw new Error(`pinned Pi exited ${result.status}: ${result.stderr}`);
  };

  it("Pi appends exactly ONE thinking_level_change on the sealed leaf, and the seam's resume passes over it", async () => {
    const s = scene();
    const w = await world({ sealed: s.sealed });
    await piResume(w.journalFile, s);
    const after = fs.readFileSync(w.journalFile, "utf8");
    expect(after.startsWith(s.sealed)).toBe(true);
    const suffix = after.slice(s.sealed.length);
    expect(suffix.endsWith("\n")).toBe(true);
    expect(suffix.trim().split("\n")).toHaveLength(1);
    const appended = JSON.parse(suffix) as Record<string, unknown>;
    expect(Object.keys(appended).sort()).toEqual(["id", "parentId", "thinkingLevel", "timestamp", "type"]);
    expect(appended).toMatchObject({ type: "thinking_level_change", parentId: "e1", id: expect.stringMatching(/^[0-9a-f]{8}$/),
      timestamp: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/), thinkingLevel: expect.any(String) });
    setKube(w, RAW, { line: identityLine(w, { journal: journalBranch(w.journalFile, "e1", Buffer.byteLength(s.sealed)) }) });
    const result = await run(w, "resume", resumeInputs);
    expect(result.receipt).toMatchObject({ result: "pass", observed: { leafId: appended.id, suffixBase64: Buffer.from(suffix).toString("base64"),
      appendedSinceSeal: [{ id: appended.id, parentId: "e1", type: "thinking_level_change", thinkingLevel: appended.thinkingLevel }] } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json").measured.observations).toBe(1);
  }, 90_000);

  it("before Pi has opened the session (zero appends), the seam refuses after the bounded wait", async () => {
    const s = scene();
    const w = await world({ sealed: s.sealed });
    setKube(w, RAW, { line: identityLine(w, { journal: journalBranch(w.journalFile, "e1", Buffer.byteLength(s.sealed)) }) });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
    // The same file, once the real Pi has opened it, passes: the refusal was only "not resumed yet".
    await piResume(w.journalFile, s);
    const again = await world({ sealed: s.sealed, podJournal: fs.readFileSync(w.journalFile, "utf8") });
    setKube(again, RAW, { line: identityLine(again, { journal: journalBranch(again.journalFile, "e1", Buffer.byteLength(s.sealed)) }) });
    expect((await run(again, "resume", resumeInputs)).receipt).toMatchObject({ result: "pass" });
  }, 90_000);
});

const owner = { template: { name: "t", uid: "00000000-0000-4000-8000-000000000101" }, claim: { name: "c", uid: "00000000-0000-4000-8000-000000000102" },
  sandbox: { name: "s", uid: "00000000-0000-4000-8000-000000000103" }, pod: MANAGED };
const MACHINE = "sp-org-d3synth";
const routable = (incarnation = INCARNATION, machine = MACHINE) => ({ ok: true, verdict: "routable", side: "nats",
  file: { remoteHost: "v2", live: true, ownerIncarnation: incarnation }, nats: { read: true, records: [{ machine, live: true, ownerIncarnation: incarnation }] } });
const gateLive = (side: "file" | "nats", machine?: string) => ({ side, key: `session:${SID}`, kind: "root", status: "live", id: `session:${SID}`,
  ...(machine ? { machine } : {}) });

describe.skipIf(!linux)("b6 bridge exec diagnostics (a20): never discard a failed exec", () => {
  it.each(["resume", "publish"])("%s preserves JSON errors or non-JSON first lines and both sanitized streams", async phase => {
    for (const json of [true, false]) {
      const w = await world();
      const cause = 'transport denied token="never-log-this"';
      const line = json ? JSON.stringify({ error: cause }) : `${cause}\nnot the first line`;
      const stderr = 'Forbidden Authorization: Bearer never-log-this\nhttps://u:never-log-this@cluster.test/?token=never-log-this';
      setKube(w, phase === "resume" ? RAW : MANAGED, { line, code: 1, stderr });
      const before = fs.readFileSync(w.config);
      expect(await run(w, phase, phase === "resume" ? resumeInputs : { owner })).toMatchObject({ code: 0, receipt: { result: "refused" } });
      const outcome = evidenceOf(w, `fabric-v2-bridge-${phase}-outcome.json`);
      expect(outcome.errorClass).toBe("KubectlExecError");
      expect(outcome.measured).toMatchObject({ execCode: 1, execStderr: expect.stringContaining("Forbidden") });
      expect(outcome.measured.execStdout).toContain("transport denied");
      expect(outcome.measured.execStdout).not.toContain("not the first line");
      expect(JSON.parse(outcome.errorMessage)).toEqual({ execCode: 1, stdout: outcome.measured.execStdout, stderr: outcome.measured.execStderr });
      expect(JSON.stringify(outcome)).not.toContain("never-log-this");
      expect(fs.readFileSync(w.config)).toEqual(before);
      expect(fs.existsSync(w.marker)).toBe(false);
    }
  });

  it.each(["resume", "publish"])("%s redacts before the 500-character bound, including a credential crossing it", async phase => {
    const w = await world();
    const text = 'prefix password="' + "secret-value".repeat(100) + '" ' + "x".repeat(1000);
    setKube(w, phase === "resume" ? RAW : MANAGED, { line: text, code: 1, stderr: text });
    await run(w, phase, phase === "resume" ? resumeInputs : { owner });
    const outcome = evidenceOf(w, `fabric-v2-bridge-${phase}-outcome.json`);
    expect(outcome.measured.execStdout).toHaveLength(500);
    expect(outcome.measured.execStderr).toHaveLength(500);
    expect(outcome.measured.execStdout).toContain('password=***');
    expect(JSON.stringify(outcome)).not.toContain("secret-value");
  });

  it("retains the initial publish exec failure even when a later owner read succeeds", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: JSON.stringify({ error: "pod-owner-unreadable" }), code: 1, stderr: "startup projection not yet present" });
    w.receipt = routable(); w.gate = gateLive("nats", MACHINE);
    const result = await run(w, "publish", { owner }, { roots: async () => {
      setKube(w, MANAGED, { line: identityLine(w) });
      return [{ id: `session:${SID}`, machine: MACHINE, ownerHostId: `session:${SID}`, ownerIdentityId: `session:${SID}`,
        ownerIncarnation: INCARNATION, live: true }] as never;
    } });
    expect(result.receipt.result).toBe("pass");
    const outcome = evidenceOf(w, "fabric-v2-bridge-publish-outcome.json");
    expect(outcome.measured.execs).toHaveLength(2);
    expect(outcome.measured.execs[0]).toMatchObject({ execCode: 1, execStderr: "startup projection not yet present" });
    expect(outcome.measured.execs[1]).toMatchObject({ execCode: 0, execStderr: "" });
    expect(outcome.errorClass).toBe("KubectlExecError");
    expect(JSON.parse(outcome.errorMessage)).toMatchObject({ execCode: 1, stdout: '{"error":"pod-owner-unreadable"}' });
  });

  it.each(["resume", "publish"])("%s persists failed exec evidence when the after-read API throws", async phase => {
    const w = await world();
    setKube(w, phase === "resume" ? RAW : MANAGED, { line: "", code: 1, stderr: "Forbidden pods/exec" });
    const actual = kube.readPod;
    vi.spyOn(kube, "readPod").mockImplementationOnce(actual).mockRejectedValue(new SeamGap("after-read-failed"));
    expect(await run(w, phase, phase === "resume" ? resumeInputs : { owner })).toMatchObject({ code: 2 });
    expect(evidenceOf(w, `fabric-v2-bridge-${phase}-outcome.json`)).toMatchObject({ reason: "after-read-failed",
      errorClass: "KubectlExecError", measured: { execCode: 1, execStdout: "", execStderr: "Forbidden pods/exec" } });
  });

  it.each(["resume", "publish"])("%s records stderr on successful exec too, without changing the resume/publish verdict", async phase => {
    const w = await world();
    setKube(w, phase === "resume" ? RAW : MANAGED, { line: identityLine(w), stderr: "benign kubectl warning" });
    w.receipt = routable(); w.gate = gateLive("nats", MACHINE);
    expect(await run(w, phase, phase === "resume" ? resumeInputs : { owner })).toMatchObject({ code: 0, receipt: { result: "pass" } });
    expect(evidenceOf(w, `fabric-v2-bridge-${phase}-outcome.json`).measured.execs[0])
      .toMatchObject({ execCode: 0, execStderr: "benign kubectl warning" });
  });
});

describe.skipIf(!linux)("the shared Pod identity startup wait before publish effects", () => {
  it("resume startup regression: publish waits for its pinned managed Pod and rechecks local process identity against live presence", async () => {
    const w = await world();
    const before = fs.readFileSync(w.config);
    setKube(w, MANAGED, { line: identityLine(w) }, { running: false, terminated: null });
    w.receipt = routable();
    w.gate = gateLive("nats", MACHINE);
    let t = 0;
    let polls = 0;
    const sleep = vi.fn(async (ms: number) => {
      expect(fs.readFileSync(w.config)).toEqual(before);
      expect(fs.existsSync(w.marker)).toBe(false);
      t += ms;
      if (++polls === 2) setKube(w, MANAGED, { line: identityLine(w) });
    });
    expect(await run(w, "publish", { owner }, { now: () => t, sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "pass" } });
    expect(sleep).toHaveBeenCalledTimes(2);
    expect(t).toBeGreaterThan(0);
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "pass", written: true, elapsedMs: t,
      measured: { observations: 3, waitedMs: t } });
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(2);
    expect(fs.readFileSync(w.config)).not.toEqual(before);
    expect(fs.readFileSync(w.marker, "utf8")).toBe("--user restart smarty-nats-link.service\n");
  });

  it("resume startup regression: publish neverstarts refuses normally within 180 s without writing or restarting", async () => {
    const w = await world();
    const before = fs.readFileSync(w.config);
    setKube(w, MANAGED, { line: identityLine(w) }, { running: false, terminated: null });
    let t = 0;
    const sleep = vi.fn(async () => { t += 30_000; });
    expect(await run(w, "publish", { owner }, { now: () => t, sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(t).toBe(180_000);
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "pod-not-running",
      written: false, elapsedMs: t, measured: { waitedMs: t } });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json").measured.observations).toBeGreaterThan(1);
    expect(sleep).toHaveBeenCalled();
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
  });

  it("resume startup regression: publish startup time counts against the later route wait's original phase deadline", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) }, { running: false, terminated: null });
    let t = 0;
    let startupMs = 0;
    const sleep = vi.fn(async (ms: number) => {
      t += ms;
      if (startupMs === 0) { startupMs = t; setKube(w, MANAGED, { line: identityLine(w) }); }
    });
    expect(await run(w, "publish", { owner }, { deadlineMs: 5_000, now: () => t, sleep }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(startupMs).toBeGreaterThan(0);
    expect(t).toBe(5_000);
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "seat-not-routable",
      written: true, elapsedMs: 5_000, measured: { observations: 2, waitedMs: startupMs } });
  });

  it("resume startup regression: a publish reload timer throw after INTENT attempts one outcome and keeps its GAP", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.holderStuck = true;
    const error = new Error("synthetic publish reload timer failure");
    const onError = vi.fn();
    const open = vi.spyOn(fs, "openSync");
    const outcomeFile = path.join(w.seat.runDir, "fabric-v2-bridge-publish-outcome.json");
    expect(await run(w, "publish", { owner }, { sleep: async () => { throw error; }, onError }))
      .toMatchObject({ code: 2, stdout: "", stderr: "GAP: seam-internal [owner: fabric-v2]\n" });
    expect(onError).toHaveBeenCalledWith(error);
    expect(open.mock.calls.filter(([file, flags]) => file === outcomeFile && typeof flags === "number" && (flags & fs.constants.O_EXCL) !== 0))
      .toHaveLength(1);
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "seam-internal", written: true,
      errorClass: "Error", errorMessage: error.message,
      intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-publish-intent.json"))) });
  });
});

describe.skipIf(!linux)("bridge publish: the seat's one allow-list entry, a link restart, and the read-backs", () => {
  it.each(["missing", "no incarnation", "different incarnation", "stale", "other machine", "other owner"])(
    "b5: presence %s refuses; no unapproved no-incarnation fallback", async fault => {
      const w = await world();
      setKube(w, MANAGED, { line: identityLine(w) });
      w.gate = gateLive("nats", MACHINE); w.receipt = routable();
      const record = { id: `session:${SID}`, machine: MACHINE, ownerHostId: `session:${SID}`, ownerIdentityId: `session:${SID}`,
        ownerIncarnation: INCARNATION, live: true };
      if (fault === "no incarnation") record.ownerIncarnation = "";
      if (fault === "different incarnation") record.ownerIncarnation = V1_INCARNATION;
      if (fault === "stale") record.live = false;
      if (fault === "other machine") record.machine = "sandbox1";
      if (fault === "other owner") record.ownerHostId = "other-host";
      expect(await run(w, "publish", { owner }, { deadlineMs: 5_000, roots: async () => fault === "missing" ? [] : [record] as never }))
        .toMatchObject({ code: 0, receipt: { result: "refused" } });
      expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "seat-not-routable", errorClass: null,
        errorMessage: null, measured: { incarnationSource: "pod-local-nats-projection-and-nats-presence" } });
    });

  it("b5: live presence incarnation matches this Pod's locally process-bound projection", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.gate = gateLive("nats", MACHINE); w.receipt = routable();
    expect(await run(w, "publish", { owner })).toMatchObject({ code: 0, receipt: { result: "pass" } });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ measured: {
      incarnationSource: "pod-local-nats-projection-and-nats-presence", presence: [{ ownerIncarnation: INCARNATION, live: true }] },
      asserted: { identity: { ownerIncarnation: INCARNATION, pid: 7, processStartedAt: "123" } } });
    const exec = w.calls().find(call => call.argv.includes("/usr/bin/python3"))!;
    expect(exec.argv.slice(exec.argv.indexOf("--"))).toEqual(["--", ...podOwnerArgv("/var/lib/fabric/mesh", SID)]);
  });

  it("passes when the real seat route becomes live at +659 s, before its 780 s cap", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "publish", { owner }, { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 659_000) { w.gate = gateLive("nats", MACHINE); w.receipt = routable(); }
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 659_000,
      measured: { gate: { status: "live", side: "nats" }, receipt: { verdict: "routable" } } });
    expect(t).toBe(659_000);
  });

  it("refuses publish when the seat route would appear only after its 780 s cap", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    let t = 0;
    const availableAt = 821_000;
    const result = await run(w, "publish", { owner }, { now: () => t, sleep: async ms => {
      t += ms;
      if (t >= availableAt) { w.gate = gateLive("nats", MACHINE); w.receipt = routable(); }
    } });
    expect(result.receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "seat-not-routable",
      elapsedMs: 780_000, measured: { gate: null, receipt: {} } });
    expect(t).toBe(780_000);
    expect(t).toBeLessThan(availableAt);
  });

  it("waits beyond 120 s for another publish route to recover", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.gate = gateLive("nats", MACHINE);
    w.receipt = routable();
    w.statusAfter = { ok: true, problems: [], allow: { file: [{ identity: LEAD, live: false },
      { identity: `session:${SID}`, live: true }], nats: [] } };
    let t = 0;
    const result = await run(w, "publish", { owner }, { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 121_000) w.statusAfter!.allow.file[0]!.live = true;
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 121_000,
      measured: { otherRoutes: { checked: [LEAD], down: [] } } });
    expect(t).toBe(121_000);
  });

  it("shares the phase deadline when the seat route consumes most of it", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.statusAfter = { ok: true, problems: [], allow: { file: [{ identity: LEAD, live: false },
      { identity: `session:${SID}`, live: true }], nats: [] } };
    let t = 0;
    const result = await run(w, "publish", { owner }, { deadlineMs: 500_000, now: () => t, sleep: async ms => {
      t += ms;
      if (t === 470_000) { w.gate = gateLive("nats", MACHINE); w.receipt = routable(); }
    } });
    expect(result.receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "other-routes-down",
      elapsedMs: 500_000, measured: { otherRoutes: { checked: [LEAD], down: [LEAD] }, gate: { status: "live" }, receipt: { verdict: "routable" } } });
    expect(t).toBe(500_000);
  });
  it("moves the seat's entry, restarts the pinned unit, and passes on the proof chain (incarnation = the managed Pod UID)", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.receipt = routable();
    w.gate = gateLive("nats", MACHINE);
    const result = await run(w, "publish", { owner });
    const config = JSON.parse(fs.readFileSync(w.config, "utf8"));
    expect(config.allow).toEqual({ file: [leadEntry], nats: [{ kind: "root", id: `session:${SID}`, principal: "Delta", machine: MACHINE,
      send: ["followUp"], receive: ["followUp", "cancel"] }] });
    // #1609 review P0-1: every reader after the swap got the config ON DISK (the seat on allow.nats, gone from allow.file).
    const last = w.seenConfigs.at(-1)!.allow as { file: Array<{ id: string }>; nats: Array<{ id: string }> };
    expect(last.nats.map(entry => entry.id)).toEqual([`session:${SID}`]);
    expect(last.file.map(entry => entry.id)).toEqual([LEAD]);
    expect(fs.readFileSync(w.marker, "utf8")).toBe("--user restart smarty-nats-link.service\n");
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: w.seat.run, phase: "publish", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, incarnation: MANAGED.uid, route: `v2@${MANAGED.uid}:${sha(fs.readFileSync(w.config)).slice(0, 12)}` } });
    const intent = evidenceOf(w, "fabric-v2-bridge-publish-intent.json");
    expect(intent.plan).toMatchObject({ change: "move", fileEntry: { index: 1, entry: seatFileEntry } });
    expect(intent.rollback).toMatchObject({ beforeSha256: sha(Buffer.from(intent.plan.beforeBytes)), afterSha256: sha(fs.readFileSync(w.config)) });
  });

  it("b5: piRunning before Fabric activation waits for presence, then reads the new local process-bound projection", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: JSON.stringify({ error: "pod-owner-unreadable" }), code: 1 });
    w.receipt = routable();
    w.gate = gateLive("nats", MACHINE);
    const result = await run(w, "publish", { owner }, { roots: async () => {
      setKube(w, MANAGED, { line: identityLine(w) });
      return [{ id: `session:${SID}`, machine: MACHINE, ownerHostId: `session:${SID}`,
        ownerIdentityId: `session:${SID}`, ownerIncarnation: INCARNATION, live: true }] as never;
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(w.calls().filter(call => call.argv.includes("/usr/bin/python3"))).toHaveLength(2);
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({
      measured: { incarnationSource: "pod-local-nats-projection-and-nats-presence" }, asserted: { identity: { ownerIncarnation: INCARNATION } } });
  });

  it("b5: a thrown initial publish Pod read still records one intent-linked refusal outcome", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    const before = fs.readFileSync(w.config);
    vi.spyOn(kube, "readPod").mockRejectedValue(new SeamGap("pod-read-failed"));
    expect(await run(w, "publish", { owner })).toMatchObject({ code: 2, stdout: "", stderr: "GAP: pod-read-failed [owner: fabric-v2]\n" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "pod-read-failed",
      written: false, errorClass: "SeamGap", errorMessage: "pod-read-failed",
      intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-publish-intent.json"))) });
    expect(fs.readFileSync(w.config)).toEqual(before);
  });

  it("#1609 review P1-4: a route the link's REAL identity gate refuses (an ambiguous id) is refused, never passed", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.receipt = routable();
    w.gate = { ...gateLive("nats", MACHINE), status: "BRIDGE_NATS_AMBIGUOUS", reason: "answers on more than one machine" };
    expect((await run(w, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "seat-route-refused-by-gate",
      measured: { gate: { status: "BRIDGE_NATS_AMBIGUOUS" } } });
    // The same snapshot with a second NATS record (a stale one) is no single route either.
    const two = await world();
    setKube(two, MANAGED, { line: identityLine(two) });
    two.gate = gateLive("nats", MACHINE);
    two.receipt = { ...routable(), nats: { read: true, records: [{ machine: "hub-a-bridge", live: false, ownerIncarnation: V1_INCARNATION },
      { machine: MACHINE, live: true, ownerIncarnation: INCARNATION }] } };
    expect((await run(two, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
  });

  it("#1609 review P1-3 / A T-FIX6: a run without natsMachine is UNKNOWN; the Herdr instance is never used as the machine", async () => {
    const w = await world({ machines: ["hub-a-bridge", "sandbox1", "sp-org-d3synth", "org-d3synth"] });
    resealRun(w, run => { delete run.natsMachine; });
    setKube(w, MANAGED, { line: identityLine(w) }, { natsMachine: "org-d3synth" });
    const before = fs.readFileSync(w.config);
    expect(await run(w, "publish", { owner })).toMatchObject({ code: 2, stdout: "", stderr: "GAP: seat-nats-machine-unpinned [owner: fabric-v2]\n" });
    expect(fs.readFileSync(w.config)).toEqual(before);
  });

  it("#1609 review P1-3: the machine is the run's natsMachine, and the managed Pod's own NATS config must name it", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) }, { natsMachine: "org-d3synth" });
    const before = fs.readFileSync(w.config);
    expect((await run(w, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "pod-nats-machine-mismatch", written: false });
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
  });

  it("a machine outside nats.pathBMachines is UNKNOWN before any write or restart", async () => {
    const w = await world({ machines: ["hub-a-bridge", "sandbox1"] });
    setKube(w, MANAGED, { line: identityLine(w) });
    const before = fs.readFileSync(w.config);
    expect(await run(w, "publish", { owner })).toMatchObject({ code: 2, stderr: "GAP: seat-machine-not-path-b [owner: fabric-v2]\n" });
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
  });

  it("an unreadable managed Pod Main writes nothing and restarts nothing", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: "not json", code: 1 });
    const before = fs.readFileSync(w.config);
    expect((await run(w, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
  });

  it("refuses when NATS or the hub mirror carries another incarnation, or the presence is on another machine", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.gate = gateLive("nats", MACHINE);
    w.receipt = routable("00000000-0000-4000-8000-0000000009ff");
    expect((await run(w, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "seat-not-routable", written: true });
    const other = await world();
    setKube(other, MANAGED, { line: identityLine(other) });
    other.gate = gateLive("nats", MACHINE);
    other.receipt = routable(INCARNATION, "sandbox1");
    expect((await run(other, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
  });

  it("refuses when the link did not reload, or another route it carried is not back", async () => {
    const stuck = await world();
    setKube(stuck, MANAGED, { line: identityLine(stuck) });
    stuck.gate = gateLive("nats", MACHINE);
    stuck.receipt = routable();
    stuck.holderStuck = true;
    let reloadTime = 0;
    expect((await run(stuck, "publish", { owner }, { now: () => reloadTime, sleep: async ms => { reloadTime += ms; } }))
      .receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(stuck, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "link-not-reloaded",
      measured: { reload: { waitedMs: 120_000 } } });
    expect(reloadTime).toBe(120_000);
    const down = await world();
    setKube(down, MANAGED, { line: identityLine(down) });
    down.gate = gateLive("nats", MACHINE);
    down.receipt = routable();
    // Live before the restart; after it, the other seat's route stays down.
    down.statusAfter = { ok: true, problems: [], allow: { file: [{ identity: LEAD, live: false }], nats: [{ identity: `session:${SID}`, live: true }] } };
    let t = 0;
    expect((await run(down, "publish", { owner }, { now: () => t, sleep: async ms => { t += ms; } }))
      .receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(down, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "other-routes-down", elapsedMs: 480_000,
      measured: { otherRoutes: { checked: [LEAD], down: [LEAD] } } });
    expect(t).toBe(480_000);
  });
});

describe.skipIf(!linux)("the REAL bridge bundle: built as scripts/build.mjs builds it, installed with its pins, run as A runs it", () => {
  it("pins the interpreter, the link config and the unit into the bytes; uninstalled refuses; the lock proof holds", async () => {
    const w = await world();
    const { build } = await import("esbuild");
    const bundle = path.join(w.seat.base, "fabric-seat-bridge.cjs");
    await build({ entryPoints: ["src/seat-bridge/entry.ts"], outfile: bundle, bundle: true, platform: "node", format: "cjs", target: "node24",
      logLevel: "silent" });
    const { spawnSync } = await import("node:child_process");
    // A runs seams with a fixed env (no NODE_OPTIONS from this test runner). HOME is an empty directory here: nothing
    // may fall back to a host file under the real home (a host's org.json above all).
    const emptyHome = path.join(w.seat.base, "empty-home");
    fs.mkdirSync(emptyHome, { mode: 0o700 });
    const env = { PATH: process.env.PATH ?? "/usr/bin:/bin", LANG: "C", LC_ALL: "C", HOME: emptyHome };
    const uninstalled = spawnSync("node", [bundle, "--run-dir", w.seat.runDir, "--phase", "resume"], { encoding: "utf8", input: "{}", env });
    expect(uninstalled).toMatchObject({ status: 2, stdout: "", stderr: "GAP: seam-not-installed [owner: fabric-v2]\n" });
    const out = path.join(w.seat.base, "fabric-seat-bridge");
    const installed = spawnSync("node", [bundle, "--install", "--out", out, "--link-config", w.config, "--link-unit", "smarty-nats-link.service"],
      { encoding: "utf8", env });
    expect(installed.status).toBe(0);
    const printed = JSON.parse(installed.stdout) as Record<string, unknown>;
    expect(printed).toMatchObject({ path: out, sha256: sha(fs.readFileSync(out)), owner: "fabric-v2", linkConfig: w.config,
      linkUnit: "smarty-nats-link.service" });
    const bytes = fs.readFileSync(out, "utf8");
    expect(bytes).toContain(JSON.stringify(w.config));
    expect(bytes).not.toContain("__FABRIC_SEAT_BRIDGE_");
    expect(fs.statSync(out).mode & 0o777).toBe(0o500);
    // The test-only deps (the in-process link restart, the one durable state directory) are unreachable here: the seam's
    // main is called once, with the pins and no deps, and nothing in the bundle supplies either hook.
    const seamMain = /\bvar (\w+) = async \(argv\d*, io\d*, pins\d*, deps\d* = \{\}\) =>/.exec(bytes)?.[1];
    expect(seamMain).toBeDefined();
    expect([...bytes.matchAll(new RegExp(`\\b${seamMain}\\(([^;]*)\\);`, "g"))].map(call => call[1]))
      .toEqual(["argv2, io, { linkConfig: PINNED_LINK_CONFIG, linkUnit: PINNED_LINK_UNIT }"]);
    expect(bytes).not.toMatch(/\brestartLink\s*:/);
    expect(bytes).toMatch(/\bdeps\.restartLink\b/);
    expect([...bytes.matchAll(/\bdurableStateDir\s*:\s*([\w.]+)/g)].map(match => match[1]).sort())
      .toEqual(["context.deps.durableStateDir", "options.durableStateDir"]);
    // A bad unit name, or a link whose state lives in a temporary store, is refused at install.
    expect(spawnSync("node", [bundle, "--install", "--out", `${out}-2`, "--link-config", w.config, "--link-unit", "x;y"], { env }).status).toBe(2);
    const temporary = path.join(path.dirname(w.config), "temporary.json");
    fs.writeFileSync(temporary, JSON.stringify({ ...JSON.parse(fs.readFileSync(w.config, "utf8")),
      stateDir: "/srv/scratch/agent/smarty-pi-tmp/pi-0000/nats-link-state/v2" }), { mode: 0o600 });
    const refused = spawnSync("node", [bundle, "--install", "--out", `${out}-3`, "--link-config", temporary, "--link-unit", "smarty-nats-link.service"],
      { env, encoding: "utf8" });
    expect(refused).toMatchObject({ status: 2, stderr: "install: link-state-dir-temporary\n" });
    expect(fs.existsSync(`${out}-3`)).toBe(false);
    const { runSeam } = await import("./seat-drain-fixture.js");
    const inputs = writerInputs(w);
    const body = request(w.seat, "rc-restore-route", inputs);
    expect(await runSeam(w.seat, out, "rc-restore-route", body, true)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: lock-not-held [owner: fabric-v2]\n" });
    // With the lock, the installed seam reads ITS pinned config, the seat pins from the config's orgJson and the seat's
    // mesh, then stops on a link it cannot read: in A's environment (no HOME), and with HOME an empty directory.
    expect(await runSeam(w.seat, out, "rc-restore-route", body)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: link-status-unreadable [owner: fabric-v2]\n" });
    expect(await runSeam(w.seat, out, "rc-restore-route", body, false, emptyHome)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: link-status-unreadable [owner: fabric-v2]\n" });
    expect(fs.readdirSync(emptyHome)).toEqual([]);
    // The counterexample: a config without orgJson falls back to the factory paths under HOME, finds nothing in the
    // empty one, and refuses (it reached no host org.json).
    const unpinned = path.join(path.dirname(w.config), "unpinned.json");
    const { orgJson: _orgJson, ...withoutOrg } = JSON.parse(fs.readFileSync(w.config, "utf8")) as Record<string, unknown>;
    fs.writeFileSync(unpinned, JSON.stringify(withoutOrg), { mode: 0o600 });
    expect(spawnSync("node", [bundle, "--install", "--out", `${out}-4`, "--link-config", unpinned, "--link-unit", "smarty-nats-link.service"],
      { env }).status).toBe(0);
    expect(await runSeam(w.seat, `${out}-4`, "rc-restore-route", body, false, emptyHome)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: seat-pins-unreadable [owner: fabric-v2]\n" });
  }, 60_000);
});

interface A19CapturedFile { path: string; sha256: string; bytes: number; mode: string; uid: number; base64: string }
const a19Asset = () => JSON.parse(fs.readFileSync(path.join(import.meta.dirname, "fixtures", "seat-bridge-a19-replay.json"), "utf8")) as {
  schema: string; provenance: { inputSHA: string; liveCaptureSHA: string; legacyV1Commit: string }; files: Record<string, A19CapturedFile>;
  request: { base64: string; sha256: string; exactMatch: boolean; journalRequestSha256: string[] };
  liveCapture: { bundle: A19CapturedFile; mainDrainStatusReply: A19CapturedFile; natsLinkStatus: A19CapturedFile;
    processSubstitution: { field: string; captured: number; reboundTo: number; explanation: string } };
};
const a19Decode = (entry: A19CapturedFile): Buffer => {
  const bytes = Buffer.from(entry.base64, "base64");
  expect(sha(bytes)).toBe(entry.sha256);
  expect(bytes.length).toBe(entry.bytes);
  return bytes;
};

// Check the ACTUAL client acceptance boundary, not merely what a direct mock can return. This socket and all
// captured filesystem replicas stay under createSeat's test-owned temporary root. Peer liveness is modeled below.
const a19WireReply = async (base: string, sessionId: string, wire: unknown, verifyRefusal?: string) => {
  const directory = path.join(base, "wire");
  fs.mkdirSync(directory, { mode: 0o700 });
  const socket = path.join(directory, "status.sock");
  const server = net.createServer(client => { client.once("data", () => client.end(`${JSON.stringify(wire)}\n`)); });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(socket, resolve); });
  try {
    return { reply: await mainClient.callMainDrain(socket, sessionId, "status", null, 2_000,
      verifyRefusal ? async () => verifyRefusal : undefined) };
  } catch (error) { return { error }; }
  finally { await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())); }
};

interface A19Scenario { shape: string; reason: string | null; clientError?: string; capture?: "raw" | "pid-rebound"; fault?: string }
const a19Cases: A19Scenario[] = [
  { shape: "captured legacy raw reply keeps its distinct PID and refuses writer mismatch", reason: "v1-writer-mismatch", capture: "raw" },
  { shape: "captured legacy PID-only substitution (explicit process substitution, no incarnation)", reason: null, capture: "pid-rebound" },
  ...[
    { fault: "reply root", reason: "v1-unreachable" },
    { fault: "owner pid", reason: "v1-writer-mismatch" },
    { fault: "owner root", reason: "v1-writer-mismatch" },
    { fault: "owner start time", reason: "v1-writer-mismatch" },
    { fault: "owner session file", reason: "v1-session-file-mismatch" },
    { fault: "wrong gate identity", reason: "v1-not-routable" },
    { fault: "gate not live", reason: "v1-not-routable" },
    { fault: "cross-side gate", reason: "v1-not-routable" },
    { fault: "cross-side receipt", reason: "v1-not-routable" },
    { fault: "file not live", reason: "v1-not-routable" },
    { fault: "file remote host", reason: "v1-not-routable" },
    { fault: "link incarnation mismatch", reason: "v1-not-routable" },
  ].map(value => ({ ...value, shape: `legacy captured ${value.fault}; PID-substituted`, capture: "pid-rebound" as const })),
  { shape: "legacy captured link incarnation match; PID-substituted", reason: null, capture: "pid-rebound", fault: "link incarnation match" },
  { shape: "legitimate matching v1 status", reason: null },
  { shape: "missing session", reason: null },
  { shape: "null session", reason: "v1-session-unreadable" },
  { shape: "numeric session", reason: "v1-session-unreadable" },
  { shape: "array session", reason: "v1-session-unreadable" },
  { shape: "missing file", reason: "v1-session-file-mismatch" },
  { shape: "null file", reason: "v1-session-file-mismatch" },
  { shape: "missing incarnation", reason: "v1-incarnation-unknown" },
  { shape: "null incarnation", reason: "v1-incarnation-unknown" },
  { shape: "numeric incarnation", reason: "v1-incarnation-unknown" },
  { shape: "object incarnation", reason: "v1-incarnation-unknown" },
  { shape: "wrong root", reason: "v1-unreachable" },
  { shape: "string pid", reason: "v1-writer-mismatch" },
  { shape: "error reply", reason: "v1-unreachable", clientError: "MAIN_DRAIN_REFUSED_MAIN_DRAIN_BUSY" },
  { shape: "null reply", reason: "v1-unreachable", clientError: "MAIN_DRAIN_REPLY_INVALID" },
  { shape: "missing native", reason: "v1-unreachable", clientError: "MAIN_DRAIN_REPLY_MISMATCH" },
  { shape: "invalid probes", reason: "v1-unreachable", clientError: "MAIN_DRAIN_REPLY_MISMATCH" },
  // A literal null local owner cannot arise from varying reply fields while retaining the captured owner bytes.
  // Its real verifier returns this refusal instead; do not bypass it and claim an impossible successful call.
  { shape: "owner unavailable at peer verification", reason: "v1-unreachable", clientError: "MAIN_DRAIN_OWNER_UNREADABLE" },
  { shape: "null route receipt", reason: "v1-not-routable" },
  { shape: "malformed route records", reason: "v1-not-routable" },
  { shape: "missing live gate", reason: "v1-not-routable" },
] as const;

// Recorded catch: a19 b3 refused a no-publish restore with seam-internal. A later actual pinned-v1 status
// capture lacks session entirely. Its unchanged PID MUST refuse against the older writer packet. The regression
// explicitly substitutes ONLY that volatile PID; it is not the unchanged historical reply. Gate/receipt are modeled.
describe.skipIf(!linux)("bridge real-a19 replay", () => {
  it.each(a19Cases)("real-a19 replay: $shape through the actual restore main", async scenario => {
    const asset = a19Asset();
    expect(asset.schema).toBe("seat-bridge-a19-replay-v1");
    expect(asset.provenance.inputSHA).toBe("76a40da18543ae2a30058094e0c690c8b9ecf40cb6ddd2d72720436818e65951");
    expect(asset.provenance.liveCaptureSHA).toBe("0e8af23ea620ec6297d7bf048493bba14a53f19b9e8dc488027ea0b2cf2ecd2f");
    expect(asset.provenance.legacyV1Commit).toBe("b7221c04ab7d40027af10b548d8cc2cb07e25480");
    const liveBundle = JSON.parse(a19Decode(asset.liveCapture.bundle).toString("utf8"));
    expect(asset.liveCapture.bundle.sha256).toBe(asset.provenance.liveCaptureSHA);
    const capturedStatusBytes = a19Decode(asset.liveCapture.mainDrainStatusReply);
    const capturedLinkBytes = a19Decode(asset.liveCapture.natsLinkStatus);
    expect(capturedStatusBytes.toString("utf8")).toBe(liveBundle.mainDrainStatusReply);
    expect(capturedLinkBytes.toString("utf8")).toBe(liveBundle.natsLinkStatus.stdout);
    const capturedStatus = JSON.parse(capturedStatusBytes.toString("utf8"));
    const capturedLinkStatus = JSON.parse(capturedLinkBytes.toString("utf8"));
    expect(capturedStatus).not.toHaveProperty("session");
    // Decode and hash EVERY retained original file; the old refusal is evidence, not an expected replay result.
    const bytes = Object.fromEntries(Object.entries(asset.files).map(([name, file]) => [name, a19Decode(file)]));
    const capturedRun = JSON.parse(bytes["seat-run.json"]!.toString("utf8"));
    const capturedInputs = JSON.parse(bytes["seat-inputs.json"]!.toString("utf8"));
    const oldIntent = JSON.parse(bytes["fabric-v2-bridge-rc-restore-route-intent.json"]!.toString("utf8"));
    const oldOutcome = JSON.parse(bytes["fabric-v2-bridge-rc-restore-route-outcome.json"]!.toString("utf8"));
    const capturedConfig = JSON.parse(bytes["link-config d3.json"]!.toString("utf8"));
    const ownerName = Object.keys(bytes).find(name => name.startsWith("mesh owner "))!;
    const capturedOwner = JSON.parse(bytes[ownerName]!.toString("utf8"));
    const relaunch = JSON.parse(bytes["receipts/rc-relaunch.json"]!.toString("utf8"));
    const stdin = Buffer.from(asset.request.base64, "base64");
    const req = JSON.parse(stdin.toString("utf8"));
    expect(sha(stdin)).toBe(asset.request.sha256);
    expect(asset.request).toMatchObject({ exactMatch: true, journalRequestSha256: [sha(stdin)] });
    expect(oldIntent.requestSha256).toBe(sha(stdin));
    expect(oldOutcome).toMatchObject({ result: "refused", reason: "seam-internal", written: false,
      intentSha256: sha(bytes["fabric-v2-bridge-rc-restore-route-intent.json"]!) });
    expect(req.runSha256).toBe(sha(bytes["seat-run.json"]!));
    expect(capturedInputs).toMatchObject({ run: req.run, runSha256: req.runSha256 });
    expect(req.inputs.writer).toEqual(relaunch.observed);
    expect(capturedOwner).toMatchObject({ pid: req.inputs.writer.pid, sessionFile: req.inputs.writer.sessionFile,
      sessionId: req.sessionId, rootId: `session:${req.sessionId}` });
    expect(capturedConfig.allow.nats).toEqual([]);
    expect(oldIntent.plan).toMatchObject({ change: "none", beforeHadNats: true, beforeSha256: sha(bytes["link-config d3.json"]!) });

    const seat = await createSeat({ start: false });
    seats.push(seat);
    const config = path.join(seat.base, "captured-link", "d3.json");
    fs.mkdirSync(path.dirname(config), { mode: 0o700 });
    fs.writeFileSync(config, bytes["link-config d3.json"]!, { mode: 0o600 });
    const localOwner = structuredClone(capturedOwner);
    if (scenario.fault === "owner pid") localOwner.pid += 1;
    if (scenario.fault === "owner root") localOwner.rootId = "session:wrong-owner";
    if (scenario.fault === "owner start time") localOwner.processStartedAt += "0";
    if (scenario.fault === "owner session file") localOwner.sessionFile += ".wrong";
    fs.writeFileSync(path.join(seat.meshRoot, "main-followups", `${req.sessionId}.owner.json`),
      scenario.fault?.startsWith("owner ") ? JSON.stringify(localOwner) : bytes[ownerName]!, { mode: 0o600 });
    const lockFd = seat.lockFd(); // Real held flock and real main() proveSeatLock/verifyRunDir, not a fabricated fd.
    // Control journal/source header/Fabric config were not captured. Supply ONLY their sealed metadata/resolution
    // boundary, derived from the original byte-verified pair; preserve all captured fields, including home and UID.
    vi.spyOn(runBoundary, "readSealedRun").mockImplementation((runDir, request, _uid, seamName) => {
      expect(runDir).toBe(seat.runDir);
      expect(request).toMatchObject(req);
      expect(seamName).toBe("bridge");
      return { runBytes: bytes["seat-run.json"]!, run: capturedRun, inputs: capturedInputs,
        inputsSha256: sha(bytes["seat-inputs.json"]!), seat: capturedInputs.seat, journal: capturedRun.source.journal };
    });
    const resolveMesh = vi.spyOn(runBoundary, "resolveSeatMeshRoot").mockImplementation((journal, sessionId) => {
      expect(journal).toBe(capturedRun.source.journal);
      expect(sessionId).toBe(req.sessionId);
      return seat.meshRoot;
    });
    const realpath = fs.realpathSync;
    vi.spyOn(fs, "realpathSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      // Preserve config bytes and logical paths. Never resolve a captured path against the principal's live FS.
      if (file === capturedConfig.file.mesh) return seat.meshRoot;
      if (file === capturedConfig.stateDir) return capturedConfig.stateDir;
      return realpath(file, options as never);
    }) as typeof fs.realpathSync);
    const startTime = mainBinding.procStartTime;
    vi.spyOn(mainBinding, "procStartTime").mockImplementation((pid, procRoot) =>
      pid === capturedOwner.pid ? capturedOwner.processStartedAt : startTime(pid, procRoot));
    // Generate the FULL current v1 wire shape from the real controller, then relocate only its live identity.
    // Incarnation and leaf were not captured: this modeled live reply is NOT evidence of the historical response.
    seat.native.state.sessionFile = req.inputs.writer.sessionFile;
    const statusReply = await seat.control.handle("status", null, { pid: process.pid, uid: process.getuid!() }, process.getuid!());
    expect(typeof statusReply).toBe("object");
    const reply: MainDrainReply = { ...(statusReply as MainDrainReply), rootId: capturedOwner.rootId,
      sessionId: req.sessionId, meshRoot: capturedConfig.file.mesh, pid: capturedOwner.pid,
      session: { file: capturedOwner.sessionFile, leafId: null, ownerIncarnation: V1_INCARNATION } };
    expect(reply).toMatchObject({ v: 1, ok: true, op: "status", error: null });
    let wire: any = scenario.capture ? structuredClone(capturedStatus) : structuredClone(reply);
    if (scenario.capture) {
      expect(capturedStatus.pid).not.toBe(req.inputs.writer.pid);
      expect(asset.liveCapture.processSubstitution).toMatchObject({ field: "pid", captured: capturedStatus.pid, reboundTo: req.inputs.writer.pid });
      if (scenario.capture === "pid-rebound") wire.pid = req.inputs.writer.pid;
      const { pid: _wirePid, ...preserved } = wire;
      const { pid: _capturedPid, ...original } = capturedStatus;
      expect(preserved).toEqual(original); // All fields other than the explicitly substituted PID remain captured.
    }
    if (scenario.shape === "missing session") delete wire.session;
    if (scenario.shape === "null session") wire.session = null;
    if (scenario.shape === "numeric session") wire.session = 7;
    if (scenario.shape === "array session") wire.session = [];
    if (scenario.shape === "missing file") delete wire.session.file;
    if (scenario.shape === "null file") wire.session.file = null;
    if (scenario.shape === "missing incarnation") delete wire.session.ownerIncarnation;
    if (scenario.shape === "null incarnation") wire.session.ownerIncarnation = null;
    if (scenario.shape === "numeric incarnation") wire.session.ownerIncarnation = 7;
    if (scenario.shape === "object incarnation") wire.session.ownerIncarnation = {};
    if (scenario.shape === "wrong root" || scenario.fault === "reply root") wire.rootId = "session:wrong";
    if (scenario.shape === "string pid") wire.pid = String(capturedOwner.pid);
    if (scenario.shape === "error reply") { wire.ok = false; wire.error = "MAIN_DRAIN_BUSY"; }
    if (scenario.shape === "null reply") wire = null;
    if (scenario.shape === "missing native") delete wire.native;
    if (scenario.shape === "invalid probes") wire.probes = {};
    // Read the real client result before mocking that environmental call for main(). Missing/null/malformed
    // session and incarnation are genuinely CLIENT-ACCEPTED JSON, unlike a null or error reply.
    const received = await a19WireReply(seat.base, req.sessionId, wire,
      scenario.shape === "owner unavailable at peer verification" ? "MAIN_DRAIN_OWNER_UNREADABLE" : undefined);
    if (scenario.clientError) expect(received.error).toMatchObject({ code: scenario.clientError });
    else expect(received.reply).toEqual(wire);
    vi.spyOn(mainClient, "mainDrainSocket").mockImplementation((meshRoot, sessionId) => {
      expect(meshRoot).toBe(seat.meshRoot);
      expect(sessionId).toBe(req.sessionId);
      return path.join(seat.base, "unavailable-live-main-drain.sock");
    });
    const status = vi.spyOn(mainClient, "callMainDrain").mockImplementation(async (_socket, sessionId, op) => {
      expect(sessionId).toBe(req.sessionId);
      expect(op).toBe("status");
      if ("error" in received) throw received.error;
      return structuredClone(received.reply!);
    });
    const roots = vi.fn(async () => { throw new Error("no live NATS root read is permitted in this no-publish replay"); });
    const legacy = scenario.capture !== undefined || scenario.shape === "missing session";
    // The old owner activationId is NOT an ownerIncarnation. Neither captured v1 nor link status exposes one.
    const liveRoute: Record<string, any> = { ok: true, verdict: "routable", side: "file", file: { remoteHost: null, live: true,
      ...(legacy ? {} : { ownerIncarnation: V1_INCARNATION }) },
      nats: { read: true, records: [{ machine: capturedConfig.nats.machine, live: true,
        ...(legacy ? {} : { ownerIncarnation: V1_INCARNATION }) }] } };
    const liveGate: Record<string, unknown> = { side: "file", key: capturedOwner.rootId,
      kind: "root", status: "live", id: capturedOwner.rootId, principal: capturedConfig.allow.file[0].principal };
    if (scenario.fault === "wrong gate identity") { liveGate.id = "session:wrong-gate"; liveGate.key = liveGate.id; }
    if (scenario.fault === "gate not live") liveGate.status = "not-live";
    if (scenario.fault === "cross-side gate") liveGate.side = "nats";
    if (scenario.fault === "cross-side receipt") liveRoute.side = "nats";
    if (scenario.fault === "file not live") liveRoute.file.live = false;
    if (scenario.fault === "file remote host") liveRoute.file.remoteHost = "synthetic-remote";
    if (scenario.fault?.startsWith("link incarnation")) {
      liveGate.ownerIncarnation = V1_INCARNATION;
      liveRoute.file.ownerIncarnation = scenario.fault === "link incarnation match" ? V1_INCARNATION : INCARNATION;
      liveRoute.nats.records[0].ownerIncarnation = liveRoute.file.ownerIncarnation;
    }
    let t = 0;
    const deps: BridgeDeps = { seatPins: () => readBridgeNatsSeatPins({ productOwner: { name: capturedConfig.allow.file[0].principal },
      principals: [{ name: capturedConfig.allow.file[0].principal, orgInstance: { fabricSession: req.sessionId,
        herdrAgent: capturedRun.instance, herdrWorkspaceLabel: capturedConfig.allow.file[0].principal } }] }),
      now: () => t, sleep: async ms => { t += ms; }, deadlineMs: 2_000, roots,
      status: async () => legacy ? structuredClone(capturedLinkStatus) : ({ ok: true, problems: [], allow: { file: Object.entries(oldIntent.routesBefore)
        .filter(([, route]) => (route as { side: string }).side === "file").map(([identity, route]) => ({ identity, live: (route as { live: boolean }).live })), nats: [] } }),
      dryRun: async () => ({ rows: scenario.shape === "missing live gate" ? [] : [structuredClone(liveGate)] }),
      receipt: async () => scenario.shape === "null route receipt" ? null as never : scenario.shape === "malformed route records"
        ? { ...liveRoute, nats: { read: true, records: {} } } : structuredClone(liveRoute),
      onError: vi.fn(),
    };
    // Test-only exact-HEAD baseline; the artifact rebases static imports only. Normal CI always uses current main.
    const replayMain: typeof main = process.env.D3_REPLAY_BASE
      ? (await import(/* @vite-ignore */ process.env.D3_REPLAY_BASE)).main : main;
    let stdout = ""; let stderr = "";
    const code = await replayMain(["--run-dir", seat.runDir, "--phase", req.phase], {
      stdin: () => stdin, stdout: value => { stdout += value; }, stderr: value => { stderr += value; },
      env: { D3_SEAT_LOCK_FD: String(lockFd) },
    }, { linkConfig: config, linkUnit: oldIntent.unit }, deps);
    // onError exposes any REAL throw's class/message/stack to the assertion, without inventing malformed inputs.
    const errors = (deps.onError as ReturnType<typeof vi.fn>).mock.calls.map(([error]) => {
      const safe = redactNatsError(error);
      return { errorClass: safe.name, errorMessage: safe.message, stack: safe.stack };
    });
    expect({ code, stderr, errors }, JSON.stringify({ code, stderr, errors })).toEqual({ code: 0, stderr: "", errors: [] });
    const intent = JSON.parse(fs.readFileSync(path.join(seat.runDir, "fabric-v2-bridge-rc-restore-route-intent.json"), "utf8"));
    const outcome = JSON.parse(fs.readFileSync(path.join(seat.runDir, "fabric-v2-bridge-rc-restore-route-outcome.json"), "utf8"));
    expect(JSON.parse(stdout), JSON.stringify(outcome)).toMatchObject({ result: scenario.reason === null ? "pass" : "refused",
      phase: req.phase, run: req.run,
      observed: { sessionId: req.sessionId, route: `v1@${capturedOwner.pid}:${capturedOwner.processStartedAt}` } });
    const { file: _originalPath, ...oldPlan } = oldIntent.plan;
    expect(intent.plan).toEqual({ ...oldPlan, file: config });
    expect(intent).toMatchObject({ requestSha256: sha(stdin), writer: req.inputs.writer, routesBefore: oldIntent.routesBefore });
    expect(outcome).toMatchObject({ result: scenario.reason === null ? "pass" : "refused", written: false,
      measured: { owner: localOwner } });
    if (scenario.reason === null && legacy) {
      const linkIncarnation = scenario.fault === "link incarnation match";
      expect(outcome).toMatchObject({ measured: { incarnation: linkIncarnation ? V1_INCARNATION : null,
        incarnationSource: linkIncarnation ? "link-file-route" : "unavailable-v1-b7221c04" },
        asserted: { v1: { pid: wire.pid, rootId: wire.rootId } } });
      expect(outcome.measured.incarnation).not.toBe(capturedOwner.activationId);
      expect(outcome.measured.gate).toMatchObject({ side: "file", status: "live", id: capturedOwner.rootId });
      expect(outcome.measured.receipt.file).toMatchObject({ remoteHost: null, live: true });
      if (linkIncarnation) expect(outcome.measured.receipt.file.ownerIncarnation).toBe(outcome.measured.gate.ownerIncarnation);
      else expect(outcome.measured.receipt.file).not.toHaveProperty("ownerIncarnation");
    } else if (scenario.reason === null) expect(outcome).toMatchObject({ asserted: { v1: { pid: reply.pid, rootId: reply.rootId, session: reply.session } } });
    else expect(outcome).toMatchObject({ reason: scenario.reason });
    // Exact-base replay predates diagnostics. Current-source non-pass outcomes must always state null/null
    // when no exception escaped, including client refusals normalized by localIdentity.
    if (scenario.reason !== null && !process.env.D3_REPLAY_BASE) expect(outcome).toMatchObject({ errorClass: null, errorMessage: null });
    expect(fs.readFileSync(config)).toEqual(bytes["link-config d3.json"]);
    expect(resolveMesh).toHaveBeenCalledTimes(1);
    expect(status).toHaveBeenCalledTimes(1);
    expect(roots).not.toHaveBeenCalled();
  });
});

// Data-safety/failure-path coverage: outcomes must be useful without retaining exception stacks or credentials.
describe.skipIf(!linux)("bridge nonpass diagnostics", () => {
  const phases = ["resume", "publish", "rc-restore-route"] as const;
  const scenario = async (phase: typeof phases[number]) => {
    const w = await world({ allow: { file: [leadEntry, seatFileEntry], nats: [] } });
    if (phase === "resume") setKube(w, RAW, { line: identityLine(w) }, { running: false });
    if (phase === "publish") setKube(w, MANAGED, { line: identityLine(w) });
    if (phase === "rc-restore-route") w.seat.native.state.sessionFile = "/synthetic/v1/session.jsonl";
    const inputs = phase === "resume" ? resumeInputs(w) : phase === "publish" ? { owner } : writerInputs(w);
    return { w, inputs };
  };
  it.each(phases)("nonpass diagnostics: %s normal refusal has explicit null/null fields", async phase => {
    const { w, inputs } = await scenario(phase);
    const result = await run(w, phase, inputs, { deadlineMs: 2_000 });
    expect(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    const outcome = evidenceOf(w, `fabric-v2-bridge-${phase}-outcome.json`);
    const reason = phase === "resume" ? "pod-not-running" : phase === "publish" ? "seat-not-routable" : "v1-not-routable";
    expect(outcome).toMatchObject({ result: "refused", reason, errorClass: null, errorMessage: null });
    expect(outcome).not.toHaveProperty("gap");
    expect(Object.hasOwn(outcome, "errorClass") && Object.hasOwn(outcome, "errorMessage")).toBe(true);
  });
  it.each(phases.flatMap(phase => ["error", "string", "object"].map(kind => ({ phase, kind }))))(
    "nonpass diagnostics: $phase sanitizes a thrown $kind and retains exactly one intent-linked outcome", async ({ phase, kind }) => {
      const { w, inputs } = await scenario(phase);
      const sensitive = "password=b4-password Bearer b4-bearer https://b4-user:b4-url-password@example.invalid/path?token=b4-query ";
      const typed = new TypeError(`${sensitive}${"m".repeat(700)} token=b4-tail`);
      typed.name = `TypeError secret=b4-class ${"n".repeat(600)}`;
      typed.stack = "B4_STACK_MUST_NOT_LEAK";
      Object.assign(typed, { rawpayload: "B4_RAW_PAYLOAD_MUST_NOT_LEAK" });
      const error: unknown = kind === "error" ? typed : kind === "string" ? `${sensitive}${"s".repeat(700)}`
        : { code: "B4_DIAGNOSTIC", rawpayload: "B4_RAW_PAYLOAD_MUST_NOT_LEAK", password: "b4-password", Bearer: "b4-bearer" };
      const safe = redactNatsError(error);
      const onError = vi.fn();
      const outcomeFile = path.join(w.seat.runDir, `fabric-v2-bridge-${phase}-outcome.json`);
      const originalOpen = fs.openSync;
      const attempts: number[] = [];
      vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: number | string, mode?: fs.Mode) => {
        if (file === outcomeFile && typeof flags === "number" && (flags & fs.constants.O_EXCL) !== 0) attempts.push(flags);
        return originalOpen(file, flags, mode);
      }) as typeof fs.openSync);
      const result = await run(w, phase, inputs, { deadlineMs: 5_000, sleep: async () => { throw error; }, onError });
      expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: seam-internal [owner: fabric-v2]\n" });
      expect(onError).toHaveBeenCalledExactlyOnceWith(error);
      expect(attempts).toHaveLength(1);
      const outcome = evidenceOf(w, `fabric-v2-bridge-${phase}-outcome.json`);
      expect(outcome).toMatchObject({ result: "refused", reason: "seam-internal", errorClass: safe.name.slice(0, 500),
        errorMessage: safe.message.slice(0, 500),
        intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, `fabric-v2-bridge-${phase}-intent.json`))) });
      expect(outcome.errorClass.length).toBeLessThanOrEqual(500);
      expect(outcome.errorMessage.length).toBeLessThanOrEqual(500);
      if (kind === "error" || kind === "string") expect(outcome.errorMessage).toHaveLength(500);
      if (kind === "error") expect(outcome.errorClass).toHaveLength(500);
      const serialized = JSON.stringify(outcome);
      for (const secret of ["b4-password", "b4-bearer", "b4-url-password", "b4-query", "b4-class", "b4-tail",
        "B4_STACK_MUST_NOT_LEAK", "B4_RAW_PAYLOAD_MUST_NOT_LEAK"]) expect(serialized).not.toContain(secret);
      expect(outcome).not.toHaveProperty("stack");
      expect(outcome).not.toHaveProperty("rawpayload");
    });
});

const writerInputs = (w: World, extra: Record<string, unknown> = {}) => ({ recoveryRun: "c".repeat(32), parentRun: w.seat.run,
  parentRunSha256: w.seat.runSha256, writer: { sessionId: SID, pid: process.pid, sessionFile: "/synthetic/v1/session.jsonl", singleWriter: true, ...extra } });

describe.skipIf(!linux)("bridge rc-restore-route: the reverse write, and the relaunched v1 as the live owner", () => {
  const writer = writerInputs;

  const published = async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.receipt = routable();
    w.gate = gateLive("nats", MACHINE);
    expect((await run(w, "publish", { owner })).receipt?.result).toBe("pass");
    w.seat.native.state.sessionFile = "/synthetic/v1/session.jsonl";
    w.receipt = { ok: true, verdict: "routable", side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
      nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }] } };
    w.gate = gateLive("file");
    // After the restore, the hub still holds the v2 seat's old mirror row: the one tolerated status problem.
    w.statusAfter = { ok: false, problems: [`mirror not on allow.nats: session:${SID}`],
      allow: { file: [{ identity: LEAD, live: true }, { identity: `session:${SID}`, live: true }], nats: [] } };
    w.roots = [{ id: `session:${SID}`, machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }];
    return w;
  };

  /** Failed init leaves the raw Pod's pi waiting: resume refuses before publish ever runs. */
  const failedInit = async () => {
    const w = await world({ allow: { file: [leadEntry, seatFileEntry], nats: [] } });
    const before = fs.readFileSync(w.config);
    setKube(w, RAW, { line: "", code: 1 }, { running: false });
    // Dedicated startup tests cover 180 s; this restore regression needs only a bounded failed-start refusal.
    expect(await run(w, "resume", resumeInputs, { deadlineMs: 5_000 })).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason: "pod-not-running" });
    expect(w.calls().some(call => call.argv.includes("exec"))).toBe(false);
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-publish-intent.json"))).toBe(false);
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-publish-outcome.json"))).toBe(false);
    expect(JSON.parse(before.toString("utf8")).allow).toEqual({ file: [leadEntry, seatFileEntry], nats: [] });
    expect(fs.readFileSync(w.config)).toEqual(before);
    // A relaunches v1 on its RECOVERED journal, not the original sealed source; it still owns the same mesh.
    const recovered = path.join(w.seat.base, "org-recovery", "c".repeat(32), "pi", "sessions", `${SID}.jsonl`);
    const source = JSON.parse(fs.readFileSync(path.join(w.seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
    fs.mkdirSync(path.dirname(recovered), { recursive: true, mode: 0o700 });
    fs.copyFileSync(source, recovered);
    expect(recovered).not.toBe(source);
    w.seat.native.state.sessionFile = recovered;
    w.gate = gateLive("file");
    w.receipt = { ok: true, verdict: "routable", side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
      nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }] } };
    return { w, before, inputs: writer(w, { sessionFile: recovered }) };
  };

  it("restore regression: failed raw init then no-publish restore passes without reading roots", async () => {
    const { w, before, inputs } = await failedInit();
    const roots = vi.fn(async () => { throw new Error("synthetic roots unavailable"); });
    const result = await run(w, "rc-restore-route", inputs, { roots });
    expect.soft(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "pass", observed: {
      sessionId: SID, route: `v1@${process.pid}:${procStartTime(process.pid)}` } } });
    const intent = evidenceOf(w, "fabric-v2-bridge-rc-restore-route-intent.json");
    expect(intent.plan).toMatchObject({ change: "none", beforeHadNats: true });
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
    expect.soft(roots).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-rc-restore-route-outcome.json"))).toBe(true);
    const outcome = evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json");
    expect(outcome).toMatchObject({ result: "pass", written: false, asserted: { v1: { session: { file: inputs.writer.sessionFile } } },
      measured: { gate: { side: "file", status: "live" }, receipt: { verdict: "routable", side: "file" } } });
    expect(outcome).not.toHaveProperty("reason");
  });

  it.each([
    { fault: "writer pid", reason: "v1-writer-mismatch" },
    { fault: "missing writer", reason: "v1-unreachable" },
    { fault: "session file", reason: "v1-session-file-mismatch" },
    { fault: "identity gate", reason: "v1-not-routable" },
    { fault: "route incarnation", reason: "v1-not-routable" },
  ])("restore regression: no-publish restore still refuses a bad $fault", async ({ fault, reason }) => {
    const { w, before, inputs } = await failedInit();
    if (fault === "writer pid") inputs.writer.pid += 100_000;
    if (fault === "missing writer") await w.seat.server.close();
    if (fault === "session file") w.seat.native.state.sessionFile = "/synthetic/wrong-session.jsonl";
    if (fault === "identity gate") w.gate = { ...gateLive("file"), status: "BRIDGE_NATS_AMBIGUOUS" };
    if (fault === "route incarnation") w.receipt = { ...w.receipt,
      nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: INCARNATION }] } };
    const roots = vi.fn(async () => []);
    expect(await run(w, "rc-restore-route", inputs, { roots, deadlineMs: 2_000 }))
      .toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused", reason, written: false });
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
    expect(roots).not.toHaveBeenCalled();
  });

  it("restore regression: published restore retains the roots GAP and records its refusal outcome", async () => {
    const w = await published();
    const original = evidenceOf(w, "fabric-v2-bridge-publish-intent.json").plan.beforeBytes as string;
    const roots = vi.fn(async () => { throw new Error("synthetic roots unavailable"); });
    expect(await run(w, "rc-restore-route", writer(w), { roots })).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: nats-presence-unreadable [owner: fabric-v2]\n" });
    expect(roots).toHaveBeenCalledTimes(1);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-intent.json").plan.change).toBe("revert");
    expect(fs.readFileSync(w.config, "utf8")).toBe(original);
    expect(fs.readFileSync(w.marker, "utf8").trim().split("\n")).toHaveLength(2);
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-rc-restore-route-outcome.json"))).toBe(true);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused",
      reason: "nats-presence-unreadable", written: true, errorClass: "SeamGap", errorMessage: "nats-presence-unreadable" });
  });

  it("restore regression: applyPlan GAP after locked intent still records an outcome", async () => {
    const { w, before, inputs } = await failedInit();
    const intentFile = path.join(w.seat.runDir, "fabric-v2-bridge-rc-restore-route-intent.json");
    const readFile = fs.readFileSync;
    let injected = 0;
    // applyPlan re-reads the specific config through a numeric fd, AFTER intent but before the lock callback returns.
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (typeof file === "number" && fs.existsSync(intentFile) && fs.readlinkSync(`/proc/self/fd/${file}`) === w.config) {
        injected += 1;
        if (injected === 1) throw new SeamGap("link-config-unreadable");
      }
      return readFile(file, options as never);
    }) as typeof fs.readFileSync);
    const result = await run(w, "rc-restore-route", inputs).finally(() => read.mockRestore());
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: link-config-unreadable [owner: fabric-v2]\n" });
    expect(injected).toBe(1);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-intent.json").plan.change).toBe("none");
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-rc-restore-route-outcome.json"))).toBe(true);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused",
      reason: "link-config-unreadable", written: false, errorClass: "SeamGap", errorMessage: "link-config-unreadable",
      intentSha256: sha(fs.readFileSync(intentFile)) });
  });

  it("restore regression: a non-SeamGap after intent retains seam-internal and records an outcome", async () => {
    const { w, before, inputs } = await failedInit();
    w.gate = null;
    const error = new Error("synthetic route timer failure");
    const onError = vi.fn();
    expect(await run(w, "rc-restore-route", inputs, { sleep: async () => { throw error; }, onError }))
      .toMatchObject({ code: 2, stdout: "", stderr: "GAP: seam-internal [owner: fabric-v2]\n" });
    expect(onError).toHaveBeenCalledWith(error);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-intent.json").plan.change).toBe("none");
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-rc-restore-route-outcome.json"))).toBe(true);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused",
      reason: "seam-internal", written: false, errorClass: "Error", errorMessage: error.message,
      intentSha256: sha(fs.readFileSync(path.join(w.seat.runDir, "fabric-v2-bridge-rc-restore-route-intent.json"))) });
  });

  // Regression: restoring the route needs only the source header, even for a Paul-sized Pi session.
  it.each([157_000_000, 314_000_000])("restores the route with a sparse %i-byte source journal", async bytes => {
    const w = await published();
    const source = JSON.parse(fs.readFileSync(path.join(w.seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
    fs.truncateSync(source, bytes);
    const result = await run(w, "rc-restore-route", writer(w));
    expect(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "pass", observed: {
      sessionId: SID, route: `v1@${process.pid}:${procStartTime(process.pid)}` } } });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "pass" });
  });

  it("passes rc-restore-route when its file-side seat row appears at +640 s", async () => {
    const w = await published();
    w.gate = null;
    w.receipt = {};
    let t = 0;
    const result = await run(w, "rc-restore-route", writer(w), { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 640_000) {
        w.gate = gateLive("file");
        w.receipt = { ok: true, verdict: "routable", side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
          nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }] } };
      }
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 640_000,
      measured: { gate: { status: "live", side: "file" }, receipt: { verdict: "routable", side: "file" } } });
    expect(t).toBe(640_000);
  });

  it("refuses rc-restore-route when the file-side route would appear only after its 820 s cap", async () => {
    const w = await published();
    w.gate = null;
    w.receipt = {};
    let t = 0;
    const result = await run(w, "rc-restore-route", writer(w), { now: () => t, sleep: async ms => {
      t += ms;
      if (t >= 860_000) {
        w.gate = gateLive("file");
        w.receipt = { ok: true, verdict: "routable", side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
          nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }] } };
      }
    } });
    expect(result.receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused", reason: "v1-not-routable",
      elapsedMs: 820_000, measured: { gate: null, receipt: {} } });
    expect(t).toBe(820_000);
  });

  it("does not restart rollback's phase deadline after delayed other-route recovery", async () => {
    const w = await published();
    w.gate = null;
    w.receipt = {};
    let t = 0;
    let statusCalls = 0;
    let otherRecovered = false;
    const result = await run(w, "rc-restore-route", writer(w), { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 300_000) otherRecovered = true;
    }, status: async () => {
      statusCalls += 1;
      if (statusCalls === 1) return structuredClone(w.status);
      const value = structuredClone(w.statusAfter!);
      value.allow.file[0]!.live = otherRecovered;
      return value;
    } });
    expect(result.receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused", reason: "v1-not-routable",
      elapsedMs: 840_000, measured: { otherRoutes: { checked: [LEAD], down: [] }, gate: null, receipt: {} } });
    expect(t).toBe(840_000);
  });

  it("waits beyond 120 s for another route to recover during rollback", async () => {
    const w = await published();
    let t = 0;
    let statusCalls = 0;
    let recovered = false;
    const result = await run(w, "rc-restore-route", writer(w), { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 121_000) recovered = true;
    }, status: async () => {
      statusCalls += 1;
      const value = structuredClone(w.statusAfter!);
      value.allow.file[0]!.live = statusCalls < 3 || recovered;
      return value;
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 121_000,
      measured: { otherRoutes: { checked: [LEAD], down: [] } } });
    expect(t).toBe(121_000);
  });

  it("limits rollback's other-route recovery to 480 s", async () => {
    const w = await published();
    let t = 0;
    let statusCalls = 0;
    const result = await run(w, "rc-restore-route", writer(w), { now: () => t, sleep: async ms => { t += ms; }, status: async () => {
      statusCalls += 1;
      const value = structuredClone(w.statusAfter!);
      value.allow.file[0]!.live = statusCalls < 3;
      return value;
    } });
    expect(result.receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "refused", reason: "other-routes-down",
      elapsedMs: 480_000, measured: { otherRoutes: { checked: [LEAD], down: [LEAD] } } });
    expect(t).toBe(480_000);
  });

  it("puts the seat's file entry back exactly, restarts the link, and passes on the live v1 writer", async () => {
    const w = await published();
    const original = evidenceOf(w, "fabric-v2-bridge-publish-intent.json").plan.beforeBytes as string;
    const result = await run(w, "rc-restore-route", writer(w));
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: w.seat.run, phase: "rc-restore-route", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, route: `v1@${process.pid}:${procStartTime(process.pid)}` } });
    // #1609 review P1-6: the file is back byte for byte (it still held publish's bytes).
    expect(fs.readFileSync(w.config, "utf8")).toBe(original);
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-intent.json").plan.change).toBe("revert");
    expect(fs.readFileSync(w.marker, "utf8").trim().split("\n")).toHaveLength(2);
  });

  it("refuses while a v2 presence for the session is still live, or when the writer is not the live owner", async () => {
    const live = await published();
    live.roots = [...live.roots, { id: `session:${SID}`, machine: MACHINE, live: true, ownerIncarnation: INCARNATION }];
    expect((await run(live, "rc-restore-route", writer(live))).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(live, "fabric-v2-bridge-rc-restore-route-outcome.json").reason).toBe("v2-presence-live");
    const wrong = await published();
    expect((await run(wrong, "rc-restore-route", writer(wrong, { pid: process.pid + 100_000 }))).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(wrong, "fabric-v2-bridge-rc-restore-route-outcome.json").reason).toBe("v1-writer-mismatch");
  });

  it("a rollback before any publish write changes nothing and restarts nothing", async () => {
    const w = await world();
    w.seat.native.state.sessionFile = "/synthetic/v1/session.jsonl";
    // The seat's v1 file entry never moved: the gate resolves it live and the link mirrors v1 under its own machine.
    w.gate = gateLive("file");
    w.receipt = { side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
      nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }] } };
    const before = fs.readFileSync(w.config);
    expect((await run(w, "rc-restore-route", writer(w))).receipt).toMatchObject({ result: "pass" });
    expect(fs.readFileSync(w.config)).toEqual(before);
    expect(fs.existsSync(w.marker)).toBe(false);
  });

  it("another status problem after the reload is not tolerated", async () => {
    const w = await published();
    w.statusAfter = { ...w.statusAfter!, problems: [`mirror not on allow.nats: ${LEAD}`] };
    expect((await run(w, "rc-restore-route", writer(w))).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json").reason).toBe("link-not-reloaded");
  });

  it("#1609 review (status P1): every status problem is judged, not only the 50 kept as evidence", async () => {
    const leftover = `mirror not on allow.nats: session:${SID}`;
    // 50 tolerated rows, then ONE untolerated row past the evidence cut: refused.
    const w = await published();
    w.statusAfter = { ...w.statusAfter!, problems: [...Array<string>(50).fill(leftover), `mirror not on allow.nats: ${LEAD}`] };
    expect((await run(w, "rc-restore-route", writer(w))).receipt).toMatchObject({ result: "refused" });
    const outcome = evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json");
    expect(outcome.reason).toBe("link-not-reloaded");
    expect(outcome.measured.reload).toMatchObject({ statusOk: false, problems: Array<string>(50).fill(leftover) });
    // The counterexample: 51 rows that are ALL tolerated still pass.
    const ok = await published();
    ok.statusAfter = { ...ok.statusAfter!, problems: Array<string>(51).fill(leftover) };
    expect((await run(ok, "rc-restore-route", writer(ok))).receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(ok, "fabric-v2-bridge-rc-restore-route-outcome.json").measured.reload).toMatchObject({ statusOk: true });
  });

  it("#1609 review P1-5: a no-op publish (the entry already there) gives the rollback nothing to remove", async () => {
    const entryThere = { kind: "root", id: `session:${SID}`, principal: "Delta", machine: MACHINE, send: ["followUp"], receive: ["followUp", "cancel"] };
    const w = await world({ allow: { file: [leadEntry], nats: [entryThere] } });
    setKube(w, MANAGED, { line: identityLine(w) });
    w.receipt = routable();
    w.gate = gateLive("nats", MACHINE);
    expect((await run(w, "publish", { owner })).receipt?.result).toBe("pass");
    expect(evidenceOf(w, "fabric-v2-bridge-publish-intent.json")).toMatchObject({ plan: { change: "none" }, rollback: null });
    // The rollback must not remove that PRE-EXISTING entry: the seat's nats entry without our authority refuses.
    w.seat.native.state.sessionFile = "/synthetic/v1/session.jsonl";
    const before = fs.readFileSync(w.config);
    expect(await run(w, "rc-restore-route", writer(w))).toMatchObject({ code: 2, stderr: "GAP: seat-entry-conflict [owner: fabric-v2]\n" });
    expect(fs.readFileSync(w.config)).toEqual(before);
  });

  it("never removes a seat entry that publish did not write in this RUN", async () => {
    const w = await world({ allow: { file: [leadEntry], nats: [{ kind: "root", id: `session:${SID}`, principal: "Delta", machine: MACHINE,
      send: ["followUp"], receive: ["followUp", "cancel"] }] } });
    const before = fs.readFileSync(w.config);
    expect(await run(w, "rc-restore-route", writer(w))).toMatchObject({ code: 2, stderr: "GAP: seat-entry-conflict [owner: fabric-v2]\n" });
    expect(fs.readFileSync(w.config)).toEqual(before);
  });
});
