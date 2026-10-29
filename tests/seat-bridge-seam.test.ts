import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { procStartTime } from "../src/main-drain/binding.js";
import { readBridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { journalBranch } from "../src/seat-drain/journal-branch.js";
import * as kube from "../src/seat-drain/kube.js";
import { main, type BridgeDeps } from "../src/seat-bridge/seam.js";
import { createSeat, SID, type Seat } from "./seat-drain-fixture.js";

// The D3 bridge seam (principal-seat move), synthetic only: a fake kubectl (no cluster), a fake systemctl, fake link
// readers, a synthetic org (principal "Delta", seat = the fixture's session) and a synthetic nats-link config.
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
  fs.copyFileSync(path.join("tests", "fixtures", "fake-kubectl.py"), kubectl);
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

const setKube = (w: World, pod: { name: string; uid: string }, identity: { line: string; code?: number; changeUid?: string },
  podExtra: Record<string, unknown> = {}) => {
  fs.writeFileSync(path.join(w.kube, "kube-state.json"), JSON.stringify({ server: SERVER, caB64: CA.toString("base64"), cliPresent: true,
    pod: { name: pod.name, uid: pod.uid, meshRoot: "/var/lib/fabric/mesh", running: true, terminated: null, args: ["--session", POD_JOURNAL],
      natsMachine: "sp-org-d3synth", ...podExtra },
    exec: { line: "" }, identity }));
};

/** The in-Pod CLI's `--identity` line, with the journal read by the REAL reader over the synthetic Pod journal. */
const identityLine = (w: World, options: { leafId?: string; sessionFile?: string; ownerIncarnation?: string; journal?: unknown } = {}) => {
  const journal = options.journal ?? journalBranch(w.journalFile, "e2", Buffer.byteLength(w.sealed));
  const leaf = options.leafId ?? (journal as { leafId: string }).leafId;
  return JSON.stringify({ format: 1, sessionId: SID, ptraceScope: "1", op: "identity", identity: { rootId: `session:${SID}`, pid: 7,
    meshRoot: "/var/lib/fabric/mesh", sessionFile: options.sessionFile ?? POD_JOURNAL, leafId: leaf,
    ownerIncarnation: options.ownerIncarnation ?? INCARNATION }, journal });
};

const request = (seat: Seat, phase: string, inputs: Record<string, unknown>) => JSON.stringify({ schema: "d3-seam-request-v1", run: seat.run,
  runSha256: seat.runSha256, phase, principal: "d3synth", sessionId: SID, inputs });

const run = async (w: World, phase: string, given: Record<string, unknown> | ((w: World) => Record<string, unknown>),
  extra: Partial<BridgeDeps> = {}, stdin?: Buffer) => {
  const inputs = typeof given === "function" ? given(w) : given;
  let stdout = ""; let stderr = "";
  let t = 0;
  const deps: BridgeDeps = {
    kubectl: path.join(w.kube, "kubectl"), podCli: "/opt/pi-fabric/dist/cli/index.js", systemctl: path.join(w.seat.base, "systemctl"),
    seatPins: () => pins, now: () => t, sleep: async ms => { t += ms; },
    holder: () => ({ pid: !w.holderStuck && fs.existsSync(w.marker) ? 101 + fs.readFileSync(w.marker, "utf8").split("\n").length : 100,
      startTime: "1", purpose: "start" }),
    status: async config => { w.seenConfigs.push({ allow: structuredClone(config.allow) });
      return structuredClone(w.statusAfter && fs.existsSync(w.marker) ? w.statusAfter : w.status); },
    receipt: async config => { w.seenConfigs.push({ allow: structuredClone(config.allow) }); return structuredClone(w.receipt); },
    dryRun: async config => { w.seenConfigs.push({ allow: structuredClone(config.allow) }); return { rows: w.gate ? [structuredClone(w.gate)] : [] }; },
    roots: async () => structuredClone(w.roots) as never,
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
  it("passes with no append: the true leaf, the file's sha256, an empty suffix, [] appended", async () => {
    const w = await world();
    setKube(w, RAW, { line: identityLine(w) });
    const result = await run(w, "resume", resumeInputs);
    expect(result).toMatchObject({ code: 0, stderr: "" });
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: w.seat.run, phase: "resume", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, journal: POD_JOURNAL, leafId: "e2", podUid: RAW.uid, appendedSinceSeal: [], journalSha256: sha(SEALED), suffixBase64: "" } });
    const exec = w.calls().find(call => call.argv.includes("--identity"))!;
    expect(exec.argv.slice(exec.argv.indexOf("--"))).toEqual(["--", "/opt/pi-fabric/dist/cli/index.js", "session", "drain", SID, "--mesh",
      "/var/lib/fabric/mesh", "--identity", "--since", "e2", "--sealed-bytes", String(Buffer.byteLength(SEALED))]);
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
    expect(outcome).toMatchObject({ result: "unknown", gap: "resume-thinking-append-missing", elapsedMs,
      measured: { observations, finalSeal: { sealHasThinkingEntry: false }, journal: { appendedSinceSeal: [], suffixBytes: 0 } } });
    expect(w.calls().filter(call => call.argv.includes("--identity"))).toHaveLength(observations);
    expect(t).toBe(elapsedMs);
    expect(t).toBeLessThan(phaseCap);
  });

  it("keeps Pi's missing-append wait at 600 s under the default 780 s phase ceiling", async () => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep: async () => { t += 200_000; } });
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown",
      gap: "resume-thinking-append-missing", elapsedMs: 400_000, measured: { observations: 2 } });
    expect(t).toBe(400_000);
    expect(t).toBeLessThan(600_000);
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

  // F1: a readable but mismatched identity is a refusal, not a candidate for the missing-append wait. The Main's
  // reported file is a real, byte-identical copy here: content equality cannot substitute for rendered --session.
  it.each([
    { name: "missing rendered --session", args: [], sessionFile: POD_JOURNAL, leafId: "e2", reason: "pod-session-arg-unknown" },
    { name: "changed rendered --session", args: ["--session", "/var/lib/fabric/sessions/changed.jsonl"],
      sessionFile: POD_JOURNAL, leafId: "e2", reason: "journal-mismatch" },
    { name: "Main on another byte-identical file", args: ["--session", POD_JOURNAL],
      sessionFile: "/var/lib/fabric/sessions/other.jsonl", leafId: "e2", reason: "journal-mismatch" },
    { name: "Main memory leaf differs from its file", args: ["--session", POD_JOURNAL],
      sessionFile: POD_JOURNAL, leafId: "e1", reason: "leaf-memory-file-mismatch" },
  ])("F1: $name refuses immediately with a no-thinking seal", async ({ args, sessionFile, leafId, reason }) => {
    const w = await world({ sealed: SEALED_BARE });
    const mainFile = path.join(w.seat.base, "main-journal-copy.jsonl");
    fs.copyFileSync(w.journalFile, mainFile);
    const journal = journalBranch(mainFile, "e2", Buffer.byteLength(SEALED_BARE));
    expect(journal).toMatchObject({ journalSha256: sha(SEALED_BARE), journalBytes: Buffer.byteLength(SEALED_BARE),
      leafId: "e2", appendedSinceSeal: [], suffixBytes: 0 });
    setKube(w, RAW, { line: identityLine(w, { sessionFile, leafId, journal }) }, { args });
    let t = 0;
    const sleep = vi.fn(async (ms: number) => { t += ms; throw new Error("F1: unexpected append wait for mismatched identity"); });
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep });
    expect(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason, elapsedMs: 0,
      measured: { observations: 1 } });
    expect(w.calls().filter(call => call.argv.includes("--identity"))).toHaveLength(1);
    expect(sleep).not.toHaveBeenCalled();
    expect(t).toBe(0);
  });

  it.each([
    { name: "Main file changes", sessionFile: "/var/lib/fabric/sessions/other.jsonl", leafId: "e2", reason: "journal-mismatch" },
    { name: "Main memory leaf changes", sessionFile: POD_JOURNAL, leafId: "e1", reason: "leaf-memory-file-mismatch" },
  ])("F1: $name after a coherent observation stops polling at the next observation", async ({ sessionFile, leafId, reason }) => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const sleep = vi.fn(async (ms: number) => {
      t += ms;
      if (t !== 2_000) throw new Error("F1: unexpected second wait after identity mismatch");
      // The journal stays byte-for-byte sealed; only the live Main's identity changes on the next read.
      setKube(w, RAW, { line: identityLine(w, { sessionFile, leafId }) });
    });
    const result = await run(w, "resume", resumeInputs, { now: () => t, sleep });
    expect(result).toMatchObject({ code: 0, stderr: "", receipt: { result: "refused" } });
    expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "refused", reason, elapsedMs: 2_000,
      measured: { observations: 2, journal: { journalSha256: sha(SEALED_BARE), journalBytes: Buffer.byteLength(SEALED_BARE),
        leafId: "e2", appendedSinceSeal: [], suffixBytes: 0 } } });
    expect(w.calls().filter(call => call.argv.includes("--identity"))).toHaveLength(2);
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledWith(2_000);
    expect(t).toBe(2_000);
  });

  it.each([
    { field: "journalBytes", value: Buffer.byteLength(SEALED_BARE) + 1, gap: "journal-suffix-inconsistent" },
    { field: "journalSha256", value: sha("not the sealed journal"), gap: "journal-suffix-inconsistent" },
    { field: "leafId", value: "e1", gap: "journal-suffix-not-the-branch" },
    { field: "truncated", value: true, gap: "journal-suffix-not-the-branch" },
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
    expect(w.calls().filter(call => call.argv.includes("--identity"))).toHaveLength(1);
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
      reason: "pod-identity-unreadable", elapsedMs: 2_000, measured: { observations: 2 } });
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
    if (phaseCap === 200_000) {
      // No coherent initial identity was established: this is not a missing-append diagnosis.
      expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: phase-deadline [owner: fabric-v2]\n" });
      expect(budgets).toEqual([30_000, 30_000, 120_000, 20_000]);
      expect(t).toBe(phaseCap);
      expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-outcome.json"))).toBe(false);
    } else {
      expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: resume-thinking-append-missing [owner: fabric-v2]\n" });
      expect(budgets).toEqual([30_000, 30_000, 120_000, 30_000, 30_000, 30_000, 120_000, 30_000]);
      expect(t).toBe(424_000);
      expect(evidenceOf(w, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown",
        gap: "resume-thinking-append-missing", elapsedMs: 424_000, measured: { observations: 2 } });
      expect(t).toBeLessThan(phaseCap);
    }
  });

  it("caps an oversized phase budget at 780 s using the real fake-kubectl path", async () => {
    const w = await world({ sealed: SEALED_BARE });
    setKube(w, RAW, { line: identityLine(w) });
    let t = 0;
    const budgets: number[] = [];
    const actualVerify = kube.verifyKubeconfig;
    const actualReadPod = kube.readPod;
    vi.spyOn(kube, "verifyKubeconfig").mockImplementation(async (access, timeout) => {
      await actualVerify(access, timeout);
      budgets.push(timeout); t += 750_000;
    });
    vi.spyOn(kube, "readPod").mockImplementation(async (access, name, timeout) => {
      const value = await actualReadPod(access, name, timeout);
      budgets.push(timeout); t += timeout;
      return value;
    });
    const result = await run(w, "resume", resumeInputs, { deadlineMs: 900_000, now: () => t, sleep: async ms => { t += ms; } });
    expect(result).toMatchObject({ code: 2, stdout: "", stderr: "GAP: phase-deadline [owner: fabric-v2]\n" });
    expect(t).toBe(780_000);
    expect(budgets).toEqual([30_000, 30_000]);
    expect(w.calls()).toHaveLength(2);
    expect(fs.existsSync(path.join(w.seat.runDir, "fabric-v2-bridge-resume-outcome.json"))).toBe(false);
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
    expect(evidenceOf(off, "fabric-v2-bridge-resume-outcome.json")).toMatchObject({ result: "unknown", gap: "resume-tail-id-reused" });
    // An on-branch sealed id: a cycle under later-id-wins. The real reader refuses the branch, and the CLI says so.
    const tail = thinkingLine("e1", "e2");
    const cycle = await world({ sealed: SEALED_BARE, podJournal: SEALED_BARE + tail });
    expect(() => journalBranch(cycle.journalFile, "e2", Buffer.byteLength(SEALED_BARE)))
      .toThrow(expect.objectContaining({ code: "JOURNAL_BRANCH_BROKEN" }));
    setKube(cycle, RAW, { line: identityLine(cycle, { leafId: "e1", journal: { error: "JOURNAL_BRANCH_BROKEN" } }), code: 1 });
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

  it("a CLI whose appended list does not match the suffix bytes is UNKNOWN (the seam checks the branch itself)", async () => {
    const w = await world({ podJournal: SEALED + THINKING });
    const honest = journalBranch(w.journalFile, "e2", Buffer.byteLength(SEALED));
    setKube(w, RAW, { line: identityLine(w, { journal: { ...honest, appendedSinceSeal: [] } }) });
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stderr: "GAP: journal-suffix-not-the-branch [owner: fabric-v2]\n" });
  });

  it("refuses (truthfully) a Main on another session file, or whose leaf differs from its file's", async () => {
    const file = await world();
    setKube(file, RAW, { line: identityLine(file, { sessionFile: "/var/lib/fabric/sessions/other.jsonl" }) });
    expect((await run(file, "resume", resumeInputs)).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(file, "fabric-v2-bridge-resume-outcome.json").reason).toBe("journal-mismatch");
    const leaf = await world();
    setKube(leaf, RAW, { line: identityLine(leaf, { leafId: "e1" }) });
    expect((await run(leaf, "resume", resumeInputs)).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(leaf, "fabric-v2-bridge-resume-outcome.json").reason).toBe("leaf-memory-file-mismatch");
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

describe.skipIf(!linux)("bridge publish: the seat's one allow-list entry, a link restart, and the read-backs", () => {
  it("passes when the real seat route becomes live at +200 s", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    let t = 0;
    const result = await run(w, "publish", { owner }, { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 200_000) { w.gate = gateLive("nats", MACHINE); w.receipt = routable(); }
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 200_000,
      measured: { gate: { status: "live", side: "nats" }, receipt: { verdict: "routable" } } });
    expect(t).toBe(200_000);
  });

  it("refuses publish when the seat route would appear only at +310 s", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    let t = 0;
    const availableAt = 310_000;
    const result = await run(w, "publish", { owner }, { now: () => t, sleep: async ms => {
      t += ms;
      if (t >= availableAt) { w.gate = gateLive("nats", MACHINE); w.receipt = routable(); }
    } });
    expect(result.receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ result: "refused", reason: "seat-not-routable",
      elapsedMs: 300_000, measured: { gate: null, receipt: {} } });
    expect(t).toBe(300_000);
    expect(t).toBeLessThan(availableAt);
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
    expect((await run(stuck, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(stuck, "fabric-v2-bridge-publish-outcome.json").reason).toBe("link-not-reloaded");
    const down = await world();
    setKube(down, MANAGED, { line: identityLine(down) });
    down.gate = gateLive("nats", MACHINE);
    down.receipt = routable();
    // Live before the restart; after it, the other seat's route stays down.
    down.statusAfter = { ok: true, problems: [], allow: { file: [{ identity: LEAD, live: false }], nats: [{ identity: `session:${SID}`, live: true }] } };
    expect((await run(down, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(down, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "other-routes-down",
      measured: { otherRoutes: { checked: [LEAD], down: [LEAD] } } });
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

  it("passes rc-restore-route when its real file-side seat row appears at +200 s", async () => {
    const w = await published();
    w.gate = null;
    w.receipt = {};
    let t = 0;
    const result = await run(w, "rc-restore-route", writer(w), { now: () => t, sleep: async ms => {
      t += ms;
      if (t === 200_000) {
        w.gate = gateLive("file");
        w.receipt = { ok: true, verdict: "routable", side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
          nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }] } };
      }
    } });
    expect(result.receipt).toMatchObject({ result: "pass" });
    expect(evidenceOf(w, "fabric-v2-bridge-rc-restore-route-outcome.json")).toMatchObject({ result: "pass", elapsedMs: 200_000,
      measured: { gate: { status: "live", side: "file" }, receipt: { verdict: "routable", side: "file" } },
      asserted: { v1: { pid: process.pid, rootId: `session:${SID}`, session: { file: "/synthetic/v1/session.jsonl" } } } });
    expect(t).toBe(200_000);
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
