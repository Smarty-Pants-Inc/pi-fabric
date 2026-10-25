import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { procStartTime } from "../src/main-drain/binding.js";
import { readBridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { journalBranch } from "../src/seat-drain/journal-branch.js";
import { main, type BridgeDeps } from "../src/seat-bridge/seam.js";
import { createSeat, SID, type Seat } from "./seat-drain-fixture.js";

// The D3 bridge seam (smarty-dev#15310), synthetic only: a fake kubectl (no cluster), a fake systemctl, fake link
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
const pins = readBridgeNatsSeatPins({ productOwner: { name: "Delta" }, principals: [
  { name: "Delta", orgInstance: { herdrAgent: "org-delta", herdrWorkspaceLabel: "Delta", fabricSession: SID } },
  { name: "Echo", orgInstance: { herdrAgent: "org-echo", herdrWorkspaceLabel: "Echo", fabricSession: "00000000-0000-4000-8000-0000000000e5" } },
] });
const seatFileEntry = { kind: "root", id: `session:${SID}`, principal: "Delta", send: ["followUp"], receive: ["followUp", "cancel"] };
const leadEntry = { kind: "root", id: LEAD, send: ["followUp"] };

const header = JSON.stringify({ type: "session", version: 3, id: SID, timestamp: "2026-10-25T00:00:00.000Z", cwd: "/synthetic" });
const entry = (id: string, parentId: string | null, extra: Record<string, unknown> = {}) =>
  JSON.stringify({ type: "message", id, parentId, timestamp: "2026-10-25T00:00:01.000Z", ...extra });
const SEALED = `${header}\n${entry("e1", null)}\n${entry("e2", "e1")}\n`;
const THINKING = `${JSON.stringify({ type: "thinking_level_change", id: "e3", parentId: "e2", timestamp: "2026-10-25T00:00:02.000Z", thinkingLevel: "high" })}\n`;

interface World {
  seat: Seat; kube: string; marker: string; config: string; calls: () => Array<{ argv: string[] }>; journalFile: string;
  status: { ok: boolean; problems: string[]; allow: { file: Array<Record<string, unknown>>; nats: Array<Record<string, unknown>> } };
  receipt: Record<string, unknown>; roots: Array<Record<string, unknown>>; holderStuck: boolean;
  /** What `nats-link status` reads once the unit was restarted (default: the same as before). */
  statusAfter?: World["status"];
}

const world = async (options: { podJournal?: string; machines?: string[]; allow?: Record<string, unknown> } = {}): Promise<World> => {
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
  // A's final seal of the source journal (SEALED), journaled once.
  const sourceJournal = JSON.parse(fs.readFileSync(path.join(seat.runDir, "seat-run.json"), "utf8")).source.journal as string;
  const seal = Buffer.from(`${JSON.stringify({ schema: "d3-seat-final-seal-v1", run: seat.run, journal: sourceJournal,
    journalSha256: sha(SEALED), journalBytes: Buffer.byteLength(SEALED), leafId: "e2" })}\n`);
  fs.mkdirSync(path.join(seat.runDir, "receipts"), { mode: 0o700 });
  fs.writeFileSync(path.join(seat.runDir, "receipts", "final-seal.json"), seal, { mode: 0o400 });
  records.push({ event: "final-seal", sha256: sha(seal) });
  fs.writeFileSync(journalPath, records.map(record => `${JSON.stringify(record)}\n`).join(""));
  fs.writeFileSync(path.join(kube, "config"), "synthetic", { mode: 0o600 });
  // The Pod's own journal, read by the REAL journal reader to make the in-Pod CLI's line.
  const journalFile = path.join(seat.base, "pod-journal.jsonl");
  fs.writeFileSync(journalFile, options.podJournal ?? SEALED);
  const kubectl = path.join(kube, "kubectl");
  fs.copyFileSync(path.join("tests", "fixtures", "fake-kubectl.py"), kubectl);
  fs.chmodSync(kubectl, 0o700);
  // The synthetic link config: hub file mesh = the seat's mesh, the seat's own Path B machine listed.
  const linkDir = path.join(seat.base, "link");
  fs.mkdirSync(linkDir, { mode: 0o700 });
  const config = path.join(linkDir, "v2.json");
  fs.writeFileSync(config, `${JSON.stringify({ version: 1, name: "hub-a", link: "v2", file: { mesh: seat.meshRoot, lockProtocol: 2 },
    stateDir: path.join(seat.base, "link-state"), nats: { servers: ["tls://nats.example.test:4222"], machine: "hub-a-bridge",
      credentials: "/synthetic/creds", caFile: "/synthetic/ca.pem", custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
      pathBMachines: options.machines ?? ["hub-a-bridge", "sandbox1", "org-d3synth"], operatorMachine: "fabric-ops" },
    allow: options.allow ?? { file: [leadEntry, seatFileEntry] } }, null, 2)}\n`, { mode: 0o600 });
  const marker = path.join(seat.base, "systemctl-calls");
  const systemctl = path.join(seat.base, "systemctl");
  fs.writeFileSync(systemctl, `#!/bin/sh\necho "$@" >> ${marker}\n`, { mode: 0o700 });
  const w: World = {
    seat, kube, marker, config, journalFile,
    calls: () => fs.existsSync(path.join(kube, "kube-calls.jsonl"))
      ? fs.readFileSync(path.join(kube, "kube-calls.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)) : [],
    status: { ok: true, problems: [], allow: { file: [{ identity: LEAD, live: true }, { identity: `session:${SID}`, live: true }], nats: [] } },
    receipt: {}, roots: [], holderStuck: false,
  };
  return w;
};

const setKube = (w: World, pod: { name: string; uid: string }, identity: { line: string; code?: number; changeUid?: string },
  podExtra: Record<string, unknown> = {}) => {
  fs.writeFileSync(path.join(w.kube, "kube-state.json"), JSON.stringify({ server: SERVER, caB64: CA.toString("base64"), cliPresent: true,
    pod: { name: pod.name, uid: pod.uid, meshRoot: "/var/lib/fabric/mesh", running: true, terminated: null, args: ["--session", POD_JOURNAL], ...podExtra },
    exec: { line: "" }, identity }));
};

/** The in-Pod CLI's `--identity` line, with the journal read by the REAL reader over the synthetic Pod journal. */
const identityLine = (w: World, options: { leafId?: string; sessionFile?: string; ownerIncarnation?: string; journal?: unknown } = {}) => {
  const journal = options.journal ?? journalBranch(w.journalFile, "e2", Buffer.byteLength(SEALED));
  const leaf = options.leafId ?? (journal as { leafId: string }).leafId;
  return JSON.stringify({ format: 1, sessionId: SID, ptraceScope: "1", op: "identity", identity: { rootId: `session:${SID}`, pid: 7,
    meshRoot: "/var/lib/fabric/mesh", sessionFile: options.sessionFile ?? POD_JOURNAL, leafId: leaf,
    ownerIncarnation: options.ownerIncarnation ?? INCARNATION }, journal });
};

const request = (seat: Seat, phase: string, inputs: Record<string, unknown>) => JSON.stringify({ schema: "d3-seam-request-v1", run: seat.run,
  runSha256: seat.runSha256, phase, principal: "d3synth", sessionId: SID, inputs });

const run = async (w: World, phase: string, inputs: Record<string, unknown>, extra: Partial<BridgeDeps> = {}) => {
  let stdout = ""; let stderr = "";
  let t = 0;
  const deps: BridgeDeps = {
    kubectl: path.join(w.kube, "kubectl"), podCli: "/opt/pi-fabric/dist/cli/index.js", systemctl: path.join(w.seat.base, "systemctl"),
    seatPins: () => pins, now: () => t, sleep: async ms => { t += ms; }, deadlineMs: 600_000,
    holder: () => ({ pid: !w.holderStuck && fs.existsSync(w.marker) ? 101 + fs.readFileSync(w.marker, "utf8").split("\n").length : 100,
      startTime: "1", purpose: "start" }),
    status: async () => structuredClone(w.statusAfter && fs.existsSync(w.marker) ? w.statusAfter : w.status),
    receipt: async () => structuredClone(w.receipt),
    roots: async () => structuredClone(w.roots) as never,
    onError: error => { if (!(error instanceof Error && error.name === "SeamGap")) throw error; },
    ...extra,
  };
  const code = await main(["--run-dir", w.seat.runDir, "--phase", phase], {
    stdin: () => Buffer.from(request(w.seat, phase, inputs)), stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(w.seat.lockFd()) },
  }, { linkConfig: w.config, linkUnit: "smarty-nats-link.service" }, deps);
  return { code, stdout, stderr, receipt: stdout ? JSON.parse(stdout) as Record<string, any> : undefined };
};

const finalSeal = { journalSha256: sha(SEALED), journalBytes: Buffer.byteLength(SEALED), leafId: "e2" };
const resumeInputs = { pod: RAW, finalSeal };
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
    const w = await world({ podJournal: SEALED + THINKING });
    setKube(w, RAW, { line: identityLine(w) });
    const result = await run(w, "resume", resumeInputs);
    expect(result.receipt).toMatchObject({ result: "pass", observed: { leafId: "e3", journalSha256: sha(SEALED + THINKING),
      suffixBase64: Buffer.from(THINKING).toString("base64"),
      appendedSinceSeal: [{ id: "e3", parentId: "e2", type: "thinking_level_change", thinkingLevel: "high" }] } });
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
    expect(await run(w, "resume", { pod: RAW, finalSeal: { ...finalSeal, leafId: "e1" } }))
      .toMatchObject({ code: 2, stderr: "GAP: final-seal-mismatch [owner: fabric-v2]\n" });
    const file = path.join(w.seat.runDir, "receipts", "final-seal.json");
    fs.chmodSync(file, 0o600); fs.appendFileSync(file, " ");
    expect(await run(w, "resume", resumeInputs)).toMatchObject({ code: 2, stderr: "GAP: final-seal-unjournaled [owner: fabric-v2]\n" });
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

const owner = { template: { name: "t", uid: "00000000-0000-4000-8000-000000000101" }, claim: { name: "c", uid: "00000000-0000-4000-8000-000000000102" },
  sandbox: { name: "s", uid: "00000000-0000-4000-8000-000000000103" }, pod: MANAGED };
// The link's own verdict also counts the old, no longer live NATS key of the v1 mirror; the seam judges live records.
const routable = (incarnation = INCARNATION, machine = "org-d3synth") => ({ ok: false, verdict: "not-routable", side: "nats",
  file: { remoteHost: "v2", live: true, ownerIncarnation: incarnation }, nats: { read: true, records: [
    { machine: "hub-a-bridge", live: false, ownerIncarnation: V1_INCARNATION }, { machine, live: true, ownerIncarnation: incarnation }] } });

describe.skipIf(!linux)("bridge publish: the seat's one allow-list entry, a link restart, and the read-backs", () => {
  it("moves the seat's entry, restarts the pinned unit, and passes on the proof chain (incarnation = the managed Pod UID)", async () => {
    const w = await world();
    setKube(w, MANAGED, { line: identityLine(w) });
    w.receipt = routable();
    const result = await run(w, "publish", { owner });
    const config = JSON.parse(fs.readFileSync(w.config, "utf8"));
    expect(config.allow).toEqual({ file: [leadEntry], nats: [{ kind: "root", id: `session:${SID}`, principal: "Delta", machine: "org-d3synth",
      send: ["followUp"], receive: ["followUp", "cancel"] }] });
    expect(fs.readFileSync(w.marker, "utf8")).toBe("--user restart smarty-nats-link.service\n");
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: w.seat.run, phase: "publish", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, incarnation: MANAGED.uid, route: `v2@${MANAGED.uid}:${sha(fs.readFileSync(w.config)).slice(0, 12)}` } });
    const intent = evidenceOf(w, "fabric-v2-bridge-publish-intent.json");
    expect(intent.plan).toMatchObject({ change: "move", fileEntry: { index: 1, entry: seatFileEntry } });
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
    w.receipt = routable("00000000-0000-4000-8000-0000000009ff");
    expect((await run(w, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(w, "fabric-v2-bridge-publish-outcome.json")).toMatchObject({ reason: "seat-not-routable", written: true });
    const other = await world();
    setKube(other, MANAGED, { line: identityLine(other) });
    other.receipt = routable(INCARNATION, "sandbox1");
    expect((await run(other, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
  });

  it("refuses when the link did not reload, or another route it carried is not back", async () => {
    const stuck = await world();
    setKube(stuck, MANAGED, { line: identityLine(stuck) });
    stuck.receipt = routable();
    stuck.holderStuck = true;
    expect((await run(stuck, "publish", { owner })).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(stuck, "fabric-v2-bridge-publish-outcome.json").reason).toBe("link-not-reloaded");
    const down = await world();
    setKube(down, MANAGED, { line: identityLine(down) });
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
    // A runs seams with a fixed env (no NODE_OPTIONS from this test runner).
    const env = { PATH: process.env.PATH ?? "/usr/bin:/bin", LANG: "C", LC_ALL: "C" };
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
    // A bad unit name or an unsafe link config is refused at install.
    expect(spawnSync("node", [bundle, "--install", "--out", `${out}-2`, "--link-config", w.config, "--link-unit", "x;y"], { env }).status).toBe(2);
    const { runSeam } = await import("./seat-drain-fixture.js");
    const inputs = writerInputs(w);
    const body = request(w.seat, "rc-restore-route", inputs);
    expect(await runSeam(w.seat, out, "rc-restore-route", body, true)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: lock-not-held [owner: fabric-v2]\n" });
    // With the lock, the installed seam reads ITS pinned config and the seat's mesh, then stops on a link it cannot read.
    expect(await runSeam(w.seat, out, "rc-restore-route", body)).toMatchObject({ code: 2, stdout: "",
      stderr: "GAP: link-status-unreadable [owner: fabric-v2]\n" });
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
    expect((await run(w, "publish", { owner })).receipt?.result).toBe("pass");
    w.seat.native.state.sessionFile = "/synthetic/v1/session.jsonl";
    w.receipt = { ok: false, verdict: "not-routable", side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION },
      nats: { read: true, records: [{ machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION },
        { machine: "org-d3synth", live: false, ownerIncarnation: INCARNATION }] } };
    // After the restore, the hub still holds the v2 seat's old mirror row: the one tolerated status problem.
    w.statusAfter = { ok: false, problems: [`mirror not on allow.nats: session:${SID}`],
      allow: { file: [{ identity: LEAD, live: true }, { identity: `session:${SID}`, live: true }], nats: [] } };
    w.roots = [{ id: `session:${SID}`, machine: "hub-a-bridge", live: true, ownerIncarnation: V1_INCARNATION }];
    return w;
  };

  it("puts the seat's file entry back exactly, restarts the link, and passes on the live v1 writer", async () => {
    const w = await published();
    const result = await run(w, "rc-restore-route", writer(w));
    expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: w.seat.run, phase: "rc-restore-route", owner: "fabric-v2", result: "pass",
      observed: { sessionId: SID, route: `v1@${process.pid}:${procStartTime(process.pid)}` } });
    expect(JSON.parse(fs.readFileSync(w.config, "utf8")).allow).toEqual({ file: [leadEntry, seatFileEntry], nats: [] });
    expect(fs.readFileSync(w.marker, "utf8").trim().split("\n")).toHaveLength(2);
  });

  it("refuses while a v2 presence for the session is still live, or when the writer is not the live owner", async () => {
    const live = await published();
    live.roots = [...live.roots, { id: `session:${SID}`, machine: "org-d3synth", live: true, ownerIncarnation: INCARNATION }];
    expect((await run(live, "rc-restore-route", writer(live))).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(live, "fabric-v2-bridge-rc-restore-route-outcome.json").reason).toBe("v2-presence-live");
    const wrong = await published();
    expect((await run(wrong, "rc-restore-route", writer(wrong, { pid: process.pid + 100_000 }))).receipt).toMatchObject({ result: "refused" });
    expect(evidenceOf(wrong, "fabric-v2-bridge-rc-restore-route-outcome.json").reason).toBe("v1-writer-mismatch");
  });

  it("a rollback before any publish write changes nothing and restarts nothing", async () => {
    const w = await world();
    w.seat.native.state.sessionFile = "/synthetic/v1/session.jsonl";
    w.receipt = { side: "file", file: { remoteHost: null, live: true, ownerIncarnation: V1_INCARNATION }, nats: { read: true, records: [] } };
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

  it("never removes a seat entry that publish did not write in this RUN", async () => {
    const w = await world({ allow: { file: [leadEntry], nats: [{ kind: "root", id: `session:${SID}`, principal: "Delta", machine: "org-d3synth",
      send: ["followUp"], receive: ["followUp", "cancel"] }] } });
    const before = fs.readFileSync(w.config);
    expect(await run(w, "rc-restore-route", writer(w))).toMatchObject({ code: 2, stderr: "GAP: seat-entry-conflict [owner: fabric-v2]\n" });
    expect(fs.readFileSync(w.config)).toEqual(before);
  });
});
