// The D3 bridge seam (principal-seat move) on the REAL path short of Kubernetes and systemd: a real authenticated
// nats-server (pinned in CI), the real A' link in-process (tests/helpers/nats-file-mixed.ts), real FILE and NATS
// Mains, the link's real readers (status, receipt, roots) and its REAL identity gate (dry-run). Stand-ins, all
// test-only deps the installed bundle cannot reach (seat-bridge-seam.test.ts asserts it):
// - kubectl (tests/fixtures/fake-kubectl.py): the managed Pod and its in-image CLI's identity line, built from the
//   real NATS Main's own presence;
// - the unit restart: the in-process link is restarted with the config the seam wrote; the lock holder is that
//   restart's generation (the in-process link takes no CLI lock);
// - the link's state directory: the test deletion guard confines renames to the private TMPDIR,
//   which the production rule refuses as temporary; the seam counts that ONE exact directory as durable, and the
//   first test shows the rule still refuses it without that exemption.
// Synthetic ids only; the seat's pins come from the harness's fixture org.json.
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { parseNatsLinkConfig } from "../src/mesh/bridge-nats-config.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { main, type BridgeDeps } from "../src/seat-bridge/seam.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, E2E_WAIT_MS, linkTail, NATS_MAIN_MACHINE, NATS_PEER_SESSION, natsLinkPresent, rootId, SEAT_PRINCIPAL,
  selfRecord, waitForValue, type MixedHarness, type MixedMain,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the bridge seam E2E needs NATS_BIN");
const linked = natsLinkPresent();
const linux = process.platform === "linux" && typeof process.getuid === "function";
const sha = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");
const SEAT = rootId(NATS_PEER_SESSION);
const CA = Buffer.from("synthetic CA bytes");
const SERVER = "https://synthetic.invalid:6443";
const MANAGED = { name: "org-d3synth-0", uid: "00000000-0000-4000-8000-0000000000bb" };
const OWNER = { template: { name: "t", uid: "00000000-0000-4000-8000-000000000101" }, claim: { name: "c", uid: "00000000-0000-4000-8000-000000000102" },
  sandbox: { name: "s", uid: "00000000-0000-4000-8000-000000000103" }, pod: MANAGED };
/** The seat while on v1 (Ryzen 1's real shape): its root on allow.file, as a principal seat. */
const seatFileEntry = { kind: "root" as const, id: SEAT, principal: SEAT_PRINCIPAL, send: ["followUp" as const], receive: ["followUp" as const, "cancel" as const] };

interface Scene {
  h: MixedHarness; runDir: string; run: string; runSha256: string; config: string; kube: string; lockFd: number;
  restarts: number; v1?: MixedMain;
}

/** A sealed synthetic RUN for the seat session, as A and the preflight seam would leave it, with its lock held. */
const sealRun = (h: MixedHarness, kube: string): Pick<Scene, "runDir" | "run" | "runSha256" | "lockFd"> => {
  const runDir = path.join(h.root, "run");
  fs.mkdirSync(runDir, { mode: 0o700 });
  // The seat's sealed source journal: its header cwd names a project whose Fabric config points at the hub mesh.
  const project = path.join(h.root, "seat-project");
  fs.mkdirSync(path.join(project, ".pi"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(project, ".pi", "fabric.json"), JSON.stringify({ mesh: { root: h.fileMeshRoot } }), { mode: 0o600 });
  const journal = path.join(h.root, "seat-agent", "sessions", "--seat--", `2026-10-25T00-00-00-000Z_${NATS_PEER_SESSION}.jsonl`);
  fs.mkdirSync(path.dirname(journal), { recursive: true, mode: 0o700 });
  fs.writeFileSync(journal, `${JSON.stringify({ type: "session", version: 3, id: NATS_PEER_SESSION, timestamp: "2026-10-25T00:00:00.000Z", cwd: project })}\n`,
    { mode: 0o600 });
  const run = randomBytes(16).toString("hex");
  const runBytes = Buffer.from(JSON.stringify({ schema: "d3-seat-run-v1", run, org: "smarty-pants-inc", principal: "d3synth", instance: "org-d3synth",
    sessionId: NATS_PEER_SESSION, plannedSeats: ["d3synth"], context: "synthetic", namespace: "org-synthetic",
    namespaceUID: "00000000-0000-4000-8000-000000000001", natsSecret: "org-d3synth-nats",
    source: { journal, journalSha256: "0".repeat(64), journalBytes: 1, leafId: "leaf", handoffs: null },
    target: { claim: "synthetic-claim", node: "synthetic-node" }, natsMachine: NATS_MAIN_MACHINE,
    seams: { bridge: { owner: "fabric-v2", path: "/synthetic/bridge", sha256: "3".repeat(64) } },
    recoveryRoot: "/synthetic/recovery", createdAt: "2026-10-25T00:00:00Z" }));
  fs.writeFileSync(path.join(runDir, "seat-run.json"), runBytes, { mode: 0o400 });
  const runSha256 = sha(runBytes);
  const inputBytes = Buffer.from(JSON.stringify({ schema: "d3-seat-inputs-v1", run, runSha256,
    seat: { uid: process.getuid!(), home: os.userInfo().homedir }, kube: { kubeconfig: path.join(kube, "config"), server: SERVER, caSha256: sha(CA) } }));
  fs.writeFileSync(path.join(runDir, "seat-inputs.json"), inputBytes, { mode: 0o400 });
  fs.writeFileSync(path.join(runDir, "seat-journal.jsonl"), [{ event: "begin", run, runSha256 }, { event: "inputs", sha256: sha(inputBytes) }]
    .map(record => `${JSON.stringify(record)}\n`).join(""), { mode: 0o600 });
  fs.writeFileSync(path.join(runDir, "seat.lock"), "", { mode: 0o600 });
  // A held exclusive flock on THIS process's open file description, as A passes it (a child takes it on the shared OFD).
  const lockFd = fs.openSync(path.join(runDir, "seat.lock"), fs.constants.O_RDWR);
  const taken = spawnSync("/usr/bin/python3", ["-c", "import fcntl; fcntl.flock(3, fcntl.LOCK_EX | fcntl.LOCK_NB)"], { stdio: ["ignore", "ignore", "pipe", lockFd] });
  if (taken.status !== 0) throw new Error("could not take the synthetic seat lock");
  return { runDir, run, runSha256, lockFd };
};

/** The managed Pod (fake kubectl) whose in-image CLI reports the REAL NATS seat Main's identity. */
const podFor = (scene: Scene, seatMain: MixedMain) => {
  const self = selfRecord(seatMain)!;
  const line = JSON.stringify({ format: 1, sessionId: NATS_PEER_SESSION, ptraceScope: "1", op: "identity", identity: { rootId: SEAT, pid: 7,
    meshRoot: "/var/lib/fabric/mesh", sessionFile: "/var/lib/fabric/sessions/seat.jsonl", leafId: "e1", ownerIncarnation: self.ownerIncarnation } });
  fs.writeFileSync(path.join(scene.kube, "kube-state.json"), JSON.stringify({ server: SERVER, caB64: CA.toString("base64"), cliPresent: true,
    pod: { name: MANAGED.name, uid: MANAGED.uid, meshRoot: "/var/lib/fabric/mesh", running: true, terminated: null, natsMachine: NATS_MAIN_MACHINE },
    exec: { line: "" }, identity: { line } }));
  return self.ownerIncarnation!;
};

const setUp = async (): Promise<Scene> => {
  const h = await createMixedHarness({ binary: binary!, natsPeer: true, seat: true,
    allow: (allow, ids) => {
      // The seat is NOT on allow.nats yet: on v1 it is a file-side principal seat (the move turns it into a NATS one).
      allow.nats = allow.nats.filter(entry => entry.id !== ids.natsPeer);
      allow.file = [...allow.file, seatFileEntry];
    } });
  const kube = path.join(h.root, "kube");
  fs.mkdirSync(kube, { mode: 0o700 });
  fs.writeFileSync(path.join(kube, "config"), "synthetic", { mode: 0o600 });
  fs.copyFileSync(path.join("tests", "fixtures", "fake-kubectl.py"), path.join(kube, "kubectl"));
  fs.chmodSync(path.join(kube, "kubectl"), 0o700);
  // The pinned config file the seam may change: exactly the config the running link was started with.
  const configDir = path.join(h.root, "link-config");
  fs.mkdirSync(configDir, { mode: 0o700 });
  const config = path.join(configDir, "v2.json");
  fs.writeFileSync(config, `${JSON.stringify(h.linkConfig, null, 2)}\n`, { mode: 0o600 });
  return { h, kube, config, restarts: 0, ...sealRun(h, kube) };
};

const tearDown = async (scene: Scene | undefined) => {
  if (!scene) return;
  try { fs.closeSync(scene.lockFd); } catch { /* closed */ }
  await scene.h.stop();
};

/** Run one bridge phase as A would (the held seat lock, one request), with the real link readers and gate. */
const phase = async (scene: Scene, name: string, inputs: Record<string, unknown>, options: { exempt?: boolean; deadlineMs?: number } = {}) => {
  let stdout = ""; let stderr = "";
  const deps: BridgeDeps = {
    ...(options.deadlineMs === undefined ? {} : { deadlineMs: options.deadlineMs }),
    ...(options.exempt === false ? {} : { durableStateDir: scene.h.linkConfig.stateDir }),
    kubectl: path.join(scene.kube, "kubectl"), podCli: "/opt/pi-fabric/dist/cli/index.js",
    // TEST-ONLY: the unit restart is an in-process link restart with the config the seam just wrote (parsed by the
    // link's own parser), and the lock holder is that restart's generation (the in-process link takes no CLI lock).
    restartLink: async () => {
      const parsed = parseNatsLinkConfig(JSON.parse(fs.readFileSync(scene.config, "utf8")));
      await scene.h.restartLink(parsed);
      scene.restarts += 1;
      return 0;
    },
    holder: () => ({ pid: process.pid, startTime: `generation-${scene.restarts}`, purpose: "start" }),
    onError: error => { if (!(error instanceof Error && error.name === "SeamGap")) throw error; },
  };
  const code = await main(["--run-dir", scene.runDir, "--phase", name], {
    stdin: () => Buffer.from(JSON.stringify({ schema: "d3-seam-request-v1", run: scene.run, runSha256: scene.runSha256, phase: name,
      principal: "d3synth", sessionId: NATS_PEER_SESSION, inputs })),
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
    env: { D3_SEAT_LOCK_FD: String(scene.lockFd) },
  }, { linkConfig: scene.config, linkUnit: "smarty-nats-link.service" }, deps);
  const outcome = path.join(scene.runDir, `fabric-v2-bridge-${name}-outcome.json`);
  return { code, stdout, stderr, receipt: stdout ? JSON.parse(stdout) as Record<string, any> : undefined,
    outcome: fs.existsSync(outcome) ? JSON.parse(fs.readFileSync(outcome, "utf8")) as Record<string, any> : undefined };
};

const allowOf = (scene: Scene) => (JSON.parse(fs.readFileSync(scene.config, "utf8")) as { allow: { file: Array<{ id?: string }>; nats: Array<{ id?: string }> } }).allow;

describe.skipIf(!binary || !linux || !linked)("the D3 bridge seam on a real broker and a real A' link", () => {
  describe("publish -> route (real gate) -> a message -> rc-restore-route -> the v1 route", () => {
    let scene: Scene | undefined;
    beforeAll(async () => { scene = await setUp(); }, E2E_WAIT_MS + 60_000);
    afterAll(async () => { await tearDown(scene); }, 90_000);

    it("moves the seat's entry, restarts the link, and passes only once the REAL gate routes the seat on its machine", async () => {
      const s = scene!;
      const before = fs.readFileSync(s.config);
      const incarnation = podFor(s, s.h.natsPeer!);
      // The production rule: this link's state is in the test's private temp store, so without the exemption the seam
      // refuses before any write.
      const temporary = await phase(s, "publish", { owner: OWNER }, { exempt: false });
      expect(temporary).toMatchObject({ code: 2, stdout: "", stderr: "GAP: link-state-dir-temporary [owner: fabric-v2]\n", outcome: undefined });
      expect(fs.readFileSync(s.config)).toEqual(before);
      expect(s.restarts).toBe(0);
      const result = await phase(s, "publish", { owner: OWNER });
      if (result.receipt?.result !== "pass") throw new Error(`publish: ${JSON.stringify(result.outcome?.reason ?? result.stderr)}\n${linkTail(s.h)}`);
      expect(result.receipt).toEqual({ schema: "d3-seam-receipt-v1", run: s.run, phase: "publish", owner: "fabric-v2", result: "pass",
        observed: { sessionId: NATS_PEER_SESSION, incarnation: MANAGED.uid, route: `v2@${MANAGED.uid}:${sha(fs.readFileSync(s.config)).slice(0, 12)}` } });
      expect(s.restarts).toBe(1);
      expect(allowOf(s).file.map(entry => entry.id)).not.toContain(SEAT);
      expect(allowOf(s).nats.map(entry => entry.id)).toContain(SEAT);
      // The real gate's row and the real records the pass rests on.
      expect(result.outcome!.measured.gate).toMatchObject({ key: SEAT, side: "nats", status: "live", machine: NATS_MAIN_MACHINE });
      expect(result.outcome!.measured.receipt).toMatchObject({ side: "nats", file: { remoteHost: s.h.linkName, live: true, ownerIncarnation: incarnation } });
      expect(result.outcome!.measured.otherRoutes.down).toEqual([]);
      // The rollback authority names the pre-publish bytes.
      const intent = JSON.parse(fs.readFileSync(path.join(s.runDir, "fabric-v2-bridge-publish-intent.json"), "utf8"));
      expect(intent.rollback.beforeSha256).toBe(sha(before));
    }, E2E_WAIT_MS * 4);

    it("one message crosses the new route: the hub Main reaches the moved seat through the link", async () => {
      const s = scene!;
      const token = `SEAT_ROUTE_${randomUUID()}`;
      await expect(s.h.fileMain.invoke("agents.followUp", { id: SEAT, message: token, idempotencyKey: `r-${randomUUID()}` })
        .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(s.h)}`); })).resolves.toMatchObject({ acknowledged: true });
      await waitForValue(() => s.h.natsPeer!.delivered().some(row => row.text.includes(token)) ? true : undefined, "the seat received it", E2E_WAIT_MS);
    }, E2E_WAIT_MS * 2);

    it("rollback: v2 stops, v1 relaunches, rc-restore-route puts the config back byte for byte and passes on the live v1", async () => {
      const s = scene!;
      const original = Buffer.from(JSON.parse(fs.readFileSync(path.join(s.runDir, "fabric-v2-bridge-publish-intent.json"), "utf8")).rollback.beforeBytesBase64,
        "base64");
      await s.h.natsPeer!.stop();
      const v1File = path.join(s.h.root, "v1-session.jsonl");
      s.v1 = await s.h.addFileMain("v1-seat", NATS_PEER_SESSION, { sessionManager: { getSessionId: () => NATS_PEER_SESSION,
        getSessionFile: () => v1File, getBranch: () => [], getLeafId: () => null, getEntries: () => [] } });
      const result = await phase(s, "rc-restore-route", { recoveryRun: "c".repeat(32), parentRun: s.run, parentRunSha256: s.runSha256,
        writer: { sessionId: NATS_PEER_SESSION, pid: process.pid, sessionFile: v1File, singleWriter: true } });
      if (result.receipt?.result !== "pass") throw new Error(`rc-restore-route: ${JSON.stringify(result.outcome?.reason ?? result.stderr)} ` +
        `${JSON.stringify(result.outcome?.measured?.gate)} ${JSON.stringify(result.outcome?.measured?.receipt)}\n${linkTail(s.h)}`);
      expect(result.receipt.observed.sessionId).toBe(NATS_PEER_SESSION);
      expect(result.receipt.observed.route).toMatch(new RegExp(`^v1@${process.pid}:[0-9]+$`));
      expect(fs.readFileSync(s.config)).toEqual(original);
      expect(s.restarts).toBe(2);
      expect(result.outcome!.measured.gate).toMatchObject({ key: SEAT, side: "file", status: "live" });
      expect(result.outcome!.measured.v2Live).toEqual([]);
    }, E2E_WAIT_MS * 4);

    it("the v1 route carries a message again: the moved-back seat is reached from the NATS side through the link", async () => {
      const s = scene!;
      const token = `V1_ROUTE_${randomUUID()}`;
      await expect(s.h.natsMain.invoke("agents.followUp", { id: SEAT, message: token, idempotencyKey: `v-${randomUUID()}` })
        .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(s.h)}`); })).resolves.toMatchObject({ acknowledged: true });
      await waitForValue(() => s.v1!.delivered().some(row => row.text.includes(token)) ? true : undefined, "the v1 seat received it", E2E_WAIT_MS);
    }, E2E_WAIT_MS * 2);
  });

  describe("a stale presence record for the seat", () => {
    let scene: Scene | undefined;
    beforeAll(async () => { scene = await setUp(); }, E2E_WAIT_MS + 60_000);
    afterAll(async () => { await tearDown(scene); }, 90_000);

    it("a stale mirror row of the seat: the link's REAL gate refuses it, so publish refuses and never claims the route", async () => {
      const s = scene!;
      // The seat on v1: the link mirrors its FILE presence onto NATS under the link's own machine. Keep that row's
      // exact value, let the v1 seat leave (the link withdraws the row), then put the row back: what a link that died
      // without its graceful withdraw leaves behind (no lease: not live), until the KV TTL ages it out.
      await s.h.natsPeer!.stop();
      const v1 = await s.h.addFileMain("v1-seat", NATS_PEER_SESSION);
      const mirrored = await waitForValue(async () => (await s.h.kvRoot(BRIDGE_MACHINE, SEAT))?.presence, "the link mirrors the v1 seat",
        E2E_WAIT_MS);
      expect(mirrored).toMatchObject({ id: SEAT });
      await v1.stop();
      await waitForValue(async () => (await s.h.kvRoot(BRIDGE_MACHINE, SEAT))?.presence ? undefined : true, "the link withdraws it",
        E2E_WAIT_MS);
      await s.h.natsPeer!.start();
      podFor(s, s.h.natsPeer!);
      const writer = await s.h.openPeer(BRIDGE_MACHINE, `stale-writer-${randomUUID()}`);
      // The same KV owner the link wrote (`<ownerHostId>#<ownerIncarnation>`: readers drop a row whose owner differs).
      await writer.kv("presence").put(`${BRIDGE_MACHINE}.p.${hashToken(SEAT)}`, `${mirrored.ownerHostId}#${mirrored.ownerIncarnation}`, mirrored, 0);
      // A bounded phase (the gate would refuse until the row ages out of the 10-minute KV TTL).
      const result = await phase(s, "publish", { owner: OWNER }, { deadlineMs: 30_000 });
      // The NATS directory the gate reads hides an id with rows on two machines (live or not), so the gate's row says
      // "no nats presence" (TARGET_UNAVAILABLE), not AMBIGUOUS, while the live v2 row is right there in the KV.
      if (result.outcome?.measured?.gate?.status !== "BRIDGE_NATS_TARGET_UNAVAILABLE") {
        throw new Error(`publish: ${JSON.stringify(result.outcome?.measured?.gate)} ${JSON.stringify(result.outcome?.measured?.receipt)}\n` +
          `KV a: ${JSON.stringify(await s.h.kvRoot(BRIDGE_MACHINE, SEAT))}\nKV b: ${JSON.stringify(await s.h.kvRoot(NATS_MAIN_MACHINE, SEAT))}\n${linkTail(s.h)}`);
      }
      expect(result.outcome).toMatchObject({ reason: "seat-route-refused-by-gate", written: true,
        measured: { gate: { key: SEAT, status: "BRIDGE_NATS_TARGET_UNAVAILABLE", reason: `${SEAT} has no nats presence` },
          receipt: { ok: false, nats: { read: true, records: [] } } } });
      // The refusal is truthful: the stale row is still there next to the seat's live v2 row.
      expect((await s.h.kvRoot(BRIDGE_MACHINE, SEAT))?.presence).toEqual(mirrored);
      expect((await s.h.kvRoot(NATS_MAIN_MACHINE, SEAT))?.lease).toBeDefined();
      expect(result.receipt).toMatchObject({ result: "refused", phase: "publish" });
    }, E2E_WAIT_MS * 4);
  });
});
