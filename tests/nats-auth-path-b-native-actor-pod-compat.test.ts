// expected-skip: non-Linux: Linux kernel session/OWNER fence + detached native SDK coverage required.
// Keep: real mixed-version trust boundary + durable application-fixture data safety (DESIGN N8-N10).
// Required broker/SDK/history/build/live launcher custody failures are BLOCKED, never successful skips.
// Synthetic N8 composition, NOT N9/N10, #344 CLIENT, installed/canary or actual-app acceptance.
import fs from "node:fs";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { startNatsLink, type NatsLinkHandle, type NatsLinkStartConfig } from "../src/mesh/bridge-nats-link.js";
import { readNativeIngressReceipt } from "../src/mesh/bridge-nats-ingress.js";
import { bridgeNatsMappedCommandId, type BridgeNatsPort } from "../src/mesh/bridge-nats-types.js";
import { openNatsBridgeSide } from "../src/mesh/bridge-nats-port.js";
import type { TransportEnvelope, TransportSent } from "../src/transport/types.js";
import { residentHostId } from "../src/residency/protocol.js";
import { readResidentGenerationExit } from "../src/residency/generation-exit.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { readHostLeaseCurrent } from "../src/topology/host-leases.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import { FROZEN_F3393715_COMMIT, FROZEN_F3393715_SHA256 } from "./helpers/frozen-f3393715.js";
import { createNativeActorOwner, type NativeActorOwner } from "./helpers/native-actor-owner.js";
import { launchLog, same, stopOwned, type Owned } from "./helpers/owned-processes.js";
import { NatsWireFaults } from "./helpers/nats-wire-faults.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, PATH_B_POLICY_ENV, resolveNatsBinary,
  teardownWithin, type PathBHarness, type PathBUser } from "./helpers/nats-cluster.js";

const repo = fileURLToPath(new URL("../", import.meta.url));
const producerModule = fileURLToPath(new URL("./fixtures/native-old-provider-pod.mjs", import.meta.url));
const residentModule = fileURLToPath(new URL("./fixtures/native-inbox-resident-handler.mjs", import.meta.url));
// Dedicated synthetic identity, NEVER the reserved production actor.
const ACTOR = "34200000000000000000000000000008";
const RPC = "smarty-inbox.rpc.v1";
const HUB = "machine-hub", UNAUTHORIZED = "machine-unauthorized";
const INVENTORY = ["machine-a", "machine-b", HUB, UNAUTHORIZED];
const zeros = { beforeAgentStart: 0, agentStart: 0, providerRequest: 0, providerStream: 0, providerResponse: 0, toolCall: 0, toolExecution: 0 };
const json = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const save = (file: string, value: unknown): void => fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", { mode: 0o600, flag: "wx" });
const sha256 = (file: string): string => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
type Pin = { rootId: string; sessionId: string; sessionFile: string };
type Ready = { pin: Pin; timeoutMs: number; sdkUnconfigured: boolean; counts: typeof zeros;
  nats: { machine: string; custodyStream: string; livenessStream: string; pathBMachines: string[] };
  provenance: { commit: string; sourceTree: string; archiveSha256: string; sha256: typeof FROZEN_F3393715_SHA256 } };
type Item = { id: string; text: string; resolved: boolean };
type ReplyData = { v: number; requestId: string; ok: boolean; status: string; result?: { item?: Item; items?: Item[] }; error?: { code: string } };
type Reply = { id: string; actorId: string; direction: string; data: ReplyData };
type Request = { v: 1; requestId: string; operation: "add" | "list" | "resolve"; opKey: string; args: Record<string, unknown> };
type Callback = { pid: number; moduleLoadId: string; databaseOpenId: string; enteredAt: number; settledAt: number; request: { message: string; data: Request };
  context: { actorId: string; source: string; sender: { id: string; kind: string; sessionId: string }; sourceMachine: string;
    link: string; commandId: string; sourceCommandId: string; sourceEventId: string; deadlineAt: number; signalAborted: boolean };
  data: ReplyData; readback: { items: number; operations: number; addEffects: number; resolveEffects: number } };

/** Separate actual old provider process, positive-allowlist test environment.
 * Preserves the inherited guard and owned-launch preload; never copies Main's
 * model/auth profile, sender role, session override or ambient API credentials.
 * All source/session/result/lifecycle bytes stay under owner.root for its final
 * evidence manifest + the external live launcher's archive-before-cleanup gate.
 */
const startOldPod = (owner: NativeActorOwner, broker: PathBHarness, user: PathBUser<string>, label: string,
  sdkDirectory: string, nodeBinary: string) => {
  const root = path.join(owner.root, `old-provider-${label}`);
  fs.mkdirSync(root, { mode: 0o700 });
  const cwd = path.join(root, "service"), agentDir = path.join(root, "profile"), sessions = path.join(root, "sessions");
  const sourceDirectory = path.join(root, "frozen-source");
  for (const directory of [cwd, agentDir, sessions, sourceDirectory]) fs.mkdirSync(directory, { mode: 0o700 });
  save(path.join(agentDir, "settings.json"), { modelPolicy: { allow: [] }, packages: [], extensions: [],
    enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } });
  const pinsFile = path.join(root, "pod-pins.json");
  save(pinsFile, { cwd, agentDir, sourceDirectory, sdkDirectory, nodeBinary, repoRoot: repo, actorId: owner.binding.actorId,
    sessionFile: path.join(sessions, "pod.jsonl"), sessionPin: path.join(root, "session-pin.json"),
    nativeOwnerEntry: owner.nativeOwnerEntry, nativeOwnerEntrySha256: sha256(owner.nativeOwnerEntry),
    policyEnv: broker.policyEnv, nats: { ...broker.runtimeOptions(user), operatorMachine: "fabric-ops" } });
  const owned = launchLog(root);
  const env: NodeJS.ProcessEnv = Object.fromEntries(["PATH", "LANG", "LC_ALL", "CI", "GITHUB_ACTIONS", "RUNNER_ENVIRONMENT", "RUNNER_TEMP", "PI_TEST_FLEET_HOST"]
    .flatMap(key => process.env[key] === undefined ? [] : [[key, process.env[key]]]));
  Object.assign(env, owned.env, broker.policyEnv, { PI_OFFLINE: "1", HOME: path.resolve(owner.root, "../home"), USERPROFILE: path.resolve(owner.root, "../home"),
    TMPDIR: root, TMP: root, TEMP: root, PI_CODING_AGENT_DIR: agentDir, PI_CODING_AGENT_SESSION_DIR: sessions,
    XDG_CONFIG_HOME: path.join(root, "config"), XDG_CACHE_HOME: path.join(root, "cache"), XDG_DATA_HOME: path.join(root, "data"),
    XDG_STATE_HOME: path.join(root, "state"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh"), PI_FABRIC_PROJECT_ROOT: cwd,
    PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_LOCK_STATS: "0",
    FABRIC_NATIVE_OLD_POD_PINS: pinsFile });
  // No current provider, makeMain(), fake-worker, NatsWire prototype patch, or
  // in-memory owner. jiti resolution is wholly inside the pinned old archive.
  const child = spawn(nodeBinary, [producerModule], { cwd, env, stdio: ["ignore", "pipe", "pipe", "ipc"] });
  const lifetime = watchResidentChild(child);
  let readyResolve!: (value: Ready) => void, readyReject!: (error: unknown) => void;
  let readySettled = false, sequence = 0;
  const pending = new Map<string, { resolve(value: unknown): void; reject(error: unknown): void }>();
  const ready = new Promise<Ready>((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  // Observe failure immediately even when the other seat is still initializing.
  void ready.catch(() => undefined);
  const readyTimer = setTimeout(() => { readyReject(new Error(`N8 BLOCKED: ${label} actual frozen producer startup deadline; see ${root}`)); }, 90_000);
  const terminal = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("close", (code, signal) => {
      save(path.join(root, "parent-native-close.json"), { pid: child.pid, code, signal, launchReceipts: owned.owned() });
      const error = new Error(`Old provider ${label} terminal before pending result (${code}/${signal}); see ${root}`);
      clearTimeout(readyTimer);
      if (!readySettled) readyReject(error);
      for (const waiter of pending.values()) waiter.reject(error);
      pending.clear(); resolve({ code, signal });
    });
  });
  child.once("error", error => { clearTimeout(readyTimer); readyReject(error); for (const waiter of pending.values()) waiter.reject(error); pending.clear(); });
  child.stdout!.on("data", bytes => fs.appendFileSync(path.join(root, "stdout.log"), bytes, { mode: 0o600 }));
  child.stderr!.on("data", bytes => fs.appendFileSync(path.join(root, "stderr.log"), bytes, { mode: 0o600 }));
  child.on("message", (message: unknown) => {
    const value = message as { event?: string; id?: string; ok?: boolean; result?: unknown; error?: string };
    if (value.event === "ready") { readySettled = true; clearTimeout(readyTimer); readyResolve(message as Ready); }
    else if (value.id) {
      const waiter = pending.get(value.id); if (!waiter) return;
      pending.delete(value.id);
      if (value.ok) waiter.resolve(value.result); else waiter.reject(new Error(value.error ?? "Old provider refused"));
    }
  });
  const call = <T>(action: string, payload: Record<string, unknown> = {}, ms = 65_000): Promise<T> => {
    if (!child.connected || lifetime.exited) return Promise.reject(new Error("Actual old provider is not running"));
    const id = `${label}-${++sequence}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`N8 old provider ${action} observation deadline; NOT business notRun`)); }, ms);
      pending.set(id, { resolve: value => { clearTimeout(timer); resolve(value as T); }, reject: error => { clearTimeout(timer); reject(error); } });
      child.send({ ...payload, id, action }, error => { if (error) { pending.get(id)?.reject(error); pending.delete(id); } });
    });
  };
  return { root, child, ready, call,
    async stop(): Promise<void> {
      let failure: unknown;
      try {
        if (!lifetime.exited) {
          if (readySettled) await call("stop");
          else await lifetime.stop(); // Only captured owned ChildProcess, native exit required.
        }
        const result = await teardownWithin(`${label} actual terminal CLOSE`, () => terminal, 10_000);
        if (readySettled && (result.code !== 0 || result.signal !== null)) throw new Error(`Old producer ${label} did not stop cleanly`);
      } catch (error) { failure = error; }
      if (failure) {
        await lifetime.stop();
        await teardownWithin(`${label} forced actual terminal CLOSE`, () => terminal, 10_000);
        throw failure;
      }
    },
  };
};
type OldPod = ReturnType<typeof startOldPod>;

const provision = async (broker: PathBHarness): Promise<void> => {
  const env = broker.adminEnv(broker.users.bootstrap);
  const previous = Object.fromEntries(Object.keys(env).map(key => [key, process.env[key]]));
  for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value);
  try {
    const result = await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", broker.permissionPolicy.pathBMachines.join(",")]);
    if (result !== 0) throw new Error(`N8 BLOCKED: owned test broker provisioning failed (${result})`);
  } finally { for (const [key, value] of Object.entries(previous)) vi.stubEnv(key, value); }
};
const database = (owner: NativeActorOwner) => {
  const db = new DatabaseSync(path.join(owner.root, "native-inbox.sqlite"), { readOnly: true });
  try {
    return {
      items: db.prepare("SELECT id,seat,text,resolved FROM fixture_items ORDER BY seat,id").all(),
      effects: db.prepare("SELECT seat,operation,COUNT(*) AS n FROM fixture_effects GROUP BY seat,operation ORDER BY seat,operation").all(),
      operations: db.prepare("SELECT seat,operation,op_key,args_digest,result_json FROM fixture_operations ORDER BY seat,operation,op_key").all(),
    };
  } finally { db.close(); }
};
const stopBrokerPreservingEvidence = async (owner: NativeActorOwner, broker: PathBHarness, child: ChildProcess, label = "n8"): Promise<void> => {
  // The existing harness deletes its owned broker root on stop. FIRST stop its
  // exact captured server child and retain its entire synthetic state privately.
  // No live broker deletion or process-by-name. The external launcher archives
  // owner.root's manifest (including these generated TEST credentials) privately.
  const lifetime = watchResidentChild(child);
  if (child.exitCode === null && child.signalCode === null) {
    await lifetime.stop();
    await teardownWithin("owned broker terminal", () => lifetime.exit, 10_000);
  }
  save(path.join(owner.root, `${label}-broker-terminal.json`), { pid: child.pid, code: child.exitCode, signal: child.signalCode, serverVersion: broker.serverVersion });
  fs.cpSync(broker.root, path.join(owner.root, `${label}-broker-retained`), { recursive: true, dereference: false, errorOnExist: true, force: false });
  await broker.stop();
};

// Linux kernel terminal evidence for an EXACT launch-recorded identity. A signal
// acknowledgement, owner.json alone or a zombie's kill(0) is not a live process.
const terminalIdentity = (identity: Owned): string | undefined => {
  let stat: string;
  try { stat = fs.readFileSync(`/proc/${identity.pid}/stat`, "utf8"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent"; throw error; }
  const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
  if (!/^\d+$/.test(fields[19] ?? "")) throw new Error("N8 BLOCKED: unknown owned process birth at terminal boundary");
  if (fields[19] !== identity.started) return "reused";
  return fields[0] === "Z" || fields[0] === "X" ? `kernel-${fields[0]}` : undefined;
};

// This is one finite setup and bounded positive acceptance leg. No unavailable
// prerequisite is caught and converted into a green/skipped native acceptance.
describe.skipIf(process.platform !== "linux")("N8 bounded TEST topology: independent hub + actual f339 SDK callers -> native resident", () => {
  it.each<[string, string[]]>([
    ["default duplicate", ["machine-a"]], ["extra duplicate", [HUB, HUB]],
    ["operator", ["fabric-ops"]], ["operator suffix", ["fabric-ops-test"]],
    ["wildcard", ["machine-*"]], ["line terminator", ["machine-hub\n"]],
    ["inventory overflow", Array.from({ length: 63 }, (_, index) => `extra-${index}`)],
  ])("refuses %s inventory before binary/root/port/key effects", async (_label, additionalRuntimeMachines) => {
    // Invalid binary is a sentinel: the production inventory validator MUST refuse first.
    await expect(createPathBHarness({ binary: "not-absolute", additionalRuntimeMachines }))
      .rejects.toThrow("invalid pathBMachines");
  });
  it("refuses ambiguous factory/inventory composition before calling the identity factory", async () => {
    const identityFactory = vi.fn(() => { throw new Error("identityFactory must not run"); });
    await expect(createPathBHarness({ additionalRuntimeMachines: [HUB], identityFactory }))
      .rejects.toThrow("cannot be combined with identityFactory");
    expect(identityFactory).not.toHaveBeenCalled();
  });
  it("preserves default policy/two TEST seats/unassigned +0; loses a post-COMMIT public response, reconstructs resident+DB+bridge, dedupes and survives >600000ms default-valued idle", async () => {
    const binary = resolveNatsBinary();
    const sdkDirectory = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST;
    const nodeBinaryInput = process.env.FABRIC_NATIVE_TEST_NODE;
    if (!binary || !sdkDirectory || !nodeBinaryInput) throw new Error("N8 BLOCKED: NATS_BIN + explicit approved FABRIC_NATIVE_TEST_PI_SDK_DIST + FABRIC_NATIVE_TEST_NODE required");
    const nodeBinary = fs.realpathSync(nodeBinaryInput);
    const owner = await createNativeActorOwner({ candidateRoot: repo, sdkDirectory, nodeBinary, actorId: ACTOR, module: residentModule,
      // Helper omission selects 1000ms. Explicit DEFAULT-VALUED option, not an omitted-key test.
      residentIdleExitMs: DEFAULT_FABRIC_CONFIG.mesh.residentIdleExitMs });
    let broker: PathBHarness | undefined, brokerChild: ChildProcess | undefined, link: NatsLinkHandle | undefined, relay: NatsWireFaults | undefined;
    const pods: OldPod[] = [], cleanupErrors: unknown[] = [];
    const linkAbort = new AbortController();
    try {
      expect(owner.idleExitMs).toBe(600_000);
      save(path.join(owner.root, "n8-prepared-scope.json"), { slice: "synthetic two-seat public reply + post-COMMIT response loss + resident/DB/bridge reconstruction + real default-valued >600s idle",
        topology: { hub: HUB, callers: ["machine-a", "machine-b"], unassigned: UNAUTHORIZED, inventory: INVENTORY },
        notEstablished: ["N9/N10", "production application authorization", "N6 exactMain alarm/containment", "both-down fixed #344 CLIENT <=60s", "actual app", "installed ca4", "canary", "production"],
        featureSourceHead: "71cbbe689d573f155d773edfabedca51a176f2ca",
        sourceCustody: "parent must bind final N8 bytes and one fresh compiled generation; this file is not a validation receipt",
        producerSha256: sha256(producerModule), residentAdapterSha256: sha256(residentModule),
        currentLinkSourceSha256: sha256(path.join(repo, "src/mesh/bridge-nats-link.ts")) });
      // Real default-harness counterexample: opting in below must not mutate the
      // existing two-machine constants, fields, policy/env or signed broker scope.
      const defaults = await createPathBHarness({ binary, tls: {} });
      try {
        expect(defaults.serverVersion).toBe("2.14.7");
        expect(PATH_B_MACHINES).toEqual(["machine-a", "machine-b"]);
        expect(defaults.permissionPolicy).toBe(PATH_B_PERMISSION_POLICY);
        expect(defaults.permissionPolicy).toEqual({ custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: ["machine-a", "machine-b"] });
        expect(defaults.policyEnv).toBe(PATH_B_POLICY_ENV);
        expect(defaults.policyEnv).toEqual({ PI_FABRIC_NATS_CUSTODY_STREAM: "FABRIC_CUSTODY", PI_FABRIC_NATS_LIVENESS_STREAM: "FABRIC_LIVENESS", PI_FABRIC_NATS_MACHINES: "machine-a,machine-b" });
        expect(defaults.additionalRuntimeUsers).toEqual([]);
        expect(Object.keys(defaults.users).sort()).toEqual(["bootstrap", "machineA", "machineB", "maintainer", "principal", "system"]);
        expect(defaults.users.machineA.machine).toBe("machine-a"); expect(defaults.users.machineB.machine).toBe("machine-b");
        expect(defaults.users.maintainer.machine).toBe("fabric-ops"); expect(defaults.users.bootstrap.machine).toBe("fabric-ops");
        for (const user of [defaults.users.machineA, defaults.users.machineB, defaults.users.maintainer]) {
          const options = user === defaults.users.maintainer ? defaults.operatorOptions(user) : defaults.runtimeOptions(user);
          expect(options).toMatchObject(PATH_B_PERMISSION_POLICY);
          expect(defaults.adminEnv(user)).toMatchObject(PATH_B_POLICY_ENV);
          const verified = await openNatsConnection(options);
          try {
            expect(verified.isVerified()).toBe(true);
            expect(verified.userInfo).toMatchObject({ data: { user: user.publicKey } });
            expect(JSON.stringify(verified.userInfo)).not.toContain("FABRIC_IN_machine-hub");
            expect(JSON.stringify(verified.userInfo)).not.toContain("FABRIC_IN_machine-unauthorized");
          } finally { await teardownWithin("default verified connection close", () => verified.close(), 5_000); }
        }
      } finally {
        if (!defaults.serverProcess) throw new Error("N8 BLOCKED: default broker process receipt missing");
        await stopBrokerPreservingEvidence(owner, defaults, defaults.serverProcess, "n8-default");
      }
      broker = await createPathBHarness({ binary, tls: {}, additionalRuntimeMachines: [HUB, UNAUTHORIZED] });
      brokerChild = broker.serverProcess;
      if (!brokerChild) throw new Error("N8 BLOCKED: no owned actual broker process receipt");
      expect(broker.serverVersion).toBe("2.14.7");
      expect(broker.permissionPolicy.pathBMachines).toEqual(INVENTORY);
      expect(broker.policyEnv.PI_FABRIC_NATS_MACHINES).toBe(INVENTORY.join(","));
      expect(PATH_B_PERMISSION_POLICY.pathBMachines).toEqual(["machine-a", "machine-b"]);
      expect(PATH_B_POLICY_ENV.PI_FABRIC_NATS_MACHINES).toBe("machine-a,machine-b");
      await provision(broker);
      const hubUser = broker.additionalRuntimeUsers.find(user => user.machine === HUB);
      const unauthorizedUser = broker.additionalRuntimeUsers.find(user => user.machine === UNAUTHORIZED);
      if (!hubUser || !unauthorizedUser) throw new Error("N8 BLOCKED: independent TEST hub/unassigned credentials missing");
      const runtimeUsers = [broker.users.machineA, broker.users.machineB, hubUser, unauthorizedUser];
      expect(new Set(runtimeUsers.map(user => user.machine)).size).toBe(4);
      expect(new Set(runtimeUsers.map(user => user.publicKey)).size).toBe(4);
      expect(new Set(runtimeUsers.map(user => user.credsPath)).size).toBe(4);
      for (const user of runtimeUsers) {
        const stat = fs.lstatSync(user.credsPath);
        expect(stat.isFile() && !stat.isSymbolicLink() && stat.uid === process.getuid?.() && (stat.mode & 0o777) === 0o600).toBe(true);
        expect(path.dirname(user.credsPath)).toBe(broker.root);
        expect(broker.runtimeOptions(user).pathBMachines).toEqual(INVENTORY);
        expect(broker.adminEnv(user).PI_FABRIC_NATS_MACHINES).toBe(INVENTORY.join(","));
        // Genuine production JWT/USER.INFO verification, never trust a typed machine label alone.
        const verified = await openNatsConnection(broker.runtimeOptions(user));
        try {
          expect(verified.isVerified()).toBe(true);
          expect(verified.userInfo).toMatchObject({ data: { user: user.publicKey, permissions: { publish: { deny: expect.arrayContaining([
            "$JS.API.STREAM.MSG.GET.FABRIC_IN_machine-a", "$JS.API.STREAM.MSG.GET.FABRIC_IN_machine-b",
            "$JS.API.STREAM.MSG.GET.FABRIC_IN_machine-hub", "$JS.API.STREAM.MSG.GET.FABRIC_IN_machine-unauthorized",
          ]) } } } });
        } finally { await teardownWithin("extended verified connection close", () => verified.close(), 5_000); }
      }
      const a = startOldPod(owner, broker, broker.users.machineA, "a", sdkDirectory, nodeBinary); pods.push(a);
      const b = startOldPod(owner, broker, broker.users.machineB, "b", sdkDirectory, nodeBinary); pods.push(b);
      const unauthorized = startOldPod(owner, broker, unauthorizedUser, "unauthorized", sdkDirectory, nodeBinary); pods.push(unauthorized);
      const [aReady, bReady, unauthorizedReady] = await Promise.all(pods.map(pod => pod.ready));
      const readies = [aReady!, bReady!, unauthorizedReady!];
      expect(new Set(pods.map(pod => pod.child.pid)).size).toBe(3);
      expect(new Set(readies.map(ready => ready.pin.rootId)).size).toBe(3);
      expect(new Set(readies.map(ready => ready.pin.sessionId)).size).toBe(3);
      expect(new Set(readies.map(ready => ready.pin.sessionFile)).size).toBe(3);
      for (const [index, ready] of readies.entries()) {
        expect(ready.nats).toEqual({ ...broker.permissionPolicy, machine: ["machine-a", "machine-b", UNAUTHORIZED][index] });
        expect(ready.provenance.commit).toBe(FROZEN_F3393715_COMMIT);
        expect(ready.provenance.sourceTree).toBe("98c0b004c04662de15b3d4e1440125d67892fcce");
        expect(ready.provenance.sha256).toEqual(FROZEN_F3393715_SHA256);
        expect(ready.provenance.archiveSha256).toMatch(/^[0-9a-f]{64}$/);
        expect(ready.timeoutMs).toBe(86_400_000); expect(ready.sdkUnconfigured).toBe(true); expect(ready.counts).toEqual(zeros);
      }
      const callers = [
        { link: owner.binding.ingress.link, sourceMachine: "machine-a", senderId: aReady!.pin.rootId },
        { link: owner.binding.ingress.link, sourceMachine: "machine-b", senderId: bReady!.pin.rootId },
        { link: owner.binding.ingress.link, sourceMachine: UNAUTHORIZED, senderId: unauthorizedReady!.pin.rootId },
      ];
      const seats = [{ ...callers[0]!, seat: "test-seat-a" }, { ...callers[1]!, seat: "test-seat-b" }];
      // The third valid agent is intentionally bridge-admitted but NOT in the
      // TEST handler's seat map: its real callback must return unauthorized.
      save(path.join(owner.root, "native-inbox-fixture.json"), { v: 1, seats }); // BEFORE resident module load/start.
      await owner.start();
      expect(owner.owner().hostId).toBe(residentHostId(owner.pin.rootId));
      expect(owner.owner().nativeActors).toEqual([owner.binding]);
      const orgJson = path.join(owner.root, "n8-org.json");
      // Fictional unbooted principal is ONLY the link parser's required org shape;
      // test seats above are application fixture configuration, never real org seats.
      save(orgJson, { productOwner: { name: "TestOwner" }, principals: [{ name: "TestOwner", orgInstance: {
        herdrAgent: "test-owner", herdrWorkspaceLabel: "N8 fixture", fabricSession: "34200000-0000-4000-8000-000000000099" } }] });
      relay = await NatsWireFaults.open(broker.runtimeOptions(hubUser).servers, broker.tls!);
      const config: NatsLinkStartConfig = { name: "native-n8-hub", link: owner.binding.ingress.link,
        file: { mesh: owner.meshRoot, lockProtocol: DEFAULT_FABRIC_CONFIG.mesh.lockProtocol, stateBackend: "file" },
        // EXACT helper-pinned ledgerDir = stateDir/ledger. No second receipt
        // producer or lock owner: the old SDK callers only own NATS runtimes.
        stateDir: owner.root, orgJson,
        nats: { servers: [relay.url], machine: hubUser.machine, credentials: hubUser.credsPath, caFile: broker.tls!.caFile,
          custodyStream: broker.permissionPolicy.custodyStream, livenessStream: broker.permissionPolicy.livenessStream,
          pathBMachines: [...broker.permissionPolicy.pathBMachines] },
        allow: { file: [{ kind: "root", id: owner.pin.rootId, send: [], receive: [] },
          { kind: "actor", id: ACTOR, rootId: owner.pin.rootId, name: owner.binding.name, send: [], receive: ["ask", "cancel"] }],
          nats: callers.map(caller => ({ kind: "root" as const, id: caller.senderId, machine: caller.sourceMachine, send: ["ask", "cancel"], receive: [] })) } };
      expect(path.join(config.stateDir, "ledger")).toBe(owner.ledgerDir);
      save(path.join(owner.root, "n8-link-config.json"), config);
      let linkGeneration = 0;
      const raw: { phase: "inbound" | "send"; envelope: TransportEnvelope; sourceMachine?: string; linkGeneration: number }[] = [];
      const recordRaw = (phase: "inbound" | "send", envelope: TransportEnvelope, sourceMachine?: string): void => {
        const frame = { phase, envelope: structuredClone(envelope), linkGeneration, ...(sourceMachine ? { sourceMachine } : {}) };
        raw.push(frame);
        fs.appendFileSync(path.join(owner.root, "n8-actual-port-frames.jsonl"), JSON.stringify(frame) + "\n", { mode: 0o600 });
      };
      const startLink = (): Promise<NatsLinkHandle> => {
        linkGeneration++;
        return startNatsLink(config, { signal: linkAbort.signal,
        log: line => fs.appendFileSync(path.join(owner.root, "n8-link.log"), `${line}\n`, { mode: 0o600 }),
        factories: { async openPort(portConfig, options): Promise<BridgeNatsPort> {
          const actual = await openNatsBridgeSide(portConfig, { ...options,
            onError: error => options.log(`NATS port: ${error instanceof Error ? error.message : String(error)}`) });
          // Supported, OWNED source/frame observation port. Every operation is
          // delegated unchanged to the REAL broker port. No prototype/global
          // patch, fake delivery, forged receipt, suppression or wire rewrite.
          return { machine: actual.machine, presence: () => actual.presence(),
            watchPresence: (changed, failed) => actual.watchPresence(changed, failed), resolve: id => actual.resolve(id),
            presenceView: () => actual.presenceView(), mirror: (set, at) => actual.mirror(set, at),
            fence: () => actual.fence(), withdraw: () => actual.withdraw(), close: () => actual.close(),
            async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal) {
              // Record the real source call BEFORE awaiting PubAck; the actual
              // caller's returned reply (not this observation) proves delivery.
              recordRaw("send", envelope); return actual.send(envelope, signal);
            },
            async *inbox(signal) {
              for await (const delivery of actual.inbox(signal)) {
                recordRaw("inbound", delivery.envelope, delivery.sourceMachine); yield delivery;
              }
            },
          };
        } },
        });
      };
      link = await startLink();
      const discovery = await Promise.all(pods.map(pod => pod.call<Record<string, unknown>>("discover", { actorId: ACTOR })));
      for (const row of discovery) {
        expect(row).toMatchObject({ id: ACTOR, kind: "actor", rootId: owner.pin.rootId, ownerHostId: residentHostId(owner.pin.rootId), machine: HUB });
        expect(row.capabilities).toContain("ask");
        // Actual old parser discovery proves optional runner compatibility. It
        // must NOT advertise native runner/model bindings on the old wire.
        expect(row.runner).toBeUndefined(); expect(row.capabilities).not.toContain("actor-bindings");
      }
      save(path.join(owner.root, "n8-discovery.json"), discovery);
      const callbacks = (): Callback[] => owner.callbacks() as unknown as Callback[];
      const publicAsk = async (pod: OldPod, callerIndex: number, operation: Request["operation"], opKey: string,
        args: Record<string, unknown>, message = RPC): Promise<{ reply: Reply; data: Request; callback: Callback }> => {
        const data: Request = { v: 1, requestId: randomUUID(), operation, opKey, args };
        const before = callbacks().length;
        const reply = await pod.call<Reply>("ask", { actorId: ACTOR, message, data });
        expect(reply).toMatchObject({ actorId: ACTOR, direction: "out", data: { v: 1, requestId: data.requestId } });
        expect(reply.id).toEqual(expect.any(String));
        expect(callbacks()).toHaveLength(before + 1);
        const callback = callbacks()[before]!;
        expect(callback.pid).toBe(owner.owner().pid); expect(callback.pid).not.toBe(owner.mainPid());
        expect(callback.request).toEqual({ message, data }); // exact whitespace/data snapshot
        expect(callback.data).toEqual(reply.data); // ACTUAL public reply.data, not a reconstructed envelope
        const seat = callers[callerIndex]!;
        expect(callback.context).toMatchObject({ actorId: ACTOR, source: "nats-bridge", sourceMachine: seat.sourceMachine,
          link: owner.binding.ingress.link, sender: { id: seat.senderId, kind: "main", sessionId: seat.senderId.slice(8) }, signalAborted: false });
        expect(callback.context.deadlineAt).toBeGreaterThan(callback.enteredAt);
        expect(callback.context.commandId).toBe(bridgeNatsMappedCommandId(owner.binding.ingress.link, "toFile", seat.senderId, callback.context.sourceCommandId));
        expect(callback.context.sourceCommandId).not.toBe(data.requestId);
        expect(callback.context.sourceCommandId).not.toBe(data.opKey);
        const receipt = readNativeIngressReceipt(owner.ledgerDir, callback.context.commandId);
        expect(receipt).toMatchObject({ commandId: callback.context.commandId, sourceCommandId: callback.context.sourceCommandId,
          sourceEventId: callback.context.sourceEventId, sender: callback.context.sender, sourceMachine: seat.sourceMachine,
          link: owner.binding.ingress.link, targetId: ACTOR, ownerHostId: residentHostId(owner.pin.rootId),
          ownerIdentityId: residentHostId(owner.pin.rootId), deadlineAt: callback.context.deadlineAt });
        const event = owner.mesh.read({ topic: "fabric.control.command", limit: 10_000 }).find(event =>
          (event.data as Record<string, unknown> | undefined)?.commandId === callback.context.commandId);
        expect(event).toBeDefined(); expect(event?.from.id).toBe(seat.senderId);
        const body = event!.data as Record<string, unknown>;
        expect(body.message).toBe(message); expect(body.data).toEqual(data); expect(body.deadlineAt).toBe(callback.context.deadlineAt);
        expect(body).not.toHaveProperty("binding"); expect(body).not.toHaveProperty("overrides");
        expect(body).not.toHaveProperty("nativeAdmission"); expect(body).not.toHaveProperty("nativeContext");
        const incoming = raw.filter(frame => frame.phase === "inbound" && frame.envelope.kind === "command" &&
          (frame.envelope.body as Record<string, unknown>).commandId === callback.context.sourceCommandId);
        expect(incoming.length).toBeGreaterThan(0);
        for (const frame of incoming) {
          expect(frame.sourceMachine).toBe(seat.sourceMachine);
          expect(frame.envelope).toMatchObject({ from: { id: seat.senderId }, targetId: ACTOR, operation: "ask",
            to: { machine: HUB, hostId: residentHostId(owner.pin.rootId) } });
          const wireBody = frame.envelope.body as Record<string, unknown>;
          expect(wireBody.message).toBe(message); expect(wireBody.data).toEqual(data);
          expect(wireBody).not.toHaveProperty("binding"); expect(wireBody).not.toHaveProperty("overrides");
        }
        const response = raw.find(frame => frame.phase === "send" && frame.envelope.kind === "ack" &&
          (frame.envelope.body as Record<string, unknown>).commandId === callback.context.sourceCommandId);
        expect(response?.envelope).toMatchObject({ to: { machine: seat.sourceMachine, hostId: seat.senderId },
          body: { accepted: true, result: reply } });
        return { reply, data, callback };
      };
      const refusal = async (pod: OldPod, callerIndex: number, operation: Request["operation"], opKey: string,
        args: Record<string, unknown>, status: string, message = RPC) => {
        const before = database(owner);
        const result = await publicAsk(pod, callerIndex, operation, opKey, args, message);
        expect(result.reply.data).toEqual({ v: 1, requestId: result.data.requestId, ok: false, status, error: { code: status } });
        // Real callback/refusal is expected; +0 covers ALL durable tables, including the op cache.
        expect(database(owner)).toEqual(before);
        return result;
      };
      const key = "n8-stable-business-key";
      const text = "  TEST seat B: opaque whitespace\nkept exactly  ";
      // Suppress only the real post-COMMIT application response on the hub's TLS
      // egress. The producer still uses its unchanged 40s observation signal and
      // frozen 24h provider default. 1000 is the existing relay's hard bound;
      // exhausting it and delivering during this first call MUST fail this test.
      const lostData: Request = { v: 1, requestId: randomUUID(), operation: "add", opKey: key, args: { text } };
      const lossStartedNs = process.hrtime.bigint();
      relay.loseResponseEnvelopes(frame => {
        // Existing transport encoding wraps the actual envelope; do not mistake
        // the wire container for an envelope or replace the frozen wire binding.
        const wire = JSON.parse(frame.body.toString("utf8")) as { format: number; sourceMachine: string;
          envelope: TransportSent<{ result?: Reply }>["envelope"] };
        return wire.format === 1 && wire.sourceMachine === HUB && wire.envelope.kind === "ack" &&
          wire.envelope.body.result?.data?.requestId === lostData.requestId;
      }, 1_000);
      const lostOutcome = await b.call<Reply>("ask", { actorId: ACTOR, message: RPC, data: lostData }).then(
        reply => ({ received: true as const, reply }), error => ({ received: false as const, error: String(error) }));
      const firstSettledNs = process.hrtime.bigint();
      expect(lostOutcome.received).toBe(false); // Redelivery received by the FIRST call is NOT response loss.
      expect(callbacks()).toHaveLength(1);
      const committed = callbacks()[0]!;
      expect(committed.pid).toBe(owner.owner().pid);
      expect(committed.request).toEqual({ message: RPC, data: lostData });
      expect(committed.data).toMatchObject({ v: 1, requestId: lostData.requestId, ok: true, status: "added",
        result: { item: { text, resolved: false } } });
      expect(committed.context).toMatchObject({ actorId: ACTOR, source: "nats-bridge", sourceMachine: "machine-b",
        link: owner.binding.ingress.link, sender: { id: bReady!.pin.rootId, kind: "main", sessionId: bReady!.pin.sessionId }, signalAborted: false });
      expect(committed.context.deadlineAt).toBeGreaterThan(committed.settledAt);
      expect(committed.context.commandId).toBe(bridgeNatsMappedCommandId(owner.binding.ingress.link, "toFile", bReady!.pin.rootId, committed.context.sourceCommandId));
      expect(committed.context.sourceCommandId).not.toBe(lostData.requestId);
      expect(committed.context.sourceCommandId).not.toBe(lostData.opKey);
      expect(readNativeIngressReceipt(owner.ledgerDir, committed.context.commandId)).toMatchObject({
        commandId: committed.context.commandId, sourceCommandId: committed.context.sourceCommandId,
        sourceEventId: committed.context.sourceEventId, sender: committed.context.sender, sourceMachine: "machine-b",
        link: owner.binding.ingress.link, targetId: ACTOR, ownerHostId: residentHostId(owner.pin.rootId) });
      const lostResponse = raw.find(frame => frame.phase === "send" && frame.envelope.kind === "ack" &&
        (frame.envelope.body as { commandId?: string }).commandId === committed.context.sourceCommandId);
      expect(lostResponse?.envelope).toMatchObject({ to: { machine: "machine-b", hostId: bReady!.pin.rootId },
        body: { accepted: true, result: { actorId: ACTOR, direction: "out", data: committed.data } } });
      expect(relay.responseEnvelopeDrops).toBeGreaterThan(0);
      expect(relay.responseEnvelopeDrops).toBeLessThan(1_000);
      expect(relay.ackDrops).toBe(0); expect(relay.pubAckDrops).toBe(0);
      for (const frame of relay.droppedResponseEnvelopes) {
        const wire = JSON.parse(frame.body) as { format: number; sourceMachine: string;
          envelope: TransportSent<{ commandId: string; result: Reply }>["envelope"] };
        expect(wire.format).toBe(1); expect(wire.sourceMachine).toBe(HUB);
        const envelope = wire.envelope;
        expect(envelope.kind).toBe("ack");
        expect(envelope.body.commandId).toBe(committed.context.sourceCommandId);
        expect(envelope.body.result.data).toEqual(committed.data);
      }
      const committedDb = database(owner);
      expect(committedDb.items).toEqual([{ ...committed.data.result!.item!, seat: "test-seat-b", resolved: 0 }]);
      expect(committedDb.effects).toEqual([{ seat: "test-seat-b", operation: "add", n: 1 }]);
      expect(committedDb.operations).toEqual([{ seat: "test-seat-b", operation: "add", op_key: key,
        args_digest: createHash("sha256").update(JSON.stringify({ text })).digest("hex"),
        result_json: JSON.stringify(committed.data.result) }]);
      expect(committed.readback).toEqual({ items: 1, operations: 1, addEffects: 1, resolveEffects: 0 });
      const producerTrace = fs.readFileSync(path.join(b.root, "producer.jsonl"), "utf8").split("\n").filter(Boolean)
        .map(line => JSON.parse(line) as { event: string; action?: string });
      expect(producerTrace.filter(row => row.event === "result" && row.action === "ask")).toHaveLength(0);
      expect(producerTrace.filter(row => row.event === "refusal" && row.action === "ask")).toHaveLength(1);
      // No rule removal before the native public invocation has settled. Stop
      // the real old bridge with suppression STILL armed, joining its publisher
      // before clearing rules; no late old response may hide this loss.
      await link.stop(); await link.closed; link = undefined;
      relay.clearFaults(); // Existing API name in these bytes; counters/evidence remain.
      save(path.join(owner.root, "n8-post-commit-loss.json"), { lossStartedNs: String(lossStartedNs),
        firstSettledNs: String(firstSettledNs), lostOutcome, request: lostData, callback: committed, database: committedDb,
        outboundResponse: lostResponse, applicationResponseDrops: relay.responseEnvelopeDrops,
        droppedApplicationResponses: relay.droppedResponseEnvelopes, brokerAckDrops: relay.ackDrops,
        brokerAcks: relay.acks, pubAckDrops: relay.pubAckDrops });

      // Reconstruct the ACTUAL resident AND its DB handle, not merely Main.
      // Custody comes exclusively from the inherited launch preload, then binds
      // owner/launcher metadata; never manufacture an Owned record from JSON.
      const previousResident = owner.owner();
      const previousMain = owner.mainIdentity();
      const recorded = (pid: number, started: string | undefined): Owned => {
        const matches = owner.ownedProcesses().filter(value => value.pid === pid && value.started === started);
        expect(matches).toHaveLength(1);
        const entry = matches[0]!;
        expect(same(entry)).toBe(true); expect(terminalIdentity(entry)).toBeUndefined();
        return entry;
      };
      const residentProcess = recorded(previousResident.pid, previousResident.processStartTime);
      const launcherIdentity = previousResident.handover?.launcher;
      if (!launcherIdentity) throw new Error("N8 BLOCKED: actual resident launcher custody missing");
      const launcherProcess = recorded(launcherIdentity.pid, launcherIdentity.processStartTime);
      const oldModules = owner.observations().filter(event => event.event === "module-loaded");
      const oldDbOpens = owner.observations().filter(event => event.event === "database-opened");
      expect(oldModules).toHaveLength(1); expect(oldDbOpens).toHaveLength(1);
      expect(oldModules[0]).toMatchObject({ pid: previousResident.pid, residentIdleExitMs: 600_000,
        rootId: owner.pin.rootId, sessionId: owner.pin.sessionId });
      expect(oldDbOpens[0]).toMatchObject({ pid: previousResident.pid, databaseOpenId: committed.databaseOpenId });
      const sessionPinBytes = fs.readFileSync(owner.sessionPin), sessionBytes = fs.readFileSync(owner.sessionFile);
      const sessionPath = fs.realpathSync(owner.sessionFile), rootPath = fs.realpathSync(owner.root);
      const inode = (file: string) => { const stat = fs.lstatSync(file); expect(stat.isFile() && !stat.isSymbolicLink()).toBe(true);
        return { dev: stat.dev, ino: stat.ino }; };
      const dbPath = path.join(owner.root, "native-inbox.sqlite");
      const sessionInode = inode(owner.sessionFile), dbInode = inode(dbPath);
      const residentFencePath = path.join(owner.residencyRoot, "host.lock");
      const residentFenceInode = inode(residentFencePath), mainFenceInode = inode(`${owner.sessionFile}.main.lock`);
      const linkConfigHash = sha256(path.join(owner.root, "n8-link-config.json"));
      await owner.stopMain(); // Native EOF/CLOSE + positive Main death; no two SDK writers.
      expect(owner.mainPid()).toBeUndefined();
      expect(same(residentProcess)).toBe(true);
      await stopOwned(residentProcess, 2_000, 5_000); // Only exact launch-recorded birth; never name/group/stale unlink.
      // Generation files drive normal observation. The bounded safety poll is
      // solely for kernel-only death/reaping of these two already-owned PIDs.
      await owner.until(() => !!terminalIdentity(residentProcess) && !!terminalIdentity(launcherProcess) &&
        readResidentGenerationExit(owner.residencyRoot, previousResident.token)?.pid === previousResident.pid,
      "old resident + native launcher terminal + matching generation exit BEFORE successor", 30_000, { pollMs: 25 });
      const oldGenerationExit = readResidentGenerationExit(owner.residencyRoot, previousResident.token)!;
      expect(oldGenerationExit.generation).toBe(previousResident.token);
      expect(database(owner)).toEqual(committedDb);
      const terminalNs = process.hrtime.bigint();
      const residentDeath = terminalIdentity(residentProcess), launcherDeath = terminalIdentity(launcherProcess);
      expect(residentDeath).toBeTruthy(); expect(launcherDeath).toBeTruthy();
      await owner.start();
      const successor = owner.owner();
      expect(successor.pid).not.toBe(previousResident.pid);
      expect(successor.processStartTime).not.toBe(previousResident.processStartTime);
      expect(successor.token).not.toBe(previousResident.token);
      expect(successor.hostId).toBe(previousResident.hostId); expect(successor.nativeActors).toEqual([owner.binding]);
      expect(owner.mainPid()).not.toBe(previousMain?.pid);
      expect(fs.realpathSync(owner.root)).toBe(rootPath); expect(fs.realpathSync(owner.sessionFile)).toBe(sessionPath);
      expect(inode(owner.sessionFile)).toEqual(sessionInode); expect(inode(dbPath)).toEqual(dbInode);
      expect(inode(residentFencePath)).toEqual(residentFenceInode); expect(inode(`${owner.sessionFile}.main.lock`)).toEqual(mainFenceInode);
      expect(fs.readFileSync(owner.sessionPin)).toEqual(sessionPinBytes);
      expect(fs.readFileSync(owner.sessionFile).subarray(0, sessionBytes.length)).toEqual(sessionBytes);
      const newModules = owner.observations().filter(event => event.event === "module-loaded");
      const newDbOpens = owner.observations().filter(event => event.event === "database-opened");
      expect(newModules).toHaveLength(2); expect(newDbOpens).toHaveLength(2);
      expect(newModules[1]).toMatchObject({ pid: successor.pid, resident: true, residentIdleExitMs: 600_000,
        rootId: owner.pin.rootId, sessionId: owner.pin.sessionId });
      expect(newModules[1]!.moduleLoadId).not.toBe(oldModules[0]!.moduleLoadId);
      expect(newDbOpens[1]).toMatchObject({ pid: successor.pid, databasePath: dbPath, ...dbInode, readback: committed.readback });
      expect(newDbOpens[1]!.databaseOpenId).not.toBe(committed.databaseOpenId);
      expect(database(owner)).toEqual(committedDb);
      expect(sha256(path.join(owner.root, "n8-link-config.json"))).toBe(linkConfigHash);
      link = await startLink(); // SAME durable stateDir/ledger/config, entirely new real port/link.
      expect(linkGeneration).toBe(2);
      save(path.join(owner.root, "n8-reconstruction.json"), { previousMain, previousResident, residentProcess, launcherProcess,
        residentDeath, launcherDeath, oldGenerationExit, terminalNs: String(terminalNs), successor,
        successorObservedNs: String(process.hrtime.bigint()), sessionPath, sessionInode, dbPath, dbInode,
        residentFenceInode, mainFenceInode, oldModules, newModules, oldDbOpens, newDbOpens, linkConfigHash, linkGeneration });
      const first = await publicAsk(b, 1, "add", key, { text });
      expect(first.data.requestId).not.toBe(lostData.requestId);
      expect(first.callback.context.sourceCommandId).not.toBe(committed.context.sourceCommandId);
      expect(first.callback.context.commandId).not.toBe(committed.context.commandId);
      expect(first.callback.pid).toBe(successor.pid);
      expect(first.callback.moduleLoadId).toBe(newModules[1]!.moduleLoadId);
      expect(first.callback.databaseOpenId).toBe(newDbOpens[1]!.databaseOpenId);
      expect(first.reply.data).toEqual({ ...committed.data, requestId: first.data.requestId });
      expect(database(owner)).toEqual(committedDb); // ONE effect/cache identity across loss AND all reconstruction.
      expect(first.reply.data).toMatchObject({ ok: true, status: "added", result: { item: { text, resolved: false } } });
      const item = first.reply.data.result!.item!;
      const beforeRetry = database(owner);
      const retry = await publicAsk(b, 1, "add", key, { text });
      expect(retry.data.requestId).not.toBe(first.data.requestId);
      expect(retry.callback.context.sourceCommandId).not.toBe(first.callback.context.sourceCommandId);
      expect(retry.callback.context.commandId).not.toBe(first.callback.context.commandId);
      expect(retry.reply.data).toEqual({ ...first.reply.data, requestId: retry.data.requestId });
      expect(retry.callback.readback).toMatchObject({ items: 1, addEffects: 1 });
      expect(database(owner)).toEqual(beforeRetry);
      expect((await publicAsk(a, 0, "list", "a-empty", {})).reply.data.result).toEqual({ items: [] });
      await refusal(a, 0, "resolve", "resolve", { itemId: item.id }, "not-found");
      const textA = "  TEST seat A: private fixture data  ";
      // SAME opKey, different seat/args: must be an independent durable add, not
      // B's cached data or a global opKey conflict. Both contexts are genuine.
      const firstA = await publicAsk(a, 0, "add", key, { text: textA });
      expect(firstA.reply.data).toMatchObject({ ok: true, status: "added", result: { item: { text: textA, resolved: false } } });
      const itemA = firstA.reply.data.result!.item!;
      expect(itemA.id).not.toBe(item.id);
      const beforeRetryA = database(owner);
      const retryA = await publicAsk(a, 0, "add", key, { text: textA });
      expect(retryA.data.requestId).not.toBe(firstA.data.requestId);
      expect(retryA.callback.context.sourceCommandId).not.toBe(firstA.callback.context.sourceCommandId);
      expect(retryA.callback.context.commandId).not.toBe(firstA.callback.context.commandId);
      expect(retryA.reply.data).toEqual({ ...firstA.reply.data, requestId: retryA.data.requestId });
      expect(database(owner)).toEqual(beforeRetryA);
      expect((await publicAsk(a, 0, "list", "list-before", {})).reply.data.result).toEqual({ items: [itemA] });
      expect((await publicAsk(b, 1, "list", "list-before", {})).reply.data.result).toEqual({ items: [item] });
      for (const [pod, index, own, foreign] of [[a, 0, itemA, item], [b, 1, item, itemA]] as const) {
        const resolved = await publicAsk(pod, index, "resolve", "resolve", { itemId: own.id });
        expect(resolved.reply.data).toMatchObject({ ok: true, status: "resolved", result: { item: { ...own, resolved: true } } });
        const before = database(owner);
        const resolvedAgain = await publicAsk(pod, index, "resolve", "resolve", { itemId: own.id });
        expect(resolvedAgain.data.requestId).not.toBe(resolved.data.requestId);
        expect(resolvedAgain.callback.context.commandId).not.toBe(resolved.callback.context.commandId);
        expect(resolvedAgain.reply.data).toEqual({ ...resolved.reply.data, requestId: resolvedAgain.data.requestId });
        expect(database(owner)).toEqual(before);
        // Membership before cache: same cached resolve opKey must NOT reveal a
        // foreign row/result or report an args conflict instead of not-found.
        await refusal(pod, index, "resolve", "resolve", { itemId: foreign.id }, "not-found");
        expect((await publicAsk(pod, index, "list", "list-after", {})).reply.data.result).toEqual({ items: [{ ...own, resolved: true }] });
      }
      await refusal(b, 1, "add", key, { text: "CHANGED MUST NOT COMMIT" }, "conflict");
      await refusal(a, 0, "add", key, { text: "CHANGED MUST NOT COMMIT" }, "conflict");
      // Authenticated runtime, distinct machine/key/session, bridge-admitted but
      // intentionally absent from TEST seats. No mutation/cast of native context.
      await refusal(unauthorized, 2, "add", key, { text }, "unauthorized");
      await refusal(unauthorized, 2, "list", "list-before", {}, "unauthorized");
      await refusal(unauthorized, 2, "resolve", "resolve", { itemId: item.id }, "unauthorized");
      // Opaque message is not trimmed into a route/handler key. The exact spaced
      // string reaches a legitimate native callback and the fixture refuses it.
      await refusal(b, 1, "list", "spaced-message", {}, "invalid-request", ` ${RPC} `);
      expect(database(owner).items).toEqual([
        { id: itemA.id, seat: "test-seat-a", text: textA, resolved: 1 }, { id: item.id, seat: "test-seat-b", text, resolved: 1 },
      ]);
      expect(database(owner).effects).toEqual([
        { seat: "test-seat-a", operation: "add", n: 1 }, { seat: "test-seat-a", operation: "resolve", n: 1 },
        { seat: "test-seat-b", operation: "add", n: 1 }, { seat: "test-seat-b", operation: "resolve", n: 1 },
      ]);
      for (const pod of pods) expect((await pod.call<Ready>("state")).counts).toEqual(zeros);
      const assertZeroNative = (): void => {
        const observations = owner.observations();
        expect(observations.some(event => String(event.event).startsWith("FORBIDDEN-"))).toBe(false);
        const starts = observations.filter(event => event.event === "session-start");
        expect(starts.length).toBeGreaterThanOrEqual(4); // two actual Mains + two actual resident SDK sessions
        expect(starts.every(event => event.unconfigured === true && event.selectedModel === false)).toBe(true);
        expect(starts.filter(event => event.role === "main").every(event => event.projectTrusted === true)).toBe(true);
        for (const type of ["agent_start", "turn_start", "tool_execution_start", "tool_execution_update", "tool_execution_end"]) {
          expect(owner.rpcEvents.filter(event => event.type === type)).toHaveLength(0);
        }
      };
      assertZeroNative();
      const registry = new ActorRegistryStore(path.join(owner.meshRoot, "actors", owner.pin.sessionId));
      const idleRow = () => registry.snapshot().actors.find(row => row.id === ACTOR);
      await owner.until(() => idleRow()?.status === "idle", "actual native actor idle before real default-valued gap");
      const alive = owner.owner(), idleDatabase = database(owner), idleCallbacks = callbacks().length;
      const idleModules = owner.observations().filter(event => event.event === "module-loaded");
      const idleDbOpens = owner.observations().filter(event => event.event === "database-opened");
      const idleStarts = owner.observations().filter(event => event.event === "session-start");
      const idleInputs = owner.observations().filter(event => event.event === "input-handled");
      const idleCommands = raw.filter(frame => frame.phase === "inbound" && frame.envelope.kind === "command").length;
      const idleSessionBytes = fs.readFileSync(owner.sessionFile);
      const leaseBefore = readHostLeaseCurrent(owner.meshRoot, alive.hostId);
      expect(leaseBefore?.expiresAt).toBeGreaterThan(Date.now());
      expect(alive.pid).toBe(successor.pid); expect(alive.token).toBe(successor.token);
      expect(idleRow()).toMatchObject({ id: ACTOR, rootId: owner.pin.rootId, runner: "native", status: "idle" });
      const idleStartedNs = process.hrtime.bigint();
      // ONE real timer, no business ask, get_state, discovery, wake or callback
      // during the interval. Native heartbeats/maintenance are left untouched.
      await new Promise<void>(resolve => setTimeout(resolve, 600_100));
      const idleEndedNs = process.hrtime.bigint();
      const elapsedMs = Number(idleEndedNs - idleStartedNs) / 1_000_000;
      expect(elapsedMs).toBeGreaterThan(600_000);
      const afterIdle = owner.owner(), leaseAfter = readHostLeaseCurrent(owner.meshRoot, alive.hostId);
      expect({ pid: afterIdle.pid, birth: afterIdle.processStartTime, token: afterIdle.token })
        .toEqual({ pid: alive.pid, birth: alive.processStartTime, token: alive.token });
      expect(terminalIdentity(recorded(alive.pid, alive.processStartTime))).toBeUndefined();
      expect(leaseAfter?.expiresAt).toBeGreaterThan(Date.now());
      expect(leaseAfter?.updatedAt).toBeGreaterThan(leaseBefore!.updatedAt);
      expect(idleRow()).toMatchObject({ id: ACTOR, rootId: owner.pin.rootId, runner: "native", status: "idle" });
      expect(database(owner)).toEqual(idleDatabase); expect(callbacks()).toHaveLength(idleCallbacks);
      expect(owner.observations().filter(event => event.event === "module-loaded")).toEqual(idleModules);
      expect(owner.observations().filter(event => event.event === "database-opened")).toEqual(idleDbOpens);
      expect(owner.observations().filter(event => event.event === "session-start")).toEqual(idleStarts);
      expect(owner.observations().filter(event => event.event === "input-handled")).toEqual(idleInputs);
      expect(raw.filter(frame => frame.phase === "inbound" && frame.envelope.kind === "command")).toHaveLength(idleCommands);
      expect(fs.readFileSync(owner.sessionFile)).toEqual(idleSessionBytes);
      assertZeroNative();
      for (const pod of pods) expect((await pod.call<Ready>("state")).counts).toEqual(zeros);
      // Actual frozen public response AFTER the real gap, still from this same
      // resident/DB module. Same operation dedupes without another effect/cache.
      const afterGapReply = await publicAsk(b, 1, "add", key, { text });
      expect(afterGapReply.reply.data).toEqual({ ...committed.data, requestId: afterGapReply.data.requestId });
      expect(afterGapReply.callback.pid).toBe(alive.pid);
      expect(afterGapReply.callback.moduleLoadId).toBe(idleModules[1]!.moduleLoadId);
      expect(afterGapReply.callback.databaseOpenId).toBe(idleDbOpens[1]!.databaseOpenId);
      expect(database(owner)).toEqual(idleDatabase);
      expect(owner.owner().token).toBe(alive.token);
      expect(relay.acks.length).toBeGreaterThan(0); // REAL broker consumer +ACKs, distinct from application envelopes.
      expect(relay.ackDrops).toBe(0); expect(relay.pubAckDrops).toBe(0);
      assertZeroNative();
      save(path.join(owner.root, "n8-real-default-idle.json"), { configuration: "explicit option equals unchanged product default, NOT omitted key",
        productDefaultMs: DEFAULT_FABRIC_CONFIG.mesh.residentIdleExitMs, configuredMs: owner.idleExitMs,
        idleStartedNs: String(idleStartedNs), idleEndedNs: String(idleEndedNs), elapsedMs,
        before: alive, after: afterIdle, leaseBefore, leaseAfter, idleCallbacks, idleModules, idleDbOpens,
        database: idleDatabase, afterGapReply, modelProviderToolCounters: zeros });
      expect(owner.observations().some(event => String(event.event).startsWith("FORBIDDEN-"))).toBe(false);
      for (const type of ["agent_start", "turn_start", "tool_execution_start", "tool_execution_update", "tool_execution_end"]) {
        expect(owner.rpcEvents.filter(event => event.type === type)).toHaveLength(0);
      }
      save(path.join(owner.root, "n8-slice-observations.json"), { scope: "synthetic N8 public reply/loss/dedupe/reconstruction/default-valued idle; NOT actual app/N9/N10/installed/canary/production",
        wire: { inboxFrames: relay.inboxFrames, brokerDeliveries: relay.deliveries, brokerAcks: relay.acks,
          responseEnvelopeDrops: relay.responseEnvelopeDrops, droppedResponseEnvelopes: relay.droppedResponseEnvelopes,
          brokerAckDrops: relay.ackDrops, pubAckDrops: relay.pubAckDrops },
        callbacks: callbacks(), database: database(owner), producerStates: await Promise.all(pods.map(pod => pod.call("state"))) });
    } finally {
      const cleanup = async (operation: () => Promise<unknown>): Promise<void> => { try { await operation(); } catch (error) { cleanupErrors.push(error); } };
      if (link) await cleanup(async () => { await link!.stop(); await link!.closed; });
      linkAbort.abort();
      for (const pod of pods.reverse()) await cleanup(() => pod.stop());
      if (relay) {
        // Failure paths retain the same raw fault evidence, not just the green receipt.
        await cleanup(async () => save(path.join(owner.root, "n8-relay-final.json"), { inboxFrames: relay!.inboxFrames, brokerDeliveries: relay!.deliveries,
          brokerAcks: relay!.acks, responseEnvelopeDrops: relay!.responseEnvelopeDrops,
          droppedResponseEnvelopes: relay!.droppedResponseEnvelopes, brokerAckDrops: relay!.ackDrops, pubAckDrops: relay!.pubAckDrops }));
        await cleanup(() => relay!.close());
      }
      if (broker && brokerChild) await cleanup(() => stopBrokerPreservingEvidence(owner, broker!, brokerChild!));
      else if (broker) await cleanup(() => broker!.stop());
      // LAST: joins Main/resident then hashes ALL producer/raw native traces and
      // retained broker state. No test removes owner.root; parent archives it.
      await cleanup(() => owner.close());
      if (cleanupErrors.length) throw new AggregateError(cleanupErrors, "N8 owned lifecycle/evidence custody did not complete; retain launcher root");
    }
  // Test-only watchdog budget: real >10min gap + 40s loss + native startup/reconstruction/cleanup.
  // NOT a product timeout/SLA increase, frozen-provider deadline change or fixed #344 CLIENT claim.
  }, 1_200_000);
});
