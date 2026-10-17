// expected-skip: PI_FABRIC_NATS_REHEARSAL is not exactly 1: explicit private rehearsal opt-in required.
// Opted-in FILE close cases remain known RED until smarty-dev#14612 is fixed; assertions stay active.
// expected-skip: no NATS_BIN/PATH executable: private authenticated TLS broker required.
// expected-skip: no NATS_SWITCH_PROBE_MODULE: full R1 waits for shared admission probe.
// Keep reason: rollback data safety across real registry/control/receiver and OS boundaries.
// Source integration, NOT installed-TUI or live-fleet proof. No shared broker/root is used.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { MeshStore } from "../src/mesh/store.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { transportDurableName, transportInboxFilter, transportInboxStream } from "../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { collectControlProbeEvidence, waitForControlProbeEvidence } from "./helpers/nats-path-b-probe.js";
import { createSwitchRoot, requireSwitchMirrors, runSwitchRollback, startSwitchBridge, startSwitchRuntime, switchError,
  type SwitchMirrorStatus, type SwitchProcess, type SwitchRuntimeOptions, type SwitchConsumerSnapshot, type SwitchProbeSnapshot,
} from "./helpers/nats-switch-runtime.js";

const rehearsalEnabled = process.env.PI_FABRIC_NATS_REHEARSAL === "1";
const binary = rehearsalEnabled ? resolveNatsBinary() : undefined;
if (rehearsalEnabled && !binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: switch rehearsal requires NATS_BIN");
const probeModule = process.env.NATS_SWITCH_PROBE_MODULE?.trim();
const hubSession = "00000000-0000-4000-8000-000000000001";
const workSession = "00000000-0000-4000-8000-000000000002";
const hubId = `session:${hubSession}`, workId = `session:${workSession}`;
type SwitchSeedInput = {
  root: string;
  hubMesh: string;
  workMesh: string;
  ownerOptions: SwitchRuntimeOptions;
  senderOptions: SwitchRuntimeOptions;
  carrier: "nats" | "file";
};
type SwitchPopulationInput = {
  seedResult: unknown;
  ownerRoutes: unknown;
  senderRoutes: unknown;
  ownerId: string;
  senderId: string;
  carrier: "nats" | "file";
};
type SwitchSeedHooks = {
  seedSwitchFixture(input: SwitchSeedInput): unknown | Promise<unknown>;
  verifySwitchPopulation?(input: SwitchPopulationInput): unknown | Promise<unknown>;
};

async function fixture(carrier: "nats" | "file" = "nats") {
  if (!binary) throw new Error("switch fixture requires a private NATS executable");
  const root = createSwitchRoot();
  let broker: PathBHarness | undefined;
  let bridge: SwitchProcess | undefined, owner: SwitchProcess | undefined, sender: SwitchProcess | undefined;
  const processes: SwitchProcess[] = [];
  let connection: Awaited<ReturnType<PathBHarness["connect"]>> | undefined;
  const artifactParent = process.env.NATS_SWITCH_ARTIFACT_DIR?.trim();
  const artifact = artifactParent ? fs.mkdtempSync(path.join(path.resolve(artifactParent), "fixture-")) : undefined;
  if (artifact) fs.chmodSync(artifact, 0o700);
  const save = (name: string, value: unknown) => {
    if (artifact) fs.writeFileSync(path.join(artifact, name), `${JSON.stringify(value)}\n`, { mode: 0o600 });
  };
  const preserve = () => {
    if (!artifact) return;
    // Never copy/rename out of the private test ceiling. The canonical stdout
    // receipt retains the archive bytes; a separate audit-only decoder owns extraction.
    const pids = processes.flatMap(process => {
      if (!process.terminal) throw new Error("cannot archive without terminal native process inventory");
      return [process.terminal.pid, ...process.terminal.preStopDescendants].filter((pid): pid is number => pid !== undefined);
    });
    if (pids.some(pid => fs.existsSync(`/proc/${pid}`))) throw new Error("cannot archive while an actual native PID remains");
    let fileCount = 0, bytes = 0;
    const inspect = (directory: string) => {
      for (const item of fs.readdirSync(directory, { withFileTypes: true })) {
        const file = path.join(directory, item.name), stat = fs.lstatSync(file);
        if (stat.isDirectory()) inspect(file);
        else if (stat.isFile() && stat.nlink === 1) { fileCount++; bytes += stat.size; }
        else throw new Error("archive refuses symlink/hardlink/special entry");
        if (fileCount > 20_000 || bytes > 128 * 1024 * 1024) throw new Error("archive source exceeds size bound");
      }
    };
    inspect(root);
    const readJson = (file: string): any => fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : null;
    const senderEvidence = path.join(root, "work", "control-probe-evidence.jsonl");
    const sends = fs.existsSync(senderEvidence) ? fs.readFileSync(senderEvidence, "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line)).filter(row => row.type === "send").map(row => row.send) : [];
    const roots = ([ ["hub", hubSession], ["work", workSession] ] as const).map(([side, session]) => {
      const mesh = path.join(root, side, "mesh"), options = readJson(path.join(root, side, "runtime.json"));
      const journalPath = path.join(mesh, "main-followups", `${session}.json`), journal = readJson(journalPath), delivered = readJson(`${journalPath}.delivered`);
      const seen = path.join(mesh, "control-seen", createHash("sha256").update(`session:${session}`).digest("hex").slice(0, 32));
      const state = readJson(path.join(seen, "state.json"));
      const rows = Object.values(state?.entries ?? {}).map((entry: any) => entry.value).filter((value: any) => typeof value?.commandId === "string") as any[];
      return { side, hostId: `session:${session}`, originalMeshRoot: options?.meshRoot, stateBackend: options?.config.mesh.stateBackend,
        transport: options?.config.mesh.transport, hostTransport: options?.config.mesh.hostTransport,
        seenRoot: path.relative(root, seen), seenRows: rows.map(value => ({ commandId: value.commandId, carrier: value.carrier ?? "file",
          writtenAt: value.writtenAt ?? null, accepted: value.acceptance?.accepted ?? null })),
        perCommand: sends.map(send => ({ commandId: send.commandId, journalCount: (journal?.items ?? []).filter((item: any) => item.deliveryId === send.commandId).length,
          deliveredCount: (delivered?.ids ?? []).filter((id: string) => id === send.commandId).length,
          seenCount: rows.filter(value => value.commandId === send.commandId).length })) };
    });
    const forensic = { type: "switch-archive-forensic", label: "pre-projection", schema: 1, id: path.basename(root),
      manifest: sends.map(send => ({ probeId: send.probeId, commandId: send.commandId, accepted: send.result?.accepted === true })),
      roots, bridgeCursor: readJson(path.join(root, "bridge", "cursor.json")),
      sqliteFiles: fs.readdirSync(path.join(root, "hub", "mesh")).filter(name => /^state\.db(?:-wal|-shm)?$/.test(name)).map(name => {
        const data = fs.readFileSync(path.join(root, "hub", "mesh", name)); return { name, bytes: data.length, sha256: createHash("sha256").update(data).digest("hex") };
      }) };
    console.info(JSON.stringify(forensic));
    const archive = path.join(fs.realpathSync(process.env.TMPDIR!), `${path.basename(root)}.tar.gz`);
    const result = spawnSync("tar", ["--format=ustar", "-czf", archive, "-C", path.dirname(root), path.basename(root)], { encoding: "utf8", timeout: 20_000 });
    if (result.error || result.status !== 0) throw result.error ?? new Error(`archive failed: ${result.stderr}`);
    fs.chmodSync(archive, 0o600);
    const data = fs.readFileSync(archive);
    if (data.length > 64 * 1024 * 1024) throw new Error("compressed archive exceeds bound");
    const sha256 = createHash("sha256").update(data).digest("hex"), id = path.basename(root);
    const encoded = data.toString("base64"), chunkSize = 48_000, chunks = Math.ceil(encoded.length / chunkSize);
    for (let index = 0; index < chunks; index++) console.info(JSON.stringify({ type: "switch-archive-chunk", label: "pre-projection",
      schema: 1, id, index, total: chunks, sha256, fileCount, bytes: data.length, data: encoded.slice(index * chunkSize, (index + 1) * chunkSize) }));
    const receipt = { type: "switch-archive-complete", label: "pre-projection", schema: 1, id, sha256, chunks,
      fileCount, bytes: data.length, sourceBytes: bytes, originalRoot: root, privateArchive: archive, at: Date.now(), stoppedPids: pids };
    console.info(JSON.stringify(receipt));
    save("preservation.json", receipt);
  };
  try {
    broker = await createPathBHarness({ binary, tls: {} });
    // Pin explicitly even when the caller omitted the harness version environment.
    expect(broker.serverVersion).toBe("2.14.7");
    expect(broker.configText).toContain("sync_interval: always");
    for (const [key, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(key, value);
    try {
      expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
    } finally { vi.unstubAllEnvs(); }
    const hubRoot = path.join(root, "hub"), workRoot = path.join(root, "work");
    // Every test owns fresh mesh descendants. A capture hook copies private
    // inputs into these paths; independent tests never reuse capture roots.
    const hubMesh = path.join(hubRoot, "mesh");
    const workMesh = path.join(workRoot, "mesh");
    if (path.resolve(hubMesh) === path.resolve(workMesh)) throw new Error("switch rehearsal requires distinct mesh roots");
    for (const mesh of [hubMesh, workMesh]) {
      const relative = path.relative(fs.realpathSync(process.env.TMPDIR!), path.resolve(mesh));
      if (!relative || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
        throw new Error("switch mesh must be below the current test TMPDIR");
      fs.mkdirSync(mesh, { recursive: true, mode: 0o700 });
    }
    const runtimeOptions = (side: "hub" | "work"): SwitchRuntimeOptions => {
      const isHub = side === "hub", sideRoot = isHub ? hubRoot : workRoot;
      return { root: sideRoot, meshRoot: isHub ? hubMesh : workMesh, agentDir: path.join(sideRoot, "agent"),
        sessionId: isHub ? hubSession : workSession, name: isHub ? "switch-owner" : "switch-sender",
        ...(probeModule ? { probeModule: path.resolve(probeModule) } : {}),
        config: normalizeFabricConfig({ fullCodeMode: false,
          mesh: { enabled: true, root: isHub ? hubMesh : workMesh, stateBackend: isHub ? "sqlite" : "file",
            transport: carrier, hostTransport: carrier, actorPollMs: 20,
            nats: { ...broker!.runtimeOptions(isHub ? broker!.users.machineA : broker!.users.machineB), operatorMachine: "fabric-ops" } },
          agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
          jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
        }),
      };
    };
    const ownerOptions = runtimeOptions("hub"), senderOptions = runtimeOptions("work");
    let populationEvidence: unknown;
    let seedHooks: SwitchSeedHooks | undefined;
    let seedResult: unknown;
    const seedModule = carrier === "nats" ? process.env.NATS_SWITCH_SEED_MODULE?.trim() : undefined;
    if (seedModule) {
      seedHooks = await import(pathToFileURL(path.resolve(seedModule)).href) as SwitchSeedHooks;
      if (typeof seedHooks.seedSwitchFixture !== "function") throw new Error("NATS_SWITCH_SEED_MODULE must export seedSwitchFixture");
      seedResult = await seedHooks.seedSwitchFixture({ root, hubMesh, workMesh, ownerOptions, senderOptions, carrier });
    }
    // Use the real public import/cutover path, not the test-only fresh-sqlite
    // initialization symbol. Child processes must open an actually imported hub.
    if (!fs.existsSync(path.join(hubMesh, "state.db"))) {
      const files = new MeshStore(hubMesh, 65_536, 1_000, { stateBackend: "file" });
      await files.put({ key: "switch-fixture/initialized", value: true,
        identity: { id: "switch-fixture", name: "switch-fixture", kind: "agent" } });
      files.closeState();
      await importMeshState(hubMesh);
    }
    const bridgeOptions = { root: path.join(root, "bridge"), hubMesh, workMesh, hubBackend: "sqlite" as const, workBackend: "file" as const };
    bridge = startSwitchBridge(bridgeOptions); processes.push(bridge);
    await bridge.request("ready");
    owner = startSwitchRuntime(ownerOptions); sender = startSwitchRuntime(senderOptions); processes.push(owner, sender);
    const [a, b] = await Promise.all([owner.request<{ pid: number; stateBackend: string }>("ready"), sender.request<{ pid: number; stateBackend: string }>("ready")]);
    expect(a.pid).not.toBe(b.pid); expect(a.stateBackend).toBe("sqlite"); expect(b.stateBackend).toBe("file");
    if (seedHooks?.verifySwitchPopulation) {
      // Each process is ready after its own native publication. The peer's KV
      // watch may still be catching up; observe both real routes before handing
      // one immutable pair of snapshots to the private population verifier.
      await vi.waitFor(async () => {
        const [ownerRoutes, senderRoutes] = await Promise.all([
          owner!.request<Array<{ id: string; stale?: boolean }>>("routes"),
          sender!.request<Array<{ id: string; stale?: boolean }>>("routes"),
        ]);
        expect(ownerRoutes.find(row => row.id === workId)).toMatchObject({ stale: false });
        expect(senderRoutes.find(row => row.id === hubId)).toMatchObject({ stale: false });
      }, { timeout: 5_000, interval: 25 });
      const [ownerRoutes, senderRoutes] = await Promise.all([owner.request("routes"), sender.request("routes")]);
      const evidence = await seedHooks.verifySwitchPopulation({ seedResult, ownerRoutes, senderRoutes,
        ownerId: hubId, senderId: workId, carrier });
      const serialized = JSON.stringify(evidence);
      if (serialized === undefined) throw new Error("verifySwitchPopulation must return serializable safe evidence");
      populationEvidence = JSON.parse(serialized);
    }
    // Route publication is driven by actual broker KV watches. These bounded
    // inspector RPCs are only the safety-net observation of that native state.
    await vi.waitFor(async () => {
      const routes = await sender!.request<Array<{ id: string; stale?: boolean }>>("routes");
      expect(routes.find(row => row.id === hubId)).toMatchObject({ stale: false });
    }, { timeout: 5_000, interval: 25 });
    connection = await broker.connect(broker.users.bootstrap);
    const { manager } = await openJetStream(connection);
    let streamManifest: string | undefined;
    const consumers = async (): Promise<SwitchConsumerSnapshot[]> => {
      const rows: SwitchConsumerSnapshot[] = [];
      const streams: string[] = [];
      for await (const info of manager.streams.list()) if (info.config.name.startsWith("FABRIC_IN_")) streams.push(info.config.name);
      streams.sort();
      for (const machine of PATH_B_MACHINES) if (!streams.includes(transportInboxStream(machine))) throw new Error("missing native inbox stream");
      const names = JSON.stringify(streams);
      if (streamManifest !== undefined && streamManifest !== names) throw new Error("global native inbox stream inventory changed");
      streamManifest = names;
      for (const stream of streams) {
        for await (const info of manager.consumers.list(stream)) {
          // One durable inbox's host filter covers BOTH commands and ACKs.
          rows.push({ stream, consumer: info.name, numPending: info.num_pending, numAckPending: info.num_ack_pending,
            delivered: info.delivered.stream_seq, ackFloor: info.ack_floor.stream_seq,
            filters: info.config.filter_subjects ?? (info.config.filter_subject ? [info.config.filter_subject] : []) });
        }
      }
      if (rows.length !== 2) throw new Error(`expected exactly two host command+ACK inbox consumers in GLOBAL inventory, found ${rows.length}`);
      if (rows.some(row => [row.numPending, row.numAckPending, row.delivered, row.ackFloor].some(value => !Number.isSafeInteger(value) || value < 0)))
        throw new Error("invalid global native inbox counters");
      for (const [machine, hostId] of [[PATH_B_MACHINES[0], hubId], [PATH_B_MACHINES[1], workId]] as const) {
        const inbox = rows.find(row => row.stream === transportInboxStream(machine) && row.consumer === transportDurableName(hostId));
        if (!inbox || inbox.filters.length !== 1 || inbox.filters[0] !== transportInboxFilter(machine, hostId))
          throw new Error("durable consumer manifest does not match the native owner and sender inboxes");
      }
      return rows;
    };
    return { root, broker, bridge, owner, sender, ownerOptions, senderOptions, bridgeOptions, consumers, populationEvidence, save,
      setProcesses(nextOwner: SwitchProcess, nextSender: SwitchProcess, nextBridge: SwitchProcess) {
        owner = nextOwner; sender = nextSender; bridge = nextBridge; processes.push(nextOwner, nextSender, nextBridge);
      },
      async close() {
        const stops = await Promise.allSettled(processes.map(process => process.stop()));
        const otherStops = await Promise.allSettled([connection?.close(), broker!.stop()]);
        const terminal = processes.map(process => process.terminal);
        save("terminal.json", { terminal, stops: stops.map(row => row.status === "fulfilled" ? { status: row.status } :
          { status: row.status, reason: switchError(row.reason) }), otherStops: otherStops.map(row => row.status === "fulfilled" ? { status: row.status } :
          { status: row.status, reason: switchError(row.reason) }), completedAt: Date.now() });
        console.info(JSON.stringify({ type: "switch-native-terminal", label: "pre-projection", terminal, artifact,
          stops: stops.map(row => row.status === "fulfilled" ? { status: row.status } : { status: row.status, reason: String(row.reason) }) }));
        const failures: unknown[] = [...stops, ...otherStops].flatMap(row => row.status === "rejected" ? [row.reason] : []);
        if (terminal.some(row => !row || row.exitCode !== 0 || row.signalCode !== null)) failures.push(new Error("fixture owned native process cleanup incomplete"));
        try { preserve(); } catch (error) { failures.push(error); }
        if (failures.length) throw new AggregateError(failures, "fixture cleanup and/or preservation failed");
        // The private test environment owns temp-root retirement. Evidence was
        // exported above before it can remove this command's private TMPDIR.
      },
    };
  } catch (error) {
    const stops = await Promise.allSettled(processes.map(process => process.stop()));
    const otherStops = await Promise.allSettled([connection?.close(), broker?.stop()]);
    const failures = [error, ...[...stops, ...otherStops].flatMap(row => row.status === "rejected" ? [row.reason] : [])];
    save("failure.json", { errors: failures.map(switchError), terminal: processes.map(process => process.terminal) });
    try { preserve(); } catch (preservationError) { failures.push(preservationError); }
    throw new AggregateError(failures, "fixture initialization, cleanup and/or preservation failed");
  }
}

function bridgeDiagnostic(carrier: "nats" | "file", status: SwitchMirrorStatus) {
  let freshNativeMirrors = false;
  try { requireSwitchMirrors(status, hubId, workId); freshNativeMirrors = true; } catch { /* emitted as RED evidence below */ }
  const mirrors = status.mirrors.filter(row => (row.mesh === "hub" && row.id === workId) || (row.mesh === "work" && row.id === hubId));
  return { type: "switch-native-bridge-preflight", carrier, linked: status.linked, checkedAt: status.checkedAt,
    nativeHostIds: { hub: status.nativeHostIds.hub.filter(id => id === hubId), work: status.nativeHostIds.work.filter(id => id === workId) },
    mirrors, omittedHistoricalMirrors: status.mirrors.length - mirrors.length, freshNativeMirrors,
    expectedNativeHostIds: { hub: hubId, work: workId },
    nativeHostCounts: { hub: status.nativeHostIds.hub.length, work: status.nativeHostIds.work.length },
    nativeMirrorCounts: { hub: status.mirrors.filter(row => row.mesh === "hub" && row.id === workId && row.fresh).length,
      work: status.mirrors.filter(row => row.mesh === "work" && row.id === hubId && row.fresh).length } };
}

describe.skipIf(!binary || process.platform !== "linux")("native Path B separate-root switch rehearsal", () => {
  it("shows Path B NATS KV presence is absent from FILE bridge snapshots by design", async () => {
    const f = await fixture();
    try {
      if (f.populationEvidence !== undefined) console.info(JSON.stringify({ type: "switch-capture-consumer-manifest", consumers: await f.consumers() }));
      const sends = [
        { id: "preflip-followup", ref: "agents.followUp", marker: "SWITCH_PREFLIP_FOLLOWUP" },
        { id: "preflip-steer", ref: "agents.steer", marker: "SWITCH_PREFLIP_STEER" },
      ];
      for (const send of sends) {
        const startedAt = Date.now();
        const result = await f.sender.request<{ acknowledged: boolean }>("invoke", { ref: send.ref, args: { id: hubId, message: send.marker } });
        expect(result).toMatchObject({ acknowledged: true });
        const messages = await f.owner.request<Array<{ message: unknown; options: unknown }>>("messages");
        const visibleCount = messages.filter(row => JSON.stringify(row.message).includes(send.marker)).length;
        expect(visibleCount).toBe(1);
        // UI-boundary count is NOT advertised as an admission-journal count or
        // ACK-of-ACK proof. The shared probe supplies those independently.
        console.info(JSON.stringify({ type: "switch-send-smoke", id: send.id, startedAt, completedAt: Date.now(), result, visibleCount,
          coverage: "real registry/control/receiver; Pi sendMessage boundary fixture; broker PubAck/ACK-of-ACK not instrumented" }));
      }
      const [ownerRoutes, senderRoutes] = await Promise.all([f.owner.request<Array<{ id: string; stale?: boolean }>>("routes"),
        f.sender.request<Array<{ id: string; stale?: boolean }>>("routes")]);
      expect(ownerRoutes.find(row => row.id === workId)).toMatchObject({ stale: false });
      expect(senderRoutes.find(row => row.id === hubId)).toMatchObject({ stale: false });
      const status = await f.bridge.request<SwitchMirrorStatus>("mirrors");
      console.info(JSON.stringify({ ...bridgeDiagnostic("nats", status), expectation: "Path B NATS KV presence is absent from FILE bridge snapshots by design",
        populationEvidence: f.populationEvidence }));
      expect(status.linked).toBe(true);
      expect(status.nativeHostIds.hub).not.toContain(hubId);
      expect(status.nativeHostIds.work).not.toContain(workId);
      expect(status.mirrors.some(row => row.mesh === "hub" && row.id === workId && row.fresh)).toBe(false);
      expect(status.mirrors.some(row => row.mesh === "work" && row.id === hubId && row.fresh)).toBe(false);
    } finally { await f.close(); }
  }, 60_000);

  it("FILE close without business traffic counterexample", async () => {
    const f = await fixture("file");
    try { requireSwitchMirrors(await f.bridge.request<SwitchMirrorStatus>("mirrors"), hubId, workId); }
    finally { await f.close(); }
  }, 60_000);

  it("counterexample FILE from start links fresh native mirrors and routes actual file followUp/steer", async () => {
    const f = await fixture("file");
    try {
      const status = await f.bridge.request<SwitchMirrorStatus>("mirrors");
      console.info(JSON.stringify(bridgeDiagnostic("file", status)));
      requireSwitchMirrors(status, hubId, workId);
      for (const [ref, marker] of [["agents.followUp", "FILE_START_FOLLOWUP"], ["agents.steer", "FILE_START_STEER"]] as const) {
        const startedAt = Date.now();
        const result = await f.sender.request("invoke", { ref, args: { id: hubId, message: marker } });
        expect(result).toMatchObject({ acknowledged: true });
        const messages = await f.owner.request<Array<{ message: unknown }>>("messages");
        const visibleCount = messages.filter(row => JSON.stringify(row.message).includes(marker)).length;
        expect(visibleCount).toBe(1);
        console.info(JSON.stringify({ type: "switch-file-counterexample-send", ref, startedAt, completedAt: Date.now(), result, visibleCount }));
      }
    } finally { await f.close(); }
  }, 60_000);

  it.skipIf(!probeModule)("R1 exact STOPALL frozen global inbox recheck before FILE flip and native manifest reconciliation", async () => {
    const f = await fixture();
    try {
      const prefixes = [f.ownerOptions.meshRoot, f.senderOptions.meshRoot].map(root => {
        const file = path.join(root, "events.jsonl"); return fs.existsSync(file) ? fs.readFileSync(file) : Buffer.alloc(0);
      });
      const report = await runSwitchRollback({ owner: f.owner, sender: f.sender, bridge: f.bridge,
        ownerOptions: f.ownerOptions, senderOptions: f.senderOptions, bridgeOptions: f.bridgeOptions, consumers: f.consumers,
        onPhase(event) { f.save(`phase-${event.at}-${event.phase}.json`, event); },
        inFlight: [
          { id: "rollback-followup", ref: "agents.followUp", args: { id: hubId, message: "ROLLBACK_FOLLOWUP" } },
          { id: "rollback-steer", ref: "agents.steer", args: { id: hubId, message: "ROLLBACK_STEER" } },
        ],
        postFlip: [
          { id: "file-followup", ref: "agents.followUp", args: { id: hubId, message: "FILE_FOLLOWUP" } },
          { id: "file-steer", ref: "agents.steer", args: { id: hubId, message: "FILE_STEER" } },
        ],
        onRestart: f.setProcesses,
      });
      const history = [f.ownerOptions.meshRoot, f.senderOptions.meshRoot].map((root, index) => {
        const file = path.join(root, "events.jsonl"), before = prefixes[index]!;
        const after = fs.existsSync(file) ? fs.readFileSync(file) : Buffer.alloc(0);
        expect(after.subarray(0, before.length).equals(before)).toBe(true);
        expect(report.eventEvidence.afterFile[index]!.eventArchive).toEqual(report.eventEvidence.before[index]!.eventArchive);
        return { originalBytes: before.length, finalBytes: after.length, originalPrefixPreserved: true,
          originalHistoryPresent: before.length !== 0, archivesUnchanged: true };
      });
      f.save("r1-report.json", { populationEvidence: f.populationEvidence, history, ...report });
      console.info(JSON.stringify({ type: "switch-rollback-report", populationEvidence: f.populationEvidence, history, ...report }));
      expect(report.phase).toBe("file-verified");
    } finally { await f.close(); }
  }, 90_000);

  it.skipIf(!probeModule)("R2 full capture-backed native followUp and steer both-role all-stage evidence and byte invariance", async () => {
    const f = await fixture();
    try {
      const before = await Promise.all([f.owner.request<SwitchProbeSnapshot>("probeSnapshot"), f.sender.request<SwitchProbeSnapshot>("probeSnapshot")]);
      await f.owner.request("probeHold");
      const pending = ["followUp", "steer"].map(operation => f.sender.request("probeSend", {
        id: `r2-${operation}`, ref: `agents.${operation}`, args: { id: hubId, message: `R2_NATIVE_${operation}` },
      }, 30_000));
      for (const send of pending) void send.catch(() => undefined);
      const ingress = await f.owner.request<{ commandId: string; source: string }>("probeIngress");
      expect(ingress.source).toBe("receiver-ingress");
      const held = await f.owner.request<SwitchProbeSnapshot>("probeSnapshot");
      expect(held.mainJournalSha256).toBe(before[0]!.mainJournalSha256);
      expect(held.events.some(row => row.commandId === ingress.commandId && row.type === "main-followup-journal-commit")).toBe(false);
      await f.owner.request("probeRelease");
      const results = await Promise.all(pending);
      expect(results.every(result => (result as { accepted?: boolean }).accepted === true)).toBe(true);
      const sender = await f.sender.request<SwitchProbeSnapshot>("probeSnapshot");
      expect(sender.sends).toHaveLength(2);
      const ids = sender.sends.map(send => send.commandId);
      await Promise.all([f.owner.request("probeWaitForEvidence", { ids }), f.sender.request("probeWaitForEvidence", { ids })]);
      let snapshots: SwitchProbeSnapshot[] = [];
      const complete = await waitForControlProbeEvidence(sender.sends, async () => {
        snapshots = await Promise.all([f.owner.request<SwitchProbeSnapshot>("probeSnapshot"), f.sender.request<SwitchProbeSnapshot>("probeSnapshot")]);
        return snapshots.flatMap(row => row.events);
      }, "nats");
      expect(complete.perSend.every(row => row.accepted && row.logicalAdmissionCount === 1 && row.allStagesVerified)).toBe(true);
      for (let index = 0; index < 2; index++) expect(snapshots[index]!.eventEvidence).toEqual(before[index]!.eventEvidence);
      expect(collectControlProbeEvidence(sender.sends, snapshots[1]!.events, "nats").every(row => !row.allStagesVerified)).toBe(true);
      const reconciled = await Promise.all([f.owner.request<{ lost: string[]; duplicates: string[] }>("probeReconcile", { ids }),
        f.sender.request<{ lost: string[]; duplicates: string[] }>("probeReconcile", { ids })]);
      for (const row of reconciled) { expect(row.lost).toEqual([]); expect(row.duplicates).toEqual([]); }
      const report = { type: "switch-r2-report", label: "pre-projection", populationEvidence: f.populationEvidence,
        operations: ["followUp", "steer"], perSend: complete.perSend, manifest: sender.sends, reconciled,
        eventEvidence: { before: before.map(row => row.eventEvidence), after: snapshots.map(row => row.eventEvidence), unchanged: true },
        holdBeforeJournal: true, roleLocalProofRejected: true, consumers: await f.consumers(),
        limits: ["shared v2 owner ACK publication stage does not inspect acceptance; native accepted API and journal reconciliation separately required",
          "original absent history proves absence invariance only", "actors not exercised"] };
      f.save("r2-report.json", report);
      console.info(JSON.stringify(report));
    } finally { await f.close(); }
  }, 90_000);
});
