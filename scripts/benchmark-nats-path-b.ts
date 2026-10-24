#!/usr/bin/env bun
import fs from "node:fs";
import path from "node:path";
import { createPathBHarness, PATH_B_MACHINES, type PathBHarness } from "../tests/helpers/nats-cluster.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { runControlProbe, runFileCounterprobe, type ControlProbeOptions, type ProbeBackend, type ProbeLoadShape, type ProbeMeshRoots, type ProbeNatsOptions, type ProbeStateBackend } from "../tests/helpers/nats-path-b-probe.js";

function usage(): string {
  return `Usage:
  bun scripts/benchmark-nats-path-b.ts --backend FILE-fixed|SQLite-fixed|NATS-Path-B --n N [options]

Backends:
  FILE-fixed      mesh.transport=file, mesh.stateBackend=file
  SQLite-fixed    mesh.transport=file, mesh.stateBackend=sqlite (refuses silent fallback)
  NATS-Path-B     mesh.transport=nats; state backends selected independently per role

Options:
  --mesh-root hub=/path       externally seeded owner/hub root; repeat for work=/path
  --mesh-root work=/path      sender/work root (NATS roles stay separate; FILE roles share one root)
  --load-shape steady|burst   steady is ~14 aggregate sends/s; burst is synchronized
  --sender-processes N        separate sender Main processes (default: N for burst, min(8,N) steady)
  --state-backend-hub file|sqlite
  --state-backend-work file|sqlite
  --server tls://HOST:PORT --owner-machine NAME --sender-machine NAME --operator-machine NAME
  --owner-credentials PATH --sender-credentials PATH --ca-file PATH
  --custody-stream NAME --liveness-stream NAME
  --local-test-server         start the real authenticated Path B harness (JetStream sync_interval: always)
  --runtime-config PATH       pass this JSON runtime configuration through to each Main, with role roots/transport applied
  --presence-projection pre-projection|enabled (metadata; only enabled results may claim the bar)
  --headroom-gib N (default 95); N>1 requires --admission-pilot PATH from a measured prior cell
  --server-label TEXT --sync-interval TEXT --deadline-ms N --rate-per-second N
  --hold-owner-admission      hold first Main admission; probe verifies it is pending, then releases by file
  --counterprobe              after NATS, run a shared-root FILE counterprobe
  --json PATH                 also write the result JSON to PATH
  --help

NATS policy streams are FABRIC_CUSTODY/FABRIC_LIVENESS; pathB machine inventory is the owner/sender pair.
The probe uses the public agents.followUp API; PRIMARY is sender-clock API invocation to acknowledged:true.
Without the presence projection, output is labelled pre-projection and barClaimable=false.
`;
}

function argumentsOf(args: readonly string[]) {
  const values = new Map<string, string[]>();
  const flags = new Set<string>();
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (arg === "--help" || arg === "-h") { flags.add("help"); continue; }
    if (arg === "--local-test-server" || arg === "--hold-owner-admission" || arg === "--counterprobe") { flags.add(arg.slice(2)); continue; }
    if (!arg.startsWith("--")) throw new Error(`unexpected argument: ${arg}`);
    const value = args[++index];
    if (value === undefined) throw new Error(`missing value for ${arg}`);
    const key = arg.slice(2);
    const entries = values.get(key) ?? [];
    entries.push(value);
    values.set(key, entries);
  }
  return { values, flags, get(key: string, fallback?: string) { return values.get(key)?.at(-1) ?? fallback; },
    all(key: string) { return values.get(key) ?? []; }, has(flag: string) { return flags.has(flag); } };
}

function numberValue(value: string | undefined, name: string, fallback: number): number {
  if (value === undefined) return fallback;
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 1) throw new Error(`${name} must be a positive integer`);
  return number;
}

function rootsOf(values: readonly string[]): ProbeMeshRoots {
  const roots: Partial<Record<keyof ProbeMeshRoots, string>> = {};
  for (const value of values) {
    const equals = value.indexOf("=");
    if (equals <= 0) throw new Error(`--mesh-root must be role=/absolute/path: ${value}`);
    const role = value.slice(0, equals);
    const root = value.slice(equals + 1);
    if (!( ["hub", "work", "owner", "sender"] as const).includes(role as "hub" | "work" | "owner" | "sender") || !path.isAbsolute(root)) throw new Error(`invalid --mesh-root assignment: ${value}`);
    roots[role as keyof ProbeMeshRoots] = root;
  }
  return roots;
}

async function withAdminEnv<T>(broker: PathBHarness, run: () => Promise<T>): Promise<T> {
  const saved = new Map<string, string | undefined>();
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) {
    saved.set(name, process.env[name]);
    process.env[name] = value;
  }
  try { return await run(); }
  finally {
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

async function main() {
  const args = argumentsOf(process.argv.slice(2));
  if (args.has("help")) { process.stdout.write(usage()); return; }
  const rawBackend = args.get("backend");
  if (!rawBackend) throw new Error("--backend is required\n" + usage());
  const backend = rawBackend === "NATS-Path-B" ? "NATS Path B" : rawBackend as ProbeBackend;
  if (!["FILE-fixed", "SQLite-fixed", "NATS Path B"].includes(backend)) throw new Error(`unsupported backend: ${rawBackend}`);
  const n = numberValue(args.get("n"), "--n", 0);
  const load = args.get("load-shape", "steady");
  const loadShape: ProbeLoadShape = load === "steady" ? "steady-14-ops-per-second" : load === "burst" ? "synchronized-burst" :
    (() => { throw new Error("--load-shape must be steady or burst"); })();
  const stateBackendHub = (args.get("state-backend-hub", backend === "SQLite-fixed" ? "sqlite" : "file")) as ProbeStateBackend;
  const stateBackendWork = (args.get("state-backend-work", backend === "SQLite-fixed" ? "sqlite" : "file")) as ProbeStateBackend;
  if (!["file", "sqlite"].includes(stateBackendHub) || !["file", "sqlite"].includes(stateBackendWork)) throw new Error("state backend must be file or sqlite");
  const runtimeConfigPath = args.get("runtime-config");
  const runtimeConfig = runtimeConfigPath ? JSON.parse(fs.readFileSync(runtimeConfigPath, "utf8")) as Record<string, unknown> : undefined;
  const projectionLabel = args.get("presence-projection", "pre-projection");
  if (projectionLabel !== "pre-projection" && projectionLabel !== "enabled") throw new Error("--presence-projection must be pre-projection or enabled");
  if (projectionLabel === "enabled" && !runtimeConfig) throw new Error("--presence-projection enabled requires --runtime-config with the projection config");
  const roots = rootsOf(args.all("mesh-root"));
  const headroomGiB = Number(args.get("headroom-gib", "95"));
  if (!Number.isFinite(headroomGiB) || headroomGiB <= 0) throw new Error("--headroom-gib must be positive");
  const headroomLimitBytes = Math.floor(headroomGiB * 1_073_741_824);
  let capacityAdmission: Record<string, unknown> | undefined;
  const admissionPilotPath = args.get("admission-pilot");
  const senderProcessCount = numberValue(args.get("sender-processes"), "--sender-processes", loadShape === "synchronized-burst" ? n : Math.min(8, n));
  if (n > 1 && senderProcessCount !== n) throw new Error("capacity-approved N20/N50 cells require one separate sender Main process per send");
  if (n > 1 && !admissionPilotPath) throw new Error("N>1 requires --admission-pilot from a prior measured cell");
  if (admissionPilotPath) {
    const envelope = JSON.parse(fs.readFileSync(admissionPilotPath, "utf8")) as { result?: Record<string, unknown> };
    const pilot = envelope.result;
    const pilotRun = pilot?.run as Record<string, unknown> | undefined;
    const observed = pilot?.capacityObservation as Record<string, unknown> | undefined;
    if (!pilotRun || pilotRun.backend !== backend || !observed ||
        typeof observed.peakWriterRssBytes !== "number" || typeof observed.ownerMainReaderPeakRssBytes !== "number" ||
        typeof observed.controllerPeakRssBytes !== "number") throw new Error("admission pilot is missing valid same-backend process RSS evidence");
    if (pilotRun.placement !== "same-host separate Main processes" || pilotRun.barClaimable !== false) throw new Error("admission pilot placement/projection label is not eligible");
    if ((pilot?.checks as Record<string, unknown> | undefined)?.allAcceptedHaveMainJournalCommit !== true) throw new Error("admission pilot did not prove accepted owner journal commits");
    const auxiliary = observed.auxiliaryReaderPeakRssBytes as Record<string, unknown> | undefined;
    if (backend === "NATS Path B" && typeof auxiliary?.["local NATS server"] !== "number") throw new Error("NATS admission pilot lacks broker RSS evidence");
    const auxiliaryBytes = Object.values(auxiliary ?? {}).reduce<number>((sum, value) => sum + (typeof value === "number" ? value : 0), 0);
    const readerControllerBytes = Number(observed.ownerMainReaderPeakRssBytes) + Number(observed.controllerPeakRssBytes) + auxiliaryBytes;
    const estimateBytes = Math.ceil((Number(observed.peakWriterRssBytes) * n + readerControllerBytes) * 1.2);
    capacityAdmission = { pilotPath: path.resolve(admissionPilotPath), backend, pilotN: pilotRun.n, targetN: n,
      headroomLimitBytes, writerPeakRssBytes: observed.peakWriterRssBytes, readerControllerAndAuxiliaryBytes: readerControllerBytes,
      estimateWith20PercentBytes: estimateBytes, admitted: estimateBytes <= headroomLimitBytes };
    if (estimateBytes > headroomLimitBytes) throw new Error(`capacity admission refused: estimate ${estimateBytes} exceeds headroom ${headroomLimitBytes}`);
  }
  const transport: "file" | "nats" = backend === "NATS Path B" ? "nats" : "file";
  let broker: PathBHarness | undefined;
  let natsOwner: ProbeNatsOptions | undefined;
  let natsSender: ProbeNatsOptions | undefined;
  try {
    if (transport === "nats" && args.has("local-test-server")) {
      broker = await createPathBHarness({ tls: {} });
      await withAdminEnv(broker, async () => {
        const status = await runTransportAdmin(["provision", "--ca-file", broker!.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")]);
        if (status !== 0) throw new Error(`local Path B provisioning failed with status ${status}`);
      });
      natsOwner = broker.runtimeOptions(broker.users.machineA);
      natsSender = broker.runtimeOptions(broker.users.machineB);
    } else if (transport === "nats") {
      const servers = args.get("server");
      const ownerCredentials = args.get("owner-credentials");
      const senderCredentials = args.get("sender-credentials");
      const ownerMachine = args.get("owner-machine");
      const senderMachine = args.get("sender-machine");
      const caFile = args.get("ca-file");
      if (!servers || !ownerCredentials || !senderCredentials || !ownerMachine || !senderMachine || !caFile) {
        throw new Error("external NATS mode requires --server, --owner-machine, --sender-machine, both credentials paths, and --ca-file");
      }
      const machines = [ownerMachine, senderMachine];
      const base = { servers, custodyStream: args.get("custody-stream", "FABRIC_CUSTODY")!,
        livenessStream: args.get("liveness-stream", "FABRIC_LIVENESS")!, pathBMachines: machines,
        tls: { caFile }, operatorMachine: args.get("operator-machine", "fabric-ops")! };
      natsOwner = { ...base, machine: ownerMachine, credentials: ownerCredentials };
      natsSender = { ...base, machine: senderMachine, credentials: senderCredentials };
    }
    const common: ControlProbeOptions = {
      n, backend, transport, ...(natsOwner ? { natsOwner, natsSender } : {}), meshRoots: roots,
      stateBackendHub, stateBackendWork, loadShape, headroomLimitBytes,
      ...(capacityAdmission ? { capacityAdmission } : {}),
      ...(natsOwner && broker?.processId ? { auxiliaryProcesses: [{ label: "local NATS server", pid: broker.processId }] } : {}),
      senderProcesses: senderProcessCount,
      ...(args.get("rate-per-second") ? { ratePerSecond: Number(args.get("rate-per-second")) } : {}),
      ...(args.get("deadline-ms") ? { deadlineMs: numberValue(args.get("deadline-ms"), "--deadline-ms", 1) } : {}),
      serverLabel: args.get("server-label", broker ? "LOCAL TEST SERVER" : "external NATS server")!,
      ...(args.get("sync-interval") ? { syncInterval: args.get("sync-interval")! } : broker ? { syncInterval: "always" } : {}),
      ...(runtimeConfig ? { runtimeConfig } : {}),
      presenceProjectionLabel: projectionLabel,
      ...(args.has("hold-owner-admission") ? { holdAdmission: true } : {}),
    };
    const result = await runControlProbe(common);
    const output: Record<string, unknown> = { result };
    if (args.has("counterprobe") && transport === "nats") {
      output.counterprobe = await runFileCounterprobe({ n: 1, stateBackend: "file", loadShape: "synchronized-burst",
        deadlineMs: common.deadlineMs ?? 30_000, presenceProjectionLabel: projectionLabel, runtimeConfig });
    }
    const json = `${JSON.stringify(output, null, 2)}\n`;
    process.stdout.write(json);
    const outputPath = args.get("json");
    if (outputPath) fs.writeFileSync(outputPath, json, { mode: 0o600 });
  } finally { await broker?.stop(); }
}

await main();
