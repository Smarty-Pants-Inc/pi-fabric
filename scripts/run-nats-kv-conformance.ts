// No download/auth plumbing: all three inputs must already be local, verified official release files.
// Usage: bun scripts/run-nats-kv-conformance.ts /abs/nats-server /abs/release.tar.gz /abs/SHA256SUMS [--async-seam]
// --async-seam requires a fresh build; substitutes full R3 restart/reconnect/public-entry probes for latency.
import { spawn, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { isSupportedNatsKvServer } from "../src/mesh/state-nats-kv.ts";
import { probeNatsAsyncReconnect } from "./probe-nats-async-reconnect.ts";
const asyncSeam = process.argv.includes("--async-seam");

const [binary, archive, sums] = process.argv.slice(2);
const output = process.env.FABRIC_NATS_EVIDENCE_DIR ?? process.env.TASK_OUT;
if (!output) throw new Error("Set TASK_OUT or FABRIC_NATS_EVIDENCE_DIR");
for (const input of [binary, archive, sums]) if (!input || !path.isAbsolute(input) || !fs.existsSync(input)) throw new Error(`BLOCKED: missing absolute local input ${input ?? "(unspecified)"}`);
const hash = (data: Uint8Array | string) => createHash("sha256").update(data).digest("hex");
const officialSums = fs.readFileSync(sums!, "utf8");
const name = path.basename(archive!);
const line = officialSums.split(/\r?\n/).find(line => line.trim().split(/\s+/).slice(1).join(" ").replace(/^\*/, "") === name);
if (!line || !/^[a-fA-F0-9]{64}\s/.test(line)) throw new Error(`Official SHA256SUMS has no entry for ${name}`);
const expected = line.split(/\s+/)[0]!.toLowerCase();
const actual = hash(fs.readFileSync(archive!));
if (actual !== expected) throw new Error("Official archive SHA256 mismatch");
const members = execFileSync("tar", ["-tf", archive!], { encoding: "utf8" }).trim().split("\n").filter(member => member.endsWith("/nats-server"));
if (members.length !== 1) throw new Error("Official archive must contain exactly one nats-server executable");
const archiveBinary = execFileSync("tar", ["-xOf", archive!, members[0]!], { maxBuffer: 128 * 1024 * 1024 });
const binarySha = hash(fs.readFileSync(binary!));
if (hash(archiveBinary) !== binarySha) throw new Error("Lane binary is not the checksum-verified official archive binary");
const versionOutput = execFileSync(binary!, ["-v"], { encoding: "utf8" }).trim();
const version = /v(\d+\.\d+\.\d+)(?:$|\s)/.exec(versionOutput)?.[1] ?? "";
if (!isSupportedNatsKvServer(version)) throw new Error(`nats-server 2.14.7+ required: ${versionOutput}`);
fs.mkdirSync(output, { recursive: true });
fs.writeFileSync(path.join(output, "nats-server-SHA256SUMS.official"), officialSums);
fs.writeFileSync(path.join(output, "official-release-verification.json"), JSON.stringify({ binary, archive, sums, expected, actual, binarySha, versionOutput }, null, 2) + "\n");

const deadline = async <T>(promise: Promise<T>, ms: number): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Cluster deadline spent")), ms); })]); }
  finally { clearTimeout(timer); }
};
const port = async () => {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const value = (server.address() as net.AddressInfo).port;
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return value;
};
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-u2-r3-"));
const ports = await Promise.all(Array.from({ length: 9 }, port));
if (new Set(ports).size !== 9) throw new Error("Ephemeral ports collided; retry runner with a new allocation");
type RunningNode = { child: ReturnType<typeof spawn>; done: Promise<void>; ready: Promise<void> };
const nodes: RunningNode[] = [], current: RunningNode[] = [];
const streamLeaderWaiters = new Map<string, () => void>();
let leaderReady!: () => void;
let metadataLeader = new Promise<void>(resolve => { leaderReady = resolve; });
const env = { ...process.env, FABRIC_NATS_TEST_SERVERS: ports.slice(0, 3).map(p => `nats://127.0.0.1:${p}`).join(","),
  FABRIC_NATS_TEST_MONITORS: ports.slice(3, 6).map(p => `http://127.0.0.1:${p}`).join(","), FABRIC_NATS_EVIDENCE_DIR: output };
const run = async (command: string, args: string[], name: string, extra: Record<string, string> = {}) => {
  const log = fs.createWriteStream(path.join(output, name));
  const child = spawn(command, args, { cwd: process.cwd(), env: { ...env, ...extra }, stdio: ["ignore", "pipe", "pipe"] });
  child.stdout!.pipe(log, { end: false }); child.stderr!.pipe(log, { end: false });
  try {
    await new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", code => code === 0 ? resolve() : reject(new Error(`${name} exit ${code}`))); });
  } finally { await new Promise<void>(resolve => log.end(resolve)); }
};
const startNode = (n: number) => {
  const config = `server_name: "fabric-u2-${n}"\nhost: "127.0.0.1"\nport: ${ports[n]}\nhttp: "127.0.0.1:${ports[n + 3]}"\nmax_payload: 2097152\njetstream {\n store_dir: ${JSON.stringify(path.join(scratch, `node-${n}`))}\n max_memory_store: 67108864\n max_file_store: 1073741824\n sync_interval: always\n}\ncluster {\n name: "fabric-u2"\n host: "127.0.0.1"\n port: ${ports[n + 6]}\n routes: [${ports.slice(6).filter((_, i) => i !== n).map(p => `"nats://127.0.0.1:${p}"`).join(",")} ]\n}\n`;
  const configPath = path.join(output, `nats-node-${n}.conf`); fs.writeFileSync(configPath, config);
  execFileSync(binary!, ["-t", "-c", configPath], { stdio: "pipe" });
  const log = fs.createWriteStream(path.join(output, `nats-node-${n}.log`), { flags: "a" });
  const child = spawn(binary!, ["-c", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  let ready!: () => void, failed!: (error: Error) => void;
  const started = new Promise<void>((resolve, reject) => { ready = resolve; failed = reject; }); started.catch(() => undefined);
  let tail = "";
  const consume = (data: Buffer) => {
    log.write(data); tail = (tail + data.toString()).slice(-8_192);
    if (/Server is ready/.test(tail)) ready();
    if (/JetStream cluster new metadata leader/.test(tail)) leaderReady();
    for (const [stream, ready] of streamLeaderWaiters) {
      if (tail.split("\n").some(line => line.includes("JetStream cluster new stream leader") && line.includes(stream))) {
        streamLeaderWaiters.delete(stream); ready();
      }
    }
  };
  child.stdout!.on("data", consume); child.stderr!.on("data", consume);
  const done = new Promise<void>((resolve, reject) => {
    child.once("error", error => { failed(error); log.end(); reject(error); });
    child.once("close", code => { failed(new Error(`Node ${n} exited ${code} before ready`)); log.end(resolve); });
  }); done.catch(() => undefined);
  const node = { child, done, ready: started };
  nodes.push(node); current[n] = node;
  return node;
};
const stopNodes = async (owned: RunningNode[]) => {
  const running = (node: RunningNode) => node.child.exitCode === null && node.child.signalCode === null;
  for (const node of owned) if (running(node)) node.child.kill("SIGTERM");
  const kill = setTimeout(() => { for (const node of owned) if (running(node)) node.child.kill("SIGKILL"); }, 10_000);
  try {
    const stopped = await Promise.allSettled(owned.map(node => node.done));
    const failed = stopped.find(result => result.status === "rejected");
    if (failed?.status === "rejected") throw failed.reason;
  } finally { clearTimeout(kill); }
};

try {
  for (let n = 0; n < 3; n++) startNode(n);
  await deadline(Promise.all([...nodes.map(node => node.ready), metadataLeader]), 60_000);
  fs.writeFileSync(path.join(output, "cluster-topology.json"), JSON.stringify({ hostname: os.hostname(), nodes: ports.slice(0, 3), replicas: 3,
    sync_interval: "always", faultDomain: "ONE HOST: local R3 functional conformance only; NOT three-host production durability proof" }, null, 2) + "\n");
  console.log("R3 ready: running dedicated live-NATS backend and async provider cases");
  await run("bunx", ["vitest", "run", "--reporter=verbose", "--fileParallelism=false", "tests/mesh-state-nats-kv-live.test.ts", "tests/mesh-provider-nats-live.test.ts"], "live-r3.log");
  console.log("Live cases passed: running complete targeted state conformance (including five formerly skipped NATS cases)");
  await run("bunx", ["vitest", "run", "--reporter=verbose", "tests/mesh-state-async-contract.test.ts", "tests/mesh-state-async-multiprocess.test.ts",
    "tests/mesh-state-backend.test.ts", "tests/mesh-state-backend-multiprocess.test.ts"], "conformance-r3.log");
  if (asyncSeam) {
    console.log("Conformance passed: restarting all R3 nodes and proving same-client reconnect + built public API");
    await probeNatsAsyncReconnect({
      servers: env.FABRIC_NATS_TEST_SERVERS.split(","), output,
      stopCluster: () => stopNodes(current),
      restartCluster: async stream => {
        const elected = new Promise<void>(resolve => { streamLeaderWaiters.set(stream, resolve); });
        // A restored stream can elect before metadata. Wait for BOTH current-generation
        // events before a reopened client asks JetStreamManager for account/stream authority.
        metadataLeader = new Promise<void>(resolve => { leaderReady = resolve; });
        for (let n = 0; n < 3; n++) startNode(n);
        await deadline(Promise.all([...current.map(node => node.ready), elected, metadataLeader]), 60_000);
      },
    });
  }
  if (!asyncSeam) {
    console.log("Conformance passed: measuring file/SQLite/NATS get/put/CAS at 1 KiB and 100 KiB");
    await run("bun", ["scripts/benchmark-state-kv.ts"], "latency-r3-1024.log", { FABRIC_STATE_BENCH_VALUE_BYTES: "1024" });
    await run("bun", ["scripts/benchmark-state-kv.ts"], "latency-r3-102400.log", { FABRIC_STATE_BENCH_VALUE_BYTES: "102400" });
  }
  console.log(`PASS: official binary verified; local R3 conformance and ${asyncSeam ? "async restart/built-entry" : "latency"} artifacts retained`);
} finally {
  try { await stopNodes(nodes); } finally {
    fs.writeFileSync(path.join(output, "server-shutdown.json"), JSON.stringify(nodes.map(node => ({ pid: node.child.pid, exitCode: node.child.exitCode, signal: node.child.signalCode })), null, 2) + "\n");
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}
