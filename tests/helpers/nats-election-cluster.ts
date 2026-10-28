import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { waitForNatsCluster, type NatsReadinessNode, type NatsReadinessOptions } from "../../scripts/nats-cluster-readiness.mjs";

/** Match the backend's /varz-only durability gate, not the shared monitor gate's /jsz fallback.
 * Reuse its single 60s deadline and bounded request polling; never retry a failed backend open. */
export const waitForNatsElectionReadiness = (nodes: NatsReadinessNode[], options: NatsReadinessOptions = {}) => {
  const fetchMonitor = options.fetch ?? globalThis.fetch;
  return waitForNatsCluster(nodes, { ...options, fetch: async (input, init) => {
    const response = await fetchMonitor(input, init);
    const url = new URL(input instanceof Request ? input.url : String(input));
    if (response.ok && url.pathname === "/varz") {
      const body = await response.clone().json() as { jetstream?: { config?: { sync_always?: unknown } } };
      const syncAlways = body.jetstream?.config?.sync_always;
      if (syncAlways !== true) throw new Error(`/varz must report jetstream.config.sync_always=true before a backend open (got ${String(syncAlways)})`);
    }
    return response;
  } });
};
const reservePorts = async (count: number): Promise<{ ports: number[]; release(): Promise<void> }> => {
  const servers: net.Server[] = [];
  try {
    const ports: number[] = [];
    for (let index = 0; index < count; index++) {
      const server = net.createServer();
      servers.push(server);
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("could not reserve loopback port");
      ports.push(address.port);
    }
    let released = false;
    return {
      ports,
      async release() {
        if (released) return;
        released = true;
        await Promise.all(servers.map(server => new Promise<void>((resolve, reject) => {
          if (!server.listening) { resolve(); return; }
          server.close(error => error ? reject(error) : resolve());
        })));
      },
    };
  } catch (error) {
    await Promise.all(servers.map(server => new Promise<void>(resolve => {
      if (!server.listening) { resolve(); return; }
      server.close(() => resolve());
    })));
    throw error;
  }
};
const waitForExit = async (child: ChildProcess, ms: number, what: string): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    const done = () => { clearTimeout(timer); child.removeListener("exit", onExit); resolve(); };
    const onExit = () => done();
    const timer = setTimeout(() => {
      child.removeListener("exit", onExit);
      reject(new Error(`timeout waiting for ${what}`));
    }, ms);
    child.once("exit", onExit);
    if (child.exitCode !== null || child.signalCode !== null) done();
  });
};
const json = async (url: string): Promise<Record<string, any>> => {
  const response = await fetch(url, { signal: AbortSignal.timeout(1_000) });
  if (!response.ok) throw new Error(`${url} returned HTTP ${response.status}`);
  return await response.json() as Record<string, any>;
};

export interface ElectionNode { name: string; clientUrl: string; monitorUrl: string; child: ChildProcess; metaId?: string }
export interface NatsElectionCluster { root: string; nodes: ElectionNode[]; readonly metaLeader: string; readonly seedUrl: string; readonly seedMonitorUrl: string; pauseMetaLeader(onStopped?: (leader: ElectionNode) => void, prepare?: (seedUrl: string) => Promise<void>): Promise<ElectionNode>; cleanup(): Promise<void>; cleanupReceipt(): unknown }

/** Starts three owned local nats-server children, connected by routes and configured for durable JetStream. */
export const startNatsElectionCluster = async (binary: string): Promise<NatsElectionCluster> => {
  if (!path.isAbsolute(binary) || !fs.existsSync(binary)) throw new Error(`NATS_BIN must name an existing absolute binary: ${binary}`);
  const scratch = process.env.TMPDIR;
  if (!scratch || !path.isAbsolute(scratch)) throw new Error("TMPDIR must be an absolute private scratch directory");
  const root = fs.mkdtempSync(path.join(scratch, "pi-fabric-nats-election-"));
  const nodes: ElectionNode[] = [];
  let cleaned = false;
  let cleanupReceipt: unknown;
  const stopAll = async (): Promise<void> => {
    if (cleaned) return;
    const failures: string[] = [];
    for (const node of nodes) if (node.child.exitCode === null && node.child.signalCode === null) {
      try { node.child.kill("SIGCONT"); } catch { /* already exited */ }
    }
    for (const node of nodes) {
      const child = node.child;
      if (child.exitCode !== null || child.signalCode !== null) continue;
      try { child.kill("SIGTERM"); } catch { /* await the exit receipt below */ }
      try { await waitForExit(child, 5_000, `${node.name} SIGTERM exit`); }
      catch {
        try { child.kill("SIGKILL"); } catch { /* await bounded receipt */ }
        try { await waitForExit(child, 2_000, `${node.name} SIGKILL exit`); }
        catch { failures.push(`${node.name} pid ${child.pid} did not exit`); }
      }
    }
    for (const node of nodes) if (node.child.exitCode === null && node.child.signalCode === null) failures.push(`${node.name} remains live`);
    try { fs.rmSync(root, { recursive: true, force: true }); }
    catch (error) { failures.push(`scratch removal failed: ${String(error)}`); }
    cleanupReceipt = { exits: nodes.map(node => ({ name: node.name, pid: node.child.pid, exitCode: node.child.exitCode, signalCode: node.child.signalCode })), noLiveChildren: nodes.every(node => node.child.exitCode !== null || node.child.signalCode !== null), scratchRemoved: !fs.existsSync(root) };
    if (failures.length) throw new Error(`NATS process cleanup failed: ${failures.join(", ")}`);
    cleaned = true;
  };
  try {
    const reservation = await reservePorts(9);
    const ports = reservation.ports;
    await reservation.release();
    if (new Set(ports).size !== ports.length) throw new Error("port allocation collision");
    const records = Array.from({ length: 3 }, (_, i) => ({ client: ports[i * 3]!, route: ports[i * 3 + 1]!, monitor: ports[i * 3 + 2]! }));
    for (let index = 0; index < 3; index++) {
      const name = `election-${index + 1}`;
      const own = records[index]!;
      const routes = records.filter((_, peer) => peer !== index).map(peer => `nats://127.0.0.1:${peer.route}`);
      const config = {
        server_name: name,
        listen: `127.0.0.1:${own.client}`,
        http: `127.0.0.1:${own.monitor}`,
        jetstream: { store_dir: path.join(root, name), sync_interval: "always" },
        cluster: { name: "election-test", listen: `127.0.0.1:${own.route}`, routes },
      };
      const configPath = path.join(root, `${name}.json`);
      fs.writeFileSync(configPath, JSON.stringify(config));
      const child = spawn(binary, ["--config", configPath], { stdio: ["ignore", "ignore", "pipe"] });
      let stderr = "";
      child.stderr?.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-8_000); });
      nodes.push({ name, clientUrl: `nats://127.0.0.1:${own.client}`, monitorUrl: `http://127.0.0.1:${own.monitor}`, child });
      child.once("error", error => { stderr += `\n${error.message}`; });
      (child as ChildProcess & { electionStderr?: () => string }).electionStderr = () => stderr;
    }
    let readiness;
    try {
      readiness = await waitForNatsElectionReadiness(nodes.map(node => ({ name: node.name, monitorUrl: node.monitorUrl })));
    } catch (error) {
      const childDiagnostics = nodes.map(node => {
        const child = node.child as ChildProcess & { electionStderr?: () => string };
        return `${node.name} child pid=${child.pid ?? "unknown"} exit=${child.exitCode ?? child.signalCode ?? "running"} stderr:\n${child.electionStderr?.() ?? "(no stderr captured)"}`;
      }).join("\n");
      const readinessMessage = error instanceof Error ? error.message : String(error);
      throw new Error(`three-node NATS readiness failed:\n${readinessMessage}\n\nPer-node fixture child diagnostics:\n${childDiagnostics}`);
    }
    for (const state of readiness.nodes) {
      const node = nodes.find(candidate => candidate.name === state.name);
      if (node) node.metaId = state.serverId;
    }
    const agreedLeader = readiness.metaLeader!;
    let seed = nodes.find(node => node.name !== agreedLeader && nodes.some(candidate => candidate.name === agreedLeader || candidate.metaId === agreedLeader))!;
    let seedUrl = seed.clientUrl;
    let seedMonitorUrl = seed.monitorUrl;
    return {
      root, nodes, metaLeader: agreedLeader!, get seedUrl() { return seedUrl; }, get seedMonitorUrl() { return seedMonitorUrl; }, cleanup: stopAll, cleanupReceipt: () => cleanupReceipt,
      async pauseMetaLeader(onStopped?: (leader: ElectionNode) => void, prepare?: (seedUrl: string) => Promise<void>) {
        const states = await Promise.all(nodes.map(async node => ({ node, jsz: await json(`${node.monitorUrl}/jsz?raft=true`), varz: await json(`${node.monitorUrl}/varz`) })));
        const leaders = new Set(states.map(state => state.jsz.meta_cluster?.leader));
        if (leaders.size !== 1 || typeof [...leaders][0] !== "string") throw new Error(`metadata leader is not agreed: ${JSON.stringify([...leaders])}`);
        const leader = [...leaders][0] as string;
        const selected = states.find(state => state.node.name === leader || state.varz.server_id === leader)?.node;
        if (!selected || !selected.child.pid || selected.child.exitCode !== null || selected.child.signalCode !== null) throw new Error(`cannot map metadata leader ${leader} to a live direct child`);
        seed = nodes.find(node => node !== selected)!;
        seedUrl = seed.clientUrl;
        seedMonitorUrl = seed.monitorUrl;
        await prepare?.(seedUrl);
        const confirmed = await Promise.all(nodes.map(async node => ({ node, jsz: await json(`${node.monitorUrl}/jsz?raft=true`) })));
        const confirmedLeaders = new Set(confirmed.map(state => state.jsz.meta_cluster?.leader));
        if (confirmedLeaders.size !== 1 || [...confirmedLeaders][0] !== leader) throw new Error(`metadata leader changed while preparing pause: expected ${leader}, got ${JSON.stringify([...confirmedLeaders])}`);
        if (!selected.child.kill("SIGSTOP")) throw new Error(`failed to SIGSTOP owned metadata leader ${selected.name}`);
        onStopped?.(selected);
        return selected;
      },
    };
  } catch (error) {
    await stopAll().catch(cleanup => { throw new AggregateError([error, cleanup], "cluster setup and cleanup both failed"); });
    throw error;
  }
};

export const stopNatsElectionCluster = async (cluster: NatsElectionCluster): Promise<void> => {
  await cluster.cleanup();
};
