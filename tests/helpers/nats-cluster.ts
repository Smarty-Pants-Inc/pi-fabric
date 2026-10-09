import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";

export const natsServerBinary = process.env.NATS_SERVER ?? path.resolve(".lane/nats/nats-server-v2.14.7-linux-amd64/nats-server");
export const natsAvailable = fs.existsSync(natsServerBinary);
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const freePort = async (): Promise<number> => {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const port = (server.address() as net.AddressInfo).port;
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return port;
};

export interface LocalNatsCluster {
  servers: string[];
  names: string[];
  root: string;
  monitors: string[];
  kill(name: string): Promise<void>;
  stopServer(name: string): Promise<void>;
  restart(name: string): Promise<void>;
  stop(): Promise<void>;
}

/** Owns and reaps every server, including startup failures. Storage/config live only in TMPDIR. */
export async function startNatsCluster(size: 1 | 3 = 1, label = "integration"): Promise<LocalNatsCluster> {
  if (!natsAvailable) throw new Error(`NATS_SERVER binary missing: ${natsServerBinary}`);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-"));
  const id = randomUUID().slice(0, 8);
  const names = Array.from({ length: size }, (_, i) => `fabric_${id}_${i}`);
  const ports = await Promise.all(names.map(() => freePort()));
  const routes = await Promise.all(names.map(() => freePort()));
  const monitors = await Promise.all(names.map(() => freePort()));
  const children: Array<{ process: ChildProcess; closed: Promise<void> } | undefined> = [];
  const logs = path.join(process.env.TASK_OUT ?? root, `nats-${label}-${id}`);
  fs.mkdirSync(logs, { recursive: true });
  const start = (i: number): void => {
    const config = [
      `server_name: "${names[i]}"`, `host: "127.0.0.1"`, `port: ${ports[i]}`,
      `http: "127.0.0.1:${monitors[i]}"`,
      `jetstream { store_dir: "${path.join(root, `data-${i}`).replaceAll("\\", "/")}", sync_interval: "always" }`,
      ...(size === 3 ? [`cluster { name: "fabric_${id}", host: "127.0.0.1", port: ${routes[i]}, routes: [${routes.filter((_, j) => j !== i).map(port => `"nats://127.0.0.1:${port}"`).join(",")}] }`] : []),
    ].join("\n");
    const file = path.join(root, `server-${i}.conf`);
    fs.writeFileSync(file, config);
    const fd = fs.openSync(path.join(logs, `server-${i}.log`), "a");
    try {
      const child = spawn(natsServerBinary, ["-c", file], { stdio: ["ignore", fd, fd] });
      let error: Error | undefined;
      child.once("error", cause => { error = cause; });
      const closed = new Promise<void>((resolve, reject) => child.once("close", () => error ? reject(error) : resolve()));
      // Prevent an unobserved rejection; stop/kill still await the real receipt.
      void closed.catch(() => undefined);
      children[i] = { process: child, closed };
    } finally { fs.closeSync(fd); }
  };
  const ready = async (): Promise<void> => {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      for (const child of children) {
        if (child && (child.process.exitCode !== null || child.process.signalCode !== null)) {
          throw new Error(`NATS startup failed; logs: ${logs}`);
        }
      }
      const responses = await Promise.all(monitors.map(async port => {
        try {
          const response = await fetch(`http://127.0.0.1:${port}/jsz?raft=true`, { signal: AbortSignal.timeout(500) });
          const json = await response.json() as { meta_cluster?: { leader?: string } };
          return response.ok && (size === 1 || Boolean(json.meta_cluster?.leader));
        } catch { return false; }
      }));
      if (responses.every(Boolean)) return;
      await pause(100);
    }
    throw new Error(`NATS cluster not ready in 30 s; logs: ${logs}`);
  };
  const kill = async (name: string): Promise<void> => {
    const i = names.indexOf(name);
    if (i < 0) throw new Error(`Unknown owned NATS server: ${name}`);
    const child = children[i];
    if (child) { child.process.kill("SIGKILL"); await child.closed; children[i] = undefined; }
  };
  const stopServer = async (name: string): Promise<void> => {
    const i = names.indexOf(name);
    if (i < 0) throw new Error(`Unknown owned NATS server: ${name}`);
    const child = children[i];
    if (!child) return;
    child.process.kill("SIGTERM");
    const timer = setTimeout(() => { child.process.kill("SIGKILL"); }, 5000);
    try { await child.closed; children[i] = undefined; } finally { clearTimeout(timer); }
  };
  const emergencyStop = () => { for (const child of children) child?.process.kill("SIGKILL"); };
  process.once("exit", emergencyStop);
  const stop = async (): Promise<void> => {
    process.removeListener("exit", emergencyStop);
    await Promise.all(children.map(async child => {
      if (!child) return;
      if (child.process.exitCode === null && child.process.signalCode === null) child.process.kill("SIGTERM");
      const timer = setTimeout(() => { child.process.kill("SIGKILL"); }, 5000);
      try { await child.closed; } finally { clearTimeout(timer); }
    }));
    fs.rmSync(root, { recursive: true, force: true });
  };
  try {
    for (let i = 0; i < size; i++) start(i);
    await ready();
    return { servers: ports.map(port => `nats://127.0.0.1:${port}`), names, root,
      monitors: monitors.map(port => `http://127.0.0.1:${port}`), kill, stopServer, stop,
      restart: async name => {
        const i = names.indexOf(name);
        if (i < 0 || children[i]) throw new Error("Restart requires a stopped owned NATS server");
        start(i);
        await ready();
      },
    };
  } catch (error) { await stop(); throw error; }
}
