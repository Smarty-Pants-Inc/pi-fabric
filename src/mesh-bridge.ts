import { childProcessEnvironment } from "./core/atomic-write.js";
// mesh-bridge (smarty-dev#2004): link this host's Fabric mesh to one remote host's mesh.
//
//   mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE --ssh HOST [--ssh-key KEY]
//   mesh-bridge run ... -- COMMAND [ARGS...]      (any stdio transport, for tests)
//   mesh-bridge agent --mesh ROOT --peer NAME     (the remote end; the ssh forced command)
//
// The remote pins the agent in authorized_keys, so the key can run nothing else:
//   command="node /opt/pi-fabric/bin/mesh-bridge agent --mesh /home/u/proj/.pi/fabric/mesh --peer dev1",restrict ssh-ed25519 ...
import { spawn } from "node:child_process";
import { DEFAULT_CALL_TIMEOUT_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, validBridgeName } from "./mesh/bridge.js";
import { MeshStore } from "./mesh/store.js";

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts).
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;

const USAGE = `usage:
  mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE [--lock-protocol 1|2] [--max-state-bytes BYTES] [--call-timeout-ms MS] (--ssh HOST [--ssh-key KEY] [--ssh-port N] [--ssh-known-hosts FILE] | -- COMMAND...)
  mesh-bridge agent --mesh ROOT --peer NAME [--lock-protocol 1|2] [--max-state-bytes BYTES]`;

const parseArgs = (argv: string[]): { mode: string; flags: Map<string, string>; command: string[] } => {
  const [mode = "", ...rest] = argv;
  const flags = new Map<string, string>();
  const split = rest.indexOf("--");
  const own = split >= 0 ? rest.slice(0, split) : rest;
  for (let index = 0; index < own.length; index += 2) {
    const flag = own[index]!;
    const value = own[index + 1];
    if (!flag.startsWith("--") || value === undefined) throw new Error(`Bad argument: ${flag}\n${USAGE}`);
    flags.set(flag.slice(2), value);
  }
  return { mode, flags, command: split >= 0 ? rest.slice(split + 1) : [] };
};

const required = (flags: Map<string, string>, name: string): string => {
  const value = flags.get(name);
  if (!value) throw new Error(`--${name} is required\n${USAGE}`);
  return value;
};

/** The transport argv: ssh to the forced command, or an explicit command. */
export const transportCommand = (flags: Map<string, string>, command: string[]): string[] => {
  if (command.length > 0) return command;
  const host = required(flags, "ssh");
  const key = flags.get("ssh-key");
  const port = flags.get("ssh-port");
  const knownHosts = flags.get("ssh-known-hosts");
  if (port !== undefined && !/^[0-9]{1,5}$/.test(port)) throw new Error("--ssh-port must be a port number");
  if (host.startsWith("-")) throw new Error("--ssh must be a host, not an option");
  // Explicit flags, not an ssh option passthrough: a target needs no global ssh config, and no
  // option can run a local command (ProxyCommand, LocalCommand).
  return [
    "ssh", "-T", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
    ...(key ? ["-o", "IdentitiesOnly=yes", "-i", key] : []),
    ...(port ? ["-p", port] : []),
    ...(knownHosts ? ["-o", `UserKnownHostsFile=${knownHosts}`, "-o", "StrictHostKeyChecking=yes"] : []),
    // The forced command on the remote replaces whatever is asked for here.
    host, "mesh-bridge", "agent",
  ];
};

export const openBridgeStore = (root: string, flags: Map<string, string>): MeshStore => {
  const selected = flags.get("lock-protocol") ?? "1";
  if (selected !== "1" && selected !== "2") throw new Error("--lock-protocol must be 1 or 2");
  const capacity = flags.get("max-state-bytes");
  const maxStateBytes = capacity === undefined ? undefined : Number(capacity);
  if (maxStateBytes !== undefined && (!Number.isSafeInteger(maxStateBytes) || maxStateBytes < MAX_EVENT_BYTES * 2)) {
    throw new Error(`--max-state-bytes must be a safe integer of at least ${MAX_EVENT_BYTES * 2}`);
  }
  return new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, {
    lockProtocol: selected === "1" ? 1 : 2,
    ...(maxStateBytes === undefined ? {} : { maxStateBytes }),
  });
};
const log = (message: string): void => void process.stderr.write(`mesh-bridge: ${message}\n`);

const runAgent = async (flags: Map<string, string>): Promise<void> => {
  const peer = required(flags, "peer");
  const side = new StoreBridgeSide(openBridgeStore(required(flags, "mesh"), flags), peer);
  await serveBridgeAgent(side, process.stdin, process.stdout);
};

/** A transport child that ignores SIGTERM is killed this long after it. */
const CHILD_KILL_MS = 2_000;

/**
 * Run one bridge until the signal aborts it or the transport fails. Every wait on the remote is
 * bounded (security review F3): a silent agent cannot keep the bridge, its local mirrors or its
 * transport child alive past the call deadline plus the stop bound.
 */
export const runBridge = async (
  flags: Map<string, string>,
  command: string[],
  signal: AbortSignal,
  onChild?: (pid: number | undefined) => void,
): Promise<number> => {
  // Everything that can fail without a transport is checked and built first; from the spawn on,
  // one finally owns the child (security review round 3, F5).
  const localName = required(flags, "name");
  const remoteName = required(flags, "remote");
  if (!validBridgeName(localName) || !validBridgeName(remoteName)) throw new Error("Invalid bridge name");
  if (localName === remoteName) throw new Error("Bridge side names must differ");
  const callTimeoutMs = Number(flags.get("call-timeout-ms") ?? DEFAULT_CALL_TIMEOUT_MS);
  if (!Number.isSafeInteger(callTimeoutMs) || callTimeoutMs <= 0) throw new Error("--call-timeout-ms must be a positive integer");
  const cursorPath = required(flags, "cursor");
  const local = new StoreBridgeSide(openBridgeStore(required(flags, "mesh"), flags), remoteName);
  const argv = transportCommand(flags, command);
  if (signal.aborted) return 0;

  const child = spawn(argv[0]!, argv.slice(1), { env: childProcessEnvironment(), stdio: ["pipe", "pipe", "inherit"] });
  let spawnError: Error | undefined;
  let remote: RemoteBridgeSide | undefined;
  // A transport that cannot start (no ssh, not executable) emits error and close, never exit:
  // either settles the child, and the error fails the bridge by name.
  const exited = new Promise<void>((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) resolve();
    child.once("exit", () => resolve());
    child.once("close", () => resolve());
    child.once("error", (error) => {
      spawnError ??= new Error(`Bridge transport ${argv[0]} failed: ${error.message}`);
      remote?.close(spawnError);
      resolve();
    });
  });
  child.stdin.on("error", () => undefined);
  let bridge: MeshBridge | undefined;
  let code = 0;
  try {
    onChild?.(child.pid);
    remote = new RemoteBridgeSide(child.stdout, child.stdin, callTimeoutMs);
    if (spawnError) remote.close(spawnError);
    const linked = remote;
    bridge = new MeshBridge({
      localName, remoteName, local, remote: linked, cursorPath,
      stopMs: Math.min(callTimeoutMs, 5_000),
      log,
    });
    const running = bridge;
    const aborted = new Promise<void>((resolve) => {
      if (signal.aborted) resolve();
      else signal.addEventListener("abort", () => resolve(), { once: true });
    });
    await Promise.race([
      (async () => {
        await linked.hello();
        log(`linked ${localName} <-> ${remoteName}`);
        await running.run();
      })(),
      linked.closed.then((error) => {
        throw error;
      }),
      aborted,
    ]);
  } catch (error) {
    if (!signal.aborted || spawnError) {
      log(`stopped: ${(spawnError ?? (error instanceof Error ? error : new Error(String(error)))).message}`);
      code = 1;
    }
  } finally {
    // The mirrors of the remote on this side go at once; the remote's lapse with their lease.
    await (bridge ? bridge.stop() : local.withdraw()).catch(() => undefined);
    remote?.close();
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    const timer = setTimeout(() => child.kill("SIGKILL"), CHILD_KILL_MS);
    await exited;
    clearTimeout(timer);
  }
  return spawnError ? 1 : code;
};

export const main = async (argv: string[]): Promise<number> => {
  const { mode, flags, command } = parseArgs(argv);
  if (mode === "agent") {
    await runAgent(flags);
    return 0;
  }
  if (mode === "run") {
    const controller = new AbortController();
    const abort = (): void => controller.abort();
    process.once("SIGTERM", abort);
    process.once("SIGINT", abort);
    return runBridge(flags, command, controller.signal);
  }
  process.stderr.write(`${USAGE}\n`);
  return 2;
};
