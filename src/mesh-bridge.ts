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
  mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE [--call-timeout-ms MS] (--ssh HOST [--ssh-key KEY] | -- COMMAND...)
  mesh-bridge agent --mesh ROOT --peer NAME`;

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
const transportCommand = (flags: Map<string, string>, command: string[]): string[] => {
  if (command.length > 0) return command;
  const host = required(flags, "ssh");
  const key = flags.get("ssh-key");
  return [
    "ssh", "-T", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
    ...(key ? ["-o", "IdentitiesOnly=yes", "-i", key] : []),
    // The forced command on the remote replaces whatever is asked for here.
    host, "mesh-bridge", "agent",
  ];
};

const store = (root: string): MeshStore => new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS);
const log = (message: string): void => void process.stderr.write(`mesh-bridge: ${message}\n`);

const runAgent = async (flags: Map<string, string>): Promise<void> => {
  const peer = required(flags, "peer");
  const side = new StoreBridgeSide(store(required(flags, "mesh")), peer);
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
  const localName = required(flags, "name");
  const remoteName = required(flags, "remote");
  if (!validBridgeName(localName) || !validBridgeName(remoteName)) throw new Error("Invalid bridge name");
  const callTimeoutMs = Number(flags.get("call-timeout-ms") ?? DEFAULT_CALL_TIMEOUT_MS);
  if (!Number.isSafeInteger(callTimeoutMs) || callTimeoutMs <= 0) throw new Error("--call-timeout-ms must be a positive integer");
  const argv = transportCommand(flags, command);
  const child = spawn(argv[0]!, argv.slice(1), { stdio: ["pipe", "pipe", "inherit"] });
  onChild?.(child.pid);
  const remote = new RemoteBridgeSide(child.stdout, child.stdin, callTimeoutMs);
  // A transport that cannot start (no ssh, not executable) emits error and close, never exit
  // (review round 2, F5): either settles the child, and the error fails the bridge by name.
  let spawnError: Error | undefined;
  const exited = new Promise<void>((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) resolve();
    child.once("exit", () => resolve());
    child.once("close", () => resolve());
    child.once("error", (error) => {
      spawnError ??= new Error(`Bridge transport ${argv[0]} failed: ${error.message}`);
      remote.close(spawnError);
      resolve();
    });
  });
  child.stdin.on("error", () => undefined);
  const bridge = new MeshBridge({
    localName, remoteName,
    local: new StoreBridgeSide(store(required(flags, "mesh")), remoteName),
    remote,
    cursorPath: required(flags, "cursor"),
    stopMs: Math.min(callTimeoutMs, 5_000),
    log,
  });
  const aborted = new Promise<void>((resolve) => {
    if (signal.aborted) resolve();
    else signal.addEventListener("abort", () => resolve(), { once: true });
  });
  let code = 0;
  try {
    await Promise.race([
      (async () => {
        await remote.hello();
        log(`linked ${localName} <-> ${remoteName}`);
        await bridge.run();
      })(),
      remote.closed.then((error) => {
        throw error;
      }),
      aborted,
    ]);
  } catch (error) {
    if (!signal.aborted || spawnError) {
      log(`stopped: ${(spawnError ?? (error instanceof Error ? error : new Error(String(error)))).message}`);
      code = 1;
    }
  }
  // The mirrors of the remote on this side go at once; the remote's lapse with their lease.
  await bridge.stop();
  remote.close();
  if (child.pid !== undefined) child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), CHILD_KILL_MS);
  await exited;
  clearTimeout(timer);
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
