// mesh-bridge (smarty-dev#2004): link this host's Fabric mesh to one remote host's mesh.
//
//   mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE --ssh HOST [--ssh-key KEY]
//   mesh-bridge run ... -- COMMAND [ARGS...]      (any stdio transport, for tests)
//   mesh-bridge agent --mesh ROOT --peer NAME     (the remote end; the ssh forced command)
//
// The remote pins the agent in authorized_keys, so the key can run nothing else:
//   command="node /opt/pi-fabric/bin/mesh-bridge agent --mesh /home/u/proj/.pi/fabric/mesh --peer dev1",restrict ssh-ed25519 ...
import { spawn } from "node:child_process";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, validBridgeName } from "./mesh/bridge.js";
import { MeshStore } from "./mesh/store.js";

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts).
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;

const USAGE = `usage:
  mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE (--ssh HOST [--ssh-key KEY] | -- COMMAND...)
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

const runBridge = async (flags: Map<string, string>, command: string[]): Promise<number> => {
  const localName = required(flags, "name");
  const remoteName = required(flags, "remote");
  if (!validBridgeName(localName) || !validBridgeName(remoteName)) throw new Error("Invalid bridge name");
  const argv = transportCommand(flags, command);
  const child = spawn(argv[0]!, argv.slice(1), { stdio: ["pipe", "pipe", "inherit"] });
  const remote = new RemoteBridgeSide(child.stdout, child.stdin);
  const bridge = new MeshBridge({
    localName, remoteName,
    local: new StoreBridgeSide(store(required(flags, "mesh")), remoteName),
    remote,
    cursorPath: required(flags, "cursor"),
    log,
  });
  let stopping = false;
  const stop = (): void => {
    if (stopping) return;
    stopping = true;
    void bridge.stop().finally(() => child.kill("SIGTERM"));
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  try {
    await remote.hello();
    log(`linked ${localName} <-> ${remoteName}`);
    await Promise.race([
      bridge.run(),
      remote.closed.then((error) => {
        throw error;
      }),
    ]);
    return 0;
  } catch (error) {
    if (stopping) return 0;
    log(`stopped: ${error instanceof Error ? error.message : String(error)}`);
    // The mirrors of the remote on this side go at once; the remote's lapse with their lease.
    await bridge.stop();
    child.kill("SIGTERM");
    return 1;
  }
};

export const main = async (argv: string[]): Promise<number> => {
  const { mode, flags, command } = parseArgs(argv);
  if (mode === "agent") {
    await runAgent(flags);
    return 0;
  }
  if (mode === "run") return runBridge(flags, command);
  process.stderr.write(`${USAGE}\n`);
  return 2;
};
