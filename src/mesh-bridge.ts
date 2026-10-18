import { childProcessEnvironment } from "./core/atomic-write.js";
// mesh-bridge (smarty-dev#2004): link this host's Fabric mesh to one remote host's mesh.
//
//   mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE --ssh HOST [--ssh-key KEY]
//   mesh-bridge run ... -- COMMAND [ARGS...]      (any stdio transport, for tests)
//   mesh-bridge agent --mesh ROOT --peer NAME     (the remote end; the ssh forced command)
//   mesh-bridge handoff --mesh ROOT --session ID --link NAME [--operator NAME]
//                                                 (on the hub: hand a moved Main's dead native identity to one link,
//                                                  smarty-dev#9074; also handoff-revoke and handoff-list)
//
// The remote pins the agent in authorized_keys, so the key can run nothing else:
//   command="node /opt/pi-fabric/bin/mesh-bridge agent --mesh /home/u/proj/.pi/fabric/mesh --peer dev1",restrict ssh-ed25519 ...
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readMeshStateMovedMarker } from "./mesh/backend-fence.js";
import { BRIDGE_STALL_ALARM_TOPIC, DEFAULT_CALL_TIMEOUT_MS, DEFAULT_FACTORY_RATE, MeshBridge, RemoteBridgeSide,
  serveBridgeAgent, StoreBridgeSide, validBridgeName, type BridgeStallAlarm } from "./mesh/bridge.js";
import { BridgeRelayUnavailableError, startBridgeRelay } from "./mesh/bridge-relay.js";
import { BridgeHandoffRefusedError, BridgeHandoffUsageError, handOffNativeIdentity, handoffRootId, listHandoffs, revokeHandoff } from "./mesh/bridge-handoff.js";
import { MeshStore, meshAlarmRecipient, type MeshStoreOptions } from "./mesh/store.js";
import { systemdWatchdogNotify } from "./mesh/systemd-watchdog.js";

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts).
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;

// The handoff commands' synopses, also returned in their JSON usage result.
const HANDOFF_USAGE: ReadonlyMap<string, string> = new Map([
  ["handoff", "mesh-bridge handoff --mesh ROOT --session ID --link NAME [--operator NAME] [--lock-protocol 1|2]"],
  ["handoff-revoke", "mesh-bridge handoff-revoke --mesh ROOT --session ID [--lock-protocol 1|2]"],
  ["handoff-list", "mesh-bridge handoff-list --mesh ROOT [--lock-protocol 1|2]"],
]);

const USAGE = `usage:
  mesh-bridge run --mesh ROOT --name LOCAL --remote NAME --cursor FILE [--lock-protocol 1|2] [--max-state-bytes BYTES] [--call-timeout-ms MS] [--log-level info|debug] [--factory-rate PER_MINUTE] [--factory-burst N] [--factory-bytes BYTES_PER_MINUTE] [--factory-byte-burst BYTES] (--ssh HOST [--ssh-key KEY] [--ssh-port N] [--ssh-known-hosts FILE] | -- COMMAND...)
  mesh-bridge agent --mesh ROOT --peer NAME [--lock-protocol 1|2] [--max-state-bytes BYTES] [--log-level info|debug]
${[...HANDOFF_USAGE.values()].map((line) => `  ${line}`).join("\n")}`;

/**
 * How a command reports an argument the operator must fix. run and agent throw it; the bin prints
 * it and exits 2. The handoff commands throw BridgeHandoffUsageError, which their dispatcher turns
 * into the documented JSON usage result with exit 2 (smarty-dev#9074).
 */
type UsageFault = (message: string) => Error;
const cliUsage: UsageFault = (message) => new Error(`${message}\n${USAGE}`);
const handoffUsage: UsageFault = (message) => new BridgeHandoffUsageError(message);

const parseArgs = (argv: string[], usage: UsageFault = cliUsage): { mode: string; flags: Map<string, string>; command: string[] } => {
  const [mode = "", ...rest] = argv;
  const flags = new Map<string, string>();
  const split = rest.indexOf("--");
  const own = split >= 0 ? rest.slice(0, split) : rest;
  for (let index = 0; index < own.length; index += 2) {
    const flag = own[index]!;
    const value = own[index + 1];
    if (!flag.startsWith("--") || value === undefined) throw usage(`Bad argument: ${flag}`);
    flags.set(flag.slice(2), value);
  }
  return { mode, flags, command: split >= 0 ? rest.slice(split + 1) : [] };
};

const required = (flags: Map<string, string>, name: string, usage: UsageFault = cliUsage): string => {
  const value = flags.get(name);
  if (!value) throw usage(`--${name} is required`);
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

export const openBridgeStore = (
  root: string,
  flags: Map<string, string>,
  options: Pick<MeshStoreOptions, "stateBackend"> = {},
  usage: UsageFault = (message) => new Error(message),
): MeshStore => {
  const selected = flags.get("lock-protocol") ?? "1";
  if (selected !== "1" && selected !== "2" && selected !== "3") throw usage("--lock-protocol must be 1, 2 or 3");
  const capacity = flags.get("max-state-bytes");
  const maxStateBytes = capacity === undefined ? undefined : Number(capacity);
  if (maxStateBytes !== undefined && (!Number.isSafeInteger(maxStateBytes) || maxStateBytes < MAX_EVENT_BYTES * 2)) {
    throw usage(`--max-state-bytes must be a safe integer of at least ${MAX_EVENT_BYTES * 2}`);
  }
  return new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, {
    lockProtocol: selected === "1" ? 1 : selected === "2" ? 2 : 3,
    ...(maxStateBytes === undefined ? {} : { maxStateBytes }),
    ...options,
  });
};
const log = (message: string): void => void process.stderr.write(`mesh-bridge: ${message}\n`);

const runAgent = async (flags: Map<string, string>): Promise<void> => {
  const peer = required(flags, "peer");
  const side = new StoreBridgeSide(openBridgeStore(required(flags, "mesh"), flags), peer, Date.now, true);
  const logLevel = flags.get("log-level") ?? "info";
  if (logLevel !== "info" && logLevel !== "debug") throw new Error("--log-level must be info or debug");
  await side.store.prepareLock();
  await serveBridgeAgent(side, process.stdin, process.stdout, logLevel === "debug" ? log : undefined);
};

/** Grace period after clean stdin EOF before terminating a stuck transport child. */
const CHILD_STOP_GRACE_MS = 2_000;
const CHILD_TERM_GRACE_MS = 500;
const BRIDGE_STOP_MS = 5_000;

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
  const logLevel = flags.get("log-level") ?? "info";
  if (logLevel !== "info" && logLevel !== "debug") throw new Error("--log-level must be info or debug");
  // The per-link bound on events crossing only as addressed to the hub factory (default 30/min,
  // burst 60, and 512 KiB/min, burst 1 MiB).
  const factoryRate = {
    perMinute: Number(flags.get("factory-rate") ?? DEFAULT_FACTORY_RATE.perMinute),
    burst: Number(flags.get("factory-burst") ?? DEFAULT_FACTORY_RATE.burst),
    bytesPerMinute: Number(flags.get("factory-bytes") ?? DEFAULT_FACTORY_RATE.bytesPerMinute),
    byteBurst: Number(flags.get("factory-byte-burst") ?? DEFAULT_FACTORY_RATE.byteBurst),
  };
  if (!(Number.isFinite(factoryRate.perMinute) && factoryRate.perMinute > 0)) throw new Error("--factory-rate must be a positive number of events a minute");
  if (!(Number.isSafeInteger(factoryRate.burst) && factoryRate.burst > 0)) throw new Error("--factory-burst must be a positive integer");
  if (!(Number.isSafeInteger(factoryRate.bytesPerMinute) && factoryRate.bytesPerMinute > 0)) throw new Error("--factory-bytes must be a positive integer of bytes a minute");
  if (!(Number.isSafeInteger(factoryRate.byteBurst) && factoryRate.byteBurst > 0)) throw new Error("--factory-byte-burst must be a positive integer");
  const local = new StoreBridgeSide(openBridgeStore(required(flags, "mesh"), flags), remoteName, Date.now, true);
  const argv = transportCommand(flags, command);
  if (signal.aborted) {
    local.store.closeState();
    return 0;
  }
  // Prepare before starting the link, not inside a short registry/mesh try. Readiness is
  // immutable for this store; an unknown identity still permits conservative publication.
  let removePrepareAbort: (() => void) | undefined;
  const abortedDuringPrepare = new Promise<void>((resolve) => {
    if (signal.aborted) { resolve(); return; }
    const onAbort = (): void => resolve();
    signal.addEventListener("abort", onAbort, { once: true });
    removePrepareAbort = () => signal.removeEventListener("abort", onAbort);
  });
  let prepared: boolean;
  try {
    prepared = await Promise.race([local.store.prepareLock().then(() => true), abortedDuringPrepare.then(() => false)]);
  } catch (error) {
    local.store.closeState();
    throw error;
  } finally {
    removePrepareAbort?.();
  }
  if (!prepared || signal.aborted) {
    local.store.closeState();
    return 0;
  }

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
  const childAlive = (): boolean => child.exitCode === null && child.signalCode === null;
  const waitForExit = async (ms: number): Promise<void> => {
    if (!childAlive()) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([exited, new Promise<void>((resolve) => { timer = setTimeout(resolve, ms); })]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  };
  let transportStop: Promise<void> | undefined;
  const stopTransport = (): Promise<void> => {
    if (transportStop) return transportStop;
    const stopError = new Error("Bridge stop requested");
    if (remote?.closeGracefully) remote.closeGracefully(stopError);
    else if (remote) remote.close?.(stopError);
    else child.stdin.end();
    transportStop = (async () => {
      await waitForExit(CHILD_STOP_GRACE_MS);
      if (childAlive()) {
        child.kill("SIGTERM");
        await waitForExit(CHILD_TERM_GRACE_MS);
        if (childAlive()) child.kill("SIGKILL");
        await waitForExit(CHILD_TERM_GRACE_MS);
      }
    })();
    return transportStop;
  };
  child.stdin.on("error", () => undefined);
  let bridge: MeshBridge | undefined;
  let relay: Awaited<ReturnType<typeof startBridgeRelay>> | undefined;
  let starting: Promise<void> | undefined;
  let stopDeadline: number | undefined;
  let abortListener: (() => void) | undefined;
  let code = 0;
  try {
    onChild?.(child.pid);
    remote = new RemoteBridgeSide(child.stdout, child.stdin, callTimeoutMs);
    if (spawnError) remote.close(spawnError);
    const linked = remote;
    bridge = new MeshBridge({
      localName, remoteName, local, remote: linked, cursorPath,
      relay: async (destinationRemote, event) => {
        if (!relay || signal.aborted) throw new BridgeRelayUnavailableError();
        await relay.send(destinationRemote, event);
      },
      stopMs: Math.min(callTimeoutMs, 5_000),
      log,
      notifyWatchdog: systemdWatchdogNotify,
      publishStallAlarm: async (alarm: BridgeStallAlarm) => {
        const to = meshAlarmRecipient();
        if (!to) {
          log("stall alarm has no configured mesh alarm session recipient");
          return;
        }
        await local.store.publish({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall",
          from: { id: `bridge:${localName}`, name: "mesh-bridge", kind: "main" }, to,
          text: alarm.reason, data: alarm, dedupeKey: `bridge-stall:${localName}:${remoteName}:${alarm.stallId}` });
      },
      factoryRate,
      ...(logLevel === "debug" ? { debug: (message: string) => log(`debug: ${message}`) } : {}),
    });
    const running = bridge;
    let resolveAborted!: () => void;
    const aborted = new Promise<void>((resolve) => { resolveAborted = resolve; });
    const onAbort = (): void => {
      stopDeadline ??= Date.now() + BRIDGE_STOP_MS;
      // Fence sync/forwarding and end remote stdin now; waiting for the startup or pass promise
      // before closing the transport could exceed systemd's stop budget on a silent peer.
      void running.stop();
      void stopTransport();
      resolveAborted();
    };
    abortListener = onAbort;
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
    // Start/sync the link before binding a delivery server. The registered script is the
    // exact canonical bin entry in THIS package generation, never argv[1] or a current link.
    starting = (async () => {
      await linked.hello();
      if (signal.aborted) return;
      await running.start();
      if (signal.aborted) return;
      if (process.platform === "linux") {
        try {
          const startedRelay = await startBridgeRelay({ localName, remoteName, meshRoot: local.store.root,
            bridgeScriptPath: fs.realpathSync(fileURLToPath(new URL("../bin/mesh-bridge", import.meta.url))),
            deliver: (sourceRemote, event) => running.deliverRelay(sourceRemote, event) });
          if (signal.aborted) {
            await startedRelay.close().catch(() => undefined);
            return;
          }
          relay = startedRelay;
        } catch {
          // Unsafe/missing authentication or socket authority is a relay-only refusal.
          // Do not leak exception details; hub-local bridge traffic remains supported.
          log("relay unavailable; hub-local link remains active");
        }
      }
      if (signal.aborted) return;
      log(`linked ${localName} <-> ${remoteName}`);
    })();
    await Promise.race([
      starting.then(async () => { if (!signal.aborted) await running.run(); }),
      linked.closed.then((error) => {
        throw error;
      }),
      aborted,
    ]);
  } catch (error) {
    if (!signal.aborted) {
      log(`stopped: ${(spawnError ?? (error instanceof Error ? error : new Error(String(error)))).message}`);
      code = 1;
    }
  } finally {
    abortListener && signal.removeEventListener("abort", abortListener);
    stopDeadline ??= Date.now() + BRIDGE_STOP_MS;
    const within = async (work: Promise<unknown>): Promise<void> => {
      const remaining = stopDeadline! - Date.now();
      if (remaining <= 0) {
        void work.catch(() => undefined);
        return;
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([work.catch(() => undefined), new Promise<void>((resolve) => {
          timer = setTimeout(resolve, remaining);
          timer.unref?.();
        })]);
      } finally { if (timer !== undefined) clearTimeout(timer); }
    };
    // On SIGTERM/SIGINT this joins the already-started, idempotent fence and graceful close.
    // Startup, relay, bridge and child cleanup all share the same five-second stop budget.
    if (starting) await within(starting.catch(() => undefined));
    const relayClose = relay?.close().catch(() => undefined) ?? Promise.resolve();
    relay = undefined;
    const bridgeStop = bridge?.stop() ?? Promise.resolve();
    const childStop = stopTransport();
    await within(Promise.all([relayClose, bridgeStop, childStop]));
    local.store.closeState();
  }
  return signal.aborted ? 0 : spawnError ? 1 : code;
};

/**
 * The hub's handoff commands (smarty-dev#9074), for move-main.sh. Run them on the hub with the hub
 * bridge's mesh root, --lock-protocol and environment. One JSON object on stdout; exit 0 done, 1
 * refused (nothing written), 2 usage.
 */
const runHandoff = async (mode: string, flags: Map<string, string>, out: (value: unknown) => void): Promise<number> => {
  const root = path.resolve(required(flags, "mesh", handoffUsage));
  // A wrong path must fail, never become a new empty mesh holding a handoff.
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory() ||
    !["state.json", "state.db"].some((name) => fs.existsSync(path.join(root, name)))) {
    throw new BridgeHandoffUsageError(`Not a Fabric mesh directory: ${root}`);
  }
  // A root switched to SQLite is written there, whatever this environment selects.
  const store = openBridgeStore(root, flags, readMeshStateMovedMarker(root) !== undefined ? { stateBackend: "sqlite" } : {}, handoffUsage);
  try {
    if (mode === "handoff-list") {
      out({ ok: true, action: mode, handoffs: listHandoffs(store) });
      return 0;
    }
    const id = handoffRootId(required(flags, "session", handoffUsage));
    if (mode === "handoff-revoke") {
      const removed = await revokeHandoff(store, id);
      out({ ok: true, action: mode, id, removed: removed ?? null,
        message: removed ? `handoff of native identity ${id} revoked` : `no handoff of native identity ${id}` });
      return 0;
    }
    const link = required(flags, "link", handoffUsage);
    try {
      const result = await handOffNativeIdentity(store, {
        session: id, link, operator: flags.get("operator") ?? process.env.SUDO_USER ?? os.userInfo().username,
      });
      out({ ok: true, action: mode, id, link,
        message: `native identity ${id} handed off to link ${link} at ${new Date(result.record.handedOffAt).toISOString()} by ${result.record.operator}`,
        ...result });
      return 0;
    } catch (error) {
      if (!(error instanceof BridgeHandoffRefusedError)) throw error;
      out({ ok: false, action: mode, id, link, message: `refused: native identity ${id} not handed off to link ${link}: ${error.reason}`,
        refused: error.reason, evidence: error.evidence });
      return 1;
    }
  } finally {
    store.closeState();
  }
};

export const main = async (
  argv: string[],
  out: (value: unknown) => void = (value) => void process.stdout.write(`${JSON.stringify(value, null, 2)}\n`),
): Promise<number> => {
  const synopsis = HANDOFF_USAGE.get(argv[0] ?? "");
  if (synopsis !== undefined) {
    const mode = argv[0]!;
    try {
      // A missing or malformed flag is a usage error too: one JSON object, exit 2, nothing written.
      return await runHandoff(mode, parseArgs(argv, handoffUsage).flags, out);
    } catch (error) {
      if (!(error instanceof BridgeHandoffUsageError)) throw error;
      out({ ok: false, action: mode, message: `usage: ${error.message}`, usage: synopsis });
      return 2;
    }
  }
  const { mode, flags, command } = parseArgs(argv);
  if (mode === "agent") {
    await runAgent(flags);
    return 0;
  }
  if (mode === "run") {
    const controller = new AbortController();
    let signals = 0;
    let transportPid: number | undefined;
    const stop = (): void => {
      if (++signals === 1) {
        controller.abort();
        return;
      }
      if (transportPid !== undefined) {
        try { process.kill(transportPid, "SIGKILL"); } catch { /* Child already exited. */ }
      }
      process.exit(0);
    };
    process.on("SIGTERM", stop);
    process.on("SIGINT", stop);
    try {
      return await runBridge(flags, command, controller.signal, (pid) => { transportPid = pid; });
    } finally {
      // Keep the two-signal contract armed until process exit after a requested stop; cleanup may
      // finish before systemd or an operator delivers a second signal.
      if (!controller.signal.aborted) {
        process.off("SIGTERM", stop);
        process.off("SIGINT", stop);
      }
    }
  }
  process.stderr.write(`${USAGE}\n`);
  return 2;
};
