import fs from "node:fs";
import path from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { childProcessEnvironment } from "../core/atomic-write.js";
import { LaunchRecordStore, processPidStart, readBootId, type LaunchRecord } from "./launch-record.js";
import { watchResidentChild, type ResidentChildExit, type ResidentChildLifetime } from "./child-lifetime.js";
import { nativePlacementEnforceAvailable, nativePlacementPathAvailable } from "../agents/placement-config.js";

export const SYSTEMD_RUN = "/usr/bin/systemd-run";
export const SYSTEMCTL = "/usr/bin/systemctl";
const UNIT_POLL_MS = 50;
const UNIT_STOP_TIMEOUT_MS = 15_000;
const UNIT_REGISTER_TIMEOUT_MS = 2_000;

export interface NativeUnitIdentity {
  boot_id: string;
  invocation_id: string;
  main_pid_start: string;
}

export interface NativeUnitStatus {
  unit: string;
  load_state: string;
  active_state: string;
  invocation_id: string | null;
  main_pid: number;
}

export const systemdRunAvailable = (run = SYSTEMD_RUN): boolean => {
  try { return process.platform === "linux" && fs.statSync(run).isFile() && (fs.statSync(run).mode & 0o111) !== 0; }
  catch { return false; }
};

const CONTROL_ENV = ["PATH", "HOME", "LANG", "TZ", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS"] as const;
const UNIT_BASE_ENV = ["PATH", "HOME", "LANG", "TZ", "XDG_RUNTIME_DIR"] as const;
const UNIT_CONTEXT_ENV = [
  "PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME", "PI_FABRIC_ACTOR_SESSION_FILE", "PI_FABRIC_DEPTH",
  "PI_FABRIC_HOST_ID", "PI_FABRIC_IDENTITY_ID", "PI_FABRIC_LINEAGE", "PI_FABRIC_MAIN_AGENT_ID",
  "PI_FABRIC_MESH_HOST_TRANSPORT", "PI_FABRIC_MESH_NATS", "PI_FABRIC_MESH_ROOT", "PI_FABRIC_MESH_TRANSPORT",
  "PI_FABRIC_PARENT_RUN", "PI_FABRIC_PROJECT_ROOT", "PI_FABRIC_RUN_ROOT", "PI_FABRIC_SESSION_ID",
  "PI_FABRIC_WRITE_POLICY", "PI_FABRIC_CLAUDE_BINARY", "PI_FABRIC_VEDA_BINARY", "PI_SESSION_ID",
  "PI_FABRIC_RESIDENT_CONFIG", "PI_FABRIC_RESIDENT_LAUNCHER", "PI_FABRIC_RESIDENT_SPEC_DIGEST",
  "PI_FABRIC_RESIDENT_ATTEMPT", "PI_FABRIC_RESIDENT_GENERATION", "PI_FABRIC_RESIDENT_LAUNCH_ATTESTATION",
] as const;
const SECRET_ENV_NAME = /(?:TOKEN|KEY|SECRET|PASSWORD)/iu;
const UNIT_ENV = new Set<string>([...UNIT_BASE_ENV, ...UNIT_CONTEXT_ENV]);
const NATS_CREDENTIAL_ID = "pi-fabric-mesh-nats";

interface PreparedEnvironment {
  client: NodeJS.ProcessEnv;
  unit: NodeJS.ProcessEnv;
  credentials: Array<{ id: string; source: string }>;
}

const sanitizeNatsEnvironment = (raw: string, credentials: PreparedEnvironment["credentials"]): string => {
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { throw new Error("PI_FABRIC_MESH_NATS is not valid JSON"); }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed) || Object.getPrototypeOf(parsed) !== Object.prototype) {
    throw new Error("PI_FABRIC_MESH_NATS must be an object");
  }
  const value = parsed as Record<string, unknown>;
  const allowed = new Set(["servers", "credentials", "machine", "inboxLanes", "operatorMachine", "rollbackHoldUntil",
    "custodyStream", "livenessStream", "pathBMachines", "tls"]);
  if (Object.keys(value).some(key => !allowed.has(key))) throw new Error("PI_FABRIC_MESH_NATS has an unapproved field");
  if (value.credentials !== undefined) {
    if (typeof value.credentials !== "string" || !path.isAbsolute(value.credentials) || /[\u0000-\u001f\u007f,]/u.test(value.credentials)) {
      throw new Error("NATS credentials must be an absolute credential file path for systemd LoadCredential");
    }
    credentials.push({ id: NATS_CREDENTIAL_ID, source: value.credentials });
    value.credentials = `%d/${NATS_CREDENTIAL_ID}`;
  }
  if (value.tls !== undefined) {
    if (!value.tls || typeof value.tls !== "object" || Array.isArray(value.tls) || Object.getPrototypeOf(value.tls) !== Object.prototype ||
        Object.keys(value.tls).some(key => key !== "ca" && key !== "caFile")) throw new Error("PI_FABRIC_MESH_NATS TLS fields are not approved");
  }
  return JSON.stringify(value);
};

const prepareEnvironment = (environment: NodeJS.ProcessEnv = process.env): PreparedEnvironment => {
  const credentials: PreparedEnvironment["credentials"] = [];
  const unit: NodeJS.ProcessEnv = {};
  for (const key of UNIT_ENV) {
    if (!Object.hasOwn(environment, key) || environment[key] === undefined || SECRET_ENV_NAME.test(key)) continue;
    const value = key === "PI_FABRIC_MESH_NATS"
      ? sanitizeNatsEnvironment(environment[key]!, credentials)
      : environment[key]!;
    unit[key] = value;
  }
  const client: NodeJS.ProcessEnv = {};
  for (const key of CONTROL_ENV) if (environment[key] !== undefined) client[key] = environment[key];
  Object.assign(client, unit);
  return { client, unit, credentials };
};

/** Environment given to systemd-run itself; ambient secrets and credential paths are removed. */
export const systemdRunEnvironment = (environment?: NodeJS.ProcessEnv): NodeJS.ProcessEnv => prepareEnvironment(environment).client;

export const systemdRunArguments = (unit: string, cwd: string, command: readonly string[], environment?: NodeJS.ProcessEnv): string[] => {
  const prepared = prepareEnvironment(environment);
  return ["--user", "--unit", unit, "--wait", "--pipe", "--quiet", "--working-directory", cwd,
    ...prepared.credentials.map(({ id, source }) => `--property=LoadCredential=${id}:${source}`),
    ...Object.entries(prepared.unit).map(([key, value]) => `--setenv=${key}=${value}`),
    "--", ...command];
};

const parseUnitStatus = (unit: string, stdout: string): NativeUnitStatus | undefined => {
  const fields = new Map<string, string>();
  for (const line of stdout.split(/\r?\n/u)) {
    const separator = line.indexOf("=");
    if (separator < 0) continue;
    const key = line.slice(0, separator);
    if (["LoadState", "ActiveState", "InvocationID", "MainPID"].includes(key)) {
      if (fields.has(key)) return undefined;
      fields.set(key, line.slice(separator + 1));
    }
  }
  const loadState = fields.get("LoadState");
  const activeState = fields.get("ActiveState");
  const invocation = fields.get("InvocationID");
  const mainPid = fields.get("MainPID");
  if (!loadState || !activeState || invocation === undefined || !mainPid || !/^\d+$/u.test(mainPid)) return undefined;
  if (invocation !== "" && !/^[0-9a-f]{32}$/u.test(invocation)) return undefined;
  return { unit, load_state: loadState, active_state: activeState, invocation_id: invocation || null, main_pid: Number(mainPid) };
};

export const systemdUnitStatus = (unit: string, systemctl = SYSTEMCTL): NativeUnitStatus | undefined => {
  if (process.platform !== "linux") return undefined;
  let result;
  try {
    result = spawnSync(systemctl, ["--user", "show", unit, "--property=LoadState,ActiveState,InvocationID,MainPID", "--no-pager"], {
      env: { ...childProcessEnvironment(), SYSTEMD_COLORS: "0", SYSTEMD_PAGER: "cat" }, encoding: "utf8", timeout: 1_000,
    });
  } catch { return undefined; }
  if (result.error || result.status !== 0 || typeof result.stdout !== "string") return undefined;
  return parseUnitStatus(unit, result.stdout);
};

const identityFromStatus = (status: NativeUnitStatus): NativeUnitIdentity | undefined => {
  if (status.load_state !== "loaded" || status.active_state === "inactive" || status.active_state === "failed" ||
      !status.invocation_id || status.main_pid < 1) return undefined;
  const boot = readBootId();
  const start = processPidStart(status.main_pid);
  if (!boot || !start) return undefined;
  return { boot_id: boot, invocation_id: status.invocation_id, main_pid_start: start };
};

export const systemdUnitIdentity = (unit: string, systemctl = SYSTEMCTL): NativeUnitIdentity | undefined => {
  const status = systemdUnitStatus(unit, systemctl);
  return status ? identityFromStatus(status) : undefined;
};

const runSystemctlStop = (unit: string, systemctl: string, timeoutMs: number): Promise<boolean> => new Promise(resolve => {
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const finish = (result: boolean): void => {
    if (settled) return;
    settled = true;
    if (timer) clearTimeout(timer);
    resolve(result);
  };
  let child: ChildProcess;
  try {
    child = spawn(systemctl, ["--user", "stop", unit], {
      shell: false, stdio: "ignore", windowsHide: true,
      env: { ...childProcessEnvironment(), SYSTEMD_COLORS: "0", SYSTEMD_PAGER: "cat" },
    });
  } catch { finish(false); return; }
  timer = setTimeout(() => { try { child.kill("SIGKILL"); } catch { /* bounded refusal */ } finish(false); }, timeoutMs);
  child.once("error", () => finish(false));
  child.once("close", (code, signal) => finish(code === 0 && signal === null));
});

export interface NativeSystemdLifetime extends ResidentChildLifetime {
  readonly closed: Promise<void>;
  readonly registered: Promise<void>;
  isAlive(): Promise<boolean>;
  lostContact(): string | undefined;
  stopDebt(): string | undefined;
}

/**
 * Tracks the systemd unit, not its `systemd-run` client. The durable record is
 * terminal only after an exact unit/invocation inactive read-back. Ambiguous
 * launches remain fenced and the close receipt intentionally stays pending.
 */
export const watchNativeSystemdUnit = (
  child: ChildProcess,
  store: LaunchRecordStore,
  record: Pick<LaunchRecord, "native_launch_id" | "generation" | "unit">,
  systemctl = SYSTEMCTL,
  options: { pollMs?: number; registerMs?: number; stopTimeoutMs?: number } = {},
): NativeSystemdLifetime => {
  const client = watchResidentChild(child);
  let spawnFailed = false;
  child.once("error", () => { spawnFailed = true; });
  let unitProved = false;
  let stopRequested = false;
  let stopCommandComplete = false;
  let done = false;
  let clientExit: ResidentChildExit | undefined;
  let debt: string | undefined;
  let stopPromise: Promise<void> | undefined;
  let registeredDone = false;
  const registerDeadline = Date.now() + (options.registerMs ?? UNIT_REGISTER_TIMEOUT_MS);
  let resolveExit!: (value: ResidentChildExit) => void;
  let resolveClosed!: () => void;
  let resolveRegistered!: () => void;
  const exit = new Promise<ResidentChildExit>(resolve => { resolveExit = resolve; });
  const closed = new Promise<void>(resolve => { resolveClosed = resolve; });
  const registered = new Promise<void>(resolve => { resolveRegistered = resolve; });
  const markRegistered = (): void => { if (!registeredDone) { registeredDone = true; resolveRegistered(); } };
  const setUncertain = (): void => {
    try {
      const current = store.get(record.native_launch_id);
      if (current?.generation === record.generation && (current.fence.state === "open" || current.fence.state === "creating")) {
        store.transition(record.native_launch_id, record.generation, "closed-uncertain");
      }
    } catch { /* durable bytes stay fenced on failure */ }
  };
  const finishIfReady = (): void => {
    if (done || !unitProved || !clientExit) return;
    done = true;
    resolveExit(clientExit);
    resolveClosed();
  };
  const reconcile = (): void => {
    if (done) return;
    if (spawnFailed) {
      try {
        const current = store.get(record.native_launch_id);
        if (current && current.generation === record.generation && current.fence.state !== "closed-started" && !current.terminal) {
          store.closeUnstartedBeforeCreate(record.native_launch_id, record.generation);
          unitProved = true;
        }
      } catch { /* an error event proves the native client never submitted a unit; retain if durable close fails */ }
      markRegistered(); finishIfReady(); return;
    }
    const status = systemdUnitStatus(record.unit, systemctl);
    let current = store.get(record.native_launch_id);
    if (status && current?.generation === record.generation) {
      const identity = identityFromStatus(status);
      if (identity && (current.fence.state === "creating" || current.fence.state === "closed-uncertain")) {
        try { current = store.transition(record.native_launch_id, record.generation, "closed-started", identity); }
        catch { /* never infer started without a durable matching identity */ }
      } else if (identity && current.fence.state === "open") {
        try { current = store.transition(record.native_launch_id, record.generation, "closed-uncertain"); } catch { /* retain open fence */ }
      }
      if (status.load_state === "loaded" && status.active_state === "inactive" && status.invocation_id === null &&
          current && current.fence.state !== "closed-started" && !current.terminal) {
        try {
          store.closeUnstartedFromReadback(record.native_launch_id, record.generation, status);
          unitProved = true;
        } catch { /* mismatched/uncertain evidence never releases custody */ }
      } else if ((!stopRequested || stopCommandComplete) && status.load_state === "loaded" && status.active_state === "inactive" &&
          current?.fence.state === "closed-started" && current.invocation_id === status.invocation_id && current.boot_id === readBootId()) {
        try {
          store.terminal(record.native_launch_id, record.generation, { ...status, boot_id: readBootId() });
          unitProved = true;
        } catch { /* exact invocation read-back is required to terminalize */ }
      }
    }
    if (clientExit && !unitProved && Date.now() >= registerDeadline) {
      setUncertain();
      markRegistered();
    }
    if (!registeredDone && (unitProved || (current?.fence.state === "closed-started"))) markRegistered();
    finishIfReady();
  };
  client.exit.then(value => { clientExit = value; reconcile(); });
  const schedule = (): void => {
    if (done) return;
    reconcile();
    if (!done) setTimeout(schedule, options.pollMs ?? UNIT_POLL_MS);
  };
  schedule();

  const stop = async (): Promise<void> => {
    if (done) return;
    if (stopPromise) return stopPromise;
    const operation = (async () => {
      stopRequested = true;
      reconcile();
      const accepted = await runSystemctlStop(record.unit, systemctl, options.stopTimeoutMs ?? UNIT_STOP_TIMEOUT_MS);
      if (!accepted) { stopRequested = false; throw new Error(`systemd stop was not confirmed for ${record.unit}`); }
      stopCommandComplete = true;
      const deadline = Date.now() + (options.stopTimeoutMs ?? UNIT_STOP_TIMEOUT_MS);
      while (!done && Date.now() < deadline) {
        reconcile();
        if (!done) await new Promise(resolve => setTimeout(resolve, options.pollMs ?? UNIT_POLL_MS));
      }
      if (!unitProved) throw new Error(`systemd read-back did not prove inactive invocation for ${record.unit}`);
      if (!client.exited) await client.stop();
      reconcile();
      if (!done) throw new Error(`systemd-run client did not provide a native close receipt for ${record.unit}`);
    })();
    stopPromise = operation;
    try { await operation; debt = undefined; }
    catch (error) { stopPromise = undefined; debt = error instanceof Error ? error.message : String(error); throw error; }
  };
  return {
    exit,
    get exited() { return done; },
    closed,
    registered,
    isAlive: async () => !done,
    lostContact: () => debt,
    stopDebt: () => debt,
    stop,
  };
};

/** Reconcile all pending launch records before the resident host admits actors. */
export const reconcileNativeLaunchRecords = async (store: LaunchRecordStore, systemctl = SYSTEMCTL): Promise<LaunchRecord[]> =>
  store.withAdmissionLock(() => {
    const reconciled: LaunchRecord[] = [];
    for (const initial of store.census()) {
      let record = store.get(initial.native_launch_id);
      if (!record || record.terminal || record.fence.state === "closed-unstarted") continue;
      const status = systemdUnitStatus(record.unit, systemctl);
      if (record.fence.state === "open") {
        if (!status || status.load_state === "not-found" || (status.load_state === "loaded" && status.active_state === "inactive" && status.invocation_id === null)) {
          record = store.closeUnstartedBeforeCreate(record.native_launch_id, record.generation);
        } else if (status) {
          record = store.transition(record.native_launch_id, record.generation, "closed-uncertain");
        }
      } else if (status) {
        const identity = identityFromStatus(status);
        if (identity && (record.fence.state === "creating" || record.fence.state === "closed-uncertain")) {
          record = store.transition(record.native_launch_id, record.generation, "closed-started", identity);
        } else if (status.load_state === "loaded" && status.active_state === "inactive" && status.invocation_id === null &&
            (record.fence.state === "creating" || record.fence.state === "closed-uncertain")) {
          record = store.closeUnstartedFromReadback(record.native_launch_id, record.generation, status);
        } else if (status.load_state === "loaded" && status.active_state === "inactive" && record.fence.state === "closed-started" &&
            record.invocation_id === status.invocation_id && record.boot_id === readBootId()) {
          record = store.terminal(record.native_launch_id, record.generation, { ...status, boot_id: readBootId() });
        } else if (record.fence.state === "creating") {
          record = store.transition(record.native_launch_id, record.generation, "closed-uncertain");
        }
      } else if (record.fence.state === "creating") {
        record = store.transition(record.native_launch_id, record.generation, "closed-uncertain");
      }
      reconciled.push(record);
    }
    return reconciled;
  });

/** Verify one crash-window record using complete unit state, not PID absence. */
export const verifyNativeLaunchRecord = async (store: LaunchRecordStore, nativeLaunchId: string, systemctl = SYSTEMCTL) => {
  const record = store.get(nativeLaunchId);
  if (!record) throw new Error("Unknown launch record");
  return (await reconcileNativeLaunchRecords(store, systemctl)).find(value => value.native_launch_id === nativeLaunchId) ?? record;
};

/** Native systemd paths are available for the existing shadow path; no capability is required. */
export const nativeResidentPlacementAvailable = (contextCommand: readonly string[] | undefined, run = SYSTEMD_RUN, recordRoot?: string): boolean =>
  nativePlacementPathAvailable(contextCommand, run, recordRoot);

/** Enforce additionally requires the context helper's verified-caller capability. */
export const nativeResidentPlacementEnforceAvailable = (contextCommand: readonly string[] | undefined, run = SYSTEMD_RUN, recordRoot?: string): boolean =>
  nativePlacementEnforceAvailable(contextCommand, run, recordRoot);
