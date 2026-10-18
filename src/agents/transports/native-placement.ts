import { spawn } from "node:child_process";
import type { AgentTransportHandle, AgentTransportLaunch } from "../types.js";
import { LaunchRecordStore, makeLaunchRecord } from "../../residency/launch-record.js";
import {
  systemdRunArguments, systemdRunAvailable, systemdRunEnvironment, watchNativeSystemdUnit, SYSTEMD_RUN, SYSTEMCTL,
} from "../../residency/native-launch.js";

export interface NativePlacementExecutables {
  systemdRun?: string;
  systemctl?: string;
  lifecycle?: { pollMs?: number; registerMs?: number; stopTimeoutMs?: number };
}

/** Native local placement launch for resident-host descendants. The systemd unit is
 * the admission boundary; a worker argument or environment value cannot name its parent. */
export const launchNativePlacementWorker = async (
  request: AgentTransportLaunch,
  workerPath: string,
  workerArguments: readonly string[],
  cwd: string,
  environment: NodeJS.ProcessEnv,
  executables: NativePlacementExecutables = {},
): Promise<AgentTransportHandle> => {
  const carrier = request.placementGrant;
  if (!carrier?.record || carrier.record.role !== "actor-activation") throw new Error("Native placement requires an actor-activation record binding");
  const systemdRun = executables.systemdRun ?? SYSTEMD_RUN;
  const systemctl = executables.systemctl ?? SYSTEMCTL;
  if (!systemdRunAvailable(systemdRun)) throw new Error("Native placement requires systemd-run");
  carrier.revalidate();
  const decision = carrier.decision;
  const metadata = carrier.record;
  const record = makeLaunchRecord({
    nativeLaunchId: decision.grant.launchId, launchSpecId: decision.grant.launchSpecId, generation: decision.generation,
    fenceToken: decision.fence, orgId: metadata.orgId, workId: metadata.workId, parentNativeLaunchId: metadata.parentNativeLaunchId,
    role: "actor-activation", profile: metadata.profile, sourcePin: metadata.sourcePin, memoryProfile: metadata.memoryProfile,
    origin: metadata.origin,
  });
  const store = new LaunchRecordStore(metadata.recordRoot);
  try { await store.admit(record, true); }
  catch (error) {
    try { store.closeUnstartedBeforeCreate(record.native_launch_id, record.generation); } catch { /* preserve the durable fence */ }
    throw error;
  }
  let args: string[];
  let childEnvironment: NodeJS.ProcessEnv;
  try {
    args = systemdRunArguments(record.unit, cwd, [workerPath, ...workerArguments], environment);
    childEnvironment = systemdRunEnvironment(environment);
  } catch (error) {
    try { store.closeUnstartedBeforeCreate(record.native_launch_id, record.generation); } catch { /* preserve the durable fence */ }
    throw error;
  }
  let child;
  try {
    child = spawn(systemdRun, args, { cwd, env: childEnvironment, stdio: ["ignore", "pipe", "pipe"], detached: true });
  } catch (error) {
    try { store.closeUnstartedBeforeCreate(record.native_launch_id, record.generation); } catch { /* preserve the durable fence */ }
    throw error;
  }
  let stderr = "";
  child.stdout?.on("data", () => undefined);
  child.stderr?.on("data", chunk => { stderr = `${stderr}${String(chunk)}`.slice(-4_000); });
  const native = watchNativeSystemdUnit(child, store, record, systemctl, executables.lifecycle);
  return {
    kind: "process", ...(child.pid === undefined ? {} : { sessionId: String(child.pid) }), controls: false, relaunchable: false,
    isAlive: native.isAlive,
    lostContact: native.lostContact,
    stopDebt: native.stopDebt,
    waitForClose: () => native.closed,
    closed: native.closed,
    primaryExit: native.exit.then(() => undefined),
    stop: native.stop,
    readStderr: () => stderr,
  };
};
