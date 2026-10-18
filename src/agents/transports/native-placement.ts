import { spawn } from "node:child_process";
import type { AgentTransportHandle, AgentTransportLaunch } from "../types.js";
import { LaunchRecordStore, makeLaunchRecord } from "../../residency/launch-record.js";
import { systemdRunArguments, systemdRunAvailable, systemdUnitIdentity } from "../../residency/native-launch.js";

/** Native local placement launch for resident-host descendants. The systemd unit is
 * the admission boundary; a worker argument or environment value cannot name its parent. */
export const launchNativePlacementWorker = async (
  request: AgentTransportLaunch,
  workerPath: string,
  workerArguments: readonly string[],
  cwd: string,
  environment: NodeJS.ProcessEnv,
): Promise<AgentTransportHandle> => {
  const carrier = request.placementGrant;
  if (!carrier?.record || carrier.record.role !== "actor-activation") throw new Error("Native placement requires an actor-activation record binding");
  if (!systemdRunAvailable()) throw new Error("Native placement requires systemd-run");
  carrier.revalidate();
  const decision = carrier.decision;
  const metadata = carrier.record;
  const record = makeLaunchRecord({
    nativeLaunchId: decision.grant.launchId, launchSpecId: decision.grant.launchSpecId, generation: decision.generation,
    fenceToken: decision.fence, orgId: metadata.orgId, workId: metadata.workId, parentNativeLaunchId: metadata.parentNativeLaunchId,
    role: "actor-activation", profile: metadata.profile, sourcePin: metadata.sourcePin, memoryProfile: metadata.memoryProfile,
    origin: metadata.origin,
  });
  const store = new LaunchRecordStore();
  try { await store.admit(record, true); }
  catch (error) {
    try { store.transition(record.native_launch_id, record.generation, "closed-unstarted"); } catch { /* preserve the durable open record */ }
    throw error;
  }
  const child = spawn("/usr/bin/systemd-run", systemdRunArguments(record.unit, cwd, [workerPath, ...workerArguments]), {
    cwd, env: environment, stdio: ["ignore", "pipe", "pipe"], detached: true,
  });
  let stderr = "";
  child.stdout?.on("data", () => undefined);
  child.stderr?.on("data", chunk => { stderr = `${stderr}${String(chunk)}`.slice(-4_000); });
  const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
  const registered = (async (): Promise<void> => {
    const deadline = Date.now() + 2_000;
    while (child.exitCode === null && child.signalCode === null && Date.now() < deadline) {
      const identity = systemdUnitIdentity(record.unit);
      if (identity) { store.transition(record.native_launch_id, record.generation, "closed-started", identity); return; }
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    try { store.transition(record.native_launch_id, record.generation, "closed-uncertain"); } catch { /* retain the last complete record */ }
  })();
  void registered;
  const stop = async (): Promise<void> => {
    try { spawn("/usr/bin/systemctl", ["--user", "stop", record.unit], { stdio: "ignore" }); } catch { /* close remains uncertain */ }
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    await closed;
  };
  void closed.then(() => {
    try { const current = store.get(record.native_launch_id); if (current?.fence.state === "closed-started" && !current.terminal) store.terminal(record.native_launch_id, record.generation); }
    catch { /* terminal evidence is conservative; retain the record */ }
  });
  return {
    kind: "process", ...(child.pid === undefined ? {} : { sessionId: String(child.pid) }), controls: false, relaunchable: false,
    isAlive: async () => child.exitCode === null && child.signalCode === null,
    waitForClose: () => closed,
    closed, primaryExit: closed, stop,
    readStderr: () => stderr,
  };
};

