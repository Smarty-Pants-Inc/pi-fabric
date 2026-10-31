import path from "node:path";
import { kubectl, type KubeAccess, type PodView as DrainPodView } from "../seat-drain/kube.js";

// Bridge-only view: leave the separately pinned DRAIN's module and exported API unchanged.
export { fixedKubectl, kubectl, verifyKubeconfig } from "../seat-drain/kube.js";
export type { KubeAccess } from "../seat-drain/kube.js";
export interface PodView extends DrainPodView {
  piRestartCount: number | null;
  piStartedAt: string | null;
  piContainerId: string | null;
  /** Concurrent containers with writable mounts of pi's PVCs. */
  pvcWriters: string[];
}
const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const json = (text: string): unknown => { try { return JSON.parse(text); } catch { return undefined; } };

/** One API snapshot for the bridge's rendered path, container lifetime and writable-PVC evidence. */
export const readPod = async (access: KubeAccess, name: string, timeoutMs: number): Promise<PodView | undefined> => {
  const result = await kubectl(access, ["-n", access.namespace, "get", "pod", name, "-o", "json"], timeoutMs);
  const pod = result.code === 0 ? json(result.stdout) : undefined;
  if (!object(pod) || !object(pod.metadata) || typeof pod.metadata.uid !== "string") return undefined;
  const spec = object(pod.spec) && Array.isArray(pod.spec.containers) ? pod.spec.containers : [];
  const pi = spec.find(container => object(container) && container.name === "pi") as Record<string, unknown> | undefined;
  const env = Array.isArray(pi?.env) ? pi.env as Array<Record<string, unknown>> : [];
  const mesh = env.find(entry => entry.name === "PI_FABRIC_MESH_ROOT");
  const meshRoot = typeof mesh?.value === "string" && path.isAbsolute(mesh.value) && /^[A-Za-z0-9/._-]+$/.test(mesh.value) ? mesh.value : null;
  const statuses = object(pod.status) && Array.isArray(pod.status.containerStatuses) ? pod.status.containerStatuses : [];
  const status = statuses.find(entry => object(entry) && entry.name === "pi") as Record<string, unknown> | undefined;
  const state = object(status?.state) ? status.state : {};
  const terminated = object(state.terminated) && Number.isSafeInteger(state.terminated.exitCode) ? { exitCode: state.terminated.exitCode as number } : null;
  const nodeName = object(pod.spec) && typeof pod.spec.nodeName === "string" ? pod.spec.nodeName : null;
  const piArgs = Array.isArray(pi?.args) && pi.args.every(arg => typeof arg === "string") ? [...pi.args as string[]] : null;
  const natsEnv = env.find(entry => entry.name === "PI_FABRIC_MESH_NATS");
  const nats = typeof natsEnv?.value === "string" ? json(natsEnv.value) : undefined;
  const natsMachine = object(nats) && typeof nats.machine === "string" && /^[a-z0-9][a-z0-9-]{0,31}$/.test(nats.machine) ? nats.machine : null;
  const volumes = object(pod.spec) && Array.isArray(pod.spec.volumes) ? pod.spec.volumes.filter(object) : [];
  const pvcs = new Set(volumes.filter(volume => object(volume.persistentVolumeClaim)).map(volume => volume.name));
  const mounts = (container: Record<string, unknown>) => Array.isArray(container.volumeMounts) ? container.volumeMounts.filter(object) : [];
  const piPvcs = new Set(pi ? mounts(pi).filter(mount => pvcs.has(mount.name) && mount.readOnly !== true).map(mount => mount.name) : []);
  const sidecars = object(pod.spec) && Array.isArray(pod.spec.initContainers)
    ? pod.spec.initContainers.filter(container => object(container) && container.restartPolicy === "Always") : [];
  const ephemeral = object(pod.spec) && Array.isArray(pod.spec.ephemeralContainers) ? pod.spec.ephemeralContainers : [];
  const pvcWriters = [...spec, ...sidecars, ...ephemeral].filter(object).filter(container =>
    mounts(container).some(mount => piPvcs.has(mount.name) && mount.readOnly !== true)).map(container => String(container.name)).sort();
  return { uid: pod.metadata.uid, nodeName, piRunning: object(state.running), piTerminated: terminated, meshRoot, piArgs, natsMachine,
    piRestartCount: Number.isSafeInteger(status?.restartCount) && (status!.restartCount as number) >= 0 ? status!.restartCount as number : null,
    piStartedAt: object(state.running) && typeof state.running.startedAt === "string" ? state.running.startedAt : null,
    piContainerId: typeof status?.containerID === "string" && status.containerID ? status.containerID : null, pvcWriters };
};
