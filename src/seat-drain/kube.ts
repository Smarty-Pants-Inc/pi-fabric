import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { SeamGap } from "./run.js";

/**
 * D3 resume-drain (smarty-dev#15310, DRAIN-DESIGN.r1 §1.13): the raw Pod is reached ONLY through the
 * fixed root-owned kubectl with the sealed packet's kubeconfig and context and a minimal env
 * (SEAT.md §1c `kube`). Before the first API call the kubeconfig must name exactly one cluster with
 * the pinned server and CA and no insecure-skip-tls-verify (AUDIT-A A5). Fixed codes only.
 */
export interface KubeAccess {
  kubectl: string;
  kubeconfig: string;
  context: string;
  namespace: string;
  server: string;
  caSha256: string;
  home: string;
}
const KUBECTL = ["/usr/local/bin/kubectl", "/usr/bin/kubectl"];
const MAX_OUTPUT = 4 * 1024 * 1024;

const rootOwnedChain = (file: string): boolean => {
  for (let current = path.dirname(file); ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || stat.uid !== 0 || (stat.mode & 0o022) !== 0) return false;
    if (path.dirname(current) === current) return true;
  }
};

/** The fixed client: the first of the two fixed paths that exists, resolved, root-owned and not writable by others. */
export const fixedKubectl = (candidates: readonly string[] = KUBECTL): string => {
  for (const candidate of candidates) {
    let real: string;
    try { real = fs.realpathSync(candidate); } catch { continue; }
    const stat = fs.statSync(real);
    if (!stat.isFile() || stat.uid !== 0 || (stat.mode & 0o022) !== 0 || !rootOwnedChain(real)) throw new SeamGap("kubectl-unsafe");
    return real;
  }
  throw new SeamGap("kubectl-missing");
};

export const kubectl = (access: KubeAccess, args: readonly string[], timeoutMs: number): Promise<{ code: number | null; stdout: string }> =>
  new Promise(resolve => {
    const child = spawn(access.kubectl, ["--kubeconfig", access.kubeconfig, "--context", access.context, ...args], {
      stdio: ["ignore", "pipe", "ignore"], timeout: Math.max(1_000, Math.floor(timeoutMs)),
      env: { PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C", LC_ALL: "C", HOME: access.home, KUBECONFIG: access.kubeconfig },
    });
    let out = Buffer.alloc(0);
    child.stdout!.on("data", (chunk: Buffer) => { if (out.length < MAX_OUTPUT) out = Buffer.concat([out, chunk]); });
    child.on("error", () => resolve({ code: null, stdout: "" }));
    child.on("close", code => resolve({ code, stdout: out.subarray(0, MAX_OUTPUT).toString("utf8") }));
  });

const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const json = (text: string): unknown => { try { return JSON.parse(text); } catch { return undefined; } };

/** AUDIT-A A5: one cluster, the pinned server, the pinned CA bytes, no TLS bypass. */
export const verifyKubeconfig = async (access: KubeAccess, timeoutMs: number): Promise<void> => {
  const view = await kubectl(access, ["config", "view", "--minify", "--raw", "-o", "json"], timeoutMs);
  const config = view.code === 0 ? json(view.stdout) : undefined;
  const clusters = object(config) && Array.isArray(config.clusters) ? config.clusters : [];
  const cluster = clusters.length === 1 && object(clusters[0]) && object(clusters[0].cluster) ? clusters[0].cluster : undefined;
  if (!cluster || cluster.server !== access.server || cluster["insecure-skip-tls-verify"] === true) throw new SeamGap("kubeconfig-unpinned");
  let ca: Buffer | undefined;
  if (typeof cluster["certificate-authority-data"] === "string") ca = Buffer.from(cluster["certificate-authority-data"], "base64");
  else if (typeof cluster["certificate-authority"] === "string") {
    try { ca = fs.readFileSync(cluster["certificate-authority"]); } catch { ca = undefined; }
  }
  if (!ca || createHash("sha256").update(ca).digest("hex") !== access.caSha256) throw new SeamGap("kubeconfig-ca-mismatch");
};

export interface PodView { uid: string; nodeName: string | null; piRunning: boolean; piTerminated: { exitCode: number } | null; meshRoot: string | null }

/** The pod as the API reports it; undefined when it cannot be read. */
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
  return { uid: pod.metadata.uid, nodeName, piRunning: object(state.running), piTerminated: terminated, meshRoot };
};
