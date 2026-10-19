import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import type { AgentPlacementConfig } from "../placement-config.js";
import type { AgentTransportLaunch } from "../types.js";
import { agentPlacementProbe } from "../placement-config.js";
import { AgentInputError, RequiredInputMissingError } from "../input-validation.js";
import { taskAgentEnvironment } from "../task-environment.js";

const containsPath = (root: string, file: string): boolean => {
  const relative = path.relative(root, file);
  return relative === "" || relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
};

/** Resolve aliases even when the caller names a not-yet-created descendant. */
const realpathWithMissingTail = (file: string): string => {
  let ancestor = path.resolve(file);
  const tail: string[] = [];
  for (;;) {
    try { return path.join(fs.realpathSync(ancestor), ...tail); }
    catch (error) {
      const parent = path.dirname(ancestor);
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || parent === ancestor) throw error;
      try {
        const target = fs.readlinkSync(ancestor);
        return path.join(realpathWithMissingTail(path.resolve(parent, target)), ...tail);
      } catch (linkError) {
        if (!["ENOENT", "EINVAL"].includes((linkError as NodeJS.ErrnoException).code ?? "")) throw linkError;
      }
      tail.unshift(path.basename(ancestor));
      ancestor = parent;
    }
  }
};

/** Caller-owned scratch/state wins over even an explicitly declared shared mount. */
export const placementLocalPathReason = (cwd: string, config: AgentPlacementConfig): string | undefined => {
  if (!path.isAbsolute(cwd)) return undefined;
  const localReason = `caller-local cwd requires the local worker: ${cwd}`;
  const localRoots = [process.env.TMPDIR || os.tmpdir(), "/srv/scratch", path.join(os.homedir(), ".local", "state")]
    .filter(root => path.isAbsolute(root));
  if (localRoots.some(root => containsPath(root, cwd))) return localReason;
  try {
    const resolved = realpathWithMissingTail(cwd);
    for (const root of localRoots) {
      try { if (containsPath(realpathWithMissingTail(root), resolved)) return localReason; }
      catch { /* An unavailable local root still retains its lexical fence. */ }
    }
  } catch { return `caller-local cwd cannot be inspected safely: ${cwd}`; }
  let canonical: string;
  try { canonical = fs.realpathSync(cwd); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    return `caller-local cwd cannot be inspected safely: ${cwd}`;
  }
  for (const root of config.sharedMounts ?? []) {
    if (!path.isAbsolute(root)) continue;
    try {
      const shared = fs.realpathSync(root);
      if (shared === path.parse(shared).root || !fs.statSync(shared).isDirectory()) continue;
      if (containsPath(shared, canonical)) return undefined;
    } catch { /* An unavailable shared root proves no portability. */ }
  }
  return localReason;
};

const hostName = /^[A-Za-z0-9][A-Za-z0-9._-]*$/u;
const quote = (value: string): string => `'${value.replaceAll("'", `'"'"'`)}'`;
const failure = (host: string, reason: string): AgentInputError => new AgentInputError("placement",
  `Placement refused host=${host}: ${reason}; use needs:['local'] for caller-only inputs; no task started`);

/** Native close is the normal event; a one-shot deadline bounds stuck read-only
 * queries. Their own group includes the picker's SSH children, never a launcher.
 */
const query = (executable: string, argv: string[], deadline: number, environment: NodeJS.ProcessEnv, signal?: AbortSignal, input?: string): Promise<{ stdout: string; stderr: string; code: number | null }> =>
  new Promise((resolve, reject) => {
    if (signal?.aborted || Date.now() >= deadline) { reject(new Error("Preflight cancelled or deadline expired")); return; }
    const options = { env: environment, detached: process.platform !== "win32" };
    const child = input
      ? spawn(executable, argv, { ...options, stdio: "pipe" })
      : spawn(executable, argv, { ...options, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "", error: Error | undefined;
    const terminate = (reason: string): void => {
      error ??= new Error(reason);
      if (child.pid && process.platform !== "win32") {
        try { process.kill(-child.pid, "SIGKILL"); } catch { /* Native close remains the receipt. */ }
      } else child.kill("SIGKILL");
      child.stdin?.destroy(); child.stdout.destroy(); child.stderr.destroy();
    };
    const timer = setTimeout(() => terminate("Preflight deadline expired"), Math.max(1, deadline - Date.now()));
    const abort = (): void => terminate("Preflight cancelled");
    signal?.addEventListener("abort", abort, { once: true });
    child.stdout.setEncoding("utf8"); child.stderr.setEncoding("utf8");
    child.stdout.on("data", (part: string) => { stdout += part; if (stdout.length > 1_000_000) terminate("Preflight output too large"); });
    child.stderr.on("data", (part: string) => { stderr += part; if (stderr.length > 1_000_000) terminate("Preflight output too large"); });
    child.on("error", cause => { error = cause; });
    child.stdout.on("error", cause => { error ??= cause; });
    child.stderr.on("error", cause => { error ??= cause; });
    child.stdin?.on("error", cause => {
      // A fast query may close stdin before consuming input; its output and exit remain authoritative.
      if ((cause as NodeJS.ErrnoException).code !== "EPIPE") error ??= cause;
    });
    child.once("close", code => {
      clearTimeout(timer); signal?.removeEventListener("abort", abort);
      if (error) reject(error); else resolve({ stdout, stderr, code });
    });
    child.stdin?.end(input);
    if (signal?.aborted) abort();
  });

/** Only locate reviewed CLIs; bar/ceiling state belongs entirely to the picker. */
const fleetRoot = (config: AgentPlacementConfig, cwd: string): string | undefined => {
  const executable = agentPlacementProbe(config, cwd).executable;
  if (!executable) return undefined;
  let canonical: string;
  try { canonical = fs.realpathSync(executable); }
  catch (error) {
    // A disappearing generic executable retains its normal ENOENT launch receipt,
    // not a new local retry through the fleet-only selector.
    if (path.basename(executable) !== "smarty-task-ryzen2") return undefined;
    throw error;
  }
  if (![path.basename(executable), path.basename(canonical)].includes("smarty-task-ryzen2")) return undefined;
  const bin = path.dirname(canonical);
  return bin === path.join(os.homedir(), ".local", "bin")
    ? path.join(os.homedir(), ".local", "share", "smarty-dev", "factory", "current") : path.dirname(bin);
};

/** Corpus can bypass local placement only with a trusted fleet helper and a configured host allow-list. */
export const corpusRemotePreflightEligible = (config: AgentPlacementConfig, cwd: string, host?: string): boolean => {
  if (!config.corpusRemoteHosts.length) return false;
  try {
    if (fleetRoot(config, cwd) === undefined) return false;
    return host === undefined || config.corpusRemoteHosts.includes(host);
  } catch { return false; }
};

const hostSlot = (command: readonly string[]): number | undefined => {
  const end = command.indexOf("--") < 0 ? command.length : command.indexOf("--");
  const slots = command.slice(1, end).flatMap((entry, index) => entry === "--host" ? [index + 1] : []);
  if (slots.length > 1) throw new Error("Multiple --host options");
  if (slots[0] !== undefined && slots[0] + 1 >= end) throw new Error("Invalid --host option");
  return slots[0];
};

export type FleetPlacementSelection = { host: string } | { localReason: string };

/** Decision C: only --list-room's eligible hosts may receive remote work, even
 * for explicit --host. Missing/broken/slow picker safely selects the local worker.
 * An injected host-only picker opts a custom launcher into the same contract.
 */
export const selectFleetPlacement = async (config: AgentPlacementConfig, cwd: string, signal?: AbortSignal, preflightDeadline?: number, environment: NodeJS.ProcessEnv = taskAgentEnvironment(), allowedHosts?: readonly string[]): Promise<FleetPlacementSelection | undefined> => {
  try {
    const root = fleetRoot(config, cwd);
    if (!root && !config.pickerCommand) return undefined;
    const picker = config.pickerCommand ?? [path.join(root!, "bin/smarty-pick-host")];
    const deadline = Math.min(preflightDeadline ?? Infinity, Date.now() + Math.min(config.commandTimeoutMs, 5_000));
    const slot = hostSlot(config.command);
    const requested = slot === undefined ? "auto" : config.command[slot + 1]!;
    if (requested !== "auto" && !hostName.test(requested)) throw new Error("Invalid --host option");
    const picked = await query(picker[0]!, [...picker.slice(1), "--list-room"], deadline, environment, signal);
    const hosts = picked.stdout.trim().split(/\r?\n/u).filter(Boolean);
    if (picked.code !== 0 || !hosts.length || hosts.some(host => !hostName.test(host)) || new Set(hosts).size !== hosts.length) throw new Error(`picker refused: ${picked.stderr.trim() || "no eligible host"}`);
    if (requested !== "auto" && !hosts.includes(requested)) throw new Error(`host ${requested} is not picker-eligible${picked.stderr.trim() ? `: ${picked.stderr.trim()}` : ""}`);
    if (requested !== "auto" && allowedHosts && !allowedHosts.includes(requested)) throw new Error(`host ${requested} is not in corpusRemoteHosts`);
    const candidates = allowedHosts ? hosts.filter(candidate => allowedHosts.includes(candidate)) : hosts;
    if (!candidates.length) throw new Error(allowedHosts ? "no corpusRemoteHosts are picker-eligible" : "no eligible host");
    const host = requested === "auto" ? candidates[0]! : requested;
    if (!Object.hasOwn(config.sshAliases ?? {}, host) || !hostName.test(config.sshAliases![host]!)) throw new Error(`host ${host} has no trusted placement sshAliases entry`);
    return { host };
  } catch (error) {
    return { localReason: `placement-picker-local: ${error instanceof Error ? error.message : String(error)}` };
  }
};

export interface FleetPlacementPreflight { host: string; command: string[]; environment: NodeJS.ProcessEnv }

/** No launcher runs here. Only the admitted host gets strict, bounded read-only
 * input checks. Generic launchers without a picker retain their existing protocol.
 */
export const preflightFleetPlacement = async (request: AgentTransportLaunch, config: AgentPlacementConfig, command: string[], task: string, environment: NodeJS.ProcessEnv, preflightDeadline?: number, selectedHost?: string): Promise<FleetPlacementPreflight | undefined> => {
  let host = selectedHost ?? "auto";
  try {
    const root = fleetRoot(config, request.cwd);
    const corpusNeed = request.needs?.includes("corpus") ?? false;
    const corpusRemoteCandidate = corpusNeed && corpusRemotePreflightEligible(config, request.cwd);
    if (!root && corpusNeed) throw new Error("corpus need has no trusted fleet preflight and qualified host allow-list");
    if (!root && !config.pickerCommand) return undefined;
    const deadline = Math.min(preflightDeadline ?? Infinity, Date.now() + Math.min(config.commandTimeoutMs, 5_000));
    if (!selectedHost) {
      const selection = await selectFleetPlacement({ ...config, command }, request.cwd, request.signal, deadline, environment,
        corpusRemoteCandidate ? config.corpusRemoteHosts : undefined);
      if (!selection) throw new Error("No fleet placement selection");
      if ("localReason" in selection) throw new Error(selection.localReason);
      host = selection.host;
    }
    const corpusRemoteEligible = corpusRemoteCandidate && corpusRemotePreflightEligible(config, request.cwd, host);
    if (corpusNeed && corpusRemoteCandidate && !corpusRemoteEligible) throw new Error(`host ${host} is not a qualified corpus target`);
    const alias = config.sshAliases?.[host];
    if (!alias || !Object.hasOwn(config.sshAliases!, host) || !hostName.test(alias)) throw new Error("host has no trusted placement sshAliases entry");
    const end = command.indexOf("--") < 0 ? command.length : command.indexOf("--");
    const optionValue = (flag: string): string => {
      const index = command.slice(0, end).indexOf(flag);
      return index < 0 ? "" : command[index + 1] ?? "";
    };
    const targetCwd = optionValue("--cwd");
    const project = optionValue("--src") || targetCwd;
    const requires = request.requires ?? [];
    const pairs: [string, string][] = [];
    if (targetCwd) pairs.push([targetCwd, targetCwd]);
    let guard = ":";
    if (root) {
      // Reuse installed literal-path/corpus inference, not another prompt scanner.
      const source = path.join(root, "setup/factory/task-preflight.py");
      // Only the selected allow-listed host makes an explicit corpus need placement-neutral.
      // The helper still derives corpus isolation from task and required paths.
      const admitted = await query("python3", ["-I", source, "--admit", "--root", project, String(requires.length), ...requires, "0", ...(request.needs ?? []).filter(need => need === "github" || (need === "corpus" && !corpusRemoteEligible))], deadline, environment, request.signal, task);
      const fields = admitted.stdout.split("\0");
      if (fields.at(-1) === "") fields.pop();
      const guards = Number(fields[3]);
      if (admitted.code !== 0 || fields.length < 4 || !/^\d+$/u.test(fields[3]!) || !Number.isSafeInteger(guards) || guards > fields.length - 4 || (fields.length - 4 - guards) % 2) throw new Error(`Installed task admission failed: ${admitted.stderr.trim()}`);
      if (fields[0] || fields[1]) throw new Error(fields[0] || `needs ${fields[1]}: ${fields[2]}`);
      for (let index = 4 + guards; index < fields.length; index += 2) pairs.push([fields[index]!, fields[index + 1]!]);
      guard = ["python3", "-I", "-c", fs.readFileSync(source, "utf8"), "--probe", "--root", project, "--workspace", targetCwd, ...fields.slice(4, 4 + guards)].map(quote).join(" ");
    } else {
      // Explicit injected/custom protocol: honor declared literal inputs only.
      pairs.push(...requires.map(file => [file, file] as [string, string]));
    }
    if (pairs.some(pair => pair.some(file => !path.posix.isAbsolute(file) || file.includes("\0")))) throw new Error("Invalid literal path admission receipt");
    const checks = pairs.map(([literal, alternative], index) => `test -e ${quote(literal)} || test -e ${quote(alternative)} || { printf 'PREFLIGHT_MISSING ${index}\\n'; exit 3; }`).join("; ");
    const remote = `timeout 3 sh -c ${quote(`${guard} || exit $?; ${checks || ":"}`)}`;
    const probed = await query("ssh", ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "KnownHostsCommand=none", "-o", "ConnectionAttempts=1", "-o", "ConnectTimeout=2", "-o", "ControlMaster=no", "-o", "ControlPath=none", alias, remote], deadline, environment, request.signal);
    if (probed.code !== 0) {
      const missing = probed.stdout.match(/^PREFLIGHT_MISSING (\d+)$/mu);
      const absent = missing ? pairs[Number(missing[1])]?.[0] : undefined;
      if (absent !== undefined) {
        const index = requires.indexOf(absent);
        if (index >= 0) {
          const error = new RequiredInputMissingError(absent, index);
          error.message = failure(host, `missing path ${absent}`).message;
          throw error;
        }
        throw new Error(`missing path ${absent}`);
      }
      throw new Error(`Read-only SSH preflight failed: ${probed.stdout.trim() || probed.stderr.trim() || `exit ${probed.code}`}`);
    }
    const pinned = [...command];
    const slot = hostSlot(pinned);
    if (slot !== undefined) pinned[slot + 1] = host;
    else pinned.splice(end, 0, "--host", host);
    if (Date.now() >= deadline || request.signal?.aborted) throw new Error("Preflight cancelled or deadline expired");
    return { host, command: pinned, environment };
  } catch (error) {
    if (error instanceof RequiredInputMissingError) throw error;
    throw failure(host, error instanceof Error ? error.message : String(error));
  }
};
