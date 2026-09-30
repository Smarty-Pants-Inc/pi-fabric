import fs from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import { activeFabricRoot, loadedFabricRoot, releaseLabel, resolveAgentDir, SELF_RELOAD_COMMAND } from "../core/agent-dir.js";

export const STALE_MAIN_TOPIC = "ops.fabric.stale-main";
export const SAFETY_FILE = "releases-safety.json";

export interface StaleMainNotice {
  loaded: string;
  active: string;
  loadedRoot: string;
  activeRoot: string;
  notice: string;
  refused: boolean;
  criticalReleases: string[];
  reason?: string;
}

interface ReleaseMetadata { safetyCritical?: boolean; installedAt?: string; activatedAt?: string }
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const readObject = (file: string): Record<string, unknown> | undefined => {
  let text: string;
  try { text = fs.readFileSync(file, "utf8"); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error(`Cannot read ${file}`, { cause: error });
  }
  const value: unknown = JSON.parse(text);
  if (!object(value)) throw new Error(`Invalid release metadata: ${file}`);
  return value;
};

const metadata = (row: Record<string, unknown>, file: string): ReleaseMetadata => {
  if (row.safetyCritical !== undefined && typeof row.safetyCritical !== "boolean") {
    throw new Error(`Invalid safetyCritical boolean: ${file}`);
  }
  const result: ReleaseMetadata = {};
  if (typeof row.safetyCritical === "boolean") result.safetyCritical = row.safetyCritical;
  for (const key of ["installedAt", "activatedAt"] as const) {
    if (row[key] !== undefined) {
      if (typeof row[key] !== "string" || !Number.isFinite(Date.parse(row[key]))) {
        throw new Error(`Invalid ${key}: ${file}`);
      }
      result[key] = row[key];
    }
  }
  return result;
};

/** Immutable receipts supply chronology; the adjacent mutable policy supplies safety marks. */
export const readReleaseCatalog = (releasesDir: string): Map<string, ReleaseMetadata> => {
  const base = path.dirname(releasesDir);
  const rows = new Map<string, ReleaseMetadata>();
  const merge = (release: string, row: ReleaseMetadata) => {
    const previous = rows.get(release) ?? {};
    // A false policy value can never erase a safety mark from an install manifest.
    rows.set(release, { ...previous, ...row,
      ...(previous.safetyCritical === true || row.safetyCritical === true ? { safetyCritical: true } : {}),
    });
  };
  for (const name of fs.readdirSync(base)) {
    if (!name.endsWith(".receipt.json")) continue;
    const file = path.join(base, name);
    const row = readObject(file)!;
    const release = name.slice(0, -".receipt.json".length);
    if (row.commit !== undefined && row.commit !== release) throw new Error(`Receipt commit mismatch: ${file}`);
    merge(release, metadata(row, file));
  }
  for (const entry of fs.readdirSync(releasesDir, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name.startsWith(".")) continue;
    for (const name of ["manifest.json", "install-receipt.json"]) {
      const file = path.join(releasesDir, entry.name, name);
      const row = readObject(file);
      if (row) merge(entry.name, metadata(row, file));
    }
  }
  const local = path.join(base, SAFETY_FILE);
  const packageRoot = loadedFabricRoot(import.meta.url);
  const policyFile = fs.existsSync(local) ? local : packageRoot ? path.join(packageRoot, SAFETY_FILE) : local;
  const policy = readObject(policyFile);
  if (!policy || policy.version !== undefined && policy.version !== 1 || !object(policy.releases)) {
    throw new Error(`Missing or invalid safety policy: ${policyFile}`);
  }
  for (const [release, row] of Object.entries(policy.releases)) {
    if (!release || path.basename(release) !== release || !object(row)) throw new Error(`Invalid release row: ${policyFile}`);
    merge(release, metadata(row, policyFile));
  }
  return rows;
};

const installedTime = (row: ReleaseMetadata | undefined): number | undefined => {
  // First installation stays stable across reactivation/rollback; activation is a legacy fallback.
  const time = row?.installedAt ?? row?.activatedAt;
  return time === undefined ? undefined : Date.parse(time);
};

const reloadPath = `Let this Main settle safely, then /${SELF_RELOAD_COMMAND} (or /reload after its running work finishes); its resident host drains and restarts on the reloaded release. Do not resolve a newer worker under this Main.`;

/** A pre-launch admission failure, never an executed/failed actor activation. */
export class StaleMainRefusal extends Error {
  readonly code = "FABRIC_STALE_MAIN";
}

/** Pure admission decision, reread at actual launch so activation during a queue wait is covered. */
export const inspectStaleMain = (loadedRoot: string | undefined, settingsPath: string): StaleMainNotice | undefined => {
  if (!loadedRoot) return undefined;
  const activeRoot = activeFabricRoot(settingsPath);
  if (!activeRoot) {
    if (path.basename(path.dirname(loadedRoot)) === "releases") {
      throw new StaleMainRefusal(`Fabric spawn refused: cannot determine the fleet's active release from ${settingsPath}. Repair the packages selector; ${reloadPath}`);
    }
    return undefined;
  }
  if (loadedRoot === activeRoot) return undefined;
  const loaded = releaseLabel(loadedRoot);
  const active = releaseLabel(activeRoot);
  const result: StaleMainNotice = {
    loaded, active, loadedRoot, activeRoot,
    notice: `This Main runs ${loaded}; the fleet runs ${active}; it self-reloads at its next safe settle`,
    refused: false, criticalReleases: [],
  };
  // Explicit -e/development packages are not an installed release lineage. Never compare their
  // filesystem mtimes or pretend they occupy a position in the fleet's install chronology.
  if (path.basename(path.dirname(loadedRoot)) !== "releases") return result;
  try {
    if (path.dirname(loadedRoot) !== path.dirname(activeRoot)) throw new Error("loaded and active releases have different release directories");
    const rows = readReleaseCatalog(path.dirname(activeRoot));
    // Shipped, undated safety marks describe fleet history, not an installation on every host.
    // A receipt/time or a surviving release directory is host-local installation evidence.
    // Without any installed critical release there is no safety gap to establish; merely-old
    // Mains still get the notice on fresh/receipt-less hosts. Installed ambiguous gaps fail closed.
    const critical = [...rows].filter(([release, row]) => row.safetyCritical === true && (
      installedTime(row) !== undefined || fs.existsSync(path.join(path.dirname(activeRoot), release)) ||
      fs.existsSync(path.join(path.dirname(path.dirname(activeRoot)), `${release}.receipt.json`))
    ));
    if (critical.length === 0) return result;
    const start = installedTime(rows.get(loaded));
    const end = installedTime(rows.get(active));
    if (start === undefined || end === undefined) throw new Error(`missing install/activation time for ${start === undefined ? loaded : active}`);
    if (end < start) throw new Error("active release is a rollback; forward safety gap cannot be established");
    if (end === start) throw new Error("loaded and active releases have tied install/activation times");
    for (const [release, row] of critical) {
      const time = installedTime(row);
      if (time === undefined) throw new Error(`missing install/activation time for safetyCritical release ${release}`);
      if (release !== loaded && time === start) throw new Error(`safetyCritical release ${release} has the loaded release's timestamp; order is ambiguous`);
      if (time > start && time <= end) result.criticalReleases.push(release);
    }
    result.criticalReleases.sort((a, b) => installedTime(rows.get(a))! - installedTime(rows.get(b))!);
    if (result.criticalReleases.length) {
      result.refused = true;
      result.reason = `Fabric spawn refused: safetyCritical release(s) ${result.criticalReleases.join(", ")} lie between loaded ${loaded} (exclusive) and active ${active} (inclusive). ${reloadPath}`;
    }
  } catch (error) {
    result.refused = true;
    result.reason = `Fabric spawn refused: cannot verify the safetyCritical release gap from ${loaded} to ${active}: ${error instanceof Error ? error.message : String(error)}. ${reloadPath}`;
  }
  return result;
};

// Survives manager replacement and native /reload; scope is this Main identity, not a worker run.
const REPORTED = Symbol.for("pi-fabric.stale-main.reported");
// A resident host and nested task processes may share this Main identity. The atomic marker
// keeps their best-effort notice publication once per profile/Main/active release too.
const claimNotice = (mainId: string, settingsPath: string, status: StaleMainNotice): boolean => {
  const directory = path.join(path.dirname(settingsPath), "fabric", "stale-main-events");
  const key = createHash("sha256").update(JSON.stringify([mainId, status.activeRoot])).digest("hex");
  try {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(directory, `${key}.json`), JSON.stringify({ mainId, ...status }), { flag: "wx", mode: 0o600 });
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    return true; // Unwritable profile: process-local dedup still applies; admission never depends on this marker.
  }
};
const reported = (): Map<string, Set<string>> =>
  ((globalThis as Record<symbol, unknown>)[REPORTED] ??= new Map<string, Set<string>>()) as Map<string, Set<string>>;

export class StaleMainGuard {
  constructor(
    readonly loadedRoot: string | undefined,
    readonly mainId: string,
    readonly settingsPath = path.join(resolveAgentDir(), "settings.json"),
    readonly publish?: (data: StaleMainNotice) => void | Promise<void>,
  ) {}

  check(): string | undefined {
    const status = inspectStaleMain(this.loadedRoot, this.settingsPath);
    if (!status) return undefined;
    let targets = reported().get(this.mainId);
    if (!targets) reported().set(this.mainId, targets = new Set());
    if (this.publish && !targets.has(status.activeRoot)) {
      targets.add(status.activeRoot);
      if (claimNotice(this.mainId, this.settingsPath, status)) {
        try { void Promise.resolve(this.publish(status)).catch(() => undefined); } catch { /* best effort ops */ }
      }
    }
    if (status.refused) throw new StaleMainRefusal(`${status.notice}. ${status.reason}`);
    return status.notice;
  }
}
