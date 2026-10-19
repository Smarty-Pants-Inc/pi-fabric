// Trust-boundary/data-safety seam shared by the native rehearsal and its broker-free regressions.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import type { SwitchRuntimeOptions } from "./nats-switch-runtime.js";

export interface SwitchSeedInput {
  root: string; hubMesh: string; workMesh: string;
  ownerOptions: SwitchRuntimeOptions; senderOptions: SwitchRuntimeOptions; carrier: "nats" | "file";
}
export interface SwitchPopulationInput {
  seedResult: unknown; ownerRoutes: unknown; senderRoutes: unknown;
  ownerId: string; senderId: string; carrier: "nats" | "file";
}
export interface SwitchSeedHooks {
  seedSwitchFixture(input: SwitchSeedInput): unknown | Promise<unknown>;
  verifySwitchPopulation(input: SwitchPopulationInput): unknown | Promise<unknown>;
}
const captureError = (reason: string) => new Error(`not capture-backed: ${reason}`);

/** Resolve before allocating any fixture root or starting a broker. No module means expected skip. */
export async function loadSwitchSeedHooks(modulePath: string | undefined): Promise<SwitchSeedHooks | undefined> {
  if (!modulePath?.trim()) return undefined;
  let hooks: SwitchSeedHooks;
  try { hooks = await import(pathToFileURL(path.resolve(modulePath)).href) as SwitchSeedHooks; }
  catch { throw captureError("NATS_SWITCH_SEED_MODULE could not be loaded"); }
  if (typeof hooks.seedSwitchFixture !== "function" || typeof hooks.verifySwitchPopulation !== "function")
    throw captureError("NATS_SWITCH_SEED_MODULE must export seedSwitchFixture AND verifySwitchPopulation");
  return hooks;
}

export function requireSwitchSeedResult(seedResult: unknown): void {
  if (!seedResult || typeof seedResult !== "object" || Array.isArray(seedResult))
    throw captureError("seedSwitchFixture must return a nonempty serializable object");
  try {
    const value: unknown = JSON.parse(JSON.stringify(seedResult));
    if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).length === 0) throw new Error();
  } catch { throw captureError("seedSwitchFixture must return a nonempty serializable object"); }
}

/** Keep the original seedResult identity (private seeders associate captured IDs through a WeakMap). */
export async function verifySwitchCapture(hooks: SwitchSeedHooks, input: SwitchPopulationInput) {
  requireSwitchSeedResult(input.seedResult);
  const freshIds = new Set([input.ownerId, input.senderId]);
  if (freshIds.size !== 2 || input.carrier !== "nats") throw captureError("distinct NATS owner and sender required");
  const inspect = (value: unknown) => {
    if (!Array.isArray(value)) throw captureError("actual native route arrays required");
    const ids = new Set<string>(), captured = new Set<string>();
    for (const row of value) {
      if (!row || typeof row.id !== "string" || !row.id || ids.has(row.id)) throw captureError("invalid or duplicate native route");
      ids.add(row.id);
      if (freshIds.has(row.id)) {
        if (row.stale !== false) throw captureError("both actual fresh native routes required");
      } else {
        if (row.stale !== true) throw captureError("historical native routes must be stale");
        captured.add(row.id);
      }
    }
    if (![...freshIds].every(id => ids.has(id)) || captured.size === 0) throw captureError("positive captured population and both fresh routes required");
    return { captured, total: value.length };
  };
  const owner = inspect(input.ownerRoutes), sender = inspect(input.senderRoutes);
  if (owner.captured.size !== sender.captured.size || [...owner.captured].some(id => !sender.captured.has(id)))
    throw captureError("captured native populations differ between roles");
  // Invoke on the actual native snapshots, not seed metadata or a reconstructed view.
  const value = await hooks.verifySwitchPopulation(input);
  if (!value || typeof value !== "object" || Array.isArray(value)) throw captureError("meaningful population evidence required");
  const e = value as Record<string, unknown>;
  const expected = { capturedOnOwner: owner.captured.size, capturedOnSender: sender.captured.size,
    historicalLiveRoutes: 0, freshLiveOnEach: 2, ownerTotalRoutes: owner.total, senderTotalRoutes: sender.total };
  for (const [key, count] of Object.entries(expected)) if (e[key] !== count) throw captureError(`population evidence mismatch: ${key}`);
  if (!Number.isSafeInteger(e.checkedAt) || (e.checkedAt as number) <= 0 ||
    typeof e.elapsedMs !== "number" || !Number.isFinite(e.elapsedMs) || e.elapsedMs < 0)
    throw captureError("population check timing required");
  // Arbitrary private hook fields never cross the public log boundary.
  return { ...expected, checkedAt: e.checkedAt as number, elapsedMs: e.elapsedMs };
}

/** Only these two projections are used by runtime IPC/phase logging; full evidence stays private. */
export function logSwitchCheckpointEntry(value: unknown): void {
  const e = value as { entryKind?: unknown; sequence?: unknown } | null;
  console.info(JSON.stringify({ type: "switch-checkpoint-entry",
    entryKind: e?.entryKind === "checkpointForRelease" || e?.entryKind === "releaseCheckpointReceipt" ? e.entryKind : "unknown",
    sequence: Number.isSafeInteger(e?.sequence) ? e!.sequence : null }));
}
export function logSwitchPhase(event: { phase: string; at: number; elapsedMs: number; phaseMs: number }): void {
  console.info(JSON.stringify({ type: "switch-phase", label: "pre-projection", phase: event.phase,
    at: event.at, elapsedMs: event.elapsedMs, phaseMs: event.phaseMs }));
}

/** Archive only inside the current private TMPDIR. No stdout byte transport or export opt-in. */
export function preserveSwitchArchive(root: string, stoppedPids: readonly number[]) {
  const parent = fs.realpathSync(process.env.TMPDIR!);
  const actualRoot = fs.realpathSync(root), relative = path.relative(parent, actualRoot);
  if (!relative || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
    throw new Error("archive root must be inside the current private TMPDIR");
  if (stoppedPids.some(pid => fs.existsSync(`/proc/${pid}`))) throw new Error("cannot archive while an actual native PID remains");
  let fileCount = 0, sourceBytes = 0;
  const inspect = (directory: string) => {
    for (const item of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, item.name), stat = fs.lstatSync(file);
      if (stat.isDirectory()) inspect(file);
      else if (stat.isFile() && stat.nlink === 1) { fileCount++; sourceBytes += stat.size; }
      else throw new Error("archive refuses symlink/hardlink/special entry");
      if (fileCount > 20_000 || sourceBytes > 128 * 1024 * 1024) throw new Error("archive source exceeds size bound");
    }
  };
  inspect(actualRoot);
  const archive = path.join(parent, `${path.basename(actualRoot)}.tar.gz`);
  // Reserve private permissions before tar opens the destination (including partial failures).
  fs.closeSync(fs.openSync(archive, "wx", 0o600));
  const result = spawnSync("tar", ["--format=ustar", "-czf", archive, "-C", path.dirname(actualRoot), path.basename(actualRoot)],
    { encoding: "utf8", timeout: 20_000 });
  if (result.error || result.status !== 0) throw new Error("private archive creation failed");
  const data = fs.readFileSync(archive);
  if (data.length > 64 * 1024 * 1024) throw new Error("compressed archive exceeds bound");
  const sha256 = createHash("sha256").update(data).digest("hex");
  const receipt = { type: "switch-archive-complete", label: "pre-projection", schema: 1, id: path.basename(actualRoot),
    sha256, fileCount, bytes: data.length, sourceBytes, originalRoot: actualRoot, privateArchive: archive, at: Date.now(), stoppedPids };
  console.info(JSON.stringify({ sha256, privateArchive: archive }));
  return receipt;
}
