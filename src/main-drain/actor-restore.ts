import fs from "node:fs";
import path from "node:path";
import type { FabricActorRequest, FabricActorRunBinding } from "../actors/types.js";
import type { MainDrainBindingRequest } from "./binding.js";
import { readOwned, writeOnce } from "../seat-drain/run.js";

/** Reuse the ordinary create path; identity/authority is re-minted only by this owning Main. */
const CREATE_KEYS = ["name", "instructions", "events", "eventsStartAt", "topics", "delivery", "responseMode", "triggerTurn",
  "coalesce", "coalesceKey", "dedupeKey", "activation", "activationFilter", "deadLetterReplayMaxAgeMs", "batchWindowMs",
  "batchMax", "batchMaxBytes", "residency", "runner", "kernel", "pythonRuntime", "model", "modelReason", "thinking",
  "modelPolicyOriginalModel", "routeClass", "protected", "tools", "transport", "timeoutMs", "nice", "bashTimeoutSeconds",
  "extensions", "inferenceContext", "validWhile", "records", "principalScope"] as const;

export interface DrainActorSpec { id: string; spec: Record<string, unknown>; stoppedAt: number }
export const actorRestoreRequest = (spec: Record<string, unknown>): FabricActorRequest => {
  const request: Record<string, unknown> = {};
  for (const key of CREATE_KEYS) if (spec[key] !== undefined) request[key] = structuredClone(spec[key]);
  if (spec.requirements !== undefined) request.requires = structuredClone(spec.requirements);
  if (typeof request.name !== "string" || typeof request.instructions !== "string" || !request.instructions.trim() ||
    (request.residency !== undefined && request.residency !== "session")) throw new Error("ACTOR_SPEC_UNRESTORABLE");
  return request as unknown as FabricActorRequest;
};
export const actorLiveBinding = (spec: Record<string, unknown>): FabricActorRunBinding => {
  const binding = (spec.resolvedBinding ?? spec) as FabricActorRunBinding;
  return { ...(binding.model ? { model: binding.model } : {}), ...(binding.thinking ? { thinking: binding.thinking } : {}),
    ...(binding.modelReason !== undefined ? { modelReason: binding.modelReason } : {}) };
};

/** Stable equality for configuration, not execution metadata, ids or transcript paths. */
const canonical = (value: unknown): string => JSON.stringify(value, (_key, child: unknown) =>
  typeof child === "object" && child !== null && !Array.isArray(child)
    ? Object.fromEntries(Object.entries(child).sort(([a], [b]) => a.localeCompare(b))) : child);
export const sameActorConfiguration = (a: Record<string, unknown>, b: Record<string, unknown>): boolean =>
  a.rootId === b.rootId && a.supervisorFor === b.supervisorFor && a.project === b.project &&
  canonical(actorRestoreRequest(a)) === canonical(actorRestoreRequest(b));
export const sameActorSpec = (a: Record<string, unknown>, b: Record<string, unknown>): boolean =>
  sameActorConfiguration(a, b) && canonical(actorLiveBinding(a)) === canonical(actorLiveBinding(b));

/** Success restoration is not an undo: only a passing drain AND the pinned fence owner's receipt authorize it. */
export const assertCutoverRestore = (request: MainDrainBindingRequest, rootId: string): void => {
  if (!request.runDir || request.phase !== "drain-restore") throw new Error("RESTORE_NOT_AUTHORIZED");
  const read = (name: string) => JSON.parse(readOwned(path.join(request.runDir!, name), process.geteuid!(), 4 * 1024 * 1024,
    undefined, "restore-receipt-unreadable").toString("utf8")) as Record<string, unknown>;
  const run = read("seat-run.json");
  const seams = run.seams as Record<string, { owner?: unknown }> | undefined;
  for (const phase of ["drain", "fence"]) {
    const value = read(`receipts/${phase}.json`);
    const owner = phase === "drain" ? "fabric-v2" : seams?.fence?.owner;
    const observed = value.observed as Record<string, unknown> | undefined;
    if (!owner || value.schema !== "d3-seam-receipt-v1" || value.run !== request.run || value.phase !== phase ||
      value.owner !== owner || value.result !== "pass" || (phase === "drain" && observed?.sessionId !== rootId.slice(8))) {
      throw new Error("RESTORE_NOT_AUTHORIZED");
    }
  }
};

const NAME = /^fabric-v2-drain-actor-([a-f0-9]{32})\.json$/;
/** Write once BEFORE stop, not only in the client: survives a seam crash or Main restart. */
export const saveDrainActor = (request: MainDrainBindingRequest, rootId: string, entry: DrainActorSpec): void => {
  if (!request.runDir) return; // Raw Pod: the process-global slot owns undo; its successful exit is terminal.
  const name = `fabric-v2-drain-actor-${entry.id}.json`;
  if (!NAME.test(name)) throw new Error("ACTOR_SPEC_UNRESTORABLE");
  const old = readDrainActors(request, rootId).find(row => row.id === entry.id);
  if (old) {
    if (!sameActorSpec(old.spec, entry.spec)) throw new Error("ACTOR_SPEC_CHANGED");
    return;
  }
  writeOnce(request.runDir, name, Buffer.from(JSON.stringify({ schema: "fabric-v2-drain-actor-v1", run: request.run,
    runSha256: request.runSha256, operationId: request.operationId, generation: request.generation, rootId, ...entry })));
};

export const readDrainActors = (request: MainDrainBindingRequest, rootId: string): DrainActorSpec[] => {
  if (!request.runDir) return [];
  return fs.readdirSync(request.runDir).filter(name => NAME.test(name)).sort().map(name => {
    const row = JSON.parse(readOwned(path.join(request.runDir!, name), process.geteuid!(), 64 * 1024 * 1024,
      { mode: 0o600 }, "actor-restore-unreadable").toString("utf8")) as Record<string, unknown>;
    const spec = row.spec as Record<string, unknown> | undefined;
    if (row.schema !== "fabric-v2-drain-actor-v1" || row.run !== request.run || row.runSha256 !== request.runSha256 ||
      row.operationId !== request.operationId || row.generation !== request.generation || row.rootId !== rootId ||
      row.id !== NAME.exec(name)![1] || !spec || spec.id !== row.id || spec.rootId !== rootId ||
      typeof row.stoppedAt !== "number") throw new Error("ACTOR_RESTORE_BINDING_MISMATCH");
    actorRestoreRequest(spec);
    return { id: row.id as string, spec, stoppedAt: row.stoppedAt };
  });
};
