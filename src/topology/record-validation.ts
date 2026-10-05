import { createHash } from "node:crypto";
import type { MeshStateEntry } from "../mesh/store.js";
import type { FabricHostRecord, FabricParticipantKind, FabricParticipantRecord } from "./types.js";

const keyFor = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const participantKind = (value: unknown): FabricParticipantKind | undefined =>
  value === "root" || value === "agent" || value === "actor" ? value : undefined;
const transports = new Set(["host", "auto", "process", "tmux", "screen", "localterm", "herdr"]);
const capabilities = new Set(["steer", "followUp", "stop", "ask", "actor-bindings", "attach", "fabric"]);
const REMOTE_HOST = /^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/;
const remoteHostValid = (value: unknown): boolean =>
  value === undefined || (typeof value === "string" && REMOTE_HOST.test(value));
const optionalStrings = (value: Record<string, unknown>, keys: readonly string[]): boolean =>
  keys.every((key) => value[key] === undefined || typeof value[key] === "string");

/** Canonical topology readers and expiry share the same ownership/schema contract. */
export const participantFromEntry = (entry: MeshStateEntry): FabricParticipantRecord | undefined => {
  if (!isObject(entry.value) || entry.value.format !== 1) return undefined;
  const value = entry.value as Partial<FabricParticipantRecord> & Record<string, unknown>;
  const kind = participantKind(value.kind);
  if (
    !kind ||
    (value.interactive !== undefined && typeof value.interactive !== "boolean") ||
    !remoteHostValid(value.remoteHost) ||
    !optionalStrings(value, ["sessionId", "cwd", "label", "role", "project", "projectRoot", "repository", "model", "thinking", "parentId"]) ||
    (value.remoteHost !== undefined && kind !== "root") ||
    typeof value.id !== "string" ||
    entry.key !== keyFor("topology/participants/", value.id) ||
    typeof value.rootId !== "string" ||
    typeof value.ownerHostId !== "string" ||
    typeof value.ownerIdentityId !== "string" ||
    entry.updatedBy?.id !== value.ownerIdentityId ||
    typeof value.name !== "string" ||
    typeof value.status !== "string" ||
    (value.runner !== "pi" && value.runner !== "claude" && value.runner !== "veda") ||
    typeof value.transport !== "string" || !transports.has(value.transport) ||
    !Array.isArray(value.capabilities) ||
    !value.capabilities.every(capability => typeof capability === "string" && capabilities.has(capability)) ||
    typeof value.startedAt !== "number" ||
    typeof value.updatedAt !== "number" ||
    (value.controlProtocol !== "v1" && value.controlProtocol !== "legacy")
  ) return undefined;
  return value as FabricParticipantRecord;
};

export const hostFromEntry = (entry: MeshStateEntry): FabricHostRecord | undefined => {
  if (!isObject(entry.value) || entry.value.format !== 1) return undefined;
  const value = entry.value as Partial<FabricHostRecord>;
  if (
    typeof value.id !== "string" ||
    entry.key !== keyFor("topology/hosts/", value.id) ||
    !remoteHostValid(value.remoteHost) ||
    typeof value.rootId !== "string" ||
    !isObject(value.identity) ||
    typeof value.identity.id !== "string" ||
    typeof value.identity.name !== "string" ||
    entry.updatedBy?.id !== value.identity.id ||
    (value.identity.kind !== "main" && value.identity.kind !== "agent" && value.identity.kind !== "actor") ||
    typeof value.startedAt !== "number" ||
    typeof value.updatedAt !== "number" ||
    typeof value.expiresAt !== "number"
  ) return undefined;
  return value as FabricHostRecord;
};
