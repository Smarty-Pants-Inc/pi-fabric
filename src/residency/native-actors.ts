// INTERNAL host composition. No application module is imported at registration or by Main.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { NativeActorBinding, NativeActorRegistration, NativeActorHandler } from "../actors/native-handler.js";
import type { ResidentHostConfig } from "./protocol.js";

const refused = (reason: string): never => { throw new Error(`Native resident configuration refused: ${reason}`); };
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const text = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0 && value.length <= 4096 && !value.includes("\0");
const absolute = (value: unknown): value is string => text(value) && path.isAbsolute(value) && path.resolve(value) === value;
const fields = (value: Record<string, unknown>, keys: readonly string[]): boolean =>
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));

type NativeConfig = Pick<ResidentHostConfig, "rootId" | "sessionId" | "mesh"> & { readonly nativeActors?: unknown };

/** Serializable declarations only. Own copies survive mutation of host extension/config objects. */
export const copyResidentNativeActorBindings = (config: NativeConfig): readonly NativeActorBinding[] => {
  if (config.nativeActors === undefined) return Object.freeze([]);
  if (!Array.isArray(config.nativeActors) || config.nativeActors.length > 64) return refused("invalid bindings array");
  if (config.nativeActors.length && (config.mesh.enabled !== true || config.mesh.transport !== "file" ||
      config.mesh.hostTransport === "nats" || config.mesh.actorScope !== "session" ||
      !/^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(config.rootId) ||
      config.rootId !== `session:${config.sessionId}`)) return refused("native actors require their canonical FILE/session owner");
  const ids = new Set<string>(), names = new Set<string>();
  return Object.freeze(config.nativeActors.map((value: unknown): NativeActorBinding => {
    if (!record(value) || !fields(value, ["actorId", "rootId", "scope", "name", "handlerKey", "residentModule", "ingress"])) {
      return refused("unsupported binding fields");
    }
    const { actorId, rootId, scope, name, handlerKey, residentModule, ingress } = value;
    if (typeof actorId !== "string" || !/^[0-9a-f]{32}$/.test(actorId) || rootId !== config.rootId || scope !== "session" ||
        !text(name) || !text(handlerKey) || !absolute(residentModule) || !record(ingress) ||
        !fields(ingress, ["link", "ledgerDir"]) || !text(ingress.link) || !absolute(ingress.ledgerDir) ||
        ids.has(actorId) || names.has(name)) return refused("invalid, foreign or duplicate binding");
    ids.add(actorId); names.add(name);
    return Object.freeze({ actorId, rootId, scope, name, handlerKey, residentModule,
      ingress: Object.freeze({ link: ingress.link, ledgerDir: ingress.ledgerDir }) });
  }));
};

/** Omission and an empty declaration set agree; every actual pin is equality-bearing. */
export const residentNativeActorBindingsMatch = (left: NativeConfig, right: NativeConfig): boolean => {
  try { return JSON.stringify(copyResidentNativeActorBindings(left)) === JSON.stringify(copyResidentNativeActorBindings(right)); }
  catch { return false; }
};

/** Same-UID replacement is outside the host trust boundary; no symlink discovery or repair. */
export const assertResidentNativeModulePath = (file: string): void => {
  const uid = process.geteuid?.();
  if (!absolute(file) || uid === undefined || process.platform === "win32") return refused("unsupported or relative module path");
  let current = path.parse(file).root;
  const parts = file.slice(current.length).split(path.sep);
  for (let index = 0; index < parts.length; index++) {
    current = path.join(current, parts[index]!);
    const stat = fs.lstatSync(current);
    const leaf = index === parts.length - 1;
    if (stat.isSymbolicLink() || (leaf ? !stat.isFile() || stat.uid !== uid : !stat.isDirectory() || (stat.uid !== uid && stat.uid !== 0)) ||
        ((stat.mode & 0o022) !== 0 && (leaf || stat.uid !== 0 || (stat.mode & 0o1000) === 0))) {
      return refused("module or parent chain is not trusted");
    }
  }
};

/** Called only by the real resident AFTER its OWNER fence and live directory lease. */
export const loadResidentNativeActorHandlers = async (
  bindings: readonly NativeActorBinding[],
  assertCustody: () => void,
): Promise<readonly NativeActorRegistration[]> => {
  const loaded: NativeActorRegistration[] = [];
  for (const binding of bindings) {
    assertCustody();
    assertResidentNativeModulePath(binding.residentModule);
    const module = await import(pathToFileURL(binding.residentModule).href) as Record<string, unknown>;
    assertCustody();
    assertResidentNativeModulePath(binding.residentModule);
    if (module.handlerKey !== binding.handlerKey || typeof module.handle !== "function") return refused("module key/export mismatch");
    // Code cannot choose bindings/grants, even if it exports additional configuration.
    loaded.push(Object.freeze({ binding, handler: module.handle as NativeActorHandler }));
  }
  assertCustody();
  return Object.freeze(loaded);
};

/** A failed close is not release authority while a callback still owns its real promise. */
export class NativeResidentCustodyBusyError extends Error {
  readonly code = "NATIVE_RESIDENT_CUSTODY_BUSY";
  constructor(options?: ErrorOptions) {
    super("Native resident callback has not settled; OWNER custody retained until settlement or confirmed process termination", options);
    this.name = "NativeResidentCustodyBusyError";
  }
}
