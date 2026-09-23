import { FABRIC_BASH_MIDDLEWARE, type FabricBashMiddlewareV1 } from "../protocol.js";

/** Invalid opt-ins fail closed: never silently run without an extension's filters. */
export const readFabricBashMiddleware = (definition: object | undefined): FabricBashMiddlewareV1 | undefined => {
  if (!definition || !(FABRIC_BASH_MIDDLEWARE in definition)) return undefined;
  const value = Reflect.get(definition, FABRIC_BASH_MIDDLEWARE) as unknown;
  const invalid = (): never => { throw new Error("Invalid Fabric bash middleware v1; refusing to bypass shell protection"); };
  if (!value || typeof value !== "object" || Array.isArray(value)) return invalid();
  const record = value as Record<string, unknown>;
  if (record.version !== 1 || typeof record.wrapOperations !== "function") return invalid();
  if (record.options !== undefined) {
    if (!record.options || typeof record.options !== "object" || Array.isArray(record.options)) return invalid();
    const options = record.options as Record<string, unknown>;
    const fields: Record<string, string> = {
      commandPrefix: "string", shellPath: "string", exposeSessionEnvironment: "boolean", spawnHook: "function",
    };
    for (const [key, option] of Object.entries(options)) {
      if (!Object.hasOwn(fields, key) || (option !== undefined && typeof option !== fields[key])) return invalid();
    }
  }
  return value as FabricBashMiddlewareV1;
};
