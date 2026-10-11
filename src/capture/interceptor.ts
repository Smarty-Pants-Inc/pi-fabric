import { existsSync, readdirSync, readFileSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type {
  ExtensionRunner,
  RegisteredTool,
  ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG, type FabricToolCaptureConfig } from "../config.js";
import { CapturedToolCatalog } from "./catalog.js";

type ToolCaptureListener = (tools: RegisteredTool[], runner: ExtensionRunner) => RegisteredTool[];

interface ToolCaptureHub {
  listeners: Set<ToolCaptureListener>;
}

export interface RegisteredToolCaptureController {
  setPolicy(config: FabricToolCaptureConfig): void;
  dispose(): void;
}

export interface RegisteredToolCaptureOptions {
  anchorDefinition: ToolDefinition<any, any, any>;
  catalog: CapturedToolCatalog;
  initialPolicy?: FabricToolCaptureConfig;
  // Called after each refresh of the captured catalog so callers can re-assert
  // active-tool ownership. Tools are deliberately left in Pi's registry — the
  // listener observes rather than filters — because extensions that gate tool
  // calls against `pi.getAllTools()` (e.g. permission systems) must still see
  // captured tools as registered; hiding from the model happens exclusively in
  // the active tool set (see FabricToolOwnership).
  onCatalogRefresh?: () => void;
}

const HUB_SYMBOL = Symbol.for("pi-fabric.registered-tool-capture.v1");
/**
 * Set by Fabric's native SDK task entry (src/worker/task-entry.ts) to the
 * ExtensionRunner class of the SDK it runs. There process.argv[1] is the entry
 * script, so neither argv lookup below finds the host package and the
 * in-realm fallback patches Fabric's private peer copy instead (#8606).
 */
export const HOST_EXTENSION_RUNNER_SYMBOL = Symbol.for("pi-fabric.host-extension-runner.v1");
const ANCHOR_SYMBOL = Symbol.for("pi-fabric.registered-tool-anchor.v1");

const definitionDelegatesTo = (
  definition: ToolDefinition<any, any, any>,
  target: ToolDefinition<any, any, any>,
): boolean => {
  let current: object | null = definition;
  while (current) {
    if (current === target) return true;
    current = Object.getPrototypeOf(current) as object | null;
  }
  return false;
};

const clonePolicy = (config: FabricToolCaptureConfig): FabricToolCaptureConfig => ({
  enabled: config.enabled,
  hideFromModel: config.hideFromModel,
  keepVisible: [...config.keepVisible],
  defaultRisk: config.defaultRisk,
  risks: { ...config.risks },
});

type ExtensionRunnerConstructor = {
  prototype: ExtensionRunner;
};

const isExtensionRunnerConstructor = (value: unknown): value is ExtensionRunnerConstructor =>
  typeof value === "function" &&
  typeof (value as { prototype?: unknown }).prototype === "object" &&
  typeof ((value as { prototype: Record<string, unknown> }).prototype).getAllRegisteredTools ===
    "function";

// pi >= 0.84.3 loads the CLI from dist/bundle/cli.js, whose rollup chunks carry
// their own ExtensionRunner class identity — the library-level patch alone
// never fires because the live host runner is an instance of the bundle's copy.
// Importing each chunk inside the running CLI is a Node module-cache hit, so
// scanning the bundle is free and yields the class the host actually runs.
export const bundleExtensionRunnerConstructors = async (
  bundleDir: string,
): Promise<ExtensionRunnerConstructor[]> => {
  const chunksDir = path.join(bundleDir, "chunks");
  if (!existsSync(chunksDir)) return [];
  let files: string[];
  try {
    files = readdirSync(chunksDir);
  } catch {
    return [];
  }
  const constructors = new Set<ExtensionRunnerConstructor>();
  for (const file of files) {
    if (!file.endsWith(".js")) continue;
    try {
      const module = (await import(pathToFileURL(path.join(chunksDir, file)).href)) as Record<
        string,
        unknown
      >;
      for (const exported of Object.values(module)) {
        if (isExtensionRunnerConstructor(exported)) constructors.add(exported);
      }
    } catch { /* chunk not importable in this realm (worker entries, natives); skip */ }
  }
  return [...constructors];
};

const captureHub = (Runner: ExtensionRunnerConstructor): ToolCaptureHub => {
  const prototype = Runner.prototype as ExtensionRunner & Record<PropertyKey, unknown>;
  const existing = prototype[HUB_SYMBOL] as ToolCaptureHub | undefined;
  if (existing) return existing;

  const original = prototype.getAllRegisteredTools;
  if (typeof original !== "function") {
    throw new Error("Pi Fabric could not intercept ExtensionRunner.getAllRegisteredTools");
  }

  const hub: ToolCaptureHub = { listeners: new Set() };
  Object.defineProperty(prototype, HUB_SYMBOL, {
    value: hub,
    configurable: false,
    enumerable: false,
    writable: false,
  });
  prototype.getAllRegisteredTools = function getFabricVisibleTools(): RegisteredTool[] {
    let tools = original.call(this);
    for (const listener of [...hub.listeners]) tools = listener(tools, this);
    return tools;
  };
  return hub;
};

const PI_PACKAGE_NAME = "@earendil-works/pi-coding-agent";

const piPackageRootAbove = (startDirectory: string): string | undefined => {
  let directory = startDirectory;
  while (directory !== path.dirname(directory)) {
    const manifestPath = path.join(directory, "package.json");
    if (existsSync(manifestPath)) {
      try {
        const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as { name?: unknown };
        if (manifest.name === PI_PACKAGE_NAME) return directory;
      } catch { /* unreadable or invalid manifest; keep searching */ }
    }
    directory = path.dirname(directory);
  }
  return undefined;
};

const hostPackageRoot = (): string | undefined => {
  const cliPath = process.argv[1];
  if (!cliPath) return undefined;
  try {
    return piPackageRootAbove(path.dirname(realpathSync(cliPath)));
  } catch {
    return undefined;
  }
};

// Embedded hosts (pi-web sessiond, SDK embeds) instantiate ExtensionRunner from
// their own node_modules copy while this extension runs from a separate package
// realm: the in-realm fallback import below resolves this extension's private
// dependency and patches a class identity the live runner never uses, leaving
// the captured catalog permanently empty (extensions.* exposes nothing). Node
// caches modules by absolute path, so locating the host's copy through the
// process entry's module search path and importing it yields the very class
// the host instantiates.
const entryRealmPackageRoot = (): string | undefined => {
  const cliPath = process.argv[1];
  if (!cliPath) return undefined;
  let entryDirectory: string;
  try {
    entryDirectory = path.dirname(realpathSync(cliPath));
  } catch {
    return undefined;
  }
  // The package exports map is ESM-only, so plain resolve() fails with
  // ERR_PACKAGE_PATH_NOT_EXPORTED; resolve.paths() lists the node_modules
  // directories Node would search regardless of exports.
  const searchPaths = createRequire(path.join(entryDirectory, "probe.js"))
    .resolve.paths(PI_PACKAGE_NAME);
  for (const searchPath of searchPaths ?? []) {
    const manifestPath = path.join(searchPath, PI_PACKAGE_NAME, "package.json");
    if (!existsSync(manifestPath)) continue;
    try {
      const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as { name?: unknown };
      if (manifest.name === PI_PACKAGE_NAME) return path.join(searchPath, PI_PACKAGE_NAME);
    } catch { /* unreadable or invalid manifest; keep searching */ }
  }
  return undefined;
};

// Every runner method Fabric's capture and its tool_call/tool_result paths call
// on a captured runner (catalog, wrapper, shell preflight, providers).
const CAPTURED_RUNNER_METHODS = [
  "getAllRegisteredTools",
  "getActiveTools",
  "createContext",
  "emit",
  "emitToolCall",
  "emitToolResult",
] as const;

/**
 * The task entry's registration, validated. Absent means no native task entry.
 * Anything else that is not a plain data property holding an ExtensionRunner
 * constructor fails closed: capturing a bogus class would leave the live SDK
 * runner unpatched and Fabric's shell paths outside the profile guards (#8606).
 */
export const registeredHostExtensionRunner = (): ExtensionRunnerConstructor | undefined => {
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, HOST_EXTENSION_RUNNER_SYMBOL);
  if (!descriptor) return undefined;
  const refuse = (reason: string): never => {
    throw new Error(`Pi Fabric refuses the host ExtensionRunner registered under ${String(HOST_EXTENSION_RUNNER_SYMBOL)}: ${reason}`);
  };
  if (!("value" in descriptor)) refuse("it is an accessor, not a data property");
  const value: unknown = descriptor.value;
  if (typeof value !== "function") refuse("it is not a constructor");
  const prototype = (value as { prototype?: unknown }).prototype;
  if (typeof prototype !== "object" || prototype === null) refuse("its prototype is not an object");
  const missing = CAPTURED_RUNNER_METHODS.filter(
    (method) => typeof (prototype as Record<string, unknown>)[method] !== "function",
  );
  if (missing.length) refuse(`its prototype lacks ${missing.join(", ")}`);
  return value as ExtensionRunnerConstructor;
};

const extensionRunnerConstructors = async (): Promise<ExtensionRunnerConstructor[]> => {
  const constructors = new Set<ExtensionRunnerConstructor>();
  const registered = registeredHostExtensionRunner();
  if (registered) constructors.add(registered);
  const packageRoots = new Set(
    [process.env.PI_PACKAGE_DIR, hostPackageRoot(), entryRealmPackageRoot()].filter(
      (root): root is string => typeof root === "string" && Boolean(root),
    ),
  );
  for (const packageRoot of packageRoots) {
    try {
      const hostEntry = path.join(packageRoot, "dist", "index.js");
      const hostModule = (await import(pathToFileURL(hostEntry).href)) as {
        ExtensionRunner?: ExtensionRunnerConstructor;
      };
      if (hostModule.ExtensionRunner) constructors.add(hostModule.ExtensionRunner);
    } catch { /* host entry not importable; skip */ }
    for (const Runner of await bundleExtensionRunnerConstructors(
      path.join(packageRoot, "dist", "bundle"),
    )) {
      constructors.add(Runner);
    }
  }
  if (constructors.size === 0) {
    // The host does not advertise its package directory (tests, embeds,
    // future layouts): fall back to resolving it in this module realm.
    try {
      const hostModule = (await import("@earendil-works/pi-coding-agent")) as {
        ExtensionRunner?: ExtensionRunnerConstructor;
      };
      if (hostModule.ExtensionRunner) constructors.add(hostModule.ExtensionRunner);
    } catch { /* host unavailable in this realm; tool capture stays inert */ }
  }
  return [...constructors];
};

export const installRegisteredToolCapture = async (
  options: RegisteredToolCaptureOptions,
): Promise<RegisteredToolCaptureController> => {
  const hubs = (await extensionRunnerConstructors()).map(captureHub);
  const anchorToken = {};
  Object.defineProperty(options.anchorDefinition, ANCHOR_SYMBOL, {
    value: anchorToken,
    configurable: false,
    enumerable: true,
    writable: false,
  });
  let policy = clonePolicy(options.initialPolicy ?? DEFAULT_FABRIC_CONFIG.capture);
  let disposed = false;

  const listener: ToolCaptureListener = (tools, runner) => {
    if (disposed) return tools;
    const anchor = tools.find(
      (tool) =>
        (tool.definition as unknown as Record<PropertyKey, unknown>)[ANCHOR_SYMBOL] ===
          anchorToken || definitionDelegatesTo(tool.definition, options.anchorDefinition),
    );
    if (!anchor) return tools;

    options.catalog.replace(tools, runner, policy, anchor.sourceInfo.path);
    options.onCatalogRefresh?.();
    return tools;
  };

  for (const hub of hubs) hub.listeners.add(listener);
  return {
    setPolicy(config) {
      policy = clonePolicy(config);
      if (!policy.enabled) options.catalog.clear();
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const hub of hubs) hub.listeners.delete(listener);
      options.catalog.clear();
    },
  };
};
