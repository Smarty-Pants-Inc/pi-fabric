export const NODE_PROCESS_CHILD_SOURCE = String.raw`
import vm from "node:vm";

const pending = new Map();
let nextCallId = 0;

const send = (message) => {
  if (process.connected) process.send?.(message);
};

const formatValue = (value) => {
  if (typeof value === "string") return value;
  if (value instanceof Error) return value.stack ?? value.message;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const jsonCompatible = (value) => {
  if (value === undefined) return undefined;
  const serialized = JSON.stringify(value);
  return serialized === undefined ? undefined : JSON.parse(serialized);
};

const run = async (message) => {
  const logs = [];
  let logChars = 0;
  let logsTruncated = false;
  const hostCall = (ref, args) => {
    const id = nextCallId++;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      send({
        type: "call",
        id,
        ref: String(ref),
        args:
          typeof args === "object" && args !== null && !Array.isArray(args)
            ? args
            : {},
      });
    });
  };
  const print = (...values) => {
    if (logsTruncated) return;
    const line = values.map(formatValue).join(" ");
    const remaining = message.maxLogChars - logChars;
    if (line.length > remaining) {
      if (remaining > 0) logs.push(line.slice(0, remaining));
      logs.push("[Pi Fabric log output truncated]");
      logsTruncated = true;
      return;
    }
    logs.push(line);
    logChars += line.length;
  };

  // Bun module namespace for the guest. The child resolves the specifier in
  // its own module context; under the node runtime it fails and the binding
  // stays undefined. Namespace objects come from this realm, so instanceof
  // against guest-realm classes fails (same caveat as other sandbox bridges).
  const __bun = await import("bun").catch(() => undefined);
  const sandbox = {
    __fabricHostCall: hostCall,
    __fabricTokenBudget: message.tokenBudget ?? Number.POSITIVE_INFINITY,
    // Native-module bridge for guests whose vm lacks the dynamic-import
    // callback. Bun >= 1.4 honors importModuleDynamically on runInContext
    // (natural import() works there), so this is mostly a fallback and an
    // explicit escape hatch. Cross-realm caveat: namespaces come from this
    // realm, so instanceof against guest-realm classes fails.
    __fabricImport: (specifier) => import(specifier),
    // Resolved above; undefined under the node runtime.
    __bun,
    print,
    π: jsonCompatible(message.strings),
  };
  const context = vm.createContext(sandbox, {
    name: "pi-fabric-node-process",
    codeGeneration: { strings: true, wasm: false },
  });

  try {
    vm.runInContext(message.setup, context, { filename: "pi-fabric-setup.js" });
    const promise = vm.runInContext(message.code + "\n__piFabricMain()", context, {
      filename: "pi-fabric-guest.js",
      // Node requires --experimental-vm-modules (set at spawn). Bun >= 1.4
      // honors this option on runInContext (unlike on createContext), so
      // natural guest import() works there; __fabricImport is a fallback.
      importModuleDynamically: (specifier) => import(specifier),
    });
    const value = jsonCompatible(await promise);
    send({ type: "result", result: { value, logs, terminationReason: "completed" } });
  } catch (error) {
    send({
      type: "result",
      result: {
        logs,
        terminationReason: "runtime_error",
        error: error?.stack ?? error?.message ?? String(error),
      },
    });
  }
};

process.on("message", (message) => {
  if (typeof message !== "object" || message === null) return;
  if (message.type === "execute") {
    void run(message);
    return;
  }
  if (message.type !== "response") return;
  const operation = pending.get(message.id);
  if (!operation) return;
  pending.delete(message.id);
  if (message.ok) operation.resolve(message.value);
  else {
    const error = new Error(message.error ?? "Host call failed");
    if (message.bashExit) error.__fabricBashExit = message.bashExit;
    operation.reject(error);
  }
});
`;
