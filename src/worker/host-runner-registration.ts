/**
 * Registers the native SDK task entry's ExtensionRunner class for Fabric's tool
 * capture (src/capture/interceptor.ts, HOST_EXTENSION_RUNNER_SYMBOL). Kept apart
 * from the capture module so the task entry does not load it.
 *
 * The property is defined once, non-writable and non-configurable: task code
 * that runs later can neither delete nor replace it, so capture always patches
 * the live SDK runner that carries the profile guards' tool_call hooks
 * (smarty-dev#8606). A second registration throws instead of silently winning.
 */
export const HOST_EXTENSION_RUNNER_KEY = Symbol.for("pi-fabric.host-extension-runner.v1");

export const registerHostExtensionRunner = (runner: unknown, target: object = globalThis): void => {
  if (Object.prototype.hasOwnProperty.call(target, HOST_EXTENSION_RUNNER_KEY)) {
    throw new Error("Pi Fabric host ExtensionRunner is already registered");
  }
  Object.defineProperty(target, HOST_EXTENSION_RUNNER_KEY, {
    value: runner,
    writable: false,
    configurable: false,
    enumerable: false,
  });
};
