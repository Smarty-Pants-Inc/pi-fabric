import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { installSelfReload, takeSelfReload } from "../src/lifecycle/self-reload.js";

const slotReleases = vi.hoisted(() => [] as Array<ReturnType<typeof vi.fn>>);
vi.mock("../src/lifecycle/reload-slots.js", async original => {
  const slots = await original<typeof import("../src/lifecycle/reload-slots.js")>();
  return { ...slots, tryAcquireReloadSlot: (...args: Parameters<typeof slots.tryAcquireReloadSlot>) => {
    const release = slots.tryAcquireReloadSlot(...args);
    if (!release) return undefined;
    const tracked = vi.fn(release); slotReleases.push(tracked); return tracked;
  } };
});

let root: string;
const stops: Array<() => void> = [];
const jobs: Array<Promise<unknown>> = [];
const gates: Array<() => void> = [];
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  gates.push(resolve);
  return { promise, resolve };
};
beforeEach(() => {
  slotReleases.length = 0;
  root = fs.mkdtempSync(path.join(os.tmpdir(), "self-reload-activation-"));
  for (const name of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_NO_AUTO_RELOAD"]) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", root);
  vi.spyOn(FabricState.prototype, "bootstrap").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "shouldEagerlyActivate").mockReturnValue(false);
});
afterEach(async () => {
  gates.splice(0).forEach(resolve => resolve());
  await Promise.allSettled(jobs.splice(0));
  stops.splice(0).forEach(stop => stop());
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async () => {
  const old = path.join(root, "old"), next = path.join(root, "next"), slots = path.join(root, "slots");
  for (const dir of [old, next]) {
    fs.mkdirSync(dir); fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: "pi-fabric" }));
  }
  const settingsPath = path.join(root, "settings.json");
  fs.writeFileSync(settingsPath, JSON.stringify({ packages: [old] }));
  const main = (id: string) => {
    const handlers = new Map<string, (event: any, ctx: any) => unknown>();
    let command!: { handler: (args: string, ctx: any) => Promise<void> };
    const context = { mode: "rpc", cwd: root, hasUI: false, isIdle: () => true,
      isProjectTrusted: () => false, hasPendingMessages: () => false,
      sessionManager: { getSessionId: () => `${root}-${id}`, getEntries: () => [], getBranch: () => [] },
      reload: vi.fn(async () => {}), ui: { notify: vi.fn(), setStatus: vi.fn() } };
    const pi = { on: (name: string, handler: any) => handlers.set(name, handler),
      registerCommand: (_name: string, value: any) => { command = value; }, sendUserMessage: vi.fn() };
    const controller = installSelfReload(pi as never, { busy: () => 0, autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(old, "index.js")).href, settingsPath,
      selfReloadConcurrency: () => 1, reloadJitterMs: () => 0, reloadSlotsDirectory: slots });
    controller.sessionStart("startup", context as never);
    stops.push(() => handlers.get("session_shutdown")?.({}, context));
    return { context, execute: () => command.handler("auto", context) };
  };
  const first = main("first"), second = main("second");
  // Use the actual index.ts session_start path, with only runtime work injected.
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
  const freshPi = { events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), setActiveTools: vi.fn(),
    registerCommand: vi.fn(), registerTool: vi.fn(), registerMessageRenderer: vi.fn(),
    on: (name: string, handler: any) => handlers.set(name, [...(handlers.get(name) ?? []), handler]) };
  await piFabric(freshPi as unknown as ExtensionAPI);
  const activation = () => handlers.get("session_start")![0]!({ reason: "reload" }, first.context as unknown as ExtensionContext);
  fs.writeFileSync(settingsPath, JSON.stringify({ packages: [next] }));
  const date = new Date(Date.now() + 1000); fs.utimesSync(settingsPath, date, date);
  return { first, second, activation, slots };
};

it("releases an unclaimed native failure once and does not transfer its spent callback", async () => {
  const { first, second, slots } = await fixture();
  first.context.reload.mockRejectedValue(new Error("native failure"));
  await expect(first.execute()).rejects.toThrow("native failure");
  expect(slotReleases[0]).toHaveBeenCalledTimes(1);
  expect(fs.readdirSync(slots)).toEqual([]);
  expect(takeSelfReload(first.context.sessionManager.getSessionId(), "reload")).not.toHaveProperty("releaseSlot");
  await second.execute(); expect(second.context.reload).toHaveBeenCalledOnce();
  expect(slotReleases[0]).toHaveBeenCalledTimes(1);
});

it.each(["success", "ensure failure", "publish failure"].flatMap(outcome =>
  [false, true].map(nativeEarly => ({ outcome, nativeEarly }))))(
  "holds N=1 until actual session_start settles ($outcome, nativeEarly=$nativeEarly)", async ({ outcome, nativeEarly }) => {
  const ensureGate = deferred(), publishGate = deferred(), nativeGate = deferred();
  const ensure = vi.spyOn(FabricState.prototype, "ensure").mockImplementation(async () => {
    await ensureGate.promise;
    if (outcome === "ensure failure") throw new Error(outcome);
    return { current: () => true };
  });
  const publish = vi.spyOn(FabricState.prototype, "publishOpsEvent").mockImplementation(async () => {
    await publishGate.promise;
    if (outcome === "publish failure") throw new Error(outcome);
  });
  const { first, second, activation, slots } = await fixture();
  first.context.reload.mockImplementation(() => nativeGate.promise);
  const native = first.execute(); jobs.push(native);
  await vi.waitFor(() => expect(first.context.reload).toHaveBeenCalledOnce());
  const activated = Promise.resolve(activation()); jobs.push(activated);
  // Install rejection observation before releasing either asynchronous boundary.
  const settled = activated.then(() => "success", error => error.message);
  await vi.waitFor(() => expect(ensure).toHaveBeenCalledOnce());
  if (nativeEarly) { nativeGate.resolve(); await native; }
  expect(slotReleases[0]).not.toHaveBeenCalled();
  await second.execute();
  expect(second.context.reload, "ensure is still activating; a second Main must not enter").not.toHaveBeenCalled();
  expect(fs.readdirSync(slots)).toEqual(["slot-0"]);
  ensureGate.resolve();
  if (outcome !== "ensure failure") {
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    await second.execute();
    expect(second.context.reload, "publish has not settled; the lease must still be held").not.toHaveBeenCalled();
    expect(publish.mock.calls[0]![2]).not.toHaveProperty("releaseSlot");
  }
  publishGate.resolve();
  expect(await settled).toBe(outcome);
  expect(fs.readdirSync(slots)).toEqual([]);
  expect(slotReleases[0]).toHaveBeenCalledTimes(1);
  expect(takeSelfReload(first.context.sessionManager.getSessionId(), "reload")).toBeUndefined();
  // Activation, not resolution of the old native command, releases capacity.
  await second.execute(); expect(second.context.reload).toHaveBeenCalledOnce();
  nativeGate.resolve(); await native;
  expect(slotReleases[0]).toHaveBeenCalledTimes(1);
});
