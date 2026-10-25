import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as nativeActors from "../src/native-actors.js";
import type { NativeActorBinding, NativeActorHandler } from "../src/native-actors.js";
import { snapshotNativeActorHandlers } from "../src/actors/native-handler.js";

// Keep reason: host registration is a trust boundary. Pin substitution, stale
// disposal or accidental model/loader entry can change who executes an actor.
const unexpectedLoad = vi.hoisted(() => vi.fn((module: string): never => {
  throw new Error(`Native actor registration unexpectedly loaded ${module}`);
}));
vi.mock("../src/actors/manager.js", () => unexpectedLoad("ActorManager"));
vi.mock("../src/agents/manager.js", () => unexpectedLoad("AgentManager"));
vi.mock("../src/agents/model-route.js", () => unexpectedLoad("model routing"));
vi.mock("../src/core/host-sdk.js", () => unexpectedLoad("Pi host SDK"));
vi.mock("@earendil-works/pi-ai", () => unexpectedLoad("pi-ai"));
vi.mock("@earendil-works/pi-coding-agent", () => unexpectedLoad("Pi SDK"));
vi.mock("@pydantic/monty", () => unexpectedLoad("Monty"));
vi.mock("shiki", () => unexpectedLoad("Shiki"));
vi.mock("node:fs", () => unexpectedLoad("filesystem"));
vi.mock("node:fs/promises", () => unexpectedLoad("filesystem promises"));

const ROOT_A = "session:abcdefab-1234-4567-89ab-abcdef012345";
const ROOT_B = "session:abcdefab-1234-4567-89ab-abcdef012346";
const ACTOR_A = "a".repeat(32);
const ACTOR_B = "b".repeat(32);
const echo: NativeActorHandler = async ({ data }) => ({ data });
const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const dispose of cleanups.splice(0).reverse()) dispose();
  vi.clearAllMocks();
});

const bindingFor = () => ({
  actorId: ACTOR_A,
  rootId: ROOT_A,
  scope: "session" as const,
  name: "native-test",
  handlerKey: "native-test.rpc.v1",
  // Deliberately nonexistent: declaration registration must not touch disk.
  residentModule: path.resolve("nonexistent-native-test", "handler.mjs"),
  ingress: { link: "native-test-link", ledgerDir: path.resolve("nonexistent-native-test", "ledger") },
});

const register = (binding: NativeActorBinding, handler: NativeActorHandler = echo): (() => void) => {
  const dispose = nativeActors.registerNativeActorHandler(binding, handler);
  cleanups.push(dispose);
  return dispose;
};

const invalidBindings: Array<[string, Record<string, unknown>, RegExp]> = [
  ["uppercase actor ID", { actorId: "A".repeat(32) }, /actorId/],
  ["short actor ID", { actorId: "a".repeat(31) }, /actorId/],
  ["actor ID newline", { actorId: `${ACTOR_A}\n` }, /actorId/],
  ["nonhex actor ID", { actorId: "g".repeat(32) }, /actorId/],
  ["non-session root", { rootId: ROOT_A.replace("session:", "project:") }, /rootId/],
  ["uppercase UUID", { rootId: ROOT_A.toUpperCase().replace("SESSION:", "session:") }, /rootId/],
  ["noncanonical UUID", { rootId: ROOT_A.replaceAll("-", "") }, /rootId/],
  ["root newline", { rootId: `${ROOT_A}\n` }, /rootId/],
  ["project scope", { scope: "project" }, /scope/],
  ["missing scope", { scope: undefined }, /scope/],
  ["relative module", { residentModule: "relative/handler.mjs" }, /residentModule/],
  ["module URL", { residentModule: "file:///handler.mjs" }, /residentModule/],
  ["NUL in module", { residentModule: `${path.resolve("handler.mjs")}\0` }, /residentModule/],
  ["relative ledger", { ingress: { link: "link", ledgerDir: "relative/ledger" } }, /ledgerDir/],
  ["missing ingress", { ingress: undefined }, /ingress/],
  ["empty link", { ingress: { link: "", ledgerDir: path.resolve("ledger") } }, /link/],
  ["empty name", { name: " " }, /name/],
  ["empty handler key", { handlerKey: "" }, /handlerKey/],
];

describe("host-only native actor registration", () => {
  it("registers the exact binding and callback, snapshots it, and unregisters without fallback", () => {
    const binding = bindingFor();
    const handler = vi.fn<NativeActorHandler>(echo);
    const empty = snapshotNativeActorHandlers(ROOT_A, "session");
    const dispose = register(binding, handler);
    const snapshot = snapshotNativeActorHandlers(ROOT_A, "session");
    expect(snapshot).toEqual([{ binding, handler }]);
    expect(snapshot[0]!.binding).not.toBe(binding);
    expect(snapshot[0]!.binding.ingress).not.toBe(binding.ingress);
    expect(snapshot[0]!.handler).toBe(handler);
    expect(empty).toEqual([]);
    expect(handler).not.toHaveBeenCalled();
    dispose();
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([]);
    expect(snapshot).toEqual([{ binding, handler }]);
  });

  it("rejects duplicates and every conflicting pin without replacing the original", () => {
    const binding = bindingFor();
    register(binding);
    const snapshot = snapshotNativeActorHandlers(ROOT_A, "session");
    const otherHandler: NativeActorHandler = async () => ({ data: "other" });
    for (const conflict of [
      { ...binding },
      { ...binding, name: "other-name" },
      { ...binding, handlerKey: "other-key" },
      { ...binding, residentModule: path.resolve("other-handler.mjs") },
      { ...binding, ingress: { ...binding.ingress, link: "other-link" } },
      { ...binding, ingress: { ...binding.ingress, ledgerDir: path.resolve("other-ledger") } },
    ]) {
      expect(() => register(conflict)).toThrow(/already registered/);
    }
    expect(() => register(binding, otherHandler)).toThrow(/already registered/);
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual(snapshot);
  });

  it("scopes identical actor/key pins by exact root and allows distinct actors sharing a key", () => {
    const first = bindingFor();
    const otherRoot = { ...first, rootId: ROOT_B, name: "other-root" };
    const otherActor = { ...first, actorId: ACTOR_B, name: "other-actor" };
    register(first);
    register(otherRoot);
    register(otherActor);
    expect(snapshotNativeActorHandlers(ROOT_A, "session").map(({ binding }) => binding))
      .toEqual([first, otherActor]);
    expect(snapshotNativeActorHandlers(ROOT_B, "session").map(({ binding }) => binding)).toEqual([otherRoot]);
  });

  it.each(invalidBindings)("rejects %s before publishing any declaration", (_name, patch, error) => {
    expect(() => register({ ...bindingFor(), ...patch } as never)).toThrow(error);
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([]);
  });

  it("rejects malformed inputs and snapshot selectors at runtime, not just in TypeScript", () => {
    for (const value of [null, [], "binding"]) {
      expect(() => nativeActors.registerNativeActorHandler(value as never, echo)).toThrow(/binding/);
    }
    for (const handler of [undefined, null, {}, 1]) {
      expect(() => nativeActors.registerNativeActorHandler(bindingFor(), handler as never)).toThrow(/handler/);
    }
    expect(() => snapshotNativeActorHandlers("session:not-a-uuid", "session")).toThrow(/rootId/);
    expect(() => snapshotNativeActorHandlers(ROOT_A, "project" as never)).toThrow(/scope/);
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([]);
  });

  it("copies all caller-owned pins so later mutations cannot change identity or disposal", () => {
    const binding = bindingFor();
    const ingress = binding.ingress;
    const expected = { ...binding, ingress: { ...ingress } };
    const dispose = register(binding);
    const snapshot = snapshotNativeActorHandlers(ROOT_A, "session");
    expect(Object.isFrozen(binding)).toBe(false);
    expect(Object.isFrozen(ingress)).toBe(false);
    binding.actorId = ACTOR_B;
    binding.rootId = ROOT_B;
    Reflect.set(binding, "scope", "project");
    binding.name = "mutated";
    binding.handlerKey = "mutated";
    binding.residentModule = path.resolve("mutated-handler.mjs");
    ingress.link = "mutated";
    ingress.ledgerDir = path.resolve("mutated-ledger");
    binding.ingress = { link: "replaced", ledgerDir: path.resolve("replaced-ledger") };
    expect(snapshot).toEqual([{ binding: expected, handler: echo }]);
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual(snapshot);
    expect(snapshotNativeActorHandlers(ROOT_B, "session")).toEqual([]);
    dispose();
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([]);
  });

  it("freezes snapshots and lets a disposer remove only its own registration", () => {
    const binding = bindingFor();
    const oldDispose = register(binding);
    const snapshot = snapshotNativeActorHandlers(ROOT_A, "session");
    const entry = snapshot[0]!;
    for (const value of [snapshot, entry, entry.binding, entry.binding.ingress]) {
      expect(Object.isFrozen(value)).toBe(true);
    }
    expect(Reflect.set(snapshot, "length", 0)).toBe(false);
    expect(Reflect.set(entry, "handler", async () => ({ data: "forged" }))).toBe(false);
    expect(Reflect.set(entry.binding, "residentModule", path.resolve("forged.mjs"))).toBe(false);
    expect(Reflect.set(entry.binding.ingress, "link", "forged")).toBe(false);
    oldDispose();
    oldDispose();
    const empty = snapshotNativeActorHandlers(ROOT_A, "session");
    expect(Object.isFrozen(empty)).toBe(true);
    const replacement: NativeActorHandler = async () => ({ data: "replacement" });
    const changed = { ...binding, name: "replacement" };
    const newDispose = register(changed, replacement);
    oldDispose();
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([{ binding: changed, handler: replacement }]);
    expect(snapshot).toEqual([{ binding, handler: echo }]);
    expect(empty).toEqual([]);
    newDispose();
    expect(snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([]);
  });

  it("shares the singleton across separately evaluated module copies", async () => {
    const binding = bindingFor();
    const dispose = register(binding);
    vi.resetModules();
    const secondPublic = await import("../src/native-actors.js");
    const secondInternal = await import("../src/actors/native-handler.js");
    expect(secondPublic.registerNativeActorHandler).not.toBe(nativeActors.registerNativeActorHandler);
    expect(secondInternal.snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([{ binding, handler: echo }]);
    expect(() => secondPublic.registerNativeActorHandler(binding, echo)).toThrow(/already registered/);
    dispose();
    expect(secondInternal.snapshotNativeActorHandlers(ROOT_A, "session")).toEqual([]);
  });

  it("exposes only registration and does not load filesystem, model or optional engines on import/use", async () => {
    vi.resetModules();
    const publicApi = await import("../src/native-actors.js");
    expect(Object.keys(publicApi)).toEqual(["registerNativeActorHandler"]);
    const dispose = publicApi.registerNativeActorHandler(bindingFor(), echo);
    cleanups.push(dispose);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(unexpectedLoad).not.toHaveBeenCalled();
    expect(snapshotNativeActorHandlers(ROOT_A, "session")[0]?.handler).toBe(echo);
  });
});
