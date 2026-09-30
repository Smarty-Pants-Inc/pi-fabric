import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

const checkerLoaded = vi.hoisted(() => vi.fn());
vi.mock("../src/coordination/unverified-ids.js", async original => {
  checkerLoaded();
  return original<typeof import("../src/coordination/unverified-ids.js")>();
});
afterEach(() => vi.unstubAllEnvs());

describe("message identifier checker first-use boundary", () => {
  it("stays unloaded during cold registration, idle hooks and no-id sends; loads on a candidate", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-id-startup-"));
    fs.mkdirSync(path.join(cwd, "agent"));
    fs.mkdirSync(path.join(cwd, ".pi"));
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
      prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [],
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST"]) vi.stubEnv(key, undefined);
    type Handler = (event: unknown, context: ExtensionContext) => unknown;
    const handlers = new Map<string, Handler[]>();
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      on: vi.fn((event: string, handler: Handler) => handlers.set(event, [...(handlers.get(event) ?? []), handler])),
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
      setActiveTools: vi.fn(), sendMessage: vi.fn(),
    } as unknown as ExtensionAPI;
    const context = {
      cwd, hasUI: false, isProjectTrusted: () => true,
      sessionManager: { getSessionId: () => "startup-session", getBranch: () => [] },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const emit = async (event: string) => {
      for (const handler of handlers.get(event) ?? []) await handler({}, context);
    };
    try {
      vi.resetModules();
      const { default: register } = await import("../src/index.js");
      await register(pi);
      expect(checkerLoaded).not.toHaveBeenCalled();
      for (const event of ["resources_discover", "session_start"]) {
        await emit(event);
        expect(checkerLoaded).not.toHaveBeenCalled();
      }
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(checkerLoaded).not.toHaveBeenCalled();
      const { outgoingMessageNotice } = await import("../src/providers/message-id-notice.js");
      for (const text of ["Ready to review", "decafed abc1234 is prose", "color abcdef12", "comment 12", "pid nope", "session:not-an-id"]) {
        expect(await outgoingMessageNotice(text, { extensionContext: context } as Parameters<typeof outgoingMessageNotice>[1])).toEqual({ text });
      }
      expect(checkerLoaded).not.toHaveBeenCalled();
      expect(await outgoingMessageNotice("head abc1234", { extensionContext: context } as Parameters<typeof outgoingMessageNotice>[1]))
        .toHaveProperty("notice", "unverified ids: check failed");
      expect(checkerLoaded).toHaveBeenCalledOnce();
    } finally {
      await emit("session_shutdown");
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });
});
