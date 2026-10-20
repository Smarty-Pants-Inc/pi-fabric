import type { ExtensionAPI, ExtensionFactory } from "@earendil-works/pi-coding-agent";
import { createEventBus } from "../node_modules/@earendil-works/pi-coding-agent/dist/core/event-bus.js";
import { createExtensionRuntime, loadExtensionFromFactory } from "../node_modules/@earendil-works/pi-coding-agent/dist/core/extensions/loader.js";
import { requestSessionChannelApi, registerSessionChannelApi, type FabricSessionChannelApi } from "../src/transport/nats/session-channel-api.js";

const testModule = process.versions.bun ? "bun:test" : "vitest";
const { describe, expect, it } = await import(/* @vite-ignore */ testModule) as typeof import("vitest");

const sessionChannelApi = Object.freeze({ open: async () => { throw new Error("not opened in API handoff test"); } });

describe("Pi extension session-channel API handoff", () => {
  it.each(["Fabric first", "consumer first"] as const)("shares the API with two independently-created pi objects (%s)", async (order) => {
    const eventBus = createEventBus();
    const runtime = createExtensionRuntime();
    let fabricPi: ExtensionAPI | undefined;
    let consumerApi: Promise<FabricSessionChannelApi> | undefined;
    const fabric: ExtensionFactory = (pi) => {
      fabricPi = pi;
      registerSessionChannelApi(pi, sessionChannelApi, pi);
    };
    const consumer: ExtensionFactory = (pi) => {
      consumerApi = requestSessionChannelApi(pi);
    };
    const extensions = order === "Fabric first"
      ? [["fabric", fabric], ["consumer", consumer]] as const
      : [["consumer", consumer], ["fabric", fabric]] as const;

    for (const [name, factory] of extensions) {
      await loadExtensionFromFactory(factory, process.cwd(), eventBus, runtime, `<inline:${name}>`);
    }

    expect(await consumerApi).toBe(sessionChannelApi);
    expect((fabricPi as (ExtensionAPI & { fabric?: { sessionChannel?: unknown } }) | undefined)?.fabric?.sessionChannel)
      .toBe(sessionChannelApi);
  });
});
