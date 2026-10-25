// Keep: trust boundary. Exact reviewed SDK sentinel acceptance must never widen
// to a real model; actual AgentSession refusal is separate from N7 input handling.
// Expected platform gap: this subprocess/custody fixture requires Linux + the
// explicit approved SDK/Node pins and an externally owned launcherTestTemp root.
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { createNativeActorOwner } from "./helpers/native-actor-owner.js";
import { NATIVE_SDK_RELEASE } from "./helpers/native-sdk-no-model.js";

describe.skipIf(process.platform !== "linux")("N7 approved SDK UNCONFIGURED contract", () => {
  it("accepts the actual sentinel only, rejects every-field mutations/catalog models, and refuses an actual would-be turn with zero provider calls", async () => {
    const sdkDirectory = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST, nodeBinary = process.env.FABRIC_NATIVE_TEST_NODE;
    if (!sdkDirectory || !nodeBinary) throw new Error("N7 BLOCKED: explicit FABRIC_NATIVE_TEST_PI_SDK_DIST + FABRIC_NATIVE_TEST_NODE required");
    const owner = await createNativeActorOwner({ candidateRoot: fileURLToPath(new URL("../", import.meta.url)),
      sdkDirectory, nodeBinary, actorId: "6027cfd59b1247b6bb4666970c481cc4" });
    try {
      const result = await owner.probeModelPolicy();
      expect(result.sdkRelease).toBe(NATIVE_SDK_RELEASE);
      expect(result.sentinelAccepted).toBe(true);
      expect(result.observerBound).toBe(true); expect(result.sessionStarts).toBe(1);
      expect(result.instrumentedDispatchMethods).toBeGreaterThan(0);
      expect(result.catalogModelRejected).toMatchObject({ provider: expect.any(String), id: expect.any(String) });
      expect(result.catalogModelRejected).not.toMatchObject({ provider: "unknown", id: "unknown" });
      const mutations = result.mutations as string[];
      for (const key of ["id", "name", "api", "provider", "baseUrl", "reasoning", "input", "cost", "contextWindow", "maxTokens",
        "cost.input", "cost.output", "cost.cacheRead", "cost.cacheWrite", "input.length", "input.0"]) {
        expect(mutations).toContain(`field:${key}`);
      }
      for (const location of ["root", "cost", "input"]) {
        for (const kind of ["extra", "hidden", "symbol", "prototype"]) expect(mutations).toContain(`${kind}:${location}`);
      }
      expect(result.turnRefusal).toMatch(/^No API key found for the selected model\./);
      expect(result.counts).toEqual({ beforeAgentStart: 0, agentStart: 0, providerCalls: 0,
        beforeProviderRequest: 0, providerStream: 0, toolCalls: 0, toolExecution: 0 });
    } finally { await owner.close(); }
  }, 60_000);
});
