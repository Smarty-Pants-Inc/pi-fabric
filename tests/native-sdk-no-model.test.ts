// Keep: trust boundary. Exact reviewed SDK sentinel acceptance must never widen
// to a real model; actual AgentSession refusal is separate from N7 input handling.
// Expected platform gap: this subprocess/custody fixture requires Linux + the
// explicit approved SDK/Node pins and an externally owned launcherTestTemp root.
import { fileURLToPath } from "node:url";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import { createNativeActorOwner } from "./helpers/native-actor-owner.js";
import { NATIVE_SDK_RELEASE, NATIVE_SDK_DIRECTORY, NATIVE_SDK_SOURCE_PINS, nativeSdkTestPrerequisite } from "./helpers/native-sdk-no-model.js";

// Expected skip: missing Linux/approved SDK files/executable Node/env in ordinary CI only.
const nativeSkipReason = nativeSdkTestPrerequisite();

describe("native prerequisite gate (no sessions)", () => {
  it("keeps the approved release and every source pin exact; unapproved/tampered SDKs fail even without Node", () => {
    expect(NATIVE_SDK_RELEASE).toBe("031c749962972b0cb966e443e921d64654627b20");
    expect(NATIVE_SDK_SOURCE_PINS).toEqual({
      "../package.json": "a87892a2985221d6c80a921a5016341117ebf8223f5f49eac12c79b3ac43719c",
      "../../pi-agent-core/package.json": "541f131cd43832f75a9c64ee886c367c219c0772da7047c66053d7cc80db98b9",
      "../../pi-agent-core/dist/agent.js": "0e73cc09ae5563b42e34272d5f3257766923b4086461944785c24701f7d0d04d",
      "index.js": "b2285dc6a9c15d8b577a6dbc287ec6babec2703cf8637afb733ad824156a8d6a",
      "core/sdk.js": "3f4859912cb8465239924468690fb8bf1f3d3a843e79273aa234c2923b6ecede",
      "core/model-runtime.js": "58a8afd627708c0e43fde668234c70be883134b4c8428846691e503928cb067f",
      "core/model-policy.js": "fdb5dc9c090e4b77e084e2bb3a40022eedd537d356767357b15301d9907de88f",
      "core/agent-session.js": "8dbef0a2e0f00b99ba176808442840201f29e1b4e80ffa1fd2bf7cc332a65f28",
      "core/settings-manager.js": "3ce7458e9c41ccb2bb83f11249148e8d5a97e9bbb6fd68f51dda4665827ffcbe",
      "core/session-manager.js": "ca138e988f07b62d403218007a95cf9e9a70d0f194c1eaeb7a4b796f5311d5a9",
    });
    vi.stubEnv("FABRIC_NATIVE_TEST_REQUIRED", "0");
    vi.stubEnv("FABRIC_NATIVE_TEST_NODE", undefined);
    vi.stubEnv("FABRIC_NATIVE_TEST_PI_SDK_DIST", "/unapproved-sdk");
    try {
      expect(() => nativeSdkTestPrerequisite()).toThrow(/unapproved SDK/);
      vi.stubEnv("FABRIC_NATIVE_TEST_PI_SDK_DIST", NATIVE_SDK_DIRECTORY);
      vi.spyOn(fs, "realpathSync").mockReturnValue(NATIVE_SDK_DIRECTORY);
      vi.spyOn(fs, "readFileSync").mockReturnValue(Buffer.from("tampered approved SDK bytes"));
      expect(() => nativeSdkTestPrerequisite()).toThrow(/approved SDK source changed/);
      vi.mocked(fs.readFileSync).mockImplementation(() => { throw Object.assign(new Error("absent"), { code: "ENOENT" }); });
      expect(nativeSdkTestPrerequisite()).toContain("approved SDK files");
      vi.stubEnv("FABRIC_NATIVE_TEST_REQUIRED", "1");
      expect(() => nativeSdkTestPrerequisite()).toThrow(/NATIVE_TEST_REQUIRED/);
    } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); }
  });

  it.each(["native-sdk-no-model", "native-owner-entry"])("%s declares an ordinary absent-env skip and REQUIRED makes it fail in actual subprocess collection", file => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-prerequisite-probe-"));
    const env: NodeJS.ProcessEnv = { ...process.env, CI: "false" };
    delete env.FABRIC_NATIVE_TEST_PI_SDK_DIST; delete env.FABRIC_NATIVE_TEST_NODE;
    try {
      for (const required of ["0", "1"]) {
        const report = path.join(root, `${required}.json`);
        const args = [fileURLToPath(new URL("../node_modules/vitest/vitest.mjs", import.meta.url)), "run", `tests/${file}.test.ts`,
          "--reporter=json", `--outputFile=${report}`, "-t", file === "native-sdk-no-model" ? "N7 approved SDK UNCONFIGURED contract" : "F1\\(b\\) actual candidate-installed native owner entry"];
        const result = spawnSync(process.execPath, args, { cwd: fileURLToPath(new URL("../", import.meta.url)),
          env: { ...env, FABRIC_NATIVE_TEST_REQUIRED: required }, encoding: "utf8", timeout: 45_000 });
        console.info(JSON.stringify({ nativePrerequisiteProbe: file, required, command: [process.execPath, ...args],
          status: result.status, signal: result.signal, stdout: result.stdout, stderr: result.stderr }));
        expect(result.error).toBeUndefined(); expect(result.signal).toBeNull();
        const collected = JSON.parse(fs.readFileSync(report, "utf8"));
        if (required === "0") {
          expect(result.status).toBe(0); expect(collected.numPassedTests).toBe(0);
          expect(collected.numPendingTests).toBeGreaterThan(0);
          expect(JSON.stringify(collected)).toContain("SKIP: native prerequisites absent:");
        } else {
          expect(result.status).not.toBe(0);
          expect(JSON.stringify(collected) + result.stderr).toContain("NATIVE_TEST_REQUIRED");
        }
      }
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  }, 120_000);
});

describe.skipIf(!!nativeSkipReason)(`N7 approved SDK UNCONFIGURED contract${nativeSkipReason ? ` [SKIP: ${nativeSkipReason}]` : ""}`, () => {
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
