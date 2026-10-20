import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { loadFabricConfigForScope } from "../src/config.js";
import { normalizePlacementApiConfig } from "../src/placement/config.js";

const temporaryRoots: string[] = [];
afterEach(() => {
  for (const root of temporaryRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const validConfig = (overrides: Record<string, unknown> = {}) => ({
  enabled: true,
  url: "http://smarty-placement.smarty-island.svc:8080",
  ...overrides,
});

describe("Placement API configuration URL policy", () => {
  it.each([
    "http://smarty-placement.smarty-island.svc:8080",
    "http://100.64.0.1:30880",
    "http://100.127.255.255:30880",
  ])("accepts the approved endpoint %s", url => {
    expect(normalizePlacementApiConfig(validConfig({ url }))?.url).toBe(url);
  });

  it.each([
    "https://example.com",
    "https://smarty-placement.smarty-island.svc:8443",
    "http://smarty-placement.smarty-island.svc:8081",
    "http://100.63.255.255:30880",
    "http://100.128.0.1:30880",
    "http://100.64.0.1:80",
    "http://user@smarty-placement.smarty-island.svc:8080",
    "http://smarty-placement.smarty-island.svc:8080/path",
    "http://smarty-placement.smarty-island.svc:8080/?query=1",
  ])("rejects an endpoint outside the allowlist: %s", url => {
    expect(() => normalizePlacementApiConfig(validConfig({ url }))).toThrow("Invalid agents.placementApi configuration");
  });
});

describe("Placement API timeout validation", () => {
  it.each(["requestTimeoutMs", "watchTimeoutMs"] as const)("rejects invalid positive-integer values for %s", key => {
    for (const value of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "5000", null]) {
      expect(() => normalizePlacementApiConfig(validConfig({ [key]: value })))
        .toThrow(`Invalid agents.placementApi.${key}`);
    }
  });

  it("accepts positive safe-integer request and watch timeouts", () => {
    expect(normalizePlacementApiConfig(validConfig({ requestTimeoutMs: 1, watchTimeoutMs: 2_147_483_647 })))
      .toMatchObject({ requestTimeoutMs: 1, watchTimeoutMs: 2_147_483_647 });
  });
});

describe("Placement API host-only configuration", () => {
  it("ignores project attempts to enable or replace the host setting", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "placement-config-test-"));
    temporaryRoots.push(root);
    const agentDir = path.join(root, "agent");
    const projectDir = path.join(root, "project");
    fs.mkdirSync(agentDir, { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.join(projectDir, ".pi"), { recursive: true, mode: 0o700 });
    const hostConfig = validConfig({ requestTimeoutMs: 1_000 });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { placementApi: hostConfig } }));
    fs.writeFileSync(path.join(projectDir, ".pi", "fabric.json"), JSON.stringify({ agents: {
      placementApi: { ...validConfig(), url: "http://100.64.0.1:30880", enabled: false },
    } }));

    const loaded = loadFabricConfigForScope({ cwd: projectDir, agentDir, projectTrusted: true }, "project");
    expect(loaded.agents.placementApi).toMatchObject({ url: "http://smarty-placement.smarty-island.svc:8080", requestTimeoutMs: 1_000 });

    fs.rmSync(path.join(agentDir, "fabric.json"));
    const projectOnly = loadFabricConfigForScope({ cwd: projectDir, agentDir, projectTrusted: true }, "project");
    expect(projectOnly.agents.placementApi).toBeUndefined();
  });

  it("rejects removed identity and credential fields", () => {
    expect(() => normalizePlacementApiConfig(validConfig({ tokenFile: "/tmp/token" })))
      .toThrow("Invalid agents.placementApi configuration");
  });
});
