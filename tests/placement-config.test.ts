import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createServer } from "node:http";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { loadFabricConfigForScope, normalizeFabricConfig } from "../src/config.js";
import { PlacementClient, type PlacementIntent } from "../src/placement/client.js";
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
    "http://127.0.0.1:1024",
    "http://127.0.0.1:18080",
    "http://127.0.0.1:65535",
  ])("accepts the approved endpoint %s", url => {
    expect(normalizePlacementApiConfig(validConfig({ url }))?.url).toBe(url);
  });

  it("rejects a leading-zero loopback port through Fabric configuration normalization", () => {
    expect(() => normalizeFabricConfig({ agents: { placementApi: validConfig({ url: "http://127.0.0.1:018080" }) } }))
      .toThrow("Invalid agents.placementApi configuration");
  });

  it("sends a configured loopback URL through the real PlacementClient", async () => {
    let receivedPath: string | undefined;
    let receivedIntent: PlacementIntent | undefined;
    const server: Server = createServer((request: IncomingMessage, response: ServerResponse) => {
      void (async () => {
        receivedPath = request.url;
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(Buffer.from(chunk));
        const submitted = JSON.parse(Buffer.concat(chunks).toString("utf8")) as PlacementIntent;
        receivedIntent = submitted;
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({
          runId: submitted.runId, status: "admitted", code: "ADMITTED", detail: "Admitted",
          retryAfterSeconds: null, host: "placement-node", claimRef: null, claimUID: null, podUID: null,
        }));
      })().catch(() => { if (!response.headersSent) response.writeHead(500).end(); });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    try {
      const address = server.address();
      if (!address || typeof address === "string" || address.port < 1024) throw new Error("Loopback HTTP stub did not bind an allowed port");
      const config = normalizeFabricConfig({ agents: { placementApi: {
        enabled: true, url: `http://127.0.0.1:${address.port}`, requestTimeoutMs: 1_000, watchTimeoutMs: 1_000,
      } } });
      const api = config.agents.placementApi;
      if (!api) throw new Error("Placement API config unexpectedly disabled");
      const client = new PlacementClient(api);
      const intent: PlacementIntent = {
        v: 1, runId: "loopback-config-check", kind: "process", org: "smarty-pants", parentSession: "parent-session",
        role: "task-agent", complexity: "normal", model: "openai/gpt-6.1-sol", thinking: "high", needs: [],
        repository: "https://github.com/Smarty-Pants-Inc/pi-fabric", cwd: ".", expectedSeconds: 60,
        timeoutSeconds: 60, deadline: null,
      };

      expect(await client.requestSpawn(intent)).toEqual({
        status: "admitted", host: "placement-node", claimRef: null, claimUID: null, podUID: null,
      });
      expect(receivedPath).toBe("/v1/spawns");
      expect(receivedIntent).toEqual(intent);
    } finally {
      await new Promise<void>(resolve => {
        server.close(() => resolve());
        server.closeAllConnections();
      });
    }
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
    "http://localhost:18080",
    "http://127.0.0.2:18080",
    "http://127.1:18080",
    "http://2130706433:18080",
    "http://0x7f000001:18080",
    "http://0.0.0.0:18080",
    "http://[::1]:18080",
    "http://127.0.0.1",
    "http://127.0.0.1:80",
    "http://127.0.0.1:1023",
    "http://127.0.0.1:018080",
    "http://127.0.0.1:65536",
    "https://127.0.0.1:18080",
    "http://u:p@127.0.0.1:18080",
    "http://127.0.0.1:18080/x",
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
