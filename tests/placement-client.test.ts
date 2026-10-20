import { createServer } from "node:http";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { describe, expect, it } from "vitest";
import { PlacementClient, type PlacementBinding, type PlacementIntent, type PlacementOutcome } from "../src/placement/client.js";
import { placementBindingMatches } from "../src/placement/spawn.js";

const intent: PlacementIntent = {
  v: 1,
  runId: "run-123",
  kind: "process",
  org: "smarty-pants",
  parentSession: "parent-session",
  role: "task-agent",
  complexity: "normal",
  model: "openai/gpt-6.1-sol",
  thinking: "high",
  needs: [],
  repository: "https://github.com/Smarty-Pants-Inc/pi-fabric",
  cwd: ".",
  expectedSeconds: 60,
  timeoutSeconds: 60,
  deadline: null,
};
const admitted = (runId: string) => ({
  runId, status: "admitted", code: "ADMITTED", detail: "Admitted", retryAfterSeconds: null,
  host: "placement-node", claimRef: null, claimUID: null, podUID: null,
});
const timestamp = (ms: number) => new Date(ms).toISOString().replace(/\.\d{3}Z$/u, "Z");
const makeBinding = (submitted: PlacementIntent): Extract<PlacementBinding, { status: "admitted" }> => {
  const admittedAt = Math.floor(Date.now() / 1_000) * 1_000;
  return {
    runId: submitted.runId,
    status: "admitted",
    claimRef: null,
    claimUID: null,
    podUID: null,
    podName: null,
    host: "placement-node",
    org: submitted.org,
    role: submitted.role,
    model: submitted.model,
    provider: "openai",
    thinking: submitted.thinking,
    expectedSeconds: submitted.expectedSeconds,
    deadline: submitted.deadline,
    executionDeadline: timestamp(admittedAt + submitted.expectedSeconds * 1_000),
    admittedAt: timestamp(admittedAt),
  };
};
const bodyText = async (request: IncomingMessage): Promise<string> => {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
};
const json = (response: ServerResponse, value: unknown, status = 200) => {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(value));
};
const close = async (server: Server) => {
  if (!server.listening) return;
  await new Promise<void>(resolve => {
    server.close(() => resolve());
    server.closeAllConnections();
  });
};

describe("PlacementClient plain HTTP contract", () => {
  it("submits, reads, binds, and watches one run without authentication", async () => {
    const requests: { method: string | undefined; path: string | undefined; authorization: string | undefined }[] = [];
    let submitted = intent;
    const server: Server = createServer((request, response) => {
      void (async () => {
        requests.push({ method: request.method, path: request.url, authorization: request.headers.authorization });
        if (request.method === "POST" && request.url === "/v1/spawns") {
          submitted = JSON.parse(await bodyText(request)) as PlacementIntent;
          json(response, admitted(submitted.runId));
        } else if (request.method === "GET" && request.url === `/v1/spawns/${intent.runId}`) {
          json(response, admitted(intent.runId));
        } else if (request.method === "GET" && request.url === `/v1/spawns/${intent.runId}/binding`) {
          json(response, makeBinding(submitted));
        } else if (request.method === "GET" && request.url?.startsWith(`/v1/spawns/${intent.runId}/watch?`)) {
          response.writeHead(200, { "Content-Type": "application/x-ndjson; charset=utf-8" });
          response.end(`${JSON.stringify(admitted(intent.runId))}\n`);
        } else json(response, { code: "NOT_FOUND", detail: "Missing" }, 404);
      })().catch(() => { if (!response.headersSent) json(response, { code: "PLACEMENT_UNAVAILABLE", detail: "Fixture failed" }, 503); });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("HTTP fixture did not bind");
    const client = new PlacementClient({
      url: `http://127.0.0.1:${address.port}`,
      requestTimeoutMs: 1_000,
      watchTimeoutMs: 1_000,
    });

    try {
      expect(await client.requestSpawn(intent)).toMatchObject({ status: "admitted", host: "placement-node" });
      expect(await client.getSpawn(intent.runId)).toMatchObject({ status: "admitted" });
      expect(await client.getBinding(intent.runId)).toMatchObject({ status: "admitted", model: intent.model });
      const events: unknown[] = [];
      for await (const event of client.watchSpawn(intent.runId)) events.push(event);
      expect(events).toEqual([expect.objectContaining({ status: "admitted", host: "placement-node" })]);
      expect(requests).toEqual([
        { method: "POST", path: "/v1/spawns", authorization: undefined },
        { method: "GET", path: `/v1/spawns/${intent.runId}`, authorization: undefined },
        { method: "GET", path: `/v1/spawns/${intent.runId}/binding`, authorization: undefined },
        { method: "GET", path: expect.stringMatching(new RegExp(`/v1/spawns/${intent.runId}/watch\\?`)), authorization: undefined },
      ]);
    } finally {
      await close(server);
    }
  });

  it("allows an ordinary pod restart with a new pod UID for the same run", () => {
    const claimRef = { namespace: "smarty-island" as const, name: `spawn-${intent.runId}`, uid: "claim-uid" };
    const result: PlacementOutcome = { status: "admitted", host: "placement-node", claimRef, claimUID: "claim-uid", podUID: "old-pod-uid" };
    const binding: PlacementBinding = {
      ...makeBinding(intent), claimRef, claimUID: "claim-uid", podUID: "new-pod-uid", podName: "spawn-run-123",
    };
    expect(placementBindingMatches(result, binding)).toBe(true);
  });

  it("preserves typed refusal without exposing server detail", async () => {
    const server: Server = createServer((_request, response) => {
      json(response, { code: "POLICY_DENIED", detail: "private server detail" }, 403);
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("HTTP fixture did not bind");
    const client = new PlacementClient({ url: `http://127.0.0.1:${address.port}`, requestTimeoutMs: 1_000, watchTimeoutMs: 1_000 });

    try {
      expect(await client.requestSpawn(intent)).toEqual({ status: "refused", code: "POLICY_DENIED" });
    } finally {
      await close(server);
    }
  });
});
