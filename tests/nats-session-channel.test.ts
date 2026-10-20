import { randomUUID } from "node:crypto";
import { headers, type Msg, type NatsConnection } from "@nats-io/transport-node";
import {
  NatsSessionChannelManager,
  SESSION_CHANNEL_REPLY_LIMIT,
  SESSION_CHANNEL_REQUEST_LIMIT,
  sessionChannelEventsSubject,
  sessionChannelRequestSubject,
  type SessionChannel,
} from "../src/transport/nats/session-channel.js";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import { createPathBHarness, teardownWithin, type PathBHarness } from "./helpers/nats-cluster.js";

// The requested Bun command and the repository's Vitest runner exercise the same integration checks.
const testModule = process.versions.bun ? "bun:test" : "vitest";
const { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } =
  await import(/* @vite-ignore */ testModule) as typeof import("vitest");

// Keep reason: the real broker is the trust boundary for scoped request/reply and event permissions.
// Literal subject vector: SHA-256 UTF-8 host/session IDs, truncated to the first 32 hex characters.
const identity = {
  org: "smarty-pants-inc",
  hostId: "session-channel-host",
  sessionId: "11111111-2222-4333-8444-555555555555",
  sessionGeneration: "generation-1",
};
const service = "smarty-code-ordinary";
const subjectPrefix = "fabric.svc.smarty-pants-inc.e8f6226427beb35d340fd1f431ce5441.cf4c4732fd3b8f8a55b60871950a2f22.smarty-code-ordinary";
const requestSubject = `${subjectPrefix}.snapshot`;
const eventsSubject = `${subjectPrefix}.events`;
const caller = { id: "session-channel-test" };
const timeout = 2_500;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

let fixture: PathBHarness | undefined;
let agent: NatsConnection;
let requester: NatsConnection;
let manager: NatsSessionChannelManager | undefined;
let channel: SessionChannel;
let connectionErrors: string[];

function observeErrors(connection: NatsConnection, role: string): void {
  void (async () => {
    for await (const status of connection.status()) {
      // Only fixed SDK error names: never log credential bytes, reply inbox identities or raw errors.
      if (status.type === "error") connectionErrors.push(`${role}:${status.error.name}`);
    }
  })().catch(() => undefined);
}

async function request(
  body: unknown,
  requestId: string = randomUUID(),
  options: { caller?: unknown; data?: Uint8Array; operation?: string } = {},
): Promise<Msg> {
  const requestHeaders = headers();
  requestHeaders.set("Fabric-Request-Id", requestId);
  requestHeaders.set("Fabric-Caller", JSON.stringify(options.caller ?? caller));
  try {
    const subject = sessionChannelRequestSubject({ ...identity, service }, options.operation ?? "snapshot");
    return await requester.request(subject, options.data ?? new TextEncoder().encode(JSON.stringify(body)), {
      headers: requestHeaders,
      timeout,
    });
  } catch (cause) {
    if (connectionErrors.length) throw new Error(`session request failed (${connectionErrors.join(", ")})`, { cause });
    throw cause;
  }
}

beforeAll(async () => {
  // No silent skip: the requested integration check must exercise the pinned real server.
  fixture = await createPathBHarness({ org: identity.org, tls: {} });
  expect(fixture.serverVersion).toBe("2.14.7");
}, 30_000);

beforeEach(async () => {
  if (!fixture) throw new Error("org-scoped NATS fixture was not initialized");
  agent = await fixture.connect(fixture.users.machineA);
  // Alpha deliberately uses the same org-scoped runtime credential for agents and callers.
  requester = await fixture.connect(fixture.users.machineA);
  connectionErrors = [];
  observeErrors(agent, "agent");
  observeErrors(requester, "requester");
  manager = new NatsSessionChannelManager(agent);
  channel = await manager.open(identity, { service, version: 1 }); // Resolves only after the broker has installed SUB.
});

afterEach(async () => {
  manager?.close();
  manager = undefined;
  await fixture?.closeConnections();
});

afterAll(async () => { await fixture?.stop(); });

describe("session channels over real org-scoped NATS", () => {
  it("verifies the shared org role's required grants through the real broker USER.INFO", async () => {
    const verified = await openNatsConnection({ ...fixture!.runtimeOptions(fixture!.users.machineA), org: identity.org });
    try {
      expect(verified.userInfo).toMatchObject({
        data: { permissions: {
          publish: { allow: expect.arrayContaining([`fabric.svc.${identity.org}.>`]) },
          subscribe: { allow: expect.arrayContaining([`fabric.svc.${identity.org}.>`]) },
          responses: { max: 16, ttl: 120_000_000_000 },
        } },
      });
    } finally { await verified.close(); }
  });

  it("validates one-token services, operation names and the session-generation fence", async () => {
    for (const invalidService of ["", "two.parts", "UPPER", "a".repeat(49)]) {
      await expect(manager!.open(identity, { service: invalidService, version: 1 })).rejects.toThrow("session channel service");
    }
    expect(() => channel.handle("events", async () => ({ status: 200, body: null }))).toThrow("unsupported session channel operation");
    expect(() => channel.handle("other", async () => ({ status: 200, body: null }))).toThrow("unsupported session channel operation");
    await expect(manager!.open({ ...identity, sessionGeneration: "generation-2" }, { service: "other", version: 1 }))
      .rejects.toThrow("session channel identity changed");
    expect(manager!.services()).toEqual([{ name: service, version: 1 }]);
  });

  it("matches the smarty-code-ordinary / smarty-pants-inc subject vector and completes request/reply", async () => {
    expect(sessionChannelRequestSubject({ ...identity, service }, "snapshot")).toBe(requestSubject);
    expect(sessionChannelEventsSubject({ ...identity, service })).toBe(eventsSubject);
    const received: unknown[] = [];
    const requestId = randomUUID();
    channel.handle("snapshot", async (req) => {
      received.push(req);
      return { status: 200, body: { sessionId: identity.sessionId, echo: req.body } };
    });
    const body = { prompt: "real broker round trip", cursor: 7 };
    const response = await request(body, requestId);
    expect(response.json()).toEqual({ status: 200, body: { sessionId: identity.sessionId, echo: body } });
    expect(received).toEqual([{ requestId, body, caller }]);
  });

  it("replays the completed response for a duplicate request ID without invoking the handler again", async () => {
    let calls = 0;
    channel.handle("snapshot", async (req) => ({ status: 200, body: { invocation: ++calls, input: req.body } }));
    const requestId = randomUUID();
    const first = await request({ revision: 1 }, requestId);
    const duplicate = await request({ revision: 2 }, requestId);
    expect(first.json()).toEqual({ status: 200, body: { invocation: 1, input: { revision: 1 } } });
    expect(duplicate.string()).toBe(first.string());
    expect(calls).toBe(1);
  });

  it("scopes replay keys by caller and operation and validates the caller before caching", async () => {
    let calls = 0;
    channel.handle("snapshot", async (req) => ({ status: 200, body: { operation: "snapshot", caller: req.caller.id, invocation: ++calls } }));
    channel.handle("history", async (req) => ({ status: 200, body: { operation: "history", caller: req.caller.id, invocation: ++calls } }));
    const requestId = randomUUID();
    const invalid = await request({}, requestId, { caller: { id: "caller-a", scopes: ["admin"] } });
    expect(invalid.json()).toEqual({ status: 400, body: { error: "invalid request" } });
    const first = await request({}, requestId, { caller: { id: "caller-a" } });
    const otherCaller = await request({}, requestId, { caller: { id: "caller-b" } });
    const otherOperation = await request({}, requestId, { caller: { id: "caller-a" }, operation: "history" });
    const replay = await request({ changed: true }, requestId, { caller: { id: "caller-a" } });
    expect(first.json()).toEqual({ status: 200, body: { operation: "snapshot", caller: "caller-a", invocation: 1 } });
    expect(otherCaller.json()).toEqual({ status: 200, body: { operation: "snapshot", caller: "caller-b", invocation: 2 } });
    expect(otherOperation.json()).toEqual({ status: 200, body: { operation: "history", caller: "caller-a", invocation: 3 } });
    expect(replay.string()).toBe(first.string());
    expect(calls).toBe(3);
  });

  it("coalesces concurrent duplicate requests while the single handler is still pending", async () => {
    const release = deferred<void>();
    const delivered = deferred<void>();
    const duplicates = 8;
    let calls = 0;
    let observed = 0;
    channel.handle("snapshot", async () => {
      calls += 1;
      await release.promise;
      return { status: 202, body: { accepted: true, invocation: calls } };
    });
    // Same scoped agent observes delivery, not a timer: hold the handler until every duplicate arrived.
    const observer = agent.subscribe(requestSubject, { callback: (error) => {
      if (!error && ++observed === duplicates) delivered.resolve();
    } });
    await agent.flush();
    const requestId = randomUUID();
    const responses = Promise.all(Array.from({ length: duplicates }, () => request({ input: "same" }, requestId)));
    void responses.catch(() => undefined); // Observe failures even if the delivery deadline fails first.
    try {
      await teardownWithin("concurrent request delivery", () => delivered.promise, timeout);
      expect(observed).toBe(duplicates);
      expect(calls).toBe(1);
      release.resolve();
      const replies = await responses;
      expect(replies.map((reply) => reply.json())).toEqual(Array.from({ length: duplicates }, () => ({
        status: 202, body: { accepted: true, invocation: 1 },
      })));
      expect(calls).toBe(1);
    } finally {
      release.resolve();
      observer.unsubscribe();
      await responses.catch(() => undefined);
    }
  });

  it("returns 413 for a request one byte over the channel limit without invoking the handler", async () => {
    let calls = 0;
    channel.handle("snapshot", async () => { calls += 1; return { status: 200, body: null }; });
    // Valid JSON below the broker's max_payload: rejection must come from the channel, not NATS.
    const data = new TextEncoder().encode(JSON.stringify("x".repeat(SESSION_CHANNEL_REQUEST_LIMIT - 1)));
    expect(data.byteLength).toBe(SESSION_CHANNEL_REQUEST_LIMIT + 1);
    const response = await request(null, randomUUID(), { data });
    expect(response.json()).toEqual({ status: 413, body: { error: "request too large" } });
    expect(calls).toBe(0);
  });

  it("bounds the complete status/body reply at 1 MiB", async () => {
    channel.handle("snapshot", async () => ({ status: 200, body: "x".repeat(SESSION_CHANNEL_REPLY_LIMIT) }));
    const response = await request({});
    expect(response.json()).toEqual({ status: 500, body: { error: "handler failed" } });
  });

  it("publishes JSON events on the exact org, host, session and service events subject", async () => {
    const delivery = deferred<Msg>();
    const subscription = requester.subscribe(`fabric.svc.${identity.org}.*.*.*.events`, {
      callback: (error, message) => { if (!error) delivery.resolve(message); },
    });
    try {
      await requester.flush();
      const event = { type: "session-updated", revision: 9, sessionId: identity.sessionId };
      channel.publish(event);
      await agent.flush();
      const message = await teardownWithin("session event delivery", () => delivery.promise, timeout);
      expect(message.subject).toBe(eventsSubject);
      expect(message.json()).toEqual(event);
    } finally { subscription.unsubscribe(); }
  });

  for (const close of ["channel", "manager"] as const) {
    it(`${close}.close stops requests and event publishing`, async () => {
      let calls = 0;
      channel.handle("snapshot", async () => ({ status: 200, body: { invocation: ++calls } }));
      expect((await request({ beforeClose: true })).json()).toEqual({ status: 200, body: { invocation: 1 } });
      const events: unknown[] = [];
      const firstEvent = deferred<void>();
      const subscription = requester.subscribe(eventsSubject, { callback: (error, message) => {
        if (!error) { events.push(message.json()); firstEvent.resolve(); }
      } });
      try {
        await requester.flush();
        channel.publish({ beforeClose: true });
        await teardownWithin("pre-close event delivery", () => firstEvent.promise, timeout);
        if (close === "channel") channel.close();
        else manager!.close();
        await agent.flush(); // UNSUB is installed before the requester probes the closed service.
        await expect(request({ afterClose: true })).rejects.toMatchObject({ cause: { name: "NoResponders" } });
        expect(() => channel.publish({ afterClose: true })).toThrow("session channel is closed");
        expect(() => channel.handle("snapshot", async () => ({ status: 200, body: null }))).toThrow("session channel is closed");
        await agent.flush();
        await requester.flush();
        expect(calls).toBe(1);
        expect(events).toEqual([{ beforeClose: true }]);
        expect(manager!.services()).toEqual([]);
      } finally { subscription.unsubscribe(); }
    });
  }
});
