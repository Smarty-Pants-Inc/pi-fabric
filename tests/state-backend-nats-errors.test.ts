// Keep: the NATS error trust boundary and the real getter-only SDK regression (smarty-dev#12633).
// Pure installed-client classes/parser only: no server, filesystem operations, timers or deletion.
import { describe, expect, it } from "vitest";
import { JetStreamApiError } from "@nats-io/jetstream";
import { BaseApiClientImpl, JetStreamStatus, jserrors } from "@nats-io/jetstream/internal";
import {
  AuthorizationError, ClosedConnectionError, ConnectionError, NoRespondersError, PermissionViolationError,
  ProtocolError, RequestError, TimeoutError, UserAuthenticationExpiredError, headers, type Msg,
} from "@nats-io/nats-core";
import { guardNatsPort, redactNatsError, type JetStreamPort } from "../src/mesh/state-backend-nats.js";

type ApiError = ConstructorParameters<typeof JetStreamApiError>[0];
type SafeError = Error & Record<string, unknown>;

// The actual SDK response parser produces the specialized stream/account errors, not stand-ins for them.
const apiFailure = (api: ApiError): Error => {
  try {
    BaseApiClientImpl.prototype.parseJsResponse({ data: new TextEncoder().encode(JSON.stringify({ error: api })) } as Msg);
  } catch (error) {
    expect(error).toBeInstanceOf(Error);
    return error as Error;
  }
  throw new Error("SDK response parser did not reject the API error");
};

const rejectingPort = (error: unknown): JetStreamPort => {
  const fail = async (): Promise<never> => { throw error; };
  return { server: fail, ensureStream: fail, streamInfo: fail, createStream: fail, deleteStream: fail,
    lastSequence: fail, firstSequence: fail, streamExists: fail,
    getMessage: fail, lastBySubject: fail, purge: fail, publish: fail, consume: fail, close: fail };
};
const guardedFailure = (error: unknown): Promise<Error> => guardNatsPort(rejectingPort(error)).lastSequence("STREAM").then(
  () => { throw new Error("port did not reject"); },
  (caught: unknown) => { expect(caught).toBeInstanceOf(Error); return caught as Error; },
);

const url = "nats://urlUser731:urlPass732@localhost:4222?token=querySecret733#fragmentSecret734";
const jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5In0.eyJzdWIiOiJVQUJDIn0.c2lnbmF0dXJl";
const seed = `SU${"A".repeat(54)}`;
const detail = `${url} token=bareToken735 password=barePass736 Authorization: Bearer bearer737 Basic YmFzaWM3Mzg= ${jwt} ${seed}\n` +
  "-----BEGIN NATS USER JWT-----\ncredsBlock739\n-----END NATS USER JWT-----";
const secrets = ["urlUser731", "urlPass732", "querySecret733", "fragmentSecret734", "bareToken735", "barePass736",
  "bearer737", "YmFzaWM3Mzg=", jwt, seed, "credsBlock739", "opaqueSensitive740"];

// Inspect every outward surface, including recursively sanitized causes/members and the public SDK method.
const views = (error: Error, depth = 0): string[] => {
  const safe = error as SafeError;
  const result = [String(error), error.name, error.message, error.stack ?? "", JSON.stringify(error),
    JSON.stringify(Object.entries(error)), JSON.stringify(Object.getOwnPropertyDescriptors(error))];
  if (typeof safe.apiError === "function") result.push(JSON.stringify(safe.apiError.call(error)));
  if (depth < 5 && error.cause instanceof Error) result.push(...views(error.cause, depth + 1));
  if (depth < 5 && Array.isArray(safe.errors)) {
    for (const inner of safe.errors) if (inner instanceof Error) result.push(...views(inner, depth + 1));
  }
  return result;
};
const expectRedacted = (error: Error): void => {
  for (const view of views(error)) for (const secret of secrets) expect(view).not.toContain(secret);
};

const apiShapes = [
  { label: "stream-info missing", name: "StreamNotFoundError", code: 404, err_code: 10059, description: "stream not found" },
  { label: "concurrent-create conflict", name: "JetStreamApiError", code: 400, err_code: 10058,
    description: "stream name already in use with a different configuration" },
  { label: "placement peer offline", name: "JetStreamApiError", code: 400, err_code: 10005,
    description: "no suitable peers for placement, peer offline" },
  { label: "leader unavailable", name: "JetStreamApiError", code: 503, err_code: 10008,
    description: "JetStream system temporarily unavailable" },
  { label: "wrong last sequence", name: "JetStreamApiError", code: 400, err_code: 10071,
    description: "wrong last sequence: 7" },
];

describe("real NATS SDK errors survive the sanitizer", () => {
  it.each(apiShapes)("keeps readable code/status/apiError for $label, including a second redaction", async (shape) => {
    const api = { code: shape.code, err_code: shape.err_code, description: `${shape.description}: ${detail}` };
    const original = apiFailure(api);
    expect(original).toBeInstanceOf(JetStreamApiError);
    expect(original.name).toBe(shape.name);
    for (const key of ["code", "status"]) {
      const descriptor = Object.getOwnPropertyDescriptor(JetStreamApiError.prototype, key);
      expect(descriptor?.get).toBeTypeOf("function");
      expect(descriptor?.set).toBeUndefined();
    }
    Object.assign(original, { transient: shape.code === 503, expected: 7, afterLostAck: false });

    const direct = redactNatsError(original);
    const guarded = await guardedFailure(original);
    const twice = redactNatsError(direct);
    const guardedTwice = await guardedFailure(direct);
    for (const error of [direct, guarded, twice, guardedTwice]) {
      expect(error).toBeInstanceOf(JetStreamApiError);
      expect(Object.getPrototypeOf(error)).toBe(Object.getPrototypeOf(original));
      const safe = error as JetStreamApiError & { transient: boolean; expected: number; afterLostAck: boolean };
      expect(safe.name).toBe(shape.name);
      expect(safe.message).toContain(shape.description);
      expect(safe.stack).toContain(shape.description);
      expect(safe.code).toBe(shape.err_code);
      expect(safe.status).toBe(shape.code);
      expect(safe.apiError()).toEqual({ code: shape.code, err_code: shape.err_code, description: direct.message });
      expect([safe.transient, safe.expected, safe.afterLostAck]).toEqual([shape.code === 503, 7, false]);
      expect(Object.getOwnPropertyDescriptor(safe, "code")?.value).toBe(shape.err_code);
      expect(Object.getOwnPropertyDescriptor(safe, "status")?.value).toBe(shape.code);
      expectRedacted(error);
    }
    // The clone has no private payload/brand; only the shadowing safe method may be used.
    expect(() => JetStreamApiError.prototype.apiError.call(direct)).toThrow(TypeError);
    expect((original as JetStreamApiError).apiError()).toEqual(api);
  });

  it("defines an own enumerable numeric code without invoking the SDK prototype setter", () => {
    const original = new JetStreamApiError({ code: 503, err_code: 10008, description: "unavailable" });
    Object.defineProperty(original, "code", { value: 10008, enumerable: true });
    const safe = redactNatsError(original) as JetStreamApiError;
    expect(safe).toBeInstanceOf(JetStreamApiError);
    expect(safe.code).toBe(10008);
    expect(safe.status).toBe(503);
    expect(safe.apiError().err_code).toBe(10008);
  });

  it("keeps the SDK NoResponders -> RequestError -> JetStreamNotEnabled cause chain readable", async () => {
    const noResponders = new NoRespondersError(`$JS.API.INFO ${detail}`);
    const request = new RequestError(`request failed ${detail}`, { cause: noResponders });
    const original = new jserrors.JetStreamNotEnabled("jetstream is not enabled", { cause: request });
    for (const safe of [redactNatsError(original), await guardedFailure(original), redactNatsError(redactNatsError(original))]) {
      expect(safe).toBeInstanceOf(jserrors.JetStreamNotEnabled);
      expect(safe.message).toBe("jetstream is not enabled");
      expect(safe.cause).toBeInstanceOf(RequestError);
      expect((safe.cause as RequestError).isNoResponders()).toBe(true);
      const inner = (safe.cause as RequestError).cause as NoRespondersError;
      expect(inner).toBeInstanceOf(NoRespondersError);
      expect(inner.name).toBe("NoResponders");
      expect(inner.message).toContain("$JS.API.INFO");
      expect(Object.hasOwn(inner, "subject")).toBe(false);
      expectRedacted(safe);
    }
  });

  it("keeps the real account-disabled API cause, distinct from transport no-responders", async () => {
    const original = apiFailure({ code: 503, err_code: 10039, description: `JetStream not enabled for account ${detail}` });
    const safe = await guardedFailure(original);
    expect(safe).toBeInstanceOf(jserrors.JetStreamNotEnabled);
    expect(safe.cause).toBeInstanceOf(JetStreamApiError);
    const cause = safe.cause as JetStreamApiError;
    expect(cause.code).toBe(10039);
    expect(cause.status).toBe(503);
    expect(cause.apiError().err_code).toBe(10039);
    expectRedacted(safe);
  });

  it.each([
    new TimeoutError(), new ConnectionError(`connection failed ${detail}`), new ClosedConnectionError(),
    new AuthorizationError(`authorization violation ${detail}`), new UserAuthenticationExpiredError(`expired ${detail}`),
    new PermissionViolationError(`permissions violation ${detail}`, "publish", url, "secretQueue"),
    new ProtocolError(`protocol violation ${detail}`),
  ])("keeps transport/permanent class $name without leaking extra fields", async (original) => {
    Object.assign(original, { transient: original instanceof ConnectionError, payload: { whatever: "opaqueSensitive740" } });
    const safe = await guardedFailure(original);
    expect(Object.getPrototypeOf(safe)).toBe(Object.getPrototypeOf(original));
    expect(safe.name).toBe(original.name);
    expect(safe.message).not.toBe("[unprintable error]");
    expect((safe as SafeError).transient).toBe(original instanceof ConnectionError);
    expect(Object.hasOwn(safe, "payload")).toBe(false);
    expect(Object.hasOwn(safe, "subject")).toBe(false);
    expect(Object.hasOwn(safe, "operation")).toBe(false);
    expect(Object.hasOwn(safe, "queue")).toBe(false);
    expectRedacted(safe);
  });

  it("keeps the real 503 status error's own numeric code", async () => {
    const status = new JetStreamStatus({ headers: headers(503, `no responders token=bareToken735 ${url}`) } as Msg);
    const original = status.toError();
    const safe = await guardedFailure(original);
    expect(Object.getPrototypeOf(safe)).toBe(Object.getPrototypeOf(original));
    expect((safe as SafeError).code).toBe(503);
    expect(safe.message).toContain("no responders");
    expectRedacted(safe);
  });
});

describe("real SDK error redaction remains a closed trust boundary", () => {
  it("redacts name/message/stack/code/apiError/cause and drops original private, hidden and arbitrary payloads", async () => {
    const payload = { whatever: "opaqueSensitive740", nested: { token: "bareToken735" } };
    const api = { code: 503, err_code: 10008, description: `server failed ${detail}`, payload, url };
    const cause = Object.assign(new ConnectionError(`cause failed ${detail}`), { code: `connection ${detail}`, status: detail, payload });
    const original = new JetStreamApiError(api, { cause });
    original.name = `JetStreamApiError ${detail}`;
    original.stack = `JetStreamApiError: server failed ${detail}\n at client`;
    Object.assign(original, { payload, url });
    Object.defineProperty(original, "hiddenPayload", { value: payload });
    Object.defineProperty(original, Symbol("privatePayload"), { value: payload });
    for (const safe of [redactNatsError(original), await guardedFailure(original)]) {
      expect(safe).toBeInstanceOf(JetStreamApiError);
      expect(safe.name).toContain("JetStreamApiError nats://");
      expect(safe.message).toContain("server failed nats://");
      expect(safe.cause).toBeInstanceOf(ConnectionError);
      expect((safe.cause as SafeError).code).toContain("connection nats://");
      expect(Object.hasOwn(safe.cause as Error, "status")).toBe(false); // No new text allowance for status.
      expect((safe as JetStreamApiError).apiError()).toEqual({ code: 503, err_code: 10008, description: safe.message });
      expect(Object.getOwnPropertyNames(safe).sort()).toEqual(["apiError", "cause", "code", "message", "name", "stack", "status"]);
      expect(Object.getOwnPropertySymbols(safe)).toEqual([]);
      expectRedacted(safe);
    }
    expect(original.apiError().description).toContain("urlPass732");
    expect(original.apiError()).toHaveProperty("payload", payload);
  });

  it("never coerces or serializes object-valued SDK fields, descriptions or causes", async () => {
    let coercions = 0;
    const opaque = { name: "Payload", status: 503, whatever: "opaqueSensitive740", nested: { token: "bareToken735" },
      toString() { coercions++; throw new Error("object coercion"); },
      toJSON() { coercions++; throw new Error("object serialization"); } };
    const api: ApiError = { code: 503, err_code: 10008, description: "server failed" };
    const original = new JetStreamApiError(api, { cause: opaque });
    // Mutate after the SDK constructor: its own Error message coercion is not part of the sanitizer.
    api.description = opaque as unknown as string;
    api.err_code = opaque as unknown as number;
    for (const key of ["name", "message", "stack", "code"]) Object.defineProperty(original, key, { value: opaque, configurable: true });
    Object.assign(original, { payload: opaque });
    const safe = await guardedFailure(original) as JetStreamApiError;
    const printable = "[object Object] {name: Payload, status: 503}";
    expect([safe.name, safe.message, safe.stack, safe.code]).toEqual([printable, printable, printable, printable]);
    expect((safe.cause as Error).message).toBe(printable);
    expect(safe.apiError()).toEqual({ code: 503, err_code: NaN, description: printable });
    expect(Object.hasOwn(safe, "payload")).toBe(false);
    expectRedacted(safe);
    expect(coercions).toBe(0);
  });

  it("sanitizes aggregate members and caps a cyclic real SDK cause chain", () => {
    const member = new JetStreamApiError({ code: 503, err_code: 10008, description: `api failed ${detail}` });
    const aggregate = redactNatsError(new AggregateError([member, new ConnectionError(`connection ${detail}`), { payload: "opaqueSensitive740" }],
      `all failed ${detail}`, { cause: new TimeoutError() })) as AggregateError;
    expect(aggregate).toBeInstanceOf(AggregateError);
    expect(aggregate.errors[0]).toBeInstanceOf(JetStreamApiError);
    expect(aggregate.errors[0].code).toBe(10008);
    expect(aggregate.errors[1]).toBeInstanceOf(ConnectionError);
    expect(aggregate.errors[2].message).toBe("[object Object]");
    expect(aggregate.cause).toBeInstanceOf(TimeoutError);
    expectRedacted(aggregate);

    const cyclic = new RequestError(`request ${detail}`);
    Object.defineProperty(cyclic, "cause", { value: cyclic });
    let safe = redactNatsError(cyclic);
    expectRedacted(safe);
    for (let depth = 0; depth < 4; depth++) {
      expect(safe).toBeInstanceOf(RequestError);
      expect(safe.cause).toBeInstanceOf(RequestError);
      safe = safe.cause as Error;
    }
    expect(safe.cause).toBeUndefined();
  });

  it("sanitizes the consumer callback and returned stop rejection, including double guards", async () => {
    const original = new JetStreamApiError({ code: 503, err_code: 10008, description: `consumer failed ${detail}` });
    const port = rejectingPort(original);
    port.consume = async (_stream, _after, _onMessage, onError) => {
      onError?.(original);
      return { stop: async () => { throw original; } };
    };
    const seen: unknown[] = [];
    const consumer = await guardNatsPort(guardNatsPort(port)).consume("STREAM", 0, () => undefined, error => seen.push(error));
    expect(seen).toHaveLength(1);
    const callback = seen[0] as JetStreamApiError;
    expect(callback).toBeInstanceOf(JetStreamApiError);
    expect(callback.code).toBe(10008);
    expect(callback.status).toBe(503);
    expect(callback.apiError().description).toContain("consumer failed");
    expectRedacted(callback);
    await expect(consumer.stop()).rejects.toSatisfy((error: JetStreamApiError) => {
      expect(error).toBeInstanceOf(JetStreamApiError);
      expect(error.code).toBe(10008);
      expect(error.status).toBe(503);
      expectRedacted(error);
      return true;
    });
  });
});
