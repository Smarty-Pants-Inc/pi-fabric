import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  connectionOptions: undefined as unknown,
  publisherOptions: undefined as unknown,
  publishInput: undefined as unknown,
  closed: 0,
  result: { event: { id: "published", sequence: 17 }, duplicate: false },
}));
vi.mock("../src/transport/nats/connection.js", () => ({
  openBootstrapNatsConnection: async (options: unknown) => {
    state.connectionOptions = options;
    return { close: async () => { state.closed += 1; } };
  },
}));
vi.mock("../src/transport/nats/events-publish.js", () => ({
  openNatsEventsPublisher: async (_connection: unknown, options: unknown) => {
    state.publisherOptions = options;
    return {
      stream: "FABRIC_EV_DEMO",
      publishWithAck: async (input: unknown) => { state.publishInput = input; return state.result; },
    };
  },
}));

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  state.connectionOptions = undefined;
  state.publisherOptions = undefined;
  state.publishInput = undefined;
  state.closed = 0;
  state.result = { event: { id: "published", sequence: 17 }, duplicate: false };
  vi.restoreAllMocks();
});

const baseArgs = ["publish", "--topic", "mail.inbound", "--creds", "/secure/user.creds", "--ca", "/secure/ca.pem",
  "--servers", "tls://nats.example.test:4222", "--org", "demo"];

describe("pi-fabric events publish CLI", () => {
  it("publishes one JSON event and reports only stream, sequence, and duplicate", async () => {
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const { runFabricCli } = await import("../src/cli/index.js");
    expect(await runFabricCli(["events", ...baseArgs, "--id", "mail-event-17", "--data", "{\"message\":\"hello\"}"])).toBe(0);
    expect(state.connectionOptions).toMatchObject({ servers: "tls://nats.example.test:4222", credentials: "/secure/user.creds", tls: { caFile: "/secure/ca.pem" } });
    expect(state.publisherOptions).toEqual({ org: "demo" });
    expect(state.publishInput).toMatchObject({
      id: "mail-event-17", topic: "mail.inbound", kind: "message", data: { message: "hello" },
      from: { id: "mail-service", name: "mail-service", kind: "agent" }, origin: "external", untrusted: true,
    });
    expect(stdout).toHaveBeenCalledWith('{"stream":"FABRIC_EV_DEMO","seq":17,"duplicate":false}\n');
    expect(stderr).not.toHaveBeenCalled();
    expect(state.closed).toBe(1);
  });

  it("accepts --data-file and refuses missing or conflicting input before dialing", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-events-cli-"));
    roots.push(root);
    const file = path.join(root, "event.json");
    fs.writeFileSync(file, "{\"message\":\"from file\"}");
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const { runFabricCli } = await import("../src/cli/index.js");
    expect(await runFabricCli(["events", ...baseArgs, "--data-file", file])).toBe(0);
    expect(state.publishInput).toMatchObject({ data: { message: "from file" } });
    state.connectionOptions = undefined;
    expect(await runFabricCli(["events", ...baseArgs])).toBe(2);
    expect(await runFabricCli(["events", ...baseArgs, "--data", "{}", "--data-file", file])).toBe(2);
    expect(state.connectionOptions).toBeUndefined();
    expect(stderr).toHaveBeenCalled();
    expect(stdout).toHaveBeenCalledTimes(1);
  });
});
