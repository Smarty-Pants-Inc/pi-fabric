import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
const MACHINE_A = "machine-a";
const MACHINE_B = "machine-b";
let harness: PathBHarness;

describe.skipIf(!binary)("authenticated Path B transport", () => {
  beforeAll(async () => { harness = await createPathBHarness({ binary: binary! }); });
  afterAll(async () => { await harness?.stop(); });

  it("carries a command and its admission ACK between two authenticated machine runtimes", async () => {
    const fixture = harness;
    const runtimes: Array<Awaited<ReturnType<typeof openNatsTransport>>> = [];
    const abort = new AbortController();
    try {
      const bootstrap = await fixture.connect(fixture.users.bootstrap);
      const manager = await jetstreamManager(bootstrap);
      const js = jetstream(bootstrap);
      const policy = { maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000, replicas: 1 as const };
      await ensureTransportStream(manager, { ...policy, name: ADVISORY_CAPTURE_STREAM, subjects: [transportAdvisoryStreamSubject], maxAgeMs: 30 * 86_400_000, maxMessageBytes: 65_536 });
      await openNatsKvForMachine(js, "leases", MACHINE_A, true);
      await openNatsKvForMachine(js, "presence", MACHINE_A, true);
      for (const machine of [MACHINE_A, MACHINE_B]) await ensureTransportStream(manager, { ...policy, name: transportInboxStream(machine), subjects: [transportInboxStreamSubject(machine)] });
      const a = await openNatsTransport({ servers: fixture.serverUrl, credentials: fixture.users.machineA.credsPath, machine: MACHINE_A, hostId: "runtime-auth-a" });
      runtimes.push(a);
      const b = await openNatsTransport({ servers: fixture.serverUrl, credentials: fixture.users.machineB.credsPath, machine: MACHINE_B, hostId: "runtime-auth-b" });
      runtimes.push(b);
      const bInbox = b.inbox(abort.signal)[Symbol.asyncIterator]();
      const aInbox = a.inbox(abort.signal)[Symbol.asyncIterator]();
      const command = { id: "authenticated-command", kind: "command" as const, to: b.self, targetId: "child-auth-b", operation: "followUp", from: { id: a.self.hostId, name: "a", kind: "main" as const }, body: { text: "two identities" } };
      await a.send(command);
      const received = (await bInbox.next()).value!;
      expect(received.sourceMachine).toBe(MACHINE_A);
      expect(received.payload.body).toEqual(command.body);
      await b.send({ ...command, id: `ack:${command.id}`, kind: "ack", operation: "accepted", to: a.self, from: { id: b.self.hostId, name: "b", kind: "main" }, body: { commandId: command.id } });
      await received.ack();
      const acknowledgement = (await aInbox.next()).value!;
      expect(acknowledgement).toMatchObject({ sourceMachine: MACHINE_B, payload: { id: `ack:${command.id}`, kind: "ack" } });
      await acknowledgement.ack();
      expect(await a.drainState()).toEqual({ pending: 0, ackPending: 0 });
      expect(await b.drainState()).toEqual({ pending: 0, ackPending: 0 });
      abort.abort();
      await bInbox.return?.();
      await aInbox.return?.();
    } finally {
      abort.abort();
      await Promise.all(runtimes.map((runtime) => runtime.close()));
    }
  }, 30_000);

  it("verifies USER.INFO against the signed user and exact own-source grant", async () => {
    const connection = await openNatsConnection({ servers: harness.serverUrl, credentials: harness.users.machineA.credsPath, machine: MACHINE_A });
    expect(connection.userInfo).toBeDefined();
    await connection.close();
  });

  it("accepts the operator's narrower own-source ACK grant", async () => {
    const connection = await openNatsConnection({ servers: harness.serverUrl, credentials: harness.users.maintainer.credsPath, machine: harness.users.maintainer.machine });
    await connection.close();
  });

  it.each([">", "fabric.>", "fabric.in.*.*.*.*.machine-b"])("refuses a server-expanded broader source grant: %s", async (grant) => {
    const fixture = await createPathBHarness({ binary: binary!, additionalAgentPublish: [grant] });
    try {
      await expect(openNatsConnection({ servers: fixture.serverUrl, credentials: fixture.users.machineA.credsPath, machine: MACHINE_A })).rejects.toThrow("machine tag mismatch");
    } finally {
      await fixture.stop();
    }
  });

  it("refuses credentials bound to another configured machine", async () => {
    await expect(openNatsConnection({ servers: harness.serverUrl, credentials: harness.users.machineA.credsPath, machine: MACHINE_B })).rejects.toThrow("machine tag mismatch");
  });
});
