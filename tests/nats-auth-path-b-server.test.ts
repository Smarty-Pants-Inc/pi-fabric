import { Kvm } from "@nats-io/kv";
import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  ReplayPolicy,
  StorageType,
} from "@nats-io/jetstream";
import { PermissionViolationError } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { PATH_B_ADVISORY_DURABLE } from "../src/mesh/nats-auth/permissions.js";

const API_TIMEOUT_MS = 2_500;
const DENIAL_TIMEOUT_MS = 2_000;
const MACHINE_A = "machine-a";
const MACHINE_B = "machine-b";
const STREAM_A = "FABRIC_IN_machine-a";
const STREAM_B = "FABRIC_IN_machine-b";
const sourceSubject = (machine: string) => `fabric.in.${machine}.run.cmd.message.${machine}`;

class PermissionOracle {
  private readonly waiters: Array<(error: Error) => void> = [];

  constructor(private readonly nc: import("@nats-io/transport-node").NatsConnection) {
    void (async () => {
      for await (const status of nc.status()) {
        if (status.type === "error") this.waiters.shift()?.(status.error);
      }
    })().catch(() => undefined);
  }

  async expect(operation: "publish" | "subscription", subject: string, action: () => unknown | Promise<unknown>): Promise<void> {
    let deliver!: (error: Error) => void;
    const received = new Promise<Error>((resolve) => {
      deliver = resolve;
      this.waiters.push(resolve);
    });
    let thrown: unknown;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      try {
        await action();
        await this.nc.flush();
      } catch (error) {
        thrown = error;
      }
      const error = thrown instanceof PermissionViolationError
        ? thrown
        : await Promise.race([
            received,
            new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`no permission denial for ${operation} ${subject}`)), DENIAL_TIMEOUT_MS); }),
          ]);
      expect(error).toBeInstanceOf(PermissionViolationError);
      if (error instanceof PermissionViolationError) {
        expect({ operation: error.operation, subject: error.subject }).toEqual({ operation, subject });
      }
    } finally {
      if (timer) clearTimeout(timer);
      const index = this.waiters.indexOf(deliver);
      if (index >= 0) this.waiters.splice(index, 1);
    }
  }
}

async function expectDenied(
  oracle: PermissionOracle,
  operation: "publish" | "subscription",
  subject: string,
  action: () => unknown | Promise<unknown>,
): Promise<void> {
  await oracle.expect(operation, subject, action);
}

function streamConfig(name: string, subject: string) {
  return {
    name,
    subjects: [subject],
    storage: StorageType.File,
    allow_direct: false,
    max_age: 0,
  };
}

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
let harness: PathBHarness | undefined;

describe("Path B real nats-server authorization", () => {
  if (!binary) {
    it.skip("set NATS_BIN or put nats-server on PATH to run the real-server authorization test", () => undefined);
    return;
  }

  beforeAll(async () => {
    harness = await createPathBHarness({ binary });
  }, 60_000);

  it("T-OPS-NOREAD: expands machine tags and enforces runtime, report-only operator, inbox and KV boundaries", async () => {
    if (!harness) throw new Error("Path B harness was not initialized");
    const fixture = harness;
    const maintainerNc = await fixture.connect(fixture.users.maintainer);
    const bootstrapNc = await fixture.connect(fixture.users.bootstrap);
    const machineANc = await fixture.connect(fixture.users.machineA);
    const machineBNc = await fixture.connect(fixture.users.machineB);
    const operatorManager = await jetstreamManager(maintainerNc);
    const bootstrap = await jetstreamManager(bootstrapNc);
    const machineAManager = await jetstreamManager(machineANc);
    const machineBManager = await jetstreamManager(machineBNc);
    const machineAJs = jetstream(machineANc);
    const machineBJs = jetstream(machineBNc);
    const machineAOracle = new PermissionOracle(machineANc);
    const maintainerOracle = new PermissionOracle(maintainerNc);
    expect(fixture.configText.includes("sync_interval: always")).toBe(true);
    expect(fixture.serverVersion).toMatch(/^\d+\.\d+\.\d+/);

    // A separate fixture-only bootstrap scope seeds resources. Neither runtime nor maintainer gets lifecycle authority.
    await bootstrap.streams.add(streamConfig(STREAM_A, `fabric.in.${MACHINE_A}.>`));
    await bootstrap.streams.add(streamConfig(STREAM_B, `fabric.in.${MACHINE_B}.>`));
    await new Kvm(bootstrapNc).create("fabric_presence", { history: 1, allow_direct: false });
    await new Kvm(bootstrapNc).create("fabric_leases", { history: 1, allow_direct: false });
    for (const subject of [
      "$JS.API.STREAM.CREATE.FABRIC_ADV",
      "$JS.API.STREAM.UPDATE.FABRIC_ADV",
      "$JS.API.STREAM.DELETE.FABRIC_ADV",
      "$JS.API.STREAM.PURGE.FABRIC_ADV",
    ]) {
      await expectDenied(maintainerOracle, "publish", subject, () => maintainerNc.publish(subject, "{}"));
    }

    const ownConsumer = await machineAManager.consumers.add(STREAM_A, {
      name: "machine-a-consumer",
      durable_name: "machine-a-consumer",
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      filter_subject: sourceSubject(MACHINE_A),
    });
    expect(ownConsumer.name).toBe("machine-a-consumer");
    const foreignConsumer = await machineBManager.consumers.add(STREAM_B, {
      name: "machine-b-consumer",
      durable_name: "machine-b-consumer",
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      filter_subject: sourceSubject(MACHINE_B),
    });
    expect(foreignConsumer.name).toBe("machine-b-consumer");

    // The JWT machine tag expands {{tag(machine)}}: own source publish and own consumer pull/ack work.
    const ownPublish = await machineAJs.publish(sourceSubject(MACHINE_A), "machine-a-owned");
    expect(ownPublish.stream).toBe(STREAM_A);
    // E19: the operator can inspect counters/configuration, never retained private bodies.
    expect((await operatorManager.streams.info(STREAM_A)).config.name).toBe(STREAM_A);
    expect((await operatorManager.consumers.info(STREAM_A, "machine-a-consumer")).name).toBe("machine-a-consumer");
    await expectDenied(maintainerOracle, "publish", `$JS.API.STREAM.MSG.GET.${STREAM_A}`,
      () => operatorManager.streams.getMessage(STREAM_A, { seq: ownPublish.seq }));
    const ownConsumerClient = await machineAJs.consumers.get(STREAM_A, "machine-a-consumer");
    const ownMessage = await ownConsumerClient.next({ expires: API_TIMEOUT_MS });
    expect(ownMessage?.string()).toBe("machine-a-owned");
    if (!ownMessage) throw new Error("own consumer did not deliver its published message");
    expect(await ownMessage.ackAck({ timeout: API_TIMEOUT_MS })).toBe(true);
    expect((await machineAManager.consumers.info(STREAM_A, "machine-a-consumer")).num_ack_pending).toBe(0);

    const foreignPublish = await machineBJs.publish(sourceSubject(MACHINE_B), "machine-b-owned");
    expect(foreignPublish.stream).toBe(STREAM_B);
    await expectDenied(maintainerOracle, "publish", `$JS.API.STREAM.MSG.GET.${STREAM_B}`,
      () => operatorManager.streams.getMessage(STREAM_B, { seq: foreignPublish.seq }));
    const relaySubject = `fabric.in.${MACHINE_B}.run.ack.message.fabric-ops`;
    await expectDenied(maintainerOracle, "publish", relaySubject, () => maintainerNc.publish(relaySubject, "operator-relay"));
    const outsideInventory = "fabric.in.machine-c.run.ack.message.fabric-ops";
    await expectDenied(maintainerOracle, "publish", outsideInventory, () => maintainerNc.publish(outsideInventory, "outside-inventory"));
    const foreignMessage = await machineBNc.request(`$JS.API.CONSUMER.MSG.NEXT.${STREAM_B}.machine-b-consumer`, JSON.stringify({ batch: 1 }), { timeout: API_TIMEOUT_MS, noMux: true, reply: `_INBOX.${fixture.users.machineB.publicKey}.foreign-pull` });
    expect(foreignMessage.string()).toBe("machine-b-owned");
    expect(foreignMessage.reply?.startsWith(`$JS.ACK.${STREAM_B}.`)).toBe(true);
    if (!foreignMessage.reply) throw new Error("foreign consumer message has no ACK subject");

    // A forged source, every foreign consumer operation, and the foreign ACK subject are refused by the real server.
    await expectDenied(machineAOracle, "publish", sourceSubject(MACHINE_B), () => machineANc.publish(sourceSubject(MACHINE_B), "forged-source"));
    await expectDenied(machineAOracle, "publish", `$JS.API.CONSUMER.CREATE.${STREAM_B}.machine-a-forged-consumer.${sourceSubject(MACHINE_B)}`, () => machineAManager.consumers.add(STREAM_B, {
      name: "machine-a-forged-consumer",
      durable_name: "machine-a-forged-consumer",
      ack_policy: AckPolicy.Explicit,
      filter_subject: sourceSubject(MACHINE_B),
    }));
    await expectDenied(machineAOracle, "publish", `$JS.API.CONSUMER.INFO.${STREAM_B}.machine-b-consumer`, () => machineAManager.consumers.info(STREAM_B, "machine-b-consumer"));
    await expectDenied(machineAOracle, "publish", `$JS.API.CONSUMER.MSG.NEXT.${STREAM_B}.machine-b-consumer`, () => machineANc.request(
      `$JS.API.CONSUMER.MSG.NEXT.${STREAM_B}.machine-b-consumer`,
      JSON.stringify({ batch: 1 }),
      { timeout: API_TIMEOUT_MS },
    ));
    await expectDenied(machineAOracle, "publish", foreignMessage.reply, () => machineANc.publish(foreignMessage.reply!, "+ACK"));
    expect((await machineBManager.consumers.info(STREAM_B, "machine-b-consumer")).num_ack_pending).toBe(1);
    foreignMessage.respond("+ACK");
    await machineBNc.flush();
    await expectDenied(machineAOracle, "publish", `$JS.API.CONSUMER.DELETE.${STREAM_B}.machine-b-consumer`, () => machineAManager.consumers.delete(STREAM_B, "machine-b-consumer"));

    // Neither own nor foreign private streams permit bypass reads or message deletion.
    for (const stream of [STREAM_A, STREAM_B]) {
      for (const subject of [`$JS.API.STREAM.MSG.GET.${stream}`, `$JS.API.DIRECT.GET.${stream}`, `$JS.API.DIRECT.GET.${stream}.fabric.in.any`, `$JS.API.STREAM.MSG.DELETE.${stream}`]) {
        await expectDenied(machineAOracle, "publish", subject, () => machineANc.publish(subject, JSON.stringify({ seq: 1 })));
        await expectDenied(maintainerOracle, "publish", subject, () => maintainerNc.publish(subject, JSON.stringify({ seq: 1 })));
      }
    }
    await expectDenied(machineAOracle, "publish", `$JS.API.STREAM.INFO.${STREAM_B}`, () => machineAManager.streams.info(STREAM_B));

    // KV own-prefix access is usable; the other machine prefix is not.
    const presence = await new Kvm(machineANc).open("fabric_presence");
    await presence.put(`${MACHINE_A}.owned`, "owned-value");
    const ownEntry = await presence.get(`${MACHINE_A}.owned`);
    expect(ownEntry?.string()).toBe("owned-value");
    await expectDenied(machineAOracle, "publish", `$KV.fabric_presence.${MACHINE_B}.foreign`, () => presence.put(`${MACHINE_B}.foreign`, "foreign-value"));
    const leases = await new Kvm(machineANc).open("fabric_leases");
    await leases.put(`${MACHINE_A}.h.owned`, "owned-lease");
    await expectDenied(machineAOracle, "publish", `$KV.fabric_leases.${MACHINE_B}.h.foreign`, () => leases.put(`${MACHINE_B}.h.foreign`, "foreign-lease"));
    for (const subject of [`fabric.in.tenant.run.ack.target.${MACHINE_B}`, `fabric.in.tenant.run.cmd.target.${MACHINE_B}`, `fabric.in.tenant.run.cmd.target.${MACHINE_A}`, `fabric.ev.${MACHINE_A}.not-ops.event`, `$JS.API.DIRECT.GET.${STREAM_A}`]) {
      await expectDenied(maintainerOracle, "publish", subject, () => maintainerNc.publish(subject, "forged-or-ungranted"));
    }
    await expectDenied(machineAOracle, "publish", `${sourceSubject(MACHINE_A)}.extra`, () => machineANc.publish(`${sourceSubject(MACHINE_A)}.extra`, "extra-token"));

    // A private inbox is not a data plane: another user's inbox and all runtime stream-admin verbs are denied.
    await expectDenied(machineAOracle, "subscription", `_INBOX.${fixture.users.machineB.publicKey}.>`, () => machineANc.subscribe(`_INBOX.${fixture.users.machineB.publicKey}.>`));
    for (const subject of [
      "$JS.API.STREAM.CREATE.FABRIC_ADV",
      "$JS.API.STREAM.UPDATE.FABRIC_ADV",
      "$JS.API.STREAM.DELETE.FABRIC_ADV",
      "$JS.API.STREAM.PURGE.FABRIC_ADV",
    ]) {
      await expectDenied(machineAOracle, "publish", subject, () => machineANc.publish(subject, "{}"));
    }

  }, 90_000);

  it("binds only the bootstrap-provisioned advisory durable and refuses all operator consumer lifecycle authority", async () => {
    if (!harness) throw new Error("Path B harness was not initialized");
    const bootstrap = await jetstreamManager(await harness.connect(harness.users.bootstrap));
    const bootstrapJs = jetstream(await harness.connect(harness.users.bootstrap));
    const nc = await harness.connect(harness.users.maintainer);
    const manager = await jetstreamManager(nc);
    const js = jetstream(nc);
    const oracle = new PermissionOracle(nc);
    const advisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.FABRIC_IN_machine-a.rt_fixture";
    // Probe mutations before either reserved resource exists: a broken grant cannot
    // destroy the bootstrap stream/durable. Bare CREATE uses name, NOT durable_name.
    const runtime = await harness.connect(harness.users.machineA);
    const runtimeOracle = new PermissionOracle(runtime);
    const bareConfig = JSON.stringify({ stream_name: "FABRIC_ADV", config: { name: PATH_B_ADVISORY_DURABLE, ack_policy: AckPolicy.None } });
    for (const subject of [
      "$JS.API.CONSUMER.CREATE.FABRIC_ADV",
      `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
      `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}.filter`,
      `$JS.API.CONSUMER.DURABLE.CREATE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
      `$JS.API.CONSUMER.DELETE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
      "$JS.API.STREAM.UPDATE.FABRIC_ADV",
      "$JS.API.STREAM.DELETE.FABRIC_ADV",
      "$JS.API.STREAM.PURGE.FABRIC_ADV",
      "$JS.API.CONSUMER.CREATE.FABRIC_ADV.other_operator",
      "$JS.API.CONSUMER.DELETE.FABRIC_ADV.other_operator",
    ]) {
      await expectDenied(oracle, "publish", subject, () => nc.publish(subject, bareConfig));
      await expectDenied(runtimeOracle, "publish", subject, () => runtime.publish(subject, bareConfig));
    }
    await bootstrap.streams.add(streamConfig("FABRIC_ADV", "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"));
    for (const name of [PATH_B_ADVISORY_DURABLE, "other_operator"]) {
      await bootstrap.consumers.add("FABRIC_ADV", {
        durable_name: name, filter_subject: "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>",
        ack_policy: AckPolicy.None, deliver_policy: DeliverPolicy.All,
      });
    }
    const before = await bootstrap.consumers.info("FABRIC_ADV", PATH_B_ADVISORY_DURABLE);
    expect(before.config.ack_policy).toBe(AckPolicy.None);
    expect(before.config.deliver_subject).toBeUndefined();
    for (const subject of [
      "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.other_operator",
      "$JS.API.CONSUMER.INFO.FABRIC_ADV.other_operator",
      "$JS.ACK.FABRIC_ADV.other_operator.1.1.1.1.0",
      `$JS.ACK.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}.1.1.1.1.0`,
      "$JS.ACK.FABRIC_IN_machine-a.rt_owner.1.1.1.1.0",
      "$JS.ACK.arbitrary",
    ]) await expectDenied(oracle, "publish", subject, () => nc.publish(subject, "{}"));
    const sent = await bootstrapJs.publish(advisorySubject, "retained advisory");
    const fixed = await js.consumers.get("FABRIC_ADV", PATH_B_ADVISORY_DURABLE);
    const delivery = await fixed.next({ expires: API_TIMEOUT_MS });
    expect(delivery?.string()).toBe("retained advisory");
    if (!delivery) throw new Error("fixed advisory durable did not deliver");
    // AckNone pull is accepted by nats-server 2.11.17; reporting never acknowledges any subject.
    const after = await manager.consumers.info("FABRIC_ADV", PATH_B_ADVISORY_DURABLE);
    expect(after.created).toBe(before.created);
    expect(after.config).toEqual(before.config);
    expect(after.num_ack_pending).toBe(0);
    expect(after.ack_floor.stream_seq).toBe(sent.seq);
    expect((await bootstrap.consumers.info("FABRIC_ADV", "other_operator")).num_pending).toBe(1);
  }, 30_000);

  it("restarts its owned server without losing the synchronous JetStream store", async () => {
    if (!harness) throw new Error("Path B harness was not initialized");
    const nc = await harness.connect(harness.users.bootstrap);
    const manager = await jetstreamManager(nc);
    await manager.streams.add(streamConfig("RESTART_PROBE", "fixture.restart"));
    await jetstream(nc).publish("fixture.restart", "survives-restart");
    await harness.restart();
    const restarted = await harness.connect(harness.users.bootstrap);
    const restartedManager = await jetstreamManager(restarted);
    expect((await restartedManager.streams.getMessage("RESTART_PROBE", { seq: 1 }))?.string()).toBe("survives-restart");
  }, 30_000);

  afterAll(async () => {
    if (harness) await harness.stop();
    harness = undefined;
  });
});