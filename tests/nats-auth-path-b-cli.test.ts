// Trust-boundary proof: registered mint CLI -> private files -> real broker authorization.
// Only the fixture bootstrap user is hand-issued (with the CLI-created account identity); it creates
// resources, never changes the CLI-issued account JWT or runtime/operator grants. No shared service is used.
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Kvm } from "@nats-io/kv";
import { AckPolicy, DeliverPolicy, jetstream, jetstreamManager, StorageType } from "@nats-io/jetstream";
import { nkeys, PermissionViolationError, type NatsConnection } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { decodeAndVerifyNatsJwt, encodeSystemUserJwt, formatUserCreds } from "../src/mesh/nats-auth/jwt.js";
import {
  createPathBHarness,
  PATH_B_MACHINES,
  resolveNatsBinary,
  type PathBHarness,
  type PathBIdentity,
  type PathBUser,
} from "./helpers/nats-cluster.js";

// Source is usable in test-only checkouts; the compiled lane runs the package.json bin after a fresh build.
const compiledCli = process.env.NATS_AUTH_TEST_COMPILED === "1";
const cliEntry = fileURLToPath(new URL(compiledCli ? "../dist/cli/index.js" : "../src/cli/index.ts", import.meta.url));
const cliRuntime = compiledCli ? "node" : process.versions.bun ? process.execPath : "bun";
const API_TIMEOUT_MS = 2_500;
const STREAM_A = "FABRIC_IN_machine-a";
const STREAM_B = "FABRIC_IN_machine-b";
// Deliberately literal: changing the production fixed durable must not silently widen this proof.
const OPERATOR_DURABLE = "transport_dlq_operator";
const OTHER_DURABLE = "not-the-operator";
const MESH = "0".repeat(32);
const STATE_STREAM = `FABRIC_MESH_${MESH}`;
const source = (machine: string) => `fabric.in.${machine}.run.cmd.message.${machine}`;
const streams = { custodyStream: "CLI_FIXTURE_CUSTODY", livenessStream: "CLI_FIXTURE_LIVENESS" };
const secretShape = /(?:^|[^A-Z2-7])S[OAUNC][A-Z2-7]{56}(?![A-Z2-7])|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/;

function mint(root: string, kind: "operator" | "account" | "user", name: string, input: object, flags: readonly string[] = []): Record<string, unknown> {
  const outDir = path.join(root, name);
  // Exactly the registered `pi-fabric nats` entry, not an invented mint wrapper or in-process minter.
  const result = spawnSync(cliRuntime, [cliEntry, "nats", "mint", kind, ...flags, "--out-dir", outDir], {
    input: JSON.stringify(input), encoding: "utf8", timeout: 30_000,
    cwd: root, env: { PATH: process.env.PATH ?? "", HOME: root, TMPDIR: root, LANG: "C" },
  });
  expect(secretShape.test(`${result.stdout ?? ""}\n${result.stderr ?? ""}`), `${kind} diagnostics contain no credential bytes`).toBe(false);
  expect(result.status, `${kind}/${name} registered mint command exits successfully`).toBe(0);
  let metadata: unknown;
  try { metadata = JSON.parse(result.stdout ?? ""); } catch { /* Do not print untrusted output. */ }
  expect(metadata !== null && typeof metadata === "object", `${kind} returns public metadata`).toBe(true);
  expect(fs.lstatSync(outDir).mode & 0o7777).toBe(0o700);
  for (const file of fs.readdirSync(outDir)) {
    const stat = fs.lstatSync(path.join(outDir, file));
    expect(stat.isFile() && !stat.isSymbolicLink(), "mint artifacts are regular files").toBe(true);
    expect(stat.mode & 0o7777).toBe(0o600);
    expect(stat.uid).toBe(process.getuid?.());
  }
  return metadata as Record<string, unknown>;
}

function cliIdentity(root: string): PathBIdentity {
  const read = (dir: string, file: string) => fs.readFileSync(path.join(root, dir, file), "utf8").trim();
  mint(root, "operator", "operator", { operatorName: "cli-path-b-fixture" });
  const operatorJwt = read("operator", "operator.jwt");
  const systemAccountJwt = read("operator", "sys-account.jwt");
  const systemAccount = decodeAndVerifyNatsJwt(systemAccountJwt).claims.sub;
  mint(root, "account", "account", {
    name: "cli-path-b-app", operatorJwt, operatorSigningSeed: read("operator", "operator-signing.nk"),
    jetstream: { memStorage: 0, diskStorage: 64 * 1024 * 1024, streams: 16, consumers: 32 },
    ...streams, pathBMachines: PATH_B_MACHINES,
  });
  const appAccountJwt = read("account", "account.jwt");
  const appAccount = decodeAndVerifyNatsJwt(appAccountJwt).claims.sub;
  const common = { operatorJwt, accountJwt: appAccountJwt, accountPublicKey: appAccount, ...streams, pathBMachines: PATH_B_MACHINES };
  const user = (machine: PathBUser["machine"], role: "agent" | "maintainer", dir: string): PathBUser => {
    const metadata = mint(root, "user", dir, {
      ...common, role, signingSeed: read("account", `signing-${role}.nk`),
      ...(role === "agent"
        ? { host: machine, lane: "cli-proof", session: "cli-proof", release: "cli-proof", meshes: [MESH] }
        : { machine, job: "compaction", runId: "cli-proof" }),
    });
    expect(typeof metadata.publicKey === "string" && metadata.publicKey.startsWith("U"), "mint returns the user public key").toBe(true);
    const credsPath = path.join(root, dir, "user.creds");
    expect(fs.readdirSync(path.join(root, dir))).toEqual(["user.creds"]);
    return { machine, publicKey: metadata.publicKey as string, credsPath };
  };
  const machineA = user("machine-a", "agent", "runtime-a");
  const machineB = user("machine-b", "agent", "runtime-b");
  const maintainer = user("fabric-ops", "maintainer", "messaging-operator");
  // These roles must retain their unchanged base scopes on a Path B account. Issue only;
  // the bootstrap below, never a break-glass client, performs fixture resource creation.
  const baseCommon = { operatorJwt, accountJwt: appAccountJwt, accountPublicKey: appAccount, ...streams };
  mint(root, "user", "principal", {
    ...baseCommon, role: "principal", signingSeed: read("account", "signing-principal.nk"), person: "fixture", device: "fixture",
  });
  mint(root, "user", "break-glass", {
    ...baseCommon, signingSeed: read("account", "signing-break-glass.nk"), person: "fixture", runId: "cli-proof", approvalRef: "fixture-approval-0001",
  }, ["--break-glass"]);

  // Fixture-only root-signed bootstrap. The broker receives the original CLI account JWT unchanged;
  // the tested clients authenticate exclusively from CLI-written user.creds files.
  const accountSeed = fs.readFileSync(path.join(root, "account", "account.nk"));
  const accountPair = nkeys.fromSeed(accountSeed);
  const bootstrapPair = nkeys.createUser();
  const bootstrapSeed = bootstrapPair.getSeed();
  try {
    const token = encodeSystemUserJwt({
      name: "fixture/bootstrap", subject: bootstrapPair.getPublicKey(), expiresAt: Math.floor(Date.now() / 1_000) + 3_600,
    }, { publicKey: accountPair.getPublicKey(), sign: (bytes) => accountPair.sign(bytes) }, { nowSeconds: () => Math.floor(Date.now() / 1_000) });
    const [header, payload] = token.split(".") as [string, string, string];
    const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    claims.nats.pub = { allow: [">"] };
    claims.nats.sub = { allow: [`_INBOX.${bootstrapPair.getPublicKey()}.>`] };
    const body = `${header}.${Buffer.from(JSON.stringify(claims)).toString("base64url")}`;
    const jwt = `${body}.${Buffer.from(accountPair.sign(new TextEncoder().encode(body))).toString("base64url")}`;
    const credsPath = path.join(root, "bootstrap.creds");
    const creds = formatUserCreds(jwt, bootstrapSeed);
    try { fs.writeFileSync(credsPath, creds, { flag: "wx", mode: 0o600 }); } finally { creds.fill(0); }
    return {
      operatorJwt, systemAccount, systemAccountJwt, appAccount, appAccountJwt,
      users: { machineA, machineB, maintainer, bootstrap: { machine: "fabric-ops", publicKey: bootstrapPair.getPublicKey(), credsPath } },
    };
  } finally {
    accountSeed.fill(0);
    bootstrapSeed.fill(0);
    accountPair.clear();
    bootstrapPair.clear();
  }
}

/** Broker status is the oracle. An allowed request, API error, or timeout is NOT a denial. */
class PermissionOracle {
  private deliver: ((error: Error) => void) | undefined;
  constructor(private readonly nc: NatsConnection) {
    void (async () => {
      for await (const status of nc.status()) if (status.type === "error") this.deliver?.(status.error);
    })().catch(() => undefined);
  }
  async denied(subject: string, payload = "{}", operation: "publish" | "subscription" = "publish"): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const denial = new Promise<Error>((resolve) => { this.deliver = resolve; });
    try {
      let thrown: unknown;
      try {
        if (operation === "subscription") this.nc.subscribe(subject);
        else this.nc.publish(subject, payload);
        await this.nc.flush();
      } catch (error) { thrown = error; }
      const error = thrown instanceof PermissionViolationError ? thrown : await Promise.race([
        denial,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`no broker PermissionViolationError for ${operation} ${subject}; timeout is not denial`)), 1_500);
        }),
      ]);
      expect(error).toBeInstanceOf(PermissionViolationError);
      if (error instanceof PermissionViolationError) expect({ operation: error.operation, subject: error.subject }).toEqual({ operation, subject });
    } finally {
      if (timer) clearTimeout(timer);
      this.deliver = undefined;
    }
  }
}

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
let harness: PathBHarness | undefined;
let runtime: NatsConnection;
let otherRuntime: NatsConnection;
let operator: NatsConnection;
let bootstrap: NatsConnection;
let runtimeOracle: PermissionOracle;
let operatorOracle: PermissionOracle;

// CLI mint intentionally refuses Windows until private ACL verification exists (#11482).
describe.skipIf(process.platform === "win32")("Path B CLI-issued credentials on a real nats-server", { concurrent: false, shuffle: false }, () => {
  if (!binary) {
    it.skip("requires NATS_BIN/PATH/local nats-server; NATS_AUTH_REQUIRED=1 makes absence fatal", () => undefined);
    return;
  }
  beforeAll(async () => {
    harness = await createPathBHarness({ binary, identityFactory: cliIdentity });
    runtime = await harness.connect(harness.users.machineA);
    otherRuntime = await harness.connect(harness.users.machineB);
    operator = await harness.connect(harness.users.maintainer);
    bootstrap = await harness.connect(harness.users.bootstrap);
    runtimeOracle = new PermissionOracle(runtime);
    operatorOracle = new PermissionOracle(operator);
    const manager = await jetstreamManager(bootstrap);
    for (const [name, subject] of [[STREAM_A, "fabric.in.machine-a.>"], [STREAM_B, "fabric.in.machine-b.>"], ["FABRIC_ADV", "$JS.EVENT.ADVISORY.>"]]) {
      await manager.streams.add({ name: name!, subjects: [subject!], storage: StorageType.File, allow_direct: false });
    }
    await manager.streams.add({ name: STATE_STREAM, subjects: [`${STATE_STREAM}.state`, `${STATE_STREAM}.snapshot`], storage: StorageType.File, allow_direct: false });
    await new Kvm(bootstrap).create("fabric_presence", { history: 1, allow_direct: false });
    await new Kvm(bootstrap).create("fabric_leases", { history: 1, allow_direct: false });
  }, 90_000);
  afterAll(async () => { await harness?.stop(); harness = undefined; });

  it("loads CLI operator/account/runtime/maintainer files and issues principal/break-glass on a Path B account", () => {
    expect(harness?.configText.includes("sync_interval: always")).toBe(true);
    expect(harness?.serverVersion).toMatch(/^\d+\.\d+\.\d+/);
    expect(harness?.users.machineA.machine).toBe("machine-a");
    expect(harness?.users.machineB.machine).toBe("machine-b");
    expect(harness?.users.maintainer.machine).toBe("fabric-ops");
  });

  // These precede bootstrap provisioning of the fixed advisory durable. On the old permissive
  // template DELETE targets a nonexistent resource and CREATE has an invalid body: no destructive
  // path or lifecycle mutation can occur even when this regression proof correctly fails.
  it.each([
    `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${OPERATOR_DURABLE}`,
    `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${OPERATOR_DURABLE}.$JS.EVENT.ADVISORY.>`,
    `$JS.API.CONSUMER.DURABLE.CREATE.FABRIC_ADV.${OPERATOR_DURABLE}`,
    `$JS.API.CONSUMER.DELETE.FABRIC_ADV.${OPERATOR_DURABLE}`,
    `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${OTHER_DURABLE}`,
    `$JS.API.CONSUMER.DELETE.FABRIC_ADV.${OTHER_DURABLE}`,
  ])("operator lifecycle is broker-denied: %s", async (subject) => {
    await operatorOracle.denied(subject);
  });

  it("runtime sends with its own source, pulls/ACKs its own consumer, and publishes bound state/snapshot", async () => {
    const manager = await jetstreamManager(runtime);
    await manager.consumers.add(STREAM_A, { name: "runtime-a", durable_name: "runtime-a", ack_policy: AckPolicy.Explicit, filter_subject: source("machine-a") });
    const js = jetstream(runtime);
    const published = await js.publish(source("machine-a"), "cli-runtime-own");
    expect(published.stream).toBe(STREAM_A);
    const consumer = await js.consumers.get(STREAM_A, "runtime-a");
    const message = await consumer.next({ expires: API_TIMEOUT_MS });
    expect(message?.string()).toBe("cli-runtime-own");
    if (!message) throw new Error("own consumer did not deliver");
    expect(await message.ackAck({ timeout: API_TIMEOUT_MS })).toBe(true);
    expect((await manager.consumers.info(STREAM_A, "runtime-a")).num_ack_pending).toBe(0);
    for (const suffix of ["state", "snapshot"]) {
      expect((await js.publish(`${STATE_STREAM}.${suffix}`, `cli-${suffix}`)).stream).toBe(STATE_STREAM);
    }
  });

  it("runtime cannot forge a foreign source, use foreign consumers/ACKs, or write foreign KV prefixes", async () => {
    const manager = await jetstreamManager(otherRuntime);
    await manager.consumers.add(STREAM_B, { name: "runtime-b", durable_name: "runtime-b", ack_policy: AckPolicy.Explicit, filter_subject: source("machine-b") });
    await jetstream(otherRuntime).publish(source("machine-b"), "cli-foreign-owned");
    const foreign = await otherRuntime.request(`$JS.API.CONSUMER.MSG.NEXT.${STREAM_B}.runtime-b`, JSON.stringify({ batch: 1 }), {
      timeout: API_TIMEOUT_MS, noMux: true, reply: `_INBOX.${harness!.users.machineB.publicKey}.foreign-pull`,
    });
    expect(foreign.string()).toBe("cli-foreign-owned");
    if (!foreign.reply) throw new Error("foreign consumer did not supply ACK subject");
    await runtimeOracle.denied(source("machine-b"), "forged");
    for (const subject of [
      `$JS.API.CONSUMER.CREATE.${STREAM_B}.forged.${source("machine-b")}`,
      `$JS.API.CONSUMER.INFO.${STREAM_B}.runtime-b`,
      `$JS.API.CONSUMER.MSG.NEXT.${STREAM_B}.runtime-b`,
      `$JS.API.CONSUMER.DELETE.${STREAM_B}.absent-consumer`,
      `$JS.API.STREAM.MSG.GET.${STREAM_A}`,
      `$JS.API.STREAM.MSG.GET.${STREAM_B}`,
    ]) await runtimeOracle.denied(subject);
    await runtimeOracle.denied(foreign.reply, "+ACK");
    expect((await manager.consumers.info(STREAM_B, "runtime-b")).num_ack_pending).toBe(1);
    await otherRuntime.request(foreign.reply, "+ACK", { timeout: API_TIMEOUT_MS });
    expect((await manager.consumers.info(STREAM_B, "runtime-b")).num_ack_pending).toBe(0);
    for (const bucket of ["fabric_presence", "fabric_leases"]) {
      const kv = await new Kvm(runtime).open(bucket);
      await kv.put("machine-a.owned", "cli-owned-value");
      expect((await kv.get("machine-a.owned"))?.string()).toBe("cli-owned-value");
      await runtimeOracle.denied(`$KV.${bucket}.machine-b.foreign`, "forged");
    }
    await runtimeOracle.denied(`_INBOX.${harness!.users.machineB.publicKey}.>`, "", "subscription");
  }, 30_000);

  it("report-only operator inspects inventory metadata and pulls its preprovisioned AckNone advisory durable without ACK authority", async () => {
    const bootstrapManager = await jetstreamManager(bootstrap);
    await bootstrapManager.consumers.add("FABRIC_ADV", {
      name: OPERATOR_DURABLE, durable_name: OPERATOR_DURABLE, ack_policy: AckPolicy.None,
      deliver_policy: DeliverPolicy.New, filter_subject: "$JS.EVENT.ADVISORY.cli-proof",
    });
    const manager = await jetstreamManager(operator);
    const js = jetstream(operator);
    for (const [stream, durable] of [[STREAM_A, "runtime-a"], [STREAM_B, "runtime-b"]] as const) {
      expect((await manager.streams.info(stream)).config.name).toBe(stream);
      expect((await manager.consumers.info(stream, durable)).name).toBe(durable);
    }
    await jetstream(bootstrap).publish("$JS.EVENT.ADVISORY.cli-proof", "cli-advisory");
    const consumer = await js.consumers.get("FABRIC_ADV", OPERATOR_DURABLE);
    const message = await consumer.next({ expires: API_TIMEOUT_MS });
    expect(message?.string()).toBe("cli-advisory");
    if (!message) throw new Error("fixed advisory durable did not deliver");
    // AckNone delivery needs no acknowledgement. The operator must never publish any $JS.ACK.
    const info = await manager.consumers.info("FABRIC_ADV", OPERATOR_DURABLE);
    expect(info.config.ack_policy).toBe(AckPolicy.None);
    expect(info.num_ack_pending).toBe(0);
  }, 15_000);

  it.each([
    "fabric.in.machine-a.run.cmd.message.fabric-ops",
    "fabric.in.machine-b.run.cmd.message.fabric-ops",
    "fabric.in.machine-a.run.ack.message.machine-a",
    "fabric.in.machine-a.run.ack.message.fabric-ops",
    "fabric.in.machine-b.run.ack.message.fabric-ops",
    "fabric.in.machine-c.run.ack.message.fabric-ops",
    "fabric.ev.fabric-ops.fabric.ops.cli-proof",
    "$JS.API.STREAM.MSG.GET.KV_fabric_presence",
    "$JS.API.STREAM.MSG.GET.KV_fabric_leases",
    ...[STREAM_A, STREAM_B].flatMap((stream) => [
      `$JS.API.STREAM.MSG.GET.${stream}`,
      `$JS.API.DIRECT.GET.${stream}`,
      `$JS.API.DIRECT.GET.${stream}.fabric.in.any`,
    ]),
    `$JS.API.CONSUMER.INFO.FABRIC_ADV.${OTHER_DURABLE}`,
    `$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.${OTHER_DURABLE}`,
    `$JS.ACK.FABRIC_ADV.${OPERATOR_DURABLE}.1.1.1.1.0`,
    `$JS.ACK.FABRIC_ADV.${OTHER_DURABLE}.1.1.1.1.0`,
    `$JS.ACK.${STREAM_A}.runtime-a.1.1.1.1.0`,
    "$JS.API.STREAM.CREATE.FABRIC_ADV",
    "$JS.API.STREAM.UPDATE.FABRIC_ADV",
    "$JS.API.STREAM.DELETE.CLI_ABSENT_STREAM",
    "$JS.API.STREAM.PURGE.CLI_ABSENT_STREAM",
  ])("report-only operator private-read/relay/command/foreign-consumer/lifecycle is broker-denied: %s", async (subject) => {
    await operatorOracle.denied(subject, subject.startsWith("$JS.ACK.") ? "+ACK" : "{}");
  });
});
