import fs from "node:fs";
import { jetstream, type JetStreamClient, type ConsumerMessages } from "@nats-io/jetstream";
import { PermissionViolationError, nkeys, type Status } from "@nats-io/transport-node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { encodeAccountJwt, encodeOperatorJwt, encodeUserJwt, formatUserCreds, type ScopedSigningKey } from "../src/mesh/nats-auth/jwt.js";
import { legacyRoleTemplateWithPathB } from "../src/mesh/nats-auth/permissions.js";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { NatsKvStore, openNatsKvForMachine } from "../src/transport/nats/kv.js";
import type { KV } from "@nats-io/kv";
import type { TransportKvEvent } from "../src/transport/types.js";
import {
  createPathBHarness,
  PATH_B_MACHINES,
  PATH_B_PERMISSION_POLICY,
  PATH_B_TEST_MESH,
  resolveNatsBinary,
  type PathBHarness,
  type PathBIdentity,
} from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const MACHINE = "machine-a" as const;
const BUCKET = "fabric_presence";
const WARNING = "NATS user lacks KV flow-control grants; KV watches will stall; re-issue credentials";

type KeyPair = { getPublicKey(): string; getSeed(): Uint8Array; sign(bytes: Uint8Array): Uint8Array };
type Event = TransportKvEvent<{ n: number; padding: string }>;

const bounded = async <T>(promise: Promise<T>, label: string, ms = 8_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out: ${label}`)), ms);
    })]);
  } finally { clearTimeout(timer); }
};

const signerOf = (pair: KeyPair) => ({ publicKey: pair.getPublicKey(), sign: (bytes: Uint8Array) => pair.sign(bytes) });

const legacyIdentityFactory = (root: string): PathBIdentity => {
  const operatorIdentity = nkeys.createOperator() as unknown as KeyPair;
  const operatorSigning = nkeys.createOperator() as unknown as KeyPair;
  const systemAccountPair = nkeys.createAccount() as unknown as KeyPair;
  const systemSigning = nkeys.createAccount() as unknown as KeyPair;
  const appAccountPair = nkeys.createAccount() as unknown as KeyPair;
  const agentSigning = nkeys.createAccount() as unknown as KeyPair;
  const bootstrapSigning = nkeys.createAccount() as unknown as KeyPair;
  const machineA = nkeys.createUser() as unknown as KeyPair;
  const machineB = nkeys.createUser() as unknown as KeyPair;
  const maintainer = nkeys.createUser() as unknown as KeyPair;
  const bootstrap = nkeys.createUser() as unknown as KeyPair;
  const now = Math.floor(Date.now() / 1_000);
  const clock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };
  const systemAccount = systemAccountPair.getPublicKey();
  const appAccount = appAccountPair.getPublicKey();
  const streams = { custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream };
  const operatorJwt = encodeOperatorJwt(
    { name: "legacy-flow-control-fixture", signingKeys: [operatorSigning.getPublicKey()], systemAccount },
    signerOf(operatorIdentity),
    clock,
  );
  const systemAccountJwt = encodeAccountJwt(
    { name: "SYS", subject: systemAccount, signingKeys: [systemSigning.getPublicKey()], limits: {
      conn: -1, leaf: -1, subs: -1, data: -1, payload: -1, mem_storage: 0, disk_storage: 0, streams: 0, consumer: 0,
    } },
    signerOf(operatorSigning),
    clock,
  );
  const scopedKeys: ScopedSigningKey[] = [
    { key: agentSigning.getPublicKey(), role: "agent", template: legacyRoleTemplateWithPathB("agent", streams, PATH_B_MACHINES) },
    { key: bootstrapSigning.getPublicKey(), role: "maintainer", template: { pub: { allow: [">"], deny: [] }, sub: { allow: ["_INBOX.{{subject()}}.>"] } } },
  ];
  const appAccountJwt = encodeAccountJwt(
    { name: "legacy-flow-control-app", subject: appAccount, signingKeys: scopedKeys, limits: {
      conn: -1, leaf: -1, subs: -1, data: -1, payload: -1, mem_storage: 0, disk_storage: 4_294_967_296, streams: 64, consumer: 256,
    } },
    signerOf(operatorSigning),
    clock,
  );
  const user = (pair: KeyPair, machine: "machine-a" | "machine-b" | "fabric-ops", signer: KeyPair, role: "agent" | "maintainer", name: string, fileName: string) => {
    const tags = role === "agent" ? [`role:${role}`, `machine:${machine}`, `mesh:${PATH_B_TEST_MESH}`] : [`role:${role}`, `machine:${machine}`];
    const jwt = encodeUserJwt({ name, subject: pair.getPublicKey(), issuerAccount: appAccount, tags, expiresAt: now + 3_600 }, signerOf(signer), clock);
    const credsPath = `${root}/${fileName}`;
    fs.writeFileSync(credsPath, formatUserCreds(jwt, pair.getSeed()), { flag: "wx", mode: 0o600 });
    fs.chmodSync(credsPath, 0o600);
    return { machine, publicKey: pair.getPublicKey(), credsPath };
  };
  const machineAUser = user(machineA, "machine-a", agentSigning, "agent", "agent/legacy-flow-control/machine-a", "machine-a.creds");
  const machineBUser = user(machineB, "machine-b", agentSigning, "agent", "agent/legacy-flow-control/machine-b", "machine-b.creds");
  const maintainerUser = user(maintainer, "fabric-ops", bootstrapSigning, "maintainer", "maintainer/legacy-flow-control", "maintainer.creds");
  const bootstrapUser = user(bootstrap, "fabric-ops", bootstrapSigning, "maintainer", "bootstrap/legacy-flow-control", "bootstrap.creds");
  return {
    operatorJwt,
    systemAccount,
    systemAccountJwt,
    appAccount,
    appAccountJwt,
    users: { machineA: machineAUser, machineB: machineBUser, maintainer: maintainerUser, bootstrap: bootstrapUser },
  };
};

const watchStore = (store: Pick<NatsKvStore, "watch">) => {
  const controller = new AbortController();
  const iterator = store.watch<Event>(`${MACHINE}.p.*`, controller.signal)[Symbol.asyncIterator]();
  return { controller, iterator };
};

describe.skipIf(!binary)("real NATS KV flow-control rollout", () => {
  let harness: PathBHarness;
  const connections: NatsConnectionHandle[] = [];

  beforeAll(async () => {
    harness = await createPathBHarness({ binary: binary!, tls: {} });
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const js = jetstream(bootstrap);
    await openNatsKvForMachine(js, "presence", MACHINE);
  }, 30_000);

  afterEach(async () => { await Promise.all(connections.splice(0).map((connection) => connection.close())); });
  afterAll(async () => { await harness?.stop(); });

  it("delivers past the ordered consumer flow-control window with the current grants", async () => {
    const connection = await openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), machine: MACHINE });
    connections.push(connection);
    const store = await openNatsKvForMachine(connection.js, "presence", MACHINE, false, connection.onStatus);
    const { controller, iterator } = watchStore(store);
    try {
      await expect(bounded(iterator.next(), "KV flow-control ready")).resolves.toEqual({ value: { type: "ready" }, done: false });
      const padding = "x".repeat(2_048);
      const writes = 96;
      for (let index = 0; index < writes; index += 1) {
        await store.put(`${MACHINE}.p.flow-${index}`, "flow-control-test", { n: index, padding }, 0);
      }
      const events: Event[] = [];
      while (events.length < writes) {
        const result = await bounded(iterator.next(), "KV flow-control delivery");
        expect(result.done).toBe(false);
        events.push(result.value!);
      }
      expect(events.filter((event) => event.type === "put")).toHaveLength(writes);
    } finally {
      controller.abort();
      if (iterator.return) await bounded(iterator.return(), "KV flow-control cleanup");
    }
  }, 30_000);

  it("accepts the installed d231bd15 scope once and warns once before re-issuing credentials", async () => {
    const legacy = await createPathBHarness({ binary: binary!, tls: {}, identityFactory: legacyIdentityFactory });
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const first = await openNatsConnection({ ...legacy.runtimeOptions(legacy.users.machineA), machine: MACHINE });
      await first.close();
      const second = await openNatsConnection({ ...legacy.runtimeOptions(legacy.users.machineA), machine: MACHINE });
      await second.close();
      expect(warning).toHaveBeenCalledTimes(1);
      expect(warning).toHaveBeenCalledWith(WARNING);
    } finally {
      warning.mockRestore();
      await legacy.stop();
    }
  }, 30_000);
});

describe("KV flow-control permission defense", () => {
  it("ends a watch with a typed denial instead of waiting forever after a denied FC reply", async () => {
    let listener: ((status: Status) => void) | undefined;
    let stopped = false;
    let wakeStatus: (() => void) | undefined;
    let resolveClosed!: (error?: Error) => void;
    const closed = new Promise<void | Error>((resolve) => { resolveClosed = resolve; });
    const messages = {
      status: () => (async function* () {
        await new Promise<void>((resolve) => { wakeStatus = resolve; });
      })(),
      closed: () => closed,
      stop: () => { stopped = true; wakeStatus?.(); resolveClosed(); },
      close: async () => { if (!stopped) { stopped = true; wakeStatus?.(); resolveClosed(); } },
    } as unknown as ConsumerMessages;
    const consumer = {
      info: async () => ({ name: "flow-control-consumer", num_pending: 0 }),
      consume: async () => messages,
      delete: async () => true,
      isPullConsumer: () => false,
      isPushConsumer: () => true,
    };
    const js = { consumers: { getPushConsumer: async () => consumer } } as unknown as JetStreamClient;
    const bucket = {} as KV;
    const store = new NatsKvStore(bucket, { bucket: BUCKET, ttlMs: 15_000, markerTtlMs: 300_000 }, js, (callback) => {
      listener = callback;
      return () => { listener = undefined; };
    });
    const controller = new AbortController();
    const iterator = store.watch<Event>(`${MACHINE}.p.*`, controller.signal)[Symbol.asyncIterator]();
    try {
      await expect(iterator.next()).resolves.toEqual({ value: { type: "ready" }, done: false });
      listener?.({ type: "error", error: new PermissionViolationError(
        'Permissions Violation for Publish to "$JS.FC.KV_fabric_presence.flow-control"',
        "publish",
        "$JS.FC.KV_fabric_presence.flow-control",
      ) });
      await expect(bounded(iterator.next(), "denied KV flow-control watch")).rejects.toMatchObject({
        code: "FABRIC_TRANSPORT_DENIED",
        message: WARNING,
      });
    } finally {
      controller.abort();
      await iterator.return?.();
    }
  });
});
