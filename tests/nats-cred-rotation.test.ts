// Keep reason: a real expiring NATS JWT crosses the credential-file and reconnect trust boundary;
// a mocked connection cannot prove server expiry, atomic replacement, or USER.INFO identity.
import fs from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { nkeys } from "@nats-io/transport-node";
import { describe, expect, it, vi } from "vitest";
import { createNatsAccount, createNatsOperator, mintNatsUser } from "../src/mesh/nats-auth/mint.js";
import { encodeUserJwt, formatUserCreds } from "../src/mesh/nats-auth/jwt.js";
import { PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, PATH_B_TEST_MESH, type PathBHarness, createPathBHarness, resolveNatsBinary, type PathBIdentity, type PathBUser } from "./helpers/nats-cluster.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { transportDurableName, transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import { TransportUnavailableError, type FabricTransport, type TransportDelivery, type TransportEnvelope } from "../src/transport/types.js";

const trace = vi.hoisted(() => ({
  opens: [] as Array<{ name: string; startedAt: number; settledAt?: number; user?: string; errorName?: string; code?: string; retryable?: boolean }>,
  statuses: [] as Array<{ name: string; type: string; at: number; user?: string }>,
  listeners: new Set<() => void>(),
  notify() { for (const listener of [...this.listeners]) listener(); },
}));

// Observational wrapper only: every call, NATS protocol exchange, USER.INFO check and returned handle are real.
// It records the transport-owned handle's status events and authenticated USER.INFO subject for assertions.
vi.mock("../src/transport/nats/connection.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/transport/nats/connection.js")>();
  return {
    ...actual,
    openNatsConnection: async (...args: Parameters<typeof actual.openNatsConnection>) => {
      const options = args[0];
      const attempt: { name: string; startedAt: number; settledAt?: number; user?: string; errorName?: string; code?: string; retryable?: boolean } = {
        name: options.name ?? "unnamed", startedAt: Date.now(),
      };
      trace.opens.push(attempt);
      trace.notify();
      try {
        const connection = await actual.openNatsConnection(...args);
        const info = connection.userInfo && typeof connection.userInfo === "object"
          ? (connection.userInfo as { data?: { user?: unknown } }).data : undefined;
        if (typeof info?.user === "string") attempt.user = info.user;
        attempt.settledAt = Date.now();
        connection.onStatus((status) => {
          trace.statuses.push({ name: attempt.name, type: status.type, at: Date.now(), ...(attempt.user === undefined ? {} : { user: attempt.user }) });
          trace.notify();
        });
        trace.notify();
        return connection;
      } catch (error) {
        attempt.settledAt = Date.now();
        if (error instanceof Error) {
          attempt.errorName = error.name;
          const candidate = error as Error & { code?: unknown; retryable?: unknown };
          if (typeof candidate.code === "string") attempt.code = candidate.code;
          attempt.retryable = candidate.retryable === true;
        }
        trace.notify();
        throw error;
      }
    },
  };
});

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const EXPIRY_SECONDS = 30;
const CONNECTION_TIMEOUT_MS = 1_500;
const STREAM_POLICY = { maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000, replicas: 1 as const };
const decoder = new TextDecoder();

function waitForTrace<T>(select: () => T | undefined, label: string, timeoutMs = 20_000): Promise<T> {
  const present = select();
  if (present !== undefined) return Promise.resolve(present);
  return new Promise<T>((resolve, reject) => {
    const check = (): void => {
      const value = select();
      if (value === undefined) return;
      clearTimeout(timer);
      trace.listeners.delete(check);
      resolve(value);
    };
    const timer = setTimeout(() => {
      trace.listeners.delete(check);
      reject(new Error(`timed out waiting for ${label}`));
    }, timeoutMs);
    trace.listeners.add(check);
    check();
  });
}

function artifact(output: { readonly artifacts: readonly { readonly name: string; readonly bytes: Uint8Array }[] }, name: string): Uint8Array {
  const found = output.artifacts.find((item) => item.name === name);
  if (!found) throw new Error(`missing test mint artifact: ${name}`);
  return found.bytes;
}

function artifactText(output: { readonly artifacts: readonly { readonly name: string; readonly bytes: Uint8Array }[] }, name: string): string {
  return decoder.decode(artifact(output, name)).trim();
}

function clearSeeds(output: { readonly artifacts: readonly { readonly name: string; readonly bytes: Uint8Array }[] } | undefined): void {
  for (const item of output?.artifacts ?? []) if (item.name.endsWith(".nk")) item.bytes.fill(0);
}

function writeCredentials(file: string, bytes: Uint8Array): void {
  fs.writeFileSync(file, bytes, { flag: "wx", mode: 0o400 });
  fs.chmodSync(file, 0o400);
}

interface RotationMaterials {
  readonly identityFactory: (root: string) => PathBIdentity;
  readonly originalExpiryMs: () => number;
  readonly positiveCredentialPath: () => string;
  readonly staleCredentialPath: () => string;
  readonly rotatedUser: () => PathBUser;
  readonly rotatedCredentials: () => Uint8Array;
  readonly clearRotatedCredentials: () => void;
}

/** Reuse the Path B real-server harness while minting two same-scope machine-a JWTs with different expiries. */
function rotationMaterials(): RotationMaterials {
  let expiryMs = 0;
  let positivePath = "";
  let stalePath = "";
  let rotated: PathBUser | undefined;
  let rotatedBytes: Uint8Array | undefined;

  const identityFactory = (root: string): PathBIdentity => {
    const clock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };
    const operator = createNatsOperator({ operatorName: "rotation-test" }, clock);
    let account: ReturnType<typeof createNatsAccount> | undefined;
    let agentSigner: ReturnType<typeof nkeys.fromSeed> | undefined;
    try {
      const operatorJwt = artifactText(operator, "operator.jwt");
      account = createNatsAccount({
        name: "rotation-test-app",
        operatorJwt,
        operatorSigningSeed: artifactText(operator, "operator-signing.nk"),
        jetstream: { memStorage: 0, diskStorage: 4_294_967_296, streams: 64, consumers: 256 },
        custodyStream: PATH_B_PERMISSION_POLICY.custodyStream,
        livenessStream: PATH_B_PERMISSION_POLICY.livenessStream,
        pathBMachines: PATH_B_MACHINES,
      }, clock);
      agentSigner = nkeys.fromSeed(artifact(account, "signing-agent.nk"));
      const appAccount = account.metadata.account.publicKey;
      const appAccountJwt = artifactText(account, "account.jwt");
      const signer = { publicKey: agentSigner.getPublicKey(), sign: (bytes: Uint8Array) => agentSigner!.sign(bytes) };
      const agent = (machine: "machine-a" | "machine-b", name: string, expiresAt: number): { readonly user: PathBUser; readonly credentials: Uint8Array } => {
        const user = nkeys.createUser();
        let seed: Uint8Array | undefined;
        try {
          const publicKey = user.getPublicKey();
          const jwt = encodeUserJwt({ name, subject: publicKey, issuerAccount: appAccount,
            tags: ["role:agent", `machine:${machine}`, `mesh:${PATH_B_TEST_MESH}`], expiresAt }, signer, clock);
          seed = Uint8Array.from(user.getSeed());
          return { user: { machine, publicKey, credsPath: "" }, credentials: formatUserCreds(jwt, seed) };
        } finally {
          seed?.fill(0);
          user.clear();
        }
      };

      const expiresAt = clock.nowSeconds() + EXPIRY_SECONDS;
      expiryMs = expiresAt * 1_000;
      const original = agent("machine-a", "agent/rotation/machine-a-original", expiresAt);
      const replacement = agent("machine-a", "agent/rotation/machine-a-replacement", clock.nowSeconds() + 3_600);
      const senderMint = mintNatsUser({ role: "agent", operatorJwt, accountPublicKey: appAccount, accountJwt: appAccountJwt,
        signingSeed: artifactText(account, "signing-agent.nk"), custodyStream: PATH_B_PERMISSION_POLICY.custodyStream,
        livenessStream: PATH_B_PERMISSION_POLICY.livenessStream, host: "machine-b", lane: "rotation-test",
        session: "sender", release: "test", meshes: [PATH_B_TEST_MESH], pathBMachines: PATH_B_MACHINES }, clock);
      const bootstrapMint = mintNatsUser({ role: "maintainer", operatorJwt, accountPublicKey: appAccount, accountJwt: appAccountJwt,
        signingSeed: artifactText(account, "signing-maintainer.nk"), custodyStream: PATH_B_PERMISSION_POLICY.custodyStream,
        livenessStream: PATH_B_PERMISSION_POLICY.livenessStream, job: "migration", runId: "rotation-test" }, clock);
      positivePath = path.join(root, "machine-a-positive.creds");
      stalePath = path.join(root, "machine-a-stale.creds");
      writeCredentials(positivePath, original.credentials);
      writeCredentials(stalePath, original.credentials);
      rotated = { ...replacement.user, credsPath: path.join(root, "machine-a-rotation.tmp") };
      rotatedBytes = Uint8Array.from(replacement.credentials);
      const senderPath = path.join(root, "machine-b-sender.creds");
      const bootstrapPath = path.join(root, "bootstrap.creds");
      writeCredentials(senderPath, artifact(senderMint, "user.creds"));
      writeCredentials(bootstrapPath, artifact(bootstrapMint, "user.creds"));
      for (const output of [senderMint, bootstrapMint]) artifact(output, "user.creds").fill(0);
      return {
        operatorJwt,
        systemAccount: operator.metadata.systemAccount.publicKey,
        systemAccountJwt: artifactText(operator, "sys-account.jwt"),
        appAccount,
        appAccountJwt,
        users: {
          machineA: { ...original.user, credsPath: positivePath },
          machineB: { machine: "machine-b", publicKey: senderMint.metadata.publicKey, credsPath: senderPath },
          bootstrap: { machine: "fabric-ops", publicKey: bootstrapMint.metadata.publicKey, credsPath: bootstrapPath },
          maintainer: { machine: "fabric-ops", publicKey: bootstrapMint.metadata.publicKey, credsPath: bootstrapPath },
        },
      };
    } finally {
      agentSigner?.clear();
      clearSeeds(account);
      clearSeeds(operator);
    }
  };

  return {
    identityFactory,
    originalExpiryMs: () => expiryMs,
    positiveCredentialPath: () => positivePath,
    staleCredentialPath: () => stalePath,
    rotatedUser: () => { if (!rotated) throw new Error("replacement identity was not minted"); return rotated; },
    rotatedCredentials: () => { if (!rotatedBytes) throw new Error("replacement credentials were not minted"); return rotatedBytes; },
    clearRotatedCredentials: () => { rotatedBytes?.fill(0); },
  };
}

async function bounded<T>(promise: Promise<T>, label: string, timeoutMs = 8_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out: ${label}`)), timeoutMs);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}

type InboxIterator = AsyncIterator<TransportDelivery<TransportEnvelope>>;
async function next(iterator: InboxIterator, label: string): Promise<TransportDelivery<TransportEnvelope>> {
  const result = await bounded(iterator.next(), label);
  expect(result.done).toBe(false);
  if (result.done || !result.value) throw new Error(`${label}: inbox ended before delivery`);
  return result.value;
}

function followUp(sender: FabricTransport, receiver: FabricTransport, id: string): TransportEnvelope {
  const now = Date.now();
  const targetId = "credential-rotation-target";
  return {
    id,
    kind: "command",
    to: receiver.self,
    targetId,
    operation: "followUp",
    from: { id: sender.self.hostId, name: "rotation-test-sender", kind: "main" },
    body: { version: 1, commandId: id, targetId, operation: "followUp", replyTo: sender.self.hostId,
      ownerIncarnation: "rotation-test-incarnation", message: id, requestedAt: now, deadlineAt: now + 60_000 },
  };
}

function userFromTrace(name: string, user: string): (() => (typeof trace.opens)[number] | undefined) {
  return () => trace.opens.find((attempt) => attempt.name === name && attempt.user === user);
}

describe.skipIf(!binary || process.platform === "win32")("real NATS credentials-file rotation", () => {
  it("reopens an expiring carrier from the atomically replaced file and keeps retrying unchanged expired credentials", async () => {
    trace.opens.length = 0;
    trace.statuses.length = 0;
    const materials = rotationMaterials();
    let fixture: PathBHarness | undefined;
    const transports: FabricTransport[] = [];
    const positiveAbort = new AbortController();
    const staleAbort = new AbortController();
    let positiveInbox: InboxIterator | undefined;
    let staleInbox: InboxIterator | undefined;
    let staleSettled: "done" | "delivered" | "error" | undefined;
    try {
      fixture = await createPathBHarness({ binary: binary!, identityFactory: materials.identityFactory, tls: {} });
      const bootstrap = await fixture.connect(fixture.users.bootstrap);
      const manager = await jetstreamManager(bootstrap);
      const js = jetstream(bootstrap);
      for (const machine of PATH_B_MACHINES) {
        await openNatsKvForMachine(js, "leases", machine, true);
        await openNatsKvForMachine(js, "presence", machine, true);
        await ensureTransportStream(manager, { ...STREAM_POLICY, name: transportInboxStream(machine),
          subjects: [transportInboxStreamSubject(machine)] });
      }

      const open = async (user: PathBUser, hostId: string, credentials?: string): Promise<FabricTransport> => {
        const transport = await openNatsTransport({ ...fixture!.runtimeOptions(user), hostId, name: hostId,
          ...(credentials === undefined ? {} : { credentials }), requestTimeoutMs: CONNECTION_TIMEOUT_MS });
        transports.push(transport);
        return transport;
      };
      const positive = await open(fixture.users.machineA, "credential-rotation-positive");
      const stale = await open(fixture.users.machineA, "credential-rotation-stale", materials.staleCredentialPath());
      const sender = await open(fixture.users.machineB, "credential-rotation-sender");
      const selfBefore = positive.self;
      const positiveName = "credential-rotation-positive";
      const staleName = "credential-rotation-stale";
      const original = fixture.users.machineA.publicKey;
      const replacement = materials.rotatedUser();
      expect(fs.statSync(materials.positiveCredentialPath()).mode & 0o777).toBe(0o400);
      expect(fs.statSync(materials.staleCredentialPath()).mode & 0o777).toBe(0o400);
      expect((userFromTrace(positiveName, original)())).toBeDefined();
      expect((userFromTrace(staleName, original)())).toBeDefined();

      positiveInbox = positive.inbox(positiveAbort.signal)[Symbol.asyncIterator]();
      staleInbox = stale.inbox(staleAbort.signal)[Symbol.asyncIterator]();
      const staleWaiting = staleInbox.next().then((result) => {
        staleSettled = result.done ? "done" : "delivered";
      }, () => { staleSettled = "error"; });
      void staleWaiting;

      const initial = followUp(sender, positive, "followup-before-rotation");
      await sender.send(initial);
      const first = await next(positiveInbox, "pre-expiry followUp");
      expect(first).toMatchObject({ sourceMachine: "machine-b", payload: { id: initial.id, operation: "followUp", body: initial.body } });
      await first.ack();

      const stage = replacement.credsPath;
      writeCredentials(stage, materials.rotatedCredentials());
      expect(fs.statSync(stage).mode & 0o777).toBe(0o400);
      expect(Date.now()).toBeLessThan(materials.originalExpiryMs());
      fs.renameSync(stage, materials.positiveCredentialPath());
      expect(fs.statSync(materials.positiveCredentialPath()).mode & 0o777).toBe(0o400);

      const positiveDisconnect = await waitForTrace(() => trace.statuses.find((status) =>
        status.name === positiveName && status.type === "disconnect" && status.user === original), "positive carrier server disconnect", 45_000);
      const staleDisconnect = await waitForTrace(() => trace.statuses.find((status) =>
        status.name === staleName && status.type === "disconnect" && status.user === original), "unrotated carrier server disconnect", 5_000);
      expect(positiveDisconnect.at).toBeGreaterThanOrEqual(materials.originalExpiryMs() - 1_000);
      expect(positiveDisconnect.at).toBeLessThanOrEqual(materials.originalExpiryMs() + 10_000);
      expect(staleDisconnect.at).toBeGreaterThanOrEqual(materials.originalExpiryMs() - 1_000);
      expect(staleDisconnect.at).toBeLessThanOrEqual(materials.originalExpiryMs() + 10_000);

      const reauthenticated = await waitForTrace(userFromTrace(positiveName, replacement.publicKey), "replacement USER.INFO identity", 10_000);
      const reconnectDelayMs = reauthenticated.settledAt! - positiveDisconnect.at;
      expect(positive.self).toBe(selfBefore);
      expect(reconnectDelayMs).toBeGreaterThanOrEqual(0);
      expect(reconnectDelayMs).toBeLessThan(8_000);

      const after = followUp(sender, positive, "followup-after-rotation");
      const afterSent = await sender.send(after);
      const second = await next(positiveInbox, "post-rotation followUp");
      expect(second).toMatchObject({ sourceMachine: "machine-b", payload: { id: after.id, operation: "followUp", body: after.body } });
      await second.ack();
      expect(afterSent.sequence).toBe(2);
      expect(await positive.drainState()).toEqual({ pending: 0, ackPending: 0 });
      const consumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(positive.self.hostId));
      expect(consumer.delivered.consumer_seq).toBe(2);
      expect(consumer.ack_floor.consumer_seq).toBe(2);
      expect(consumer.ack_floor.stream_seq).toBe(afterSent.sequence);

      const failedAttempts = await waitForTrace(() => {
        const failures = trace.opens.filter((attempt) => attempt.name === staleName && attempt.code === "FABRIC_NATS_AUTHORIZATION");
        return failures.length >= 7 ? failures : undefined;
      }, "seven bounded authentication retries with unchanged credentials", 25_000).catch((error: unknown) => {
        const attempts = trace.opens.filter((attempt) => attempt.name === staleName).map(({ startedAt, settledAt, errorName, code, retryable }) =>
          ({ elapsedMs: startedAt - materials.originalExpiryMs(), settledMs: settledAt === undefined ? undefined : settledAt - startedAt, errorName, code, retryable }));
        throw new Error(`${error instanceof Error ? error.message : "retry observation failed"}; stale attempts=${JSON.stringify(attempts)}; inbox=${String(staleSettled)}`);
      });
      expect(failedAttempts.slice(0, 7).every((attempt) => attempt.errorName === "NatsAuthorizationError" && attempt.retryable)).toBe(true);
      const retryIntervals = failedAttempts.slice(1, 7).map((attempt, index) => attempt.startedAt - failedAttempts[index]!.startedAt);
      expect(retryIntervals.every((interval) => interval >= 100 && interval < 7_000)).toBe(true);
      expect(retryIntervals.some((interval) => interval >= 5_000)).toBe(true);
      await delay(300);
      expect(staleSettled).toBeUndefined();
      await expect(stale.send(followUp(sender, stale, "stale-transport-remains-live"))).rejects.toBeInstanceOf(TransportUnavailableError);

      // Timings and identity are intentionally asserted from the same actual connection/status receipts.
      expect(reauthenticated.user).toBe(replacement.publicKey);
      expect(reauthenticated.settledAt).toBeGreaterThan(positiveDisconnect.at);
      console.info("NATS credential rotation proof", JSON.stringify({
        jwtExpiryAt: new Date(materials.originalExpiryMs()).toISOString(),
        positiveDisconnectAfterExpiryMs: positiveDisconnect.at - materials.originalExpiryMs(),
        positiveReconnectDelayMs: reconnectDelayMs,
        reauthenticatedAsUserB: reauthenticated.user === replacement.publicKey,
        staleAuthRetryIntervalsMs: retryIntervals,
      }));
    } finally {
      positiveAbort.abort();
      staleAbort.abort();
      const closing: Promise<unknown>[] = [];
      if (positiveInbox?.return) closing.push(Promise.resolve(positiveInbox.return()));
      if (staleInbox?.return) closing.push(Promise.resolve(staleInbox.return()));
      try {
        await Promise.allSettled(closing);
        await Promise.allSettled(transports.map((transport) => transport.close()));
      } finally {
        try { await fixture?.stop(); } finally { materials.clearRotatedCredentials(); }
      }
    }
  }, 90_000);
});
