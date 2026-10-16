import * as realFs from "node:fs";
import path from "node:path";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, PATH_B_POLICY_ENV, resolveNatsBinary, type PathBHarness, type PathBUser } from "./helpers/nats-cluster.js";

// Focused ownership simulation: the credential reader must use same-fd fstat; we only perturb the uid of
// one call, never chown on the host. All other fs behavior is the real module.
const fstatOverride = vi.hoisted(() => ({ foreignUid: false }));
vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  const fstatSync = ((fd: number, options?: unknown) => {
    const stats = (actual.fstatSync as (fd: number, options?: unknown) => realFs.Stats)(fd, options);
    if (!fstatOverride.foreignUid) return stats;
    return Object.assign(Object.create(Object.getPrototypeOf(stats)), stats, { uid: stats.uid + 1 });
  }) as typeof actual.fstatSync;
  return { ...actual, fstatSync, default: { ...actual, fstatSync } };
});

type ConnectionOptions = Parameters<typeof openNatsConnection>[0];
const posix = process.platform !== "win32";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
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
      const a = await openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineA), hostId: "runtime-auth-a" });
      runtimes.push(a);
      const b = await openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineB), hostId: "runtime-auth-b" });
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
    const connection = await openNatsConnection(harness.runtimeOptions(harness.users.machineA));
    expect(connection.userInfo).toBeDefined();
    await connection.close();
  });

  it("accepts the reserved report-only operator only with explicit role:'operator'", async () => {
    const connection = await openNatsConnection(harness.operatorOptions());
    await connection.close();
    // Agent is the default role: the same operator credential without role:'operator' is refused.
    await expect(openNatsConnection(harness.runtimeOptions(harness.users.maintainer))).rejects.toThrow();
  });

  it("refuses a valid report-only operator with an extra private-message subscription", async () => {
    const fixture = await createPathBHarness({ binary: binary!, additionalOperatorSubscribe: ["fabric.in.>"] });
    try {
      const authenticated = await fixture.connect(fixture.users.maintainer);
      await authenticated.close();
      await expect(openNatsConnection(fixture.operatorOptions())).rejects.toThrow("machine tag mismatch");
    } finally { await fixture.stop(); }
  });

  it("refuses bootstrap authority as the report-only operator despite a matching signed machine tag", async () => {
    await expect(openNatsConnection(harness.operatorOptions(harness.users.bootstrap))).rejects.toThrow();
  });

  describe("runtime refuses non-agent credentials even with an unknown verifyMachine:false property", () => {
    const nonAgents = (): Array<[string, PathBUser, string]> => [
      ["bootstrap", harness.users.bootstrap, harness.users.bootstrap.machine],
      ["operator", harness.users.maintainer, harness.users.maintainer.machine],
      ["principal", harness.users.principal!, MACHINE_A],
      ["system", harness.users.system!, MACHINE_A],
    ];
    it("refuses via openNatsConnection (agent default) and openNatsTransport", async () => {
      for (const [, user, machine] of nonAgents()) {
        // Prove these are valid authenticated broker identities, not malformed credential negatives.
        const authenticated = await harness.connect(user);
        await authenticated.close();
        const base = { ...harness.runtimeOptions(user), machine };
        await expect(openNatsConnection(base)).rejects.toThrow();
        await expect(openNatsConnection({ ...base, verifyMachine: false } as unknown as ConnectionOptions)).rejects.toThrow();
        await expect(openNatsTransport({ ...base, hostId: "runtime-non-agent", provision: false })).rejects.toThrow();
        await expect(openNatsTransport({ ...base, hostId: "runtime-non-agent", provision: false, verifyMachine: false } as unknown as Parameters<typeof openNatsTransport>[0])).rejects.toThrow();
      }
    });
  });

  it("fails closed when any expected policy field is missing", async () => {
    for (const field of ["custodyStream", "livenessStream", "pathBMachines"] as const) {
      const options: Record<string, unknown> = { ...harness.runtimeOptions(harness.users.machineA) };
      delete options[field];
      await expect(openNatsConnection(options as unknown as ConnectionOptions)).rejects.toThrow();
    }
    await expect(openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), pathBMachines: [MACHINE_A] })).rejects.toThrow();
    await expect(openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), custodyStream: "OTHER_CUSTODY" })).rejects.toThrow();
  });

  it.each([">", "fabric.>", "fabric.in.*.*.*.*.machine-b", "$JS.API.STREAM.NAMES", "fabric.in.*.*.*.*.machine-a.>"])("refuses a server-expanded extra publish grant: %s", async (grant) => {
    const fixture = await createPathBHarness({ binary: binary!, additionalAgentPublish: [grant] });
    try {
      await expect(openNatsConnection(fixture.runtimeOptions(fixture.users.machineA))).rejects.toThrow();
    } finally {
      await fixture.stop();
    }
  });

  it.each([">", "fabric.>", "_INBOX.>"])("refuses an extra subscribe grant: %s", async (grant) => {
    const fixture = await createPathBHarness({ binary: binary!, additionalAgentSubscribe: [grant] });
    try {
      await expect(openNatsConnection(fixture.runtimeOptions(fixture.users.machineA))).rejects.toThrow();
    } finally {
      await fixture.stop();
    }
  });

  it.each(["$JS.API.DIRECT.GET.>", "$JS.API.STREAM.DELETE.>", "$JS.API.STREAM.CREATE.FABRIC_CUSTODY"])("refuses a removed publish deny: %s", async (subject) => {
    const fixture = await createPathBHarness({ binary: binary!, removeAgentDeny: [subject] });
    try {
      await expect(openNatsConnection(fixture.runtimeOptions(fixture.users.machineA))).rejects.toThrow();
    } finally {
      await fixture.stop();
    }
  });

  it("matches the complete merged template expanded over multiple signed mesh tags", async () => {
    const fixture = await createPathBHarness({ binary: binary!, additionalAgentMeshes: ["abcdef0123456789abcdef0123456789"] });
    try {
      const connection = await openNatsConnection(fixture.runtimeOptions(fixture.users.machineA));
      expect(connection.userInfo).toBeDefined();
      await connection.close();
    } finally { await fixture.stop(); }
  });

  it("refuses credentials bound to another configured machine", async () => {
    await expect(openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), machine: MACHINE_B })).rejects.toThrow();
  });

  describe.skipIf(!posix)("credential file custody (POSIX; win32 ACLs are documented as fail-closed by source)", () => {
    const copyWithMode = (name: string, mode: number): string => {
      const target = path.join(harness.root, name);
      realFs.writeFileSync(target, realFs.readFileSync(harness.users.machineA.credsPath), { flag: "wx", mode: 0o600 });
      realFs.chmodSync(target, mode);
      return target;
    };
    it.each([0o644, 0o640, 0o604, 0o660])("refuses group/world-accessible mode %s", async (mode) => {
      const credentials = copyWithMode(`mode-${mode.toString(8)}.creds`, mode);
      await expect(openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), credentials })).rejects.toThrow();
    });
    it("accepts a private 0600 regular file", async () => {
      const credentials = copyWithMode("mode-600.creds", 0o600);
      const connection = await openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), credentials });
      await connection.close();
    });
    it("refuses a non-regular credential path", async () => {
      const dir = path.join(harness.root, "creds-dir");
      realFs.mkdirSync(dir, { mode: 0o700 });
      await expect(openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), credentials: dir })).rejects.toThrow();
    });
    it("refuses a credential file owned by another uid (simulated same-fd fstat)", async () => {
      fstatOverride.foreignUid = true;
      try {
        await expect(openNatsConnection(harness.runtimeOptions(harness.users.machineA))).rejects.toThrow();
      } finally { fstatOverride.foreignUid = false; }
    });
  });

  describe("plaintext policy", () => {
    it("refuses plaintext by default without the explicit test-only loopback opt-in", async () => {
      const { allowInsecureLoopback: _optIn, ...strict } = harness.runtimeOptions(harness.users.machineA) as ConnectionOptions & { allowInsecureLoopback?: boolean };
      await expect(openNatsConnection(strict)).rejects.toThrow();
    });
    it("never reads the loopback escape from the environment", async () => {
      const { allowInsecureLoopback: _optIn, ...strict } = harness.runtimeOptions(harness.users.machineA) as ConnectionOptions & { allowInsecureLoopback?: boolean };
      for (const name of ["PI_FABRIC_NATS_ALLOW_INSECURE_LOOPBACK", "PI_FABRIC_ALLOW_INSECURE_LOOPBACK", "NATS_ALLOW_INSECURE_LOOPBACK"]) vi.stubEnv(name, "1");
      try { await expect(openNatsConnection(strict)).rejects.toThrow(); } finally { vi.unstubAllEnvs(); }
    });
    it("ignores allowInsecureLoopback in mesh transport config (only a direct option may opt in)", async () => {
      const { allowInsecureLoopback: _optIn, ...strict } = harness.runtimeOptions(harness.users.machineA) as ConnectionOptions & { allowInsecureLoopback?: boolean };
      const nats = { ...strict, allowInsecureLoopback: true };
      // mesh.transport is the selector; mesh.nats is its sibling policy/config object.
      const config = { nats, transport: "nats" };
      await expect(openNatsTransport({ config, hostId: "runtime-config-loopback", mesh: {} } as unknown as Parameters<typeof openNatsTransport>[0])).rejects.toThrow();
      const { custodyStream: _c, livenessStream: _l, pathBMachines: _m, ...noPolicy } = strict;
      const absent = { ...noPolicy, allowInsecureLoopback: true };
      await expect(openNatsTransport({ config: { nats: absent, transport: "nats" }, hostId: "runtime-config-no-policy", mesh: {} } as unknown as Parameters<typeof openNatsTransport>[0])).rejects.toThrow();
    });
    it("refuses direct transport options with absent policy", async () => {
      for (const field of ["custodyStream", "livenessStream", "pathBMachines"] as const) {
        const options: Record<string, unknown> = { ...harness.runtimeOptions(harness.users.machineA), hostId: "runtime-no-policy", provision: false };
        delete options[field];
        await expect(openNatsTransport(options as unknown as Parameters<typeof openNatsTransport>[0])).rejects.toThrow();
      }
    });
    it("refuses a non-numeric-loopback plaintext URL even with the opt-in", async () => {
      const port = new URL(harness.serverUrl).port;
      for (const servers of [`nats://localhost:${port}`, "nats://192.0.2.1:4222", "nats://example.invalid:4222", [harness.serverUrl, "nats://192.0.2.1:4222"]]) {
        await expect(openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), servers, timeoutMs: 1_000 })).rejects.toThrow();
      }
    });
  });
});

describe.skipIf(!binary)("authenticated Path B transport over verified TLS", () => {
  // Trusted certificates differ only in SAN identity. Production TLS is DNS-only: the official
  // transport-node 3.4 numeric-host tlsName gap must fail closed even for a correctly IP-signed certificate.
  let tlsHarness: PathBHarness;
  let wrongIdentity: PathBHarness;
  let ipOnly: PathBHarness;
  let dnsOnly: PathBHarness;
  beforeAll(async () => {
    tlsHarness = await createPathBHarness({ binary: binary!, tls: {} });
    wrongIdentity = await createPathBHarness({ binary: binary!, tls: { dnsNames: ["wrong-identity.invalid"], ipAddresses: ["127.0.0.2"] } });
    ipOnly = await createPathBHarness({ binary: binary!, tls: { dnsNames: [], ipAddresses: ["127.0.0.1"] } });
    dnsOnly = await createPathBHarness({ binary: binary!, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
  }, 90_000);
  afterAll(async () => { await Promise.allSettled([tlsHarness, wrongIdentity, ipOnly, dnsOnly].map((fixture) => fixture?.stop())); });

  const at = (fixture: PathBHarness, host: string): string => `tls://${host}:${new URL(fixture.serverUrl).port}`;
  const tlsOptions = (fixture: PathBHarness, host: string) => ({ ...fixture.runtimeOptions(fixture.users.machineA), servers: at(fixture, host), timeoutMs: 2_000 });

  it("connects with a CA-verified DNS identity and refuses numeric TLS URLs with a remedy", async () => {
    const connection = await openNatsConnection(tlsOptions(tlsHarness, "localhost"));
    expect(connection.userInfo).toBeDefined();
    await connection.close();
    for (const host of ["127.0.0.1", "[::1]"]) {
      await expect(openNatsConnection(tlsOptions(tlsHarness, host))).rejects.toThrow("use the certificate DNS name");
    }
  });

  it("refuses a trusted-CA certificate with the wrong hostname and the wrong IP", async () => {
    for (const host of ["localhost", "127.0.0.1"]) await expect(openNatsConnection(tlsOptions(wrongIdentity, host))).rejects.toThrow();
  });

  it("requires a matching DNS SAN and refuses even a trusted matching IP SAN", async () => {
    await expect(openNatsConnection(tlsOptions(ipOnly, "127.0.0.1"))).rejects.toThrow("use the certificate DNS name");
    await expect(openNatsConnection(tlsOptions(ipOnly, "localhost"))).rejects.toThrow();
    const dns = await openNatsConnection(tlsOptions(dnsOnly, "localhost"));
    await dns.close();
    await expect(openNatsConnection(tlsOptions(dnsOnly, "127.0.0.1"))).rejects.toThrow("use the certificate DNS name");
  });

  it("carries a full runtime command over verified TLS without the loopback opt-in", async () => {
    const runtimes: Array<Awaited<ReturnType<typeof openNatsTransport>>> = [];
    const abort = new AbortController();
    const bootstrap = await tlsHarness.connect(tlsHarness.users.bootstrap);
    try {
      const manager = await jetstreamManager(bootstrap);
      const js = jetstream(bootstrap);
      const policy = { maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000, replicas: 1 as const };
      await ensureTransportStream(manager, { ...policy, name: ADVISORY_CAPTURE_STREAM, subjects: [transportAdvisoryStreamSubject], maxAgeMs: 30 * 86_400_000, maxMessageBytes: 65_536 });
      await openNatsKvForMachine(js, "leases", MACHINE_A, true);
      await openNatsKvForMachine(js, "presence", MACHINE_A, true);
      for (const machine of [MACHINE_A, MACHINE_B]) await ensureTransportStream(manager, { ...policy, name: transportInboxStream(machine), subjects: [transportInboxStreamSubject(machine)] });
      const options = (user: PathBUser) => {
        const value = { ...tlsHarness.runtimeOptions(user), servers: at(tlsHarness, "localhost") } as Record<string, unknown>;
        expect(value.allowInsecureLoopback).toBeUndefined();
        return value as ReturnType<PathBHarness["runtimeOptions"]>;
      };
      const nats = options(tlsHarness.users.machineA);
      for (const field of ["custodyStream", "livenessStream", "pathBMachines"] as const) {
        const missing: Record<string, unknown> = { ...nats };
        delete missing[field];
        await expect(openNatsTransport({ config: { transport: "nats", nats: missing }, hostId: "runtime-tls-config-missing", mesh: {} } as unknown as import("../src/transport/types.js").FabricTransportOptions)).rejects.toThrow();
      }
      const a = await openNatsTransport({ config: { transport: "nats", nats }, hostId: "runtime-tls-a", mesh: {} } as unknown as import("../src/transport/types.js").FabricTransportOptions);
      runtimes.push(a);
      const b = await openNatsTransport({ ...options(tlsHarness.users.machineB), hostId: "runtime-tls-b" });
      runtimes.push(b);
      const bInbox = b.inbox(abort.signal)[Symbol.asyncIterator]();
      await a.send({ id: "tls-command", kind: "command", to: b.self, targetId: "child-tls-b", operation: "followUp", from: { id: a.self.hostId, name: "a", kind: "main" }, body: { text: "verified tls" } });
      const received = (await bInbox.next()).value!;
      expect(received).toMatchObject({ sourceMachine: MACHINE_A, payload: { id: "tls-command", body: { text: "verified tls" } } });
      await received.ack();
      expect(await b.drainState()).toEqual({ pending: 0, ackPending: 0 });
      abort.abort();
      await bInbox.return?.();
      // Counterexample: the same runtime against a trusted-CA but wrong-identity server is refused.
      await expect(openNatsTransport({ ...tlsOptions(wrongIdentity, "localhost"), hostId: "runtime-tls-wrong", provision: false })).rejects.toThrow();
    } finally {
      abort.abort();
      await Promise.all(runtimes.map((runtime) => runtime.close()));
      await bootstrap.close();
    }
  }, 60_000);

  it("refuses an untrusted CA", async () => {
    await expect(openNatsConnection({ ...tlsHarness.runtimeOptions(tlsHarness.users.machineA), servers: at(tlsHarness, "localhost"),
      tls: { caFile: tlsHarness.tls!.untrustedCaFile }, timeoutMs: 2_000 })).rejects.toThrow();
  });

  it("refuses insecure TLS settings passed through the official tls option", async () => {
    const base = { ...tlsHarness.runtimeOptions(tlsHarness.users.machineA), servers: at(tlsHarness, "localhost"), timeoutMs: 2_000 };
    for (const tls of [
      { caFile: tlsHarness.tls!.caFile, rejectUnauthorized: false },
      { caFile: tlsHarness.tls!.caFile, checkServerIdentity: () => undefined },
      { caFile: tlsHarness.tls!.untrustedCaFile, rejectUnauthorized: false },
    ]) {
      await expect(openNatsConnection({ ...base, tls } as unknown as ConnectionOptions)).rejects.toThrow();
    }
  });

  it("requires tls:// or explicit TLS options, and never downgrades when TLS options are supplied", async () => {
    const { tls: _tls, ...base } = tlsOptions(tlsHarness, "localhost") as ConnectionOptions;
    const servers = `nats://localhost:${new URL(tlsHarness.serverUrl).port}`;
    await expect(openNatsConnection({ ...base, servers })).rejects.toThrow("requires tls://");
    const secured = await openNatsConnection({ ...base, servers, tls: { caFile: tlsHarness.tls!.caFile } });
    await secured.close();
    const plaintext = await createPathBHarness({ binary: binary! });
    try {
      await expect(openNatsConnection({ ...plaintext.runtimeOptions(plaintext.users.machineA), allowInsecureLoopback: false,
        servers: `nats://localhost:${new URL(plaintext.serverUrl).port}`, tls: {}, timeoutMs: 1_000 })).rejects.toThrow();
    } finally { await plaintext.stop(); }
  });
});

describe.skipIf(!binary)("authenticated Path B transport admin TLS CLI", () => {
  let tlsHarness: PathBHarness;
  const hostId = "admin-tls-host";
  const machines = `${MACHINE_A},${MACHINE_B}`;

  beforeAll(async () => {
    tlsHarness = await createPathBHarness({ binary: binary!, tls: {} });
  }, 90_000);
  afterAll(async () => { await tlsHarness?.stop(); });

  const dnsServer = (): string => `tls://localhost:${new URL(tlsHarness.serverUrl).port}`;
  const caFile = (): string => tlsHarness.tls!.caFile;
  const adminEnv = (
    user: Pick<PathBUser, "machine" | "credsPath">,
    servers: string,
    extra: Readonly<Record<string, string>> = {},
  ): Record<string, string> => ({
    ...PATH_B_POLICY_ENV,
    PI_FABRIC_NATS_SERVERS: servers,
    PI_FABRIC_NATS_CREDENTIALS: user.credsPath,
    PI_FABRIC_MACHINE: user.machine,
    ...extra,
  });
  const withEnv = async <T>(env: Readonly<Record<string, string>>, action: () => Promise<T>): Promise<T> => {
    const previous = new Map<string, string | undefined>();
    for (const [key, value] of Object.entries(env)) {
      previous.set(key, process.env[key]);
      process.env[key] = value;
    }
    try { return await action(); }
    finally {
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  };
  const runAdmin = (env: Readonly<Record<string, string>>, args: readonly string[]): Promise<number> =>
    withEnv(env, () => runTransportAdmin(args));
  const provisionArgs = (ca: string): string[] => ["provision", "--machines", machines, "--ca-file", ca];
  const provision = (): Promise<number> => runAdmin(
    adminEnv(tlsHarness.users.bootstrap, dnsServer()),
    provisionArgs(caFile()),
  );
  const openMachineConsumer = async (id: string) => openNatsTransport({
    ...tlsHarness.runtimeOptions(tlsHarness.users.machineA),
    servers: dnsServer(),
    hostId: id,
  });

  it("provision succeeds with an absolute CA file and DNS server URL", async () => {
    expect(path.isAbsolute(caFile())).toBe(true);
    await expect(provision()).resolves.toBe(0);
  });

  it("drain succeeds with an existing machine-A consumer over verified TLS", async () => {
    await provision();
    const runtime = await openMachineConsumer(hostId);
    try {
      await expect(runAdmin(
        adminEnv(tlsHarness.users.machineA, dnsServer(), { PI_FABRIC_HOST_ID: hostId }),
        ["drain", "--ca-file", caFile()],
      )).resolves.toBe(0);
    } finally { await runtime.close(); }
  });

  it("orphan succeeds with the operator over verified TLS", async () => {
    await provision();
    const runtime = await openMachineConsumer(hostId);
    try {
      await expect(runAdmin(
        adminEnv(tlsHarness.users.maintainer, dnsServer()),
        ["orphan", "--machine", MACHINE_A, "--host-id", hostId, "--ca-file", caFile()],
      )).resolves.toBe(0);
    } finally { await runtime.close(); }
  });

  it("dlq --once succeeds and finishes a finite pull when there are no advisories", async () => {
    await provision();
    await expect(runAdmin(
      adminEnv(tlsHarness.users.maintainer, dnsServer(), { PI_FABRIC_DLQ_PATH: path.join(tlsHarness.root, "admin-dlq.jsonl") }),
      ["dlq", "--once", "--ca-file", caFile()],
    )).resolves.toBe(0);
  }, 10_000);

  it("refuses every admin command when the CA file flag is absent", async () => {
    const cases: Array<[string, Readonly<Record<string, string>>, string[]]> = [
      ["provision", adminEnv(tlsHarness.users.bootstrap, dnsServer()), ["provision", "--machines", machines]],
      ["drain", adminEnv(tlsHarness.users.machineA, dnsServer(), { PI_FABRIC_HOST_ID: hostId }), ["drain"]],
      ["orphan", adminEnv(tlsHarness.users.maintainer, dnsServer()), ["orphan", "--machine", MACHINE_A, "--host-id", hostId]],
      ["dlq", adminEnv(tlsHarness.users.maintainer, dnsServer()), ["dlq", "--once"]],
    ];
    for (const [, env, args] of cases) await expect(runAdmin(env, args)).rejects.toThrow();
  });

  it("refuses every admin command when the CA file flag is paired with an IP URL", async () => {
    const cases: Array<[string, Readonly<Record<string, string>>, string[]]> = [
      ["provision", adminEnv(tlsHarness.users.bootstrap, tlsHarness.serverUrl), provisionArgs(caFile())],
      ["drain", adminEnv(tlsHarness.users.machineA, tlsHarness.serverUrl, { PI_FABRIC_HOST_ID: hostId }), ["drain", "--ca-file", caFile()]],
      ["orphan", adminEnv(tlsHarness.users.maintainer, tlsHarness.serverUrl), ["orphan", "--machine", MACHINE_A, "--host-id", hostId, "--ca-file", caFile()]],
      ["dlq", adminEnv(tlsHarness.users.maintainer, tlsHarness.serverUrl), ["dlq", "--once", "--ca-file", caFile()]],
    ];
    for (const [, env, args] of cases) await expect(runAdmin(env, args)).rejects.toThrow(/certificate DNS name|IP|numeric/i);
  });

  it("rejects missing, relative, and duplicate CA flags before broker setup for all commands", async () => {
    const cases: Array<[Readonly<Record<string, string>>, readonly string[]]> = [
      [adminEnv(tlsHarness.users.bootstrap, dnsServer()), ["provision", "--machines", machines]],
      [adminEnv(tlsHarness.users.machineA, dnsServer(), { PI_FABRIC_HOST_ID: hostId }), ["drain"]],
      [adminEnv(tlsHarness.users.maintainer, dnsServer()), ["orphan", "--machine", MACHINE_A, "--host-id", hostId]],
      [adminEnv(tlsHarness.users.maintainer, dnsServer()), ["dlq", "--once"]],
    ];
    for (const [env, base] of cases) {
      await expect(runAdmin(env, [...base, "--ca-file"])).rejects.toThrow(/ca-file/i);
      await expect(runAdmin(env, [...base, "--ca-file", "relative-ca.pem"])).rejects.toThrow(/ca-file/i);
      await expect(runAdmin(env, [...base, "--ca-file", caFile(), "--ca-file", caFile()])).rejects.toThrow(/ca-file/i);
    }
  });
});
