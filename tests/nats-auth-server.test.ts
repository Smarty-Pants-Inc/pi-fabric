import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { jetstreamManager, StorageType } from "@nats-io/jetstream";
import { createAccount, fromSeed } from "@nats-io/nkeys";
import { connect, credsAuthenticator, type NatsConnection } from "@nats-io/transport-node";
import { createRequire } from "node:module";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  freeLoopbackPort, initLocalNats, localSystemCreds, mintLocalNatsUser, mintNatsUser, natsOrgAccountJwt, natsServerConfig, pushNatsAccountJwt,
  revokeLocalNatsUsers, startLocalNatsServer, type LocalNatsInit, type LocalNatsServer, type NatsAccountKeys,
} from "../src/mesh/nats-auth.js";
import { closeNatsStateBackend, MAX_MESSAGE_BYTES, natsStreamName, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#7504 NATS auth, phase 1 on loopback (design docs/designs/nats-auth-7504.md, section 6): a single node in
// operator mode with the full resolver, made by `initLocalNats` (the self-hosted path) and started as the embedded child.
// T2 role matrix, T4 revocation, T5 expiry and skew, T6 org isolation, T8 the mesh state backend over minted creds.
// It needs a nats-server >= 2.14.7 binary: NATS_BIN (the CI job sets it) or NATS_SERVER_BIN. Without it, a visible skip;
// with NATS_SERVER_REQUIRED=1, a failure. Every server here binds 127.0.0.1 and stops in afterAll.
const binary = process.env.NATS_BIN ?? process.env.NATS_SERVER_BIN;
const available = binary !== undefined && fs.existsSync(binary);
if (process.env.NATS_SERVER_REQUIRED === "1" && !available) throw new Error("NATS_SERVER_REQUIRED=1 but NATS_BIN is not a nats-server binary");
const identity: MeshIdentity = { id: "auth-test", name: "auth-test", kind: "agent" };
const hexOf = (root: string): string => natsStreamName(root).slice("FABRIC_MESH_".length);

describe.skipIf(!available)(`NATS auth on a loopback node${available ? "" : " (skipped: set NATS_BIN to a nats-server binary)"}`, () => {
  let base: string;
  let local: LocalNatsInit;
  let server: LocalNatsServer;
  const open: NatsConnection[] = [];
  const connectAs = async (creds: string, inbox = true): Promise<NatsConnection> => {
    const userKey = /\n(U[A-Z2-7]{55})|"sub":"(U[A-Z2-7]{55})"/.exec(Buffer.from(/-----BEGIN NATS USER JWT-----\s*(\S+)/.exec(creds)![1]!.split(".")[1]!, "base64url").toString("utf8"))!;
    const nc = await connect({ servers: [server.url], authenticator: credsAuthenticator(new TextEncoder().encode(creds)),
      ...(inbox ? { inboxPrefix: `_INBOX.${userKey[1] ?? userKey[2]}` } : {}), reconnect: false });
    open.push(nc);
    return nc;
  };
  const mint = (role: "agent" | "principal" | "maintainer", mesh: string[] = [], ttlSeconds?: number, now?: number) =>
    mintLocalNatsUser({ dir: local.dir, role, name: `${role}/test`, ...(mesh.length ? { mesh } : {}), ...(ttlSeconds ? { ttlSeconds } : {}),
      ...(now === undefined ? {} : { now }), env: {} });
  /** "ok", "denied" (Permissions Violation) or the other error's message. */
  const outcome = async (run: () => Promise<unknown>): Promise<string> => {
    try { await run(); return "ok"; } catch (error) {
      const text = `${(error as Error).name} ${(error as Error).message} ${String((error as { cause?: unknown }).cause ?? "")}`;
      return /permission/i.test(text) ? "denied" : text;
    }
  };
  const roots: string[] = [];
  const makeStream = async (root: string): Promise<string> => {
    const name = natsStreamName(root);
    const maintainer = await connectAs(mint("maintainer").creds);
    const jsm = await jetstreamManager(maintainer);
    await jsm.streams.add({ name, subjects: [`${name}.state`, `${name}.snapshot`], storage: StorageType.File, num_replicas: 1, max_msg_size: MAX_MESSAGE_BYTES });
    await maintainer.close();
    return name;
  };
  const newRoot = (): string => { const root = fs.mkdtempSync(path.join(base, "root-")); roots.push(root); return root; };

  beforeAll(async () => {
    base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-auth-server-"));
    const port = await freeLoopbackPort(), httpPort = await freeLoopbackPort();
    local = initLocalNats({ dir: path.join(base, "nats"), org: "smarty-pants", port, httpPort, env: {} });
    expect(fs.readFileSync(local.configFile, "utf8")).toContain(`listen: "127.0.0.1:${port}"`);
    server = await startLocalNatsServer({ binary: binary!, configFile: local.configFile, url: `nats://127.0.0.1:${port}`, httpUrl: `http://127.0.0.1:${httpPort}` });
  }, 30_000);

  afterAll(async () => {
    for (const nc of open) await nc.close().catch(() => undefined);
    for (const root of roots) closeNatsStateBackend(root);
    await server?.stop();
    if (base) fs.rmSync(base, { recursive: true, force: true });
  });

  it("T3: a client without creds, or with an unsigned key, is refused", async () => {
    expect(await outcome(() => connect({ servers: [server.url], reconnect: false }))).toMatch(/Authorization|auth/i);
    const stranger = mintNatsUser({ role: "agent", account: createAccount().getPublicKey(), scopeSeed: new TextDecoder().decode(createAccount().getSeed()),
      name: "x", mesh: ["0".repeat(32)] });
    expect(await outcome(() => connectAs(stranger.creds))).toMatch(/Authorization|auth/i);
  });

  it("T2: the role matrix: an agent reaches only its bound streams and administers none; a principal reads; a maintainer administers", async () => {
    const mine = newRoot(), mine2 = newRoot(), other = newRoot();
    const [s1, s2, s3] = [await makeStream(mine), await makeStream(mine2), await makeStream(other)];
    const agent = await connectAs(mint("agent", [hexOf(mine), hexOf(mine2)]).creds);
    const jsm = await jetstreamManager(agent);
    const js = jsm.jetstream();
    expect(await outcome(() => jsm.streams.info(s1))).toBe("ok");
    expect(await outcome(() => jsm.streams.info(s2))).toBe("ok"); // two mesh tags both expand
    expect(await outcome(() => js.publish(`${s1}.state`, new Uint8Array([1])))).toBe("ok");
    expect(await outcome(() => jsm.streams.getMessage(s1, { seq: 1 }))).toBe("ok");
    expect(await outcome(async () => { const consumer = await js.consumers.get(s1); await consumer.next({ expires: 1_000 }); })).toBe("ok");
    expect(await outcome(() => jsm.streams.info(s3))).toBe("denied"); // another root
    expect(await outcome(() => js.publish(`${s3}.state`, new Uint8Array([1]), { timeout: 2_000 }))).not.toBe("ok");
    expect(await outcome(() => jsm.streams.purge(s1))).toBe("denied");
    expect(await outcome(() => jsm.streams.delete(s1))).toBe("denied");
    expect(await outcome(() => jsm.streams.add({ name: "FABRIC_MESH_" + "f".repeat(32), subjects: ["x.state"] }))).toBe("denied");
    expect(await outcome(() => jsm.streams.names().next())).toBe("denied");
    expect(await outcome(() => agent.request("$SYS.REQ.USER.INFO", "", { timeout: 2_000 }))).toBe("ok");
    // The default inbox (not its own) cannot receive the reply: only `_INBOX.<its user key>` is allowed.
    const shared = await connectAs(mint("agent", [hexOf(mine)]).creds, false);
    expect(await outcome(async () => (await jetstreamManager(shared)).streams.info(s1))).not.toBe("ok");

    const principal = await jetstreamManager(await connectAs(mint("principal").creds));
    expect(await outcome(() => principal.streams.info(s3))).toBe("ok");
    expect(await outcome(() => principal.jetstream().publish(`${s3}.state`, new Uint8Array([1]), { timeout: 2_000 }))).not.toBe("ok");
    expect(await outcome(() => principal.streams.purge(s3))).toBe("denied");

    const maintainer = await jetstreamManager(await connectAs(mint("maintainer").creds));
    expect(await outcome(() => maintainer.streams.purge(s3))).toBe("ok");
  }, 60_000);

  it("T8: the mesh state backend runs over minted agent creds; a root the user is not bound to refuses", async () => {
    const root = newRoot(), foreign = newRoot();
    await makeStream(root);
    await makeStream(foreign);
    const creds = mint("agent", [hexOf(root)]).creds;
    await openNatsStateBackend({ root, servers: [server.url], monitoringUrl: server.httpUrl, creds });
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "nats" });
    try {
      const first = await store.put({ key: "auth/a", value: { n: 1 }, identity });
      await expect(store.put({ key: "auth/a", value: { n: 2 }, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap/);
      await store.writeBatch({ identity, ops: [{ kind: "put", key: "auth/b", value: 2 }, { kind: "delete", key: "auth/a", ifVersion: first.version }] });
      expect(store.listAll("auth/").map(entry => [entry.key, entry.value])).toEqual([["auth/b", 2]]);
    } finally { store.closeState(); closeNatsStateBackend(root); }
    // A second open replays the same state over the same creds.
    await openNatsStateBackend({ root, servers: [server.url], monitoringUrl: server.httpUrl, creds });
    const again = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "nats" });
    try { expect(again.get("auth/b")?.value).toBe(2); } finally { again.closeState(); closeNatsStateBackend(root); }
    await expect(openNatsStateBackend({ root: foreign, servers: [server.url], monitoringUrl: server.httpUrl, creds })).rejects.toThrow(/permission/i);
  }, 60_000);

  it("T4: a revocation pushed through the resolver closes the user's live connection and refuses a reconnect", async () => {
    const root = newRoot();
    const stream = await makeStream(root);
    const minted = mint("agent", [hexOf(root)]);
    const nc = await connectAs(minted.creds);
    await (await jetstreamManager(nc)).streams.info(stream);
    const closed = nc.closed();
    const pushed = await pushNatsAccountJwt([server.url], localSystemCreds(local.dir), revokeLocalNatsUsers([minted.publicKey], { dir: local.dir, env: {} }));
    expect(pushed.ok, pushed.message).toBe(true);
    const started = Date.now();
    await Promise.race([closed, new Promise((_, reject) => setTimeout(() => reject(new Error("still connected after 5 s")), 5_000))]);
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(await outcome(() => connectAs(minted.creds))).toMatch(/Authorization|auth|revoked/i);
    // Another user of the same role is untouched.
    expect(await outcome(async () => (await jetstreamManager(await connectAs(mint("agent", [hexOf(root)]).creds))).streams.info(stream))).toBe("ok");
  }, 30_000);

  it("T5: expiry and skew: a live connection is closed at exp, a reconnect after exp refused; +90 s client skew still connects", async () => {
    const root = newRoot();
    await makeStream(root);
    const short = mint("agent", [hexOf(root)], 3);
    const nc = await connectAs(short.creds);
    const closed = nc.closed();
    await Promise.race([closed, new Promise((_, reject) => setTimeout(() => reject(new Error("still connected 8 s after a 3 s exp")), 8_000))]);
    // exp has whole-second resolution: past it by more than a second, a reconnect is refused.
    await new Promise(resolve => setTimeout(resolve, Math.max(0, short.exp * 1000 + 1_500 - Date.now())));
    expect(await outcome(() => connectAs(short.creds))).toMatch(/expired|Authorization|auth/i);
    // Fabric's own cutoff (exp - 1 h) refuses such creds before it connects.
    await expect(openNatsStateBackend({ root, servers: [server.url], monitoringUrl: server.httpUrl, creds: mint("agent", [hexOf(root)], 1_800).creds }))
      .rejects.toThrow(/FABRIC_MESH_STATE_NATS_CREDS_EXPIRED/);
    // The issuer's clock 90 s ahead: iat is 30 s in the server's future, exp far off: the server accepts it.
    expect(await outcome(() => connectAs(mint("agent", [hexOf(root)], undefined, Date.now() + 90_000).creds))).toBe("ok");
  }, 30_000);

  it("T6: org isolation: another org's user sees none of this org's streams", async () => {
    const root = newRoot();
    const stream = await makeStream(root);
    const operatorSigning = fromSeed(new TextEncoder().encode(fs.readFileSync(path.join(local.dir, "keys", "operator-signing.nk"), "utf8").trim()));
    const other: NatsAccountKeys = { name: "playful-test", identity: createAccount(),
      scopes: { agent: createAccount(), principal: createAccount(), maintainer: createAccount() } };
    const pushed = await pushNatsAccountJwt([server.url], localSystemCreds(local.dir), natsOrgAccountJwt(operatorSigning, other));
    expect(pushed.ok, pushed.message).toBe(true);
    const outsider = mintNatsUser({ role: "principal", account: other.identity.getPublicKey(),
      scopeSeed: new TextDecoder().decode(other.scopes.principal.getSeed()), name: "principal/test/other" });
    const jsm = await jetstreamManager(await connectAs(outsider.creds));
    const names: string[] = [];
    for await (const name of jsm.streams.names()) names.push(name);
    expect(names).not.toContain(stream);
    expect(await outcome(() => jsm.streams.info(stream))).toMatch(/not found/i);
  }, 30_000);
});

// The embedded node runs in its own process group: stop() also ends a server started through a wrapper (the case that
// left servers behind in a review run).
describe.skipIf(!available)(`the embedded node stops its whole process group${available ? "" : " (skipped: needs NATS_BIN)"}`, () => {
  it("a wrapper that runs nats-server as its child leaves nothing behind after stop()", async () => {
    const work = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-wrapper-"));
    try {
      const wrapper = path.join(work, "nats-wrapper.sh");
      fs.writeFileSync(wrapper, `#!/bin/sh\n${JSON.stringify(binary)} "$@"\n`, { mode: 0o700 });
      const port = await freeLoopbackPort(), httpPort = await freeLoopbackPort();
      const made = initLocalNats({ dir: path.join(work, "nats"), port, httpPort, env: {} });
      const node = await startLocalNatsServer({ binary: wrapper, configFile: made.configFile, url: `nats://127.0.0.1:${port}`, httpUrl: `http://127.0.0.1:${httpPort}` });
      await node.stop();
      await expect(fetch(new URL("/varz", `http://127.0.0.1:${httpPort}`), { signal: AbortSignal.timeout(1_000) })).rejects.toThrow();
    } finally { fs.rmSync(work, { recursive: true, force: true }); }
  }, 30_000);
});

// The self-hosting path through the real bin (dist): init --local, serve --local (the embedded node, 127.0.0.1), mint to a
// pipe, and a nats-mode store over those creds. Stopped (SIGTERM) before the test ends.
const built = fs.existsSync(path.resolve("dist/cli/index.js"));
describe.skipIf(!available || !built)(`pi-fabric nats, real bin, single machine${available && built ? "" : " (skipped: needs NATS_BIN and dist/ built)"}`, () => {
  it("init --local, serve --local and mint give a working nats-mode store, with nothing listening off loopback", async () => {
    const { spawn, spawnSync } = await import("node:child_process");
    const work = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-auth-bin-"));
    const dir = path.join(work, "nats");
    const bin = path.resolve("dist/cli/index.js");
    const port = await freeLoopbackPort(), httpPort = await freeLoopbackPort();
    const env = { ...process.env, PI_FABRIC_NATS_CUSTODY: "" };
    const init = spawnSync(process.execPath, [bin, "nats", "init", "--local", "--dir", dir, "--port", String(port), "--http-port", String(httpPort)], { encoding: "utf8", env });
    expect(init.status, init.stderr).toBe(0);
    expect(init.stdout).not.toMatch(/S[OAU][A-Z2-7]{50,}/); // no seed in the output
    const serve = spawn(process.execPath, [bin, "nats", "serve", "--local", "--dir", dir, "--nats-server", binary!], { stdio: "ignore", env });
    const root = fs.mkdtempSync(path.join(work, "root-"));
    try {
      const httpUrl = `http://127.0.0.1:${httpPort}`;
      for (let tries = 0; ; tries++) {
        try { if ((await fetch(new URL("/healthz", httpUrl))).ok) break; } catch { /* starting */ }
        if (tries > 100) throw new Error("the embedded node did not start");
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      const varz = await (await fetch(new URL("/varz", httpUrl))).json() as { host?: string; http_host?: string };
      expect([varz.host, varz.http_host]).toEqual(["127.0.0.1", "127.0.0.1"]);
      const sys = mintLocalNatsUser({ dir, role: "maintainer", name: "maintainer/test/provision", env: {} });
      const nc = await connect({ servers: [`nats://127.0.0.1:${port}`], authenticator: credsAuthenticator(new TextEncoder().encode(sys.creds)), inboxPrefix: `_INBOX.${sys.publicKey}` });
      const name = natsStreamName(root);
      await (await jetstreamManager(nc)).streams.add({ name, subjects: [`${name}.state`, `${name}.snapshot`], storage: StorageType.File, num_replicas: 1, max_msg_size: MAX_MESSAGE_BYTES });
      await nc.close();
      const mint = spawnSync(process.execPath, [bin, "nats", "mint", "--local", "--dir", dir, "--role", "agent", "--name", "agent/local/test/1", "--mesh", hexOf(root)], { encoding: "utf8", env });
      expect(mint.status, mint.stderr).toBe(0);
      await openNatsStateBackend({ root, servers: [`nats://127.0.0.1:${port}`], monitoringUrl: httpUrl, creds: mint.stdout });
      const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "nats" });
      try {
        await store.put({ key: "self/hosted", value: true, identity });
        expect(store.get("self/hosted")?.value).toBe(true);
      } finally { store.closeState(); closeNatsStateBackend(root); }
    } finally {
      const exited = new Promise(resolve => serve.once("exit", resolve));
      serve.kill("SIGTERM");
      await exited;
      fs.rmSync(work, { recursive: true, force: true });
    }
  }, 60_000);
});

// T1, the route part on loopback (design Q5): two nodes route over mutual TLS with known-URL checking; a third node with a
// certificate from the SAME CA but a name that is not in the known route URLs cannot join. Every listener is 127.0.0.1.
const openssl = (() => {
  try { return (createRequire(import.meta.url)("node:child_process") as typeof import("node:child_process")).spawnSync("openssl", ["version"]).status === 0; }
  catch { return false; }
})();
describe.skipIf(!available || !openssl)(`T1 route mTLS on loopback${available && openssl ? "" : " (skipped: needs NATS_BIN and openssl)"}`, () => {
  it("two known nodes route; a node with a valid CA certificate but an unknown name is refused as a route", async () => {
    const { spawnSync } = await import("node:child_process");
    const work = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-routes-"));
    const servers: LocalNatsServer[] = [];
    try {
      const ssl = (...args: string[]) => { const run = spawnSync("openssl", args, { cwd: work, encoding: "utf8" }); if (run.status !== 0) throw new Error(run.stderr); };
      ssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=test-ca");
      const cert = (name: string, dns: string) => {
        fs.writeFileSync(path.join(work, `${name}.ext`), `subjectAltName=DNS:${dns}\nextendedKeyUsage=serverAuth,clientAuth\n`);
        ssl("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", `${name}.key`, "-out", `${name}.csr`, "-subj", `/CN=${dns}`);
        ssl("x509", "-req", "-in", `${name}.csr`, "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", `${name}.pem`, "-days", "1", "-extfile", `${name}.ext`);
        return { certFile: path.join(work, `${name}.pem`), keyFile: path.join(work, `${name}.key`), caFile: path.join(work, "ca.pem") };
      };
      const ports = await Promise.all(Array.from({ length: 9 }, () => freeLoopbackPort()));
      const local = initLocalNats({ dir: path.join(work, "keys"), org: "route-test", env: {} });
      const operatorJwt = fs.readFileSync(path.join(local.dir, "operator.jwt"), "utf8").trim();
      const config = fs.readFileSync(local.configFile, "utf8");
      const preload = Object.fromEntries([...config.matchAll(/^ {2}(A[A-Z2-7]{55}): "([^"]+)"$/gm)].map(match => [match[1]!, match[2]!]));
      const start = async (name: string, client: number, http: number, route: number, routes: string[], tls: ReturnType<typeof cert>) => {
        const dir = path.join(work, name);
        fs.mkdirSync(path.join(dir, "jwt"), { recursive: true });
        const file = path.join(dir, "nats.conf");
        fs.writeFileSync(file, natsServerConfig({ serverName: name, listen: `127.0.0.1:${client}`, http: `127.0.0.1:${http}`, operatorJwt,
          systemAccount: local.systemAccount, resolverDir: path.join(dir, "jwt"), preload, jetstreamDir: path.join(dir, "js"),
          cluster: { name: "fabric-test", listen: `127.0.0.1:${route}`, routes, tls } }));
        const node = await startLocalNatsServer({ binary: binary!, configFile: file, url: `nats://127.0.0.1:${client}`, httpUrl: `http://127.0.0.1:${http}`, ready: "server" });
        servers.push(node);
        return node;
      };
      // Route pooling opens several routes per peer: count the distinct peer names.
      const peers = async (node: LocalNatsServer): Promise<string[]> => [...new Set((((await (await fetch(new URL("/routez", node.httpUrl))).json()) as
        { routes?: { remote_name?: string }[] }).routes ?? []).map(route => route.remote_name ?? "?"))].sort();
      const [c1, h1, r1, c2, h2, r2, c3, h3, r3] = ports as [number, number, number, number, number, number, number, number, number];
      const n1 = await start("n1", c1, h1, r1, [`tls://localhost:${r2}`], cert("n1", "localhost"));
      const n2 = await start("n2", c2, h2, r2, [`tls://localhost:${r1}`], cert("n2", "localhost"));
      await vi.waitFor(async () => { expect(await peers(n1)).toEqual(["n2"]); }, { timeout: 15_000, interval: 200 });
      // The rogue: same CA, a name no known route URL has. It dials n1 and n2; both must refuse it.
      const rogue = await start("rogue", c3, h3, r3, [`tls://localhost:${r1}`, `tls://localhost:${r2}`], cert("rogue", "rogue.invalid"));
      await new Promise(resolve => setTimeout(resolve, 3_000));
      expect(await peers(rogue)).toEqual([]);
      expect(await peers(n1)).toEqual(["n2"]);
      expect(await peers(n2)).toEqual(["n1"]);
    } finally {
      for (const node of servers) await node.stop();
      fs.rmSync(work, { recursive: true, force: true });
    }
  }, 60_000);
});

// T1 (cluster formation, route mTLS, a 4th host refused as a route) runs on Ryzen 2, Intel 1 and qs32729 after the
// tailnet ports; it needs those hosts, so it is skipped unless NATS_AUTH_CLUSTER names the 3 client URLs.
const cluster = process.env.NATS_AUTH_CLUSTER?.split(",").filter(Boolean) ?? [];
describe.skipIf(cluster.length !== 3)(`T1 nats-auth-cluster-form${cluster.length === 3 ? "" : " (skipped: needs NATS_AUTH_CLUSTER=3 URLs and the deployed nodes)"}`, () => {
  it("3 nodes route in operator mode; every node reports the cluster and the org account", async () => {
    for (const url of cluster) {
      const monitor = url.replace(/^tls:\/\//, "https://").replace(/:4222$/, ":8222");
      const varz = await (await fetch(new URL("/varz", monitor))).json() as { cluster?: { urls?: string[] }; system_account?: string };
      expect(varz.cluster?.urls?.length).toBe(2);
      expect(varz.system_account).toMatch(/^A/);
    }
  });
});
