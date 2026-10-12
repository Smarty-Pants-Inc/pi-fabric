import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createAccount, createOperator, createUser, fromSeed } from "@nats-io/nkeys";
import { afterEach, describe, expect, it } from "vitest";
import {
  decodeNatsJwt, encodeNatsJwt, formatNatsCreds, initLocalNats, mintLocalNatsUser, mintNatsUser, natsCredsIdentity, natsCredsJwt,
  natsOperatorJwt, natsOrgAccountJwt, natsRoleTemplate, natsServerConfig, revokeLocalNatsUsers, type NatsAccountKeys,
} from "../src/mesh/nats-auth.js";
import { runNatsCli } from "../src/cli/nats.js";

// smarty-dev#7504 NATS auth, phase 1 (design docs/designs/nats-auth-7504.md): the encoder, the mint, the role templates,
// the server config and the self-hosted store, without a server. The server-side checks are tests/nats-auth-server.test.ts.
const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });
const tempDir = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-auth-")); dirs.push(dir); return dir; };
const HEX = "0123456789abcdef0123456789abcdef";
const seedOf = (pair: ReturnType<typeof createAccount>): string => new TextDecoder().decode(pair.getSeed());

const account = (): NatsAccountKeys => ({ name: "smarty-pants", identity: createAccount(),
  scopes: { agent: createAccount(), principal: createAccount(), maintainer: createAccount() } });

describe("NATS JWT encoding (D4: own encoder, nsc as the oracle)", () => {
  it("signs and verifies; a changed payload or signer fails; the jti is Go's (sha512/256 of the standard claims)", () => {
    const signer = createAccount();
    const jwt = encodeNatsJwt({ iat: 1_700_000_000, iss: signer.getPublicKey(), name: "n", sub: createUser().getPublicKey(),
      exp: 1_700_000_600, nats: { type: "user", version: 2 } }, signer);
    const claims = decodeNatsJwt(jwt);
    expect(Object.keys(claims)).toEqual(["exp", "jti", "iat", "iss", "name", "sub", "nats"]);
    expect(claims.jti).toMatch(/^[A-Z2-7]{52}$/);
    const [head, body, signature] = jwt.split(".");
    const forged = Buffer.from(JSON.stringify({ ...claims, name: "other" })).toString("base64url");
    expect(() => decodeNatsJwt(`${head}.${forged}.${signature}`)).toThrow(/signature/);
    expect(() => decodeNatsJwt(`${head}.${body}`)).toThrow(/not a JWT/);
  });

  it("mints an agent user: no permissions of its own, its role and mesh tags, issuer_account, 7-day exp, iat 60 s back", () => {
    const keys = account();
    const now = Date.UTC(2026, 9, 12);
    const minted = mintNatsUser({ role: "agent", account: keys.identity.getPublicKey(), scopeSeed: seedOf(keys.scopes.agent),
      name: "agent/qs32729/fs-nats2/0001", mesh: [HEX], tags: { host: "qs32729", lane: "fs-nats2" }, now });
    const claims = decodeNatsJwt(natsCredsJwt(minted.creds)!);
    expect(claims.iss).toBe(keys.scopes.agent.getPublicKey());
    expect(claims.sub).toBe(minted.publicKey);
    expect(claims.iat).toBe(now / 1000 - 60);
    expect(claims.exp).toBe(now / 1000 + 7 * 86_400);
    expect(claims.nats).toEqual({ issuer_account: keys.identity.getPublicKey(), pub: {}, sub: {}, tags: ["role:agent", `mesh:${HEX}`, "host:qs32729", "lane:fs-nats2"],
      type: "user", version: 2 });
    expect(minted.creds).toMatch(/-----BEGIN USER NKEY SEED-----\nSU[A-Z2-7]+\n/);
    expect(fromSeed(new TextEncoder().encode(/\n(SU[A-Z2-7]+)\n/.exec(minted.creds)![1]!)).getPublicKey()).toBe(minted.publicKey);
  });

  it("refuses an agent without a mesh stream, a mesh on another role, bad hex, bad tags and a non-account signer", () => {
    const keys = account();
    const base = { account: keys.identity.getPublicKey(), scopeSeed: seedOf(keys.scopes.agent), name: "n" };
    expect(() => mintNatsUser({ ...base, role: "agent" })).toThrow(/needs at least one --mesh/);
    expect(() => mintNatsUser({ ...base, role: "principal", mesh: [HEX] })).toThrow(/only an agent/);
    expect(() => mintNatsUser({ ...base, role: "agent", mesh: ["ABC"] })).toThrow(/bad mesh stream hex/);
    expect(() => mintNatsUser({ ...base, role: "agent", mesh: [HEX], tags: { Host: "x" } })).toThrow(/bad tag/);
    expect(() => mintNatsUser({ ...base, role: "agent", mesh: [HEX], tags: { mesh: HEX } })).toThrow(/bad tag/);
    expect(() => mintNatsUser({ ...base, role: "agent", mesh: [HEX], scopeSeed: new TextDecoder().decode(createOperator().getSeed()) }))
      .toThrow(/account key/);
  });

  it("the role templates: an agent reaches only its tagged streams and never administers one", () => {
    const agent = natsRoleTemplate("agent");
    expect(agent.pub.allow).toContain("FABRIC_MESH_{{tag(mesh)}}.state");
    expect(agent.pub.allow).toContain("$JS.API.CONSUMER.CREATE.FABRIC_MESH_{{tag(mesh)}}.>");
    expect(agent.pub.allow.join(" ")).not.toMatch(/PURGE|STREAM\.DELETE|STREAM\.CREATE|STREAM\.UPDATE|\*\.state|STREAM\.NAMES/);
    expect(agent.sub.allow).toEqual(["_INBOX.{{subject()}}.>"]);
    expect(natsRoleTemplate("principal").pub.allow.join(" ")).not.toMatch(/\.state|PURGE|STREAM\.(CREATE|UPDATE|DELETE)/);
    expect(natsRoleTemplate("maintainer").pub.allow).toEqual(expect.arrayContaining(["$JS.API.STREAM.PURGE.*", "$JS.API.STREAM.CREATE.*"]));
  });

  it("Fabric's cutoff refuses creds within 1 h of exp, and creds without a user JWT", () => {
    const keys = account();
    const minted = mintNatsUser({ role: "agent", account: keys.identity.getPublicKey(), scopeSeed: seedOf(keys.scopes.agent), name: "n",
      mesh: [HEX], ttlSeconds: 2 * 3_600 });
    expect(natsCredsIdentity(minted.creds).userKey).toBe(minted.publicKey);
    expect(() => natsCredsIdentity(minted.creds, { now: Date.now() + 61 * 60_000 })).toThrow(/FABRIC_MESH_STATE_NATS_CREDS_EXPIRED/);
    expect(() => natsCredsIdentity("nothing")).toThrow(/FABRIC_MESH_STATE_NATS_NO_CREDS/);
  });
});

describe("the server config (operator mode, full resolver, route mTLS)", () => {
  it("one node and a cluster node have the same shape; routes must be tls:// and are checked against known URLs", () => {
    const base = { serverName: "ryzen2", operatorJwt: "OPJWT", systemAccount: "ASYS", resolverDir: "/r", preload: { ASYS: "SYSJWT" }, jetstreamDir: "/j" };
    const single = natsServerConfig(base);
    expect(single).toContain("resolver: {\n  type: full");
    expect(single).toContain("allow_delete: false");
    expect(single).toContain("sync_interval: always");
    expect(single).toContain('listen: "127.0.0.1:4222"');
    expect(single).not.toContain("cluster");
    const node = natsServerConfig({ ...base, listen: "0.0.0.0:4222", tls: { certFile: "/c", keyFile: "/k" },
      cluster: { name: "fabric", listen: "0.0.0.0:6222", routes: ["tls://intel1.tail:6222", "tls://qs32729.tail:6222"],
        tls: { certFile: "/rc", keyFile: "/rk", caFile: "/ca" } } });
    expect(node).toContain("verify_cert_and_check_known_urls: true");
    expect(node).not.toMatch(/password|authorization/);
    expect(node.startsWith(single.split("listen:")[0]!)).toBe(true);
    expect(() => natsServerConfig({ ...base, cluster: { name: "f", listen: "x", routes: ["nats://a:6222"], tls: { certFile: "", keyFile: "", caFile: "" } } }))
      .toThrow(/tls:\/\//);
  });
});

describe("the self-hosted store (init --local, S1)", () => {
  it("writes the keys 0600 in a 0700 dir, refuses a second init and a fleet host, and mints from the store", () => {
    const dir = path.join(tempDir(), "nats");
    const made = initLocalNats({ dir, org: "home", env: {} });
    expect((fs.statSync(dir).mode & 0o777).toString(8)).toBe("700");
    for (const name of fs.readdirSync(path.join(dir, "keys"))) expect((fs.statSync(path.join(dir, "keys", name)).mode & 0o777).toString(8)).toBe("600");
    expect((fs.statSync(path.join(dir, "sys-user.creds")).mode & 0o777).toString(8)).toBe("600");
    expect(fs.readFileSync(made.configFile, "utf8")).toContain(made.systemAccount);
    expect(() => initLocalNats({ dir, env: {} })).toThrow(/already holds/);
    expect(() => initLocalNats({ dir: path.join(tempDir(), "x"), env: { PI_FABRIC_NATS_CUSTODY: "fleet" } })).toThrow(/custody `fleet`/);
    expect(() => mintLocalNatsUser({ dir, role: "agent", name: "a", mesh: [HEX], env: { PI_FABRIC_NATS_CUSTODY: "fleet" } })).toThrow(/fleet/);
    const minted = mintLocalNatsUser({ dir, role: "agent", name: "a", mesh: [HEX], env: {} });
    expect(decodeNatsJwt(minted.jwt).nats).toMatchObject({ issuer_account: made.account });
    const revoked = decodeNatsJwt(revokeLocalNatsUsers([minted.publicKey], { dir, env: {} }));
    expect((revoked.nats.revocations as Record<string, number>)[minted.publicKey]).toBeGreaterThan(0);
  });

  it("the CLI never writes creds to a terminal", async () => {
    const dir = path.join(tempDir(), "nats");
    initLocalNats({ dir, env: {} });
    let out = "", err = "";
    const io = { stdout: (text: string) => { out += text; }, stderr: (text: string) => { err += text; }, env: {} };
    expect(await runNatsCli(["mint", "--local", "--dir", dir, "--role", "agent", "--name", "a", "--mesh", HEX], { ...io, stdoutIsTTY: true })).toBe(2);
    expect(err).toContain("never a terminal");
    expect(out).toBe("");
    expect(await runNatsCli(["mint", "--local", "--dir", dir, "--role", "agent", "--name", "a", "--mesh", HEX], { ...io, stdoutIsTTY: false })).toBe(0);
    expect(natsCredsJwt(out)).toBeDefined();
  });
});

// G-7 / D4: nsc decodes our tokens to the same claim shapes it writes itself. NSC_BIN names a pinned nsc binary.
const nsc = process.env.NSC_BIN;
describe.skipIf(!nsc)(`nsc parity (G-7)${nsc ? "" : " (skipped: set NSC_BIN to a pinned nsc binary)"}`, () => {
  const describeJwt = (jwt: string): Record<string, unknown> => {
    const file = path.join(tempDir(), "t.jwt");
    fs.writeFileSync(file, jwt);
    const run = spawnSync(nsc!, ["describe", "jwt", "-f", file, "--json"], { encoding: "utf8", env: { ...process.env, NSC_HOME: tempDir(), NKEYS_PATH: tempDir() } });
    expect(run.status, run.stderr).toBe(0);
    return JSON.parse(run.stdout) as Record<string, unknown>;
  };
  it("operator, org account (scoped keys, revocations) and user tokens decode in nsc to exactly our claims", () => {
    const operator = createOperator(), signing = createOperator(), keys = account();
    const opJwt = natsOperatorJwt(operator, { name: "op", signingKeys: [signing.getPublicKey()], systemAccount: createAccount().getPublicKey() });
    const minted = mintNatsUser({ role: "agent", account: keys.identity.getPublicKey(), scopeSeed: seedOf(keys.scopes.agent), name: "u", mesh: [HEX] });
    const accJwt = natsOrgAccountJwt(signing, keys, { revocations: { [minted.publicKey]: 1_800_000_000 } });
    for (const jwt of [opJwt, accJwt, minted.jwt]) expect(describeJwt(jwt)).toEqual(decodeNatsJwt(jwt));
  });
  it("our claim shapes match nsc's own reference tokens field for field (.local/nsc-ref, when present)", () => {
    const ref = path.resolve(".local/nsc-ref/user.json");
    if (!fs.existsSync(ref)) return;
    const nscUser = JSON.parse(fs.readFileSync(ref, "utf8")) as { nats: Record<string, unknown> };
    const keys = account();
    const ours = decodeNatsJwt(mintNatsUser({ role: "agent", account: keys.identity.getPublicKey(), scopeSeed: seedOf(keys.scopes.agent), name: "u", mesh: [HEX] }).jwt);
    expect(Object.keys(ours.nats).sort()).toEqual(Object.keys(nscUser.nats).sort());
  });
});

export { formatNatsCreds };
