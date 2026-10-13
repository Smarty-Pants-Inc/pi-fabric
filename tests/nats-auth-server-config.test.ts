import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { nkeys } from "@nats-io/transport-node";
import { beforeAll, describe, expect, it } from "vitest";

// smarty-dev#7504 phase 1 slice 1: pure NATS server config renderer (design c4d26466 sections 3, 9.1; Q5).
// RED-first: written before src/mesh/nats-auth/server-config.ts exists. Each case loads the module itself after
// asserting the file is present, so a missing implementation fails every case on its own (not a collection error).
// The GREEN run uses these same cases and code paths.
//
// Secret hygiene: every key is generated at runtime by the installed nkeys (via @nats-io/transport-node). The config
// only carries PUBLIC material (public keys, public operator/account JWTs). The test-side mock JWTs below are signed
// in memory with runtime keys; seeds are never printed, and the one seed-shaped counterexample is compared as a boolean.
//
// Decision C (why unit, not E2E): these check the deterministic rendering and refusal contract of a pure function.
// They cannot prove that nats-server 2.14.7 parses the text, enforces operator auth, or forms a route-mTLS cluster.
// That is E1 hosted T8 `nats-auth-single-node` (one loopback node, operator mode, full resolver, creds-only connect)
// plus T2 `nats-auth-role-matrix` on the pinned real NATS_BIN 2.14.7, owner fabric-store; 3/3 route mTLS is T1.
// No nats-server exists on this host and none is started here.

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const SOURCE = join(repoRoot, "src/mesh/nats-auth/server-config.ts");

interface HostPort {
  host: string;
  port: number;
}
interface ServerConfigInput {
  bus: { nodes: number; replicas: number };
  serverName: string;
  listen: HostPort;
  monitor?: HostPort;
  jetstream: { storeDir: string };
  operatorJwt: string;
  systemAccount: string;
  resolver: { dir: string; interval?: string; timeout?: string };
  preload: Record<string, string>;
  cluster?: {
    name: string;
    listen: HostPort;
    routes: string[];
    tls: { certFile: string; keyFile: string; caFile: string };
  };
}
interface ConfigError extends Error {
  field: string;
}
interface ServerConfigModule {
  renderNatsServerConfig(input: ServerConfigInput): string;
  validateNatsServerConfigInput(input: unknown): ServerConfigInput;
  NatsServerConfigError: new (...args: never[]) => ConfigError;
}

async function load(): Promise<ServerConfigModule> {
  // Absence is RED, labelled honestly; never a stub.
  expect(existsSync(SOURCE), `RED (#7504, implementation absent): ${relative(repoRoot, SOURCE)} does not exist yet`).toBe(
    true,
  );
  return (await import(/* @vite-ignore */ SOURCE)) as ServerConfigModule;
}

// ---- test-only public mock JWTs (Go nats-io/jwt v2 layout; the product encoder is jwt.ts, not this file) ----

type KeyPair = ReturnType<typeof nkeys.createOperator>;
const b64url = (bytes: Uint8Array | string) => Buffer.from(bytes).toString("base64url");
const HEADER = { typ: "JWT", alg: "ed25519-nkey" };

function signJwt(signer: KeyPair, payload: Record<string, unknown>, header: Record<string, unknown> = HEADER): string {
  const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(payload))}`;
  return `${signingInput}.${b64url(signer.sign(new TextEncoder().encode(signingInput)))}`;
}

function operatorJwt(op: KeyPair, sys: string, extra: { signingKeys?: string[]; nats?: Record<string, unknown> } = {}) {
  const sub = op.getPublicKey();
  return signJwt(op, {
    iat: 1_760_000_000,
    iss: sub,
    name: "test-operator",
    sub,
    nats: { signing_keys: extra.signingKeys ?? [], system_account: sys, type: "operator", version: 2, ...extra.nats },
  });
}

function accountJwt(signer: KeyPair, account: string, name: string, nats: Record<string, unknown> = {}) {
  return signJwt(signer, {
    iat: 1_760_000_000,
    iss: signer.getPublicKey(),
    name,
    sub: account,
    nats: { limits: {}, type: "account", version: 2, ...nats },
  });
}

let op: KeyPair;
let opSigning: KeyPair;
let sysKey: string;
let orgKey: string;
let OP_JWT: string;
let SYS_JWT: string;
let ORG_JWT: string;

beforeAll(() => {
  op = nkeys.createOperator();
  opSigning = nkeys.createOperator();
  sysKey = nkeys.createAccount().getPublicKey();
  orgKey = nkeys.createAccount().getPublicKey();
  OP_JWT = operatorJwt(op, sysKey, { signingKeys: [opSigning.getPublicKey()] });
  SYS_JWT = accountJwt(op, sysKey, "SYS");
  ORG_JWT = accountJwt(opSigning, orgKey, "smarty-pants");
});

function single(): ServerConfigInput {
  return {
    bus: { nodes: 1, replicas: 1 },
    serverName: "fabric-local",
    listen: { host: "127.0.0.1", port: 4222 },
    monitor: { host: "127.0.0.1", port: 8222 },
    jetstream: { storeDir: "/srv/fabric/nats/js" },
    operatorJwt: OP_JWT,
    systemAccount: sysKey,
    resolver: { dir: "/srv/fabric/nats/jwt" },
    preload: { [sysKey]: SYS_JWT },
  };
}

const ROUTES = [
  "nats-route://n1.example.ts.net:6222",
  "nats-route://n2.example.ts.net:6222",
  "nats-route://n3.example.ts.net:6222",
];

function triple(): ServerConfigInput {
  return {
    bus: { nodes: 3, replicas: 3 },
    serverName: "n2",
    listen: { host: "n2.example.ts.net", port: 4222 },
    monitor: { host: "127.0.0.1", port: 8222 },
    jetstream: { storeDir: "/home/nats/.local/share/smarty/nats/js" },
    operatorJwt: OP_JWT,
    systemAccount: sysKey,
    resolver: { dir: "/home/nats/.local/share/smarty/nats/jwt", interval: "2m", timeout: "1900ms" },
    preload: { [sysKey]: SYS_JWT, [orgKey]: ORG_JWT },
    cluster: {
      name: "smarty-fleet",
      listen: { host: "n2.example.ts.net", port: 6222 },
      routes: [...ROUTES],
      tls: {
        certFile: "/home/nats/.local/share/smarty/nats/tls/n2.crt",
        keyFile: "/home/nats/.local/share/smarty/nats/tls/n2.key",
        caFile: "/home/nats/.local/share/smarty/nats/tls/ca.crt",
      },
    },
  };
}

/** Non-comment, non-blank lines; the renderer's own comments may mention words the config must not set. */
function configLines(text: string): string[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "" && !line.startsWith("#"));
}

function refusal(mod: ServerConfigModule, input: unknown): ConfigError {
  try {
    mod.validateNatsServerConfigInput(input);
  } catch (error) {
    expect(error instanceof mod.NatsServerConfigError).toBe(true);
    // render must refuse identically: it validates first and never emits partial text.
    expect(() => mod.renderNatsServerConfig(input as ServerConfigInput)).toThrow(mod.NatsServerConfigError);
    return error as ConfigError;
  }
  throw new Error("expected NatsServerConfigError, input was accepted");
}

function mutate(base: () => ServerConfigInput, change: (input: ServerConfigInput) => void): ServerConfigInput {
  const input = base();
  change(input);
  return input;
}

describe("renderNatsServerConfig: bus 1/1 single loopback node", () => {
  it("renders the exact operator-mode text: loopback listen, JetStream sync always, full resolver, SYS preload, no cluster", async () => {
    const mod = await load();
    const text = mod.renderNatsServerConfig(single());
    expect(text).toBe(
      [
        "# Generated by pi-fabric (smarty-dev#7504): NATS operator mode, full account resolver.",
        "# bus: nodes=1 replicas=1. Stream replicas are not set here; the adapter checks them (smarty-dev#11373).",
        'server_name: "fabric-local"',
        'listen: "127.0.0.1:4222"',
        'http: "127.0.0.1:8222"',
        "jetstream {",
        '  store_dir: "/srv/fabric/nats/js"',
        "  sync_interval: always",
        "}",
        `operator: "${OP_JWT}"`,
        `system_account: "${sysKey}"`,
        "resolver {",
        "  type: full",
        '  dir: "/srv/fabric/nats/jwt"',
        "  allow_delete: false",
        "}",
        "resolver_preload {",
        `  ${sysKey}: "${SYS_JWT}"`,
        "}",
        "",
      ].join("\n"),
    );
  });

  it("is deterministic and omits monitor when absent; preload keys render in sorted order", async () => {
    const mod = await load();
    const input = mutate(single, (i) => {
      delete i.monitor;
      i.preload = { [orgKey]: ORG_JWT, [sysKey]: SYS_JWT };
    });
    const text = mod.renderNatsServerConfig(input);
    expect(mod.renderNatsServerConfig(input)).toBe(text);
    expect(configLines(text).some((line) => line.startsWith("http"))).toBe(false);
    const preloadKeys = configLines(text)
      .filter((line) => /^A[A-Z2-7]{55}: "/.test(line))
      .map((line) => line.slice(0, 56));
    expect(preloadKeys).toEqual([orgKey, sysKey].sort());
  });

  it("never renders an auth bypass, accounts block, users, passwords, tokens or stream replicas", async () => {
    const mod = await load();
    for (const text of [mod.renderNatsServerConfig(single()), mod.renderNatsServerConfig(triple())]) {
      const lines = configLines(text);
      for (const forbidden of [
        /^no_auth_user\b/,
        /^authorization\b/,
        /^accounts\b/,
        /\busers?\b/,
        /\bpassword\b/,
        /\btoken\b/,
        /replica/i,
        /allow_non_tls/,
        /\binsecure\b/,
        /^include\b/,
      ]) {
        expect(
          lines.filter((line) => forbidden.test(line)),
          String(forbidden),
        ).toEqual([]);
      }
    }
  });

  it("refuses a cluster block, a non-loopback or wildcard listen, and a non-loopback monitor on bus 1/1", async () => {
    const mod = await load();
    const cluster = triple().cluster;
    assert(cluster, "the triple fixture has a cluster");
    expect(refusal(mod, mutate(single, (i) => (i.cluster = cluster))).field).toBe("cluster");
    for (const host of ["0.0.0.0", "", "*", "::", "10.0.0.5", "n1.example.ts.net", "localhost"]) {
      expect(refusal(mod, mutate(single, (i) => (i.listen.host = host))).field, host).toBe("listen.host");
    }
    expect(refusal(mod, mutate(single, (i) => (i.monitor = { host: "0.0.0.0", port: 8222 }))).field).toBe("monitor.host");
  });
});

describe("renderNatsServerConfig: bus 3/3 configured route mTLS", () => {
  it("renders the exact cluster block: all 3 DNS routes including self, mTLS files, known-URL check, no route auth", async () => {
    const mod = await load();
    const input = triple();
    const text = mod.renderNatsServerConfig(input);
    const preload = [sysKey, orgKey].sort().map((key) => `  ${key}: "${key === sysKey ? SYS_JWT : ORG_JWT}"`);
    expect(text).toBe(
      [
        "# Generated by pi-fabric (smarty-dev#7504): NATS operator mode, full account resolver.",
        "# bus: nodes=3 replicas=3. Stream replicas are not set here; the adapter checks them (smarty-dev#11373).",
        'server_name: "n2"',
        'listen: "n2.example.ts.net:4222"',
        'http: "127.0.0.1:8222"',
        "jetstream {",
        '  store_dir: "/home/nats/.local/share/smarty/nats/js"',
        "  sync_interval: always",
        "}",
        `operator: "${OP_JWT}"`,
        `system_account: "${sysKey}"`,
        "resolver {",
        "  type: full",
        '  dir: "/home/nats/.local/share/smarty/nats/jwt"',
        "  allow_delete: false",
        '  interval: "2m"',
        '  timeout: "1900ms"',
        "}",
        "resolver_preload {",
        ...preload,
        "}",
        "cluster {",
        '  name: "smarty-fleet"',
        '  listen: "n2.example.ts.net:6222"',
        "  routes: [",
        '    "nats-route://n1.example.ts.net:6222"',
        '    "nats-route://n2.example.ts.net:6222"',
        '    "nats-route://n3.example.ts.net:6222"',
        "  ]",
        "  tls {",
        '    cert_file: "/home/nats/.local/share/smarty/nats/tls/n2.crt"',
        '    key_file: "/home/nats/.local/share/smarty/nats/tls/n2.key"',
        '    ca_file: "/home/nats/.local/share/smarty/nats/tls/ca.crt"',
        "    verify_cert_and_check_known_urls: true",
        "  }",
        "}",
        "",
      ].join("\n"),
    );
  });

  it("renders the same uniform route list on every node (no host-derived peer selection)", async () => {
    const mod = await load();
    const routesOf = (serverName: string, host: string) => {
      const input = mutate(triple, (i) => {
        i.serverName = serverName;
        i.listen.host = host;
        if (i.cluster) i.cluster.listen.host = host;
      });
      return configLines(mod.renderNatsServerConfig(input)).filter((line) => line.startsWith('"nats-route://'));
    };
    const first = routesOf("n1", "n1.example.ts.net");
    expect(first).toHaveLength(3);
    expect(routesOf("n3", "n3.example.ts.net")).toEqual(first);
  });

  it("refuses a missing cluster, incomplete route TLS, wrong route count, IP or credentialed or wildcard routes", async () => {
    const mod = await load();
    expect(refusal(mod, mutate(triple, (i) => delete i.cluster)).field).toBe("cluster");
    for (const key of ["certFile", "keyFile", "caFile"] as const) {
      expect(refusal(mod, mutate(triple, (i) => delete (i.cluster?.tls as Partial<Record<string, string>>)[key])).field).toBe(
        `cluster.tls.${key}`,
      );
    }
    expect(refusal(mod, mutate(triple, (i) => delete (i.cluster as Partial<NonNullable<ServerConfigInput["cluster"]>>).tls)).field).toBe(
      "cluster.tls",
    );
    expect(refusal(mod, mutate(triple, (i) => i.cluster?.routes.pop())).field).toBe("cluster.routes");
    expect(refusal(mod, mutate(triple, (i) => i.cluster?.routes.push("nats-route://n4.example.ts.net:6222"))).field).toBe(
      "cluster.routes",
    );
    expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.routes[2] = ROUTES[0] as string))).field).toBe("cluster.routes");
    // Named justified counterexample: an IP-literal route cannot match the DNS SANs that
    // verify_cert_and_check_known_urls checks (Q5, preflight U5), so it is refused rather than rendered.
    for (const route of [
      "nats-route://100.64.0.3:6222",
      "nats-route://[fd7a::3]:6222",
      "nats-route://user:pass@n3.example.ts.net:6222",
      "nats-route://*.example.ts.net:6222",
      "nats://n3.example.ts.net:6222",
      "tls://n3.example.ts.net:6222",
      "nats-route://n3.example.ts.net",
      "nats-route://n3.example.ts.net:0",
      "nats-route://n3.example.ts.net:6222/x",
      'nats-route://n3.example.ts.net:6222"\nauthorization { user: x }',
    ]) {
      expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.routes[2] = route))).field, JSON.stringify(route)).toBe(
        "cluster.routes",
      );
    }
    for (const host of ["0.0.0.0", "::", "*", ""]) {
      expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.listen.host = host))).field, host).toBe(
        "cluster.listen.host",
      );
      expect(refusal(mod, mutate(triple, (i) => (i.listen.host = host))).field, host).toBe("listen.host");
    }
  });
});

describe("validateNatsServerConfigInput: bus, names, paths and injection", () => {
  it("supports exactly bus 1/1 and 3/3; refuses reduced, inflated or unsupported replicas and non-integers", async () => {
    const mod = await load();
    expect(mod.validateNatsServerConfigInput(single())).toEqual(single());
    expect(mod.validateNatsServerConfigInput(triple())).toEqual(triple());
    for (const bus of [
      { nodes: 3, replicas: 1 },
      { nodes: 1, replicas: 3 },
      { nodes: 3, replicas: 2 },
      { nodes: 2, replicas: 2 },
      { nodes: 5, replicas: 5 },
      { nodes: 0, replicas: 0 },
      { nodes: 1.5, replicas: 1 },
      { nodes: "1", replicas: 1 },
    ]) {
      const base = bus.nodes === 3 ? triple : single;
      const field = refusal(mod, mutate(base, (i) => (i.bus = bus as ServerConfigInput["bus"]))).field;
      expect(field.startsWith("bus"), JSON.stringify(bus)).toBe(true);
    }
  });

  it("does not echo secret-shaped unknown keys in validation diagnostics", async () => {
    const mod = await load();
    const secretShapedKey = `INERT-SU${"A".repeat(56)}-eyJhbGciOiJub25lIn0.eyJmb28iOiJiYXIifQ.fake`;
    const cases: Array<{ input: unknown; field: string }> = [
      { input: { ...single(), [secretShapedKey]: "x" }, field: "<unsupported>" },
      {
        input: { ...single(), resolver: { dir: "/srv/fabric/nats/jwt", [secretShapedKey]: "x" } },
        field: "resolver.<unsupported>",
      },
    ];
    for (const { input, field } of cases) {
      let caught: unknown;
      try {
        mod.validateNatsServerConfigInput(input);
      } catch (error) {
        caught = error;
      }
      expect(caught instanceof mod.NatsServerConfigError).toBe(true);
      const diagnostic = caught as ConfigError;
      const diagnosticContainsInputKey = diagnostic.field.includes(secretShapedKey) || diagnostic.message.includes(secretShapedKey);
      expect(diagnosticContainsInputKey).toBe(false);
      expect(diagnostic.field).toBe(field);
    }
  });

  it("refuses unknown input keys, including auth bypass fields (strict schema)", async () => {
    const mod = await load();
    for (const extra of ["noAuthUser", "authorization", "accounts", "include", "replicas"]) {
      const input = { ...single(), [extra]: "x" };
      expect(refusal(mod, input).field, "unknown input field").toBe("<unsupported>");
    }
    expect(refusal(mod, { ...single(), resolver: { dir: "/srv/fabric/nats/jwt", allowDelete: true } }).field).toBe(
      "resolver.<unsupported>",
    );
    expect(refusal(mod, mutate(triple, (i) => i.cluster && Object.assign(i.cluster, { authorization: {} }))).field).toBe(
      "cluster.<unsupported>",
    );
  });

  it("refuses bad server and cluster names (wildcards, spaces, dots, quotes, empty, too long)", async () => {
    const mod = await load();
    for (const name of ["", "a b", "n*", "n>", "n.1", 'n"1', "-n1", "n1\n", "x".repeat(65)]) {
      expect(refusal(mod, mutate(single, (i) => (i.serverName = name))).field, JSON.stringify(name)).toBe("serverName");
      expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.name = name))).field, JSON.stringify(name)).toBe(
        "cluster.name",
      );
    }
  });

  it("refuses relative, HOME-relative, non-normalized, control-character and variable paths; resolver dir != store dir", async () => {
    const mod = await load();
    for (const dir of ["nats/js", "~/nats/js", "$HOME/nats/js", "/srv/../etc", "/srv/./js", "/srv/js/", "/", "/srv/js\nx", "/srv/j\u0000s", "/srv/j\u00e9s"]) {
      expect(refusal(mod, mutate(single, (i) => (i.jetstream.storeDir = dir))).field, JSON.stringify(dir)).toBe(
        "jetstream.storeDir",
      );
      expect(refusal(mod, mutate(single, (i) => (i.resolver.dir = dir))).field, JSON.stringify(dir)).toBe("resolver.dir");
      expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.tls.keyFile = dir))).field, JSON.stringify(dir)).toBe(
        "cluster.tls.keyFile",
      );
    }
    expect(refusal(mod, mutate(single, (i) => (i.resolver.dir = i.jetstream.storeDir))).field).toBe("resolver.dir");
  });

  it("JSON-quotes caller strings so quote, brace, hash and backslash cannot escape their value", async () => {
    const mod = await load();
    const hostile = '/srv/a"b}c{#d\\e ;f';
    const text = mod.renderNatsServerConfig(mutate(single, (i) => (i.jetstream.storeDir = hostile)));
    expect(configLines(text)).toContain(`store_dir: ${JSON.stringify(hostile)}`);
    // The hostile value adds no line and no block; the text shape equals the benign render's.
    const benign = mod.renderNatsServerConfig(single());
    expect(text.split("\n").length).toBe(benign.split("\n").length);
    expect(text.replace(JSON.stringify(hostile), '"X"')).toBe(benign.replace('"/srv/fabric/nats/js"', '"X"'));
  });

  it("refuses bad ports, duplicate ports on one config, and bad resolver durations", async () => {
    const mod = await load();
    for (const port of [0, 65536, -1, 42.5, Number.NaN]) {
      expect(refusal(mod, mutate(single, (i) => (i.listen.port = port))).field, String(port)).toBe("listen.port");
    }
    expect(refusal(mod, mutate(single, (i) => i.monitor && (i.monitor.port = 4222))).field).toBe("monitor.port");
    expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.listen.port = 4222))).field).toBe("cluster.listen.port");
    for (const interval of ["", "2", "2 m", "-2m", "2m\n", "forever", "2d"]) {
      expect(refusal(mod, mutate(single, (i) => (i.resolver.interval = interval))).field, JSON.stringify(interval)).toBe(
        "resolver.interval",
      );
      expect(refusal(mod, mutate(single, (i) => (i.resolver.timeout = interval))).field, JSON.stringify(interval)).toBe(
        "resolver.timeout",
      );
    }
  });
});

describe("validateNatsServerConfigInput: public JWT and key consistency (installed nkeys)", () => {
  it("refuses an operator JWT given as a file path, padded, std-base64, multi-line, wrong alg or not self-signed", async () => {
    const mod = await load();
    const [h, p, s] = OP_JWT.split(".") as [string, string, string];
    const other = nkeys.createOperator();
    const cases: Record<string, string> = {
      path: "/srv/fabric/nats/operator.jwt",
      padded: `${h}.${p}.${s}==`,
      stdBase64: `${h}.${p}.${Buffer.from(s, "base64url").toString("base64").replace(/=+$/, "")}+/`,
      newline: `${OP_JWT}\nsystem_account: X`,
      twoSegments: `${h}.${p}`,
      tamperedPayload: `${h}.${b64url(Buffer.from(p, "base64url").toString().replace("test-operator", "evil-operator"))}.${s}`,
      wrongAlg: signJwt(op, JSON.parse(Buffer.from(p, "base64url").toString()), { typ: "JWT", alg: "ed25519" }),
      noVersion: signJwt(op, { iat: 1, iss: op.getPublicKey(), name: "o", sub: op.getPublicKey(), nats: { type: "operator", system_account: sysKey } }),
      accountAsOperator: SYS_JWT,
      notSelfSigned: signJwt(other, { iat: 1, iss: other.getPublicKey(), name: "o", sub: op.getPublicKey(), nats: { type: "operator", version: 2, system_account: sysKey } }),
    };
    for (const [label, jwt] of Object.entries(cases)) {
      const error = refusal(mod, mutate(single, (i) => (i.operatorJwt = jwt)));
      expect(error.field, label).toBe("operatorJwt");
      expect(error.message.includes(jwt), `${label}: error must not echo the JWT`).toBe(false);
    }
  });

  it("refuses a seed-shaped operatorJwt without echoing it", async () => {
    const mod = await load();
    const seed = new TextDecoder().decode(nkeys.createOperator().getSeed());
    const error = refusal(mod, mutate(single, (i) => (i.operatorJwt = seed)));
    expect(error.field).toBe("operatorJwt");
    expect(error.message.includes(seed)).toBe(false);
    expect(error.message.includes(seed.slice(0, 12))).toBe(false);
  });

  it("requires systemAccount to be a valid account key equal to the operator's system_account, and preloaded", async () => {
    const mod = await load();
    expect(refusal(mod, mutate(single, (i) => (i.systemAccount = op.getPublicKey()))).field).toBe("systemAccount");
    const badChecksum = `${sysKey.slice(0, -1)}${sysKey.endsWith("A") ? "B" : "A"}`;
    expect(refusal(mod, mutate(single, (i) => (i.systemAccount = badChecksum))).field).toBe("systemAccount");
    expect(
      refusal(
        mod,
        mutate(single, (i) => {
          i.systemAccount = orgKey;
          i.preload = { [orgKey]: ORG_JWT };
        }),
      ).field,
    ).toBe("systemAccount");
    const noSysOperator = operatorJwt(op, orgKey, { signingKeys: [opSigning.getPublicKey()] });
    expect(refusal(mod, mutate(single, (i) => (i.operatorJwt = noSysOperator))).field).toBe("systemAccount");
    expect(refusal(mod, mutate(single, (i) => (i.preload = { [orgKey]: ORG_JWT }))).field).toBe("preload");
    expect(refusal(mod, mutate(single, (i) => (i.preload = {}))).field).toBe("preload");
  });

  it("refuses preload entries that are not account JWTs for that key, issued by the operator or its signing keys", async () => {
    const mod = await load();
    const stranger = nkeys.createOperator();
    const bad: Record<string, [string, string]> = {
      subjectMismatch: [orgKey, SYS_JWT],
      untrustedIssuer: [orgKey, accountJwt(stranger, orgKey, "smarty-pants")],
      notAccountKey: [op.getPublicKey(), OP_JWT],
      operatorTypedClaim: [orgKey, signJwt(op, { iat: 1, iss: op.getPublicKey(), name: "x", sub: orgKey, nats: { type: "operator", version: 2 } })],
      tampered: [orgKey, ORG_JWT.replace(/\.[^.]+$/, `.${b64url(new Uint8Array(64))}`)],
      notJwt: [orgKey, "/srv/fabric/nats/org.jwt"],
    };
    for (const [label, [key, jwt]] of Object.entries(bad)) {
      const error = refusal(mod, mutate(single, (i) => (i.preload = { [sysKey]: SYS_JWT, [key]: jwt })));
      expect(error.field, label).toBe(`preload.${key}`);
      expect(error.message.includes(jwt), `${label}: error must not echo the JWT`).toBe(false);
    }
  });
});

// Review regressions C1/C2 (.local/7504-final-change-only-code-security.md). Pure config checks only: they cannot show
// that a real nats-server on Windows parses drive paths (hosted T8/T1, owner fabric-store, unconfirmed).
describe("config review regressions", () => {
  const PATH_FIELDS = ["jetstream.storeDir", "resolver.dir", "cluster.tls.certFile", "cluster.tls.keyFile", "cluster.tls.caFile"] as const;
  type PathField = (typeof PATH_FIELDS)[number];
  const setPath = (i: ServerConfigInput, field: PathField, value: string) => {
    const tls = i.cluster?.tls;
    assert(tls, "the triple fixture has route TLS");
    if (field === "jetstream.storeDir") i.jetstream.storeDir = value;
    else if (field === "resolver.dir") i.resolver.dir = value;
    else if (field === "cluster.tls.certFile") tls.certFile = value;
    else if (field === "cluster.tls.keyFile") tls.keyFile = value;
    else tls.caFile = value;
  };
  const getPath = (i: ServerConfigInput, field: PathField): string | undefined =>
    field === "jetstream.storeDir"
      ? i.jetstream.storeDir
      : field === "resolver.dir"
        ? i.resolver.dir
        : i.cluster?.tls[field.slice("cluster.tls.".length) as "certFile" | "keyFile" | "caFile"];

  it("C1 refuses a route inventory that omits this node's cluster.listen host and port", async () => {
    const mod = await load();
    const missingLocal = mutate(triple, (i) => {
      if (i.cluster) i.cluster.routes = [ROUTES[0] as string, ROUTES[2] as string, "nats-route://n4.example.ts.net:6222"];
    });
    expect(refusal(mod, missingLocal).field).toBe("cluster.routes");
    const wrongPort = mutate(triple, (i) => i.cluster && (i.cluster.listen.port = 6223));
    expect(refusal(mod, wrongPort).field).toBe("cluster.routes");
    const error = refusal(mod, missingLocal);
    expect(error.message.includes("cluster.listen")).toBe(true);
    expect(error.message.includes("n4.example")).toBe(false);
  });

  it("C1 refuses an IP cluster.listen (DNS-only routes cannot represent it) and leading-zero route ports", async () => {
    const mod = await load();
    for (const host of ["100.64.0.2", "10.0.0.2"]) {
      expect(refusal(mod, mutate(triple, (i) => i.cluster && (i.cluster.listen.host = host))).field, host).toBe(
        "cluster.listen.host",
      );
    }
    // A leading-zero port is the same endpoint as 6222 and must not pass as a distinct node.
    for (const route of ["nats-route://n1.example.ts.net:06222", "nats-route://n3.example.ts.net:006222"]) {
      const input = mutate(triple, (i) => i.cluster && (i.cluster.routes[route.includes("n1.") ? 0 : 2] = route));
      expect(refusal(mod, input).field, route).toBe("cluster.routes");
    }
  });

  it("C1 keeps accepting the uniform DNS route inventory in any order, for any configured local node", async () => {
    const mod = await load();
    const [a, b, c] = ROUTES as [string, string, string];
    for (const order of [[a, b, c], [c, b, a], [b, a, c], [c, a, b]]) {
      const input = mutate(triple, (i) => i.cluster && (i.cluster.routes = [...order]));
      expect(mod.validateNatsServerConfigInput(input).cluster?.routes, order.join(",")).toEqual(order);
    }
    for (const node of ["n1", "n3"]) {
      const input = mutate(triple, (i) => {
        i.serverName = node;
        i.listen.host = `${node}.example.ts.net`;
        if (i.cluster) i.cluster.listen.host = `${node}.example.ts.net`;
      });
      expect(mod.validateNatsServerConfigInput(input)).toEqual(input);
    }
  });

  it("C2 accepts drive-rooted Windows paths with either separator for every path field, canonicalized to forward slashes", async () => {
    const mod = await load();
    for (const field of PATH_FIELDS) {
      for (const [given, canonical] of [
        ["C:/NATS/js", "C:/NATS/js"],
        ["C:\\NATS\\js", "C:/NATS/js"],
        ["d:\\NATS/mixed sep\\x.crt", "D:/NATS/mixed sep/x.crt"],
      ] as const) {
        const input = mutate(triple, (i) => setPath(i, field, given));
        const normalized = mod.validateNatsServerConfigInput(input);
        expect(getPath(normalized, field), `${field} ${JSON.stringify(given)}`).toBe(canonical);
        const lines = configLines(mod.renderNatsServerConfig(input));
        expect(lines.some((line) => line.endsWith(`: ${JSON.stringify(canonical)}`)), `${field} rendered`).toBe(true);
        // Rendering the normalized copy is stable (render validates again).
        expect(mod.renderNatsServerConfig(normalized)).toBe(mod.renderNatsServerConfig(input));
      }
    }
  });

  it("C2 compares Windows paths after normalization for the distinctness checks", async () => {
    const mod = await load();
    const sameDir = mutate(triple, (i) => {
      i.jetstream.storeDir = "C:\\NATS\\js";
      i.resolver.dir = "c:/nats/JS";
    });
    expect(refusal(mod, sameDir).field).toBe("resolver.dir");
    const sameTls = mutate(triple, (i) => {
      if (!i.cluster) return;
      i.cluster.tls.certFile = "C:/NATS/tls/n2.crt";
      i.cluster.tls.keyFile = "C:\\NATS\\tls\\n2.crt";
    });
    expect(refusal(mod, sameTls).field).toBe("cluster.tls");
  });

  it("C2 refuses UNC, device/namespace, drive- and slash-relative, non-normalized, control, env and reserved Windows forms", async () => {
    const mod = await load();
    const forbidden = [
      "\\\\server\\share\\js",
      "//server/share/js",
      "\\\\?\\C:\\NATS\\js",
      "\\\\.\\C:\\NATS\\js",
      "\\\\?\\UNC\\server\\share",
      "\\??\\C:\\NATS",
      "C:NATS\\js",
      "C:",
      "C:/",
      "C:\\",
      "\\NATS\\js",
      "CC:/NATS",
      "1:/NATS",
      "C:/NATS/../js",
      "C:\\NATS\\..\\js",
      "C:/NATS/./js",
      "C:\\NATS\\\\js",
      "C:/NATS//js",
      "C:\\NATS\\js\\",
      "C:/NATS/js/",
      "C:/NATS/js\n",
      "C:/NA\u0000TS",
      "C:/NA\tTS",
      "C:/N\u00c4TS",
      "C:/$HOME/js",
      "C:/%USERPROFILE%/js",
      "%SYSTEMROOT%\\js",
      "C:/NATS/a:b",
      "C:/NATS/a*b",
      "C:/NATS/a?b",
      'C:/NATS/a"b',
      "C:/NATS/a<b",
      "C:/NATS/a>b",
      "C:/NATS/a|b",
      "C:/NATS/js.",
      "C:/NATS/js ",
    ];
    for (const field of PATH_FIELDS) {
      for (const value of forbidden) {
        const error = refusal(mod, mutate(triple, (i) => setPath(i, field, value)));
        expect(error.field, `${field} ${JSON.stringify(value)}`).toBe(field);
        const echoes = error.message.includes(value) || error.message.includes("USERPROFILE") || error.message.includes("share");
        expect(echoes, `${field} ${JSON.stringify(value)}: value-free message`).toBe(false);
      }
    }
  });

  it("C2 leaves POSIX paths unchanged: backslash stays literal inside a segment, relative and traversal still refused", async () => {
    const mod = await load();
    for (const field of PATH_FIELDS) {
      for (const value of ["/srv/fabric/nats/js", "/srv/nats\\js", "/srv/C:/js", '/srv/a"b}c{#d\\e ;f']) {
        const input = mutate(triple, (i) => setPath(i, field, value));
        expect(getPath(mod.validateNatsServerConfigInput(input), field), `${field} ${JSON.stringify(value)}`).toBe(value);
      }
      for (const value of ["srv/js", "~/js", "/srv/../js", "/srv//js", "/srv/js/", "/srv/$HOME", "/srv/j\u0000s"]) {
        expect(refusal(mod, mutate(triple, (i) => setPath(i, field, value))).field, `${field} ${JSON.stringify(value)}`).toBe(field);
      }
    }
  });
});
