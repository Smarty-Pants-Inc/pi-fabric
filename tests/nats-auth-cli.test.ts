import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { credsAuthenticator, nkeys } from "@nats-io/transport-node";
import { afterAll, describe, expect, it } from "vitest";
import { createNatsOperator } from "../src/mesh/nats-auth/mint.js";

// smarty-dev#7504 phase 1 slice 1, design Amendment B (frozen 231fb080): `pi-fabric nats` CLI (mint operator|account|user,
// mint user --break-glass, mint user --sys, config) and its file behaviour.
// RED-first: written before src/mesh/nats-auth/{cli,mint}.ts exist and before `nats` is registered in
// src/cli/index.ts. Every case first asserts those sources exist, labelled as absence RED, then runs the real source CLI
// entry in a child process (`bun src/cli/index.ts nats ...`). Absence fails each case on its own, not at collection.
//
// Secret hygiene: keys are generated at runtime by the CLI under test (or by the installed nkeys for negative fixtures),
// delivered to the child on stdin only, and never placed in argv or the environment. Assertions that touch stdout,
// stderr, seed files, .creds bytes or JWT strings compare booleans/lengths/public keys only, so a failure never prints a
// seed, a .creds block or a whole JWT. Decoded public claims (public keys, names, tags, permissions) may appear in a diff.
//
// Decision C (why CLI/unit here, not installed E2E): these prove argv/stdin parsing, exit codes, refuse-before-write,
// exclusive 0600/0700 artifacts and the claim fields the minter emits, through the installed nkeys/credsAuthenticator.
// They do not prove that nats-server accepts the operator/account/user chain or enforces the templates: that is E1
// hosted T2 (role matrix: exact allow/refuse per role, break-glass and SYS-only user included) and T8 (single-node
// operator auth: one org account, its users connect, unknown/expired/wrong-issuer users refused) on the pinned
// nats-server 2.14.7, owner fabric-store. No nats-server is started here; no credential store, HOME path or environment
// secret is read. Break-glass approval and the stop-first runbook are caller duty; nothing here validates an approval.

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const cliEntry = fileURLToPath(new URL("../src/cli/index.ts", import.meta.url));
const cliModule = fileURLToPath(new URL("../src/mesh/nats-auth/cli.ts", import.meta.url));
const IMPLEMENTATION = ["src/mesh/nats-auth/mint.ts", "src/mesh/nats-auth/cli.ts"];
const bun = process.versions.bun ? process.execPath : "bun";
const posix = process.platform !== "win32";
const posixMintDescribe = describe.skipIf(!posix);
const DAY = 86_400;
const MESH_HEX = "0123456789abcdef0123456789abcdef";
const OTHER_HEX = "fedcba9876543210fedcba9876543210";
// Fixture names only. The real <CUS>/<LIVE> names are open item O-2 and are not invented here.
const STREAMS = { custodyStream: "FIXTURE_CUSTODY", livenessStream: "KV_FIXTURE_LIVENESS" };
// Required finite quotas: non-negative safe integers; memory 0 is a valid disk-only account.
const JETSTREAM = { memStorage: 0, diskStorage: 1_073_741_824, streams: 64, consumers: 256 };
const APPROVAL = "fixture-approval-0001";
// A full 58-character NATS seed token, never a substring of a public key or a longer base32 token.
const SEED_SHAPE = /(?:^|[^A-Z2-7])S[OAUNC][A-Z2-7]{56}(?![A-Z2-7])/;
const JWT_SHAPE = /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/;
const utf8 = new TextEncoder();
const text = new TextDecoder();
const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

const CUS = STREAMS.custodyStream;
const LIVE = STREAMS.livenessStream;
const EVERY_ROLE_DENY = [
  `$JS.API.DIRECT.GET.${CUS}`,
  `$JS.API.DIRECT.GET.${CUS}.>`,
  `$JS.API.STREAM.PURGE.${CUS}`,
  `$JS.API.STREAM.PURGE.${LIVE}`,
  `$JS.API.STREAM.DELETE.${CUS}`,
  `$JS.API.STREAM.DELETE.${LIVE}`,
];
const NORMAL_ROLE_DENY = [
  ...EVERY_ROLE_DENY,
  `$JS.API.STREAM.CREATE.${CUS}`,
  `$JS.API.STREAM.CREATE.${LIVE}`,
  `$JS.API.STREAM.UPDATE.${CUS}`,
  `$JS.API.STREAM.UPDATE.${LIVE}`,
];
const BREAK_GLASS_ALLOW = [
  "$JS.API.INFO",
  ...["INFO", "CREATE", "UPDATE"].flatMap((verb) => [`$JS.API.STREAM.${verb}.${CUS}`, `$JS.API.STREAM.${verb}.${LIVE}`]),
  "$SYS.REQ.USER.INFO",
];
const ORG_ROLES = ["agent", "principal", "maintainer", "break-glass"] as const;
type OrgRole = (typeof ORG_ROLES)[number];

/** Absence of the CLI/minter sources is RED for a contract not yet written, not a regression. */
function requireImplementation() {
  for (const file of IMPLEMENTATION) {
    expect(fs.existsSync(path.join(repoRoot, file)), `RED (#7504 Amendment B, implementation absent): ${file} does not exist yet`).toBe(true);
  }
}

const roots: string[] = [];
afterAll(() => {
  // Teardown of this file's own private mktemp roots only.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (): string => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-nats-cli-")));
  roots.push(root);
  return root;
};

interface CliResult {
  status: number | null;
  stdout: string;
  stderr: string;
}

function cleanEnv() {
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (key.startsWith("NATS_") || key.startsWith("FABRIC_NATS")) delete env[key];
  return env;
}

function nats(args: string[], input = ""): CliResult {
  const result = spawnSync(bun, [cliEntry, "nats", ...args], { encoding: "utf8", env: cleanEnv(), input, timeout: 30_000 });
  return { status: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

type PrivateOutputFault = "dir-mode" | "dir-owner" | "dir-link" | "file-mode" | "file-owner" | "file-link";

/** Child-only fs fault injection; all arguments contain paths/options, while input bytes stay on stdin. */
function natsWithPolicyHarness(
  args: string[],
  input: string,
  options: { fault?: PrivateOutputFault; platform?: "win32" },
): CliResult {
  const script = `
import fs from "node:fs";
import path from "node:path";
const target = ${JSON.stringify(args[args.indexOf("--out-dir") + 1] ?? "")};
const fault = ${JSON.stringify(options.fault ?? "")};
const alter = (stat, area) => new Proxy(stat, {
  get(actual, property, receiver) {
    if ((fault === "dir-owner" && area === "dir" || fault === "file-owner" && area === "file") && property === "uid") return actual.uid + 1;
    if ((fault === "dir-link" && area === "dir" || fault === "file-link" && area === "file") && property === "isSymbolicLink") return () => true;
    if (fault === "dir-link" && area === "dir" && property === "isDirectory") return () => true;
    if (fault === "file-link" && area === "file" && property === "isFile") return () => true;
    return Reflect.get(actual, property, receiver);
  }
});
if (fault.startsWith("dir-")) {
  const chmodSync = fs.chmodSync.bind(fs);
  fs.chmodSync = (file, mode) => chmodSync(file, String(file) === target && fault === "dir-mode" ? 0o750 : mode);
}
if (fault === "file-mode") {
  const fchmodSync = fs.fchmodSync.bind(fs);
  fs.fchmodSync = (fd, mode) => fchmodSync(fd, 0o640);
}
if (fault.startsWith("dir-") || fault === "file-link") {
  const lstatSync = fs.lstatSync.bind(fs);
  fs.lstatSync = (file, ...rest) => {
    const stat = lstatSync(file, ...rest);
    const value = String(file);
    if (value === target && fault.startsWith("dir-")) return alter(stat, "dir");
    if (fault === "file-link" && path.dirname(value) === target) return alter(stat, "file");
    return stat;
  };
}
if (fault === "file-owner") {
  const fstatSync = fs.fstatSync.bind(fs);
  fs.fstatSync = (fd, ...rest) => alter(fstatSync(fd, ...rest), "file");
}
const { runNatsCli } = await import(${JSON.stringify(cliModule)});
${options.platform === "win32" ? 'Object.defineProperty(process, "platform", { value: "win32" });' : ""}
process.exitCode = await runNatsCli(${JSON.stringify(args)});
`;
  const result = spawnSync(bun, ["-e", script], { encoding: "utf8", env: cleanEnv(), input, timeout: 30_000 });
  return { status: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

/** True when text carries any secret-shaped value or one of the given secret strings. Boolean only. */
function leaks(value: string, secrets: string[] = []): boolean {
  return SEED_SHAPE.test(value) || JWT_SHAPE.test(value) || secrets.some((secret) => secret.length > 0 && value.includes(secret));
}

// Supplemental Decision C: deterministic classifier checks, not a server handshake. Hosted T2/T8 (fabric-store)
// remain the acceptance oracle for emitted credentials; these only prevent public keys from looking like seeds.
describe("#7504 seed token scanner", () => {
  it("does not flag a valid account public key with an internal SO run", () => {
    const publicKey = "ADXSO4PQLML4VQ7OZWRRUWDWPDWTJNSDEJUVHDWEGYDEKTPNWPIALHD2";
    const pair = nkeys.fromPublic(publicKey);
    try {
      expect(pair.getPublicKey() === publicKey, "fixture is a valid public key").toBe(true);
      expect(/S[OAU][A-Z2-7]{50,}/.test(publicKey), "fixture reproduces the old substring match").toBe(true);
      expect(leaks(JSON.stringify({ publicKey })), "public metadata is not a seed token").toBe(false);
    } finally {
      pair.clear();
    }
  });

  it("flags an embedded real seed and retains exact known-secret checks without matching longer base32 tokens", () => {
    const pair = nkeys.createUser();
    const bytes = pair.getSeed();
    try {
      const seed = text.decode(bytes);
      expect(seed.length === 58, "NATS seed has its complete encoded length").toBe(true);
      expect(leaks(`before:(${seed}):after`), "a real seed in surrounding text is flagged").toBe(true);
      expect(leaks(`A${seed}`), "a preceding base32 character prevents a seed token").toBe(false);
      expect(leaks(`${seed}A`), "a following base32 character prevents a seed token").toBe(false);
      expect(leaks(`A${seed}A`, [seed]), "exact known-secret presence still flags concatenated text").toBe(true);
      for (const prefix of ["O", "A", "U", "N", "C"]) {
        const shaped = `S${prefix}${"A".repeat(56)}`;
        expect(leaks(`(${shaped})`), "every NATS seed prefix is recognized").toBe(true);
        expect(leaks(shaped.slice(0, -1)), "truncated seed shapes are not complete tokens").toBe(false);
        expect(leaks(`${shaped}A`), "longer seed shapes are not complete tokens").toBe(false);
      }
    } finally {
      bytes.fill(0);
      pair.clear();
    }
  });

  it("flags a real seed at the start and end of a line", () => {
    const pair = nkeys.createUser();
    const bytes = pair.getSeed();
    try {
      const seed = text.decode(bytes);
      for (const value of [`${seed} trailing`, `leading ${seed}`, `\n${seed}\n`, seed]) {
        expect(leaks(value), "line boundaries do not hide a seed token").toBe(true);
      }
    } finally {
      bytes.fill(0);
      pair.clear();
    }
  });
});

/** Asserts no secret on either stream, then parses stdout JSON without ever letting the raw text into a diff. */
function publicJson(result: CliResult, secrets: string[] = []): Record<string, any> {
  expect(leaks(result.stdout, secrets), "stdout carries no seed/JWT/creds bytes").toBe(false);
  expect(leaks(result.stderr, secrets), "stderr carries no seed/JWT/creds bytes").toBe(false);
  let parsed: unknown;
  try {
    parsed = JSON.parse(result.stdout);
  } catch {
    parsed = undefined;
  }
  expect(typeof parsed === "object" && parsed !== null, "stdout is one JSON object").toBe(true);
  return parsed as Record<string, any>;
}

function expectRefused(result: CliResult, code: 1 | 2, secrets: string[] = []) {
  expect(result.status).toBe(code);
  expect(leaks(result.stdout, secrets), "refusal stdout carries no secret").toBe(false);
  expect(leaks(result.stderr, secrets), "refusal stderr carries no secret").toBe(false);
  expect(result.stderr.startsWith("pi-fabric nats:"), "refusal comes from the nats command").toBe(true);
}

const mode = (file: string) => fs.lstatSync(file).mode & 0o777;

function decodeJwt(jwt: string): { claims: Record<string, any>; verifiesWith(publicKey: string): boolean } {
  const segments = jwt.split(".");
  expect(segments.length).toBe(3);
  const [h, p, s] = segments as [string, string, string];
  const claims = JSON.parse(Buffer.from(p, "base64url").toString("utf8"));
  return {
    claims,
    verifiesWith: (publicKey) =>
      nkeys.fromPublic(publicKey).verify(utf8.encode(`${h}.${p}`), new Uint8Array(Buffer.from(s, "base64url"))),
  };
}

/** Public key owned by a seed file; the seed stays in this process and is never printed. */
function seedOwner(file: string): string {
  return nkeys.fromSeed(new Uint8Array(fs.readFileSync(file))).getPublicKey();
}

const readTrim = (file: string) => fs.readFileSync(file, "utf8").trim();

interface Operator {
  dir: string;
  operatorJwt: string;
  operatorSigningSeed: string;
  operatorIdentitySeed: string;
  systemAccountJwt: string;
  systemAccountSeed: string;
}

function bootstrapOperator(dir: string): Operator {
  const op = nats(["mint", "operator", "--out-dir", dir], JSON.stringify({ operatorName: "fixture-operator" }));
  expect(op.status, "operator bootstrap exit").toBe(0);
  return {
    dir,
    operatorJwt: readTrim(path.join(dir, "operator.jwt")),
    operatorSigningSeed: readTrim(path.join(dir, "operator-signing.nk")),
    operatorIdentitySeed: readTrim(path.join(dir, "operator.nk")),
    systemAccountJwt: readTrim(path.join(dir, "sys-account.jwt")),
    systemAccountSeed: readTrim(path.join(dir, "sys-account.nk")),
  };
}

interface Chain extends Operator {
  root: string;
  operatorDir: string;
  accountDir: string;
  accountJwt: string;
  accountPublicKey: string;
  accountSeed: string;
  signing: Record<OrgRole, { seed: string; publicKey: string }>;
  stranger: Operator;
  secrets: string[];
}

let chainPromise: Promise<Chain> | undefined;
/** Operator bootstrap then org account setup, both through the CLI under test; memoized so absence fails each case. */
function chain(): Promise<Chain> {
  chainPromise ??= (async () => {
    const root = tempRoot();
    const operatorDir = path.join(root, "operator");
    const operator = bootstrapOperator(operatorDir);
    const accountDir = path.join(root, "account");
    const accountInput = {
      name: "fixture-org",
      operatorJwt: operator.operatorJwt,
      operatorSigningSeed: operator.operatorSigningSeed,
      jetstream: JETSTREAM,
      ...STREAMS,
    };
    const account = nats(["mint", "account", "--out-dir", accountDir], JSON.stringify(accountInput));
    expect(account.status, "account setup exit").toBe(0);
    const accountJwt = readTrim(path.join(accountDir, "account.jwt"));
    const accountSeed = readTrim(path.join(accountDir, "account.nk"));
    const read = (role: OrgRole) => {
      const seed = readTrim(path.join(accountDir, `signing-${role}.nk`));
      return { seed, publicKey: nkeys.fromSeed(utf8.encode(seed)).getPublicKey() };
    };
    const signing = Object.fromEntries(ORG_ROLES.map((role) => [role, read(role)])) as Chain["signing"];
    // A second, unrelated operator: its JWT must never vouch for this org's account or SYS account.
    const stranger = bootstrapOperator(path.join(root, "stranger-operator"));
    return {
      ...operator,
      root,
      operatorDir,
      accountDir,
      accountJwt,
      accountPublicKey: decodeJwt(accountJwt).claims.sub as string,
      accountSeed,
      signing,
      stranger,
      secrets: [
        operator.operatorSigningSeed,
        operator.operatorIdentitySeed,
        operator.systemAccountSeed,
        stranger.operatorSigningSeed,
        stranger.operatorIdentitySeed,
        stranger.systemAccountSeed,
        accountSeed,
        ...Object.values(signing).map((entry) => entry.seed),
      ],
    };
  })();
  return chainPromise;
}

function orgCommon(c: Chain) {
  return { operatorJwt: c.operatorJwt, accountPublicKey: c.accountPublicKey, accountJwt: c.accountJwt, ...STREAMS };
}

function agentInput(c: Chain, overrides: Record<string, unknown> = {}) {
  return {
    role: "agent",
    ...orgCommon(c),
    signingSeed: c.signing.agent.seed,
    host: "fixture-host",
    lane: "fixture-lane",
    session: "session-0001",
    release: "fixture-release",
    meshes: [MESH_HEX],
    ...overrides,
  };
}

function breakGlassInput(c: Chain, overrides: Record<string, unknown> = {}) {
  return { ...orgCommon(c), signingSeed: c.signing["break-glass"].seed, person: "paul", runId: "run-0003", approvalRef: APPROVAL, ...overrides };
}

function sysInput(c: Chain, overrides: Record<string, unknown> = {}) {
  return {
    operatorJwt: c.operatorJwt,
    systemAccountJwt: c.systemAccountJwt,
    systemAccountSeed: c.systemAccountSeed,
    person: "paul",
    runId: "run-0004",
    ...overrides,
  };
}

function readCreds(dir: string): { userPublicKey: string; jwt: string; creds: Uint8Array } {
  const file = path.join(dir, "user.creds");
  const creds = new Uint8Array(fs.readFileSync(file));
  const auth = credsAuthenticator(creds)("fixture-nonce") as { jwt?: string; nkey?: string; sig?: string };
  const sig = nkeys.decode(auth.sig ?? "");
  expect(nkeys.fromPublic(auth.nkey ?? "").verify(utf8.encode("fixture-nonce"), sig), "creds seed owns the user key").toBe(true);
  return { userPublicKey: auth.nkey ?? "", jwt: auth.jwt ?? "", creds };
}

/** Expects one fresh user.creds in a new 0700 dir; returns the decoded user and the seed text for leak checks. */
function expectUserArtifact(dir: string) {
  expect(fs.readdirSync(dir)).toEqual(["user.creds"]);
  if (posix) {
    expect(mode(dir)).toBe(0o700);
    expect(mode(path.join(dir, "user.creds"))).toBe(0o600);
  }
  const { userPublicKey, jwt, creds } = readCreds(dir);
  const seedText = text.decode(creds).match(SEED_SHAPE)?.[0] ?? "";
  expect(seedText.length > 0).toBe(true);
  expect(userPublicKey[0]).toBe("U");
  return { userPublicKey, jwt, seedText, user: decodeJwt(jwt) };
}

function expectRefusedBeforeWrite(label: string, result: CliResult, dir: string, secrets: string[]) {
  expect(result.status, label).toBe(1);
  expect(leaks(result.stderr, secrets), `${label}: stderr secret-free`).toBe(false);
  expect(leaks(result.stdout, secrets), `${label}: stdout secret-free`).toBe(false);
  expect(result.stderr.startsWith("pi-fabric nats:"), `${label}: refusal comes from the nats command`).toBe(true);
  expect(fs.existsSync(dir), `${label}: nothing written`).toBe(false);
}

/** Metadata-only check: no test reads or prints any artifact contents. */
function outputTreeHasNoBytes(dir: string): boolean {
  if (!fs.existsSync(dir)) return true;
  const directory = fs.lstatSync(dir);
  return directory.isDirectory() && !directory.isSymbolicLink() && fs.readdirSync(dir).every((name) => {
    const file = fs.lstatSync(path.join(dir, name));
    return file.isFile() && !file.isSymbolicLink() && file.size === 0;
  });
}

describe("#7504 private-output policy", () => {
  it.skipIf(!posix)("creates mint directories and files owned by this UID at exact private modes", () => {
    requireImplementation();
    const root = tempRoot();
    const dir = path.join(root, "private-output");
    const result = nats(["mint", "operator", "--out-dir", dir], JSON.stringify({ operatorName: "fixture-private" }));
    expect(result.status).toBe(0);
    expect(leaks(result.stdout), "stdout carries no seed/JWT/creds bytes").toBe(false);
    expect(leaks(result.stderr), "stderr carries no seed/JWT/creds bytes").toBe(false);
    const uid = process.getuid?.();
    const directory = fs.lstatSync(dir);
    expect(typeof uid === "number" && directory.uid === uid).toBe(true);
    expect(directory.isDirectory() && !directory.isSymbolicLink()).toBe(true);
    expect(directory.mode & 0o7777).toBe(0o700);
    const names = fs.readdirSync(dir).sort();
    expect(names).toEqual(["operator-signing.nk", "operator.jwt", "operator.nk", "sys-account.jwt", "sys-account.nk"]);
    for (const name of names) {
      const file = fs.lstatSync(path.join(dir, name));
      expect(typeof uid === "number" && file.uid === uid, "artifact owner is this UID").toBe(true);
      expect(file.isFile() && !file.isSymbolicLink(), "artifact is a regular non-link file").toBe(true);
      expect(file.mode & 0o7777, "artifact has exact 0600 mode").toBe(0o600);
    }
  });

  it("simulates Windows and refuses mint before creating any output", () => {
    requireImplementation();
    const root = tempRoot();
    const dir = path.join(root, "windows-output");
    const args = ["mint", "operator", "--out-dir", dir];
    const input = JSON.stringify({ operatorName: "fixture-private" });
    const result = process.platform === "win32"
      ? nats(args, input)
      : natsWithPolicyHarness(args, input, { platform: "win32" });
    expectRefusedBeforeWrite("Windows private-output policy", result, dir, []);
    expect(result.stderr.includes("smarty-dev#11482"), "fixed refusal identifies the Windows ACL issue").toBe(true);
  });

  it.each(["dir-mode", "dir-owner", "dir-link", "file-mode", "file-owner", "file-link"] as const)(
    "refuses injected %s verification failure before any artifact bytes are written",
    (fault) => {
      if (!posix) return;
      requireImplementation();
      const root = tempRoot();
      const dir = path.join(root, `fault-${fault}`);
      const result = natsWithPolicyHarness(
        ["mint", "operator", "--out-dir", dir],
        JSON.stringify({ operatorName: "fixture-private" }),
        { fault },
      );
      expect(result.status).toBe(1);
      expect(leaks(result.stdout), "refusal stdout is secret-free").toBe(false);
      expect(leaks(result.stderr), "refusal stderr is secret-free").toBe(false);
      expect(result.stderr.includes("private output verification failed")).toBe(true);
      expect(outputTreeHasNoBytes(dir), "no private artifact bytes were written").toBe(true);
    },
  );
});

describe("#7504 pi-fabric nats: registration and usage", () => {
  it("is listed by pi-fabric and prints its own usage with mint operator|account|user, --break-glass, --sys and config", () => {
    requireImplementation();
    const top = spawnSync(bun, [cliEntry, "--help"], { encoding: "utf8", timeout: 30_000 });
    expect(top.status).toBe(0);
    expect(/^\s+nats$/m.test(top.stdout), "pi-fabric --help lists nats").toBe(true);
    const help = nats(["--help"]);
    expect(help.status).toBe(0);
    const fragments = ["Usage: pi-fabric nats", "mint operator", "mint account", "mint user", "--break-glass", "--sys", "--out-dir", "config", "--input", "--out"];
    for (const fragment of fragments) expect(help.stdout.includes(fragment), fragment).toBe(true);
  });

  it("exits 2 on unknown subcommands, kinds, options, conflicting and repeated flags without echoing argv values", () => {
    requireImplementation();
    const seed = text.decode(nkeys.createAccount().getSeed());
    const root = tempRoot();
    const cases: string[][] = [
      ["frobnicate"],
      ["mint"],
      ["mint", "sys", "--out-dir", path.join(root, "a")],
      ["mint", "break-glass", "--out-dir", path.join(root, "a2")],
      ["mint", "user"],
      ["mint", "user", "--seed", seed, "--out-dir", path.join(root, "b")],
      ["mint", "user", `--seed=${seed}`, "--out-dir", path.join(root, "c")],
      ["mint", "user", "--out-dir", path.join(root, "d"), "--out-dir", path.join(root, "e")],
      ["mint", "user", "--out-dir", seed],
      ["mint", "user", "--out-dir", "relative-dir"],
      ["mint", "user", "--break-glass", "--sys", "--out-dir", path.join(root, "h")],
      ["mint", "user", "--break-glass", "--break-glass", "--out-dir", path.join(root, "i")],
      ["mint", "account", "--sys", "--out-dir", path.join(root, "j")],
      ["mint", "operator", "--break-glass", "--out-dir", path.join(root, "k")],
      ["mint", "account", "--out-dir", path.join(root, "f"), "--expires", "999d"],
      ["config", "--out", path.join(root, "g.conf")],
      ["config", "--input", "-"],
      ["config", "--input", path.join(root, "in.json"), "--out", path.join(root, "l.conf")],
    ];
    for (const [index, args] of cases.entries()) {
      const result = nats(args);
      expectRefused(result, 2, [seed]);
      expect(result.stderr.includes("Usage: pi-fabric nats"), `usage shown for case ${index}`).toBe(true);
    }
    expect(fs.readdirSync(root)).toEqual([]);
  });
});

posixMintDescribe("#7504 pi-fabric nats mint operator", () => {
  it("bootstraps operator identity, operator signing key and SYS account (no SYS user) into a new 0700 dir of 0600 files", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const files = fs.readdirSync(c.operatorDir).sort();
    expect(files).toEqual(["operator-signing.nk", "operator.jwt", "operator.nk", "sys-account.jwt", "sys-account.nk"]);
    if (posix) {
      expect(mode(c.operatorDir)).toBe(0o700);
      for (const file of files) expect(mode(path.join(c.operatorDir, file)), file).toBe(0o600);
    }
    const operator = decodeJwt(c.operatorJwt);
    const identity = seedOwner(path.join(c.operatorDir, "operator.nk"));
    const signing = seedOwner(path.join(c.operatorDir, "operator-signing.nk"));
    const sys = seedOwner(path.join(c.operatorDir, "sys-account.nk"));
    expect([identity[0], signing[0], sys[0]]).toEqual(["O", "O", "A"]);
    expect(identity === signing).toBe(false);
    expect(operator.claims.iss).toBe(identity);
    expect(operator.claims.sub).toBe(identity);
    expect(operator.claims.name).toBe("fixture-operator");
    expect(operator.claims.nats).toMatchObject({ signing_keys: [signing], system_account: sys, type: "operator", version: 2 });
    expect(operator.verifiesWith(identity)).toBe(true);
    const sysJwt = decodeJwt(c.systemAccountJwt);
    expect(sysJwt.claims.sub).toBe(sys);
    expect(sysJwt.claims.name).toBe("SYS");
    expect(sysJwt.claims.iss).toBe(signing);
    expect(sysJwt.claims.nats.type).toBe("account");
    expect(sysJwt.claims.nats.version).toBe(2);
    expect(sysJwt.verifiesWith(signing)).toBe(true);
    // SYS JetStream is disabled, not an org quota bypass; Go connection defaults are explicit; no scoped user keys.
    expect(sysJwt.claims.nats.limits).toMatchObject({ mem_storage: 0, disk_storage: 0, conn: -1, leaf: -1, subs: -1, data: -1, payload: -1 });
    expect(sysJwt.claims.nats.signing_keys ?? []).toEqual([]);
    expect(sysJwt.claims.nats.exports ?? []).toEqual([]);
  });

  it("prints only public keys, names and paths on stdout", () => {
    requireImplementation();
    const root = tempRoot();
    const dir = path.join(root, "op");
    const result = nats(["mint", "operator", "--out-dir", dir], JSON.stringify({ operatorName: "fixture-operator" }));
    expect(result.status).toBe(0);
    const seeds = ["operator.nk", "operator-signing.nk", "sys-account.nk"].map((file) => readTrim(path.join(dir, file)));
    const out = publicJson(result, seeds);
    expect(out.ok).toBe(true);
    expect(out.kind).toBe("operator");
    expect(out.outDir).toBe(dir);
    expect(out.operator).toEqual({ name: "fixture-operator", publicKey: seedOwner(path.join(dir, "operator.nk")) });
    expect(out.operatorSigningKey).toEqual({ publicKey: seedOwner(path.join(dir, "operator-signing.nk")) });
    expect(out.systemAccount).toEqual({ name: "SYS", publicKey: seedOwner(path.join(dir, "sys-account.nk")) });
    expect([...out.files].sort()).toEqual(["operator-signing.nk", "operator.jwt", "operator.nk", "sys-account.jwt", "sys-account.nk"]);
  });

  it("refuses an existing directory, a symlinked directory, a missing parent and invalid input, writing nothing", () => {
    requireImplementation();
    const root = tempRoot();
    const existing = path.join(root, "existing");
    fs.mkdirSync(existing);
    const target = path.join(root, "target");
    fs.mkdirSync(target);
    const link = path.join(root, "link");
    fs.symlinkSync(target, link);
    const input = JSON.stringify({ operatorName: "fixture-operator" });
    for (const dir of [existing, link, path.join(root, "missing", "op")]) expectRefused(nats(["mint", "operator", "--out-dir", dir], input), 1);
    expect(fs.readdirSync(existing)).toEqual([]);
    expect(fs.readdirSync(target)).toEqual([]);
    const bad = ["", "not json", "{}", "[]", JSON.stringify({ operatorName: "Bad Name" }), JSON.stringify({ operatorName: "ok\n" })];
    bad.push(JSON.stringify({ operatorName: "ok", extra: 1 }));
    for (const value of bad) {
      expectRefused(nats(["mint", "operator", "--out-dir", path.join(root, "fresh")], value), 1);
      expect(fs.existsSync(path.join(root, "fresh"))).toBe(false);
    }
  });
});

posixMintDescribe("#7504 pi-fabric nats mint account", () => {
  it("issues one org account signed by the operator signing key with four separate scoped role keys", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const files = fs.readdirSync(c.accountDir).sort();
    expect(files).toEqual([
      "account.jwt",
      "account.nk",
      "signing-agent.nk",
      "signing-break-glass.nk",
      "signing-maintainer.nk",
      "signing-principal.nk",
    ]);
    if (posix) {
      expect(mode(c.accountDir)).toBe(0o700);
      for (const file of files) expect(mode(path.join(c.accountDir, file)), file).toBe(0o600);
    }
    const account = decodeJwt(c.accountJwt);
    const operatorSigning = seedOwner(path.join(c.operatorDir, "operator-signing.nk"));
    expect(account.claims.iss).toBe(operatorSigning);
    expect(account.verifiesWith(operatorSigning)).toBe(true);
    expect(account.claims.sub).toBe(seedOwner(path.join(c.accountDir, "account.nk")));
    expect(account.claims.name).toBe("fixture-org");
    expect(account.claims.nats).toMatchObject({ type: "account", version: 2 });
    expect(account.claims.nats.imports ?? []).toEqual([]);
    expect(account.claims.nats.exports ?? []).toEqual([]);
    // Caller quotas exactly, memory 0 not raised; Go non-JetStream defaults explicit -1.
    expect(account.claims.nats.limits).toMatchObject({
      mem_storage: 0,
      disk_storage: JETSTREAM.diskStorage,
      streams: JETSTREAM.streams,
      consumer: JETSTREAM.consumers,
      conn: -1,
      leaf: -1,
      subs: -1,
      data: -1,
      payload: -1,
    });
    const keys = account.claims.nats.signing_keys as Array<Record<string, any>>;
    expect(keys.length).toBe(4);
    expect(new Set(keys.map((entry) => entry.key)).size).toBe(4);
    expect(keys.map((entry) => entry.key).sort()).toEqual(Object.values(c.signing).map((entry) => entry.publicKey).sort());
    for (const role of ORG_ROLES) {
      const entry = keys.find((key) => key.key === c.signing[role].publicKey) as Record<string, any>;
      expect(entry.kind, role).toBe("user_scope");
      expect(entry.role, role).toBe(role);
      const deny = role === "break-glass" ? EVERY_ROLE_DENY : NORMAL_ROLE_DENY;
      expect([...entry.template.pub.deny].sort(), role).toEqual([...deny].sort());
      expect(entry.template.sub.allow, role).toEqual(["_INBOX.{{subject()}}.>"]);
      expect([entry.template.subs, entry.template.data, entry.template.payload], role).toEqual([-1, -1, -1]);
      expect((entry.template.pub.allow as string[]).some((subject) => subject.includes("PURGE") && role !== "maintainer"), role).toBe(false);
    }
    const byRole = (role: OrgRole) => keys.find((key) => key.role === role) as Record<string, any>;
    expect(byRole("agent").template.pub.allow.includes("FABRIC_MESH_{{tag(mesh)}}.state")).toBe(true);
    expect([...byRole("break-glass").template.pub.allow].sort()).toEqual([...BREAK_GLASS_ALLOW].sort());
  });

  it("prints only the account and the four scoped public keys", () => {
    requireImplementation();
    const root = tempRoot();
    const opDir = path.join(root, "op");
    const op = bootstrapOperator(opDir);
    const dir = path.join(root, "acct");
    const result = nats(
      ["mint", "account", "--out-dir", dir],
      JSON.stringify({ name: "fixture-org", operatorJwt: op.operatorJwt, operatorSigningSeed: op.operatorSigningSeed, jetstream: JETSTREAM, ...STREAMS }),
    );
    expect(result.status).toBe(0);
    const seedFiles = ["account.nk", ...ORG_ROLES.map((role) => `signing-${role}.nk`)];
    const seeds = seedFiles.map((file) => readTrim(path.join(dir, file)));
    const out = publicJson(result, [op.operatorSigningSeed, op.operatorIdentitySeed, ...seeds]);
    expect(out.ok).toBe(true);
    expect(out.kind).toBe("account");
    expect(out.outDir).toBe(dir);
    expect(out.account).toEqual({ name: "fixture-org", publicKey: seedOwner(path.join(dir, "account.nk")) });
    expect(out.issuer).toBe(seedOwner(path.join(opDir, "operator-signing.nk")));
    expect(out.signingKeys).toEqual(Object.fromEntries(ORG_ROLES.map((role) => [role, seedOwner(path.join(dir, `signing-${role}.nk`))])));
  });

  it("refuses a wrong-issuer seed, unlimited/missing/non-integer quotas, bad stream names and the SYS name before writing", { timeout: 90_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const strangerOperator = text.decode(nkeys.createOperator().getSeed());
    const base = { name: "fixture-org-2", operatorJwt: c.operatorJwt, operatorSigningSeed: c.operatorSigningSeed, jetstream: JETSTREAM, ...STREAMS };
    const bad: Array<[string, unknown]> = [
      ["operator identity seed is not a listed signing key", { ...base, operatorSigningSeed: c.operatorIdentitySeed }],
      ["stranger operator seed", { ...base, operatorSigningSeed: strangerOperator }],
      ["other operator's signing seed", { ...base, operatorSigningSeed: c.stranger.operatorSigningSeed }],
      ["account seed as operator signer", { ...base, operatorSigningSeed: c.accountSeed }],
      ["tampered operator jwt", { ...base, operatorJwt: `${c.operatorJwt.slice(0, -4)}AAAA` }],
      ["account jwt as operator jwt", { ...base, operatorJwt: c.accountJwt }],
      ["missing jetstream", { ...base, jetstream: undefined }],
      ["unlimited disk", { ...base, jetstream: { ...JETSTREAM, diskStorage: -1 } }],
      ["unlimited memory", { ...base, jetstream: { ...JETSTREAM, memStorage: -1 } }],
      ["negative streams", { ...base, jetstream: { ...JETSTREAM, streams: -2 } }],
      ["fractional streams", { ...base, jetstream: { ...JETSTREAM, streams: 1.5 } }],
      ["string consumers", { ...base, jetstream: { ...JETSTREAM, consumers: "256" } }],
      ["null (non-finite in JSON) disk", { ...base, jetstream: { ...JETSTREAM, diskStorage: null } }],
      ["unsafe integer disk", { ...base, jetstream: { ...JETSTREAM, diskStorage: 2 ** 60 } }],
      ["missing consumers", { ...base, jetstream: { memStorage: 0, diskStorage: 1, streams: 1 } }],
      ["unknown jetstream field", { ...base, jetstream: { ...JETSTREAM, maxAckPending: 1 } }],
      ["custody equals liveness", { ...base, livenessStream: STREAMS.custodyStream }],
      ["wildcard custody", { ...base, custodyStream: "*" }],
      ["mesh-root custody", { ...base, custodyStream: `FABRIC_MESH_${MESH_HEX}` }],
      ["missing liveness", { ...base, livenessStream: undefined }],
      ["SYS name", { ...base, name: "SYS" }],
      ["lowercase sys name", { ...base, name: "sys" }],
      ["uppercase name", { ...base, name: "Fixture" }],
      ["name with trailing LF", { ...base, name: "fixture-org-2\n" }],
      ["name with trailing U+2028", { ...base, name: "fixture-org-2\u2028" }],
      ["unknown field", { ...base, accountSeed: c.accountSeed }],
    ];
    for (const [index, [label, input]] of bad.entries()) {
      const dir = path.join(c.root, `refused-${index}`);
      expectRefusedBeforeWrite(label, nats(["mint", "account", "--out-dir", dir], JSON.stringify(input)), dir, [...c.secrets, strangerOperator]);
    }
  });

  it("accepts disk 0 as a finite caller quota and does not raise it", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const dir = path.join(c.root, "account-disk-zero");
    const input = { name: "fixture-org-zero", operatorJwt: c.operatorJwt, operatorSigningSeed: c.operatorSigningSeed, ...STREAMS };
    const result = nats(["mint", "account", "--out-dir", dir], JSON.stringify({ ...input, jetstream: { ...JETSTREAM, diskStorage: 0 } }));
    expect(result.status).toBe(0);
    const limits = decodeJwt(readTrim(path.join(dir, "account.jwt"))).claims.nats.limits;
    expect(limits).toMatchObject({ mem_storage: 0, disk_storage: 0, streams: JETSTREAM.streams, consumer: JETSTREAM.consumers });
  });

  it("never echoes stdin when it is not valid JSON or too large", () => {
    requireImplementation();
    const root = tempRoot();
    const seed = text.decode(nkeys.createOperator().getSeed());
    for (const input of [`{"operatorSigningSeed":"${seed}"`, `${seed}`, `{"name":"x","operatorSigningSeed":"${seed}",}`, "x".repeat(2 * 1024 * 1024)]) {
      const result = nats(["mint", "account", "--out-dir", path.join(root, "acct")], input);
      expectRefused(result, 1, [seed]);
      expect(fs.existsSync(path.join(root, "acct"))).toBe(false);
    }
  });
});

posixMintDescribe("#7504 pi-fabric nats mint user (normal roles)", () => {
  it("mints a fresh scoped agent user with a 7-day exp into a 0600 .creds and prints only public metadata", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const dir = path.join(c.root, "agent-1");
    const before = Math.floor(Date.now() / 1000);
    const result = nats(["mint", "user", "--out-dir", dir], JSON.stringify(agentInput(c, { meshes: [MESH_HEX, OTHER_HEX], parent: "session-0000" })));
    const after = Math.floor(Date.now() / 1000);
    expect(result.status).toBe(0);
    const { userPublicKey, jwt, seedText, user } = expectUserArtifact(dir);
    const out = publicJson(result, [...c.secrets, seedText, jwt]);
    expect(user.claims.sub).toBe(userPublicKey);
    expect(user.claims.iss).toBe(c.signing.agent.publicKey);
    expect(user.verifiesWith(c.signing.agent.publicKey)).toBe(true);
    expect(user.claims.name).toBe("agent/fixture-host/fixture-lane/session-0001");
    expect(Object.keys(user.claims.nats)).toEqual(["issuer_account", "tags", "type", "version"]);
    expect(user.claims.nats.issuer_account).toBe(c.accountPublicKey);
    expect([...user.claims.nats.tags].sort()).toEqual(
      [
        "host:fixture-host",
        "lane:fixture-lane",
        "session:session-0001",
        "release:fixture-release",
        "role:agent",
        `mesh:${MESH_HEX}`,
        `mesh:${OTHER_HEX}`,
        "parent:session-0000",
      ].sort(),
    );
    expect(user.claims.exp - user.claims.iat).toBe(7 * DAY + 60);
    expect(user.claims.exp >= before + 7 * DAY && user.claims.exp <= after + 7 * DAY).toBe(true);
    // Per-key rotation inputs: public key, role, verified A signer, iat/exp and the JWT digest; never the JWT itself.
    expect(out).toMatchObject({
      ok: true,
      kind: "user",
      outDir: dir,
      files: ["user.creds"],
      role: "agent",
      name: user.claims.name,
      publicKey: userPublicKey,
      account: c.accountPublicKey,
      org: "fixture-org",
      signer: c.signing.agent.publicKey,
      issuer: c.signing.agent.publicKey,
      issuedAt: user.claims.iat,
      expiresAt: user.claims.exp,
      jwtSha256: sha256(jwt),
    });
  });

  it("gives every mint a new user key and jti, never reusing the previous one", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const minted = ["fresh-a", "fresh-b"].map((name) => {
      const dir = path.join(c.root, name);
      expect(nats(["mint", "user", "--out-dir", dir], JSON.stringify(agentInput(c))).status).toBe(0);
      const { userPublicKey, jwt } = readCreds(dir);
      return { userPublicKey, jti: decodeJwt(jwt).claims.jti as string };
    });
    expect(minted[0]!.userPublicKey === minted[1]!.userPublicKey).toBe(false);
    expect(minted[0]!.jti === minted[1]!.jti).toBe(false);
  });

  it("mints unbound org-read principals for 90 days and maintainers for 1 h compaction / 2 h migration", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const common = orgCommon(c);
    const cases = [
      { label: "principal", input: { ...common, role: "principal", signingSeed: c.signing.principal.seed, person: "paul", device: "m5" }, name: "principal/paul/m5", lifetime: 90 * DAY },
      { label: "compaction", input: { ...common, role: "maintainer", signingSeed: c.signing.maintainer.seed, job: "compaction", runId: "run-0001" }, name: "maintainer/fixture-org/compaction/run-0001", lifetime: 3_600 },
      { label: "migration", input: { ...common, role: "maintainer", signingSeed: c.signing.maintainer.seed, job: "migration", runId: "run-0002" }, name: "maintainer/fixture-org/migration/run-0002", lifetime: 7_200 },
    ];
    for (const { label, input, name, lifetime } of cases) {
      const dir = path.join(c.root, `user-${label}`);
      const result = nats(["mint", "user", "--out-dir", dir], JSON.stringify(input));
      expect(result.status, label).toBe(0);
      const { jwt, seedText, user } = expectUserArtifact(dir);
      const out = publicJson(result, [...c.secrets, jwt, seedText]);
      const role = input.role as "principal" | "maintainer";
      expect(user.claims.iss, label).toBe(c.signing[role].publicKey);
      expect(user.claims.name, label).toBe(name);
      expect(user.claims.exp - user.claims.iat, label).toBe(lifetime + 60);
      expect((user.claims.nats.tags as string[]).includes(`role:${role}`), label).toBe(true);
      expect((user.claims.nats.tags as string[]).some((tag) => tag.startsWith("mesh:")), `${label}: deliberately no mesh binding`).toBe(false);
      expect(Object.keys(user.claims.nats), label).toEqual(["issuer_account", "tags", "type", "version"]);
      expect(out, label).toMatchObject({ role, signer: c.signing[role].publicKey, expiresAt: user.claims.exp, issuedAt: user.claims.iat, jwtSha256: sha256(jwt) });
    }
  });

  it("refuses wrong-role, unscoped, foreign and stale signers, chain/account mismatch and malformed role input before writing", { timeout: 120_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const stranger = text.decode(nkeys.createAccount().getSeed());
    const userSeed = text.decode(nkeys.createUser().getSeed());
    const otherAccount = nkeys.createAccount().getPublicKey();
    const common = orgCommon(c);
    const bad: Array<[string, Record<string, unknown>]> = [
      ["principal key for agent role", agentInput(c, { signingSeed: c.signing.principal.seed })],
      ["maintainer key for agent role", agentInput(c, { signingSeed: c.signing.maintainer.seed })],
      ["break-glass key for agent role", agentInput(c, { signingSeed: c.signing["break-glass"].seed })],
      ["break-glass role without --break-glass", { ...breakGlassInput(c), role: "break-glass" }],
      ["account identity key is not scoped", agentInput(c, { signingSeed: c.accountSeed })],
      ["operator signing seed", agentInput(c, { signingSeed: c.operatorSigningSeed })],
      ["SYS account seed", agentInput(c, { signingSeed: c.systemAccountSeed })],
      ["user seed", agentInput(c, { signingSeed: userSeed })],
      ["foreign account key", agentInput(c, { signingSeed: stranger })],
      ["account public key mismatch", agentInput(c, { accountPublicKey: otherAccount })],
      ["operator jwt instead of account jwt", agentInput(c, { accountJwt: c.operatorJwt })],
      ["SYS account jwt as org account", agentInput(c, { accountJwt: c.systemAccountJwt, accountPublicKey: decodeJwt(c.systemAccountJwt).claims.sub })],
      ["tampered account jwt", agentInput(c, { accountJwt: `${c.accountJwt.slice(0, -4)}AAAA` })],
      ["foreign operator jwt", agentInput(c, { operatorJwt: c.stranger.operatorJwt })],
      ["missing operator jwt", agentInput(c, { operatorJwt: undefined })],
      ["stale template (other custody)", agentInput(c, { custodyStream: "FIXTURE_CUSTODY_OLD" })],
      ["stale template (other liveness)", agentInput(c, { livenessStream: "KV_FIXTURE_LIVENESS_OLD" })],
      ["no mesh", agentInput(c, { meshes: [] })],
      ["uppercase mesh", agentInput(c, { meshes: [MESH_HEX.toUpperCase()] })],
      ["short mesh", agentInput(c, { meshes: [MESH_HEX.slice(1)] })],
      ["mesh with trailing LF", agentInput(c, { meshes: [`${MESH_HEX}\n`] })],
      ["wildcard mesh", agentInput(c, { meshes: [">"] })],
      ["duplicate mesh", agentInput(c, { meshes: [MESH_HEX, MESH_HEX] })],
      ["host with slash", agentInput(c, { host: "a/b" })],
      ["uppercase lane", agentInput(c, { lane: "Lane" })],
      ["session with trailing CR", agentInput(c, { session: "session-0001\r" })],
      ["expiry override", agentInput(c, { expiresAt: 4_102_444_800 })],
      ["lifetime override", agentInput(c, { lifetimeSeconds: 60 })],
      ["permission override", agentInput(c, { pub: { allow: [">"] } })],
      ["unknown role", agentInput(c, { role: "sys" })],
      ["principal with mesh", { ...common, role: "principal", signingSeed: c.signing.principal.seed, person: "paul", device: "m5", meshes: [MESH_HEX] }],
      ["maintainer provision job", { ...common, role: "maintainer", signingSeed: c.signing.maintainer.seed, job: "provision", runId: "r1" }],
    ];
    for (const [index, [label, input]] of bad.entries()) {
      const dir = path.join(c.root, `user-refused-${index}`);
      expectRefusedBeforeWrite(label, nats(["mint", "user", "--out-dir", dir], JSON.stringify(input)), dir, [...c.secrets, stranger, userSeed]);
    }
  });

  it("refuses to write into an existing directory even when the mint input is valid", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const dir = path.join(c.root, "user-existing");
    fs.mkdirSync(dir);
    const result = nats(["mint", "user", "--out-dir", dir], JSON.stringify(agentInput(c)));
    expectRefused(result, 1, c.secrets);
    expect(fs.readdirSync(dir)).toEqual([]);
  });
});

posixMintDescribe("#7504 pi-fabric nats mint user --break-glass", () => {
  it("mints a 1 h break-glass user only through the explicit flag, with public audit metadata and digest only", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const dir = path.join(c.root, "break-glass-1");
    const result = nats(["mint", "user", "--break-glass", "--out-dir", dir], JSON.stringify(breakGlassInput(c)));
    expect(result.status).toBe(0);
    const { userPublicKey, jwt, seedText, user } = expectUserArtifact(dir);
    const out = publicJson(result, [...c.secrets, seedText, jwt]);
    const signer = c.signing["break-glass"].publicKey;
    expect(user.claims.iss).toBe(signer);
    expect(user.verifiesWith(signer)).toBe(true);
    expect(user.claims.name).toBe("break-glass/fixture-org/paul/run-0003");
    expect(Object.keys(user.claims.nats)).toEqual(["issuer_account", "tags", "type", "version"]);
    expect(user.claims.nats.issuer_account).toBe(c.accountPublicKey);
    expect((user.claims.nats.tags as string[]).includes("role:break-glass")).toBe(true);
    expect((user.claims.nats.tags as string[]).some((tag) => tag.startsWith("mesh:"))).toBe(false);
    expect(user.claims.exp - user.claims.iat).toBe(3_600 + 60);
    expect(out).toMatchObject({
      ok: true,
      kind: "user",
      outDir: dir,
      files: ["user.creds"],
      role: "break-glass",
      name: user.claims.name,
      publicKey: userPublicKey,
      account: c.accountPublicKey,
      org: "fixture-org",
      signer,
      issuer: signer,
      issuedAt: user.claims.iat,
      expiresAt: user.claims.exp,
      jwtSha256: sha256(jwt),
      audit: { person: "paul", runId: "run-0003", approvalRef: APPROVAL },
    });
  });

  it("refuses normal-role keys, missing or secret-shaped approval references, role/lifetime fields and a foreign chain", { timeout: 90_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const bad: Array<[string, Record<string, unknown>]> = [
      ["agent key", breakGlassInput(c, { signingSeed: c.signing.agent.seed })],
      ["maintainer key", breakGlassInput(c, { signingSeed: c.signing.maintainer.seed })],
      ["account identity key", breakGlassInput(c, { signingSeed: c.accountSeed })],
      ["missing approval reference", breakGlassInput(c, { approvalRef: undefined })],
      ["empty approval reference", breakGlassInput(c, { approvalRef: "" })],
      ["seed-shaped approval reference", breakGlassInput(c, { approvalRef: c.signing.agent.seed })],
      ["approval reference with trailing LF", breakGlassInput(c, { approvalRef: `${APPROVAL}\n` })],
      ["missing person", breakGlassInput(c, { person: undefined })],
      ["missing run id", breakGlassInput(c, { runId: undefined })],
      ["role field", breakGlassInput(c, { role: "break-glass" })],
      ["lifetime override", breakGlassInput(c, { lifetimeSeconds: 86_400 })],
      ["foreign operator jwt", breakGlassInput(c, { operatorJwt: c.stranger.operatorJwt })],
      ["stale template (other custody)", breakGlassInput(c, { custodyStream: "FIXTURE_CUSTODY_OLD" })],
      ["agent role input", agentInput(c)],
    ];
    for (const [index, [label, input]] of bad.entries()) {
      const dir = path.join(c.root, `break-glass-refused-${index}`);
      expectRefusedBeforeWrite(label, nats(["mint", "user", "--break-glass", "--out-dir", dir], JSON.stringify(input)), dir, c.secrets);
    }
  });
});

posixMintDescribe("#7504 pi-fabric nats mint user --sys", () => {
  it("mints a 1 h SYS-only user signed by the operator-bound SYS identity: CLAIMS.UPDATE publish and own inbox only", { timeout: 60_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const dir = path.join(c.root, "sys-1");
    const result = nats(["mint", "user", "--sys", "--out-dir", dir], JSON.stringify(sysInput(c)));
    expect(result.status).toBe(0);
    const { userPublicKey, jwt, seedText, user } = expectUserArtifact(dir);
    const out = publicJson(result, [...c.secrets, seedText, jwt]);
    const sys = decodeJwt(c.operatorJwt).claims.nats.system_account as string;
    expect(sys).toBe(decodeJwt(c.systemAccountJwt).claims.sub);
    expect(user.claims.iss).toBe(sys);
    expect(user.verifiesWith(sys)).toBe(true);
    expect(user.claims.nats.issuer_account).toBeUndefined();
    expect(user.claims.nats.type).toBe("user");
    expect(user.claims.nats.version).toBe(2);
    expect(user.claims.nats.pub.allow).toEqual(["$SYS.REQ.CLAIMS.UPDATE"]);
    expect(user.claims.nats.pub.deny ?? []).toEqual([]);
    expect(user.claims.nats.sub.allow).toEqual([`_INBOX.${userPublicKey}.>`]);
    expect(user.claims.nats.sub.deny ?? []).toEqual([]);
    expect([user.claims.nats.subs, user.claims.nats.data, user.claims.nats.payload]).toEqual([-1, -1, -1]);
    expect(user.claims.exp - user.claims.iat).toBe(3_600 + 60);
    expect(out).toMatchObject({
      ok: true,
      kind: "user",
      outDir: dir,
      files: ["user.creds"],
      role: "sys",
      name: user.claims.name,
      publicKey: userPublicKey,
      account: sys,
      signer: sys,
      issuer: sys,
      issuedAt: user.claims.iat,
      expiresAt: user.claims.exp,
      jwtSha256: sha256(jwt),
    });
  });

  it("refuses an org account, a scoped or mismatched seed, a foreign operator binding, role and permission fields", { timeout: 90_000 }, async () => {
    requireImplementation();
    const c = await chain();
    const bad: Array<[string, Record<string, unknown>]> = [
      ["org account jwt and seed", sysInput(c, { systemAccountJwt: c.accountJwt, systemAccountSeed: c.accountSeed })],
      ["org account seed for SYS jwt", sysInput(c, { systemAccountSeed: c.accountSeed })],
      ["scoped agent seed for SYS jwt", sysInput(c, { systemAccountSeed: c.signing.agent.seed })],
      ["operator signing seed", sysInput(c, { systemAccountSeed: c.operatorSigningSeed })],
      ["foreign operator jwt", sysInput(c, { operatorJwt: c.stranger.operatorJwt })],
      ["foreign SYS account under this operator", sysInput(c, { systemAccountJwt: c.stranger.systemAccountJwt, systemAccountSeed: c.stranger.systemAccountSeed })],
      ["tampered SYS jwt", sysInput(c, { systemAccountJwt: `${c.systemAccountJwt.slice(0, -4)}AAAA` })],
      ["role field", sysInput(c, { role: "sys" })],
      ["permission field", sysInput(c, { pub: { allow: ["$SYS.REQ.>"] } })],
      ["lifetime override", sysInput(c, { lifetimeSeconds: 86_400 })],
      ["missing person", sysInput(c, { person: undefined })],
      ["org agent input", agentInput(c)],
    ];
    for (const [index, [label, input]] of bad.entries()) {
      const dir = path.join(c.root, `sys-refused-${index}`);
      expectRefusedBeforeWrite(label, nats(["mint", "user", "--sys", "--out-dir", dir], JSON.stringify(input)), dir, c.secrets);
    }
  });
});

describe("#7504 pi-fabric nats config", () => {
  it("renders public config input from stdin into a new 0600 file and refuses an existing output", { timeout: 60_000 }, () => {
    requireImplementation();
    const bootstrap = createNatsOperator({ operatorName: "fixture-config" });
    try {
      const operatorJwt = text.decode(bootstrap.artifacts.find((artifact) => artifact.name === "operator.jwt")!.bytes).trim();
      const systemAccountJwt = text.decode(bootstrap.artifacts.find((artifact) => artifact.name === "sys-account.jwt")!.bytes).trim();
      const sys = bootstrap.metadata.systemAccount.publicKey;
      const input = {
        bus: { nodes: 1, replicas: 1 },
        serverName: "fabric-local",
        listen: { host: "127.0.0.1", port: 4222 },
        jetstream: { storeDir: "/tmp/pi-fabric-nats-cli-test-js" },
        operatorJwt,
        systemAccount: sys,
        resolver: { dir: "/tmp/pi-fabric-nats-cli-test-jwt" },
        preload: { [sys]: systemAccountJwt },
      };
      const root = tempRoot();
      const out = path.join(root, "nats-server.conf");
      const result = nats(["config", "--input", "-", "--out", out], JSON.stringify(input));
      expect(result.status).toBe(0);
      const printed = publicJson(result);
      expect(printed).toMatchObject({ ok: true, kind: "config", out });
      if (posix) expect(mode(out)).toBe(0o600);
      const rendered = fs.readFileSync(out, "utf8");
      expect(rendered.includes(sys)).toBe(true);
      expect(/resolver\s*[:{]/.test(rendered)).toBe(true);
      expect(leaks(rendered.replaceAll(operatorJwt, "").replaceAll(systemAccountJwt, "")), "config holds public JWTs only").toBe(false);
      const again = nats(["config", "--input", "-", "--out", out], JSON.stringify(input));
      expectRefused(again, 1, []);
    } finally {
      for (const artifact of bootstrap.artifacts) artifact.bytes.fill(0);
    }
  });

  it("refuses invalid config input by field name without echoing values", () => {
    requireImplementation();
    const root = tempRoot();
    const seed = text.decode(nkeys.createOperator().getSeed());
    const out = path.join(root, "nats-server.conf");
    for (const input of ["not json", JSON.stringify({ serverName: seed }), JSON.stringify({ bus: { nodes: 2, replicas: 3 } })]) {
      const result = nats(["config", "--input", "-", "--out", out], input);
      expectRefused(result, 1, [seed]);
      expect(fs.existsSync(out)).toBe(false);
    }
  });
});
