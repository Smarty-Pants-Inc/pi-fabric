import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, expect, it, type TestContext } from "vitest";
import { ownProcessIncarnation, readPhysicalHostIdentity, readPhysicalMachineId } from "../src/core/atomic-write.js";
import { ownPidNamespace } from "../src/core/pid-namespace.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

type Kind = "mesh" | "custody";
type OwnerField = "token" | "pid" | "createdAt" | "incarnation" | "machineId" | "bootId" | "namespace";
type Scenario = "wrong-token" | "live-refusal" | "foreign-refusal" | "safe-success" | "audit-refusal" | "rename-error";
type Reachability = "token-mismatch" | "invalid-owner-pid" | "unproven-namespace" | "legacy-unproven" |
  "live-native-holder" | "foreign-namespace" | "safe-stage";

interface Layout {
  readonly id: string;
  readonly kind: Kind;
  /** Meaningful wire lines in order; the terminal LF adds the final split field. */
  readonly fields: readonly OwnerField[];
  readonly splitLength: number;
}

const layouts: readonly Layout[] = [
  { id: "mesh-legacy-4", kind: "mesh", fields: ["token", "pid", "createdAt"], splitLength: 4 },
  { id: "mesh-legacy-5", kind: "mesh", fields: ["token", "pid", "createdAt", "incarnation"], splitLength: 5 },
  { id: "mesh-modern-6", kind: "mesh", fields: ["token", "pid", "createdAt", "incarnation", "namespace"], splitLength: 6 },
  { id: "custody-legacy-4", kind: "custody", fields: ["token", "pid", "createdAt"], splitLength: 4 },
  { id: "custody-legacy-5", kind: "custody", fields: ["token", "pid", "createdAt", "incarnation"], splitLength: 5 },
  { id: "custody-modern-6", kind: "custody", fields: ["token", "pid", "createdAt", "incarnation", "namespace"], splitLength: 6 },
  { id: "custody-host-legacy-7", kind: "custody", fields: ["token", "pid", "createdAt", "incarnation", "machineId", "bootId"], splitLength: 7 },
  { id: "custody-host-modern-8", kind: "custody", fields: ["token", "pid", "createdAt", "incarnation", "machineId", "bootId", "namespace"], splitLength: 8 },
];

const TOKEN = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_TOKEN = "ABCDEF0123456789abcdef0123456789";
const AUDIT_SENTINEL = "R6-AUDIT-REFUSAL-SENTINEL\n";
const LOCAL_LINUX_FILESYSTEMS = new Set([0xEF53, 0x58465342, 0x9123683E, 0x01021994, 0x2FC12FC1, 0xF2F52010]);
const PACKAGE_ROOT = process.cwd();
const packageJson = JSON.parse(fs.readFileSync(path.join(PACKAGE_ROOT, "package.json"), "utf8")) as {
  bin: Record<string, string>;
};
const publicShim = path.resolve(PACKAGE_ROOT, packageJson.bin["fabric-mesh-backend"]!);
const builtBundle = path.resolve(PACKAGE_ROOT, "dist/mesh/mesh-backend-cli.js");
const roots: string[] = [];
let nativeNamespace: string | undefined;
let nativeIncarnation: string | undefined;
let nativeHost: { machineId: string; bootId: string } | undefined;
let exitedPid: string | undefined;

beforeAll(async () => {
  // No artifact skip: this suite must exercise the package's actual integrity-checking public shim.
  expect(fs.existsSync(publicShim)).toBe(true);
  expect(fs.existsSync(builtBundle)).toBe(true);
  nativeNamespace = await ownPidNamespace();
  nativeIncarnation = await ownProcessIncarnation().catch(() => undefined);
  nativeHost = readPhysicalHostIdentity();
  const exited = spawnSync(process.execPath, ["-e", "process.exit(0)"], { encoding: "utf8", timeout: 5_000 });
  expect(exited.error).toBeUndefined();
  expect(exited.status).toBe(0);
  expect(exited.pid).toBeGreaterThan(0);
  exitedPid = String(exited.pid);
});

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-recover-owner-matrix-"));
  fs.chmodSync(root, 0o700);
  roots.push(root);
  return root;
};

const baseValues = (): Record<OwnerField, string> => ({
  token: TOKEN,
  pid: String(process.pid),
  createdAt: String(Date.now()),
  incarnation: nativeIncarnation ?? "",
  machineId: nativeHost?.machineId ?? readPhysicalMachineId() ?? "",
  bootId: nativeHost?.bootId ?? "",
  namespace: nativeNamespace ?? "linux:0123456789abcdef0123456789abcdef:11111111-1111-4111-8111-111111111111:4026531836",
});

const fixture = (layout: Layout, values: Record<OwnerField, string>) => {
  const root = tempRoot();
  const lock = path.join(root, layout.kind === "mesh" ? ".lock" : "custody.lock");
  const ownerPath = path.join(lock, "owner");
  fs.mkdirSync(lock, { mode: 0o700 });
  const record = `${layout.fields.map(field => values[field]).join("\n")}\n`;
  expect(record.split("\n")).toHaveLength(layout.splitLength);
  expect(layout.fields.every(field => !values[field].includes("\n"))).toBe(true);
  fs.writeFileSync(ownerPath, record, { encoding: "utf8", mode: 0o600 });
  return { root, lock, ownerPath, record, values };
};

const attackValue = (layout: Layout, field: OwnerField): string =>
  `R6_OWNER_SECRET_${layout.id}_${field}_１２３\r\x1b[31m;$(echo R6_${layout.id}_${field})|'"`;

const overlongForeignNamespace = (): string => {
  const ownParts = nativeNamespace?.split(":") ?? [];
  const ownMachine = ownParts[1];
  const machine = ownMachine === "fedcba9876543210fedcba9876543210"
    ? "0123456789abcdef0123456789abcdef" : "fedcba9876543210fedcba9876543210";
  const boot = /^[0-9a-f-]{36}$/i.test(ownParts[2] ?? "")
    ? ownParts[2]! : "22222222-2222-4222-8222-222222222222";
  return `linux:${machine}:${boot}:${"9".repeat(160)}`;
};

const reachability = (layout: Layout, field: OwnerField, scenario: Scenario): Reachability => {
  if (scenario === "wrong-token") return field === "pid" ? "invalid-owner-pid" : "token-mismatch";
  if (field === "pid") return "invalid-owner-pid";
  if (scenario === "live-refusal") {
    if (field === "namespace") return "unproven-namespace";
    return layout.fields.includes("namespace") ? "live-native-holder" : "legacy-unproven";
  }
  if (scenario === "foreign-refusal") {
    return layout.fields.includes("namespace") ? "foreign-namespace" : "legacy-unproven";
  }
  if (!layout.fields.includes("namespace")) return "legacy-unproven";
  if (field === "namespace") return "unproven-namespace";
  return "safe-stage";
};

const matrixValues = (layout: Layout, field: OwnerField, scenario: Scenario): Record<OwnerField, string> => {
  const values = baseValues();
  if (scenario === "live-refusal") {
    values.pid = String(process.pid);
    values.namespace = nativeNamespace ?? values.namespace;
  } else if (scenario === "foreign-refusal") {
    values.namespace = overlongForeignNamespace();
  } else if (scenario === "safe-success" || scenario === "audit-refusal" || scenario === "rename-error") {
    values.pid = exitedPid ?? "0";
    values.namespace = nativeNamespace ?? values.namespace;
  }
  if (scenario === "foreign-refusal" && field === "namespace") values.namespace = overlongForeignNamespace();
  else values[field] = attackValue(layout, field);
  return values;
};

const runPublicCli = (argv: string[]) => {
  const result = spawnSync(process.execPath, [publicShim, ...argv], {
    cwd: PACKAGE_ROOT,
    encoding: "utf8",
    timeout: 15_000,
    maxBuffer: 1024 * 1024,
  });
  expect(result.error).toBeUndefined();
  expect(result.signal).toBeNull();
  expect(result.status).not.toBeNull();
  expect(result.stderr).not.toContain("refused before loading");
  return { status: result.status!, stdout: result.stdout, stderr: result.stderr };
};

const nativeSafeRootOrSkip = (context: TestContext, root: string): void => {
  if (process.platform !== "linux") context.skip("native safe lock recovery requires Linux");
  if (!nativeNamespace) context.skip("native ownPidNamespace evidence is unavailable");
  if (!exitedPid || !Number.isSafeInteger(Number(exitedPid)) || Number(exitedPid) <= 0) {
    context.skip("an actually exited positive child PID is unavailable");
  }
  let type: number;
  try { type = Number(fs.statfsSync(root).type); }
  catch { context.skip("native filesystem type evidence is unavailable"); }
  if (!LOCAL_LINUX_FILESYSTEMS.has(type!)) {
    context.skip(`native safe PID recovery is unavailable on filesystem type ${type}`);
  }
};

const assertNoOwnerPayload = (result: { stdout: string; stderr: string }, rawValues: readonly string[]): void => {
  const surface = result.stdout + result.stderr;
  for (const raw of rawValues) expect(surface).not.toContain(raw);
  expect(surface).not.toContain("R6_OWNER_SECRET_");
  expect(surface).not.toContain("R6_");
  expect(surface).not.toContain("\r");
  expect(surface).not.toContain("\x1b");
  expect(surface).not.toContain("\\r");
  expect(surface).not.toContain("\\u001b");
};

const assertRefusalProtocol = (result: { status: number; stdout: string; stderr: string }, json: boolean,
  includes: string, rawValues: readonly string[]): string => {
  expect(result.status).toBe(3);
  const prefix = "fabric-mesh-backend: lock-recover refused: ";
  // The CI filesystem preload may add a harness banner. Locate the exact product line, while keeping
  // assertNoOwnerPayload below on the COMPLETE untouched stdout/stderr (including every prelude line).
  const diagnostics = result.stderr.split("\n").filter(line => line.startsWith(prefix));
  expect(diagnostics).toHaveLength(1);
  expect(result.stderr.endsWith("\n")).toBe(true);
  const diagnostic = diagnostics[0]!.slice(prefix.length);
  expect(diagnostic).toContain(includes);
  if (json) {
    const payload = JSON.parse(result.stdout) as { command: string; ok: boolean; refused: boolean; code: string; error: string };
    expect(payload).toMatchObject({ command: "lock-recover", ok: false, refused: true, code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
    expect(payload.error).toBe(diagnostic);
  } else expect(result.stdout).toBe("");
  assertNoOwnerPayload(result, rawValues);
  return diagnostic;
};

const exactFencePath = (lock: string, record: string): string => {
  const stat = fs.lstatSync(lock);
  const digest = createHash("sha256").update(`${stat.dev}:${stat.ino}:${record}`).digest("hex");
  return `${lock}.dead.${digest}`;
};

const auditLine = (root: string): { text: string; value: Record<string, unknown> } => {
  const text = fs.readFileSync(path.join(root, "lock-recoveries.jsonl"), "utf8");
  expect(text.endsWith("\n")).toBe(true);
  const lines = text.trimEnd().split("\n");
  expect(lines).toHaveLength(1);
  return { text: lines[0]!, value: JSON.parse(lines[0]!) as Record<string, unknown> };
};

const assertSafeAuditProjection = (audit: Record<string, unknown>, expectedToken: string, pid: number, namespace: string,
  lock: string, rawValues: readonly string[] = []): void => {
  expect(Object.keys(audit).sort()).toEqual(["at", "root", "lock", "mode", "holder", "holderPid", "holderNamespace", "verdict", "operator"].sort());
  expect(audit).toMatchObject({ mode: "safe", holder: expectedToken, holderPid: pid, holderNamespace: namespace, verdict: "pid", lock });
  expect(Number.isSafeInteger(audit.holderPid)).toBe(true);
  // Persisted safe audit bytes must be independently safe, even when a later rename fails and no result is printed.
  assertNoOwnerPayload({ stdout: JSON.stringify(audit), stderr: "" }, rawValues);
  const operator = audit.operator as Record<string, unknown>;
  expect(operator).toMatchObject({ pid: expect.any(Number), ppid: expect.any(Number), host: expect.any(String),
    user: expect.any(String), parentExecutable: expect.any(String) });
  expect(operator.pid as number).toBeGreaterThan(0);
  expect(operator.ppid as number).toBeGreaterThan(0);
};

const assertNoAuditOrFence = (root: string, lock: string, auditSentinel?: string, allowedCollision?: string): void => {
  expect(fs.existsSync(lock)).toBe(true);
  if (auditSentinel === undefined) expect(fs.existsSync(path.join(root, "lock-recoveries.jsonl"))).toBe(false);
  else expect(fs.readFileSync(path.join(root, "lock-recoveries.jsonl"), "utf8")).toBe(auditSentinel);
  const deadNames = fs.readdirSync(root).filter(name => name.includes(".dead."));
  expect(deadNames).toEqual(allowedCollision === undefined ? [] : [path.basename(allowedCollision)]);
};

const scenarios: readonly Scenario[] = ["wrong-token", "live-refusal", "foreign-refusal", "safe-success", "audit-refusal", "rename-error"];
const fieldCells = layouts.flatMap(layout => layout.fields.map(field => ({ layout, field })));

// One systematic Cartesian matrix: 37 owner-field/layout pairs × six outcomes × both public output modes = 444 cases.
for (const { layout, field } of fieldCells) {
  for (const scenario of scenarios) {
    for (const json of [false, true]) {
      const expectedReach = reachability(layout, field, scenario);
      it(`owner matrix ${layout.id}.${field} / ${scenario} / ${json ? "JSON" : "plain"} / ${expectedReach}`, async (context) => {
        const values = matrixValues(layout, field, scenario);
        const planted = fixture(layout, values);
        const rawMutation = values[field]!;
        const malformedOtherToken = field === "token" ? attackValue(layout, "token").replace("_token_", "_other-token_") : OTHER_TOKEN;
        const holder = scenario === "wrong-token" ? malformedOtherToken : values.token;
        const secrets = [rawMutation, ...(scenario === "foreign-refusal" ? [values.namespace] : []),
          ...(scenario === "wrong-token" && field === "token" ? [holder] : [])];
        const args = ["lock-recover", "--root", planted.root, "--lock", layout.kind, "--holder", holder,
          ...(json ? ["--json"] : [])];

        let auditSentinel: string | undefined;
        let collisionFence: string | undefined;
        const expectedFence = scenario === "safe-success" ? exactFencePath(planted.lock, planted.record) : undefined;
        if (scenario === "audit-refusal") {
          auditSentinel = AUDIT_SENTINEL;
          const audit = path.join(planted.root, "lock-recoveries.jsonl");
          fs.writeFileSync(audit, auditSentinel, { encoding: "utf8", mode: 0o600 });
          fs.chmodSync(audit, 0o666);
        }
        if (scenario === "rename-error") {
          collisionFence = exactFencePath(planted.lock, planted.record);
          fs.mkdirSync(collisionFence, { mode: 0o700 });
          fs.writeFileSync(path.join(collisionFence, "collision-sentinel"), `R6 collision ${layout.id}.${field}\n`, { mode: 0o600 });
        }
        if ((scenario === "live-refusal" && expectedReach === "live-native-holder") ||
          ((scenario === "safe-success" || scenario === "audit-refusal" || scenario === "rename-error") && expectedReach === "safe-stage")) {
          nativeSafeRootOrSkip(context, planted.root);
        }

        const result = runPublicCli(args);
        if (scenario === "wrong-token") {
          if (expectedReach === "invalid-owner-pid") {
            assertRefusalProtocol(result, json, "does not hold a complete owner record", secrets);
          } else {
            const expectedOwnerToken = field === "token" ? "invalid-token" : TOKEN;
            const expectedAskedToken = field === "token" ? "invalid-token" : OTHER_TOKEN;
            const expectedMessage = `FABRIC_MESH_LOCK_RECOVER_REFUSED: ${planted.lock} is held by ${expectedOwnerToken}, not ${expectedAskedToken}`;
            const diagnostic = assertRefusalProtocol(result, json, expectedMessage, secrets);
            expect(diagnostic).toBe(expectedMessage);
          }
          expect(Buffer.from(fs.readFileSync(planted.ownerPath))).toEqual(Buffer.from(planted.record));
          assertNoAuditOrFence(planted.root, planted.lock);
          return;
        }

        if (scenario === "live-refusal" || scenario === "foreign-refusal" || expectedReach !== "safe-stage") {
          let expectedDiagnostic: string;
          if (expectedReach === "invalid-owner-pid") expectedDiagnostic = "does not hold a complete owner record";
          else if (expectedReach === "legacy-unproven" || expectedReach === "unproven-namespace") {
            expectedDiagnostic = "a PID namespace this process cannot prove";
          } else if (expectedReach === "live-native-holder") expectedDiagnostic = `holder pid ${process.pid} is running here`;
          // A Windows namespace (or unknown native identity) is deliberately unproven, not foreign.
          else if (expectedReach === "foreign-namespace") expectedDiagnostic = nativeNamespace && !nativeNamespace.startsWith("win32:")
            ? "another PID namespace" : "a PID namespace this process cannot prove";
          else if (expectedReach === "token-mismatch") expectedDiagnostic = "is held by";
          else expectedDiagnostic = "FABRIC_MESH_LOCK_RECOVER_REFUSED";

          const diagnostic = assertRefusalProtocol(result, json, expectedDiagnostic, secrets);
          if (expectedReach === "foreign-namespace" && nativeNamespace && !nativeNamespace.startsWith("win32:")) {
            expect(diagnostic).toContain("invalid-namespace");
            expect(diagnostic).not.toContain(values.namespace);
          }
          expect(Buffer.from(fs.readFileSync(planted.ownerPath))).toEqual(Buffer.from(planted.record));
          assertNoAuditOrFence(planted.root, planted.lock, auditSentinel, collisionFence);
          if (collisionFence) expect(fs.readFileSync(path.join(collisionFence, "collision-sentinel"), "utf8"))
            .toBe(`R6 collision ${layout.id}.${field}\n`);
          return;
        }

        if (scenario === "safe-success") {
          expect(result.status).toBe(0);
          expect(result.stderr).not.toContain("refused before loading");
          const audit = auditLine(planted.root);
          const expectedToken = field === "token" ? "invalid-token" : TOKEN;
          assertSafeAuditProjection(audit.value, expectedToken, Number(exitedPid), nativeNamespace!, planted.lock, secrets);
          if (json) {
            const payload = JSON.parse(result.stdout) as Record<string, unknown>;
            expect(payload).toMatchObject({ command: "lock-recover", ok: true, recovered: true, mode: "safe",
              holderPid: Number(exitedPid), holderNamespace: nativeNamespace, verdict: "pid", fence: expectedFence });
            expect(payload.audit).toEqual(audit.value);
          } else {
            expect(result.stdout).toBe(`lock-recover done (safe): ${planted.lock} -> ${expectedFence}\n`);
            expect(result.stderr).toContain(`; audit ${audit.text}`);
          }
          assertNoOwnerPayload(result, secrets);
          const fence = expectedFence!;
          expect(fs.existsSync(planted.lock)).toBe(false);
          expect(fs.existsSync(fence)).toBe(true);
          expect(Buffer.from(fs.readFileSync(path.join(fence, "owner")))).toEqual(Buffer.from(planted.record));
          return;
        }

        if (scenario === "audit-refusal") {
          const diagnostic = assertRefusalProtocol(result, json, "could not be written safely", secrets);
          expect(diagnostic).toContain("mode 666");
          expect(Buffer.from(fs.readFileSync(planted.ownerPath))).toEqual(Buffer.from(planted.record));
          assertNoAuditOrFence(planted.root, planted.lock, auditSentinel);
          return;
        }

        // The generic rename failure is deliberately after a successful safe audit append. Preserve the
        // established outer-catch protocol and its exit 1; it is not reclassified as a refusal.
        expect(scenario).toBe("rename-error");
        expect(result.status).toBe(1);
        const errorPrefix = "fabric-mesh-backend: lock-recover failed: ";
        const errorLines = result.stderr.split("\n").filter(line => line.startsWith(errorPrefix));
        expect(errorLines).toHaveLength(1);
        expect(result.stderr.endsWith("\n")).toBe(true);
        if (json) {
          const payload = JSON.parse(result.stdout) as Record<string, unknown>;
          expect(payload).toMatchObject({ command: "lock-recover", ok: false, refused: false, alarms: [] });
          expect(["EEXIST", "ENOTEMPTY"]).toContain(payload.code);
          expect(typeof payload.error).toBe("string");
          expect(errorLines).toEqual([`${errorPrefix}${String(payload.error)}`]);
        } else expect(result.stdout).toBe("");
        assertNoOwnerPayload(result, secrets);
        const audit = auditLine(planted.root);
        const expectedToken = field === "token" ? "invalid-token" : TOKEN;
        assertSafeAuditProjection(audit.value, expectedToken, Number(exitedPid), nativeNamespace!, planted.lock, secrets);
        expect(Buffer.from(fs.readFileSync(planted.ownerPath))).toEqual(Buffer.from(planted.record));
        expect(collisionFence).toBeDefined();
        expect(fs.readFileSync(path.join(collisionFence!, "collision-sentinel"), "utf8"))
          .toBe(`R6 collision ${layout.id}.${field}\n`);
        expect(fs.readdirSync(planted.root).filter(name => name === "lock-recoveries.jsonl")).toHaveLength(1);
      });
    }
  }
}

// Positive canonical controls: a real safe recovery via the built shim, exact retained bytes, then lock reacquisition.
for (const kind of ["mesh", "custody"] as const) for (const json of [false, true]) {
  it(`canonical safe recovery and reacquisition control ${kind} / ${json ? "JSON" : "plain"}`, async context => {
    const layout = layouts.find(item => item.kind === kind && item.id === (kind === "mesh" ? "mesh-modern-6" : "custody-modern-6"))!;
    const values = baseValues();
    values.token = TOKEN;
    values.pid = exitedPid ?? "0";
    values.namespace = nativeNamespace ?? values.namespace;
    const planted = fixture(layout, values);
    nativeSafeRootOrSkip(context, planted.root);
    const fence = exactFencePath(planted.lock, planted.record);
    const result = runPublicCli(["lock-recover", "--root", planted.root, "--lock", kind, "--holder", TOKEN, ...(json ? ["--json"] : [])]);
    expect(result.status).toBe(0);
    const audit = auditLine(planted.root);
    assertSafeAuditProjection(audit.value, TOKEN, Number(exitedPid), nativeNamespace!, planted.lock);
    if (json) {
      const payload = JSON.parse(result.stdout) as Record<string, unknown>;
      expect(payload).toMatchObject({ ok: true, mode: "safe", holderPid: Number(exitedPid), holderNamespace: nativeNamespace, fence });
      expect(payload.audit).toEqual(audit.value);
    } else expect(result.stdout).toBe(`lock-recover done (safe): ${planted.lock} -> ${fence}\n`);
    expect(fs.existsSync(planted.lock)).toBe(false);
    expect(Buffer.from(fs.readFileSync(path.join(fence, "owner")))).toEqual(Buffer.from(planted.record));
    if (kind === "mesh") {
      await new MeshLock(planted.root, { lockProtocol: 3, lockTimeoutMs: 1_000 }, () => undefined)
        .withLock(() => expect(fs.existsSync(path.join(planted.lock, "owner"))).toBe(true));
    } else {
      const release = await acquireMeshCustodyLock(planted.root, 1_000, { lockProtocol: 3, ownIncarnation: nativeIncarnation });
      expect(fs.existsSync(path.join(planted.lock, "owner"))).toBe(true);
      release();
    }
    expect(fs.existsSync(planted.lock)).toBe(false);
  });
}

// Strict raw equality control: distinct malformed owner/argument tokens both project to invalid-token but still refuse.
for (const kind of ["mesh", "custody"] as const) for (const json of [false, true]) {
  it(`different malformed raw tokens still refuse ${kind} / ${json ? "JSON" : "plain"}`, () => {
    const layout = layouts.find(item => item.kind === kind && item.id === (kind === "mesh" ? "mesh-modern-6" : "custody-modern-6"))!;
    const values = baseValues();
    values.token = attackValue(layout, "token");
    const planted = fixture(layout, values);
    const otherRawToken = `${attackValue(layout, "token").replace("_token_", "_different-token_")}DIFFERENT`;
    const result = runPublicCli(["lock-recover", "--root", planted.root, "--lock", kind, "--holder", otherRawToken, ...(json ? ["--json"] : [])]);
    const message = `FABRIC_MESH_LOCK_RECOVER_REFUSED: ${planted.lock} is held by invalid-token, not invalid-token`;
    expect(assertRefusalProtocol(result, json, message, [values.token, otherRawToken])).toBe(message);
    expect(Buffer.from(fs.readFileSync(planted.ownerPath))).toEqual(Buffer.from(planted.record));
    assertNoAuditOrFence(planted.root, planted.lock);
  });
}

// Explicitly excluded UNSAFE preservation control only: malformed token and namespace remain raw by design.
for (const kind of ["mesh", "custody"] as const) for (const json of [false, true]) {
  it(`UNSAFE EXEMPTION CONTROL (raw audit/return/warning preserved) ${kind} / ${json ? "JSON" : "plain"}`, () => {
    const layout = layouts.find(item => item.kind === kind && item.id === (kind === "mesh" ? "mesh-modern-6" : "custody-host-modern-8"))!;
    const values = baseValues();
    const rawToken = `R6_UNSAFE_TOKEN_${kind}\r\x1b[31m;$(echo unsafe)`;
    const rawNamespace = `R6_UNSAFE_NAMESPACE_${kind}\r\x1b[31m;$(echo unsafe-ns)`;
    values.token = rawToken;
    values.namespace = rawNamespace;
    const planted = fixture(layout, values);
    const fence = exactFencePath(planted.lock, planted.record);
    const result = runPublicCli(["lock-recover", "--root", planted.root, "--lock", kind, "--holder", rawToken,
      "--unsafe-holder-stopped", ...(json ? ["--json"] : [])]);
    expect(result.status).toBe(0);
    const auditRead = auditLine(planted.root);
    const audit = auditRead.value;
    expect(audit).toMatchObject({ mode: "unsafe", holder: rawToken, holderPid: Number(process.pid), holderNamespace: rawNamespace });
    expect(auditRead.text).toContain(JSON.stringify(rawToken));
    expect(auditRead.text).toContain(JSON.stringify(rawNamespace));
    expect(result.stderr).toContain(rawToken);
    expect(result.stderr).toContain(rawNamespace);
    if (json) {
      const payload = JSON.parse(result.stdout) as Record<string, unknown>;
      expect(payload).toMatchObject({ ok: true, mode: "unsafe", holderNamespace: rawNamespace, audit });
      expect(result.stdout).toContain(JSON.stringify(rawToken));
      expect(result.stdout).toContain(JSON.stringify(rawNamespace));
    } else {
      expect(result.stdout).toContain("lock-recover done (unsafe):");
    }
    expect(fs.existsSync(planted.lock)).toBe(false);
    expect(Buffer.from(fs.readFileSync(path.join(fence, "owner")))).toEqual(Buffer.from(planted.record));

    // Legacy readable layouts retain the established null namespace convention in unsafe metadata.
    const legacyLayout = layouts.find(item => item.kind === kind && item.id ===
      (kind === "mesh" ? "mesh-legacy-4" : "custody-host-legacy-7"))!;
    const legacyValues = baseValues();
    legacyValues.token = rawToken;
    legacyValues.pid = String(process.pid);
    const legacy = fixture(legacyLayout, legacyValues);
    const legacyFence = exactFencePath(legacy.lock, legacy.record);
    const legacyResult = runPublicCli(["lock-recover", "--root", legacy.root, "--lock", kind, "--holder", rawToken,
      "--unsafe-holder-stopped", ...(json ? ["--json"] : [])]);
    expect(legacyResult.status).toBe(0);
    const legacyAudit = auditLine(legacy.root).value;
    expect(legacyAudit).toMatchObject({ mode: "unsafe", holder: rawToken, holderPid: Number(process.pid), holderNamespace: null });
    if (json) expect(JSON.parse(legacyResult.stdout).holderNamespace).toBeNull();
    expect(fs.existsSync(legacy.lock)).toBe(false);
    expect(Buffer.from(fs.readFileSync(path.join(legacyFence, "owner")))).toEqual(Buffer.from(legacy.record));
  });
}
