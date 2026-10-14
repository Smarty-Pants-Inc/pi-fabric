import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { admitModelException, type ModelExceptionEntry } from "../src/agents/model-policy-log.js";
import { THINKING_LEVELS, thinkingRank, type FabricThinking } from "../src/thinking.js";
import { rootMetadataFixture } from "./helpers/model-exceptions.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const model = "anthropic/claude-opus-5-5";
const reason = "Bounded test admission #1";
const future = "2999-01-01T00:00:00.000Z"; // Hostile legacy inline config, never authority.
const issue = "Smarty-Pants-Inc/smarty-dev#1";
const host = os.hostname().split(".")[0]!;
const grant = (overrides: Partial<ModelExceptionEntry> = {}): ModelExceptionEntry => ({
  id: "g-admission-1", issue,
  reason_url: "https://github.com/Smarty-Pants-Inc/smarty-dev/issues/1#issuecomment-1",
  role: "task", lane: "project", host, model, thinking: "medium",
  granted_by: "model-economics-lead",
  granted_at: new Date(Date.now() - 1_000).toISOString(),
  expires_at: new Date(Date.now() + 71 * 60 * 60 * 1_000).toISOString(),
  status: "active", ...overrides,
});
const document = (grants: unknown[]) => JSON.stringify({ version: 1, grants });
const hasPosixTrustMetadata = process.platform !== "win32" && typeof process.getuid === "function";
const realLstatSync = fs.lstatSync;
const realOpenSync = fs.openSync;

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

function fixture(options: {
  bindRoot?: boolean;
  rootSessionCwd?: () => string | undefined;
  thinking?: FabricThinking;
} = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-model-exception-"));
  roots.push(root);
  const cwd = path.join(root, "project");
  const agentDir = path.join(root, "agent");
  const runRoot = path.join(root, "runs");
  const projectDir = path.join(cwd, ".pi");
  fs.mkdirSync(projectDir, { recursive: true });
  fs.mkdirSync(agentDir, { recursive: true });
  const redirectedPath = path.join(root, "attacker-selected.json");
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: {
    modelExceptionsPath: redirectedPath,
    modelExceptions: [{ ref: "smarty-dev#1", models: ["anthropic/claude"], expires: future }],
  } }));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ agents: {
    modelExceptionsPath: redirectedPath,
    modelExceptions: [{ ref: "smarty-dev#1", models: ["anthropic/claude"], expires: future }],
  } }));
  fs.writeFileSync(path.join(projectDir, "model-exceptions.json"), JSON.stringify([{
    ref: "smarty-dev#1", models: ["anthropic/claude"], expires: future,
  }]));
  const config = loadFabricConfig({ cwd, agentDir, projectTrusted: true });
  expect(config.agents.modelExceptionsPath).toBe("/etc/smarty/model-exceptions.json");
  // Direct internal embedder override: fixture path is still subjected to source trust checks.
  const exceptionPath = path.resolve(agentDir, "model-exceptions.json");
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, ...config.agents, modelExceptionsPath: exceptionPath,
    thinking: options.thinking ?? "medium", budgetUsd: 0 }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
    runRoot,
    ...(options.bindRoot === false ? {} : { rootSessionCwd: options.rootSessionCwd ?? (() => cwd) }),
  });
  managers.push(manager);
  return { cwd, agentDir, exceptionPath, manager, runRoot };
}

const writeGrant = (file: string, record: ModelExceptionEntry = grant()) =>
  fs.writeFileSync(file, document([record]));

const admissionInput = (file: string, overrides: Partial<Parameters<typeof admitModelException>[0]> = {}) => ({
  path: file, reason, model, role: "task", lane: "project", host, thinking: "medium" as const, ...overrides,
});

const expectNoRun = (state: ReturnType<typeof fixture>, launch: unknown) => {
  expect(state.manager.list()).toEqual([]);
  expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
  expect(launch).not.toHaveBeenCalled();
};

// Keep reason: the user-to-root grant trust boundary, exercised through actual manager admission.
describe("model-exception source trust boundary", () => {
  it.runIf(hasPosixTrustMetadata && process.getuid!() !== 0)("refuses a real user-owned self-written grant with no ownership seam before launch", async () => {
    const state = fixture();
    writeGrant(state.exceptionPath);
    expect(realLstatSync(state.exceptionPath).uid).toBe(process.getuid!());
    expect(realLstatSync(state.exceptionPath).uid).not.toBe(0);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "self-issued grant", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "untrusted-source" });
    expect(state.manager.list()).toEqual([]);
    expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
  });

  it.runIf(hasPosixTrustMetadata)("refuses a real symlink to a root-owned file without reading or parsing its target", async () => {
    const state = fixture();
    expect(realLstatSync("/etc/passwd").uid).toBe(0);
    fs.symlinkSync("/etc/passwd", state.exceptionPath);
    // No ownership seam: this is a real user-created symlink to a root-owned endpoint.
    const open = vi.spyOn(fs, "openSync");
    const read = vi.spyOn(fs, "readSync");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "symlink grant", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "untrusted-source" });
    expect(read).not.toHaveBeenCalled();
    const grantOpen = open.mock.calls.find(([at]) => at === state.exceptionPath);
    if (grantOpen) {
      expect(Number(grantOpen[1]) & fs.constants.O_NOFOLLOW).toBe(fs.constants.O_NOFOLLOW);
      expect(Number(grantOpen[1]) & fs.constants.O_NONBLOCK).toBe(fs.constants.O_NONBLOCK);
    }
    expect(state.manager.list()).toEqual([]);
    expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
  });

  it.runIf(hasPosixTrustMetadata)("uses real O_NOFOLLOW to refuse a symlink swapped in after root-metadata prevalidation", async () => {
    const state = fixture();
    writeGrant(state.exceptionPath);
    rootMetadataFixture(state.exceptionPath);
    const read = vi.spyOn(fs, "readSync");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const open = vi.spyOn(fs, "openSync").mockImplementation(((at, flags, mode) => {
      if (at === state.exceptionPath) {
        // Actual filesystem race, not a synthesized open error. Keep the original inode, do not delete it.
        fs.renameSync(state.exceptionPath, path.join(state.agentDir, "pre-swap-grant.json"));
        fs.symlinkSync("/etc/passwd", state.exceptionPath);
      }
      return realOpenSync(at, flags, mode);
    }) as typeof fs.openSync);
    await expect(state.manager.spawn({ task: "nofollow race", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "untrusted-source" });
    const flags = Number(open.mock.calls.find(([at]) => at === state.exceptionPath)![1]);
    expect(flags & fs.constants.O_NOFOLLOW).toBe(fs.constants.O_NOFOLLOW);
    expect(flags & fs.constants.O_NONBLOCK).toBe(fs.constants.O_NONBLOCK);
    expect(realLstatSync(state.exceptionPath).isSymbolicLink()).toBe(true);
    expect(read).not.toHaveBeenCalled();
    expect(state.manager.list()).toEqual([]);
    expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
  });

  it.runIf(hasPosixTrustMetadata)("refuses a group-writable parent in an explicitly root-metadata fixture", async () => {
    const state = fixture();
    writeGrant(state.exceptionPath);
    const metadata = rootMetadataFixture(state.exceptionPath, state.agentDir);
    const read = vi.spyOn(fs, "readSync");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "unsafe parent grant", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "untrusted-source" });
    expect(metadata.lstat.mock.calls.some(([at]) => at === state.agentDir)).toBe(true);
    expect(read).not.toHaveBeenCalled();
    expect(state.manager.list()).toEqual([]);
    expect(fs.existsSync(state.runRoot) ? fs.readdirSync(state.runRoot) : []).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
  });

  it("fails closed when getuid is absent, without opening the grant", () => {
    const descriptor = Object.getOwnPropertyDescriptor(process, "getuid");
    const open = vi.spyOn(fs, "openSync");
    try {
      Object.defineProperty(process, "getuid", { configurable: true, value: undefined });
      expect(() => admitModelException(admissionInput("/etc/smarty/model-exceptions.json")))
        .toThrow(expect.objectContaining({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "untrusted-source" }));
      expect(open).not.toHaveBeenCalled();
    } finally {
      if (descriptor) Object.defineProperty(process, "getuid", descriptor);
      else Reflect.deleteProperty(process, "getuid");
    }
  });

  it.runIf(process.platform === "win32")("fails closed on Windows rather than accepting an existing grant", () => {
    const state = fixture();
    writeGrant(state.exceptionPath);
    expect(() => admitModelException(admissionInput(state.exceptionPath)))
      .toThrow(expect.objectContaining({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "untrusted-source" }));
  });
});

describe.runIf(hasPosixTrustMetadata)("fresh root-metadata v1 host model-exception admission", () => {
  it("refuses unlisted #1 before launch, then completes the exact issue/context control", async () => {
    const state = fixture();
    vi.spyOn(os, "homedir").mockReturnValue(state.agentDir);
    fs.writeFileSync(state.exceptionPath, document([]));
    const metadata = rootMetadataFixture(state.exceptionPath);
    const open = vi.spyOn(fs, "openSync");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "unlisted issue stays denied", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ name: "FabricModelExceptionDeniedError", code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "unlisted" });
    expect(metadata.fstat).toHaveBeenCalled();
    expect(metadata.lstat.mock.calls.some(([at]) => at === path.parse(state.exceptionPath).root)).toBe(true);
    const flags = Number(open.mock.calls.find(([at]) => at === state.exceptionPath)![1]);
    expect(flags & fs.constants.O_NOFOLLOW).toBe(fs.constants.O_NOFOLLOW);
    expect(flags & fs.constants.O_NONBLOCK).toBe(fs.constants.O_NONBLOCK);
    expectNoRun(state, launch);

    const approved = grant();
    writeGrant(state.exceptionPath, approved);
    const handle = await state.manager.spawn({ task: "approved issue control", model, modelReason: reason, transport: "process" });
    expect(launch).toHaveBeenCalledOnce();
    expect(await state.manager.wait(handle.id)).toMatchObject({ status: "completed", model, modelReason: reason });
    const auditFile = path.join(state.agentDir, ".local", "state", "smarty", "route", "opus-exceptions.jsonl");
    const auditRows = fs.readFileSync(auditFile, "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as Record<string, unknown>);
    expect(auditRows.filter(row => row.reason === reason)).toEqual([expect.objectContaining({
      grant_id: approved.id, ref: issue, issue, lane: "project", host, role: "task", model,
      thinking: "medium", run_id: handle.id, time: expect.any(String),
    })]);
  });

  it("pins the admitted grant id and context when the host file is replaced before audit", async () => {
    const state = fixture();
    vi.spyOn(os, "homedir").mockReturnValue(state.agentDir);
    const reason = "Unlisted decoy #1; approved host grant smarty-dev#12000";
    const approvedIssue = "Smarty-Pants-Inc/smarty-dev#12000";
    const approved = grant({ id: "g-approved-before-commit", issue: approvedIssue,
      reason_url: "https://github.com/Smarty-Pants-Inc/smarty-dev/issues/12000#issuecomment-1" });
    writeGrant(state.exceptionPath, approved);
    rootMetadataFixture(state.exceptionPath);
    const beforeCommit = vi.fn(() => writeGrant(state.exceptionPath, grant({
      id: "g-replacement-after-admission", issue: approvedIssue,
      reason_url: approved.reason_url, status: "revoked",
    })));

    const handle = await state.manager.spawn({ task: "audit attribution race", model, modelReason: reason, transport: "process" },
      undefined, undefined, beforeCommit);
    expect(beforeCommit).toHaveBeenCalledOnce();
    expect(await state.manager.wait(handle.id)).toMatchObject({ status: "completed", model, modelReason: reason });
    const auditFile = path.join(state.agentDir, ".local", "state", "smarty", "route", "opus-exceptions.jsonl");
    const auditRows = fs.readFileSync(auditFile, "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as Record<string, unknown>);
    expect(auditRows.filter(row => row.reason === reason)).toEqual([expect.objectContaining({
      grant_id: approved.id, ref: approvedIssue, issue: approvedIssue,
      lane: approved.lane, host: approved.host, role: approved.role, model: approved.model,
      thinking: "medium", run_id: handle.id, time: expect.any(String),
    })]);
  });

  it.each([
    ["missing host file", undefined, "file-unavailable"],
    ["malformed JSON", "not json", "malformed"],
    ["unknown version", JSON.stringify({ version: 2, grants: [grant()] }), "malformed"],
    ["legacy list", JSON.stringify([{ ref: "smarty-dev#1", models: [model], expires: future }]), "malformed"],
    ["duplicate ids", document([grant(), grant()]), "malformed"],
    ["malformed unrelated row", document([grant(), { id: "g-unrelated-broken", issue: "Smarty-Pants-Inc/pi-fabric#2" }]), "malformed"],
    ["unknown status", document([grant({ status: "pending" as ModelExceptionEntry["status"] })]), "malformed"],
    ["date without time/zone", document([grant({ granted_at: "2026-10-14" })]), "malformed"],
    ["impossible calendar date", document([grant({ granted_at: "2026-02-30T00:00:00Z" })]), "malformed"],
    ["nonpositive lifetime", document([grant({ granted_at: "2026-10-14T00:00:00Z", expires_at: "2026-10-14T00:00:00Z" })]), "malformed"],
    ["expired grant", document([grant({ granted_at: "2000-01-01T00:00:00Z", expires_at: "2000-01-02T00:00:00Z" })]), "expired"],
    ["revoked grant", document([grant({ status: "revoked" })]), "revoked"],
    ["wrong issue", document([grant({ issue: "Smarty-Pants-Inc/smarty-dev#2",
      reason_url: "https://github.com/Smarty-Pants-Inc/smarty-dev/issues/2#issuecomment-1" })]), "unlisted"],
    ["wrong model", document([grant({ model: "provider/gpt" })]), "model-mismatch"],
    ["model prefix is not exact", document([grant({ model: "anthropic/claude-opus" })]), "model-mismatch"],
    ["alternate native provenance is not a grant key", document([grant({ model: `claude/${model}` })]), "model-mismatch"],
    ["wrong role", document([grant({ role: "actor:reviewer" })]), "role-mismatch"],
    ["wrong lane", document([grant({ lane: "other-lane" })]), "lane-mismatch"],
    ["wrong host", document([grant({ host: `${host}-other` })]), "host-mismatch"],
    ["FQDN is malformed rather than the required short host", document([grant({ host: `${host}.invalid` })]), "malformed"],
    ["thinking ceiling", document([grant({ thinking: "low" })]), "thinking-exceeds"],
    ["lifetime exceeds 72 hours", document([grant({
      granted_at: new Date(Date.now() - 1_000).toISOString(),
      expires_at: new Date(Date.now() + 72 * 60 * 60 * 1_000).toISOString(),
    })]), "expiry-too-long"],
  ] as const)("rejects a fresh-read mismatch (%s) before creating a run", async (_label, contents, denial) => {
    const state = fixture();
    if (contents !== undefined) fs.writeFileSync(state.exceptionPath, contents);
    rootMetadataFixture(state.exceptionPath);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "task role", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial });
    expectNoRun(state, launch);
  });

  it.each(THINKING_LEVELS)("uses the shared thinking rank for requested %s", thinking => {
    const state = fixture();
    writeGrant(state.exceptionPath);
    rootMetadataFixture(state.exceptionPath);
    const admit = () => admitModelException(admissionInput(state.exceptionPath, { thinking }));
    if (thinkingRank(thinking) <= thinkingRank("medium")) {
      expect(admit()).toMatchObject({ ref: issue, entry: { id: "g-admission-1", thinking: "medium" } });
    } else {
      expect(admit).toThrow(expect.objectContaining({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "thinking-exceeds" }));
    }
  });

  it("accepts exactly 72 hours, ignores unrelated rows, and freezes the approved snapshot", () => {
    const state = fixture();
    const now = Date.parse("2026-10-14T14:20:00Z");
    const approved = grant({ granted_at: "2026-10-14T14:20:00Z", expires_at: "2026-10-17T14:20:00Z" });
    fs.writeFileSync(state.exceptionPath, document([
      grant({ id: "g-other", lane: "other-lane" }), approved,
    ]));
    rootMetadataFixture(state.exceptionPath);
    const admitted = admitModelException(admissionInput(state.exceptionPath, { now }));
    expect(admitted).toEqual({ ref: issue, entry: approved });
    expect(Object.isFrozen(admitted.entry)).toBe(true);
  });

  it.each(["#1", "smarty-dev#1", issue, "https://github.com/Smarty-Pants-Inc/smarty-dev/issues/1#issuecomment-1"])(
    "matches the canonical issue plus full context for %s", modelReason => {
      const state = fixture();
      writeGrant(state.exceptionPath);
      rootMetadataFixture(state.exceptionPath);
      expect(admitModelException(admissionInput(state.exceptionPath, { reason: modelReason })))
        .toMatchObject({ ref: issue, entry: { id: "g-admission-1" } });
    },
  );

  it.each([
    "Other-Org/smarty-dev#1", "https://github.com/Other-Org/smarty-dev/issues/1",
    "https://tracker.invalid/docs/#1", "https://github.com/Smarty-Pants-Inc/smarty-dev/pull/1",
  ])("does not borrow an issue from foreign orgs or URL fragments (%s)", modelReason => {
    const state = fixture();
    writeGrant(state.exceptionPath);
    rootMetadataFixture(state.exceptionPath);
    expect(() => admitModelException(admissionInput(state.exceptionPath, { reason: modelReason })))
      .toThrow(expect.objectContaining({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "missing-reference" }));
  });
});

// Keep reason: callers cannot manufacture the host's bound ROOT lane or actual hostname.
describe.runIf(hasPosixTrustMetadata)("root-record lane and host binding", () => {
  it.each([
    ["absent callback", { bindRoot: false }],
    ["undefined root record", { rootSessionCwd: () => undefined }],
    ["relative root cwd", { rootSessionCwd: () => "project" }],
    ["different bound root", { rootSessionCwd: () => path.resolve("different-root") }],
  ] as const)("refuses %s despite matching manager/request cwd and hostile context", async (_label, options) => {
    const state = fixture(options);
    writeGrant(state.exceptionPath);
    rootMetadataFixture(state.exceptionPath);
    vi.stubEnv("PI_FABRIC_LANE", "project");
    vi.stubEnv("SMARTY_LANE", "project");
    vi.stubEnv("PI_FABRIC_ROOT_CWD", state.cwd);
    vi.stubEnv("HOSTNAME", host);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "hostile context", model, modelReason: reason,
      transport: "process", cwd: state.cwd, lane: "project", host,
      rootSessionCwd: state.cwd,
    } as Parameters<AgentManager["spawn"]>[0]))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "lane-mismatch" });
    expectNoRun(state, launch);
  });

  it("admits the bound ROOT basename even when manager/request cwd and env disagree", async () => {
    const rootCwd = path.resolve("bound-root-worktrees", "trusted-root-lane");
    const rootSessionCwd = vi.fn(() => rootCwd);
    const state = fixture({ rootSessionCwd });
    writeGrant(state.exceptionPath, grant({ lane: path.basename(rootCwd) }));
    rootMetadataFixture(state.exceptionPath);
    vi.spyOn(os, "homedir").mockReturnValue(state.agentDir);
    vi.stubEnv("PI_FABRIC_LANE", "hostile-lane");
    vi.stubEnv("SMARTY_LANE", "hostile-lane");
    vi.stubEnv("PI_FABRIC_ROOT_CWD", path.resolve("hostile-lane"));
    vi.stubEnv("HOSTNAME", "hostile-host");
    const handle = await state.manager.spawn({ task: "trusted root countercase", model, modelReason: reason,
      transport: "process", cwd: state.cwd, lane: "hostile-lane", host: "hostile-host",
    } as Parameters<AgentManager["spawn"]>[0]);
    expect(await state.manager.wait(handle.id)).toMatchObject({ status: "completed", model });
    expect(rootSessionCwd).toHaveBeenCalled();
  });

  it("uses actual manager default effort rather than granting an omitted-thinking bypass", async () => {
    const state = fixture({ thinking: "high" });
    writeGrant(state.exceptionPath, grant({ thinking: "medium" }));
    rootMetadataFixture(state.exceptionPath);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(state.manager.spawn({ task: "omitted effort", model, modelReason: reason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "thinking-exceeds" });
    expectNoRun(state, launch);
  });
});
