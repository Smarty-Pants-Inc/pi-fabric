import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { agentServiceDescriptors } from "../src/agents/service-schema.js";
import { validateAgentExtensionPaths } from "../src/agents/extension-paths.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { applyTaskReturnAddress } from "../src/agents/task-return-address.js";
import { taskAgentEnvironment } from "../src/agents/task-environment.js";
import { childProcessEnvironment } from "../src/core/atomic-write.js";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig } from "../src/config.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const supportsUidOwnership = typeof process.getuid === "function";
const temporary = (prefix: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  return root;
};
const sha256 = (value: string | Buffer): string => createHash("sha256").update(value).digest("hex");
const manifestSha256 = (entries: Array<[string, string, string?]>): string => {
  const compare = (left: string, right: string): number => left < right ? -1 : left > right ? 1 : 0;
  entries.sort((left, right) => compare(left[0], right[0]) || compare(left[1], right[1]) || compare(left[2] ?? "", right[2] ?? ""));
  return sha256(JSON.stringify(entries));
};
const fakePathOwner = (file: string, uid: number) => {
  const original = fs.lstatSync;
  return vi.spyOn(fs, "lstatSync").mockImplementation(((candidate: fs.PathLike, ...args: unknown[]) => {
    const stats = (original as (...values: unknown[]) => fs.Stats)(candidate, ...args);
    return path.resolve(String(candidate)) === file
      ? Object.assign(Object.create(Object.getPrototypeOf(stats)) as fs.Stats, stats, { uid })
      : stats;
  }) as typeof fs.lstatSync);
};
const managerFor = (cwd: string, extensionRoots?: string[], piBinary?: string, extensionPins?: Array<{ id: string; path: string; sha256: string }>, baseConfig: typeof DEFAULT_FABRIC_CONFIG.agents = DEFAULT_FABRIC_CONFIG.agents): AgentManager => {
  const manager = new AgentManager(cwd, {
    ...baseConfig,
    runner: "pi",
    transport: "process",
    timeoutMs: 30_000,
    sessionExport: false,
    ...(extensionRoots ? { extensionRoots } : {}),
    ...(extensionPins ? { extensionPins } : {}),
  }, {
    workerPath: path.resolve("dist/worker.js"),
    piBinary: piBinary ?? path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    runRoot: path.join(cwd, "runs"),
    fullCodeMode: false,
  });
  managers.push(manager);
  return manager;
};
const spawnRequest = (extensionPaths: string[], tools?: string[]) => ({
  task: "Call the explicitly admitted extension tool once and report its result.",
  runner: "pi" as const,
  transport: "process" as const,
  extensionPaths,
  ...(tools !== undefined ? { tools } : {}),
});
const hashEnvironment = (environment: NodeJS.ProcessEnv): string => sha256(JSON.stringify(
  Object.entries(environment)
    .filter((entry): entry is [string, string] => typeof entry[1] === "string" && entry[0] !== "NODE_CHANNEL_FD" && entry[0] !== "NODE_UNIQUE_ID")
    .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0),
));

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("host-authorized process extension paths", () => {
  it("publishes and normalizes the bounded spawn argument, but not on hosted service actions", () => {
    const run = AGENTS_ACTION_DESCRIPTORS.find(descriptor => descriptor.name === "run")!;
    const extensionPaths = (run.inputSchema as { properties: Record<string, unknown> }).properties.extensionPaths;
    expect(extensionPaths).toMatchObject({ type: "array", maxItems: 4, items: { type: "string", minLength: 1, maxLength: 4096 } });
    const request = normalizeAgentRunRequest({ task: "probe", extensionPaths: ["/approved/extension.mjs"], tools: ["probe_echo"] }, {
      runner: "pi", timeoutMs: 0,
    });
    expect(request.extensionPaths).toEqual(["/approved/extension.mjs"]);
    const hostedRun = agentServiceDescriptors().find(descriptor => descriptor.name === "run")!;
    expect((hostedRun.inputSchema as { properties: Record<string, unknown> }).properties.extensionPaths).toBeUndefined();
  });

  it.skipIf(!supportsUidOwnership)("loads a root-owned pinned extension, calls its allow-listed tool, and records its digest", async () => {
    const root = temporary("fabric-extension-real-");
    const extensionRoot = path.join(root, "approved");
    const extensionPath = path.join(extensionRoot, "spawn-extension-probe.mjs");
    const agentDir = path.join(root, "pi-agent");
    const defaultExtensions = path.join(agentDir, "extensions");
    fs.mkdirSync(extensionRoot, { recursive: true });
    fs.mkdirSync(defaultExtensions, { recursive: true });
    fs.copyFileSync(path.resolve("tests/fixtures/spawn-extension-probe.mjs"), extensionPath);
    const defaultMarker = path.join(root, "default-extension-loaded");
    const defaultExtensionPath = path.join(defaultExtensions, "default-probe.mjs");
    fs.writeFileSync(defaultExtensionPath, [
      'import fs from "node:fs";',
      'import extension from "../../approved/spawn-extension-probe.mjs";',
      'export default (pi) => { pi.on("session_start", () => fs.writeFileSync(process.env.FABRIC_DEFAULT_EXTENSION_MARKER, "loaded")); extension(pi); };',
    ].join("\n"));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ extensions: [defaultExtensionPath] }));
    const executed = path.join(root, "extension-executed.log");
    const toolsLog = path.join(root, "extension-tools.log");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("HOME", root);
    vi.stubEnv("FABRIC_DEFAULT_EXTENSION_MARKER", defaultMarker);
    vi.stubEnv("FABRIC_EXTENSION_PROBE_EXECUTED", executed);
    vi.stubEnv("FABRIC_EXTENSION_PROBE_TOOLS", toolsLog);
    const extensionPin = {
      id: "offline-probe",
      path: fs.realpathSync(extensionPath),
      sha256: sha256(fs.readFileSync(extensionPath)),
    };
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { extensionPins: [extensionPin] } }));

    const rootOwnedPin = fakePathOwner(extensionPin.path, 0);
    let result: Awaited<ReturnType<AgentManager["run"]>>;
    let activeManager: AgentManager | undefined;
    try {
      const hostConfig = loadFabricConfig({ cwd: root, agentDir, projectTrusted: false });
      activeManager = managerFor(root, undefined, undefined, undefined, hostConfig.agents);
      result = await activeManager.run({
        ...spawnRequest([extensionPin.id], ["probe_echo"]),
        model: "spawn-extension-probe/offline",
        thinking: "off",
      });
    } finally {
      rootOwnedPin.mockRestore();
    }

    expect(result.status, result.error).toBe("completed");
    expect(result.text).toContain("extension-result:probe-echo:from-child");
    expect(fs.readFileSync(executed, "utf8")).toBe("from-child\n");
    const observedTools = fs.readFileSync(toolsLog, "utf8").trim().split("\n").map(value => JSON.parse(value));
    expect(observedTools.length).toBeGreaterThanOrEqual(2);
    expect(observedTools.every(names => JSON.stringify(names) === JSON.stringify(["probe_echo"]))).toBe(true);
    for (const name of ["read", "bash", "edit", "write"]) expect(observedTools.flat()).not.toContain(name);
    expect(fs.existsSync(defaultMarker)).toBe(false);
    expect(result.extensionPaths).toEqual([{
      path: fs.realpathSync(extensionPath),
      sha256: sha256(fs.readFileSync(extensionPath)),
    }]);

    const manager = activeManager!;
    const defaultResult = await manager.run({
      task: "Verify the legacy extension-discovery path remains available.",
      runner: "pi",
      transport: "process",
      extensions: true,
      tools: ["probe_echo"],
      model: "spawn-extension-probe/offline",
      thinking: "off",
    });
    expect(defaultResult.status, defaultResult.error).toBe("completed");
    expect(fs.readFileSync(defaultMarker, "utf8")).toBe("loaded");
  });

  it.skipIf(!supportsUidOwnership)("refuses a post-admission extension swap before Pi or extension code starts", async () => {
    const root = temporary("fabric-extension-tamper-");
    const extensionRoot = path.join(root, "approved");
    const extensionPath = path.join(extensionRoot, "spawn-extension-probe.mjs");
    const agentDir = path.join(root, "pi-agent");
    const executed = path.join(root, "extension-executed.log");
    const toolsLog = path.join(root, "extension-tools.log");
    fs.mkdirSync(extensionRoot, { recursive: true });
    fs.mkdirSync(agentDir, { recursive: true });
    fs.copyFileSync(path.resolve("tests/fixtures/spawn-extension-probe.mjs"), extensionPath);
    const admittedSha = sha256(fs.readFileSync(extensionPath));
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("HOME", root);
    vi.stubEnv("FABRIC_EXTENSION_PROBE_EXECUTED", executed);
    vi.stubEnv("FABRIC_EXTENSION_PROBE_TOOLS", toolsLog);
    const extensionPin = { id: "tamper-probe", path: fs.realpathSync(extensionPath), sha256: admittedSha };
    const manager = managerFor(root, undefined, undefined, [extensionPin]);
    const originalLaunch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      fs.appendFileSync(extensionPath, "\n// changed after host admission\n");
      return originalLaunch.call(this, request);
    });

    const result = await manager.run({
      ...spawnRequest([extensionPath], ["probe_echo"]),
      model: "spawn-extension-probe/offline",
      thinking: "off",
    });

    expect(result.status, result.error).toBe("failed");
    expect(result.error).toMatch(/changed after admission before Pi load.*expected SHA-256/);
    expect(result.extensionPaths).toEqual([{ path: fs.realpathSync(extensionPath), sha256: admittedSha }]);
    expect(fs.existsSync(executed)).toBe(false);
    expect(fs.existsSync(toolsLog)).toBe(false);
  });

  it.skipIf(!supportsUidOwnership)("hashes package directories from a stable sorted manifest", () => {
    const root = temporary("fabric-extension-manifest-");
    const packageDirectory = path.join(root, "fixture");
    fs.mkdirSync(packageDirectory);
    const packageJson = '{"name":"extension-fixture","type":"module"}\n';
    const moduleText = "export default () => {};\n";
    fs.writeFileSync(path.join(packageDirectory, "package.json"), packageJson);
    fs.writeFileSync(path.join(packageDirectory, "index.mjs"), moduleText);
    const expected = manifestSha256([
      ["", "directory"],
      ["index.mjs", "file", sha256(moduleText)],
      ["package.json", "file", sha256(packageJson)],
    ]);
    const packagePin = { id: "fixture-package", path: fs.realpathSync(packageDirectory), sha256: expected };
    const first = validateAgentExtensionPaths([packagePin.id], [packagePin], [root], root)[0];
    const second = validateAgentExtensionPaths([packagePin.path], [packagePin], [root], root)[0];
    expect(first).toEqual({ path: fs.realpathSync(packageDirectory), sha256: expected });
    expect(second).toEqual(first);
    const foreignPackageEntry = fakePathOwner(path.join(packageDirectory, "index.mjs"), process.getuid!() + 1);
    expect(() => validateAgentExtensionPaths([packagePin.id], [packagePin], [root], root))
      .toThrow(/package entry index\.mjs is not owned by root or the current UID/);
    foreignPackageEntry.mockRestore();
    fs.appendFileSync(path.join(packageDirectory, "index.mjs"), "// changed\n");
    expect(() => validateAgentExtensionPaths([packagePin.id], [packagePin], [root], root))
      .toThrow(/does not match its host-pinned SHA-256/);

    const sharedModule = path.join(root, "shared.mjs");
    const symlinkPackage = path.join(root, "symlink-package");
    const symlinkPackageJson = '{"name":"symlink-package"}\n';
    fs.writeFileSync(sharedModule, "export default 1;\n");
    fs.mkdirSync(symlinkPackage);
    fs.writeFileSync(path.join(symlinkPackage, "package.json"), symlinkPackageJson);
    fs.symlinkSync(sharedModule, path.join(symlinkPackage, "alias.mjs"));
    const symlinkPin = { id: "symlink-package", path: fs.realpathSync(symlinkPackage), sha256: "0".repeat(64) };
    expect(() => validateAgentExtensionPaths([symlinkPin.id], [symlinkPin], [root], root))
      .toThrow(/package entry alias\.mjs is a symlink/);
  });

  it.skipIf(!supportsUidOwnership)("requires a host pin, enforces optional roots, and refuses before launch", () => {
    const root = temporary("fabric-extension-refusal-");
    const extensionRoot = path.join(root, "approved");
    const outsideRoot = path.join(root, "outside");
    fs.mkdirSync(extensionRoot);
    fs.mkdirSync(outsideRoot);
    const validPath = path.join(extensionRoot, "valid.mjs");
    const unlistedPath = path.join(extensionRoot, "unlisted.mjs");
    const outsidePath = path.join(outsideRoot, "outside.mjs");
    const writablePath = path.join(extensionRoot, "world-writable.mjs");
    for (const file of [validPath, unlistedPath, outsidePath, writablePath]) fs.writeFileSync(file, "export default () => {};\n");
    fs.chmodSync(writablePath, 0o666);
    const escapePath = path.join(extensionRoot, "escape.mjs");
    fs.symlinkSync(outsidePath, escapePath);
    const packageDirectory = path.join(extensionRoot, "symlink-package");
    fs.mkdirSync(packageDirectory);
    fs.writeFileSync(path.join(packageDirectory, "package.json"), '{"name":"symlink-package"}\n');
    fs.symlinkSync(outsidePath, path.join(packageDirectory, "escape.mjs"));
    const fileSha = (file: string) => sha256(fs.readFileSync(file));
    const pins = [
      { id: "valid", path: fs.realpathSync(validPath), sha256: fileSha(validPath) },
      { id: "outside", path: fs.realpathSync(outsidePath), sha256: fileSha(outsidePath) },
      { id: "writable", path: fs.realpathSync(writablePath), sha256: fileSha(writablePath) },
      { id: "symlink", path: escapePath, sha256: fileSha(outsidePath) },
      { id: "package-escape", path: fs.realpathSync(packageDirectory), sha256: "0".repeat(64) },
    ];
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = managerFor(root, [extensionRoot], undefined, pins);

    expect(() => manager.spawn(spawnRequest([unlistedPath], ["probe_echo"]))).toThrow(/not an ID or exact path in the host agents\.extensionPins/);
    expect(() => manager.spawn(spawnRequest(["outside"], ["probe_echo"]))).toThrow(/outside the optional agents\.extensionRoots constraint/);
    expect(() => manager.spawn(spawnRequest(["symlink"], ["probe_echo"]))).toThrow(/must equal its realpath/);
    expect(() => manager.spawn(spawnRequest(["package-escape"], ["probe_echo"]))).toThrow(/package entry escape\.mjs is a symlink/);
    expect(() => manager.spawn(spawnRequest(["writable"], ["probe_echo"]))).toThrow(/group- or world-writable/);
    expect(() => manager.spawn(spawnRequest([`${extensionRoot}${path.sep}..${path.sep}outside${path.sep}outside.mjs`], ["probe_echo"]))).toThrow(/'\.\.' path segments/);
    if (process.getuid) {
      const owner = fakePathOwner(fs.realpathSync(validPath), process.getuid!() + 1);
      expect(() => manager.spawn(spawnRequest(["valid"], ["probe_echo"]))).toThrow(/not owned by root or the current UID/);
      owner.mockRestore();
    }
    expect(() => manager.spawn({ ...spawnRequest(["valid"], ["probe_echo"]), runner: "claude" })).toThrow(/requires the Pi runner/);
    expect(() => manager.spawn({ ...spawnRequest(["valid"], ["probe_echo"]), transport: "tmux" })).toThrow(/requires process transport/);
    expect(() => manager.spawn({ ...spawnRequest(["valid"], ["probe_echo"]), recursive: true })).toThrow(/cannot be combined with recursive/);
    expect(() => manager.spawn(spawnRequest(["valid"]))).toThrow(/explicit tools allow-list/);
    expect(() => manager.spawn(spawnRequest(["valid", "valid", "valid", "valid", "valid"], ["probe_echo"]))).toThrow(/at most 4/);

    const unpinned = managerFor(root, [extensionRoot]);
    expect(() => unpinned.spawn(spawnRequest([validPath], ["probe_echo"]))).toThrow(/extensionPins is empty or unset/);
    const empty = managerFor(root, [], undefined, []);
    expect(() => empty.spawn(spawnRequest([validPath], ["probe_echo"]))).toThrow(/extensionPins is empty or unset/);

    const changedPinManager = managerFor(root, undefined, undefined, [pins[0]!]);
    fs.appendFileSync(validPath, "// changed after host pin\n");
    expect(() => changedPinManager.spawn(spawnRequest(["valid"], ["probe_echo"])))
      .toThrow(/does not match its host-pinned SHA-256/);
    expect(launch).not.toHaveBeenCalled();
  });

  it("keeps the legacy process launch argv and environment unchanged when extensionPaths is absent", async () => {
    const root = temporary("fabric-extension-argv-");
    const captureFile = path.join(root, "capture.json");
    const workerPath = path.join(root, "capture-worker.mjs");
    const tempDirectory = path.join(root, "tmp");
    fs.mkdirSync(tempDirectory);
    fs.writeFileSync(workerPath, [
      'import crypto from "node:crypto";',
      'import fs from "node:fs";',
      'const entries = Object.entries(process.env).filter(([name]) => name !== "NODE_CHANNEL_FD" && name !== "NODE_UNIQUE_ID").sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);',
      'const envSha256 = crypto.createHash("sha256").update(JSON.stringify(entries)).digest("hex");',
      'fs.writeFileSync(process.env.FABRIC_EXTENSION_ENV_SNAPSHOT, JSON.stringify({ argv: process.argv.slice(2), envSha256 }));',
    ].join("\n"), { mode: 0o600 });
    const arguments_ = ["--runner", "pi", "--extensions", "true", "--tools", '["probe_echo"]'];
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", '["main-only"]');
    vi.stubEnv("PI_FABRIC_PINNED_EXTENSION", "/host/pinned-extension.mjs");
    vi.stubEnv("PI_FABRIC_EXTENSION_MODEL", "host/provider");
    vi.stubEnv("FABRIC_EXTENSION_ENV_SNAPSHOT", captureFile);
    vi.stubEnv("TMPDIR", tempDirectory);
    vi.stubEnv("SMARTY_ROLE", "main@0123456789ab");

    const expectedEnvironment = childProcessEnvironment(applyTaskReturnAddress(taskAgentEnvironment(), arguments_));
    const transport = new ProcessTransport();
    const handle = await transport.launch({
      id: "extension-argv-regression",
      name: "extension argv regression",
      cwd: root,
      workerPath,
      workerArguments: arguments_,
    });
    await handle.waitForClose?.();
    const captured = JSON.parse(fs.readFileSync(captureFile, "utf8")) as { argv: string[]; envSha256: string };

    expect(captured.argv).toEqual(arguments_);
    expect(captured.envSha256).toBe(hashEnvironment(expectedEnvironment));
  });

  it("snapshots the unchanged Pi CLI extension argv and policy environment without extensionPaths", async () => {
    const root = temporary("fabric-extension-worker-snapshot-");
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir);
    const captureFile = path.join(root, "pi-capture.json");
    const piBinary = path.resolve("tests/fixtures/spawn-argv-capture-pi.mjs");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_AGENT_DIR", agentDir);
    vi.stubEnv("FABRIC_EXTENSION_ARGV_CAPTURE", captureFile);
    vi.stubEnv("HOME", root);
    const manager = managerFor(root, undefined, piBinary);
    const result = await manager.run({
      task: "Capture the ordinary extension-enabled child launch.",
      runner: "pi",
      transport: "process",
      extensions: true,
      tools: ["read", "bash", "edit", "write"],
      model: "fake/fake-model",
      thinking: "off",
    });
    expect(result.status, result.error).toBe("completed");
    const captured = JSON.parse(fs.readFileSync(captureFile, "utf8")) as {
      argv: string[];
      environment: Record<string, string | null>;
    };
    const argsSnapshot: string[][] = [];
    for (let index = 0; index < captured.argv.length; index++) {
      const argument = captured.argv[index]!;
      if (argument === "-e") argsSnapshot.push([argument, path.basename(captured.argv[++index] ?? "")]);
      else if (argument === "--no-extensions") argsSnapshot.push([argument]);
      else if (["--tools", "--no-tools", "--model", "--thinking"].includes(argument)) {
        argsSnapshot.push([argument, captured.argv[index + 1] ?? ""]);
      }
    }
    expect(argsSnapshot).toEqual([
      ["--no-extensions"],
      ["-e", "index.js"],
      ["-e", "principal-delivery.js"],
      ["-e", "session-id.js"],
      ["-e", "actor-bash-hook.js"],
      ["-e", "sender-headers.js"],
      ["--tools", "read,bash,edit,write"],
      ["--model", "fake/fake-model"],
      ["--thinking", "off"],
    ]);
    expect(captured.environment).toEqual({
      PI_CODING_AGENT_DIR: agentDir,
      PI_FABRIC_FULL_CODE_MODE: "false",
      PI_FABRIC_TOOL_ALLOWLIST: '["read","bash","edit","write"]',
      PI_FABRIC_PINNED_EXTENSION: path.resolve("dist/index.js"),
      PI_FABRIC_EXTENSION_MODEL: null,
      PI_FABRIC_PI_BINARY: piBinary,
    });
  });
});
