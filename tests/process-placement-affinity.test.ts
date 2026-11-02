import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { corpusRemotePreflightEligible, preflightFleetPlacement, placementLocalPathReason } from "../src/agents/transports/placement-preflight.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { AgentInputError } from "../src/agents/input-validation.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(options: { candidates?: string[]; pickerExit?: number; sshExit?: number; executeRemote?: boolean } = {}) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-affinity-")); roots.push(base);
  const home = path.join(base, "home"); fs.mkdirSync(home);
  vi.spyOn(os, "homedir").mockReturnValue(home);
  // The test runner allocates outside /srv/scratch; caller TMPDIR is private, not the portable workspace.
  const callerTmp = path.join(base, "caller-tmp"); fs.mkdirSync(callerTmp);
  vi.stubEnv("TMPDIR", callerTmp);
  const factory = path.join(home, ".local", "share", "smarty-dev", "factory");
  const releaseId = "a".repeat(40);
  const root = path.join(factory, "releases", releaseId);
  fs.mkdirSync(path.dirname(path.join(root, "bin", "placeholder")), { recursive: true });
  fs.symlinkSync(path.join("releases", releaseId), path.join(factory, "current"), "dir");
  const mkdir = (file: string) => fs.mkdirSync(path.dirname(file), { recursive: true });
  const write = (file: string, text: string) => { mkdir(file); fs.writeFileSync(file, text); };
  write(path.join(root, "setup/factory/task-preflight.py"), [
    "import sys",
    "args = sys.argv",
    "if '--probe' in args: sys.exit(0)",
    "task = sys.stdin.read()",
    "count_at = args.index('--admit') + 3",
    "requires = int(args[count_at])",
    "paths = args[count_at + 1:count_at + 1 + requires]",
    "needs_at = count_at + 1 + requires",
    "pin_count = int(args[needs_at])",
    "needs = args[needs_at + 1 + pin_count:]",
    "corpus_path = '/private-corpus/'",
    "if 'corpus' in needs:",
    "    result = ['', 'corpus', 'explicit corpus need', '0']",
    "elif 'github' in needs:",
    "    result = ['', 'github', 'explicit github need', '0']",
    "elif corpus_path in task or any(corpus_path in file for file in paths):",
    "    result = ['', 'corpus', 'path-derived corpus input', '0']",
    "else:",
    "    result = ['', '', '', '0'] + [p for file in paths for p in [file, file]]",
    "sys.stdout.write('\\0'.join(result) + '\\0')",
  ].join("\n"));
  const bin = path.join(root, "bin"); fs.mkdirSync(bin, { recursive: true });
  const fleet = path.join(bin, "smarty-task-ryzen2");
  write(fleet, "#!/bin/sh\nprintf 'called\\n' >> \"$LAUNCH_CALLS\"\nprintf '%s\\n' \"$@\" > \"$LAUNCH_ARGS\"\nprintf 'RYZEN2_TASK_ACCEPTED affinity on ryzen2\\n'\n"); fs.chmodSync(fleet, 0o700);
  const picker = path.join(base, "smarty-pick-host-fixture");
  write(picker, `#!/bin/sh\nprintf 'called\\n' >> "$PICKER_CALLS"\nprintf '%s\\n' "$CANDIDATES"\nprintf 'eligible0=1 eligible1=1\\n' >&2\nexit "${options.pickerExit ?? 0}"\n`); fs.chmodSync(picker, 0o700);
  const pathBin = path.join(base, "path"); fs.mkdirSync(pathBin);
  const python = path.join(pathBin, "python3");
  write(python, "#!/bin/sh\nexec /usr/bin/python3 \"$@\"\n"); fs.chmodSync(python, 0o700);
  const ssh = path.join(pathBin, "ssh");
  write(ssh, `#!/bin/sh\nprintf 'called\\n' >> "$SSH_CALLS"\n${options.executeRemote
    ? 'for remote in "$@"; do :; done\nexec sh -c "$remote"'
    : `${options.sshExit === 3 ? "printf 'PREFLIGHT_MISSING 1\\n'" : ":"}\nexit "${options.sshExit ?? 0}"`}\n`); fs.chmodSync(ssh, 0o700);
  const cwd = path.join(base, "workspace"); fs.mkdirSync(cwd);
  const config = normalizeFabricConfig({ agents: { placement: {
    default: "remote", command: [path.join(factory, "current", "bin", "smarty-task-ryzen2"), "--host", "auto", "--cwd", cwd, "--", "task"],
    resultDirectory: path.join(base, "results", "{id}"), cancelCommand: ["true"], commandTimeoutMs: 1_000,
    sshAliases: { intel1: "intel1-agent", ryzen2: "r2-agent", ryzen3: "r3-agent", qs32729: "qs32729-agent" }, sharedMounts: [cwd], pickerCommand: [picker],
  } } }).agents.placement!;
  const env = {
    ...process.env, PATH: `${pathBin}:${process.env.PATH}`,
    CANDIDATES: (options.candidates ?? ["ryzen2", "ryzen3"]).join("\n"),
    SSH_CALLS: path.join(base, "ssh-calls"), PICKER_CALLS: path.join(base, "picker-calls"), LAUNCH_CALLS: path.join(base, "launch-calls"), LAUNCH_ARGS: path.join(base, "launch-args"),
  };
  const request = (cwdPath = cwd, needs: string[] = [], withRequires = true): AgentTransportLaunch => ({
    id: "affinity", name: "affinity", cwd: cwdPath, workerPath: "unused", workerArguments: [], needs,
    ...(withRequires ? { requires: [path.join(base, "target-only", "corpus.dat")] } : {}),
  });
  return { base, home, factory, root, cwd, callerTmp, config, env, request, picker, sshCalls: env.SSH_CALLS, pickerCalls: env.PICKER_CALLS, launchCalls: env.LAUNCH_CALLS };
}

/** Model a secure mounted parent for the fixture while keeping its files inside the test's private TMPDIR. */
function trustTemporaryFixtureAncestors(base: string) {
  const ancestors = new Set<string>();
  for (let directory = path.dirname(fs.realpathSync(base)); ; directory = path.dirname(directory)) {
    ancestors.add(directory);
    if (directory === path.parse(directory).root) break;
  }
  const uid = typeof process.getuid === "function" ? process.getuid() : 0;
  const original = fs.lstatSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: { toString(): string }) => {
    const info = original(file.toString());
    if (!ancestors.has(path.resolve(file.toString()))) return info;
    return new Proxy(info, { get(target, property) {
      if (property === "uid") return target.uid === 0 ? 0 : uid;
      if (property === "mode") return target.mode & ~0o022;
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    } }) as fs.Stats;
  }) as unknown as typeof fs.lstatSync);
}

function localWorkerRequest(f: ReturnType<typeof fixture>, cwd = f.cwd) {
  for (const key of ["PATH", "CANDIDATES", "SSH_CALLS", "PICKER_CALLS", "LAUNCH_CALLS", "LAUNCH_ARGS"] as const) vi.stubEnv(key, f.env[key]);
  const runDir = path.join(f.base, "run"); fs.mkdirSync(runDir);
  const taskFile = path.join(runDir, "task.txt"); fs.writeFileSync(taskFile, "portable task");
  const marker = path.join(runDir, "local-worker.txt");
  const workerPath = path.join(f.base, "worker.mjs");
  fs.writeFileSync(workerPath, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(marker)}, 'LOCAL');`);
  const events = path.join(runDir, "events.jsonl");
  const request = { ...f.request(cwd, [], false), workerPath, workerArguments: [
    "--task-file", taskFile, "--status-file", path.join(runDir, "status.json"), "--log-file", events, "--timeout-ms", "1000",
  ] };
  return { request, marker, events };
}

// The installed fleet protocol is POSIX-only; Windows has no /usr/bin/python3 or shell launcher.
describe.skipIf(process.platform === "win32")("installed fleet placement affinity preflight", () => {
  it("pins an automatic launch to the first actually eligible picker result", async () => {
    const f = fixture({ candidates: ["intel1", "ryzen2"] });
    const result = await preflightFleetPlacement(f.request(), f.config, f.config.command, "task", f.env);
    expect(result?.host).toBe("intel1");
    expect(result?.command.slice(result.command.indexOf("--host"), result.command.indexOf("--host") + 2)).toEqual(["--host", "intel1"]);
  });

  it("preflights corpus only on an allow-listed host selected by the trusted release", async () => {
    const f = fixture({ candidates: ["ryzen3", "ryzen2"] });
    f.config.corpusRemoteHosts = ["ryzen2"];
    expect(corpusRemotePreflightEligible(f.config, f.cwd)).toBe(false);
    trustTemporaryFixtureAncestors(f.base);
    const releaseBin = path.join(f.root, "bin");
    fs.chmodSync(releaseBin, 0o770);
    expect(corpusRemotePreflightEligible(f.config, f.cwd)).toBe(false);
    fs.chmodSync(releaseBin, 0o700);
    expect(corpusRemotePreflightEligible(f.config, f.cwd)).toBe(true);
    const current = path.join(f.factory, "current");
    fs.unlinkSync(current);
    fs.symlinkSync(path.join("..", "outside-release"), current, "dir");
    expect(corpusRemotePreflightEligible(f.config, f.cwd)).toBe(false);
    fs.unlinkSync(current);
    fs.symlinkSync(path.join("releases", path.basename(f.root)), current, "dir");
    expect(corpusRemotePreflightEligible(f.config, f.cwd)).toBe(true);
    expect(corpusRemotePreflightEligible(f.config, f.cwd, "ryzen2")).toBe(true);
    expect(corpusRemotePreflightEligible(f.config, f.cwd, "ryzen3")).toBe(false);
    const request = f.request(f.cwd, ["corpus"], false);

    const result = await preflightFleetPlacement(request, f.config, f.config.command, "portable task", f.env);

    expect(result?.host).toBe("ryzen2");
    expect(result?.command.slice(result.command.indexOf("--host"), result.command.indexOf("--host") + 2)).toEqual(["--host", "ryzen2"]);
    expect(request.needs).toEqual(["corpus"]);
    expect(fs.readFileSync(f.sshCalls, "utf8")).toBe("called\n");
    expect(fs.existsSync(f.launchCalls)).toBe(false);
  });

  it.each(["local-bin", "temporary"] as const)("keeps corpus local for a %s launcher lookalike and never starts it", async location => {
    const f = fixture();
    f.config.corpusRemoteHosts = ["ryzen2"];
    const launcher = location === "local-bin"
      ? path.join(f.home, ".local", "bin", "smarty-task-ryzen2")
      : path.join(f.base, "custom-bin", "smarty-task-ryzen2");
    fs.mkdirSync(path.dirname(launcher), { recursive: true });
    fs.writeFileSync(launcher, [
      "#!/bin/sh",
      "printf 'called\\n' >> \"$LAUNCH_CALLS\"",
      "printf 'RYZEN2_TASK_ACCEPTED lookalike on ryzen2\\n'",
      "",
    ].join("\n"));
    fs.chmodSync(launcher, 0o700);
    const config = { ...f.config, command: [launcher, ...f.config.command.slice(1)] };
    expect(corpusRemotePreflightEligible(config, f.cwd)).toBe(false);
    const local = localWorkerRequest(f);
    local.request.needs = ["corpus"];

    const handle = await new ProcessTransport(undefined, config).launch(local.request);
    await handle.waitForClose?.();

    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    expect(fs.readFileSync(local.events, "utf8")).toContain('"reason":"unmet needs: corpus"');
    expect(fs.existsSync(f.launchCalls)).toBe(false);
    expect(fs.existsSync(f.sshCalls)).toBe(false);
  });

  it("keeps corpus local when the qualified-host allow-list is empty", async () => {
    const f = fixture();
    const local = localWorkerRequest(f);
    local.request.needs = ["corpus"];
    const handle = await new ProcessTransport(undefined, f.config).launch(local.request);
    await handle.waitForClose?.();

    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    expect(fs.readFileSync(local.events, "utf8")).toContain('"reason":"unmet needs: corpus"');
    for (const file of [f.pickerCalls, f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it("keeps an explicit corpus target local when it is outside the qualified-host allow-list", async () => {
    const f = fixture({ candidates: ["ryzen2", "ryzen3"] });
    f.config.corpusRemoteHosts = ["ryzen2"];
    trustTemporaryFixtureAncestors(f.base);
    const command = [...f.config.command]; command[command.indexOf("--host") + 1] = "ryzen3";
    const config = { ...f.config, command };
    const local = localWorkerRequest(f);
    local.request.needs = ["corpus"];
    const handle = await new ProcessTransport(undefined, config).launch(local.request);
    await handle.waitForClose?.();

    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    expect(fs.readFileSync(local.events, "utf8")).toContain("host ryzen3 is not in corpusRemoteHosts");
    expect(fs.readFileSync(f.pickerCalls, "utf8")).toBe("called\n");
    expect(fs.existsSync(f.sshCalls)).toBe(false);
    expect(fs.existsSync(f.launchCalls)).toBe(false);
  });

  it("passes corpus to the helper by default, preserving the explicit location check", async () => {
    const f = fixture();
    expect(f.config.corpusRemoteHosts).toEqual([]);
    await expect(preflightFleetPlacement(f.request(f.cwd, ["corpus"], false), f.config, f.config.command, "portable task", f.env))
      .rejects.toThrow("needs corpus: explicit corpus need");
    expect(fs.existsSync(f.sshCalls)).toBe(false);
  });

  it("preserves the explicit github need in fleet preflight", async () => {
    const f = fixture();
    await expect(preflightFleetPlacement(f.request(f.cwd, ["github"], false), f.config, f.config.command, "portable task", f.env))
      .rejects.toThrow("needs github: explicit github need");
    expect(fs.existsSync(f.sshCalls)).toBe(false);
  });

  it.each(["prompt", "requires"] as const)("still refuses a path-derived corpus input in %s during fleet preflight", async location => {
    const f = fixture();
    f.config.corpusRemoteHosts = ["ryzen2"];
    trustTemporaryFixtureAncestors(f.base);
    const corpusPath = path.join(f.base, "private-corpus", "secret.txt");
    const request = {
      ...f.request(f.cwd, ["corpus"], false),
      ...(location === "requires" ? { requires: [corpusPath] } : {}),
    };
    const task = location === "prompt" ? `Inspect ${corpusPath}` : "Inspect the supplied input";

    await expect(preflightFleetPlacement(request, f.config, f.config.command, task, f.env))
      .rejects.toThrow("needs corpus: path-derived corpus input");
    expect(fs.existsSync(f.sshCalls)).toBe(false);
    expect(fs.existsSync(f.launchCalls)).toBe(false);
  });

  it.each(["local", "github"] as const)("keeps needs %s on the existing local path before fleet preflight", async need => {
    const f = fixture();
    const local = localWorkerRequest(f);
    local.request.needs = [need];

    const handle = await new ProcessTransport(undefined, f.config).launch(local.request);
    await handle.waitForClose?.();

    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    expect(fs.readFileSync(local.events, "utf8")).toContain('"type":"placement.local"');
    expect(fs.existsSync(f.pickerCalls)).toBe(false);
    expect(fs.existsSync(f.sshCalls)).toBe(false);
    expect(fs.existsSync(f.launchCalls)).toBe(false);
  });

  it.each(["ineligible", "missing", "nonzero", "empty", "timeout"] as const)("runs a real local worker when picker admission is %s", async kind => {
    const f = fixture({ candidates: kind === "empty" ? [] : ["intel1", "ryzen2"], pickerExit: kind === "nonzero" ? 3 : 0 });
    const command = [...f.config.command];
    if (kind === "ineligible") command[command.indexOf("--host") + 1] = "qs32729";
    const config = { ...f.config, command, ...(kind === "timeout" ? { commandTimeoutMs: 100 } : {}),
      ...(kind === "missing" ? { pickerCommand: [path.join(f.base, "absent-picker")] } : {}) };
    if (kind === "timeout") fs.writeFileSync(f.picker, "#!/bin/sh\nsleep 5\n");
    vi.stubEnv("CANDIDATES", f.env.CANDIDATES); vi.stubEnv("PATH", f.env.PATH); vi.stubEnv("SSH_CALLS", f.sshCalls); vi.stubEnv("PICKER_CALLS", f.pickerCalls);
    const runDir = path.join(f.base, "run"); fs.mkdirSync(runDir);
    const taskFile = path.join(runDir, "task.txt"); fs.writeFileSync(taskFile, "portable task");
    const marker = path.join(runDir, "local-worker.txt");
    const workerPath = path.join(f.base, "worker.mjs");
    fs.writeFileSync(workerPath, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(marker)}, 'LOCAL');`);
    const request = { ...f.request(f.cwd, [], false), workerPath, workerArguments: [
      "--task-file", taskFile, "--status-file", path.join(runDir, "status.json"), "--log-file", path.join(runDir, "events.jsonl"), "--timeout-ms", "1000",
    ] };
    const started = Date.now();
    const handle = await new ProcessTransport(undefined, config).launch(request);
    await handle.waitForClose?.();
    expect(Date.now() - started).toBeLessThan(10_000);
    expect(fs.readFileSync(marker, "utf8")).toBe("LOCAL");
    expect(fs.existsSync(f.sshCalls)).toBe(false);
    const events = fs.readFileSync(path.join(runDir, "events.jsonl"), "utf8");
    expect(events).toContain("placement.local");
    expect(events).toContain("placement-picker-local:");
    if (kind === "ineligible") expect(events).toContain("host qs32729 is not picker-eligible");
  });

  it("refuses an explicit host omitted from the live eligible list before SSH", async () => {
    const f = fixture({ candidates: ["intel1", "ryzen2"] });
    const command = [...f.config.command]; command[command.indexOf("--host") + 1] = "qs32729";
    await expect(preflightFleetPlacement(f.request(), f.config, command, "task", f.env)).rejects.toThrow(/qs32729/);
    expect(fs.existsSync(f.sshCalls)).toBe(false);
  });

  it.each([{ candidates: [] }, { candidates: ["intel1"], pickerExit: 3 }])("fails closed when live picker yields no usable result", async options => {
    const f = fixture(options);
    await expect(preflightFleetPlacement(f.request(), f.config, f.config.command, "task", f.env)).rejects.toThrow(/host=auto/);
    expect(fs.existsSync(f.sshCalls)).toBe(false);
  });

  it("reports the selected host and exact absent required path before launch", async () => {
    const f = fixture({ candidates: ["ryzen3"], sshExit: 3 });
    const started = Date.now();
    const preflight = preflightFleetPlacement(f.request(), f.config, f.config.command, "task", f.env);
    await expect(preflight).rejects.toThrow(`host=ryzen3: missing path ${f.request().requires![0]}`);
    await expect(preflight).rejects.toThrow("needs:['local']");
    expect(Date.now() - started).toBeLessThan(10_000);
    expect(fs.readFileSync(f.sshCalls, "utf8")).toBe("called\n");
  });

  it("bounds a hung read-only picker and never starts SSH", async () => {
    const f = fixture();
    const picker = f.picker;
    fs.writeFileSync(picker, "#!/bin/sh\nsleep 5\n"); fs.chmodSync(picker, 0o700);
    const started = Date.now();
    await expect(preflightFleetPlacement(f.request(), { ...f.config, commandTimeoutMs: 100 }, f.config.command, "task", f.env)).rejects.toThrow(/host=auto/);
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(fs.existsSync(f.sshCalls)).toBe(false);
  });

  it("keeps caller TMPDIR beneath a declared shared ancestor local before any remote subprocess", async () => {
    const f = fixture();
    const cwd = path.join(f.callerTmp, "task"); fs.mkdirSync(cwd);
    const local = localWorkerRequest(f, cwd);
    const handle = await new ProcessTransport(undefined, { ...f.config, sharedMounts: [f.base] }).launch(local.request);
    await handle.waitForClose?.();
    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    expect(fs.readFileSync(local.events, "utf8")).toContain("caller-local cwd");
    for (const file of [f.pickerCalls, f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it("keeps a portable shared Git repo remote after the same target-cwd SSH check", async () => {
    const f = fixture({ executeRemote: true });
    execFileSync("git", ["init", "--quiet", f.cwd]);
    const local = localWorkerRequest(f);
    const result = path.join(f.base, "results", "affinity"); fs.mkdirSync(result, { recursive: true });
    fs.writeFileSync(path.join(result, "rc"), "0"); fs.writeFileSync(path.join(result, "result.md"), "REMOTE");
    const handle = await new ProcessTransport(undefined, { ...f.config, sharedMounts: [f.base] }).launch(local.request);
    expect(handle.sessionId).toBe("placement:affinity");
    expect(await handle.isAlive()).toBe(false);
    expect(fs.readFileSync(local.events, "utf8")).toContain('"type":"placement.remote"');
    expect(fs.existsSync(local.marker)).toBe(false);
    for (const file of [f.pickerCalls, f.sshCalls, f.launchCalls]) expect(fs.readFileSync(file, "utf8")).toBe("called\n");
  });

  it("refuses a target-only missing cwd as known-unlaunched before the launcher or local worker", async () => {
    const f = fixture({ executeRemote: true });
    const cwd = path.join(f.base, "target-only", "missing-cwd");
    const local = localWorkerRequest(f, cwd);
    const config = { ...f.config, command: f.config.command.map(entry => entry === f.cwd ? "{cwd}" : entry) };
    const started = Date.now();
    const pending = new ProcessTransport(undefined, config).launch(local.request);
    await expect(pending).rejects.toBeInstanceOf(AgentInputError);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(cwd);
    await expect(pending).rejects.toThrow("needs:['local']");
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(fs.readFileSync(f.sshCalls, "utf8")).toBe("called\n");
    expect(fs.existsSync(f.launchCalls)).toBe(false);
    expect(fs.existsSync(local.marker)).toBe(false);
  });

  it("fences lexical and canonical local-root descendants, but not prefix siblings", () => {
    const f = fixture();
    const home = f.home;
    const state = path.join(home, ".local", "state"); fs.mkdirSync(state, { recursive: true });
    const config = { ...f.config, sharedMounts: [f.base] };
    for (const root of [f.callerTmp, state]) {
      const sibling = `${root}-sibling`; fs.mkdirSync(sibling);
      expect(placementLocalPathReason(sibling, config)).toBeUndefined();
      expect(placementLocalPathReason(root, config)).toContain("caller-local cwd");
      expect(placementLocalPathReason(path.join(root, "missing", "child"), config)).toContain("caller-local cwd");
      const alias = `${root}-alias`; fs.symlinkSync(root, alias, "dir");
      expect(placementLocalPathReason(alias, config)).toContain("caller-local cwd");
      expect(placementLocalPathReason(path.join(alias, "missing", "child"), config)).toContain("caller-local cwd");
      const escape = path.join(root, "escape"); fs.symlinkSync(f.cwd, escape, "dir");
      expect(placementLocalPathReason(escape, config)).toContain("caller-local cwd");
    }
    const danglingAlias = path.join(f.base, "missing-tmp-alias");
    fs.symlinkSync(path.join(f.callerTmp, "not-created"), danglingAlias, "dir");
    expect(placementLocalPathReason(path.join(danglingAlias, "child"), config)).toContain("caller-local cwd");
    const tmpAlias = path.join(f.base, "tmp-alias"); fs.symlinkSync(f.callerTmp, tmpAlias, "dir");
    vi.stubEnv("TMPDIR", tmpAlias);
    expect(placementLocalPathReason(path.join(f.callerTmp, "missing"), config)).toContain("caller-local cwd");
    expect(placementLocalPathReason("/srv/scratch/missing/child", config)).toContain("caller-local cwd");
    expect(placementLocalPathReason("/srv/scratch-sibling/missing", config)).toBeUndefined();
  });

  it("pins shared-root paths but not prefix siblings or symlink escapes", () => {
    const f = fixture();
    const sibling = `${f.cwd}-sibling`; fs.mkdirSync(sibling);
    expect(placementLocalPathReason(sibling, f.config)).toContain("caller-local cwd");
    const outside = path.join(f.base, "outside"); fs.mkdirSync(outside);
    const escape = path.join(f.cwd, "escape");
    fs.symlinkSync(outside, escape, "dir");
    expect(placementLocalPathReason(escape, f.config)).toContain("caller-local cwd");
    expect(placementLocalPathReason(f.cwd, f.config)).toBeUndefined();
  });

  it("allows only portable shared-root declarations and ignores workspace placement overrides", () => {
    const f = fixture();
    const placement = {
      command: [f.config.command[0]], resultDirectory: path.join(f.base, "result", "{id}"), cancelCommand: ["cancel"],
      sharedMounts: [f.cwd], sshAliases: { ryzen2: "r2-agent" },
    };
    for (const sharedMounts of [["/"], ["relative"], ["/bad\nroot"]]) {
      expect(() => normalizeFabricConfig({ agents: { placement: { ...placement, sharedMounts } } })).toThrow();
    }
    const profile = path.join(f.base, "profile"); fs.mkdirSync(profile);
    fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ agents: { placement } }));
    const projectConfig = path.join(f.cwd, ".pi"); fs.mkdirSync(projectConfig);
    fs.writeFileSync(path.join(projectConfig, "fabric.json"), JSON.stringify({ agents: { placement: { ...placement, sharedMounts: ["/"] } } }));
    expect(loadFabricConfig({ cwd: f.cwd, agentDir: profile, projectTrusted: true }).agents.placement?.sharedMounts).toEqual([f.cwd]);
  });
});
