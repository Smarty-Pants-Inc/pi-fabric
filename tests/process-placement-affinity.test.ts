import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { preflightFleetPlacement, placementLocalPathReason } from "../src/agents/transports/placement-preflight.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { AgentInputError, RequiredInputMissingError } from "../src/agents/input-validation.js";
import * as processUtils from "../src/agents/transports/process-utils.js";

const roots: string[] = [];
beforeEach(() => {
  // Preserve the original strict regressions without consulting real user config.
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-home-")); roots.push(home);
  vi.stubEnv("HOME", home); vi.stubEnv("USERPROFILE", home);
  vi.spyOn(os, "homedir").mockReturnValue(home);
  vi.stubEnv("SMARTY_ALPHA", "0");
});
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(options: { candidates?: string[]; pickerExit?: number; pickerDelaySeconds?: number; sshExit?: number; executeRemote?: boolean; denied?: string; needs?: string; probeExit?: number } = {}) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-affinity-")); roots.push(base);
  // The test runner allocates outside /srv/scratch; caller TMPDIR is private, not the portable workspace.
  const callerTmp = path.join(base, "caller-tmp"); fs.mkdirSync(callerTmp);
  vi.stubEnv("TMPDIR", callerTmp);
  const root = path.join(base, "install");
  const mkdir = (file: string) => fs.mkdirSync(path.dirname(file), { recursive: true });
  const write = (file: string, text: string) => { mkdir(file); fs.writeFileSync(file, text); };
  write(path.join(root, "setup/factory/task-preflight.py"), [
    "import os, sys",
    "args = sys.argv",
    "with open(os.environ['ADMISSION_CALLS'], 'a') as calls: calls.write('probe\\n' if '--probe' in args else 'admit\\n')",
    `if '--probe' in args: sys.exit(${options.probeExit ?? 0})`,
    "sys.stdin.read()",
    "count_at = args.index('--admit') + 3",
    "requires = int(args[count_at])",
    "paths = args[count_at + 1:count_at + 1 + requires]",
    `sys.stdout.write('\\0'.join([${JSON.stringify(options.denied ?? "")}, ${JSON.stringify(options.needs ?? "")}, 'fixture policy', '0'] + [p for file in paths for p in [file, file]]) + '\\0')`,
  ].join("\n"));
  const bin = path.join(root, "bin"); fs.mkdirSync(bin, { recursive: true });
  const fleet = path.join(bin, "smarty-task-ryzen2");
  write(fleet, [
    "#!/bin/sh", 'printf \'called\\n\' >> "$LAUNCH_CALLS"', 'printf \'%s\\0\' "$@" > "$LAUNCH_ARGV"',
    'while [ "$#" -gt 0 ]; do', '  [ "$1" = "--" ] && break',
    '  if [ "$1" = "--host" ]; then host="$2"; break; fi', '  shift', "done",
    'printf \'RYZEN2_TASK_ACCEPTED affinity on %s\\n\' "$host"', "",
  ].join("\n")); fs.chmodSync(fleet, 0o700);
  const picker = path.join(base, "smarty-pick-host-fixture");
  write(picker, `#!/bin/sh\nprintf 'called\\n' >> "$PICKER_CALLS"\n${options.pickerDelaySeconds ? `sleep ${options.pickerDelaySeconds}\n` : ""}printf '%s\\n' "$CANDIDATES"\nprintf 'eligible0=1 eligible1=1\\n' >&2\nexit "${options.pickerExit ?? 0}"\n`); fs.chmodSync(picker, 0o700);
  const pathBin = path.join(base, "path"); fs.mkdirSync(pathBin);
  const python = path.join(pathBin, "python3");
  write(python, "#!/bin/sh\nexec /usr/bin/python3 \"$@\"\n"); fs.chmodSync(python, 0o700);
  const ssh = path.join(pathBin, "ssh");
  write(ssh, `#!/bin/sh\nprintf 'called\\n' >> "$SSH_CALLS"\n${options.executeRemote
    ? 'for remote in "$@"; do :; done\nexec sh -c "$remote"'
    : `${options.sshExit === 3 ? "printf 'PREFLIGHT_MISSING 1\\n'" : ":"}\nexit "${options.sshExit ?? 0}"`}\n`); fs.chmodSync(ssh, 0o700);
  const cwd = path.join(base, "workspace"); fs.mkdirSync(cwd);
  const config = normalizeFabricConfig({ agents: { placement: {
    default: "remote", command: [fleet, "--host", "auto", "--cwd", cwd, "--", "task"],
    resultDirectory: path.join(base, "results", "{id}"), cancelCommand: ["true"], commandTimeoutMs: 1_000,
    sshAliases: { intel1: "intel1-agent", ryzen2: "r2-agent", ryzen3: "r3-agent", qs32729: "qs32729-agent", "worker-a": "worker-a-agent" }, sharedMounts: [cwd], pickerCommand: [picker],
  } } }).agents.placement!;
  const env = {
    ...process.env, PATH: `${pathBin}:${process.env.PATH}`,
    CANDIDATES: (options.candidates ?? ["ryzen2", "ryzen3"]).join("\n"),
    SSH_CALLS: path.join(base, "ssh-calls"), PICKER_CALLS: path.join(base, "picker-calls"), LAUNCH_CALLS: path.join(base, "launch-calls"),
    ADMISSION_CALLS: path.join(base, "admission-calls"), LAUNCH_ARGV: path.join(base, "launch-argv"),
  };
  const request = (cwdPath = cwd, needs: string[] = [], withRequires = true): AgentTransportLaunch => ({
    id: "affinity", name: "affinity", cwd: cwdPath, workerPath: "unused", workerArguments: [], needs,
    ...(withRequires ? { requires: [path.join(base, "target-only", "corpus.dat")] } : {}),
  });
  return { base, root, cwd, callerTmp, config, env, request, picker, sshCalls: env.SSH_CALLS, pickerCalls: env.PICKER_CALLS, launchCalls: env.LAUNCH_CALLS, admissionCalls: env.ADMISSION_CALLS, launchArgv: env.LAUNCH_ARGV };
}

function localWorkerRequest(f: ReturnType<typeof fixture>, cwd = f.cwd) {
  for (const key of ["PATH", "CANDIDATES", "SSH_CALLS", "PICKER_CALLS", "LAUNCH_CALLS", "ADMISSION_CALLS", "LAUNCH_ARGV"] as const) vi.stubEnv(key, f.env[key]);
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

function remoteResult(f: ReturnType<typeof fixture>) {
  const directory = path.join(f.base, "results", "affinity"); fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "rc"), "0"); fs.writeFileSync(path.join(directory, "result.md"), "REMOTE");
}

// The installed fleet protocol is POSIX-only; Windows has no /usr/bin/python3 or shell launcher.
describe.skipIf(process.platform === "win32")("installed fleet placement affinity preflight", () => {
  // Trust boundary: exercise the actual transport and launcher argv, not just the mode helper.
  it.each([undefined, "1", "other", ""])("alpha=%s forwards home, dot-share and missing remote inputs unchanged without an input probe or local spawn", async mode => {
    const f = fixture({ candidates: ["worker-a"], denied: "fixture home input denied", sshExit: 3 });
    const local = localWorkerRequest(f);
    const start = vi.spyOn(processUtils, "spawnDetached");
    const homeFile = path.join(os.homedir(), "input file.txt"); fs.writeFileSync(homeFile, "fixture");
    const share = path.join(os.homedir(), ".local", "share", "fixture-data"); fs.mkdirSync(share, { recursive: true });
    const shareFile = path.join(share, "data.txt"); fs.writeFileSync(shareFile, "fixture");
    const alias = path.join(os.homedir(), "input-alias"); fs.symlinkSync(share, alias, "dir");
    const missing = path.join(f.base, "remote-only", "missing ' {host} ; $(literal)");
    const requires = [homeFile, share, shareFile, alias, path.join(alias, "data.txt"), missing];
    const task = `Read ${homeFile} and ${shareFile}\nKeep literal 'quotes', $HOME and {host}; missing input: ${missing}`;
    fs.writeFileSync(local.request.workerArguments[1]!, task);
    const request = { ...local.request, requires };
    const config = { ...f.config, command: f.config.command.map(entry => entry === "task" ? "{task}" : entry) };
    remoteResult(f);
    vi.stubEnv("SMARTY_ALPHA", mode);
    const handle = await new ProcessTransport(undefined, config).launch(request);
    expect(handle.sessionId).toBe("placement:affinity");
    expect(await handle.isAlive()).toBe(false);
    expect(fs.readFileSync(f.launchArgv, "utf8").split("\0").slice(0, -1)).toEqual([
      "--host", "worker-a", "--cwd", f.cwd, ...requires.flatMap(file => ["--require", file]), "--", task,
    ]);
    expect(request.requires).toEqual(requires);
    expect(start).not.toHaveBeenCalled();
    expect(fs.existsSync(local.marker)).toBe(false);
    expect(fs.existsSync(missing)).toBe(false);
    expect(fs.readFileSync(local.events, "utf8")).not.toContain('"type":"placement.local"');
    for (const file of [f.pickerCalls, f.launchCalls]) expect(fs.readFileSync(file, "utf8")).toBe("called\n");
    for (const file of [f.admissionCalls, f.sshCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it.each(["required-refusal", "nonzero", "mismatched-receipt"] as const)("alpha leaves external launcher %s authoritative without a local retry", async outcome => {
    const f = fixture({ candidates: ["worker-a"], denied: "adapter-only refusal" });
    const local = localWorkerRequest(f);
    const start = vi.spyOn(processUtils, "spawnDetached");
    const missing = path.join(f.base, "remote-only", "missing");
    vi.stubEnv("REFUSED_INPUT", missing);
    fs.writeFileSync(f.config.command[0]!, [
      "#!/bin/sh", 'printf \'called\\n\' >> "$LAUNCH_CALLS"', 'printf \'%s\\0\' "$@" > "$LAUNCH_ARGV"',
      outcome === "required-refusal" ? 'printf \'PREFLIGHT_REFUSED: %s: unavailable on target\\n\' "$REFUSED_INPUT"'
        : outcome === "nonzero" ? "printf 'factory-owned refusal\\n' >&2"
        : "printf 'RYZEN2_TASK_ACCEPTED affinity on worker-b\\n'",
      `exit ${outcome === "mismatched-receipt" ? 0 : 3}`, "",
    ].join("\n"));
    vi.stubEnv("SMARTY_ALPHA", "1");
    const debt = vi.fn();
    const pending = new ProcessTransport(undefined, f.config).launch({ ...local.request, requires: [missing], onUnconfirmedExit: debt });
    if (outcome === "required-refusal") {
      await expect(pending).rejects.toBeInstanceOf(RequiredInputMissingError);
      await expect(pending).rejects.toMatchObject({ path: missing, index: 0, launchOutcome: "unlaunched" });
      expect(debt).not.toHaveBeenCalled();
    } else {
      const handle = await pending;
      expect(handle.lostContact?.()).toContain("Placement launch outcome unknown");
      expect(debt).toHaveBeenCalledOnce();
      expect(await handle.isAlive()).toBe(false);
      expect(fs.readFileSync(local.events, "utf8")).toContain('"type":"placement.launch_unknown"');
    }
    expect(fs.readFileSync(f.launchArgv, "utf8").split("\0").slice(0, -1)).toEqual([
      "--host", "worker-a", "--cwd", f.cwd, "--require", missing, "--", "task",
    ]);
    expect(fs.readFileSync(f.launchCalls, "utf8")).toBe("called\n");
    expect(fs.existsSync(local.events) ? fs.readFileSync(local.events, "utf8") : "").not.toContain('"type":"placement.local"');
    for (const file of [f.admissionCalls, f.sshCalls, local.marker]) expect(fs.existsSync(file)).toBe(false);
    expect(start).not.toHaveBeenCalled();
  });

  it("uses live env and disable-file changes on consecutive operations of the same transport", async () => {
    const f = fixture({ candidates: ["worker-a"], denied: "fixture input denied" });
    const local = localWorkerRequest(f);
    const start = vi.spyOn(processUtils, "spawnDetached");
    const missing = path.join(f.base, "remote-only", "missing");
    const request = { ...local.request, requires: [missing] };
    const transport = new ProcessTransport(undefined, f.config);
    remoteResult(f);
    const marker = path.join(os.homedir(), ".config", "smarty", "bash-guard.disabled");
    for (const step of ["alpha", "strict", "file", "restored", "other"] as const) {
      vi.stubEnv("SMARTY_ALPHA", step === "alpha" ? "1" : step === "other" ? "other" : "0");
      if (step === "file") { fs.mkdirSync(path.dirname(marker), { recursive: true }); fs.writeFileSync(marker, ""); }
      if (step === "restored") fs.unlinkSync(marker);
      if (step === "strict" || step === "restored") {
        await expect(transport.launch(request)).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
      } else {
        const handle = await transport.launch(request);
        expect(await handle.isAlive()).toBe(false);
        expect(fs.readFileSync(f.launchArgv, "utf8").split("\0").slice(0, -1)).toEqual([
          "--host", "worker-a", "--cwd", f.cwd, "--require", missing, "--", "task",
        ]);
      }
    }
    expect(fs.readFileSync(f.admissionCalls, "utf8")).toBe("admit\nadmit\n");
    expect(fs.readFileSync(f.launchCalls, "utf8")).toBe("called\ncalled\ncalled\n");
    expect(fs.readFileSync(f.pickerCalls, "utf8")).toBe("called\n".repeat(5));
    expect(fs.existsSync(f.sshCalls)).toBe(false);
    expect(fs.existsSync(local.marker)).toBe(false);
    expect(start).not.toHaveBeenCalled();
  });

  it("reads the current process mode rather than the supplied launcher environment snapshot", async () => {
    const f = fixture({ candidates: ["worker-a"], denied: "fixture input denied" });
    vi.stubEnv("SMARTY_ALPHA", "1");
    const prepared = await preflightFleetPlacement(f.request(), f.config, f.config.command, "task", { ...f.env, SMARTY_ALPHA: "0" });
    expect(prepared?.host).toBe("worker-a");
    expect(fs.existsSync(f.admissionCalls)).toBe(false);
    vi.stubEnv("SMARTY_ALPHA", "0");
    await expect(preflightFleetPlacement(f.request(), f.config, f.config.command, "task", { ...f.env, SMARTY_ALPHA: "1" })).rejects.toThrow("fixture input denied");
    expect(fs.readFileSync(f.admissionCalls, "utf8")).toBe("admit\n");
    expect(fs.existsSync(f.sshCalls)).toBe(false);
    expect(fs.existsSync(f.launchCalls)).toBe(false);
  });

  it.each([
    { kind: "denied", denied: "fixture home input denied", message: "fixture home input denied", ssh: false },
    { kind: "needs", needs: "corpus", message: "needs corpus", ssh: false },
    { kind: "probe", probeExit: 3, message: "Read-only SSH preflight failed", ssh: true },
    { kind: "missing", message: "missing path", ssh: true },
  ])("strict zero retains $kind input refusal before any launcher or local spawn", async options => {
    const f = fixture({ ...options, candidates: ["worker-a"], executeRemote: true });
    const local = localWorkerRequest(f);
    const start = vi.spyOn(processUtils, "spawnDetached");
    const homeInput = path.join(os.homedir(), ".local", "share", "strict-input.txt");
    fs.mkdirSync(path.dirname(homeInput), { recursive: true }); fs.writeFileSync(homeInput, "fixture");
    const request = { ...local.request, requires: [...f.request().requires!, homeInput] };
    const pending = new ProcessTransport(undefined, f.config).launch(request);
    await expect(pending).rejects.toBeInstanceOf(options.kind === "missing" ? RequiredInputMissingError : AgentInputError);
    await expect(pending).rejects.toMatchObject({ launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(options.message);
    expect(fs.readFileSync(f.admissionCalls, "utf8")).toBe(options.ssh ? "admit\nprobe\n" : "admit\n");
    expect(fs.existsSync(f.sshCalls)).toBe(options.ssh);
    expect(fs.existsSync(f.launchCalls)).toBe(false);
    expect(fs.existsSync(local.marker)).toBe(false);
    expect(start).not.toHaveBeenCalled();
  });

  it.each(["cancelled", "deadline"] as const)("alpha still refuses %s after host selection without probing or launching", async reason => {
    const f = fixture({ candidates: ["worker-a"] });
    vi.stubEnv("SMARTY_ALPHA", "1");
    const controller = new AbortController();
    if (reason === "cancelled") controller.abort();
    await expect(preflightFleetPlacement({ ...f.request(), signal: controller.signal }, f.config, f.config.command, "task", f.env,
      reason === "deadline" ? Date.now() - 1 : undefined, "worker-a")).rejects.toThrow("Preflight cancelled or deadline expired");
    for (const file of [f.admissionCalls, f.sshCalls, f.launchCalls, f.pickerCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it("alpha preserves a preselected host and adds a missing host pin without rerunning the picker", async () => {
    const f = fixture({ candidates: [] });
    vi.stubEnv("SMARTY_ALPHA", "1");
    const command = [f.config.command[0]!, "--cwd", f.cwd, "--", "task"];
    const prepared = await preflightFleetPlacement(f.request(), f.config, command, "task", f.env, undefined, "worker-a");
    expect(prepared).toEqual({ host: "worker-a", command: [command[0], "--cwd", f.cwd, "--host", "worker-a", "--", "task"], environment: f.env });
    expect(command).toEqual([f.config.command[0], "--cwd", f.cwd, "--", "task"]);
    for (const file of [f.admissionCalls, f.sshCalls, f.launchCalls, f.pickerCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it("alpha keeps picker eligibility and trusted host aliases in force", async () => {
    const f = fixture({ candidates: ["worker-a"] });
    vi.stubEnv("SMARTY_ALPHA", "1");
    const command = [...f.config.command]; command[command.indexOf("--host") + 1] = "worker-b";
    await expect(preflightFleetPlacement(f.request(), f.config, command, "task", f.env)).rejects.toThrow("not picker-eligible");
    await expect(preflightFleetPlacement(f.request(), { ...f.config, sshAliases: {} }, f.config.command, "task", f.env, undefined, "worker-a")).rejects.toThrow("trusted placement sshAliases");
    for (const file of [f.admissionCalls, f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it("pins an automatic launch to the first actually eligible picker result", async () => {
    const f = fixture({ candidates: ["intel1", "ryzen2"] });
    const result = await preflightFleetPlacement(f.request(), f.config, f.config.command, "task", f.env);
    expect(result?.host).toBe("intel1");
    expect(result?.command.slice(result.command.indexOf("--host"), result.command.indexOf("--host") + 2)).toEqual(["--host", "intel1"]);
  });

  // Recorded catch: #1830's shared 5 s preflight budget forced every loaded-fleet spawn LOCAL.
  it.each([
    { source: false, pickerTimeoutMs: undefined },
    { source: true, pickerTimeoutMs: 10_000 },
  ])("keeps a picker slower than 5 s remote with its own budget (source=$source)", async ({ source, pickerTimeoutMs }) => {
    const f = fixture({ executeRemote: true, pickerDelaySeconds: 5.5 });
    execFileSync("git", ["init", "--quiet", f.cwd]);
    const local = localWorkerRequest(f);
    const config = normalizeFabricConfig({ agents: { placement: {
      ...f.config, commandTimeoutMs: 30_000, pickerTimeoutMs,
      sharedMounts: source ? [] : [f.cwd],
      command: f.config.command.map(entry => entry === "--cwd" && source ? "--src" : entry === f.cwd ? "{cwd}" : entry),
    } } }).agents.placement!;
    const result = path.join(f.base, "results", "affinity"); fs.mkdirSync(result, { recursive: true });
    fs.writeFileSync(path.join(result, "rc"), "0"); fs.writeFileSync(path.join(result, "result.md"), "REMOTE");
    const started = Date.now();
    const handle = await new ProcessTransport(undefined, config).launch(local.request);
    await handle.waitForClose?.();
    expect(handle.sessionId).toBe("placement:affinity");
    expect(Date.now() - started).toBeGreaterThan(5_000);
    expect(await handle.isAlive()).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(f.base, "run", "status.json"), "utf8")).text).toBe("REMOTE");
    expect(fs.readFileSync(local.events, "utf8")).toContain('"type":"placement.remote"');
    expect(fs.existsSync(local.marker)).toBe(false);
    for (const file of [f.pickerCalls, f.sshCalls, f.launchCalls]) expect(fs.readFileSync(file, "utf8")).toBe("called\n");
  });

  it("starts the read-only SSH budget after a standalone slow picker finishes", async () => {
    const f = fixture({ pickerDelaySeconds: 5.5 });
    const config = normalizeFabricConfig({ agents: { placement: {
      ...f.config, commandTimeoutMs: 30_000, pickerTimeoutMs: 10_000,
    } } }).agents.placement!;
    const result = await preflightFleetPlacement(f.request(), config, config.command, "task", f.env);
    expect(result?.host).toBe("ryzen2");
    for (const file of [f.pickerCalls, f.sshCalls]) expect(fs.readFileSync(file, "utf8")).toBe("called\n");
    expect(fs.existsSync(f.launchCalls)).toBe(false);
  });

  it("falls back to a real local worker with the deadline reason when the picker exceeds its own budget", async () => {
    const f = fixture({ pickerDelaySeconds: 5.5 });
    const local = localWorkerRequest(f);
    const config = normalizeFabricConfig({ agents: { placement: {
      ...f.config, commandTimeoutMs: 30_000, pickerTimeoutMs: 100,
    } } }).agents.placement!;
    const started = Date.now();
    const handle = await new ProcessTransport(undefined, config).launch(local.request);
    await handle.waitForClose?.();
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    const events = fs.readFileSync(local.events, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events).toContainEqual(expect.objectContaining({ type: "placement.local", reason: "placement-picker-local: Preflight deadline expired" }));
    expect(handle.warnings).toEqual(["placement: using local worker: placement-picker-local: Preflight deadline expired"]);
    expect(fs.readFileSync(f.pickerCalls, "utf8")).toBe("called\n");
    for (const file of [f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  // Keep reason: failure path — unavailable work host; recorded requirement smarty-dev#4605.
  it.each(["ineligible", "missing", "nonzero", "empty", "timeout"] as const)("runs a real local worker with a warning when picker admission is %s", async kind => {
    const f = fixture({ candidates: kind === "empty" ? [] : ["intel1", "ryzen2"], pickerExit: kind === "nonzero" ? 3 : 0 });
    const command = [...f.config.command];
    if (kind === "ineligible") command[command.indexOf("--host") + 1] = "qs32729";
    const config = { ...f.config, command, ...(kind === "timeout" ? { pickerTimeoutMs: 100 } : {}),
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
    const local = events.trim().split("\n").map(line => JSON.parse(line)).filter(event => event.type === "placement.local");
    expect(local).toHaveLength(1);
    expect(handle.warnings).toEqual([`placement: using local worker: ${local[0].reason}`]);
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
    await expect(preflight).rejects.toThrow("needs:['corpus']");
    expect(Date.now() - started).toBeLessThan(10_000);
    expect(fs.readFileSync(f.sshCalls, "utf8")).toBe("called\n");
  });

  it("bounds a hung read-only picker and never starts SSH", async () => {
    const f = fixture();
    const picker = f.picker;
    fs.writeFileSync(picker, "#!/bin/sh\nsleep 5\n"); fs.chmodSync(picker, 0o700);
    const started = Date.now();
    await expect(preflightFleetPlacement(f.request(), { ...f.config, pickerTimeoutMs: 100 }, f.config.command, "task", f.env)).rejects.toThrow(/host=auto/);
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
    const home = path.join(f.base, "home"); fs.mkdirSync(home);
    vi.spyOn(os, "homedir").mockReturnValue(home);
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
