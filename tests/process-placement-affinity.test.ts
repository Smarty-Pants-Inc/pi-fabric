import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { preflightFleetPlacement, placementLocalPathReason } from "../src/agents/transports/placement-preflight.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { AgentInputError } from "../src/agents/input-validation.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(options: { candidates?: string[]; pickerExit?: number; sshExit?: number; executeRemote?: boolean } = {}) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-affinity-")); roots.push(base);
  // The test runner allocates outside /srv/scratch; caller TMPDIR is private, not the portable workspace.
  const callerTmp = path.join(base, "caller-tmp"); fs.mkdirSync(callerTmp);
  vi.stubEnv("TMPDIR", callerTmp);
  const root = path.join(base, "install");
  const mkdir = (file: string) => fs.mkdirSync(path.dirname(file), { recursive: true });
  const write = (file: string, text: string) => { mkdir(file); fs.writeFileSync(file, text); };
  write(path.join(root, "setup/factory/task-preflight.py"), [
    "import sys",
    "args = sys.argv",
    "if '--probe' in args: sys.exit(0)",
    "sys.stdin.read()",
    "count_at = args.index('--admit') + 3",
    "requires = int(args[count_at])",
    "paths = args[count_at + 1:count_at + 1 + requires]",
    "sys.stdout.write('\\0'.join(['', '', '', '0'] + [p for file in paths for p in [file, file]]) + '\\0')",
  ].join("\n"));
  const bin = path.join(root, "bin"); fs.mkdirSync(bin, { recursive: true });
  const fleet = path.join(bin, "smarty-task-ryzen2");
  write(fleet, "#!/bin/sh\nprintf 'called\\n' >> \"$LAUNCH_CALLS\"\nprintf '%s\\n' \"$@\" > \"$LAUNCH_CALLS.args\"\nprintf 'RYZEN2_TASK_ACCEPTED affinity on ryzen2\\n'\n"); fs.chmodSync(fleet, 0o700);
  const picker = path.join(base, "smarty-pick-host-fixture");
  write(picker, `#!/bin/sh\nprintf 'called\\n' >> "$PICKER_CALLS"\nprintf '%s\\n' "$CANDIDATES"\nprintf 'eligible0=1 eligible1=1\\n' >&2\nexit "${options.pickerExit ?? 0}"\n`); fs.chmodSync(picker, 0o700);
  const pathBin = path.join(base, "path"); fs.mkdirSync(pathBin);
  const python = path.join(pathBin, "python3");
  write(python, "#!/bin/sh\nexec /usr/bin/python3 \"$@\"\n"); fs.chmodSync(python, 0o700);
  const ssh = path.join(pathBin, "ssh");
  write(ssh, `#!/bin/sh\nprintf 'called\\n' >> "$SSH_CALLS"\nprintf '%s\\n' "$@" > "$SSH_CALLS.args"\n${options.executeRemote
    ? 'for remote in "$@"; do :; done\nexec sh -c "$remote"'
    : `${options.sshExit === 3 ? "printf 'PREFLIGHT_MISSING 1\\n'" : ":"}\nexit "${options.sshExit ?? 0}"`}\n`); fs.chmodSync(ssh, 0o700);
  const cwd = path.join(base, "workspace"); fs.mkdirSync(cwd);
  const config = normalizeFabricConfig({ agents: { placement: {
    default: "remote", command: [fleet, "--host", "auto", "--cwd", cwd, "--", "task"],
    resultDirectory: path.join(base, "results", "{id}"), cancelCommand: ["true"], commandTimeoutMs: 1_000,
    sshAliases: { intel1: "intel1-agent", ryzen2: "r2-agent", ryzen3: "r3-agent", qs32729: "qs32729-agent" }, sharedMounts: [cwd], pickerCommand: [picker],
  } } }).agents.placement!;
  const env = {
    ...process.env, PATH: `${pathBin}:${process.env.PATH}`,
    CANDIDATES: (options.candidates ?? ["ryzen2", "ryzen3"]).join("\n"),
    SSH_CALLS: path.join(base, "ssh-calls"), PICKER_CALLS: path.join(base, "picker-calls"), LAUNCH_CALLS: path.join(base, "launch-calls"),
  };
  const request = (cwdPath = cwd, needs: string[] = [], withRequires = true): AgentTransportLaunch => ({
    id: "affinity", name: "affinity", cwd: cwdPath, workerPath: "unused", workerArguments: [], needs,
    ...(withRequires ? { requires: [path.join(base, "target-only", "corpus.dat")] } : {}),
  });
  return { base, root, cwd, callerTmp, config, env, request, picker, sshCalls: env.SSH_CALLS, pickerCalls: env.PICKER_CALLS, launchCalls: env.LAUNCH_CALLS };
}

function localWorkerRequest(f: ReturnType<typeof fixture>, cwd = f.cwd) {
  for (const key of ["PATH", "CANDIDATES", "SSH_CALLS", "PICKER_CALLS", "LAUNCH_CALLS"] as const) vi.stubEnv(key, f.env[key]);
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

  it.each(["ineligible", "missing", "nonzero", "empty", "timeout", "malformed", "duplicate", "unmapped"] as const)("refuses picker admission %s as known-unlaunched without starting any worker", async kind => {
    const candidates: Record<string, string[]> = { empty: [], malformed: ["ryzen2", "bad host"], duplicate: ["ryzen2", "ryzen2"], unmapped: ["unmapped"] };
    const f = fixture({ candidates: candidates[kind] ?? ["intel1", "ryzen2"], pickerExit: kind === "nonzero" ? 3 : 0 });
    const command = [...f.config.command];
    if (kind === "ineligible") command[command.indexOf("--host") + 1] = "qs32729";
    const config = { ...f.config, command, ...(kind === "timeout" ? { commandTimeoutMs: 100 } : {}),
      ...(kind === "missing" ? { pickerCommand: [path.join(f.base, "absent-picker")] } : {}) };
    if (kind === "timeout") fs.writeFileSync(f.picker, "#!/bin/sh\nsleep 5\n");
    const local = localWorkerRequest(f);
    const started = Date.now();
    const pending = new ProcessTransport(undefined, config).launch(local.request);
    await expect(pending).rejects.toBeInstanceOf(AgentInputError);
    await expect(pending).rejects.toMatchObject({ code: "FABRIC_AGENT_INPUT_ERROR", field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow("placement unavailable; no task started");
    if (kind === "ineligible") await expect(pending).rejects.toThrow("host qs32729 is not picker-eligible");
    expect(Date.now() - started).toBeLessThan(2_000);
    for (const file of [local.marker, local.events, f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
    if (!["missing", "timeout"].includes(kind)) expect(fs.readFileSync(f.pickerCalls, "utf8")).toBe("called\ncalled\n");
  });

  it("does not retry selection or dispatch after the shared five-second preflight budget expires", async () => {
    const f = fixture({ pickerExit: 3 });
    const local = localWorkerRequest(f);
    const now = Date.now();
    // The native picker's first receipt exhausts the original budget, even when
    // the launcher's configured timeout is longer. No real five-second sleep.
    vi.spyOn(Date, "now").mockImplementation(() => fs.existsSync(f.pickerCalls) ? now + 5_000 : now);
    const pending = new ProcessTransport(undefined, { ...f.config, commandTimeoutMs: 30_000 }).launch(local.request);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    expect(fs.readFileSync(f.pickerCalls, "utf8")).toBe("called\n");
    for (const file of [local.marker, local.events, f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
  });

  it.each(["ineligible", "unmapped"] as const)("refuses an explicit %s host before SSH rather than switching to another eligible host", async kind => {
    const f = fixture({ candidates: kind === "unmapped" ? ["unmapped", "ryzen2"] : ["intel1", "ryzen2"] });
    const host = kind === "unmapped" ? "unmapped" : "qs32729";
    const command = [...f.config.command]; command[command.indexOf("--host") + 1] = host;
    const pending = preflightFleetPlacement(f.request(), f.config, command, "task", f.env);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(`host=${host}`);
    for (const file of [f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
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

  it.each(["eligible-first", "unmapped-first", "transient-picker-failure"] as const)("keeps a portable shared Git repo remote with %s", async kind => {
    const f = fixture({ executeRemote: true, ...(kind === "unmapped-first" ? { candidates: ["unmapped", "ryzen2"] } : {}) });
    if (kind === "transient-picker-failure") fs.writeFileSync(f.picker, [
      "#!/bin/sh",
      'if [ ! -e "$PICKER_CALLS" ]; then',
      '  printf "called\\n" >> "$PICKER_CALLS"',
      '  printf "temporarily unavailable\\n" >&2; exit 3',
      "fi",
      'printf "called\\n" >> "$PICKER_CALLS"',
      'printf "%s\\n" "$CANDIDATES"',
    ].join("\n"));
    execFileSync("git", ["init", "--quiet", f.cwd]);
    const local = localWorkerRequest(f);
    const result = path.join(f.base, "results", "affinity"); fs.mkdirSync(result, { recursive: true });
    fs.writeFileSync(path.join(result, "rc"), "0"); fs.writeFileSync(path.join(result, "result.md"), "REMOTE");
    const handle = await new ProcessTransport(undefined, { ...f.config, sharedMounts: [f.base] }).launch(local.request);
    expect(handle.sessionId).toBe("placement:affinity");
    expect(await handle.isAlive()).toBe(false);
    expect(fs.readFileSync(local.events, "utf8")).toContain('"type":"placement.remote"');
    expect(fs.readFileSync(`${f.launchCalls}.args`, "utf8")).toContain("--host\nryzen2\n");
    expect(fs.readFileSync(`${f.sshCalls}.args`, "utf8")).toContain("r2-agent\n");
    expect(fs.existsSync(local.marker)).toBe(false);
    expect(fs.readFileSync(f.pickerCalls, "utf8")).toBe(kind === "transient-picker-failure" ? "called\ncalled\n" : "called\n");
    for (const file of [f.sshCalls, f.launchCalls]) expect(fs.readFileSync(file, "utf8")).toBe("called\n");
  });

  it("launches an unshared Git --src remotely without requiring the source cwd on the target", async () => {
    const f = fixture({ executeRemote: true });
    execFileSync("git", ["init", "--quiet", f.cwd]);
    fs.writeFileSync(path.join(f.cwd, "source.txt"), "portable source");
    const local = localWorkerRequest(f);
    const config = { ...f.config,
      command: [f.config.command[0]!, "--host", "auto", "--src", "{cwd}", "--", "{task}"] };
    delete config.sharedMounts;
    const result = path.join(f.base, "results", "affinity"); fs.mkdirSync(result, { recursive: true });
    fs.writeFileSync(path.join(result, "rc"), "0"); fs.writeFileSync(path.join(result, "result.md"), "REMOTE SOURCE");
    const handle = await new ProcessTransport(undefined, config).launch(local.request);
    expect(handle.sessionId).toBe("placement:affinity");
    expect(await handle.isAlive()).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(f.base, "run/status.json"), "utf8"))).toMatchObject({ status: "completed", text: "REMOTE SOURCE" });
    expect(fs.readFileSync(`${f.launchCalls}.args`, "utf8").trim().split("\n")).toEqual(["--host", "ryzen2", "--src", f.cwd, "--", "portable task"]);
    expect(fs.readFileSync(`${f.sshCalls}.args`, "utf8")).not.toContain("test -e");
    expect(fs.readFileSync(local.events, "utf8")).toContain('"type":"placement.remote"');
    expect(fs.existsSync(local.marker)).toBe(false);
    for (const file of [f.pickerCalls, f.sshCalls, f.launchCalls]) expect(fs.readFileSync(file, "utf8")).toBe("called\n");
  });

  it("still refuses missing target-only inputs before launching an unshared Git --src", async () => {
    const f = fixture({ executeRemote: true });
    execFileSync("git", ["init", "--quiet", f.cwd]);
    const local = localWorkerRequest(f);
    const required = f.request().requires![0]!;
    local.request.requires = [required];
    expect(fs.existsSync(required)).toBe(false);
    const config = { ...f.config,
      command: [f.config.command[0]!, "--host", "auto", "--src", "{cwd}", "--", "{task}"] };
    delete config.sharedMounts;
    const pending = new ProcessTransport(undefined, config).launch(local.request);
    await expect(pending).rejects.toMatchObject({ name: "RequiredInputMissingError", field: "requires", launchOutcome: "unlaunched", path: required, index: 0 });
    await expect(pending).rejects.toThrow(required);
    expect(fs.readFileSync(f.sshCalls, "utf8")).toBe("called\n");
    expect(fs.existsSync(f.launchCalls)).toBe(false);
    expect(fs.existsSync(local.marker)).toBe(false);
  });

  it.each(["tmp-lexical", "tmp-canonical", "state-lexical", "state-canonical"])("keeps an unshared %s Git --src local before remote subprocesses", async kind => {
    const f = fixture();
    execFileSync("git", ["init", "--quiet", f.cwd]);
    const home = path.join(f.base, "home"); fs.mkdirSync(home);
    vi.spyOn(os, "homedir").mockReturnValue(home);
    const root = kind.startsWith("tmp") ? f.callerTmp : path.join(home, ".local/state");
    fs.mkdirSync(root, { recursive: true });
    let cwd: string;
    if (kind.endsWith("lexical")) {
      // A lexical scratch/state path cannot escape the fence by pointing at a portable Git repo.
      cwd = path.join(root, "escape"); fs.symlinkSync(f.cwd, cwd, "dir");
    } else {
      const repo = path.join(root, "repo"); fs.mkdirSync(repo);
      execFileSync("git", ["init", "--quiet", repo]);
      cwd = path.join(f.base, "source-alias"); fs.symlinkSync(repo, cwd, "dir");
    }
    const local = localWorkerRequest(f, cwd);
    const config = { ...f.config,
      command: [f.config.command[0]!, "--host", "auto", "--src", "{cwd}", "--", "{task}"] };
    delete config.sharedMounts;
    const handle = await new ProcessTransport(undefined, config).launch(local.request);
    await handle.waitForClose?.();
    expect(fs.readFileSync(local.marker, "utf8")).toBe("LOCAL");
    expect(fs.readFileSync(local.events, "utf8")).toContain("caller-local cwd");
    for (const file of [f.pickerCalls, f.sshCalls, f.launchCalls]) expect(fs.existsSync(file)).toBe(false);
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
