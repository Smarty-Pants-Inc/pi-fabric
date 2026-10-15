import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { preflightFleetPlacement, placementLocalPathReason } from "../src/agents/transports/placement-preflight.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(options: { candidates?: string[]; pickerExit?: number; sshExit?: number } = {}) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-affinity-")); roots.push(base);
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
  const fleet = path.join(bin, "smarty-task-ryzen2"); write(fleet, "#!/bin/sh\nexit 99\n"); fs.chmodSync(fleet, 0o700);
  const picker = path.join(base, "smarty-pick-host-fixture");
  write(picker, `#!/bin/sh\nprintf '%s\\n' "$CANDIDATES"\nprintf 'eligible0=1 eligible1=1\\n' >&2\nexit "${options.pickerExit ?? 0}"\n`); fs.chmodSync(picker, 0o700);
  const pathBin = path.join(base, "path"); fs.mkdirSync(pathBin);
  const python = path.join(pathBin, "python3");
  write(python, "#!/bin/sh\nexec /usr/bin/python3 \"$@\"\n"); fs.chmodSync(python, 0o700);
  const ssh = path.join(pathBin, "ssh");
  write(ssh, `#!/bin/sh\nprintf 'called\\n' >> "$SSH_CALLS"\n${options.sshExit === 3 ? "printf 'PREFLIGHT_MISSING 1\\n'" : ":"}\nexit "${options.sshExit ?? 0}"\n`); fs.chmodSync(ssh, 0o700);
  const cwd = path.join(base, "workspace"); fs.mkdirSync(cwd);
  const config = normalizeFabricConfig({ agents: { placement: {
    default: "remote", command: [fleet, "--host", "auto", "--cwd", cwd, "--", "task"],
    resultDirectory: path.join(base, "results", "{id}"), cancelCommand: ["true"], commandTimeoutMs: 1_000,
    sshAliases: { intel1: "intel1-agent", ryzen2: "r2-agent", ryzen3: "r3-agent", qs32729: "qs32729-agent" }, sharedMounts: [cwd], pickerCommand: [picker],
  } } }).agents.placement!;
  const env = {
    ...process.env, PATH: `${pathBin}:${process.env.PATH}`,
    CANDIDATES: (options.candidates ?? ["ryzen2", "ryzen3"]).join("\n"),
    SSH_CALLS: path.join(base, "ssh-calls"),
  };
  const request = (cwdPath = cwd, needs: string[] = [], withRequires = true): AgentTransportLaunch => ({
    id: "affinity", name: "affinity", cwd: cwdPath, workerPath: "unused", workerArguments: [], needs,
    ...(withRequires ? { requires: [path.join(base, "target-only", "corpus.dat")] } : {}),
  });
  return { base, root, cwd, config, env, request, picker, sshCalls: path.join(base, "ssh-calls") };
}

// The installed fleet protocol is POSIX-only; Windows has no /usr/bin/python3 or shell launcher.
describe.skipIf(process.platform === "win32")("installed fleet placement affinity preflight", () => {
  it("pins an automatic launch to the first actually eligible picker result", async () => {
    const f = fixture({ candidates: ["intel1", "ryzen2"] });
    const result = await preflightFleetPlacement(f.request(), f.config, f.config.command, "task", f.env);
    expect(result?.host).toBe("intel1");
    expect(result?.command.slice(result.command.indexOf("--host"), result.command.indexOf("--host") + 2)).toEqual(["--host", "intel1"]);
  });

  it.each(["ineligible", "missing", "nonzero", "empty", "timeout"] as const)("runs a real local worker when picker admission is %s", async kind => {
    const f = fixture({ candidates: kind === "empty" ? [] : ["intel1", "ryzen2"], pickerExit: kind === "nonzero" ? 3 : 0 });
    const command = [...f.config.command];
    if (kind === "ineligible") command[command.indexOf("--host") + 1] = "qs32729";
    const config = { ...f.config, command, ...(kind === "timeout" ? { commandTimeoutMs: 100 } : {}),
      ...(kind === "missing" ? { pickerCommand: [path.join(f.base, "absent-picker")] } : {}) };
    if (kind === "timeout") fs.writeFileSync(f.picker, "#!/bin/sh\nsleep 5\n");
    vi.stubEnv("CANDIDATES", f.env.CANDIDATES); vi.stubEnv("PATH", f.env.PATH); vi.stubEnv("SSH_CALLS", f.sshCalls);
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
