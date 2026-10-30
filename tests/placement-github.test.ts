import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeAgentPlacement } from "../src/agents/placement-config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { preflightFleetPlacement } from "../src/agents/transports/placement-preflight.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// A native protocol fixture: NUL admission, the compatibility handshake, then
// target-side guards and a real subprocess read through a fake installed relay.
// No SSH/GitHub transport or credential store outside this fixture is accessed.
const helper = String.raw`
import json, os, re, subprocess, sys
from pathlib import Path

def log(name, value):
    with open(os.environ[name], 'a') as out:
        out.write(json.dumps(value) + '\n')

def take(args, flag):
    assert args[0] == flag, (args, flag)
    return args[1], args[2:]

def resolved(value):
    return os.path.realpath(os.path.expanduser(value))

def inferred(prompt, paths):
    needs = set()
    if re.search(r'(?<![\w-])(?:gh|smarty-gh-remote)(?![\w-])', prompt): needs.add('github')
    if re.search(r'(?<![\w-])op(?![\w-])', prompt): needs.add('credentials')
    corpus = [str(Path.home() / '.pi/agent/sessions'), str(Path.home() / '.local/share/smarty-dev/org-context/tree')]
    if any(resolved(p) == c or resolved(p).startswith(c + '/') for p in paths for c in corpus): needs.add('corpus')
    return needs

def credential(paths):
    for p in paths:
        for suffix in ['.ssh', '.config/gh', '.config/op']:
            root = str(Path.home() / suffix)
            if resolved(p) == root or resolved(p).startswith(root + '/'):
                return 'credential path refused: ' + p
    return ''

args = sys.argv[1:]
mode, args = args[0], args[1:]
if mode == '--capabilities':
    log('HANDSHAKE_CALLS', mode)
    capability = os.environ.get('HELPER_CAPABILITY', 'github-relay-v1')
    if capability == 'error': sys.exit(3)
    if capability != 'legacy': print(capability)
    sys.exit(0)
if mode == '--admit':
    root, args = take(args, '--root')
    count = int(args[0]); paths = args[1:1 + count]; args = args[1 + count:]
    assert args[0] == '0'
    explicit = args[1:]
    prompt = sys.stdin.read()
    paths += re.findall(r'(?:~/|/)[^\s]+', prompt)
    needs = inferred(prompt, paths) | set(explicit)
    fields = [credential(paths), ' '.join(sorted(needs)), 'fixture inferred/explicit union', str(len(paths)), *paths]
    for p in paths:
        if p.startswith('/'): fields += [p, p]
    sys.stdout.write('\0'.join(fields) + '\0')
    sys.exit(0)
if mode == '--probe':
    # The pre-split helper accepted unknown flags as relative paths, returning
    # zero without checking a relay. Its handshake must prevent reaching here.
    if os.environ.get('HELPER_CAPABILITY') == 'legacy': sys.exit(0)
    root, args = take(args, '--root')
    workspace, args = take(args, '--workspace')
    host, needs = '', []
    if args[:1] == ['--host']:
        host, args = take(args, '--host')
        need_string, args = take(args, '--needs'); needs = need_string.split()
    log('PROBE_CALLS', {'host': host, 'needs': needs, 'guards': args})
    denied = credential([workspace, *args])
    union = set(needs) | inferred('', args)
    if not denied and union - {'github'}: denied = 'needs ' + ' '.join(sorted(union))
    if denied:
        print(denied); sys.exit(3)
    if 'github' in union:
        relay = Path.home() / '.local/bin/smarty-gh-remote'
        try:
            code = subprocess.run([str(relay), 'run', 'Smarty-Pants-Inc', '--', 'gh', 'api', 'rate_limit'],
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
        except OSError:
            code = 127
        if code:
            print('GitHub relay unavailable on ' + host); sys.exit(3)
    sys.exit(0)
raise AssertionError(mode)
`;

function fixture(options: { generic?: boolean; capability?: string; relay?: "missing" | "nonzero" } = {}) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-github-")); roots.push(base);
  const write = (file: string, text: string, executable = false) => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, text, { mode: executable ? 0o700 : 0o600 });
  };
  const callerHome = path.join(base, "caller-home"); fs.mkdirSync(callerHome);
  const targetHome = path.join(base, "target-home"); fs.mkdirSync(targetHome);
  const callerTmp = path.join(base, "caller-tmp"); fs.mkdirSync(callerTmp);
  const cwd = path.join(base, "workspace"); fs.mkdirSync(cwd);
  const install = path.join(base, "install");
  write(path.join(install, "setup/factory/task-preflight.py"), helper);
  const launcher = path.join(install, "bin", options.generic ? "custom-launcher" : "smarty-task-ryzen2");
  write(launcher, '#!/bin/sh\nprintf "called\\n" >> "$LAUNCH_CALLS"\nprintf "%s\\n" "$@" > "$LAUNCH_CALLS.args"\nprintf "RYZEN2_TASK_ACCEPTED github-task on intel1\\n"\n', true);
  const picker = path.join(install, "bin/smarty-pick-host");
  write(picker, '#!/bin/sh\nprintf "called\\n" >> "$PICKER_CALLS"\nprintf "intel1\\n"\n', true);
  const bin = path.join(base, "path");
  write(path.join(bin, "python3"), '#!/bin/sh\nexec /usr/bin/python3 "$@"\n', true);
  write(path.join(bin, "ssh"), [
    "#!/bin/sh", 'printf "called\\n" >> "$SSH_CALLS"',
    'for remote in "$@"; do :; done',
    'if [ -n "${TARGET_LINK-}" ]; then ln -s "$TARGET_LINK_TO" "$TARGET_LINK"; fi',
    'HOME="$TARGET_HOME" exec sh -c "$remote"',
  ].join("\n"), true);
  if (options.relay !== "missing") write(path.join(targetHome, ".local/bin/smarty-gh-remote"), [
    "#!/bin/sh", 'printf "%s\\n" "$@" >> "$RELAY_CALLS"', `exit ${options.relay === "nonzero" ? 1 : 0}`,
  ].join("\n"), true);
  const env = {
    ...process.env, HOME: callerHome, TMPDIR: callerTmp, PATH: `${bin}:${process.env.PATH}`,
    TARGET_HOME: targetHome, TARGET_LINK: "", TARGET_LINK_TO: "", HELPER_CAPABILITY: options.capability ?? "github-relay-v1",
    HANDSHAKE_CALLS: path.join(base, "handshakes"), PROBE_CALLS: path.join(base, "probes"), RELAY_CALLS: path.join(base, "relays"),
    PICKER_CALLS: path.join(base, "pickers"), SSH_CALLS: path.join(base, "ssh"), LAUNCH_CALLS: path.join(base, "launches"),
  };
  for (const [key, value] of Object.entries(env)) if (value !== undefined) vi.stubEnv(key, value);
  const config = normalizeAgentPlacement({
    default: "remote", command: [launcher, "--host", "auto", "--cwd", "{cwd}", "--", "{task}"], capabilities: [],
    sharedMounts: [cwd], sshAliases: { intel1: "intel1-agent" }, pickerCommand: [picker],
    resultDirectory: path.join(base, "results", "{id}"), cancelCommand: ["true"], commandTimeoutMs: 2_000,
  })!;
  const taskFile = path.join(base, "run/task.txt"); write(taskFile, "Inspect the issue described in the request.");
  const localMarker = path.join(base, "local-worker");
  const workerPath = path.join(base, "worker.mjs");
  write(workerPath, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(localMarker)}, 'LOCAL');`);
  const statusFile = path.join(base, "run/status.json");
  const request: AgentTransportLaunch = {
    id: "github-task", name: "github-task", cwd, workerPath, needs: ["github"],
    workerArguments: ["--task-file", taskFile, "--status-file", statusFile, "--log-file", path.join(base, "run/events.jsonl"), "--timeout-ms", "10000"],
  };
  write(path.join(base, "results/github-task/rc"), "0");
  write(path.join(base, "results/github-task/result.md"), "REMOTE relay-backed result");
  const task = (value: string) => fs.writeFileSync(taskFile, value);
  const probes = () => fs.readFileSync(env.PROBE_CALLS, "utf8").trim().split("\n").map(line => JSON.parse(line));
  const noWorker = () => {
    expect(fs.existsSync(env.LAUNCH_CALLS)).toBe(false);
    expect(fs.existsSync(localMarker)).toBe(false);
  };
  const preflight = (launch = request, text = fs.readFileSync(taskFile, "utf8")) => preflightFleetPlacement(launch, config,
    config.command.map(arg => arg === "{cwd}" ? cwd : arg === "{task}" ? text : arg), text, env);
  return { base, env, config, request, task, statusFile, probes, noWorker, preflight, localMarker, cwd, targetHome };
}

// Keep reason: the helper/SSH admission trust boundary, including compatibility
// with a real pre-split helper behavior that otherwise falsely reports success.
// This protocol is POSIX-only, like the installed factory launcher.
describe.skipIf(process.platform === "win32")("fleet GitHub needs independent of corpus", () => {
  it.each(["explicit", "inferred", "both"] as const)("completes relay-backed %s GitHub work with no flat github/corpus capability", async kind => {
    const f = fixture();
    if (kind !== "explicit") f.task("gh api rate_limit");
    if (kind === "inferred") f.request.needs = [];
    const handle = await new ProcessTransport(undefined, f.config).launch(f.request);
    expect(handle.sessionId).toBe("placement:github-task");
    expect(await handle.isAlive()).toBe(false);
    expect(JSON.parse(fs.readFileSync(f.statusFile, "utf8"))).toMatchObject({ status: "completed", text: "REMOTE relay-backed result" });
    expect(f.probes()).toEqual([{ host: "intel1", needs: ["github"], guards: [] }]);
    expect(fs.readFileSync(f.env.RELAY_CALLS, "utf8")).toBe("run\nSmarty-Pants-Inc\n--\ngh\napi\nrate_limit\n");
    expect(fs.readFileSync(`${f.env.LAUNCH_CALLS}.args`, "utf8")).toContain("--needs\ngithub\n--\n");
    expect(fs.existsSync(f.localMarker)).toBe(false);
  });

  it.each(["missing", "nonzero"] as const)("leaves GitHub work known-unlaunched when the selected host relay is %s", async relay => {
    const f = fixture({ relay });
    const pending = new ProcessTransport(undefined, f.config).launch(f.request);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(/host=intel1:.*GitHub relay unavailable/u);
    f.noWorker();
    expect(fs.readFileSync(f.env.SSH_CALLS, "utf8")).toBe("called\n");
    expect(fs.readFileSync(f.env.PICKER_CALLS, "utf8")).toBe("called\n");
  });

  it.each([
    { needs: ["github", "corpus"], text: "portable work", refused: "corpus" },
    { needs: ["github"], text: "Read ~/.pi/agent/sessions with gh api rate_limit", refused: "corpus" },
    { needs: ["github"], text: "op item get something", refused: "credentials" },
    { needs: ["github", "credentials"], text: "portable work", refused: "credentials" },
    { needs: ["github", "unknown-need"], text: "portable work", refused: "unknown-need" },
  ])("refuses the full unserviceable union before SSH: $refused / $text", async ({ needs, text, refused }) => {
    const f = fixture();
    f.task(text);
    // Even stale/overbroad flat capabilities cannot make corpus, credentials
    // or unknown needs serviceable by the installed fleet relay protocol.
    const pending = new ProcessTransport(undefined, { ...f.config, capabilities: needs }).launch({ ...f.request, needs });
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(refused);
    for (const file of [f.env.SSH_CALLS, f.env.RELAY_CALLS, f.env.HANDSHAKE_CALLS]) expect(fs.existsSync(file)).toBe(false);
    f.noWorker();
  });

  it("refuses a literal credential input before even negotiating the relay", async () => {
    const f = fixture();
    f.request.requires = [path.join(f.env.HOME, ".ssh/id_ed25519")];
    const pending = new ProcessTransport(undefined, f.config).launch(f.request);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow("credential path");
    for (const file of [f.env.SSH_CALLS, f.env.RELAY_CALLS, f.env.HANDSHAKE_CALLS]) expect(fs.existsSync(file)).toBe(false);
    f.noWorker();
  });

  it.each([".ssh", ".pi/agent/sessions"])("retains target-only symlink refusal into %s before a relay read", async suffix => {
    const f = fixture();
    const alias = path.join(f.cwd, "target-alias");
    const target = path.join(f.targetHome, suffix); fs.mkdirSync(target, { recursive: true });
    vi.stubEnv("TARGET_LINK", alias); vi.stubEnv("TARGET_LINK_TO", target);
    f.request.requires = [alias];
    expect(fs.existsSync(alias)).toBe(false);
    const pending = new ProcessTransport(undefined, f.config).launch(f.request);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(suffix === ".ssh" ? "credential path" : "corpus");
    expect(fs.existsSync(f.env.RELAY_CALLS)).toBe(false);
    f.noWorker();
  });

  it("does not spill to Main when a target-only literal input is missing, even with a working relay", async () => {
    const f = fixture();
    const required = path.join(f.cwd, "missing 'input $literal");
    f.request.requires = [required];
    const pending = new ProcessTransport(undefined, f.config).launch(f.request);
    await expect(pending).rejects.toMatchObject({ name: "RequiredInputMissingError", field: "requires", path: required, index: 0, launchOutcome: "unlaunched" });
    expect(f.probes()).toEqual([{ host: "intel1", needs: ["github"], guards: [required] }]);
    f.noWorker();
  });

  it.each([
    { capability: "legacy", capabilities: [] },
    { capability: "legacy", capabilities: ["github"] },
    { capability: "error", capabilities: [] },
    { capability: "github-relay-v1 extra", capabilities: [] },
    { capability: "prefix-github-relay-v1", capabilities: [] },
  ])("never trusts an old/error/nonexact helper handshake: $capability / $capabilities", async ({ capability, capabilities }) => {
    const f = fixture({ capability });
    const pending = new ProcessTransport(undefined, { ...f.config, capabilities }).launch(f.request);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow("lacks github-relay-v1");
    for (const file of [f.env.SSH_CALLS, f.env.RELAY_CALLS]) expect(fs.existsSync(file)).toBe(false);
    f.noWorker();
  });

  it("keeps legacy helpers usable for portable work with no GitHub requirement", async () => {
    const f = fixture({ capability: "legacy", relay: "missing" });
    const handle = await new ProcessTransport(undefined, f.config).launch({ ...f.request, needs: [] });
    expect(handle.sessionId).toBe("placement:github-task");
    expect(await handle.isAlive()).toBe(false);
    expect(JSON.parse(fs.readFileSync(f.statusFile, "utf8"))).toMatchObject({ status: "completed" });
    for (const file of [f.env.HANDSHAKE_CALLS, f.env.RELAY_CALLS]) expect(fs.existsSync(file)).toBe(false);
    expect(fs.existsSync(f.localMarker)).toBe(false);
  });

  it("accepts the exact capability among additional capability lines", async () => {
    const f = fixture({ capability: "other-v1\ngithub-relay-v1\nfuture-v2" });
    const result = await f.preflight();
    expect(result?.host).toBe("intel1");
    expect(f.probes()[0]).toMatchObject({ needs: ["github"] });
    f.noWorker();
  });

  it("charges the handshake to the same five-second preflight deadline", async () => {
    const f = fixture();
    const now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => fs.existsSync(f.env.HANDSHAKE_CALLS) ? now + 5_000 : now);
    const pending = new ProcessTransport(undefined, { ...f.config, commandTimeoutMs: 30_000 }).launch(f.request);
    await expect(pending).rejects.toMatchObject({ field: "placement", launchOutcome: "unlaunched" });
    await expect(pending).rejects.toThrow(/deadline expired/u);
    expect(fs.existsSync(f.env.HANDSHAKE_CALLS)).toBe(true);
    expect(fs.existsSync(f.env.SSH_CALLS)).toBe(false);
    f.noWorker();
  });

  it.each(["local", "corpus", "credentials"])("does not use dynamic GitHub suitability to bypass the %s local need", async need => {
    const f = fixture();
    const handle = await new ProcessTransport(undefined, f.config).launch({ ...f.request, needs: ["github", need] });
    await handle.waitForClose?.();
    expect(fs.readFileSync(f.localMarker, "utf8")).toBe("LOCAL");
    for (const file of [f.env.PICKER_CALLS, f.env.HANDSHAKE_CALLS, f.env.SSH_CALLS, f.env.LAUNCH_CALLS]) expect(fs.existsSync(file)).toBe(false);
  });

  it.each([false, true])("keeps a custom launcher/picker's flat host-owned GitHub semantics (declared=%s)", async declared => {
    const f = fixture({ generic: true, capability: "legacy", relay: "missing" });
    const handle = await new ProcessTransport(undefined, { ...f.config, capabilities: declared ? ["github"] : [] }).launch(f.request);
    if (declared) {
      expect(handle.sessionId).toBe("placement:github-task");
      expect(await handle.isAlive()).toBe(false);
      expect(JSON.parse(fs.readFileSync(f.statusFile, "utf8"))).toMatchObject({ status: "completed" });
      expect(fs.existsSync(f.localMarker)).toBe(false);
      expect(fs.readFileSync(`${f.env.LAUNCH_CALLS}.args`, "utf8")).not.toContain("--needs\n");
    } else {
      await handle.waitForClose?.();
      expect(fs.readFileSync(f.localMarker, "utf8")).toBe("LOCAL");
      for (const file of [f.env.PICKER_CALLS, f.env.SSH_CALLS, f.env.LAUNCH_CALLS]) expect(fs.existsSync(file)).toBe(false);
    }
    for (const file of [f.env.HANDSHAKE_CALLS, f.env.PROBE_CALLS, f.env.RELAY_CALLS]) expect(fs.existsSync(file)).toBe(false);
  });
});
