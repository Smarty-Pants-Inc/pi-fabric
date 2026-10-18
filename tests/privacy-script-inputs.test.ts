import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

// Keep reason: script-input trust boundaries and data safety. Execute the actual
// CLI entrypoints, not extracted/source predicates. The child preload delegates
// real reads/stat/realpath, but records and refuses every launch/destructive call.
// All real fixture IO stays under the harness's owned temporary root for teardown.
// The bridge positive case uses one unique synthetic non-/tmp evidence prefix:
// only mkdir/writeFile there may be dry-recorded; that root is never created.
const repo = path.resolve(import.meta.dirname, "..");
const tempProbe = "scripts/probe-test-temp-cleanup.mjs";
const wakeProbe = "scripts/probe-wake-causes.mjs";
const terminalProof = "scripts/prove-terminal-task.mjs";
const bridgeProof = "proof/forge-ssh-bridge.mjs";
const preloadSource = `
import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
const receipt = fs.openSync(process.env.PRIVACY_EVENTS, 'a');
const writeReceipt = fs.writeSync.bind(fs);
const record = value => writeReceipt(receipt, JSON.stringify(value) + '\\n');
const dryRoot = process.env.PRIVACY_DRY_ROOT;
for (const method of ['readFileSync', 'statSync', 'realpathSync', 'existsSync', 'readdirSync']) {
  const original = fs[method].bind(fs);
  fs[method] = (...args) => {
    const file = String(args[0]);
    record({ kind: 'read', method, path: file });
    const value = original(...args);
    if (method === 'statSync' && file === process.env.PRIVACY_FOREIGN_STAT) value.uid = process.getuid() + 1;
    return value;
  };
}
for (const method of ['mkdirSync', 'mkdtempSync', 'writeFileSync', 'appendFileSync', 'copyFileSync', 'renameSync', 'rmSync', 'unlinkSync', 'rmdirSync', 'createWriteStream', 'watch']) {
  fs[method] = (...args) => {
    const file = String(args[0]);
    const dry = dryRoot && (file === dryRoot || file.startsWith(dryRoot + path.sep)) && ['mkdirSync', 'writeFileSync'].includes(method);
    let bridgeHost;
    if (method === 'writeFileSync') { try { bridgeHost = JSON.parse(String(args[1])).configuration?.bridgeHost; } catch {} }
    record({ kind: 'effect', method, path: file, dry: !!dry, bridgeHost });
    if (dry) return;
    throw new Error('OFFLINE_BOUNDARY: ' + method + ' ' + file);
  };
}
for (const method of ['spawn', 'spawnSync', 'exec', 'execSync', 'execFile', 'execFileSync', 'fork']) {
  cp[method] = (...args) => {
    record({ kind: 'launch', method, executable: String(args[0]) });
    throw new Error('OFFLINE_LAUNCH_FORBIDDEN: ' + method);
  };
}
syncBuiltinESMExports();
`;

type Observation = { kind: "read" | "effect" | "launch"; method: string; path?: string; dry?: boolean; bridgeHost?: string };
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "privacy-script-inputs-"));
  fs.chmodSync(root, 0o700);
  fs.writeFileSync(path.join(root, "preload.mjs"), preloadSource);
  return root;
};
const run = (script: string, args: string[], root = fixture(), env: Record<string, string> = {}) => {
  const events = path.join(root, "events.jsonl");
  fs.writeFileSync(events, "");
  const result = spawnSync(process.execPath, ["--import", pathToFileURL(path.join(root, "preload.mjs")).href, path.join(repo, script), ...args], {
    cwd: root,
    // Do not inherit credentials, NODE_OPTIONS, a real profile, or a CLI default.
    env: { PATH: process.env.PATH, HOME: root, TMPDIR: root, TMP: root, TEMP: root, PRIVACY_EVENTS: events, ...env },
    encoding: "utf8", timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 1024 * 1024,
  });
  // Node itself loads these exact static modules after --import. Exclude only
  // that loader IO, never CLI artifacts, manifests, profiles or proof config.
  const loaderFiles = [path.join(repo, script), ...(script === wakeProbe ? [path.join(repo, "scripts/wake-probe-observer.mjs")] : [])];
  const loaderPaths = new Set(loaderFiles.flatMap(file => [file, pathToFileURL(file).href]));
  const observations = fs.readFileSync(events, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Observation)
    .filter(event => event.kind !== "read" || !loaderPaths.has(event.path ?? ""));
  expect(result.error).toBeUndefined();
  expect(result.signal).toBeNull();
  expect(result.status).not.toBe(0);
  expect(observations.filter(event => event.kind === "launch")).toEqual([]);
  return { output: result.stdout + result.stderr, observations };
};
const expectNoEffects = (result: ReturnType<typeof run>, message: RegExp) => {
  expect(result.output).toMatch(message);
  expect(result.output).toContain("Usage:");
  expect(result.observations.filter(event => event.kind !== "read")).toEqual([]);
};
const stubCli = (root: string, version = "not-the-pinned-host") => {
  const dist = path.join(root, "pi", "dist");
  fs.mkdirSync(dist, { recursive: true });
  const cli = path.join(dist, "cli.js");
  fs.writeFileSync(cli, "throw new Error('STUB_CLI_MUST_NEVER_RUN');\n");
  fs.writeFileSync(path.join(root, "pi", "package.json"), JSON.stringify({ version }));
  return cli;
};
const builtFixture = (root: string) => {
  const fabric = path.join(root, "fabric");
  fs.mkdirSync(path.join(fabric, "dist"), { recursive: true });
  for (const file of ["index.js", "worker.js"]) fs.writeFileSync(path.join(fabric, "dist", file), "throw new Error('ARTIFACT_MUST_NEVER_RUN');\n");
  return fabric;
};
const terminalOptions = (root: string, fabric = path.join(root, "fabric")) => [
  "--fabric-root", fabric, "--evidence-dir", path.join(root, "evidence"), "--expect-terminal", "true",
];
const bridgeConfig = (root: string): Record<string, unknown> => Object.fromEntries([
  "localMesh", "remoteMesh", "localFabric", "remoteFabric", "localAgentDir", "remoteAgentDir", "localCwd", "remoteCwd",
  "evidenceRoot", "remoteEvidenceRoot", "bridgeKey", "bridgeKnownHosts",
].map(key => [key, path.join(root, ".local", key)]));
const writeConfig = (root: string, config: Record<string, unknown>) => {
  const file = path.join(root, "config.json");
  fs.writeFileSync(file, JSON.stringify(config));
  return file;
};

// These native proofs and their ownership/mode guards are POSIX-specific.
describe.skipIf(process.platform === "win32")("offline proof CLI input boundaries", () => {
  describe("temporary cleanup probe", () => {
    it.each([[], [""], ["   "], ["relative-root"], ["/unused", "extra"]].map(args => ({ args })))("rejects invalid positional roots $args before effects", ({ args }) => {
      const result = run(tempProbe, args);
      expectNoEffects(result, /PRIVATE_TEMP_ROOT/);
      expect(result.observations).toEqual([]); // Inherited private TMPDIR is not consent.
    });

    it.each([0o755, 0o600, 0o1700])("rejects a real directory with unsafe mode %i", mode => {
      const root = fixture(), caller = path.join(root, "caller");
      fs.mkdirSync(caller);
      fs.chmodSync(caller, mode);
      expectNoEffects(run(tempProbe, [caller], root), /mode 0700/);
    });

    it("requires a directory, not a private file", () => {
      const root = fixture(), file = path.join(root, "not-a-directory");
      fs.writeFileSync(file, "private", { mode: 0o700 });
      expectNoEffects(run(tempProbe, [file], root), /caller-owned directory/);
    });

    it("retains caller ownership validation", () => {
      const root = fixture();
      // No privileged chown: only this fixture's native stat UID is changed.
      expectNoEffects(run(tempProbe, [root], root, { PRIVACY_FOREIGN_STAT: root }), /caller-owned directory/);
    });

    it("uses the explicit physical private caller, not inherited TMPDIR", () => {
      const root = fixture(), caller = path.join(root, "caller"), alias = path.join(root, "alias");
      fs.mkdirSync(caller, { mode: 0o700 });
      fs.symlinkSync(caller, alias, "dir");
      const result = run(tempProbe, [alias], root);
      expect(result.output).toContain("OFFLINE_BOUNDARY: mkdtempSync");
      expect(result.observations.filter(event => event.kind === "effect")).toEqual([
        { kind: "effect", method: "mkdtempSync", path: path.join(fs.realpathSync(caller), "temp-proof-"), dry: false },
      ]);
      expect(fs.readdirSync(caller)).toEqual([]);
    });
  });

  describe("wake probe", () => {
    it.each([[], ["--pi-cli"], ["--pi-cli", ""], ["--pi-cli", "  "], ["--pi-cli="], ["--pi-cli", "--duration-ms", "1"], ["--pi-cli", "relative-cli.js"]].map(args => ({ args })))("requires an explicit absolute CLI $args before artifact reads", ({ args }) => {
      const result = run(wakeProbe, args);
      expectNoEffects(result, /--pi-cli/);
      expect(result.observations).toEqual([]);
    });

    it("does not accept an empty explicit environment CLI", () => {
      const result = run(wakeProbe, [], fixture(), { PI_FABRIC_PROBE_PI: "" });
      expectNoEffects(result, /--pi-cli/);
      expect(result.observations).toEqual([]);
    });

    it.each(["candidate", "output"])('rejects an empty %s before artifact reads', input => {
      const root = fixture(), cli = stubCli(root), candidate = path.join(root, "candidate.js");
      const result = run(wakeProbe, [input === "candidate" ? "" : candidate, input === "output" ? "" : path.join(root, "evidence"), "--pi-cli", cli], root);
      expectNoEffects(result, /nonempty path/);
      expect(result.observations).toEqual([]);
    });

    it("rejects a directory as the CLI without reading a manifest", () => {
      const root = fixture();
      const result = run(wakeProbe, ["--pi-cli", root], root);
      expectNoEffects(result, /Missing file/);
      expect(result.observations.filter(event => event.method === "readFileSync")).toEqual([]);
    });

    it.each(["option", "environment"])('advances an explicit %s CLI to the real pinned-host validation', route => {
      const root = fixture(), cli = stubCli(root), candidate = path.join(root, "candidate.js");
      fs.writeFileSync(candidate, "throw new Error('CANDIDATE_MUST_NEVER_RUN');\n");
      const result = run(wakeProbe, [candidate, path.join(root, "evidence"), ...(route === "option" ? ["--pi-cli", cli] : [])], root,
        route === "environment" ? { PI_FABRIC_PROBE_PI: cli } : {});
      expect(result.output).toContain("Replay is pinned to the real Pi 0.87.1 host");
      expect(result.observations.filter(event => event.kind !== "read")).toEqual([]);
      expect(result.observations.filter(event => event.method === "readFileSync").map(event => event.path)).toEqual([path.join(root, "pi", "package.json")]);
      expect(fs.existsSync(path.join(root, "evidence"))).toBe(false);
    });
  });

  describe("terminal proof", () => {
    it.each([undefined, "", "  ", "relative-cli.js"])('rejects missing/invalid CLI %j before artifact reads', cli => {
      const root = fixture();
      const result = run(terminalProof, [...terminalOptions(root), ...(cli === undefined ? [] : ["--pi-cli", cli])], root);
      expectNoEffects(result, /--pi-cli/);
      expect(result.observations).toEqual([]);
    });

    it("validates all required values before reading any artifacts", () => {
      const root = fixture();
      const result = run(terminalProof, ["--pi-cli", path.join(root, "unread-cli.js"), "--fabric-root", root, "--evidence-dir", "", "--expect-terminal", "true"], root);
      expectNoEffects(result, /--evidence-dir/);
      expect(result.observations).toEqual([]);
    });

    it("rejects a directory instead of the explicit native CLI", () => {
      const root = fixture(), fabric = builtFixture(root);
      expectNoEffects(run(terminalProof, [...terminalOptions(root, fabric), "--pi-cli", root], root), /--pi-cli must name an existing file/);
    });

    it("rejects a nonexistent CLI before inspecting the Fabric directory", () => {
      const root = fixture(), cli = path.join(root, "missing-cli.js");
      const result = run(terminalProof, [...terminalOptions(root), "--pi-cli", cli], root);
      expectNoEffects(result, /--pi-cli must name an existing file/);
      expect(result.observations).toEqual([{ kind: "read", method: "existsSync", path: cli }]);
    });

    it("rejects an evidence file before inspecting built artifacts", () => {
      const root = fixture(), cli = stubCli(root), fabric = builtFixture(root), evidence = path.join(root, "evidence");
      fs.writeFileSync(evidence, "not a directory");
      const result = run(terminalProof, [...terminalOptions(root, fabric), "--pi-cli", cli], root);
      expectNoEffects(result, /--evidence-dir must name a directory/);
      expect(result.observations.some(event => event.path?.startsWith(path.join(fabric, "dist")))).toBe(false);
    });

    it("preserves the forbidden scratch-location guard", () => {
      const root = fixture();
      const result = run(terminalProof, [...terminalOptions(root), "--pi-cli", path.join(root, "unread-cli.js")], root, { TMPDIR: "/var/tmp/unused-proof" });
      expect(result.output).toContain("Scratch must not use /var/tmp");
      expect(result.observations).toEqual([]);
    });

    it("advances explicit valid CLI/artifacts to the evidence-reuse guard without launching Pi", () => {
      const root = fixture(), cli = stubCli(root), fabric = builtFixture(root), evidence = path.join(root, "evidence");
      fs.mkdirSync(evidence);
      fs.writeFileSync(path.join(evidence, "sentinel"), "never overwrite proof generations");
      const result = run(terminalProof, [...terminalOptions(root, fabric), "--pi-cli", cli], root);
      expect(result.output).toContain("Use a new/empty --evidence-dir; never mix proof generations");
      expect(result.observations.filter(event => event.kind !== "read")).toEqual([]);
      expect(fs.readFileSync(path.join(evidence, "sentinel"), "utf8")).toBe("never overwrite proof generations");
    });
  });

  describe("real-host SSH bridge proof", () => {
    it.each([[], [""], [" "], ["--not-config"], ["one", "two"]].map(args => ({ args })))("rejects missing/invalid config filenames $args without fallback evidence", ({ args }) => {
      const root = fixture(), result = run(bridgeProof, args, root);
      expectNoEffects(result, /CONFIG.json/);
      expect(result.observations).toEqual([]);
      expect(fs.existsSync(path.join(root, ".local"))).toBe(false);
    });

    it("requires a config file, not a directory", () => {
      const root = fixture(), result = run(bridgeProof, [root], root);
      expectNoEffects(result, /CONFIG.json must be a file/);
      expect(result.observations.filter(event => event.method === "readFileSync")).toEqual([]);
    });

    it.each([undefined, null, "", " ", "-oProxyCommand=unsafe", "/not/a/host", "user@bridge.invalid\n"])('requires explicit valid bridgeHost %j before evidence or process work', bridgeHost => {
      const root = fixture(), config = bridgeConfig(root);
      if (bridgeHost !== undefined) config.bridgeHost = bridgeHost;
      const file = writeConfig(root, config), result = run(bridgeProof, [file], root);
      expectNoEffects(result, /bridgeHost|SSH host must not be an option/);
      expect(result.observations.filter(event => event.method === "readFileSync").map(event => event.path)).toEqual([file]);
      expect(fs.existsSync(path.join(root, ".local"))).toBe(false);
    });

    it.each(["relative-mesh", "/invalid\u0000mesh", "/invalid\rmesh", "/invalid\nmesh"])("preserves required absolute config path validation for %j", localMesh => {
      const root = fixture(), file = writeConfig(root, { ...bridgeConfig(root), bridgeHost: "proof-bridge.invalid", localMesh });
      expectNoEffects(run(bridgeProof, [file], root), /required absolute path: localMesh/);
    });

    it("rejects a lexical /tmp evidenceRoot before any evidence effects", () => {
      const root = fixture(), syntheticRoot = path.join("/tmp", `privacy-script-inputs-forbidden-${randomUUID()}`);
      const evidenceRoot = path.join(syntheticRoot, ".local", "evidenceRoot");
      const config = { ...bridgeConfig(root), bridgeHost: "proof-bridge.invalid", evidenceRoot };
      const file = writeConfig(root, config), result = run(bridgeProof, [file], root);
      // The forbidden /tmp target is config text only; real fixture IO stays in this harness-owned root.
      expectNoEffects(result, /evidenceRoot must be under \.local, not \/tmp/);
      expect(result.observations.filter(event => event.method === "readFileSync").map(event => event.path)).toEqual([file]);
      expect(fs.existsSync(syntheticRoot)).toBe(false);
      expect(fs.existsSync(evidenceRoot)).toBe(false);
    });

    it("advances an explicit host to the actual local mesh-binding guard, with evidence only dry-recorded", () => {
      const root = fixture(), syntheticRoot = `/privacy-script-inputs-dry-${randomUUID()}`;
      const evidenceRoot = path.join(syntheticRoot, ".local", "evidenceRoot");
      // Both evidence settings are lexical only. The remote is never contacted;
      // only this exact synthetic prefix permits dry-recorded evidence effects.
      const config = { ...bridgeConfig(root), bridgeHost: "proof-bridge.invalid", evidenceRoot, remoteEvidenceRoot: evidenceRoot };
      const values = config as Record<string, string>;
      expect(fs.existsSync(syntheticRoot)).toBe(false);
      for (const key of ["localMesh", "localAgentDir", "localCwd"]) fs.mkdirSync(values[key]!, { recursive: true });
      for (const key of ["localFabric", "bridgeKey", "bridgeKnownHosts"]) fs.writeFileSync(values[key]!, "fixture only\n");
      values.bridgeBin = path.join(root, "mesh-bridge");
      fs.writeFileSync(values.bridgeBin, "fixture only\n");
      fs.mkdirSync(path.join(values.localCwd!, ".pi"));
      fs.writeFileSync(path.join(values.localCwd!, ".pi", "fabric.json"), JSON.stringify({ mesh: { root: path.join(root, "wrong-mesh") } }));
      const result = run(bridgeProof, [writeConfig(root, config)], root, { PRIVACY_DRY_ROOT: values.evidenceRoot! });
      expect(result.output).toContain("own local cwd .pi/fabric.json mesh.root must match localMesh exactly");
      const effects = result.observations.filter(event => event.kind === "effect");
      expect(effects.length).toBeGreaterThan(0);
      expect(effects.every(event => event.dry && event.path?.startsWith(values.evidenceRoot! + path.sep))).toBe(true);
      expect(effects.some(event => event.bridgeHost === "proof-bridge.invalid")).toBe(true);
      // No external writes: every observed effect is dry, and neither the
      // synthetic ancestor nor its evidence directory exists after the child.
      expect(result.observations.filter(event => event.kind === "effect" && !event.dry)).toEqual([]);
      expect(fs.existsSync(syntheticRoot)).toBe(false);
      expect(fs.existsSync(values.evidenceRoot!)).toBe(false);
    });
  });
});
