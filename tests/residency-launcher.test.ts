import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { describe, expect, it } from "vitest";
import { observeResidentOwner } from "../src/residency/launcher-owner.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const hasLauncher = fs.existsSync(launcherPath);

describe("resident launcher owner observation", () => {
  it("waits while its child has not claimed residency", () => {
    expect(observeResidentOwner(undefined, 20, false)).toEqual({
      claimed: false,
      observedOwner: false,
      closeInput: false,
    });
  });

  it("keeps stdin open while its child owns residency", () => {
    expect(observeResidentOwner(20, 20, false)).toEqual({
      claimed: true,
      observedOwner: true,
      closeInput: false,
    });
  });

  it("closes a duplicate child when another live host owns residency", () => {
    expect(observeResidentOwner(10, 20, false)).toEqual({
      claimed: false,
      observedOwner: true,
      closeInput: true,
    });
  });

  it("closes stdin after its owned host releases residency", () => {
    expect(observeResidentOwner(undefined, 20, true)).toEqual({
      claimed: true,
      observedOwner: false,
      closeInput: true,
    });
  });
});

// The persistent-actor path inherits the owner's environment twice: the
// launcher spawns the resident host pi with `{ ...process.env }`, and the
// host's AgentManager passes the resolved (shim) binary to the worker, which
// spawns the child with `{ ...process.env }` again. The launcher link is
// regression-tested here with a fake host that reports only presence booleans
// of a synthetic sentinel env var — never values — mirroring what the
// LocalTerm shim injects into the parent pi's environment.
describe.skipIf(!hasLauncher || process.platform === "win32")("resident launcher env inheritance", () => {
  it("propagates the owner environment into the resident host process", { timeout: 30_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-env-"));
    const present = "FAKE_HOST_SENTINEL_KEY";
    const envLog = path.join(root, "host-env.json");
    const hostBinary = path.join(root, "fake-host.mjs");
    fs.writeFileSync(
      hostBinary,
      [
        "#!/usr/bin/env node",
        "import fs from 'node:fs';",
        `const name = ${JSON.stringify(present)};`,
        "fs.writeFileSync(process.env.FAKE_HOST_ENV_LOG, JSON.stringify({",
        "  [name]: process.env[name] ? 'present' : 'absent',",
        "}));",
        "process.exit(0);",
        "",
      ].join("\n"),
    );
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(
      configPath,
      JSON.stringify({ cwd: root, piBinary: hostBinary }),
    );

    try {
      await new Promise<void>((resolve) => {
        execFile(
          process.execPath,
          [launcherPath, "--config", configPath],
          {
            cwd: root,
            env: {
              ...process.env,
              [present]: "sentinel",
              FAKE_HOST_ENV_LOG: envLog,
            },
            timeout: 25_000,
          },
          // The launcher exits non-zero when the host exits without claiming
          // residency (expected here — the fake host exits immediately); the
          // env log is the assertion target, not the exit code.
          () => resolve(),
        );
      });

      expect(fs.existsSync(envLog)).toBe(true);
      expect(JSON.parse(fs.readFileSync(envLog, "utf8"))).toEqual({
        [present]: "present",
      });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// smarty-dev#7770 / #2010: the host names its exit itself while it owns the
// root; after a clean owned exit the launcher writes nothing into the root.
describe.skipIf(!hasLauncher || process.platform === "win32")("resident launcher after a clean owned exit", () => {
  it("writes nothing new into the root after the child exits", { timeout: 30_000 }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-exit-"));
    const root = path.join(base, "root");
    fs.mkdirSync(root);
    const owner = path.join(root, "owner.json");
    const released = path.join(base, "released.json");
    const hostBinary = path.join(base, "fake-host.mjs");
    fs.writeFileSync(hostBinary, `import fs from 'node:fs'; import path from 'node:path';
const root = ${JSON.stringify(root)};
const snapshot = () => Object.fromEntries(fs.readdirSync(root).map(name => { const s = fs.statSync(path.join(root, name)); return [name, s.size + ':' + s.mtimeMs]; }));
fs.writeFileSync(${JSON.stringify(owner)}, JSON.stringify({ pid: process.pid }));
setTimeout(() => {
  // Like the real host: its line, then the release, then exit 0.
  fs.appendFileSync(path.join(root, 'launcher.log'), JSON.stringify({ event: 'resident-exit', at: Date.now(), reason: 'idle-exit', pid: process.pid }) + '\\n');
  fs.rmSync(${JSON.stringify(owner)});
  fs.writeFileSync(${JSON.stringify(released)}, JSON.stringify(snapshot()));
  process.exit(0);
}, 500);
setInterval(() => {}, 1000);
`);
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostBinary }));
    try {
      await new Promise<void>(resolve => {
        execFile(process.execPath, [launcherPath, "--config", configPath], { cwd: root, timeout: 25_000 }, () => resolve());
      });
      await new Promise(resolve => setTimeout(resolve, 500));
      const before = JSON.parse(fs.readFileSync(released, "utf8")) as Record<string, string>;
      const after = Object.fromEntries(fs.readdirSync(root).map(name => {
        const stat = fs.statSync(path.join(root, name));
        return [name, `${stat.size}:${stat.mtimeMs}`];
      }));
      expect(after).toEqual(before);
      const rows = fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
      expect(rows.at(-1)).toMatchObject({ event: "resident-exit", reason: "idle-exit" });
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});
