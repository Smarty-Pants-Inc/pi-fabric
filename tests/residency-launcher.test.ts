import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { describe, expect, it } from "vitest";
import { exitMarkerReader, observeResidentOwner } from "../src/residency/launcher-owner.js";

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

// smarty-dev#7770: a clean idle exit must not look like an outside kill.
describe("resident exit marker reader", () => {
  it("sees a marker split across two chunks once", () => {
    const read = exitMarkerReader();
    expect(read("noise\npi-fabric-resident-exit {\"rea")).toBeUndefined();
    expect(read("son\":\"idle-exit\"}\r\nmore")).toBe("idle-exit");
    expect(read("\n")).toBeUndefined();
  });

  it("accepts only an anchored, complete line with a known reason", () => {
    const read = exitMarkerReader();
    expect(read('pi-fabric-resident-exit {"reason":"rebooted"}\n')).toBeUndefined();
    expect(read('x pi-fabric-resident-exit {"reason":"idle-exit"}\n')).toBeUndefined();
    expect(read('pi-fabric-resident-exit {"reason":"idle-exit","x":1}\n')).toBeUndefined();
    expect(read('pi-fabric-resident-exit {"reason":"idle-exit"}')).toBeUndefined(); // no line end yet
  });
});

describe.skipIf(!hasLauncher || process.platform === "win32")("resident launcher child exit log", () => {
  const marker = (reason: string) => `pi-fabric-resident-exit {"reason":"${reason}"}\n`;
  const run = async (mode: "idle" | "kill" | "stdout" | "unknown-reason"): Promise<{ rows: Record<string, unknown>[]; stderr: string }> => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-exit-"));
    const hostBinary = path.join(root, "fake-host.mjs");
    const owner = path.join(root, "owner.json");
    // Like the real host: report on stderr, release owner.json last, exit 0.
    const report = {
      // Two writes in separate ticks: the marker arrives in two chunks.
      idle: `process.stderr.write(${JSON.stringify(marker("idle-exit").slice(0, 30))}); setTimeout(() => process.stderr.write(${JSON.stringify(marker("idle-exit").slice(30))}), 100);`,
      stdout: `process.stdout.write(${JSON.stringify(marker("idle-exit"))});`,
      "unknown-reason": `process.stderr.write(${JSON.stringify(marker("rebooted"))});`,
      kill: "",
    }[mode];
    fs.writeFileSync(hostBinary, [
      "import fs from 'node:fs';",
      `fs.writeFileSync(${JSON.stringify(owner)}, JSON.stringify({ pid: process.pid }));`,
      mode === "kill"
        ? "setTimeout(() => process.kill(process.pid, 'SIGKILL'), 500);"
        : `setTimeout(() => { ${report} setTimeout(() => { fs.rmSync(${JSON.stringify(owner)}); process.exit(0); }, 300); }, 500);`,
      "setInterval(() => {}, 1000);",
      "",
    ].join("\n"));
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostBinary }));
    try {
      const stderr = await new Promise<string>(resolve => {
        execFile(process.execPath, [launcherPath, "--config", configPath], { cwd: root, timeout: 25_000 }, (_error, _stdout, err) => resolve(String(err)));
      });
      const rows = fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n")
        .map(line => JSON.parse(line) as Record<string, unknown>).filter(row => String(row.event).startsWith("child-exit"));
      return { rows, stderr };
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  };

  it("names a clean idle exit, once, from a marker split across chunks", { timeout: 30_000 }, async () => {
    // Logged while the host still owns the root; nothing is written after its release (#1882).
    const { rows } = await run("idle");
    expect(rows).toEqual([expect.objectContaining({ event: "child-exit-reported", reason: "idle-exit" })]);
  });

  it("names the signal of a killed host", { timeout: 30_000 }, async () => {
    expect((await run("kill")).rows).toEqual([expect.objectContaining({ event: "child-exit", code: null, signal: "SIGKILL", reason: "signal", seenOwner: true })]);
  });

  it.each(["stdout", "unknown-reason"] as const)("ignores a %s marker; an unreported owned exit is 'unknown' on stderr, never in the released root", { timeout: 30_000 }, async mode => {
    const { rows, stderr } = await run(mode);
    expect(rows).toEqual([]);
    const line = stderr.split("\n").map(row => { try { return JSON.parse(row) as Record<string, unknown>; } catch { return undefined; } })
      .filter(row => row?.event === "child-exit");
    expect(line).toEqual([expect.objectContaining({ code: 0, signal: null, reason: "unknown", seenOwner: true })]);
  });
});
