import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile, spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
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

  it("drops an overlong line up to its newline, so its suffix cannot forge a marker", () => {
    const read = exitMarkerReader();
    expect(read(`${"x".repeat(300)}pi-fabric-resident-exit {"reason":"idle-exit"}`)).toBeUndefined();
    expect(read("\n")).toBeUndefined();
    expect(read('pi-fabric-resident-exit {"reason":"stopped"}\n')).toBe("stopped");
    // Also across chunks: the overflow is reached only after the marker-like text.
    expect(read("y".repeat(200))).toBeUndefined();
    expect(read(`${"y".repeat(100)}pi-fabric-resident-exit {"reason":"idle-exit"}`)).toBeUndefined();
    expect(read('more\npi-fabric-resident-exit {"reason":"error"}\n')).toBe("error");
  });
});

describe.skipIf(!hasLauncher || process.platform !== "linux")("resident launcher child exit log", () => {
  const marker = (reason: string) => `pi-fabric-resident-exit {"reason":"${reason}"}\n`;
  type Mode = "fd3-split" | "stderr-marker" | "fd3-unknown-reason" | "kill" | "fence-held" | "fence-free";
  const run = async (mode: Mode): Promise<{ rows: Record<string, unknown>[]; stderr: Record<string, unknown>[]; hostLock: boolean }> => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-exit-"));
    const hostBinary = path.join(root, "fake-host.mjs");
    const owner = path.join(root, "owner.json");
    const hostLock = path.join(root, "host.lock");
    const half = marker("idle-exit").length >> 1;
    // Like the real host: report on the exit channel (fd 3), release owner.json last, exit 0.
    const report = {
      // Two writes in separate ticks: the marker arrives in two chunks.
      "fd3-split": `fs.writeSync(3, ${JSON.stringify(marker("idle-exit").slice(0, half))}); setTimeout(() => fs.writeSync(3, ${JSON.stringify(marker("idle-exit").slice(half))}), 100);`,
      "stderr-marker": `process.stderr.write(${JSON.stringify(marker("idle-exit"))});`,
      "fd3-unknown-reason": `fs.writeSync(3, ${JSON.stringify(marker("rebooted"))});`,
      kill: "", "fence-held": "", "fence-free": "",
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
    if (mode === "fence-free") fs.writeFileSync(hostLock, "");
    // A next generation's host holds the root's fence before the child exits.
    const ready = path.join(root, "holder-ready");
    const holder = mode === "fence-held"
      ? spawn("flock", ["-x", hostLock, "sh", "-c", `touch '${ready}'; sleep 20`], { stdio: "ignore" }) : undefined;
    try {
      if (holder) while (!fs.existsSync(ready)) await new Promise(resolve => setTimeout(resolve, 20));
      const stderr = await new Promise<string>(resolve => {
        execFile(process.execPath, [launcherPath, "--config", configPath], { cwd: root, timeout: 25_000 }, (_error, _stdout, err) => resolve(String(err)));
      });
      const parse = (text: string) => text.split("\n").flatMap(line => { try { return [JSON.parse(line) as Record<string, unknown>]; } catch { return []; } })
        .filter(row => String(row.event).startsWith("child-exit"));
      return { rows: parse(fs.readFileSync(path.join(root, "launcher.log"), "utf8")), stderr: parse(stderr), hostLock: fs.existsSync(hostLock) };
    } finally {
      holder?.kill("SIGKILL");
      fs.rmSync(root, { recursive: true, force: true });
    }
  };

  it("names a clean idle exit, once, from a marker split across fd 3 chunks", { timeout: 30_000 }, async () => {
    // Logged while the host still owns the root; nothing is written after its release (#1882).
    expect((await run("fd3-split")).rows).toEqual([expect.objectContaining({ event: "child-exit-reported", reason: "idle-exit" })]);
  });

  it("names the signal of a killed host", { timeout: 30_000 }, async () => {
    expect((await run("kill")).rows).toEqual([expect.objectContaining({ event: "child-exit", code: null, signal: "SIGKILL", reason: "signal", seenOwner: true })]);
  });

  it.each(["stderr-marker", "fd3-unknown-reason", "fence-free"] as const)("%s: an unreported clean exit is 'unknown', under a free fence", { timeout: 30_000 }, async mode => {
    const { rows, stderr, hostLock } = await run(mode);
    expect(rows).toEqual([expect.objectContaining({ event: "child-exit", code: 0, signal: null, reason: "unknown", seenOwner: true })]);
    expect(stderr).toEqual([]);
    // The launcher never creates host.lock: a foreign inode would block the next host.
    expect(hostLock).toBe(mode === "fence-free");
  });

  it("adopts fd 3 close-on-exec: a grandchild cannot write the exit channel", { timeout: 30_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-cloexec-"));
    const owner = path.join(root, "owner.json"), fds = path.join(root, "fds.json");
    const hostBinary = path.join(root, "fake-host.mjs");
    const helper = pathToFileURL(path.resolve("dist/residency/launcher-owner.js")).href;
    fs.writeFileSync(hostBinary, `import fs from 'node:fs'; import { execFileSync } from 'node:child_process';
import { adoptExitChannel } from ${JSON.stringify(helper)};
// The shell's own fd 3 (ls would reuse a free fd 3 for its directory).
const list = () => execFileSync('sh', ['-c', '[ -e /proc/$$/fd/3 ] && echo 3 || echo none'], { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim().split(/\\s+/);
const before = list();
const fd = adoptExitChannel();
const after = list();
const spoof = execFileSync('sh', ['-c', 'echo \\'pi-fabric-resident-exit {"reason":"stopped"}\\' >&3 && echo wrote || echo EBADF'], { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim();
fs.writeFileSync(${JSON.stringify(fds)}, JSON.stringify({ before, after, spoof, env: process.env.PI_FABRIC_EXIT_FD ?? null }));
fs.writeFileSync(${JSON.stringify(owner)}, JSON.stringify({ pid: process.pid }));
setTimeout(() => { fs.writeSync(fd, 'pi-fabric-resident-exit {"reason":"idle-exit"}\\n'); setTimeout(() => { fs.rmSync(${JSON.stringify(owner)}); process.exit(0); }, 300); }, 500);
setInterval(() => {}, 1000);
`);
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostBinary }));
    try {
      await new Promise<void>(resolve => {
        execFile(process.execPath, [launcherPath, "--config", configPath], { cwd: root, timeout: 25_000 }, () => resolve());
      });
      const seen = JSON.parse(fs.readFileSync(fds, "utf8")) as { before: string[]; after: string[]; spoof: string; env: string | null };
      // Node 24 already starts with an inherited fd 3 close-on-exec, so `before` is
      // not asserted; adoption guarantees it for any runtime.
      expect(seen.after).not.toContain("3");
      expect(seen.spoof).toBe("EBADF");
      expect(seen.env).toBeNull();
      const rows = fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n")
        .map(line => JSON.parse(line) as Record<string, unknown>).filter(row => String(row.event).startsWith("child-exit"));
      expect(rows).toEqual([expect.objectContaining({ event: "child-exit-reported", reason: "idle-exit" })]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("writes nothing into the root while a next generation holds the fence", { timeout: 30_000 }, async () => {
    const { rows, stderr } = await run("fence-held");
    expect(rows).toEqual([]);
    expect(stderr).toEqual([expect.objectContaining({ event: "child-exit", fenced: false, code: 0, reason: "unknown" })]);
  });
});
