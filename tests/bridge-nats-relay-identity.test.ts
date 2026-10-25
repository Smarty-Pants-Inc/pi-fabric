// A' PR 2 relay (smarty-dev#15310): the hub's relay IPC admits a `bin/nats-link start --config FILE` process of
// the SAME release as a relay peer, with its names and mesh read from its private config, and nothing else.
// Real processes are spawned (idle stand-in entry scripts), so the /proc identity checks run for real.
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { inspectBridgeRelayProcessForTest } from "../src/mesh/bridge-relay.js";

const RUNS = process.platform === "linux" && typeof process.getuid === "function" && process.release.name === "node";
const IDLE = "setInterval(() => {}, 1 << 30);\n";

let root: string;
const children: ChildProcess[] = [];
afterEach(() => {
  for (const child of children.splice(0)) child.kill("SIGKILL");
  if (root) fs.rmSync(root, { recursive: true, force: true });
});

const setup = () => {
  root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bridge-relay-identity-")));
  const release = (name: string) => {
    const bin = path.join(root, name, "bin");
    fs.mkdirSync(bin, { recursive: true });
    for (const entry of ["mesh-bridge", "nats-link"]) fs.writeFileSync(path.join(bin, entry), IDLE, { mode: 0o755 });
    return bin;
  };
  const bin = release("release-a");
  const otherBin = release("release-b");
  const mesh = path.join(root, "mesh");
  fs.mkdirSync(mesh, { mode: 0o700 });
  const configDir = path.join(root, "config");
  fs.mkdirSync(configDir, { mode: 0o700 });
  const config = (name: string, overrides: Record<string, unknown> = {}, mode = 0o600): string => {
    const file = path.join(configDir, name);
    fs.writeFileSync(file, JSON.stringify({
      version: 1, name: "ryzen1", link: "v2", file: { mesh, lockProtocol: 2 }, stateDir: path.join(root, "state"),
      nats: { servers: ["tls://broker.example.test:4222"], machine: "ryzen1-bridge", credentials: path.join(root, "creds"),
        caFile: path.join(root, "ca.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
        pathBMachines: ["ryzen1-bridge"], operatorMachine: "fabric-ops" },
      ...overrides,
    }), { mode });
    fs.chmodSync(file, mode);
    return file;
  };
  const expected = { localName: "ryzen1", remoteName: "self", meshRoot: mesh, bridgeScriptPath: path.join(bin, "mesh-bridge"),
    nodePath: process.execPath };
  return { bin, otherBin, mesh, config, expected };
};

/** Start an idle process whose /proc cmdline is exactly `node SCRIPT ...ARGS`, and wait until it runs. */
const run = async (script: string, args: string[]): Promise<number> => {
  const child = spawn(process.execPath, [script, ...args], { stdio: "ignore", cwd: root });
  children.push(child);
  await new Promise<void>((resolve, reject) => { child.once("spawn", resolve); child.once("error", reject); });
  // The cmdline is the exec'd one once the node binary replaced the fork image.
  for (let attempt = 0; attempt < 200; attempt++) {
    try { if (fs.readFileSync(`/proc/${child.pid}/cmdline`, "utf8").includes(script)) return child.pid!; } catch { /* starting */ }
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  throw new Error("child never showed its cmdline");
};

describe.skipIf(!RUNS)("relay peer identity for the A' link", () => {
  it("admits `nats-link start --config FILE` of the same release, with names and mesh from that config", async () => {
    const { bin, config, expected } = setup();
    const pid = await run(path.join(bin, "nats-link"), ["start", "--config", config("v2.json")]);
    const identity = inspectBridgeRelayProcessForTest(pid, expected);
    expect(identity).toMatchObject({ pid, localName: "ryzen1", remoteName: "v2", bridgeScriptPath: path.join(bin, "nats-link") });
    expect(inspectBridgeRelayProcessForTest(pid, expected, "v2").remoteName).toBe("v2");
  });

  it("still admits `mesh-bridge run` peers unchanged (the counterexample the change must keep)", async () => {
    const { bin, mesh, expected } = setup();
    const pid = await run(path.join(bin, "mesh-bridge"),
      ["run", "--mesh", mesh, "--name", "ryzen1", "--remote", "ryzen2", "--cursor", path.join(root, "c.json")]);
    expect(inspectBridgeRelayProcessForTest(pid, expected)).toMatchObject({ remoteName: "ryzen2", bridgeScriptPath: path.join(bin, "mesh-bridge") });
  });

  it("refuses a nats-link from another release, another subcommand or extra arguments", async () => {
    const { bin, otherBin, config, expected } = setup();
    const file = config("v2.json");
    for (const [script, args] of [
      [path.join(otherBin, "nats-link"), ["start", "--config", file]],
      [path.join(bin, "nats-link"), ["status", "--config", file]],
      [path.join(bin, "nats-link"), ["start", "--config", file, "--extra"]],
      [path.join(bin, "nats-link"), ["start", "--config", `${root}/config/./v2.json`]], // not a normalized path
    ] as const) {
      const pid = await run(script, [...args]);
      expect(() => inspectBridgeRelayProcessForTest(pid, expected), `${script} ${args.join(" ")}`).toThrow();
    }
  });

  it("refuses a nats-link whose config names another hub, another mesh, the hub itself, or is unsafe", async () => {
    const { bin, config, expected } = setup();
    const otherMesh = path.join(root, "other-mesh");
    fs.mkdirSync(otherMesh, { mode: 0o700 });
    for (const file of [
      config("other-hub.json", { name: "ryzen9" }),
      config("other-mesh.json", { file: { mesh: otherMesh, lockProtocol: 2 } }),
      config("writable.json", {}, 0o622),
      config("readable.json", {}, 0o644), // not 0600: refused for relay peer authentication
      config("broken.json", { version: 2 }),
    ]) {
      const pid = await run(path.join(bin, "nats-link"), ["start", "--config", file]);
      expect(() => inspectBridgeRelayProcessForTest(pid, expected), file).toThrow();
    }
  });

  it("refuses a config reached through a symlink", async () => {
    const { bin, config, expected } = setup();
    const link = path.join(root, "config", "alias.json");
    fs.symlinkSync(config("v2.json"), link);
    const pid = await run(path.join(bin, "nats-link"), ["start", "--config", link]);
    expect(() => inspectBridgeRelayProcessForTest(pid, expected)).toThrow();
  });

  it("refuses a nats-link that is not the requested remote", async () => {
    const { bin, config, expected } = setup();
    const pid = await run(path.join(bin, "nats-link"), ["start", "--config", config("v2.json")]);
    expect(() => inspectBridgeRelayProcessForTest(pid, expected, "ryzen2")).toThrow();
  });
});
