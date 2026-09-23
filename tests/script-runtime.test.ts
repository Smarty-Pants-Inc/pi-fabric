import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { commandAvailable } from "../src/agents/transports/process-utils.js";
import {
  resolveScriptRuntime,
  resolveScriptRuntimeSync,
  scriptSpawnArgs,
  workerCommand,
} from "../src/agents/transports/process-utils.js";

// path.basename splits on the host platform's separators, so a Windows-style
// "C:\\...\\node.exe" only basename-splits to node.exe on win32. Assert it
// only there; the other cases (with "/") split correctly on every platform.
const genericExecPaths = ["/usr/local/bin/node", "/usr/local/bin/bun", "/x/bun.exe"];
if (process.platform === "win32") genericExecPaths.push("C:\\node\\node.exe", "C:\\bun\\bun.exe");

describe("script runtime resolution", () => {
  it("reuses process.execPath when it is a generic node/bun runtime", async () => {
    for (const execPath of genericExecPaths) {
      expect(resolveScriptRuntimeSync({ execPath })).toBe(execPath);
      expect(await resolveScriptRuntime({ execPath })).toBe(execPath);
    }
  });

  it("falls back to PI_FABRIC_NODE_BINARY when execPath is the bundled pi binary", async () => {
    const execPath = "/usr/local/bin/pi";
    const override = "/opt/node-v22/bin/node";
    expect(
      resolveScriptRuntimeSync({ execPath, env: { PI_FABRIC_NODE_BINARY: override } }),
    ).toBe(override);
    expect(
      await resolveScriptRuntime({ execPath, env: { PI_FABRIC_NODE_BINARY: override } }),
    ).toBe(override);
  });

  it("builds the full spawn argv prefix through scriptSpawnArgs", async () => {
    const args = await scriptSpawnArgs(
      "/fabric/worker.js",
      ["--task-file", "/tmp/task.txt"],
      { execPath: "/usr/local/bin/pi", env: { PI_FABRIC_NODE_BINARY: "/opt/node" } },
    );
    expect(args).toEqual(["/opt/node", "/fabric/worker.js", "--task-file", "/tmp/task.txt"]);
  });

  it("quotes every token in workerCommand using the resolved runtime", async () => {
    const command = await workerCommand("/fabric/worker.js", ["--task-file", "/tmp/task.txt"]);
    const runtime = resolveScriptRuntimeSync();
    expect(command.startsWith(`'${runtime}'`)).toBe(true);
    expect(command).toContain("'/fabric/worker.js'");
    expect(command).toContain("'/tmp/task.txt'");
  });

  it("resolves node or bun from PATH when the bundled binary has no override", async () => {
    const node = await commandAvailable("node");
    const bun = await commandAvailable("bun");
    if (!node && !bun) return; // neither runtime discoverable in this environment
    const runtime = await resolveScriptRuntime({ execPath: "/usr/local/bin/pi", env: {} });
    expect(["node", "bun"]).toContain(path.basename(runtime).replace(/\.exe$/, ""));
    // Absolute, so a Herdr pane with the server's PATH starts the same runtime.
    expect(path.isAbsolute(runtime)).toBe(true);
  });

  // A shell-based lookup silently finds nothing on Windows runners (no `sh`),
  // which previously broke the bun-process executor there.
  it("finds executables by scanning PATH without a shell", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-path-scan-"));
    const probe = `fabric-path-probe-${process.pid}`;
    const file = path.join(directory, process.platform === "win32" ? `${probe}.exe` : probe);
    fs.writeFileSync(file, "", { mode: 0o755 });
    const env = { ...process.env, PATH: directory + path.delimiter + (process.env.PATH ?? "") };
    try {
      expect(await commandAvailable(probe, env)).toBe(true);
      expect(await commandAvailable(`${probe}-missing`, env)).toBe(false);
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it("throws a clear error when the bundled binary has no runtime and no override", () => {
    expect(() => resolveScriptRuntimeSync({ execPath: "/usr/local/bin/pi", env: {} })).toThrow(
      /requires a Node\.js or Bun runtime|PI_FABRIC_NODE_BINARY/,
    );
  });

  it("requireNode accepts a node execPath but rejects a bun execPath", () => {
    expect(resolveScriptRuntimeSync({ execPath: "/usr/local/bin/node", requireNode: true })).toBe(
      "/usr/local/bin/node",
    );
    expect(() =>
      resolveScriptRuntimeSync({ execPath: "/usr/local/bin/bun", requireNode: true }),
    ).toThrow();
  });

  it("launches TypeScript workers with bun so Node test hosts can boot src/worker.ts", async () => {
    if (!await commandAvailable("bun")) return;
    const args = await scriptSpawnArgs("src/worker.ts", ["--id", "x"]);
    expect(path.basename(args[0]!).replace(/\.exe$/i, "")).toBe("bun");
    expect(args[1]).toBe("src/worker.ts");
  });
});
