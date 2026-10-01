#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crossSpawn from "cross-spawn";
import { observeResidentOwner } from "./launcher-owner.js";
import { residentProcessAlive } from "./process-identity.js";

// Same pattern as worker.ts: on Windows, `pi` resolves to a node_modules/.bin
// .cmd shim that a raw spawn cannot execute, and a .js pi entry must run under
// a real runtime. The launcher itself is always started through a resolved
// generic runtime, so process.execPath is node or bun here.
const NODE_SCRIPT_EXTENSIONS = new Set([".js", ".cjs", ".mjs", ".ts", ".cts", ".mts"]);
const spawnPi = (
  command: string,
  args: readonly string[],
  options: Parameters<typeof crossSpawn>[2],
): ReturnType<typeof crossSpawn> =>
  NODE_SCRIPT_EXTENSIONS.has(path.extname(command).toLowerCase())
    ? crossSpawn(process.execPath, [command, ...args], options)
    : crossSpawn(command, [...args], options);

const parseConfigPath = (argv: readonly string[]): string => {
  const index = argv.indexOf("--config");
  const value = index >= 0 ? argv[index + 1] : undefined;
  if (!value) throw new Error("Missing resident launcher argument: --config");
  return path.resolve(value);
};

const readConfig = (configPath: string): { cwd: string; piBinary: string } => {
  const value: unknown = JSON.parse(fs.readFileSync(configPath, "utf8"));
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid Fabric resident host config");
  }
  const config = value as { cwd?: unknown; piBinary?: unknown };
  if (typeof config.cwd !== "string" || typeof config.piBinary !== "string") {
    throw new Error("Fabric resident host config is incomplete");
  }
  return { cwd: config.cwd, piBinary: config.piBinary };
};

const liveOwnerPid = (ownerPath: string): number | undefined => {
  try {
    const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as { pid?: unknown; processStartTime?: string };
    if (typeof owner.pid !== "number") return undefined;
    return residentProcessAlive(owner.pid, owner.processStartTime) ? owner.pid : undefined;
  } catch {
    return undefined;
  }
};

const writeFailure = (configPath: string, error: unknown): void => {
  try {
    const message = error instanceof Error ? error.message : String(error);
    const dir = path.dirname(configPath);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, "error.json"), JSON.stringify({
      error: message,
      occurredAt: Date.now(),
    }, null, 2));
    fs.appendFileSync(path.join(dir, "launcher.log"), `${JSON.stringify({ event: "launcher-failed", at: Date.now(), message })}\n`);
  } catch {
    // Startup diagnostics are best-effort.
  }
};

const configPath = parseConfigPath(process.argv);
try {
  const config = readConfig(configPath);
  // Lifecycle trace: the launcher can otherwise fail silently (broken args,
  // an unspawnable pi binary), leaving the client to guess at the cause.
  const trace = (event: string, extra: Record<string, unknown> = {}): void => {
    try {
      const dir = path.dirname(configPath);
      fs.mkdirSync(dir, { recursive: true });
      fs.appendFileSync(path.join(dir, "launcher.log"), `${JSON.stringify({
        event, at: Date.now(), ...extra,
      })}\n`);
    } catch {
      // Diagnostics are best-effort.
    }
  };
  trace("launcher-started", { pid: process.pid, configPath, platform: process.platform });
  const entry = fileURLToPath(new URL("./pi-entry.js", import.meta.url));
  // Pi loads extension peers through its virtual module runtime; raw Node cannot.
  const child = spawnPi(config.piBinary, [
    "--mode", "rpc",
    "--no-session",
    "--no-tools",
    "--no-extensions",
    "--no-skills",
    "--no-prompt-templates",
    "--no-context-files",
    "--extension", entry,
  ], {
    cwd: config.cwd,
    detached: false,
    // RPC ends on stdin EOF. Keep it open only while this child owns residency.
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, PI_FABRIC_RESIDENT_CONFIG: configPath,
      // Per-attempt argument, never the shared config another client may rewrite.
      PI_FABRIC_RESIDENT_LAUNCH_TOKEN: process.argv.includes("--launch-token")
        ? process.argv[process.argv.indexOf("--launch-token") + 1] ?? "" : "",
    },
  });
  let seenOwner = false;
  let claimedOwner = false;
  let closingInput = false;
  let stderr = "";
  const ownerPath = path.join(path.dirname(configPath), "owner.json");
  // The resident host may start slowly or fail silently; keep the child stderr
  // and the latest ownership observation on disk so a client-side startup
  // timeout can surface the real cause instead of a bare deadline error.
  const childLogPath = path.join(path.dirname(configPath), "child-stderr.log");
  try { fs.rmSync(childLogPath, { force: true }); } catch { /* best effort */ }
  // The extension loader may report load failures on stdout; capture both
  // streams so a silent child is diagnosable from the log alone.
  child.stdout?.on("data", (chunk: Buffer) => {
    try { fs.appendFileSync(childLogPath, chunk); } catch { /* best effort */ }
  });
  const ownerPoll = setInterval(() => {
    const observation = observeResidentOwner(liveOwnerPid(ownerPath), child.pid, claimedOwner);
    claimedOwner = observation.claimed;
    seenOwner ||= observation.observedOwner;
    if (observation.closeInput && !closingInput) {
      closingInput = true;
      child.stdin?.end();
    }
  }, 50);
  ownerPoll.unref();
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr = `${stderr}${chunk}`.slice(-4_000);
    try { fs.appendFileSync(childLogPath, chunk); } catch { /* best effort */ }
  });
  trace("child-spawned", { pid: child.pid });
  child.on("error", (error) => {
    trace("child-error", { message: error instanceof Error ? error.message : String(error) });
    writeFailure(configPath, error);
  });
  child.on("exit", (code, signal) => {
    clearInterval(ownerPoll);
    // A clean exit after ownership writes nothing: the host already released owner.json, and a
    // new host may own this directory now (smarty-dev#2010). Abnormal exits keep their trace.
    if (!seenOwner || code !== 0 || signal) trace("child-exit", { code, signal, seenOwner });
    if (!seenOwner) writeFailure(configPath, stderr.trim() || `Pi resident host exited (${signal ?? code ?? "unknown"})`);
    process.exitCode = code ?? 1;
  });
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => child.kill(signal));
  }
} catch (error) {
  writeFailure(configPath, error);
  process.exitCode = 1;
}
