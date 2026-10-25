#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runEventsCli } from "./events.js";

/**
 * `pi-fabric` out-of-process CLI. A standalone entry: it never imports the
 * extension graph. Mesh, decisions and NATS subcommands load on invocation; the small
 * events command is linked here to keep the standalone CLI's package path stable.
 * Subcommands take their remaining argv and return a process exit code.
 */
type FabricCliCommand = (argv: string[]) => Promise<number>;

const commands: Record<string, () => Promise<FabricCliCommand>> = {
  mesh: async () => (await import("./mesh.js")).runMeshCli,
  decisions: async () => (await import("./decisions.js")).runDecisionsCli,
  nats: async () => (await import("../mesh/nats-auth/cli.js")).runNatsCli,
  // The NATS link CLI, also as bin/nats-link; images that link only pi-fabric reach `nats-link roots` here.
  "nats-link": async () => (await import("../mesh-bridge-nats-cli.js")).main,
  events: async () => runEventsCli,
  session: async () => runSessionCli,
};

const SESSION_USAGE = [
  "Usage: pi-fabric session pending <session-uuid | session:uuid> [--mesh <meshRoot>] [--json]",
  "       pi-fabric session drain <session-uuid | session:uuid> --mesh <meshRoot> [--status | --exit | --reopen] [--deadline-s <1-600>]",
].join("\n");

/** `pi-fabric session pending`: read-only recorded pending-state inventory for one Main session. */
async function runSessionCli(argv: string[]): Promise<number> {
  const [name, ...rest] = argv;
  if (name === "pending") return (await import("./session-pending.js")).runSessionPendingCli(rest);
  if (name === "drain") return (await import("./session-drain.js")).runSessionDrainCli(rest);
  if (name === "--help" || name === "-h" || name === "help") {
    process.stdout.write(`${SESSION_USAGE}\n`);
    return 0;
  }
  process.stderr.write(`pi-fabric session: unknown subcommand ${JSON.stringify(name ?? "")}\n${SESSION_USAGE}\n`);
  return 2;
}

const usage = (): string => [
  "Usage: pi-fabric <command> [options]",
  "",
  "Commands:",
  ...Object.keys(commands).map((name) => `  ${name}`),
  "",
  "Run `pi-fabric <command> --help` for command options.",
].join("\n");

export async function runFabricCli(argv: string[]): Promise<number> {
  const [name, ...rest] = argv;
  if (!name || name === "--help" || name === "-h" || name === "help") {
    process.stdout.write(`${usage()}\n`);
    return name ? 0 : 2;
  }
  const load = Object.hasOwn(commands, name) ? commands[name] : undefined;
  if (!load) {
    process.stderr.write(`pi-fabric: unknown command ${JSON.stringify(name)}\n${usage()}\n`);
    return 2;
  }
  return (await load())(rest);
}

const invokedPath = process.argv[1];
const isMain = (() => {
  if (!invokedPath) return false;
  try {
    // npm links bins through symlinks; compare canonical paths.
    return fs.realpathSync(path.resolve(invokedPath)) === fs.realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
})();
if (isMain) {
  try {
    process.exitCode = await runFabricCli(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`pi-fabric: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
