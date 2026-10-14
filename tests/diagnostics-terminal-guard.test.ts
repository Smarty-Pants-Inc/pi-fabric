import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { expect, it } from "vitest";

// Counts pin the individual call sites: a NEW write, even in an allowed process file, fails.
// Reasons describe execution boundaries, not a blanket exemption for future output.
const allowlist: Record<string, { reason: string; calls: Record<string, number> }> = {
  "src/actors-cli.ts": { reason: "Standalone fabric-actors CLI output; not imported by the Pi extension.", calls: { "process.stdout.write": 1, "process.stderr.write": 1 } },
  "src/bash-idle-watchdog.ts": { reason: "Separate bash child wrapper forwards the captured child's streams, not the Main terminal.", calls: { "process.stderr.write": 4, "process.stdout.write": 1 } },
  "src/cli/index.ts": { reason: "Standalone pi-fabric command entry.", calls: { "process.stdout.write": 1, "process.stderr.write": 2 } },
  "src/cli/mesh.ts": { reason: "CLI subcommand, loaded only by the standalone entry.", calls: { "process.stdout.write": 2, "process.stderr.write": 2 } },
  "src/core/agent-dir.ts": { reason: "The single diagnostics sink's no-terminal-UI fallbacks preserve the caller's exact console method or raw stderr channel.", calls: { "console[level]": 1, "process.stderr.write": 1 } },
  "src/durable/worker-host.ts": { reason: "Separate durable worker's print/JSON host, never Pi Main's TUI.", calls: { "process.stderr.write": 4 } },
  "src/durable/worker.ts": { reason: "Fatal error in standalone durable worker entry.", calls: { "process.stderr.write": 1 } },
  "src/extension.ts": { reason: "Exactly one resident readiness protocol ACK, gated by mode === rpc and PI_FABRIC_PARENT_RUN; TUI refuses before this write.", calls: { "writeSync": 1 } },
  "src/guards/fixture-mode.ts": { reason: "Fatal unsupported-host refusal during extension registration, before the UI exists; exits immediately.", calls: { "console.error": 1 } },
  "src/judge-cli.ts": { reason: "Standalone JSON judge CLI, not a Main diagnostic.", calls: { "process.stdout.write": 1 } },
  "src/mesh/mesh-backend-cli.ts": { reason: "Standalone migration CLI's explicit stdout/stderr IO defaults.", calls: { "process.stdout.write": 1, "process.stderr.write": 1 } },
  "src/mesh/nats-auth/cli.ts": { reason: "Standalone pi-fabric nats subcommand, loaded only by cli/index.ts: help, usage/refusal diagnostics and requested public config/mint metadata; never Pi Main's TUI.", calls: { "process.stdout.write": 3, "process.stderr.write": 2 } },
  "src/mesh-bridge.ts": { reason: "Standalone bridge/stdio protocol process, never imported by the Main extension.", calls: { "process.stderr.write": 2, "process.stdout.write": 1 } },
  "src/mesh-lock-stats-cli.ts": { reason: "Standalone lock-statistics CLI IO defaults.", calls: { "process.stdout.write": 1, "process.stderr.write": 1 } },
  "src/participants-cli.ts": { reason: "Standalone fabric-participants CLI IO defaults.", calls: { "process.stdout.write": 1, "process.stderr.write": 1 } },
  "src/records/server.ts": { reason: "Server instantiated only by records/service-main.ts, a separate service process.", calls: { "process.stderr.write": 2 } },
  "src/records/service-main.ts": { reason: "Separate records daemon/administrative CLI; never the Pi editor.", calls: { "process.stderr.write": 7, "process.stdout.write": 7 } },
  "src/releases-cli.ts": { reason: "Standalone release-selector CLI IO default.", calls: { "process.stdout.write": 1 } },
  "src/residency/host.ts": { reason: "Standalone hidden resident daemon, no terminal UI; worker hosts run separately.", calls: { "console.warn": 8, "console.error": 1, "process.stderr.write": 1 } },
  "src/storage/retention-cli.ts": { reason: "Standalone maintenance CLI diagnostics and report output.", calls: { "console.error": 4, "console.log": 1 } },
  "src/worker/activation-window.ts": { reason: "Worker-only fatal error and parent/child activation protocol, loaded by worker task entry.", calls: { "fs.writeSync": 3 } },
  "src/worker/session-id.ts": { reason: "Worker-only parent protocol session ACK, gated by PI_FABRIC_PARENT_RUN.", calls: { "fs.writeSync": 1 } },
  "src/worker.ts": { reason: "Standalone task/actor worker process: captured output and protocol streams, no Main TUI.", calls: { "console.error": 2, "process.stdout.write": 7, "process.stderr.write": 4 } },
};

it("lists src console and direct terminal writes; new call sites outside the sink fail CI", () => {
  const root = fileURLToPath(new URL("../", import.meta.url));
  const found: Record<string, Record<string, number>> = {};
  const sites: string[] = [];
  const visit = (directory: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) { visit(file); continue; }
      if (!/\.[cm]?[jt]s$/.test(file)) continue;
      const relative = path.relative(root, file).split(path.sep).join("/");
      const source = ts.createSourceFile(file, fs.readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
      const walk = (node: ts.Node): void => {
        if (ts.isCallExpression(node)) {
          const name = node.expression.getText(source);
          const consoleCall = /^console(?:\.(warn|error|info|debug|log)|\[)/.test(name);
          const streamCall = /^process\.(stderr|stdout)\.write$/.test(name);
          const fdCall = /^(?:fs\.)?writeSync$/.test(name) && /^(1|2)$/.test(node.arguments[0]?.getText(source) ?? "");
          if (consoleCall || streamCall || fdCall) {
            const calls = found[relative] ??= {};
            calls[name] = (calls[name] ?? 0) + 1;
            const { line } = source.getLineAndCharacterOfPosition(node.getStart(source));
            sites.push(`${relative}:${line + 1} ${node.getText(source)} — ${allowlist[relative]?.reason ?? "NOT ALLOWED"}`);
          }
        }
        ts.forEachChild(node, walk);
      };
      walk(source);
    }
  };
  visit(path.join(root, "src"));
  expect(found, sites.join("\n")).toEqual(Object.fromEntries(Object.entries(allowlist).map(([file, entry]) => [file, entry.calls])));
});
