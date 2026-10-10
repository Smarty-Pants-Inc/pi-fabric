import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { idleWatchdogCommand } from "../src/guards/actor-bash-timeout.js";

const guardPath = path.join(os.homedir(), ".local/share/smarty-dev/factory/current/setup/pi/bash-guard.ts");
const guardPresent = fs.existsSync(guardPath);
// Fleet workers launch Pi with their own Bun execPath (worker.ts spawnCli), not Vitest's Node.
const fleetRuntime = (process.env.PATH ?? "").split(path.delimiter)
  .map(directory => path.resolve(directory, "bun")).find(file => fs.existsSync(file));
const quote = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;

// This is the actual installed fleet release, not a copied parser or a permissive test double.
// CI lacks the host factory installation; the skip title names the absent file explicitly.
describe.skipIf(process.platform === "win32" || !guardPresent)(
  `installed fleet bash guard (${guardPresent ? guardPath : `SKIP: absent ${guardPath}; factory not installed in CI`})`,
  () => {
    it("allows the fleet Bun literal-file wrapper around plain git log --oneline", async () => {
      const guard = await import(/* @vite-ignore */ pathToFileURL(guardPath).href) as {
        violation(command: string, cwd: string): string | undefined;
      };
      expect(fleetRuntime, "the installed fleet guard test requires its Bun runtime on PATH").toBeDefined();
      const command = idleWatchdogCommand("git log --oneline", 180, fleetRuntime!);
      const prefix = command.split("\n").slice(0, 2).join("\n");
      expect(prefix).not.toMatch(/[$`]/);
      expect(prefix).not.toContain(" -e ");
      expect(prefix).toContain("bash-idle-watchdog.js");
      expect(guard.violation(command, process.cwd())).toBeUndefined();
      // Regression control: Option A must still reject the old computed interpreter operand.
      const old = `exec ${quote(fleetRuntime!)} -e 'console.log(1)' 180 "\${BASH:-$0}" <<'OLD'\ngit log --oneline\nOLD\n`;
      expect(guard.violation(old, process.cwd())).toContain("with computed code");
    });
  },
);

// Structural coverage runs even when the installed fleet guard is absent.
describe.skipIf(process.platform === "win32")("file-based bash idle command", () => {
  it("resolves an absolute BASH at wrap time and shell-quotes literal paths", () => {
    const shell = "/tmp/bash with 'quotes'";
    vi.stubEnv("BASH", shell);
    try {
      const command = idleWatchdogCommand("echo '$HOME'; echo `date`", 2, "/tmp/node with 'quotes'");
      const prefix = command.split("\n")[1]!;
      expect(prefix).toContain(quote("/tmp/node with 'quotes'"));
      expect(prefix).toContain(` 2 ${quote(shell)} <<'PI_FABRIC_IDLE_`);
      expect(prefix).not.toMatch(/[$`]/);
      expect(command).toContain("\necho '$HOME'; echo `date`\n");
    } finally { vi.unstubAllEnvs(); }
  });

  it("ignores a relative BASH in favor of Pi's literal absolute shell", () => {
    vi.stubEnv("BASH", "relative-bash");
    try {
      const prefix = idleWatchdogCommand("git log --oneline", 180).split("\n")[1]!;
      expect(prefix).not.toContain("relative-bash");
      expect(prefix).toMatch(/ 180 '\/[^']+' <<'/);
      expect(prefix).not.toMatch(/[$`]/);
    } finally { vi.unstubAllEnvs(); }
  });
});
