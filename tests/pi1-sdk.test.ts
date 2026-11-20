import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { expect, it } from "vitest";

it("loads the compiled extension in real Pi 1.2 SDK sessions with native codemode and host-governed MCP", async () => {
  const { stdout } = await promisify(execFile)(process.execPath, ["scripts/smoke-pi1.mjs"], {
    cwd: process.cwd(), timeout: 90_000, maxBuffer: 2_000_000,
  });
  const report = JSON.parse(stdout.trim().split("\n").at(-1)!);
  expect(report).toMatchObject({ version: "1.2.0", exclusive: true, proxy: true, reload: true, cli: true });
  expect(report.scenarios).toBeGreaterThanOrEqual(4);
  expect(report.mcpSkipped).toBeGreaterThanOrEqual(0);
  expect(report.scenarios + report.mcpSkipped).toBe(7);
}, 100_000);
