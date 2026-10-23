import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const probeFile = path.join(repoRoot, "tests", "fixtures", "resident-wake-bounded-lease-probe.mjs");
const CHILD_DEADLINE_MS = 10_000;
const VITEST_DEADLINE_MS = 15_000;

type ProbeCase = {
  name: string;
  expected: "retry" | "terminal";
  posixFifo?: boolean;
};

const cases: readonly ProbeCase[] = [
  { name: "oversized-unknown", expected: "retry" },
  { name: "fifo-third-open", expected: "retry", posixFifo: true },
  { name: "directory-third-open", expected: "retry" },
  { name: "symlink-lease", expected: "retry" },
  { name: "exact-cap", expected: "terminal" },
  { name: "over-cap", expected: "retry" },
  { name: "truncate-after-fstat", expected: "retry" },
  { name: "grow-after-fstat", expected: "retry" },
  { name: "native-absent", expected: "terminal" },
  { name: "unparseable", expected: "retry" },
];

function readJson<T>(file: string): T | undefined {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch { return undefined; }
}

async function runCompiledProbe(name: string): Promise<{
  outputDir: string;
  timedOut: boolean;
  exitCode: number | null;
  result: Record<string, unknown> | undefined;
  progress: Record<string, unknown> | undefined;
}> {
  const outputDir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-bounded-lease-"));
  fs.chmodSync(outputDir, 0o700);
  const args = [probeFile, repoRoot, name, outputDir];
  return await new Promise(resolve => {
    const child = spawn("node", args, { cwd: repoRoot, env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"] });
    // Keep child diagnostics private; assertions below use only the aggregate JSON receipt.
    child.stdout.on("data", () => undefined);
    child.stderr.on("data", () => undefined);
    let timedOut = false;
    let settled = false;
    const finish = (exitCode: number | null): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ outputDir, timedOut, exitCode,
        result: readJson<Record<string, unknown>>(path.join(outputDir, "result.json")),
        progress: readJson<Record<string, unknown>>(path.join(outputDir, "progress.json")) });
    };
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, CHILD_DEADLINE_MS);
    child.once("error", () => finish(null));
    child.once("close", code => finish(code));
  });
}

function withPrivateProbe<T>(name: string, check: (probe: Awaited<ReturnType<typeof runCompiledProbe>>) => T): Promise<T> {
  return runCompiledProbe(name).then(probe => {
    try { return check(probe); }
    finally { fs.rmSync(probe.outputDir, { recursive: true, force: true }); }
  });
}

describe("compiled resident-wake terminal lease read bound", () => {
  for (const item of cases) {
    // Only the POSIX mkfifo case is platform-skipped; Windows has no mkfifo FIFO path.
    const register = item.posixFifo ? it.skipIf(process.platform === "win32") : it;
    const label = item.posixFifo ? `POSIX FIFO ${item.name}` : item.name;
    register(`${label} preserves its expected native disposition`, async () => {
      await withPrivateProbe(item.name, probe => {
        if (item.posixFifo) {
          expect(probe.progress?.thirdOpenProof).toMatchObject({ rawOpenAttempt: 3,
            insideNativeLeaseCustody: true, publicationFenceHeld: true });
        }
        expect(probe.timedOut).toBe(false);
        expect(probe.exitCode).toBe(0);
        expect(probe.result?.completed).toBe(true);
        expect(probe.result?.case).toBe(item.name);
        expect(probe.result?.expectedDisposition).toBe(item.expected);
        expect(probe.result?.actualDisposition).toBe(item.expected);
        expect(probe.result?.pass).toBe(true);
        expect(probe.result?.sqliteBackend).toBe("sqlite");
        expect(probe.result?.sqlitePutSucceeded).toBe(true);
        expect(probe.result?.sqliteDatabasePresent).toBe(true);
        expect(probe.result?.pendingBeforeRetry).toBe(true);
        expect(probe.result?.noWakeRequest).toBe(true);

        if (item.expected === "retry") {
          expect(probe.result?.pendingAfterRetry).toBe(true);
          expect(probe.result?.failureOwnsDelivery).toBe(true);
          expect(probe.result?.failureCount).toBeGreaterThan(0);
          expect(probe.result?.terminalReceipt).toBe(false);
          expect(probe.result?.exactAckCount).toBe(0);
        } else {
          expect(probe.result?.pendingAfterRetry).toBe(false);
          expect(probe.result?.terminalReceipt).toBe(true);
          expect(probe.result?.exactAckCount).toBe(1);
          expect(probe.result?.idleStable).toBe(true);
        }

        if (item.name === "oversized-unknown" || item.name === "over-cap") {
          const raw = probe.result?.rawDescriptorStats as Record<string, unknown> | undefined;
          expect(raw?.openAttempts).toBeLessThan(4);
          expect(raw?.readCalls).toBe(0);
          expect(raw?.actualBytes).toBe(0);
        }
        if (item.name === "exact-cap") {
          expect(probe.result?.leaseBytesAtProbeStart).toBe(65_536);
        }
        if (item.name === "fifo-third-open" || item.name === "directory-third-open") {
          expect(probe.result?.injectedFilesystemChange).toBe(true);
          expect(probe.result?.thirdOpenProof).toMatchObject({ rawOpenAttempt: 3,
            insideNativeLeaseCustody: true, publicationFenceHeld: true });
        }
        if (item.name === "truncate-after-fstat" || item.name === "grow-after-fstat") {
          expect(probe.result?.injectedFilesystemChange).toBe(true);
        }
      });
    }, VITEST_DEADLINE_MS);
  }

});
