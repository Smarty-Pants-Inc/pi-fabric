import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const probeFile = path.join(repoRoot, "tests", "fixtures", "resident-wake-bounded-lease-probe.mjs");
const CHILD_DEADLINE_MS = 10_000;
const VITEST_DEADLINE_MS = 15_000;

type ProbeCase = {
  name: string;
  expected: "retry" | "terminal";
  windowsSkipReason?: string;
};

const cases: readonly ProbeCase[] = [
  { name: "oversized-unknown", expected: "retry" },
  { name: "fifo-third-open", expected: "retry", windowsSkipReason: "mkfifo is POSIX-only." },
  { name: "directory-third-open", expected: "retry" },
  { name: "symlink-lease", expected: "retry", windowsSkipReason: "Windows symlink creation is privilege-dependent." },
  { name: "exact-cap", expected: "terminal" },
  { name: "over-cap", expected: "retry" },
  { name: "truncate-after-fstat", expected: "retry" },
  { name: "grow-after-fstat", expected: "retry" },
  { name: "native-absent", expected: "terminal" },
  { name: "unparseable", expected: "retry" },
];

type OptionalFsFlag = "O_NOFOLLOW" | "O_NONBLOCK";

async function evaluatePublicTerminalPredicate(options: {
  missingFlags?: readonly OptionalFsFlag[];
  withoutGetuid?: boolean;
  recordLeaseOpenFlags?: boolean;
} = {}): Promise<{ rootId: string; actual: string | undefined; leaseOpenFlags: number[] }> {
  const privateRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-terminal-flags-"));
  fs.chmodSync(privateRoot, 0o700);
  const meshRoot = path.join(privateRoot, "mesh");
  const rootId = `session:${randomUUID()}`;
  const root = path.join(meshRoot, "residency", createHash("sha256").update(rootId).digest("hex"));
  const sessionId = rootId.slice("session:".length);
  const hostId = `synthetic-host-${randomUUID()}`;
  const config = { format: 1, rootId, sessionId, cwd: privateRoot, meshRoot,
    residencyRoot: root, mesh: { transport: "file" } };
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1, rootId, hostId,
    configJson: canonicalResidentWakeConfig(config), actors: [] }), { mode: 0o600 });

  const mesh = new MeshStore(meshRoot, 65_536, 100, { stateBackend: "sqlite" });
  const leaseFile = hostLeasePath(meshRoot, rootId);
  const now = Date.now() + 6 * 60 * 60_000;
  const expiresAt = now - 60 * 60_000;
  const lease = { format: 1, id: rootId, rootId, identityId: rootId,
    updatedAt: expiresAt - 15_000, expiresAt };
  const leaseOpenFlags: number[] = [];
  const originalGetuid = Object.getOwnPropertyDescriptor(process, "getuid");

  try {
    await mesh.put({ key: "terminal-predicate/sqlite-proof",
      identity: { id: rootId, name: "synthetic-terminal-predicate", kind: "main" },
      value: { backend: "sqlite", proof: true } });
    writeHostLease(meshRoot, lease);
    vi.spyOn(Date, "now").mockReturnValue(now);
    if (options.withoutGetuid) Object.defineProperty(process, "getuid",
      { configurable: true, writable: true, value: undefined });

    const missing = new Set<PropertyKey>(options.missingFlags ?? []);
    // Empty targets let the proxy omit immutable native properties without violating proxy invariants.
    const constants = new Proxy({} as typeof fs.constants, {
      get(_target, key) {
        return missing.has(key) ? undefined : Reflect.get(fs.constants, key);
      },
      has(_target, key) { return missing.has(key) ? false : Reflect.has(fs.constants, key); },
    });
    const mockedFs = new Proxy({} as typeof fs, {
      get(_target, key) {
        if (key === "constants") return constants;
        if (key === "openSync" && options.recordLeaseOpenFlags) {
          return (file: fs.PathLike, flags: number | string, ...args: unknown[]) => {
            if (String(file) === leaseFile && typeof flags === "number") leaseOpenFlags.push(flags);
            return Reflect.apply(fs.openSync, fs, [file, flags, ...args]) as number;
          };
        }
        return Reflect.get(fs, key);
      },
    });
    vi.doMock("node:fs", () => ({ ...fs, default: mockedFs, constants }));
    vi.resetModules();
    const { residentWakeTerminalRoot } = await import("../src/residency/wake-terminal.js");
    return { rootId, actual: residentWakeTerminalRoot(mesh, root), leaseOpenFlags };
  } finally {
    vi.doUnmock("node:fs");
    vi.resetModules();
    vi.restoreAllMocks();
    if (originalGetuid) Object.defineProperty(process, "getuid", originalGetuid);
    else Reflect.deleteProperty(process, "getuid");
    try { mesh.closeState(); } catch { /* the private test workspace is removed below */ }
    fs.rmSync(privateRoot, { recursive: true, force: true });
  }
}

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
    // Only physical POSIX FIFO/symlink injections skip on Windows; predicate regressions still run there.
    const register = item.windowsSkipReason ? it.skipIf(process.platform === "win32") : it;
    const label = item.windowsSkipReason ? `${item.name} (Windows skip: ${item.windowsSkipReason})` : item.name;
    register(`${label} preserves its expected native disposition`, async () => {
      await withPrivateProbe(item.name, probe => {
        if (item.name === "fifo-third-open") {
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

describe("resident wake terminal regular-lease portability", () => {
  const windowsFlagRegressions: readonly { label: string; missingFlags: readonly OptionalFsFlag[] }[] = [
    { label: "O_NOFOLLOW undefined", missingFlags: ["O_NOFOLLOW"] },
    { label: "O_NONBLOCK undefined", missingFlags: ["O_NONBLOCK"] },
    { label: "both optional flags undefined", missingFlags: ["O_NOFOLLOW", "O_NONBLOCK"] },
  ];

  for (const regression of windowsFlagRegressions) {
    it(`accepts a valid regular lease with ${regression.label} (Windows compatibility)`, async () => {
      const result = await evaluatePublicTerminalPredicate({ missingFlags: regression.missingFlags,
        withoutGetuid: regression.missingFlags.length === 2 });
      expect(result.actual,
        "Data-safety / irreversible ACK: a valid native regular lease must remain eligible; absent optional Windows flags must not strand its delivery.")
        .toBe(result.rootId);
    }, VITEST_DEADLINE_MS);
  }

  it("accepts a valid regular lease when process.getuid is unavailable", async () => {
    const result = await evaluatePublicTerminalPredicate({ withoutGetuid: true });
    expect(result.actual,
      "Missing UID APIs must skip only the UID comparison, not the regular-file lease proof.")
      .toBe(result.rootId);
  }, VITEST_DEADLINE_MS);

  it("retains only the defined O_RDONLY/no-follow/nonblocking mask for lease opens", async () => {
    const result = await evaluatePublicTerminalPredicate({ recordLeaseOpenFlags: true });
    const { O_RDONLY, O_NOFOLLOW, O_NONBLOCK } = fs.constants;
    const sharedReaderMask = O_RDONLY | (O_NOFOLLOW ?? 0);
    const terminalMask = sharedReaderMask | (O_NONBLOCK ?? 0);
    expect(result.actual).toBe(result.rootId);
    expect(result.leaseOpenFlags).toContain(terminalMask);
    expect(result.leaseOpenFlags.every(flags => flags === sharedReaderMask || flags === terminalMask)).toBe(true);
  }, VITEST_DEADLINE_MS);
});
