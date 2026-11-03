// Expected skip on non-win32: only native Windows can prove PowerShell 5.1/DACL custody.
// Keep reason: data safety and the Windows custody trust boundary. A successful
// close must not silently leave every completed run uncollected after a batch
// protection failure, or claim collection without preserving its original tree.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { runTreeExitVeto } from "../src/storage/retention.js";
import { processAlive } from "../src/storage/scratch.js";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell-command.js";

const RUN_COUNT = 3; // Multiple paths are essential: a singleton avoids the stdin batch branch.
type InventoryEntry = {
  relative: string; dev: string; ino: string; mode: string; type: string; bytes?: string; link?: string;
};
// Independent native identities and original bytes, not the custody helper's
// inventory or a write-buffer hash. Rename may change ctime; do not compare it.
const inventory = (root: string): InventoryEntry[] => {
  const rows: InventoryEntry[] = [];
  const walk = (relative: string): void => {
    const file = path.join(root, relative), stat = fs.lstatSync(file, { bigint: true });
    rows.push({ relative, dev: String(stat.dev), ino: String(stat.ino), mode: String(stat.mode),
      type: stat.isSymbolicLink() ? "symlink" : stat.isDirectory() ? "directory" : stat.isFile() ? "file" : "special",
      ...(stat.isFile() ? { bytes: fs.readFileSync(file).toString("base64") } : {}),
      ...(stat.isSymbolicLink() ? { link: fs.readlinkSync(file) } : {}) });
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name));
  };
  walk(""); return rows;
};
type NativeAcl = {
  executable: string; major: number; minor: number; edition: string; directory: string;
  user: string; owner: string; protected: boolean; isDirectory: boolean; reparse: boolean;
  aces: Array<{ sid: string; type: number; flags: number; mask: number }>;
};
type RunProof = {
  id: string; task: string; source: string; before: InventoryEntry[];
  destination?: string; receipt?: string; prepared?: unknown; moved?: unknown; acl?: NativeAcl;
};
type Fixture = {
  root: string; runRoot: string; archive: string; manager?: AgentManager; runs: RunProof[];
  callbacks: { returned: string[]; consumed: string[]; stopped: string[]; settled: string[] };
  closeOutcomes: Array<{ phase: string; status: string; reason?: string }>;
};
const fixtures: Fixture[] = [];

// Read ONLY. In particular this must never Set-Acl and repair a bad product DACL.
// No child_process, filesystem, platform or custody module is mocked in this file.
const readNativeAcl = (container: string, rawPrefix: string): NativeAcl => {
  const command = windowsSecurityPowerShell(String.raw`
$directory = $env:PI_TEST_CUSTODY_CONTAINER
$item = Get-Item -Force -LiteralPath $directory
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$acl = Get-Acl -LiteralPath $directory
$raw = [System.Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
if ($null -eq $raw.DiscretionaryAcl) { throw 'native custody readback found a null DACL' }
$aces = @(foreach ($ace in $raw.DiscretionaryAcl) { @{ sid=$ace.SecurityIdentifier.Value; type=[int]$ace.AceType; flags=[int]$ace.AceFlags; mask=$ace.AccessMask } })
$json = @{ executable=[System.Diagnostics.Process]::GetCurrentProcess().MainModule.FileName;
  major=$PSVersionTable.PSVersion.Major; minor=$PSVersionTable.PSVersion.Minor; edition=$PSVersionTable.PSEdition;
  directory=$directory; user=$user; owner=$raw.Owner.Value; protected=$acl.AreAccessRulesProtected;
  isDirectory=[bool]$item.PSIsContainer; reparse=[bool](($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0);
  aces=$aces } | ConvertTo-Json -Depth 6 -Compress
[Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($json))
`, { ...process.env, PI_TEST_CUSTODY_CONTAINER: container });
  fs.writeFileSync(`${rawPrefix}.command.json`, JSON.stringify({ file: command.file, args: command.args }, null, 2));
  let output: Buffer;
  try {
    output = execFileSync(command.file, command.args, {
      env: command.env, timeout: 15_000, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    const failure = error as { stdout?: Buffer; stderr?: Buffer };
    if (failure.stdout) fs.writeFileSync(`${rawPrefix}.stdout`, failure.stdout);
    if (failure.stderr) fs.writeFileSync(`${rawPrefix}.stderr`, failure.stderr);
    throw error;
  }
  fs.writeFileSync(`${rawPrefix}.stdout`, output);
  // ASCII base64 preserves Unicode paths through Windows PowerShell's stdout code page.
  const json = Buffer.from(output.toString("ascii").trim(), "base64").toString("utf8");
  fs.writeFileSync(`${rawPrefix}.json`, json);
  const data = JSON.parse(json) as NativeAcl;
  expect(path.win32.isAbsolute(command.file)).toBe(true);
  expect(path.win32.basename(command.file).toLowerCase()).toBe("powershell.exe");
  expect(fs.realpathSync.native(data.executable).toLowerCase()).toBe(fs.realpathSync.native(command.file).toLowerCase());
  expect(data).toMatchObject({ major: 5, minor: 1, edition: "Desktop", directory: container });
  return data;
};

afterEach(async context => {
  const owned = fixtures.splice(0);
  try {
    for (const fixture of owned) {
      if (!fixture.manager) continue;
      try {
        await fixture.manager.close();
        fixture.closeOutcomes.push({ phase: "afterEach", status: "fulfilled" });
      } catch (error) {
        fixture.closeOutcomes.push({ phase: "afterEach", status: "rejected", reason: String(error) });
      }
    }
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  }
  // Export only after close, on failure too. Originals (including empty failed
  // containers) remain. Each whole fixture includes its external archive sibling.
  const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
  if (!evidence || !owned.length) return;
  fs.mkdirSync(evidence, { recursive: true, mode: 0o700 });
  const out = fs.mkdtempSync(path.join(evidence, "manager-close-windows-native-"));
  const exported = owned.map(({ manager: _manager, ...fixture }, index) => {
    const raw = `fixture-${index}`;
    const after = inventory(fixture.root);
    fs.cpSync(fixture.root, path.join(out, raw), { recursive: true, dereference: false });
    return { ...fixture, raw, after };
  });
  fs.writeFileSync(path.join(out, "fixtures.json"), JSON.stringify({
    test: context.task.name, platform: process.platform, expectedRuns: RUN_COUNT, fixtures: exported,
  }, null, 2));
});

describe("AgentManager close native Windows custody", () => {
  it.runIf(process.platform === "win32")("archives and protects EVERY completed unconsumed run with real PowerShell 5.1", async () => {
    // Unlike agent-manager-close.test.ts, there is NO module-level child_process
    // recorder. The product's pinned powershell.exe actually sets/reads each DACL.
    expect(vi.isMockFunction(execFileSync)).toBe(false);
    expect(vi.isMockFunction(windowsSecurityPowerShell)).toBe(false);
    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "manager-close-win-native-")));
    const runRoot = path.join(root, "runs 'native-雪'");
    const fixture: Fixture = { root, runRoot, archive: `${runRoot}.trace-archive`, runs: [],
      callbacks: { returned: [], consumed: [], stopped: [], settled: [] }, closeOutcomes: [] };
    fixtures.push(fixture); // Retain even a constructor/preparation failure's raw fixture.
    console.info(`retained native Windows close fixture (including external archives): ${root}`);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
    const { callbacks } = fixture;
    // An explicit fixture-owned runRoot exercises public close collection without
    // scheduling the unrelated managed-temp-root detached sweep. No gate changes.
    const manager = fixture.manager = new AgentManager(root, {
      ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns: false, transport: "process", sessionExport: false,
    }, {
      runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      onBeforeResultReturned: id => { callbacks.returned.push(id); },
      onResultConsumed: id => { callbacks.consumed.push(id); },
      onStoppedAtClose: results => { callbacks.stopped.push(...results.map(result => result.id)); },
      onSettled: result => { callbacks.settled.push(result.id); },
    });
    const absentPid = "2147483647";
    expect(processAlive(Number(absentPid)), "the synthetic settled worker identity must really be absent").toBe(false);
    // Only worker launch/settlement is substituted; custody, receipts, renames,
    // result authentication and both product/readback PowerShell remain native.
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const argument = (flag: string): string => {
        const index = request.workerArguments.indexOf(flag);
        expect(index, flag).toBeGreaterThanOrEqual(0);
        return request.workerArguments[index + 1]!;
      };
      fs.writeFileSync(argument("--status-file"), JSON.stringify({
        id: request.id, name: request.name, task: fs.readFileSync(argument("--task-file"), "utf8"),
        status: "completed", runner: "pi", transport: "process", sessionId: absentPid, cwd: request.cwd,
        startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(),
        text: `unread native result ${request.id}`, value: { id: request.id, original: "雪" },
        turns: 1, toolCalls: 0, exitCode: 0, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
      }));
      return { kind: "process", sessionId: absentPid, isAlive: async () => false, stop: async () => {} };
    });
    const unconsumed = () => {
      expect(callbacks.returned).toEqual([]);
      expect(callbacks.consumed).toEqual([]);
      expect(callbacks.stopped).toEqual([]);
    };
    for (let index = 0; index < RUN_COUNT; index++) {
      const task = `native close ${index}\noriginal prompt 雪`;
      const handle = await manager.spawn({ task, runner: "pi", transport: "process", extensions: false });
      await manager.join(handle.id); // Not run()/wait(): keep ALL full results unconsumed at close.
      const source = manager.runDirectory(handle.id)!;
      fs.mkdirSync(path.join(source, "raw"));
      fs.writeFileSync(path.join(source, "raw", "capture.JSONL"), Buffer.from([index, 0, 255, 13, 10, 195, 169]));
      expect(runTreeExitVeto(source, 0, undefined, true)).toBeUndefined();
      expect(manager.status(handle.id)).toMatchObject({ status: "completed", task,
        text: `unread native result ${handle.id}`, value: { id: handle.id, original: "雪" } });
      expect(JSON.parse(fs.readFileSync(path.join(source, "manager-result.json"), "utf8"))).toMatchObject({ id: handle.id, status: "completed", task });
      const before = inventory(source);
      for (const entry of before) expect(BigInt(entry.ino), `native file identity: ${entry.relative}`).toBeGreaterThan(0n);
      fixture.runs.push({ id: handle.id, task, source, before });
    }
    expect(fixture.runs).toHaveLength(RUN_COUNT);
    expect(new Set(fixture.runs.map(run => `${run.before[0]!.dev}:${run.before[0]!.ino}`)).size).toBe(RUN_COUNT);
    expect(callbacks.settled.slice().sort()).toEqual(fixture.runs.map(run => run.id).sort());
    unconsumed();
    try {
      await manager.close();
      fixture.closeOutcomes.push({ phase: "test", status: "fulfilled" });
    } catch (error) {
      fixture.closeOutcomes.push({ phase: "test", status: "rejected", reason: String(error) });
      throw error;
    }

    const containers = fs.readdirSync(fixture.archive).map(name => path.join(fixture.archive, name));
    expect(containers, "one real protected container for EVERY run, not merely a successful close promise").toHaveLength(RUN_COUNT);
    const receipts = containers.map(container => {
      expect(fs.readdirSync(container).sort()).toEqual(["custody.json", "custody.prepared.json", "tree"]);
      const receipt = path.join(container, "custody.json");
      return { container, receipt, moved: JSON.parse(fs.readFileSync(receipt, "utf8")) };
    });
    expect(receipts.map(row => row.moved.source).sort()).toEqual(fixture.runs.map(run => run.source).sort());
    expect(new Set(receipts.map(row => row.moved.destination)).size).toBe(RUN_COUNT);
    for (const run of fixture.runs) {
      const matches = receipts.filter(row => row.moved.source === run.source);
      expect(matches, `exactly one custody receipt for ${run.id}`).toHaveLength(1);
      const { container, receipt, moved } = matches[0]!;
      const destination = path.join(container, "tree");
      const prepared = JSON.parse(fs.readFileSync(path.join(container, "custody.prepared.json"), "utf8"));
      Object.assign(run, { destination, receipt, prepared, moved });
      expect(prepared).toMatchObject({ format: 1, id: run.id, source: run.source, destination,
        state: "prepared", reason: "agent cleanup run directory", time: expect.any(Number) });
      expect(moved).toEqual({ ...prepared, state: "moved", time: expect.any(Number) });
      expect(moved.time).toBeGreaterThanOrEqual(prepared.time);
      expect(fs.lstatSync(run.source, { throwIfNoEntry: false }), "original namespace is absent after whole-object rename").toBeUndefined();
      expect(inventory(destination), `EVERY original byte/name/type/dev/ino survives: ${run.id}`).toEqual(run.before);
      expect(manager.status(run.id)).toMatchObject({ id: run.id, status: "completed", task: "", text: "",
        warnings: expect.arrayContaining([expect.stringContaining("collected at close")]) });
      expect(manager.status(run.id)).not.toHaveProperty("value");

      const acl = run.acl = readNativeAcl(container, path.join(root, `acl-${run.id}`));
      expect(acl.isDirectory).toBe(true); expect(acl.reparse).toBe(false);
      expect(acl.user).toMatch(/^S-1-/);
      expect(acl.owner).toBe(acl.user); expect(acl.protected).toBe(true);
      // Exact SID multiset prevents duplicate-user ACEs from hiding missing SYSTEM.
      expect(acl.aces).toHaveLength(2);
      expect(acl.aces.map(ace => ace.sid).sort()).toEqual([acl.user, "S-1-5-18"].sort());
      for (const ace of acl.aces) {
        expect(ace.type).toBe(0); // ACCESS_ALLOWED_ACE_TYPE only, no denies/extra ACEs.
        expect(ace.flags).toBe(3); // ObjectInherit | ContainerInherit, not inherited/inherit-only.
        expect(ace.mask).toBe(0x1f01ff); // Exact FullControl, not a permissive subset check.
      }
    }
    expect(callbacks.settled.slice().sort()).toEqual(fixture.runs.map(run => run.id).sort());
    unconsumed();
  }, 60_000);
});
