import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import ts from "typescript";
import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { AgentManager, managerRunTestHooks } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const root = path.resolve(import.meta.dirname, "..");
const source = (file: string) => fs.readFileSync(path.join(root, file), "utf8");
const ast = (file: string) => ts.createSourceFile(file, source(file), ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const normalizeCall = (text: string) => text.replace(/\s+/g, " ").replace(/\(\s+/g, "(").replace(/,\s*([})])/g, "$1").replace(/\s+([})])/g, "$1");
const position = (text: string, needle: string) => {
  const at = text.indexOf(needle);
  if (at < 0) throw new Error(`static inventory boundary not found: ${needle}`);
  return at;
};
const writerNames = new Set([
  "bindSession", "initializeTaskControlDirectory", "mkdirPrivate", "trackArchiveSource",
  "writeForkSession", "writeHandoffSession", "writeJsonAtomic", "writePrivateFileAtomic", "writeRunRecord",
]);
const pathnameWriter = (name: string | undefined) => Boolean(name && (/^(?:write|append|mkdir|rename|unlink|rm|copy(?:File|Sync)|truncate|open|createWrite|link|symlink|chmod|utimes|mkdtemp)/i.test(name) || writerNames.has(name)));
const callName = (node: ts.CallExpression): string | undefined => {
  const expression = node.expression;
  return ts.isIdentifier(expression) ? expression.text : ts.isPropertyAccessExpression(expression) ? expression.name.text : undefined;
};
const inventory = (file: string, start: string, end: string): string[] => {
  const text = source(file), from = position(text, start), to = position(text, end), found: string[] = [];
  const visit = (node: ts.Node): void => {
    if (ts.isCallExpression(node) && node.getStart() >= from && node.getEnd() <= to && pathnameWriter(callName(node))) {
      found.push(normalizeCall(node.getText(ast(file))));
    }
    ts.forEachChild(node, visit);
  };
  visit(ast(file));
  return found;
};

// Reviewed §4.7(a) pathname-write inventory. F0 (smarty-dev#12583) covers planted leaf symlinks;
// F1 (smarty-dev#12587) covers non-route pathname writes after the late identity check and in bootstrap.
// The worker's central route-session open is inventoried too, but is NOT an F1 run-directory write.
// Add a reviewed entry here when a new write is intentionally introduced; do not remove one to green CI.
const MANAGER_WRITES = [
  "mkdirPrivate(runDirectory, this.#trustRoot())",
  "writeJsonAtomic(path.join(runDirectory, ARCHIVE_PENDING_FILE), { format: 1, awaitingResult: true, ownerPid: process.pid, ...(process.env.PI_FABRIC_ACTOR_SESSION_FILE && this.#spawner?.kind === \"actor\" ? { actorSessionFile: process.env.PI_FABRIC_ACTOR_SESSION_FILE, spawner: this.#spawner, notify: this.config.notifyOnComplete, actorOnly: !routeDispatch } : {}) }, { durable: true })",
  "new ActorChildCompletionStore(process.env.PI_FABRIC_ACTOR_SESSION_FILE).trackArchiveSource(id, runDirectory)",
  "writeJsonAtomic(path.join(runDirectory, ACTOR_RUN_ARCHIVE_PENDING_FILE), { format: 1, runId: id, actorId: request.actorId, sessionFile: request.sessionFile }, { durable: true })",
  "writeJsonAtomic(path.join(runDirectory, \"completion-recipient.json\"), { meshRoot: this.#meshRoot, recipient: completionRecipient, supervisor: { pid: process.pid, processStartedAt: processStartTime(process.pid) } }, { durable: true })",
  "writePrivateFileAtomic(taskFile, Buffer.from(request.task, \"utf8\"))",
  "writePrivateFileAtomic(taskFile + \".provenance.json\", Buffer.from(JSON.stringify(request.provenance), \"utf8\"))",
  "fs.writeFileSync(imagesFile, JSON.stringify(request.images), { encoding: \"utf8\", mode: 0o600 })",
  "fs.writeFileSync(schemaFile, JSON.stringify(request.schema, null, 2), { encoding: \"utf8\", mode: 0o600 })",
  "mkdirPrivate(tmpdir, this.#trustRoot())",
  "initializeTaskControlDirectory(runDirectory)",
  "fs.appendFileSync(logFile, JSON.stringify({ type, ts: Date.now(), id, runner, ...(policy ? { policy } : {}), ...(warning ? { warning } : {}) }) + \"\\n\", { mode: 0o600 })",
  "routeDispatch?.bindSession(agentCwd)",
  "writeForkSession(request.forkSeed, agentCwd, path.join(runDirectory, \"fork-session\"), request.thinkingTransfer)",
  "writeHandoffSession(request.sessionSeed, agentCwd, path.join(runDirectory, \"handoff-session\"), request.thinkingTransfer, request.handoffCompact, request.handoffCompact ? await this.#resolveHandoffCompactionBudget?.(model, agentCwd) : undefined)",
];
const WORKER_WRITES = [
  "fs.openSync(path.join(directory, \"route-session.jsonl\"), fs.constants.O_RDWR | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW ?? 0))",
  "initializeTaskControlDirectory(ownRunDirectory)",
  "mkdirPrivate(deliveryDirectory, runRoot)",
  "writeRunRecord(options.statusFile, record)",
];

describe("DELTA-SEC-02 residual write inventory and probes", () => {
  it("inventories every pathname writer in #spawn after the late check and worker bootstrap (F0/F1)", () => {
    expect(inventory("src/agents/manager.ts", "assertRunPathSafe(runDirectory, this.#inheritedRunRoot);", "await this.#launchTransport(adapter, launch").sort()).toEqual(MANAGER_WRITES.map(normalizeCall).sort());
    expect(inventory("src/worker.ts", "const options = optionHelpers.parseWorkerOptions();", "const emitLifecycle =")).toEqual(WORKER_WRITES.map(normalizeCall));
  });
});

const enabled = process.env.FABRIC_ROUTE_RESIDUAL_REQUIRED === "1";
const linux = process.platform === "linux";
const isolatedState = process.env.XDG_STATE_HOME;
const tempRoots = [os.tmpdir(), process.env.TMPDIR].filter((value): value is string => Boolean(value)).map((value) => path.resolve(value));
const inTemp = (value: string) => tempRoots.some((temp) => value === temp || value.startsWith(temp + path.sep));
const usable = enabled && linux && Boolean(isolatedState && path.isAbsolute(isolatedState) && !inTemp(path.resolve(isolatedState)));
const HOOK = managerRunTestHooks as typeof managerRunTestHooks & { afterLateRunCheck?: (runDirectory: string) => void | Promise<void> };
const managers: AgentManager[] = [];
const agents = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] };
const LANDLOCK = path.resolve("dist/native/fabric-landlock");
const SCRIPT = `const fs=require('node:fs'),path=require('node:path'); const [mode,run,victim]=process.argv.slice(2);
if(mode==='swap'){fs.renameSync(run,run+'.moved');fs.symlinkSync(victim,run,'dir');}
else fs.symlinkSync(path.join(victim,mode==='schema'?'schema.json':'images.json'),path.join(run,mode==='schema'?'schema.json':'images.json'));`;
const tempFixture = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "route-residual-")));
  const granted = path.join(base, "granted"), victim = path.join(base, "victim");
  fs.mkdirSync(granted, { mode: 0o700 }); fs.mkdirSync(victim, { mode: 0o700 });
  for (const name of ["schema.json", "images.json", "task.txt"]) fs.writeFileSync(path.join(victim, name), `victim:${name}`, { mode: 0o600 });
  const script = path.join(base, "plant.cjs"); fs.writeFileSync(script, SCRIPT, { mode: 0o600 });
  return { base, runRoot: path.join(granted, "nested"), granted, victim, script };
};
const plant = (fixture: ReturnType<typeof tempFixture>, mode: "swap" | "schema" | "images", run: string) => {
  const stat = fs.statSync(fixture.granted, { bigint: true }), quote = JSON.stringify;
  const result = spawnSync(LANDLOCK, ["-c", `exec ${quote(process.execPath)} ${quote(fixture.script)} ${quote(mode)} ${quote(run)} ${quote(fixture.victim)}`], {
    cwd: fixture.granted, encoding: "utf8", timeout: 5000,
    env: { ...process.env, LC_ALL: "C", PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: `${stat.dev}:${stat.ino}:${fixture.granted}` },
  });
  if (result.error || result.status !== 0) throw new Error(`Landlock residual setup failed: ${result.stderr || result.error}`);
};
const setupAndRun = async (mode: "swap" | "schema" | "images") => {
  expect(fs.existsSync(LANDLOCK), "hosted residual rows require the built Landlock helper").toBe(true);
  const fixture = tempFixture();
  const priorRoot = process.env.PI_FABRIC_RUN_ROOT;
  process.env.PI_FABRIC_RUN_ROOT = fixture.runRoot;
  const manager = new AgentManager(process.cwd(), agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  managers.push(manager);
  let reached = false, run = "";
  HOOK.afterLateRunCheck = (directory) => { run = directory; reached = true; plant(fixture, mode, directory); };
  const request = mode === "schema" ? { schema: { attacker: true } } : mode === "images" ? { images: ["parent-chosen"] } : {};
  try {
    await manager.run({ task: `${mode} residual`, transport: "process", ...request } as never);
  } finally {
    expect(reached, "afterLateRunCheck must be reached before any residual assertion").toBe(true);
    delete HOOK.afterLateRunCheck;
    if (priorRoot === undefined) delete process.env.PI_FABRIC_RUN_ROOT; else process.env.PI_FABRIC_RUN_ROOT = priorRoot;
  }
  return { fixture, run };
};

describe.skipIf(!enabled)("Linux Landlock-confined F0/F1 residuals (hosted rows)", () => {
  afterEach(() => { delete HOOK.afterLateRunCheck; });
  afterAll(async () => { await Promise.all(managers.splice(0).map((manager) => manager.close())); });

  describe("F1 late C swap", () => {
    let setup: Awaited<ReturnType<typeof setupAndRun>>;
    let victimTaskBefore = "";
    beforeEach(async () => {
      setup = await setupAndRun("swap");
      victimTaskBefore = "victim:task.txt";
    });
    if (linux) {
      it.fails("task.txt must not land in victim or moved C (smarty-dev#12587)", () => {
        expect(fs.readFileSync(path.join(setup.fixture.victim, "task.txt"), "utf8")).toBe(victimTaskBefore);
        expect(fs.existsSync(path.join(`${setup.run}.moved`, "task.txt"))).toBe(false);
      });
    } else it.skip("expected skip: F1 late C swap requires enforced Linux Landlock", () => {});
  });

  for (const leaf of ["schema", "images"] as const) {
    describe(`F0 planted ${leaf}.json`, () => {
      let setup: Awaited<ReturnType<typeof setupAndRun>>;
      let before = "";
      beforeEach(async () => {
        setup = await setupAndRun(leaf);
        before = `victim:${leaf}.json`;
      });
      if (linux) {
        it.fails("the victim file must remain unchanged (smarty-dev#12583)", () => {
          expect(fs.readFileSync(path.join(setup.fixture.victim, `${leaf}.json`), "utf8")).toBe(before);
        });
      } else it.skip(`expected skip: F0 ${leaf}.json planting requires enforced Linux Landlock`, () => {});
    });
  }
});
