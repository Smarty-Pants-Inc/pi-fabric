import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// E2E-13001-bun-child-guard: real inherited environment, Bun processes and fs boundaries.
// A missing guard never gets permission to delete even the disposable sentinel (safe RED).
const guardPath = fileURLToPath(new URL("./test-deletion-guard.mjs", import.meta.url));
const probe = `import fs from "node:fs";
import fsp from "node:fs/promises";
const guard = process[Symbol.for("pi-fabric.test-deletion-guard")];
console.log(JSON.stringify({bodyRan:true,guardInstalled:!!guard,root:guard?.root}));
if (!guard) process.exit(42);
// Safe RED: a report-only guard must never reach the real outside deletion below.
if (guard.report) throw new Error("outside refusal policy missing");
const refused=[];
for (const [name,remove] of [["rmSync",p=>fs.rmSync(p)],["promises.rm",p=>fsp.rm(p)]]) {
  try { await remove(process.env.GUARD_SENTINEL); throw new Error("outside deletion allowed"); }
  catch(error) { if(error.code!=="ERR_TEST_DELETION_GUARD") throw error; refused.push(name); }
  fs.writeFileSync(process.env.GUARD_INSIDE,"inside");
  await remove(process.env.GUARD_INSIDE);
  if(fs.existsSync(process.env.GUARD_INSIDE)) throw new Error("inside deletion failed");
}
console.log(JSON.stringify({refused,insideDeleted:true}));`;

function fixture(observeInstall = false) {
  // Sibling of the pinned root, still beneath the caller's private test area; never cleaned here.
  const outside = fs.mkdtempSync(join(dirname(tmpdir()), "bun-guard-outside-"));
  const sentinel = join(outside, "keep");
  fs.writeFileSync(sentinel, "keep");
  const env: NodeJS.ProcessEnv = {
    ...process.env, GITHUB_ACTIONS: "false", RUNNER_ENVIRONMENT: "", GUARD_SENTINEL: sentinel,
    GUARD_INSIDE: join(fs.mkdtempSync(join(tmpdir(), "bun-guard-inside-")), "remove"),
  };
  if (observeInstall) {
    const observer = join(tmpdir(), "bun-guard-observer.mjs");
    fs.writeFileSync(observer, `process.on("exit",()=>{
const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
console.log(JSON.stringify({guardInstalled:!!guard,root:guard?.root}));
});`);
    env.BUN_OPTIONS = `--preload=${observer} ${env.BUN_OPTIONS ?? ""}`;
  }
  return { sentinel, env };
}

function omittedArgsChild(api: string, omitted: "undefined" | "null", nested = false) {
  const { sentinel, env } = fixture();
  const root = fs.mkdtempSync(join(tmpdir(), "bun-omitted-args-"));
  const recorder = join(root, "recorder.mjs");
  fs.writeFileSync(recorder, `import fs from "node:fs";
export const calls=[];
fs.rmSync=path=>{if(path!==${JSON.stringify(sentinel)}) throw new Error("unexpected recorder path");calls.push(path);};`);
  // Prepend only a recorder import to an otherwise byte-for-byte copy of the real guard.
  // Its descendants preload the same copy, so even a missing/broken guard can only call
  // the recorder, never a native outside deleter. The body imports the cached recorder.
  const recordedGuard = join(root, "guard.mjs");
  fs.writeFileSync(recordedGuard, `import ${JSON.stringify(pathToFileURL(recorder).href)};\n${fs.readFileSync(guardPath, "utf8")}`);
  const bun = spawnSync("bun", ["-p", "process.execPath"], { encoding: "utf8", timeout: 10_000 });
  expect(bun.status, bun.stderr).toBe(0);
  const script = join(root, "child.mjs");
  fs.writeFileSync(script, `#!${bun.stdout.trim()}
import fs from "node:fs";
import {calls} from ${JSON.stringify(pathToFileURL(recorder).href)};
const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
let code=null;
try { fs.rmSync(${JSON.stringify(sentinel)}); } catch(error) { code=error.code; }
console.log(JSON.stringify({bodyRan:true,guardInstalled:!!guard,root:guard?.root,code,
  recorderCalls:calls.length,options:process.env.BUN_OPTIONS,exists:fs.existsSync(${JSON.stringify(sentinel)})}));`, { mode: 0o700 });
  const outer = `import cp from "node:child_process";
const api=${JSON.stringify(api)};
const options={env:${omitted === "null" ? '{BUN_OPTIONS:"--smol"}' : "{}"},encoding:"utf8",timeout:10000};
const argv=[${JSON.stringify(script)},${omitted},options];
let stdout,stderr,status;
if(api==="spawnSync") ({stdout,stderr,status}=cp.spawnSync(...argv));
else if(api==="execFileSync") {stdout=cp.execFileSync(...argv);stderr="";status=0;}
else if(api==="execFile") ({stdout,stderr,status}=await new Promise((resolve,reject)=>{
  cp.execFile(...argv,(error,stdout,stderr)=>error?reject(error):resolve({stdout,stderr,status:0}));
}));
else {
  const child=cp.spawn(...argv);stdout="";stderr="";
  child.stdout.on("data",x=>stdout+=x);child.stderr.on("data",x=>stderr+=x);
  status=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("close",resolve)});
}
process.stdout.write(stdout??"");process.stderr.write(stderr??"");process.exitCode=status??1;`;
  const preload = pathToFileURL(recordedGuard);
  preload.searchParams.set("root", tmpdir());
  // Native Node callers avoid Bun's additional Bun.spawn wrapper masking this overload.
  const launch = nested ? `import cp from "node:child_process";
const child=cp.spawnSync(process.execPath,["--input-type=module","-e",${JSON.stringify(outer)}],{
  env:process.env,encoding:"utf8",timeout:10000});
if(child.error) throw child.error;
process.stdout.write(child.stdout);process.stderr.write(child.stderr);process.exitCode=child.status??1;` : outer;
  const child = spawnSync(process.execPath, ["--input-type=module", "-e", launch], {
      env: { ...env, NODE_OPTIONS: `--import=${preload.href}`,
        BUN_OPTIONS: "", TEST_DELETION_GUARD_ROOT: tmpdir() },
      encoding: "utf8", timeout: 15_000,
    });
  // Assert safety before checking the expected RED/GREEN behavior.
  const sentinelValue = fs.readFileSync(sentinel, "utf8");
  console.log("OMITTED_ARGS_PROBE:", JSON.stringify({ api, omitted, nested, sentinel,
    sentinelValue, status: child.status, stdout: child.stdout, stderr: child.stderr }));
  expect(sentinelValue).toBe("keep");
  expect(child.error).toBeUndefined();
  expect(child.status, child.stderr).toBe(0);
  const result = JSON.parse(child.stdout);
  expect(result).toMatchObject({ bodyRan: true, guardInstalled: true, root: tmpdir(),
    code: "ERR_TEST_DELETION_GUARD", recorderCalls: 0, exists: true });
  expect(result.options).toContain(recordedGuard);
  if (omitted === "null") expect(result.options).toContain("--smol");
}

function expectGuarded(child: ReturnType<typeof spawnSync>, sentinel: string) {
  expect(child.error).toBeUndefined();
  expect(child.status, String(child.stderr)).toBe(0);
  expect(String(child.stdout)).toContain('"guardInstalled":true');
  expect(String(child.stdout)).toContain('"bodyRan":true');
  expect(String(child.stdout)).toContain('"refused":["rmSync","promises.rm"]');
  expect(String(child.stdout)).toContain('"insideDeleted":true');
  expect(String(child.stderr)).toContain('"refused":2');
  expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
}

describe("Bun children inherit the deletion guard", () => {
  it.each(["eval", "run", "file"])("refuses outside and allows inside default/promises deletion through %s", (form) => {
    const { sentinel, env } = fixture(true);
    const script = join(tmpdir(), `bun-guard-${form}.ts`);
    fs.writeFileSync(script, probe);
    const args = form === "eval" ? ["-e", probe] : form === "run" ? ["run", script] : [script];
    expectGuarded(spawnSync("bun", args, { env, encoding: "utf8", timeout: 10_000 }), sentinel);
  });

  it("refuses outside deletion in nested bun -e with the inherited preload", () => {
    const { sentinel, env } = fixture(true);
    const outer = `import {spawnSync} from "node:child_process";
const child=spawnSync(process.execPath,["-e",${JSON.stringify(probe)}],{
  env:process.env,encoding:"utf8",timeout:10000});
process.stdout.write(child.stdout);process.stderr.write(child.stderr);process.exit(child.status??1);`;
    expectGuarded(spawnSync("bun", ["-e", outer], { env, encoding: "utf8", timeout: 15_000 }), sentinel);
  });

  it("reports and delegates to recorders only on GitHub-hosted Bun", () => {
    const { sentinel, env } = fixture();
    const recorder = join(tmpdir(), "bun-guard-recorder.mjs");
    fs.writeFileSync(recorder, `import fs from "node:fs";
fs.rmSync = path => { if(path !== process.env.GUARD_SENTINEL) throw new Error("unexpected recorder path"); };
fs.promises.rm = async path => { if(path !== process.env.GUARD_SENTINEL) throw new Error("unexpected recorder path"); };`);
    env.BUN_OPTIONS = `--preload=${recorder} ${env.BUN_OPTIONS ?? ""}`;
    const code = `import fs from "node:fs";
import fsp from "node:fs/promises";
const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
if(!guard) throw new Error("guard missing");
fs.rmSync(process.env.GUARD_SENTINEL);
fs.writeFileSync(process.env.GUARD_SENTINEL,"keep");
await fsp.rm(process.env.GUARD_SENTINEL);
console.log(JSON.stringify({guardInstalled:true,report:guard.report,root:guard.root}));`;
    const child = spawnSync("bun", ["-e", code], {
      env: { ...env, GITHUB_ACTIONS: "true", RUNNER_ENVIRONMENT: "github-hosted" }, encoding: "utf8", timeout: 10_000,
    });
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toMatchObject({ guardInstalled: true, report: true });
    expect(child.stderr).toContain('"operation":"rmSync"');
    expect(child.stderr).toContain('"operation":"promises.rm"');
    expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
  });

  it.each(["self-hosted", ""])("refuses outside deletion with GITHUB_ACTIONS=true and runner=%s", (runner) => {
    const { sentinel, env } = fixture();
    env.GITHUB_ACTIONS = "true";
    if (runner) env.RUNNER_ENVIRONMENT = runner;
    else delete env.RUNNER_ENVIRONMENT;
    expectGuarded(spawnSync("bun", ["-e", probe], {
      env,
      encoding: "utf8", timeout: 10_000,
    }), sentinel);
  });

  it.each(["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync", "Bun.spawn", "Bun.spawnSync", "Bun.spawn(array)", "Bun.spawnSync(array)"].flatMap(api =>
    ["missing", "unrelated"].map(options => ({ api, options }))))(
    "pins the Bun child guard through $api with $options BUN_OPTIONS", ({ api, options }) => {
      const { sentinel, env } = fixture();
      const script = `${env.GUARD_INSIDE}.ts`;
      fs.writeFileSync(script, probe + '\nconsole.log(JSON.stringify({options:process.env.BUN_OPTIONS}));');
      const outer = `import cp from "node:child_process";
const env={...process.env};
delete env.BUN_OPTIONS;
delete env.TEST_DELETION_GUARD_ROOT;
${options === "unrelated" ? 'env.BUN_OPTIONS="--smol";' : ""}
const args=[${JSON.stringify(script)}];
const api=${JSON.stringify(api)};
let stdout,stderr,status;
if(api.startsWith("Bun.")) {
  const launch=api.includes("spawnSync")?Bun.spawnSync:Bun.spawn;
  const opts={env,stdout:"pipe",stderr:"pipe"};
  const child=api.includes("(array)")?launch([process.execPath,...args],opts):launch({cmd:[process.execPath,...args],...opts});
  if(api.includes("spawnSync")) {stdout=child.stdout.toString();stderr=child.stderr.toString();status=child.exitCode;}
  else { [stdout,stderr,status]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]); }
} else if(api==="spawn" || api==="spawnSync") {
  const child=cp[api](process.execPath,args,{env,encoding:"utf8"});
  if(api==="spawnSync") ({stdout,stderr,status}=child);
  else {stdout="";stderr="";child.stdout.on("data",x=>stdout+=x);child.stderr.on("data",x=>stderr+=x);status=await new Promise((resolve,reject)=>{child.on("error",reject);child.on("close",resolve)});}
} else {
  const command=JSON.stringify(process.execPath)+" "+args.map(x=>JSON.stringify(x)).join(" ");
  const argv=api.startsWith("execFile")?[process.execPath,args,{env,encoding:"utf8"}]:[command,{env,encoding:"utf8"}];
  if(api.endsWith("Sync")) {stdout=cp[api](...argv);stderr="";status=0;}
  else {({stdout,stderr,status}=await new Promise(resolve=>cp[api](...argv,(error,stdout,stderr)=>resolve({stdout,stderr,status:error?.code??0}))));}
}
process.stdout.write(stdout??"");process.stderr.write(stderr??"");process.exit(status??1);`;
      const child = spawnSync("bun", ["-e", outer], { env, encoding: "utf8", timeout: 15_000 });
      // execSync/execFileSync inherit stderr rather than returning it; all paths still prove refusal in stdout.
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(0);
      expect(child.stdout).toContain('"guardInstalled":true');
      expect(child.stdout).toContain('"refused":["rmSync","promises.rm"]');
      expect(child.stdout).toContain('"insideDeleted":true');
      if (options === "unrelated") expect(child.stdout).toContain("--smol");
      expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
    },
  );

  // The no-argument executable fixture uses a POSIX shebang to launch Bun directly.
  it.skipIf(process.platform === "win32").each(
    ["spawn", "spawnSync", "execFile", "execFileSync"].flatMap(api =>
      (["undefined", "null"] as const).map(omitted => ({ api, omitted }))),
  )("pins omitted-args $api with $omitted and an explicit env", ({ api, omitted }) => {
    omittedArgsChild(api, omitted);
  });

  it.skipIf(process.platform === "win32")("pins omitted-args Node child to explicit-env Bun grandchild", () => {
    omittedArgsChild("spawnSync", "undefined", true);
  });

  it("guards run-log cleanup in a worker-style Bun script child", () => {
    const { sentinel, env } = fixture();
    const script = join(tmpdir(), "bun-worker-cleanup.ts");
    const runLog = fileURLToPath(new URL("../src/worker/run-log.ts", import.meta.url));
    fs.writeFileSync(script, `import fs from "node:fs";
import {dirname,basename} from "node:path";
import {compactTerminalRunLog} from ${JSON.stringify(runLog)};
const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
if(!guard) process.exit(42);
const before=guard.count;
compactTerminalRunLog(process.env.GUARD_SENTINEL,"completed");
// No compaction is needed, so run-log.ts's finally block tries to unlink its owned temp.
const leftovers=fs.readdirSync(dirname(process.env.GUARD_SENTINEL)).filter(
  name=>name.startsWith(basename(process.env.GUARD_SENTINEL)+".")&&name.endsWith(".compact.tmp"));
console.log(JSON.stringify({guardInstalled:true,cleanupRefused:guard.count===before+1,leftovers}));`);
    const crossSpawn = fileURLToPath(new URL("../node_modules/cross-spawn/index.js", import.meta.url));
    const environment = fileURLToPath(new URL("../src/core/child-process-environment.ts", import.meta.url));
    // src/worker.ts:84-90: crossSpawn(process.execPath, [script, ...args]), with the real env helper.
    const outer = `import crossSpawn from ${JSON.stringify(crossSpawn)};
import {childProcessEnvironment} from ${JSON.stringify(environment)};
const child=crossSpawn(process.execPath,[${JSON.stringify(script)}],{
  env:childProcessEnvironment(process.env),stdio:"inherit"});
child.once("error",error=>{console.error(error);process.exitCode=1});
child.once("close",code=>{process.exitCode=code??1});`;
    const child = spawnSync("bun", ["-e", outer], { env, encoding: "utf8", timeout: 15_000 });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toMatchObject({ guardInstalled: true, cleanupRefused: true });
    const leftovers = JSON.parse(child.stdout).leftovers as string[];
    expect(leftovers).toHaveLength(1); // the refused outside unlink leaves the owned temp intact
    expect(fs.existsSync(join(dirname(sentinel), leftovers[0]!))).toBe(true);
    expect(child.stderr).toContain('"refused":1');
    expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
  });

  it("preserves existing Bun options and narrows the resident child ceiling", () => {
    const root = fs.mkdtempSync(join(tmpdir(), "bun-env-"));
    const home = fs.mkdtempSync(join(dirname(root), "bun-home-"));
    const code = `await import(${JSON.stringify(new URL("./test-deletion-guard.mjs", import.meta.url).href)});
console.log(JSON.stringify({options:process.env.BUN_OPTIONS,root:process.env.TEST_DELETION_GUARD_ROOT}));`;
    const child = spawnSync(process.execPath, ["--input-type=module", "-e", code], {
      env: { ...process.env, HOME: home, TMPDIR: root, TMP: root, TEMP: root, BUN_OPTIONS: "--smol" },
      encoding: "utf8", timeout: 10_000,
    });
    expect(child.status, child.stderr).toBe(0);
    const result = JSON.parse(child.stdout);
    expect(result.options).toContain("--smol");
    expect(result.options).toContain(guardPath);
    const temp = residentDrainTemp("bun-drain-env-");
    const narrowed = temp.childEnvironment(process.env.NODE_OPTIONS!);
    expect(narrowed.BUN_OPTIONS).toBe(process.env.BUN_OPTIONS);
    expect(narrowed.TEST_DELETION_GUARD_ROOT).toBe(temp.root);
    const bun = spawnSync("bun", ["-e",
      'console.log(JSON.stringify(process[Symbol.for("pi-fabric.test-deletion-guard")]))'], {
      env: { ...process.env, ...narrowed, TMPDIR: tmpdir(), GITHUB_ACTIONS: "true" },
      encoding: "utf8", timeout: 10_000,
    });
    expect(bun.status, bun.stderr).toBe(0);
    expect(JSON.parse(bun.stdout).root).toBe(temp.root); // changing TMPDIR must not widen the ceiling
    temp.assertNoOutsideDeletion();
  });
});
