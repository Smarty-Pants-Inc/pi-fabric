import childProcess from "node:child_process";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { namedChildProcessImports, namedChildProcessImportsInSource } from "./helpers/child-process-import-contract.js";

// Data-safety/trust-boundary regression: scoped Bun fixture inventory and real children,
// no mocks. The launcher probes only observe installation; they never delete.
const root = fileURLToPath(new URL("../", import.meta.url));
const guardPath = fileURLToPath(new URL("./test-deletion-guard.mjs", import.meta.url));
const contractUrl = new URL("./helpers/child-process-import-contract.ts", import.meta.url).href;

it("uses namespace/default child_process calls in the guarded Bun regression suite (smarty-dev#13001)", () => {
  const findings = namedChildProcessImports(root);
  expect(findings, `Guarded Bun fixtures must call through the child_process object.\n${findings.join("\n")}`).toEqual([]);
});

it.each(["node:child_process", "child_process"])("rejects a planted named import from %s in a real checker child", module => {
  const fixture = fs.mkdtempSync(join(tmpdir(), "child-process-import-contract-"));
  fs.writeFileSync(join(fixture, "allowed.ts"), `import cp from ${JSON.stringify(module)};
import * as processModule from ${JSON.stringify(module)};
import type { ChildProcess } from ${JSON.stringify(module)};
import { type SpawnOptions } from ${JSON.stringify(module)};
const fixture = 'import { spawn } from "node:child_process"';
// import { execFile } from 'child_process';
`);
  fs.writeFileSync(join(fixture, "planted.ts"), `import cp, {
  spawn as launch, execFile
} from ${JSON.stringify(module)};`);
  const code = `import {namedChildProcessImports} from ${JSON.stringify(contractUrl)};
const findings=namedChildProcessImports(${JSON.stringify(fixture)},["allowed.ts","planted.ts"]);
console.log(JSON.stringify(findings));process.exitCode=findings.length?1:0;`;
  const child = childProcess.spawnSync(process.execPath, ["--input-type=module", "-e", code], { encoding: "utf8", timeout: 10_000 });
  expect(child.error).toBeUndefined();
  expect(child.status, child.stderr).toBe(1);
  const findings: string[] = JSON.parse(child.stdout);
  expect(findings).toHaveLength(1);
  expect(findings[0]).toContain("planted.ts:1: spawn, execFile from " + module);
});

const apis = ["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync",
  "promisify.exec", "promisify.execFile", "Bun.spawn", "Bun.spawnSync", "Bun.spawn(array)", "Bun.spawnSync(array)"];

describe.skipIf(process.platform === "win32")("explicit-env launcher children keep the Bun guard", () => {
  it("refuses a sentinel deletion through a plain Node intermediary with a minimal explicit env", () => {
    const directory = fs.mkdtempSync(join(tmpdir(), "unknown-launcher-"));
    const privateRoot = join(directory, "private");
    fs.mkdirSync(privateRoot);
    const sentinel = join(directory, "keep");
    fs.writeFileSync(sentinel, "keep");
    const bun = childProcess.spawnSync("bun", ["-p", "process.execPath"], { encoding: "utf8", timeout: 10_000 });
    expect(bun.status, bun.stderr).toBe(0);
    const probe = `import fs from "node:fs";
const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
// Safe RED: a missing/report-only guard must not reach the native outside deleter.
if(!guard || guard.report) { console.log(JSON.stringify({installed:!!guard})); process.exit(42); }
let code=null;
try { fs.rmSync(${JSON.stringify(sentinel)}); } catch(error) { code=error.code; }
console.log(JSON.stringify({installed:true,root:guard.root,code,only:process.env.ONLY}));`;
    const relay = join(directory, "relay.mjs");
    fs.writeFileSync(relay, `import cp from "node:child_process";
const child=cp.spawnSync(${JSON.stringify(bun.stdout.trim())},["-e",${JSON.stringify(probe)}],{env:process.env,encoding:"utf8",timeout:10000});
if(child.error) throw child.error;
process.stdout.write(child.stdout);process.stderr.write(child.stderr);process.exitCode=child.status??1;`);
    const outer = `import cp from "node:child_process";
// No Bun token in this launch's executable or argv, and no inherited loader hooks.
const child=cp.spawnSync(process.execPath,[${JSON.stringify(relay)}],{
  env:{ONLY:"keep",TMPDIR:${JSON.stringify(directory)}},encoding:"utf8",timeout:15000});
if(child.error) throw child.error;
process.stdout.write(child.stdout);process.stderr.write(child.stderr);process.exitCode=child.status??1;`;
    expect(namedChildProcessImportsInSource("unknown-launcher-probe.mjs", probe)).toEqual([]);
    expect(namedChildProcessImportsInSource("unknown-launcher-outer.mjs", outer)).toEqual([]);
    const preload = new URL("./test-deletion-guard.mjs", import.meta.url);
    preload.searchParams.set("root", privateRoot);
    const child = childProcess.spawnSync(process.execPath, ["--import", preload.href, "--input-type=module", "-e", outer], {
      env: { ...process.env, NODE_OPTIONS: "", TMPDIR: privateRoot, TMP: privateRoot, TEMP: privateRoot,
        RUNNER_ENVIRONMENT: "", GITHUB_ACTIONS: "false" },
      encoding: "utf8", timeout: 20_000,
    });
    expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr + child.stdout).toBe(0);
    expect(JSON.parse(child.stdout)).toEqual({ installed: true, root: privateRoot,
      code: "ERR_TEST_DELETION_GUARD", only: "keep" });
  });
  it("pins a known launcher even when Bun is hidden behind a Node script", () => {
    const directory = fs.mkdtempSync(join(tmpdir(), "opaque-launcher-"));
    const script = join(directory, "relay.mjs");
    const bun = childProcess.spawnSync("bun", ["-p", "process.execPath"], { encoding: "utf8", timeout: 10_000 });
    expect(bun.status, bun.stderr).toBe(0);
    fs.writeFileSync(script, `import cp from "node:child_process";
const child=cp.spawnSync(${JSON.stringify(bun.stdout.trim())},["-e",'console.log(JSON.stringify({installed:!!process[Symbol.for("pi-fabric.test-deletion-guard")],root:process.env.TEST_DELETION_GUARD_ROOT,options:process.env.BUN_OPTIONS,x:process.env.BUN_X}))'],{env:process.env,encoding:"utf8"});
process.stdout.write(child.stdout);process.stderr.write(child.stderr);process.exitCode=child.status??1;`);
    // Neither executable nor argv contains a Bun token. /usr/bin/env must fail closed.
    const child = childProcess.spawnSync("/usr/bin/env", ["BUN_X=1", process.execPath, script], {
      env: { PATH: process.env.PATH, TMPDIR: tmpdir() }, encoding: "utf8", timeout: 15_000,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toMatchObject({ installed: true, root: tmpdir(), x: "1" });
    expect(JSON.parse(child.stdout).options).toContain(guardPath);
  });

  it.each(["bun", "bunx", "/unused/bun.exe", "/unused/bunx.exe"])("pins a direct Node child's env when an argv token is %s", token => {
    const code = 'console.log(JSON.stringify({root:process.env.TEST_DELETION_GUARD_ROOT,options:process.env.BUN_OPTIONS}))';
    const child = childProcess.spawnSync(process.execPath, ["-e", code, token], {
      env: { ONLY: "keep" }, encoding: "utf8", timeout: 10_000,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout).root).toBe(tmpdir());
    expect(JSON.parse(child.stdout).options).toContain(guardPath);
  });

  it.each(apis)("propagates through /usr/bin/env BUN_X=1 bun using %s", api => {
    const probe = `const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
console.log(JSON.stringify({installed:!!guard,root:guard?.root,envRoot:process.env.TEST_DELETION_GUARD_ROOT,
options:process.env.BUN_OPTIONS,x:process.env.BUN_X}));if(!guard)process.exitCode=42;`;
    const outer = `import cp from "node:child_process";
import {promisify} from "node:util";
const env={PATH:process.env.PATH,TMPDIR:process.env.TMPDIR,TMP:process.env.TMP,TEMP:process.env.TEMP};
const cmd=["/usr/bin/env","BUN_X=1",process.execPath,"-e",${JSON.stringify(probe)}];
const api=${JSON.stringify(api)};
let stdout,stderr,status;
if(api.startsWith("Bun.")) {
  const launch=api.includes("spawnSync")?Bun.spawnSync:Bun.spawn;
  const opts={env,stdout:"pipe",stderr:"pipe"};
  const child=api.includes("(array)")?launch(cmd,opts):launch({cmd,...opts});
  if(api.includes("spawnSync")){stdout=child.stdout.toString();stderr=child.stderr.toString();status=child.exitCode;}
  else [stdout,stderr,status]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);
} else if(api==="spawn" || api==="spawnSync") {
  const child=cp[api](cmd[0],cmd.slice(1),{env,encoding:"utf8"});
  if(api==="spawnSync")({stdout,stderr,status}=child);
  else {stdout="";stderr="";child.stdout.on("data",x=>stdout+=x);child.stderr.on("data",x=>stderr+=x);
    status=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("close",resolve);});}
} else {
  const method=api.replace("promisify.","");
  const command=cmd.map(x=>"'"+x.replaceAll("'", "'\\\"'\\\"'")+"'").join(" ");
  const argv=method.startsWith("execFile")?[cmd[0],cmd.slice(1),{env,encoding:"utf8"}]:[command,{env,encoding:"utf8"}];
  if(api.startsWith("promisify.")){({stdout,stderr}=await promisify(cp[method])(...argv));status=0;}
  else if(method.endsWith("Sync")){stdout=cp[method](...argv);stderr="";status=0;}
  else ({stdout,stderr,status}=await new Promise(resolve=>cp[method](...argv,(error,stdout,stderr)=>resolve({stdout,stderr,status:error?.code??0}))));
}
process.stdout.write(stdout??"");process.stderr.write(stderr??"");process.exitCode=status??1;`;
    expect(namedChildProcessImportsInSource("bun-launcher-fixture.mjs", outer)).toEqual([]);
    const child = childProcess.spawnSync("bun", ["-e", outer], { encoding: "utf8", timeout: 15_000 });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toMatchObject({ installed: true, root: tmpdir(), envRoot: tmpdir(), x: "1" });
    expect(JSON.parse(child.stdout).options).toContain(guardPath);
  });
});
