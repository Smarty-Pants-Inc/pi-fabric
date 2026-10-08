import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { MeshStateUnsupportedError, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";

const identity: MeshIdentity = { id: "mesh-soak", name: "mesh-soak", kind: "agent" };
let root: string | undefined;
afterEach(() => { if (root) fs.rmSync(root, { recursive: true, force: true }); root = undefined; });
const CHILD = `
import { createJiti } from "jiti"; import { pathToFileURL } from "node:url";
const jiti=createJiti(pathToFileURL(process.cwd()+"/index.js").href); const [mod,sql]=await Promise.all([jiti.import("./src/mesh/store.ts"),jiti.import("./src/mesh/state-sqlite.ts")]);
const [root,worker,duration]=process.argv.slice(1); const identity={id:"soak-"+worker,name:"soak",kind:"agent"};
const state=await sql.SqliteStateStore.open(root,65536,100,{lockTimeoutMs:10000,busyTimeoutMs:5}); const events=new mod.MeshStore(root,65536,100,{lockTimeoutMs:10000});
const counters={operations:0,lockTimeouts:0,sqliteBusy:0,otherErrors:[]}; const deadline=Date.now()+Number(duration);
while(Date.now()<deadline){ const n=counters.operations; const tasks=[
 state.put({key:"soak/put/"+worker,value:n,identity}),
 state.writeBatch({identity,ops:[{kind:"put",key:"soak/batch/"+worker,value:{n,at:Date.now()}},{kind:"put",key:"soak/heartbeat/"+worker,value:Date.now()}]}),
 events.publish({topic:"mesh.backend.soak",from:identity,data:{worker,n}})
 ]; const results=await Promise.allSettled(tasks); for(const result of results){if(result.status==="fulfilled")continue;const e=result.reason;const text=String(e?.message??e);if(e?.code==="FABRIC_MESH_LOCK_TIMEOUT"||text.includes("FABRIC_MESH_LOCK_TIMEOUT"))counters.lockTimeouts++;else if(e?.busyCode==="FABRIC_MESH_STATE_BUSY"||e?.code==="SQLITE_BUSY"||text.includes("SQLITE_BUSY"))counters.sqliteBusy++;else counters.otherErrors.push(text.slice(0,180));} counters.operations++; }
console.log(JSON.stringify(counters)); state.close();
`;
function start(kind: "soak" | "holder", rootPath: string, arg: string): Promise<{ code: number | null; signal: NodeJS.Signals | null; output: string }> {
  return new Promise((resolve, reject) => {
    const code = kind === "holder" ? `import { createJiti } from "jiti"; import { pathToFileURL } from "node:url"; import fs from "node:fs"; const j=createJiti(pathToFileURL(process.cwd()+"/index.js").href); const {MeshStore}=await j.import("./src/mesh/store.ts"); const s=new MeshStore(${JSON.stringify(rootPath)},65536,100); await s.exclusive(()=>{fs.writeFileSync(${JSON.stringify(arg)},"held");const until=Date.now()+1500;while(Date.now()<until){};});` : CHILD;
    const args = kind === "holder" ? ["--input-type=module", "-e", code] : ["--input-type=module", "-e", code, rootPath, arg, "60000"];
    const child = spawn(process.execPath, args, { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] }); let output = "";
    child.stdout.on("data", chunk => { output += chunk; }); child.stderr.on("data", chunk => { output += chunk; }); child.once("error", reject);
    const timer = setTimeout(() => child.kill("SIGKILL"), kind === "holder" ? 10_000 : 70_000);
    child.once("close", (exit, signal) => { clearTimeout(timer); resolve({ code: exit, signal, output }); });
  });
}

describe("mesh backend mixed-load soak", () => {
  it("runs 60s across eight SQLite writers without busy/lock failures or state .lock custody", async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-backend-soak-"));
    let probe: SqliteStateStore;
    try { probe = await SqliteStateStore.open(root, 64 * 1024, 100, { lockTimeoutMs: 2_000 }); }
    catch (error) { if (error instanceof MeshStateUnsupportedError) return; throw error; }
    // Hold the legacy mesh .lock in another process: SQLite state writes must still complete while
    // event publication remains lock-coupled. This proves keyed-state writes never enter .lock.
    const held = path.join(root, "lock-held"); const holder = start("holder", root, held);
    const until = Date.now() + 5_000; while (!fs.existsSync(held) && Date.now() < until) await new Promise(resolve => setTimeout(resolve, 10));
    expect(fs.existsSync(held)).toBe(true);
    const startAt = Date.now(); await probe.put({ key: "soak/probe", value: 1, identity });
    expect(Date.now() - startAt).toBeLessThan(1_000); probe.close();
    const workers = await Promise.all(Array.from({ length: 8 }, (_, index) => start("soak", root!, String(index))));
    const lockResult = await holder;
    expect(lockResult.code, lockResult.output).toBe(0);
    for (const worker of workers) {
      expect(worker.signal, worker.output).toBeNull(); expect(worker.code, worker.output).toBe(0);
      const line = worker.output.trim().split("\n").reverse().find((value: string) => value.startsWith("{"));
      expect(line, worker.output).toBeTruthy();
      const counts = JSON.parse(line!) as { operations: number; lockTimeouts: number; sqliteBusy: number; otherErrors: string[] };
      expect(counts.operations).toBeGreaterThan(0); expect(counts.lockTimeouts, worker.output).toBe(0); expect(counts.sqliteBusy, worker.output).toBe(0); expect(counts.otherErrors, worker.output).toEqual([]);
    }
  }, 85_000);
});
