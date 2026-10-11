import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";

// Deterministic choices make every crash point reproducible while varying the phase across rounds.
const seeded=(seed:number)=>()=>{ seed=(seed*1664525+1013904223)>>>0; return seed/0x1_0000_0000; };
const roots:string[]=[];
const rootFor=()=>{const root=fs.mkdtempSync(path.join(os.tmpdir(),"mesh powercut "));roots.push(root);return root;};
afterEach(()=>{for(const root of roots.splice(0))fs.rmSync(root,{recursive:true,force:true});});
const waitFor=async(file:string)=>{const end=Date.now()+7000;while(!fs.existsSync(file)){if(Date.now()>end)throw new Error("timed out waiting for power-cut checkpoint");await new Promise(r=>setTimeout(r,10));}};
const worker=`
import { createJiti } from "jiti"; import {pathToFileURL} from "node:url"; import fs from "node:fs";
const jiti=createJiti(pathToFileURL(process.cwd()+"/index.js").href);
const {SqliteStateStore,openNodeSqlite}=await jiti.import("./src/mesh/state-sqlite.ts");
const [root,phase,ready,ack]=process.argv.slice(1);
const open=file=>{const db=openNodeSqlite(file);return {exec(sql){db.exec(sql);if(sql.includes("synchronous = NORMAL"))db.exec("PRAGMA synchronous = FULL");},prepare(sql){return db.prepare(sql);},close(){db.close();},get isTransaction(){return db.isTransaction;}}};
const store=await SqliteStateStore.open(root,65536,1000,{open,lockTimeoutMs:3000,initialize:"create"});
const identity={id:"powercut",name:"powercut",kind:"agent"};
const spin=()=>{for(;;){}};
const stopAfter=Number(process.argv[5]);
if(phase==="between") { for(let i=0;i<stopAfter;i++){await store.put({key:"commit/"+i,value:i,identity});const fd=fs.openSync(ack,"a");fs.writeSync(fd,i+"\\n");fs.fsyncSync(fd);fs.closeSync(fd);} fs.writeFileSync(ready,"between");spin(); }
else { await store.writeBatch({identity,ops:[{kind:"put",key:"partial/a",value:1},{kind:"put",key:"partial/b",value:()=>{fs.writeFileSync(ready,"mid");spin();}}]}); }
`;
const killAt=async(root:string,phase:string,stopAfter:number)=>{
 const ready=path.join(root,"point"),ack=path.join(root,"ack");
 const child=spawn(process.execPath,["--input-type=module","-e",worker,root,phase,ready,ack,String(stopAfter)],{cwd:process.cwd(),stdio:"ignore"});
 try {await waitFor(ready);child.kill("SIGKILL");await new Promise<void>(resolve=>child.once("close",()=>resolve()));}
 finally {if(child.exitCode===null&&child.signalCode===null){child.kill("SIGKILL");await new Promise<void>(resolve=>child.once("close",()=>resolve()));}}
 return {ack:fs.existsSync(ack)?fs.readFileSync(ack,"utf8").trim().split(/\s+/).filter(Boolean).map(Number):[],wal:fs.existsSync(path.join(root,"state.db-wal")),shm:fs.existsSync(path.join(root,"state.db-shm"))};
};

describe("SQLite state power-cut recovery",()=>{
 // [heavy] is included in test:smoke, whose CI matrix runs on Linux and Windows.
 it("[heavy] preserves acknowledged FULL commits and rolls back every killed partial batch",async()=>{
   const random=seeded(0x6477);
   for(let round=0;round<6;round++){
     const root=rootFor(); const phase=random()<0.5?"mid":"between"; const stopAfter=1+Math.floor(random()*4);
     const killed=await killAt(root,phase,stopAfter);
     if(phase==="between") expect(killed.ack).toEqual(Array.from({length:stopAfter},(_,i)=>i));
     const db=openNodeSqlite(path.join(root,"state.db"));
     expect(db.prepare("PRAGMA integrity_check").get()).toEqual({integrity_check:"ok"});
     const rows=db.prepare("SELECT key, value FROM kv ORDER BY key").all();
     const keys=rows.map(row=>String(row.key));
     for(const id of killed.ack) expect(keys).toContain(`commit/${id}`);
     if(phase==="mid") { expect(keys).not.toContain("partial/a"); expect(keys).not.toContain("partial/b"); }
     // A killed writer may leave the WAL and shared-memory files behind; opening SQLite above must recover them.
     expect(killed.wal,"killed writer leaves a WAL to recover").toBe(true);
     expect(killed.shm,"killed writer leaves shared-memory state to recover").toBe(true);
     db.close();
   }
 },29_000);
});
