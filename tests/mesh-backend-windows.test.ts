import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";

const roots: string[] = [];
const makeRoot = (name: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), name)); roots.push(root); return root; };
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const itWindows = process.platform === "win32" ? it : it.skip;
const waitFor = async (file: string, timeout = 8_000) => {
  const deadline = Date.now() + timeout;
  while (!fs.existsSync(file)) { if (Date.now() > deadline) throw new Error("timed out waiting for file: " + file); await new Promise(r => setTimeout(r, 10)); }
};
const childCode = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
const jiti = createJiti(pathToFileURL(process.cwd()+"/index.js").href);
const { SqliteStateStore, openNodeSqlite } = await jiti.import("./src/mesh/state-sqlite.ts");
const root=process.argv[1], mode=process.argv[2], ready=process.argv[3], ack=process.argv[4];
const open=(file)=>{ const db=openNodeSqlite(file); return { exec(sql){ db.exec(sql); if(sql.includes("synchronous = NORMAL")) db.exec("PRAGMA synchronous = FULL"); }, prepare(sql){return db.prepare(sql)}, close(){db.close()}, get isTransaction(){return db.isTransaction} }; };
const store=await SqliteStateStore.open(root,65536,1000,{open,lockTimeoutMs:3000,initialize:"create"});
const spin=()=>{ for(;;) {} };
if(mode==="hold") { const db=openNodeSqlite(root+"/state.db"); db.exec("BEGIN IMMEDIATE"); fs.writeFileSync(ready,"held"); spin();
} else if(mode==="between") {
  for(let i=0;i<3;i++){ await store.put({key:"durable/"+i,value:{i},identity:{id:"child",name:"child",kind:"agent"}}); fs.appendFileSync(ack,i+"\\n"); }
  fs.writeFileSync(ready,"between"); spin();
} else {
  await store.writeBatch({identity:{id:"child",name:"child",kind:"agent"},ops:[
    {kind:"put",key:"atomic/left",value:"left"},
    {kind:"put",key:"atomic/right",value:()=>{fs.writeFileSync(ready,"mid");spin();}}
  ]});
}
`;
const spawnChild=(root:string,mode:string,ready:string,ack:string)=>spawn(process.execPath,["--input-type=module","-e",childCode,root,mode,ready,ack],{cwd:process.cwd(),stdio:"ignore"});
const waitClose=(child:ReturnType<typeof spawn>)=>new Promise<void>(resolve=>child.once("close",()=>resolve()));

describe("SQLite state Windows semantics", () => {
  it("uses a path containing spaces and non-ASCII characters", async () => {
    const root=path.join(makeRoot("mesh sqlite "),"més h"); fs.mkdirSync(root,{mode:0o700});
    const store=await SqliteStateStore.open(root,65536,1000);
    await store.put({key:"unicode/path",value:"ok",identity:{id:"test",name:"test",kind:"agent"}});
    expect(store.get("unicode/path")?.value).toBe("ok");
    expect(store.file).toContain("més h");
    store.close();
  }, 10_000);

  it("keeps two independent processes in WAL mode", async () => {
    const root=makeRoot("mesh-wal-process-");
    const ready=path.join(root,"ready"), ack=path.join(root,"ack");
    const writer=spawnChild(root,"between",ready,ack);
    try {
      await waitFor(ready);
      const observer=openNodeSqlite(path.join(root,"state.db"));
      expect(observer.prepare("PRAGMA journal_mode").get()).toEqual({journal_mode:"wal"});
      expect(observer.prepare("SELECT value FROM kv WHERE key = ?").get("durable/2")).toBeDefined();
      observer.close();
    } finally { writer.kill("SIGKILL"); await waitClose(writer); }
  }, 12_000);

  // POSIX permits unlink/rename of open files; the NTFS open-handle denial is Windows-specific.
  itWindows("does not rename or delete an open database on Windows", async () => {
    const root=makeRoot("mesh-windows-open-");
    const store=await SqliteStateStore.open(root,65536,1000);
    const file=store.file;
    expect(()=>fs.renameSync(file,file+".renamed")).toThrow();
    expect(()=>fs.unlinkSync(file)).toThrow();
    expect(fs.existsSync(file)).toBe(true);
    store.close();
  }, 10_000);

  it("honors a short busy budget while another process holds BEGIN IMMEDIATE", async () => {
    const root=makeRoot("mesh-windows-busy-");
    const ready=path.join(root,"held"), ack=path.join(root,"ack");
    const store=await SqliteStateStore.open(root,65536,1000,{lockTimeoutMs:80,busyTimeoutMs:0});
    const holder=spawnChild(root,"hold",ready,ack);
    try {
      await waitFor(ready);
      const started=Date.now();
      await expect(store.put({key:"busy/loser",value:1,identity:{id:"test",name:"test",kind:"agent"}})).rejects.toThrow();
      expect(Date.now()-started).toBeLessThan(500);
      expect(store.get("busy/loser")).toBeUndefined();
    } finally { holder.kill("SIGKILL"); await waitClose(holder); store.close(); }
  }, 12_000);
});
