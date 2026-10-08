import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";

const identity: MeshIdentity = { id: "crash-test", name: "crash-test", kind: "agent" };
const roots: string[] = [];
const temp = (label: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), `mesh-backend-crash-${label}-`)); roots.push(root); return root; };
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

type Store = { get(key: string): { value: unknown } | undefined; writeBatch(input: any): Promise<unknown>; close?(): void };
async function open(kind: string, root: string): Promise<Store> {
  if (kind === "sqlite") return await SqliteStateStore.open(root, 64 * 1024, 100, { lockTimeoutMs: 5_000 }) as unknown as Store;
  return new MeshStore(root, 64 * 1024, 100) as unknown as Store;
}
const CHILD = `
import { createJiti } from "jiti"; import { pathToFileURL } from "node:url"; import fs from "node:fs"; import path from "node:path";
const jiti=createJiti(pathToFileURL(process.cwd()+"/index.js").href); const [mod,sql]=await Promise.all([jiti.import("./src/mesh/store.ts"),jiti.import("./src/mesh/state-sqlite.ts")]);
const [kind,root,stage,id]=process.argv.slice(1); const identity={id:"crash",name:"crash",kind:"agent"}; const store=kind==="sqlite"?await sql.SqliteStateStore.open(root,65536,100,{lockTimeoutMs:5000}):new mod.MeshStore(root,65536,100);
const kill=()=>process.kill(process.pid,"SIGKILL"); const effectsDir=path.join(root,"effects"); fs.mkdirSync(effectsDir,{recursive:true});
if(stage==="before-begin") kill();
const fileRead=()=>{fs.readFileSync(path.join(root,"input.txt"),"utf8"); if(stage==="after-file-read")kill(); return "read";}; fileRead();
const ops=[{kind:"put",key:"crash/value",value:{committed:true,id}},{kind:"put",key:"crash/outbox/"+id,value:{id, effects:["a","b"],done:false}}];
await store.writeBatch({identity,ops,prepare:()=>{if(stage==="before-commit")kill();return[];},afterCommit:()=>{if(stage==="after-commit-before-effects")kill();}});
const apply=(name)=>{const target=path.join(effectsDir,id+"-"+name);try{fs.writeFileSync(target,"effect:"+name,{flag:"wx"});}catch(e){if(e.code!=="EEXIST")throw e;}};
apply("a"); if(stage==="mid-effects")kill(); apply("b"); store.close?.();
`;
function crash(kind: string, root: string, stage: string, id: string) {
  return new Promise<{ signal: NodeJS.Signals | null; code: number | null; output: string }>((resolve, reject) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, kind, root, stage, id], { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let output = ""; child.stdout.on("data", chunk => { output += chunk; }); child.stderr.on("data", chunk => { output += chunk; });
    const timeout = setTimeout(() => child.kill("SIGKILL"), 20_000); child.once("error", reject); child.once("close", (code, signal) => { clearTimeout(timeout); resolve({ code, signal, output }); });
  });
}

// Minimal test outbox: commit the intent with the state mutation. Replay writes each external effect
// to a unique O_EXCL file, then records completion. This isolates R11's contract from L2b's dispatcher.
async function recover(kind: string, root: string, id: string): Promise<void> {
  const store = await open(kind, root);
  try {
    const item = store.get(`crash/outbox/${id}`)?.value as { effects: string[]; done: boolean } | undefined;
    if (!item || item.done) return;
    const dir = path.join(root, "effects"); fs.mkdirSync(dir, { recursive: true });
    for (const name of item.effects) { const file = path.join(dir, `${id}-${name}`); try { fs.writeFileSync(file, `effect:${name}`, { flag: "wx" }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; } }
    await store.writeBatch({ identity, ops: [{ kind: "put", key: `crash/outbox/${id}`, value: { ...item, done: true } }] });
  } finally { store.close?.(); }
}

describe("mesh backend R11 callback-I/O crash recovery", () => {
  it.each(["file", "sqlite"])("recovers exactly-once file effects after SIGKILL at each boundary (%s)", async kind => {
    for (const stage of ["before-begin", "after-file-read", "before-commit", "after-commit-before-effects", "mid-effects"]) {
      const root = temp(`${kind}-${stage}`); fs.writeFileSync(path.join(root, "input.txt"), "stamp-1"); const id = stage;
      const killed = await crash(kind, root, stage, id); expect(killed.signal, `${stage}: ${killed.output}`).toBe("SIGKILL");
      await recover(kind, root, id); await recover(kind, root, id);
      const check = await open(kind, root);
      try {
        const committed = stage === "after-commit-before-effects" || stage === "mid-effects";
        expect(check.get("crash/value") !== undefined, `${kind}/${stage} commit`).toBe(committed);
        const effects = path.join(root, "effects"); const applied = fs.existsSync(effects) ? fs.readdirSync(effects).sort() : [];
        expect(applied).toEqual(committed ? [`${id}-a`, `${id}-b`] : []);
        const outbox = check.get(`crash/outbox/${id}`)?.value as { done: boolean } | undefined;
        expect(outbox?.done ?? false).toBe(committed);
      } finally { check.close?.(); }
    }
  }, 80_000);

  it.todo("integration: StateBackend.commitOutbox dispatches L2b's durable outbox automatically (not implemented on this base)");
});
