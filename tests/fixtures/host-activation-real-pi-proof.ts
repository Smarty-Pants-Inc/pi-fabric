import fs from "node:fs";
import path from "node:path";
import { runRealPiHostProof } from "../helpers/host-activation-real-pi.js";
if (!process.env.TMPDIR || !process.env.TASK_OUT || !process.env.PI_FABRIC_TEST_PI_BINARY) throw new Error("TMPDIR, TASK_OUT and installed PI_FABRIC_TEST_PI_BINARY are required");
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR, "real-pi-"));
const output = path.join(process.env.TASK_OUT, "real-pi"); fs.mkdirSync(output, { recursive: true });
try {
  const rows = [];
  for (const [name, limit, tasks, nested] of [["n-plus-two",2,4,false],["three-roots-eight",4,8,false],["parent-child",1,0,true]] as const) {
    if (process.argv.includes("--nested-only") && !nested) {
      rows.push({name,...JSON.parse(fs.readFileSync(path.join(output,name,"proof.json"),"utf8"))}); continue;
    }
    const root = path.join(scratch,name);
    try { rows.push({ name, ...await runRealPiHostProof(root,limit,tasks,nested) }); }
    finally { fs.cpSync(root,path.join(output,name),{recursive:true}); }
  }
  fs.writeFileSync(path.join(output,"summary.json"),JSON.stringify(rows,null,2)); console.log(JSON.stringify(rows,null,2));
} finally { fs.rmSync(scratch,{recursive:true,force:true}); }
