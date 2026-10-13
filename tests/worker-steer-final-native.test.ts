import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readFinalAnswerReceipt } from "../src/worker/terminal-answer.js";

const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  // Retain private fixtures as real-path evidence; this probe never deletes files.
});
const rows = (file: string): Record<string, any>[] => {
  try { return fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)); }
  catch { return []; }
};
const observed = async (root: string, condition: () => boolean): Promise<void> => {
  if (condition()) return;
  await new Promise<void>((resolve, reject) => {
    const finish = (error?: Error) => { clearTimeout(timer); watcher.close(); error ? reject(error) : resolve(); };
    const watcher = fs.watch(root, { recursive: true }, () => { if (condition()) finish(); });
    const timer = setTimeout(() => finish(new Error("Native final-steer proof deadline exceeded")), 30_000);
    if (condition()) finish();
  });
};
const alive = (pid: number): boolean => {
  try { process.kill(pid, 0); return process.platform !== "linux" || !/^\d+ \(.*\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8")); }
  catch { return false; }
};

// Only inference is offline. The process transport, worker, native SDK, final
// boundary, admission journal, refusal attribution and child exit are real.
const provider = `
import fs from "node:fs";
import path from "node:path";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
export default function (pi) {
  const root = process.env.FINAL_STEER_PROOF_ROOT;
  const log = value => fs.appendFileSync(path.join(root, "native.jsonl"), JSON.stringify({pid:process.pid,...value})+"\\n");
  const model = {provider:"final-steer-proof",id:"offline",name:"Offline native proof",api:"final-steer-proof-api",baseUrl:"http://invalid.local",reasoning:false,input:["text"],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:200000,maxTokens:4096};
  let calls = 0;
  const stream = (selected, context) => {
    calls++;
    log({type:"request",calls,messages:context.messages});
    // This is the concrete custody violation a second native turn would cause.
    if (calls > 1) fs.writeFileSync(path.join(root,"workspace","source.ts"),"FORBIDDEN_SOURCE_EDIT");
    const events = createAssistantMessageEventStream();
    const message = {role:"assistant",provider:selected.provider,model:selected.id,api:selected.api,timestamp:Date.now(),content:[{type:"text",text:"FINAL_AUTHORIZED_RESULT"}],stopReason:"stop",usage:{input:3,output:4,cacheRead:0,cacheWrite:0,totalTokens:7,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
    events.push({type:"start",partial:message});events.push({type:"done",reason:"stop",message});events.end();
    return events;
  };
  pi.registerProvider({id:"final-steer-proof",name:"Offline native proof",auth:{apiKey:{name:"Offline",check:async()=>({type:"api_key",source:"fixture"}),resolve:async()=>({auth:{}})}},getModels:()=>[model],stream,streamSimple:stream});
  const appendAfterFinal = () => {
    const directory = path.dirname(process.env.PI_FABRIC_DELIVERY_DIR) + ".ctl";
    const receipt = JSON.parse(fs.readFileSync(path.join(directory,"final-answer.json"),"utf8"));
    const ids = JSON.parse(process.env.FINAL_STEER_PROOF_IDS);
    const sender = {id:"session:native-proof-sender",name:"Native proof sender",kind:"main",verified:"mesh"};
    for (const [index,type] of ["steer","follow_up"].entries()) {
      const via = type === "steer" ? "steer" : "followUp";
      fs.appendFileSync(path.join(directory,"steer.jsonl"),JSON.stringify({id:ids[index],ts:Date.now(),type,message:"LATE_"+type,provenance:{v:1,channel:"fabric",via,sender}})+"\\n");
    }
    log({type:"appended-after-final",receiptId:receipt.id,ids});
  };
  pi.on("session_start",()=>{
    const send = process.send?.bind(process);
    if (!send) throw new Error("Native proof has no owned IPC channel");
    process.send = (message,...args) => {
      if (message?.type === "fabric_final_answer") appendAfterFinal();
      return send(message,...args);
    };
  });
}
`;

describe("real process task steered at its native final report", () => {
  it("retains an undelivered receipt, exits without a new turn, and leaves source unchanged", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-final-steer-"));
    const workspace = path.join(root, "workspace"); fs.mkdirSync(workspace);
    const profile = path.join(root, "profile");
    fs.mkdirSync(path.join(profile, "extensions"), { recursive: true });
    fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false }, defaultProjectTrust: "never" }));
    fs.writeFileSync(path.join(profile, "extensions", "provider.ts"), provider);
    fs.writeFileSync(path.join(workspace, "source.ts"), "AUTHORIZED_SOURCE\n");
    const ids = [randomUUID(), randomUUID()];
    vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("FINAL_STEER_PROOF_ROOT", root); vi.stubEnv("FINAL_STEER_PROOF_IDS", JSON.stringify(ids));
    const notices: Record<string, any>[] = [];
    const manager = new AgentManager(workspace, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000, nice: 19, retainRuns: true }, {
      workerPath: path.resolve(process.env.FABRIC_FINAL_STEER_WORKER ?? "src/worker.ts"),
      piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
      runRoot: path.join(root, "runs"), onTerminalNotice: notice => { notices.push(notice); }, fullCodeMode: false,
    }); managers.push(manager);
    const handle = await manager.spawn({ task: "Return FINAL_AUTHORIZED_RESULT; never reopen source custody", model: "final-steer-proof/offline", thinking: "off", transport: "process", extensions: true, tools: [] });
    const directory = path.join(root, "runs", handle.id);
    // The sender appends at native final-report IPC, AFTER stable final-result
    // production and BEFORE sending the report to the worker/parent.
    await observed(root, () => rows(path.join(root, "native.jsonl")).some(row => row.type === "appended-after-final"));
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(receipt.text).toBe("FINAL_AUTHORIZED_RESULT");
    const result = await manager.wait(handle.id, { timeoutMs: 30_000 });
    expect(result).toMatchObject({ status: "completed", text: receipt.text, turns: 1, toolCalls: 0, finalAnswerReceipt: { id: receipt.id } });
    await manager.close(); // Production owned-process/tree join, not a result-as-exit assumption.
    expect(notices.map(notice => notice.messageId).sort()).toEqual([...ids].sort());
    expect(notices.every(notice => notice.sender?.id === "session:native-proof-sender" && notice.finalAnswerReceiptId === receipt.id)).toBe(true);
    expect(rows(path.join(root, "native.jsonl")).filter(row => row.type === "request")).toHaveLength(1);
    expect(fs.readFileSync(path.join(workspace, "source.ts"), "utf8")).toBe("AUTHORIZED_SOURCE\n");
    const pid = rows(path.join(root, "native.jsonl"))[0]!.pid;
    expect(alive(pid)).toBe(false);
    console.log(`REAL_PATH: PASS worker=${process.env.FABRIC_FINAL_STEER_WORKER ?? "src/worker.ts"} root=${root} run=${handle.id} nativePid=${pid} nativeAlive=false providerCalls=1 sourceUnchanged=true refused=${ids.join(",")} finalReceipt=${receipt.id}`);
  }, 45_000);
});
