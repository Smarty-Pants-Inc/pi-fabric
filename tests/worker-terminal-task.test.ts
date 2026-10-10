import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { finalAssistantText, readFinalAnswerReceipt, saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { readTerminalControl, settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const temporary = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-terminal-task-")); roots.push(root); return root; };
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const rows = (file: string): Record<string, any>[] => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const until = async (condition: () => boolean, timeoutMs = 20000) => {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("Terminal proof deadline exceeded");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const alive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return process.platform !== "linux" || !/^\d+ \(.*\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8"));
  } catch { return false; }
};

// Only inference is synthetic. Production worker, pinned native SDK, persistence,
// queues, tools, private-envelope context consumption and owned cleanup are real.
const provider = `
import fs from "node:fs";
import path from "node:path";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
export default function (pi) {
  const root = process.env.TERMINAL_PROOF_ROOT;
  const log = value => fs.appendFileSync(path.join(root,"provider.jsonl"), JSON.stringify({pid:process.pid,...value})+"\\n");
  const model = {provider:"terminal-proof",id:"offline",name:"Offline terminal proof",api:"terminal-proof-api",baseUrl:"http://invalid.local",reasoning:false,input:["text"],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:200000,maxTokens:4096};
  let calls = 0;
  const stream = (selected, context) => {
    const events = createAssistantMessageEventStream();
    calls++;
    log({type:"request",calls,messages:context.messages});
    void (async () => {
      while (!fs.existsSync(path.join(root,"release-"+calls))) await new Promise(resolve=>setTimeout(resolve,10));
      const tool = process.env.TERMINAL_PROOF_INTERMEDIATE === "1" && calls === 1;
      const reply = process.env.TERMINAL_PROOF_REPLY === "1" && calls === 1;
      const message = {role:"assistant",provider:selected.provider,model:selected.id,api:selected.api,timestamp:Date.now(),
        content:reply ? [{type:"toolCall",id:"reply",name:"fabric_reply",arguments:{ok:true}}] : tool ? [{type:"text",text:"intermediate progress"},{type:"toolCall",id:"work",name:"bash",arguments:{command:"printf intermediate"}}] : [{type:"text",text:fs.readFileSync(path.join(root,"answer.txt"),"utf8")}],
        stopReason:tool || reply ? "toolUse":"stop",usage:{input:3,output:4,cacheRead:0,cacheWrite:0,totalTokens:7,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
      events.push({type:"start",partial:message});
      events.push({type:"done",reason:tool || reply ? "toolUse":"stop",message});events.end();
    })();
    return events;
  };
  pi.registerProvider({id:"terminal-proof",name:"Offline terminal proof",auth:{apiKey:{name:"Offline",check:async()=>({type:"api_key",source:"fixture"}),resolve:async()=>({auth:{}})}},getModels:()=>[model],stream,streamSimple:stream});
  pi.on("session_start",(_event,ctx)=>log({type:"native-start",sessionId:ctx.sessionManager.getSessionId()}));
  pi.on("turn_end",()=>{ if (process.env.TERMINAL_PROOF_EXIT === "1") process.exit(9); });
}
`;
const setup = () => {
  const root = temporary();
  const agentDir = path.join(root, "agent"); fs.mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false }, defaultProjectTrust: "never" }));
  fs.writeFileSync(path.join(agentDir, "extensions", "provider.ts"), provider);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("TERMINAL_PROOF_ROOT", root);
  const notices: unknown[] = [];
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30000, nice: 19 }, {
    workerPath: path.resolve("src/worker.ts"), piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    runRoot: path.join(root, "runs"), fullCodeMode: false, onTerminalNotice: notice => { notices.push(notice); },
  }); managers.push(manager);
  return { root, manager, notices };
};
const spawn = async (s: ReturnType<typeof setup>, extra: Record<string, unknown> = {}) => {
  const handle = await s.manager.spawn({ task: "Return the authorized answer; do not execute any late request", model: "terminal-proof/offline", thinking: "off", transport: "process", extensions: true, tools: ["bash"], ...extra });
  const directory = path.join(s.root, "runs", handle.id);
  await until(() => rows(path.join(s.root, "provider.jsonl")).some(row => row.type === "request"));
  return { handle, directory };
};

describe("durable task final-answer contract", () => {
  it("keeps the first complete full answer immutable and rejects malformed/mismatched receipts", () => {
    const root = temporary(); const text = "full answer ".repeat(100000);
    const first = saveFinalAnswerReceipt(root, "run", text);
    expect(readFinalAnswerReceipt(root, "run")).toEqual(first);
    expect(saveFinalAnswerReceipt(root, "run", "late overwrite")).toEqual(first);
    expect(first.text).toBe(text);
    expect(readFinalAnswerReceipt(root, "other")).toBeUndefined();
    expect(() => saveFinalAnswerReceipt(root, "other", "wrong run")).toThrow("mismatched");
    fs.writeFileSync(path.join(root, "final-answer.json"), "{}");
    expect(readFinalAnswerReceipt(root, "run")).toBeUndefined();
    expect(() => saveFinalAnswerReceipt(root, "run", "invalid file")).toThrow("Invalid");
  });
  it.each(["toolUse", "length", "error", "aborted", "pending", "deferred"])("does not terminate on non-final %s prose", stopReason => {
    expect(finalAssistantText({ role: "assistant", stopReason, content: [{ type: "text", text: "not final" }] })).toBeUndefined();
  });
  it("requires no tool calls even when the provider reports stop, and admits an empty final", () => {
    expect(finalAssistantText({ role: "assistant", stopReason: "stop", content: [{ type: "text", text: "progress" }, { type: "toolCall" }] })).toBeUndefined();
    expect(finalAssistantText({ role: "assistant", stopReason: "stop", content: [] })).toBe("");
  });
  it("does not resurrect a consumed control on replay", () => {
    const root = temporary(); const id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    trackTerminalControl(root, { id, delivery: "steer" }); settleTerminalControl(root, id, "delivered");
    trackTerminalControl(root, { id, delivery: "steer" }); settleTerminalControl(root, id, "refused");
    expect(readTerminalControl(root, id)?.state).toBe("delivered");
  });
});

describe("native worker task terminal boundary", () => {
  it("fences already queued steer/followUp before another request and closes its owned process", async () => {
    const s = setup(); const answer = "authorized full answer\n".repeat(10000);
    fs.writeFileSync(path.join(s.root, "answer.txt"), answer);
    const { handle, directory } = await spawn(s);
    const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
      sender: { id: "trusted-test-sender", name: "Native proof sender", kind: "main" as const, verified: "mesh" as const } };
    const steer = s.manager.steer(handle.id, "FORBIDDEN_LATE_STEER", undefined, provenance);
    const follow = s.manager.followUp(handle.id, "FORBIDDEN_LATE_FOLLOW_UP", undefined, { ...provenance, via: "followUp" }, { deadlineMs: 60000 });
    await until(() => readTerminalControl(directory, steer.messageId)?.state === "queued" && readTerminalControl(directory, follow.messageId)?.state === "queued");
    // Native queue_update proves ingress was actually queued, not just admitted by manager.
    await until(() => rows(path.join(directory, "events.jsonl")).some(row => row.type === "queue_update" && JSON.stringify(row).includes("FORBIDDEN_LATE_STEER")));
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status, result.error).toBe("completed");
    expect(result.text).toBe(answer);
    expect(result).toMatchObject({ turns: 1, toolCalls: 0, usage: { input: 3, output: 4 } });
    const receipt = readFinalAnswerReceipt(directory, handle.id)!;
    expect(receipt.text).toBe(answer);
    expect(result.finalAnswerReceipt).toEqual({ id: receipt.id, recordedAt: receipt.recordedAt });
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
    expect(rows(path.join(s.root, "provider.jsonl")).filter(row => row.type === "request")).toHaveLength(1);
    const refused = rows(path.join(directory, "lifecycle.jsonl")).filter(row => row.event === "message.refused");
    expect(refused.map(row => row.data.messageId).sort()).toEqual([steer.messageId, follow.messageId].sort());
    expect(refused.every(row => row.data.code === "FABRIC_TARGET_TERMINAL" && row.data.finalAnswerReceiptId === receipt.id)).toBe(true);
    expect(refused.every(row => row.data.sender.id === provenance.sender.id)).toBe(true);
    for (const late of [() => s.manager.followUp(handle.id, "EVEN_LATER"), () => s.manager.steer(handle.id, "EVEN_LATER")]) {
      try { late(); throw new Error("Late input was admitted"); }
      catch (error) { expect(error).toMatchObject({ code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt.id, targetId: handle.id }); }
    }
    const history = rows(path.join(directory, "session.jsonl"));
    expect(history.some(row => row.message?.role === "assistant" && row.message.content?.[0]?.text === answer)).toBe(true);
  }, 45000);

  it("allows steer after intermediate prose/tools, and refuses only the later undelivered steer", async () => {
    const s = setup(); vi.stubEnv("TERMINAL_PROOF_INTERMEDIATE", "1");
    fs.writeFileSync(path.join(s.root, "answer.txt"), "final after allowed work");
    const { handle, directory } = await spawn(s);
    const allowed = s.manager.steer(handle.id, "ALLOWED_INTERMEDIATE_STEER");
    await until(() => readTerminalControl(directory, allowed.messageId)?.state === "queued");
    // The worker's queued admission precedes asynchronous native input. Confirm
    // the native queue before opening the intermediate turn's delivery boundary.
    await until(() => rows(path.join(directory, "events.jsonl")).some(row => row.type === "queue_update" && JSON.stringify(row).includes("ALLOWED_INTERMEDIATE_STEER")));
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    await until(() => rows(path.join(s.root, "provider.jsonl")).filter(row => row.type === "request").length === 2);
    expect(readFinalAnswerReceipt(directory, handle.id)).toBeUndefined();
    expect(readTerminalControl(directory, allowed.messageId)?.state).toBe("delivered");
    expect(JSON.stringify(rows(path.join(s.root, "provider.jsonl")).find(row => row.calls === 2)?.messages)).toContain("ALLOWED_INTERMEDIATE_STEER");
    const refused = s.manager.steer(handle.id, "FORBIDDEN_FINAL_STEER");
    await until(() => readTerminalControl(directory, refused.messageId)?.state === "queued");
    fs.writeFileSync(path.join(s.root, "release-2"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status, result.error).toBe("completed");
    expect(result.text).toBe("final after allowed work");
    expect(result).toMatchObject({ turns: 2, toolCalls: 1, usage: { input: 6, output: 8 } });
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
    expect(rows(path.join(s.root, "provider.jsonl")).filter(row => row.type === "request")).toHaveLength(2);
    expect(rows(path.join(directory, "lifecycle.jsonl")).filter(row => row.event === "message.refused").map(row => row.data.messageId)).toEqual([refused.messageId]);
  }, 45000);

  it("keeps actor continuation semantics across a successful assistant stop", async () => {
    const s = setup(); fs.writeFileSync(path.join(s.root, "answer.txt"), "actor turn answer");
    const { handle, directory } = await spawn(s, { actorId: "terminal-proof-actor", actorName: "Terminal proof actor" });
    s.manager.steer(handle.id, "ALLOWED_ACTOR_CONTINUATION");
    await until(() => rows(path.join(directory, "events.jsonl")).some(row => row.type === "queue_update" && JSON.stringify(row).includes("ALLOWED_ACTOR_CONTINUATION")));
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    await until(() => rows(path.join(s.root, "provider.jsonl")).filter(row => row.type === "request").length === 2);
    expect(readFinalAnswerReceipt(directory, handle.id)).toBeUndefined();
    expect(JSON.stringify(rows(path.join(s.root, "provider.jsonl")).find(row => row.calls === 2)?.messages)).toContain("ALLOWED_ACTOR_CONTINUATION");
    fs.writeFileSync(path.join(s.root, "release-2"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status, result.error).toBe("completed");
    expect(result.finalAnswerReceipt).toBeUndefined();
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
  }, 45000);

  it("seals a terminating fabric_reply before native queues drain", async () => {
    const s = setup(); vi.stubEnv("TERMINAL_PROOF_REPLY", "1");
    const { handle, directory } = await spawn(s, { schema: { type: "object", required: ["ok"], properties: { ok: { type: "boolean" } } }, replyTool: true });
    const steer = s.manager.steer(handle.id, "FORBIDDEN_AFTER_TOOL_REPLY");
    const follow = s.manager.followUp(handle.id, "FORBIDDEN_AFTER_TOOL_FOLLOWUP");
    await until(() => readTerminalControl(directory, steer.messageId)?.state === "queued" && readTerminalControl(directory, follow.messageId)?.state === "queued");
    await until(() => rows(path.join(directory, "events.jsonl")).some(row => row.type === "queue_update" && JSON.stringify(row).includes("FORBIDDEN_AFTER_TOOL_REPLY")));
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result).toMatchObject({ status: "completed", replyVia: "tool", value: { ok: true }, text: "", turns: 1, toolCalls: 1, usage: { input: 3, output: 4 } });
    expect(readFinalAnswerReceipt(directory, handle.id)?.id).toBe(result.finalAnswerReceipt?.id);
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
    expect(rows(path.join(s.root, "provider.jsonl")).filter(row => row.type === "request")).toHaveLength(1);
    expect(rows(path.join(directory, "lifecycle.jsonl")).filter(row => row.event === "message.refused").map(row => row.data.messageId).sort())
      .toEqual([steer.messageId, follow.messageId].sort());
  }, 45000);

  it("retains a final receipt after abrupt native exit instead of requiring successful process exit", async () => {
    const s = setup(); vi.stubEnv("TERMINAL_PROOF_EXIT", "1");
    fs.writeFileSync(path.join(s.root, "answer.txt"), "durable before native exit");
    const { handle, directory } = await spawn(s);
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status, result.error).toBe("completed");
    expect(result.text).toBe("durable before native exit");
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
    expect(readFinalAnswerReceipt(directory, handle.id)?.text).toBe(result.text);
    expect(rows(path.join(s.root, "provider.jsonl")).filter(row => row.type === "request")).toHaveLength(1);
  }, 45000);

  it("does not claim success when native final receipt persistence fails", async () => {
    const s = setup(); fs.writeFileSync(path.join(s.root, "answer.txt"), "must not claim success");
    const { handle, directory } = await spawn(s);
    fs.mkdirSync(path.join(directory, "final-answer.json"));
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status).toBe("failed");
    expect(result.finalAnswerReceipt).toBeUndefined();
    expect(readFinalAnswerReceipt(directory, handle.id)).toBeUndefined();
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
  }, 45000);

  it.each([false, true])("validates structured output before receipt-bound publication (replyTool=%s)", async replyTool => {
    const s = setup(); fs.writeFileSync(path.join(s.root, "answer.txt"), "not valid JSON");
    const { handle, directory } = await spawn(s, { schema: { type: "object", required: ["ok"], properties: { ok: { type: "boolean" } } }, replyTool });
    fs.writeFileSync(path.join(s.root, "release-1"), "");
    const result = await s.manager.wait(handle.id, { timeoutMs: 30000 });
    expect(result.status).toBe("failed");
    expect(result.error).toContain(replyTool ? "Directive reply missing" : "Structured agent output was invalid");
    expect(result.finalAnswerReceipt?.id).toBe(readFinalAnswerReceipt(directory, handle.id)?.id);
    await until(() => !alive(rows(path.join(s.root, "provider.jsonl"))[0]!.pid));
  }, 45000);
});
