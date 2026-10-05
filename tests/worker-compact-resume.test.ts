import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
// Real source/dist worker with a deterministic RPC child. Refusal must remain
// caller-visible and must not delay main's ordinary settled shutdown.
const fakePi = `#!/usr/bin/env node
const emit = event => process.stdout.write(JSON.stringify(event) + '\\n');
let model = {provider:'resume-test', id:'offline'}, buffer = '';
const id = '12345678-1234-1234-1234-123456789abc';
const answer = text => emit({type:'message_end', message:{role:'assistant', provider:model.provider, model:model.id, content:text, stopReason:'stop'}});
const settled = () => { emit({type:'agent_end'}); emit({type:'agent_settled', outcome:'completed'}); };
process.stdin.on('data', chunk => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf('\\n')) >= 0) {
    const frame = JSON.parse(buffer.slice(0,newline)); buffer = buffer.slice(newline+1);
    const reply = data => emit({type:'response', id:frame.id, command:frame.type, success:true, data});
    if(frame.type === 'get_state') reply({model, thinkingLevel:'off', isStreaming:false, isCompacting:false});
    else if(frame.type === 'set_model') { model = {provider:frame.provider,id:frame.modelId}; reply(model); }
    else if(frame.type === 'set_thinking_level') reply();
    else if(frame.type === 'prompt') {
      emit({type:'agent_start'});
      answer('I will compact, then start item X');
      if(frame.message === 'resume') {
        emit({type:'entry_appended', entry:{type:'custom', customType:'fabric-compact-resume', data:{id, resume:'start item X', state:'pending'}}});
        emit({type:'compaction_end', reason:'manual'});
        emit({type:'entry_appended', entry:{type:'custom', customType:'fabric-compact-resume', data:{id, resume:'start item X', state:'cancelled', reason:'automatic compaction resume disabled (smarty-dev#5282)'}}});
        emit({type:'fabric_compact_resume_refused', protocol:1, runId:process.env.PI_FABRIC_PARENT_RUN, count:1, message:'Automatic compaction resume is disabled (smarty-dev#5282). Re-submit the pending work explicitly.'});
        settled();
      } else { emit({type:'compaction_end', reason:'manual'}); settled(); }
    }
  }
});
process.stdin.on('end', () => process.exit(0));
`;
const run = async (task: string, legacyRecovery = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-worker-resume-")); roots.push(root);
  const binary = path.join(root, "fake-pi.mjs");
  const script = legacyRecovery ? fakePi.replace("process.stdin.on('data'", "emit({type:'fabric_compact_resume_ready', protocol:1, runId:process.env.PI_FABRIC_PARENT_RUN, message:'Resume after compaction: foreign A'});\nprocess.stdin.on('data'") : fakePi;
  fs.writeFileSync(binary, script, { mode: 0o755 });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 15_000, budgetUsd: 0 }, {
    workerPath: path.resolve(process.env.FABRIC_COMPACT_RESUME_WORKER ?? "src/worker.ts"), piBinary: binary, runRoot: root,
  });
  managers.push(manager);
  const result = await manager.run({ task, cwd: root, model: "resume-test/offline", transport: "process", extensions: false });
  const events = fs.readFileSync(result.logFile!, "utf8").trim().split("\n").map(line => JSON.parse(line));
  return { result, events };
};

describe("worker compaction safe-floor refusal", () => {
  it("refuses an older child's startup recovery announcement instead of bundling it with fresh work", async () => {
    const { result, events } = await run("fresh B", true);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("restart recovery refused: original admission cannot be proven");
    expect(events.some(event => event.type === "message_end")).toBe(false);
    expect(events.some(event => event.type === "agent_start")).toBe(false);
  }, 25_000);

  it("retains live refusal as a caller warning and closes after the original settled turn", async () => {
    const { result, events } = await run("resume");
    expect(result.status).toBe("completed");
    expect(result.text).toBe("I will compact, then start item X");
    expect(events.filter(event => event.type === "agent_settled")).toHaveLength(1);
    expect(events.filter(event => event.type === "agent_start")).toHaveLength(1);
    expect(events.filter(event => event.type === "message_start" && event.message.role === "user")).toHaveLength(0);
    expect(result.warnings).toEqual([expect.stringContaining("Automatic compaction resume is disabled")]);
    expect(events.some(event => event.type === "entry_appended" && event.entry.data.state === "cancelled")).toBe(true);
  }, 25_000);

  it("still closes a plain compacted child without a pending continuation", async () => {
    const { result, events } = await run("plain");
    expect(result.status).toBe("completed");
    expect(result.text).toBe("I will compact, then start item X");
    expect(events.filter(event => event.type === "agent_settled")).toHaveLength(1);
  }, 25_000);
});
