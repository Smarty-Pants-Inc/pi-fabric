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
// Real source/dist worker with a deterministic RPC child. The delayed user
// admission specifically spans the worker's 200ms steer-timer close check.
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
        settled();
        setTimeout(() => {
          emit({type:'agent_start'});
          const content = 'Resume after compaction: start item X\\n\\n[Fabric continuation: ' + id + ']';
          emit({type:'message_start', message:{role:'user', content}});
          emit({type:'message_end', message:{role:'user', content}});
          answer('item X completed'); settled();
        }, 550);
      } else { emit({type:'compaction_end', reason:'manual'}); settled(); }
    }
  }
});
process.stdin.on('end', () => process.exit(0));
`;
const run = async (task: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-worker-resume-")); roots.push(root);
  const binary = path.join(root, "fake-pi.mjs"); fs.writeFileSync(binary, fakePi, { mode: 0o755 });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 15_000, budgetUsd: 0 }, {
    workerPath: path.resolve(process.env.FABRIC_COMPACT_RESUME_WORKER ?? "src/worker.ts"), piBinary: binary, runRoot: root,
  });
  managers.push(manager);
  const result = await manager.run({ task, cwd: root, model: "resume-test/offline", transport: "process", extensions: false });
  const events = fs.readFileSync(result.logFile!, "utf8").trim().split("\n").map(line => JSON.parse(line));
  return { result, events };
};

describe("worker compaction resume shutdown fence", () => {
  it("keeps stdin open past settled and its timer until the one resume user turn finishes", async () => {
    const { result, events } = await run("resume");
    expect(result.status).toBe("completed");
    expect(result.text).toBe("item X completed");
    expect(events.filter(event => event.type === "agent_settled")).toHaveLength(2);
    expect(events.filter(event => event.type === "message_start" && event.message.role === "user")).toHaveLength(1);
    expect(result.warnings ?? []).toEqual([]);
  }, 25_000);

  it("still closes a plain compacted child without a pending continuation", async () => {
    const { result, events } = await run("plain");
    expect(result.status).toBe("completed");
    expect(result.text).toBe("I will compact, then start item X");
    expect(events.filter(event => event.type === "agent_settled")).toHaveLength(1);
  }, 25_000);
});
