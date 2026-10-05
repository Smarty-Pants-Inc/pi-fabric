// Only model inference and summary text are synthetic; native Pi owns every lifecycle event.
import fs from 'node:fs';
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext, type StreamOptions } from '@earendil-works/pi-ai';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
export default function(pi: ExtensionAPI) {
  const receipt = (type: string, data = {}) => fs.appendFileSync(process.env.COMPACT_PROBE_TRANSCRIPT!, JSON.stringify({type, at: Date.now(), ...data}) + '\n');
  const model: Model<Api> = {provider:'compact-proof', id:'offline', name:'Keyless compact proof', api:'compact-proof-api', baseUrl:'http://invalid.local', reasoning:false, input:['text'], cost:{input:0, output:0, cacheRead:0, cacheWrite:0}, contextWindow:200000, maxTokens:4096};
  const stream = (selected: Model<Api>, context: TranscriptContext, options?: StreamOptions) => {
    const output = createAssistantMessageEventStream();
    const user = [...context.messages].reverse().find(m => m.role === 'user');
    const prompt = JSON.stringify(user);
    const summary = /conversation to summarize|NEW conversation messages|PREFIX of a turn/.test(prompt);
    const resumed = !summary && prompt.includes('Resume after compaction:');
    const tool = !summary && context.messages.at(-1)?.role !== 'toolResult' && !prompt.includes('already admitted');
    if (resumed && tool) receipt('X-selected', {prompt});
    const fresh = !summary && prompt.includes('Fresh mailbox task B:');
    const code = fresh && !resumed ? 'await pi.write({ path: "item-B.txt", text: "B completed" }); return "B completed";' : resumed ? 'await pi.write({ path: "item-X.txt", text: "X completed" }); ' + (fresh ? 'await pi.write({ path: "item-B.txt", text: "B completed" }); ' : '') + 'return "X completed";'
      : process.env.COMPACT_PROBE_ABORT === '1'
        ? 'await compact.request({ resume: "start item X", instructions: "Keep the failing test name in the summary" }); return "compaction requested";'
        : 'await compact.request({ instructions: "Keep the failing test name in the summary" }); return "compaction requested";';
    const message: AssistantMessage = {role:'assistant', provider:selected.provider, model:selected.id, api:selected.api, timestamp:Date.now(), content:tool ? [{type:'toolCall', id:resumed ? 'item-X-call' : 'compact-proof-call', name:'fabric_exec', arguments:{code}}] : [{type:'text', text:summary ? 'Compacted. Preserve failing test test_X. Pending work: start item X only if requested.' : resumed ? 'X completed' : fresh ? 'B completed' : 'Compaction requested; idle unless continuation is admitted.'}], stopReason:tool ? 'toolUse' : 'stop', usage:{input:0, output:0, cacheRead:0, cacheWrite:0, totalTokens:0, cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
    // Hold the post-request inference open until the driver sends the REAL RPC
    // abort. The request tool has completed, but agent_settled has not run.
    if (process.env.COMPACT_PROBE_ABORT === '1' && !summary && !tool && !resumed) {
      receipt('abort-ready');
      const finish = () => {
        message.stopReason = 'aborted'; message.errorMessage = 'Native RPC abort regression';
        output.push({type:'error', reason:'aborted', error:message}); output.end();
      };
      if (options?.signal?.aborted) finish();
      else options?.signal?.addEventListener('abort', finish, {once:true});
      return output;
    }
    output.push({type:'start', partial:message});
    if(tool) output.push({type:'toolcall_end', contentIndex:0, toolCall:message.content[0] as Extract<AssistantMessage['content'][number],{type:'toolCall'}>, partial:message});
    output.push({type:'done', reason:message.stopReason as 'stop'|'toolUse', message}); output.end(); return output;
  };
  pi.registerProvider({id:model.provider, name:model.name, auth:{apiKey:{name:'Keyless local test', check:async()=>({type:'api_key',source:'fixture'}), resolve:async()=>({auth:{}})}}, getModels:()=>[model], stream, streamSimple:stream});
  pi.on('session_start', async () => {
    // Native model restoration precedes extension-provider registration in Pi
    // 0.87.0. Give the inherited-model lane a native, keyless startup default;
    // the worker still has no selector and must await this real admission.
    if (process.env.COMPACT_PROBE_INHERITED === '1' && !await pi.setModel(model)) {
      throw new Error('Keyless native fixture model could not be selected');
    }
    receipt('delayed-startup-begin'); await new Promise(r=>setTimeout(r,700)); receipt('delayed-startup-finished');
  });
  pi.on('input', event => { receipt('input-preflight', {text:event.text}); });
  pi.on('tool_call', event => { if (event.toolCallId === 'item-X-call') receipt('X-start', {tool:event.toolName}); });
  pi.on('before_agent_start', (event, ctx) => { receipt('native-prompt', {prompt:event.prompt, model:ctx.model?.id, thinking:pi.getThinkingLevel()}); });
  pi.on('session_before_compact', () => { receipt('native-compaction-begin'); });
  pi.on('session_compact', () => { receipt('compaction-complete'); });
  pi.on('agent_settled', (event, ctx) => { receipt('native-settled', {event, signalPresent: Boolean(ctx.signal)}); });
}
