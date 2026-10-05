// Only model inference and summary text are synthetic; native Pi owns every lifecycle event.
import fs from 'node:fs';
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from '@earendil-works/pi-ai';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
export default function(pi: ExtensionAPI) {
  const receipt = (type: string, data = {}) => fs.appendFileSync(process.env.COMPACT_PROBE_TRANSCRIPT!, JSON.stringify({type, at: Date.now(), ...data}) + '\n');
  const model: Model<Api> = {provider:'compact-proof', id:'offline', name:'Keyless compact proof', api:'compact-proof-api', baseUrl:'http://invalid.local', reasoning:false, input:['text'], cost:{input:0, output:0, cacheRead:0, cacheWrite:0}, contextWindow:200000, maxTokens:4096};
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const output = createAssistantMessageEventStream();
    const user = [...context.messages].reverse().find(m => m.role === 'user');
    const prompt = JSON.stringify(user);
    const summary = /conversation to summarize|NEW conversation messages|PREFIX of a turn/.test(prompt);
    const resumed = !summary && prompt.includes('Resume after compaction:');
    const tool = !summary && context.messages.at(-1)?.role !== 'toolResult' && !prompt.includes('already admitted');
    if (resumed && tool) receipt('X-selected', {prompt});
    const code = resumed ? 'await pi.write({ path: "item-X.txt", text: "X completed" }); return "X completed";'
      : 'await compact.request({ instructions: "Keep the failing test name in the summary" }); return "compaction requested";';
    const message: AssistantMessage = {role:'assistant', provider:selected.provider, model:selected.id, api:selected.api, timestamp:Date.now(), content:tool ? [{type:'toolCall', id:resumed ? 'item-X-call' : 'compact-proof-call', name:'fabric_exec', arguments:{code}}] : [{type:'text', text:summary ? 'Compacted. Preserve failing test test_X. Pending work: start item X only if requested.' : resumed ? 'X completed' : 'Compaction requested; idle unless continuation is admitted.'}], stopReason:tool ? 'toolUse' : 'stop', usage:{input:0, output:0, cacheRead:0, cacheWrite:0, totalTokens:0, cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
    output.push({type:'start', partial:message});
    if(tool) output.push({type:'toolcall_end', contentIndex:0, toolCall:message.content[0] as Extract<AssistantMessage['content'][number],{type:'toolCall'}>, partial:message});
    output.push({type:'done', reason:message.stopReason as 'stop'|'toolUse', message}); output.end(); return output;
  };
  pi.registerProvider({id:model.provider, name:model.name, auth:{apiKey:{name:'Keyless local test', check:async()=>({type:'api_key',source:'fixture'}), resolve:async()=>({auth:{}})}}, getModels:()=>[model], stream, streamSimple:stream});
  pi.on('session_start', async () => { receipt('delayed-startup-begin'); await new Promise(r=>setTimeout(r,700)); receipt('delayed-startup-finished'); });
  pi.on('input', event => { receipt('input-preflight', {text:event.text}); });
  pi.on('tool_call', event => { if (event.toolCallId === 'item-X-call') receipt('X-start', {tool:event.toolName}); });
  pi.on('before_agent_start', (event, ctx) => { receipt('native-prompt', {prompt:event.prompt, model:ctx.model?.id, thinking:pi.getThinkingLevel()}); });
  pi.on('session_before_compact', () => { receipt('native-compaction-begin'); });
  pi.on('session_compact', () => { receipt('compaction-complete'); });
  pi.on('agent_settled', () => { receipt('native-settled'); });
}
