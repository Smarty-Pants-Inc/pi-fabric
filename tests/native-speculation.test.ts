import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionContext, MessageUpdateEvent, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { nativeMcpIdentity } from "../src/core/native-mcp-identity.js";
import { speculativeNativeRef } from "../src/core/native-tool-names.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { RuntimeStateSpeculation } from "../src/runtime-state-speculation.js";
import { LiteralCallScanner } from "../src/speculation/scanner.js";

const cleanups: (()=>void)[]=[];
afterEach(()=>cleanups.splice(0).forEach(fn=>fn()));
function fixture(profile:"additive"|"native", mcp=false) {
  const cwd=mkdtempSync(join(tmpdir(),"fabric-native-spec-"));
  const path=join(cwd,"input.txt"); writeFileSync(path,"hello");
  const config=normalizeFabricConfig({executor:{codemodeProfile:profile},approvals:{read:"allow",write:"allow",network:"allow"},speculation:{mcpAllowlist:["docs.*"]}});
  const ref=mcp?"mcp.docs.read":"pi.read";
  const definition={name:"mcp__docs__read",label:"docs/read",description:"Read",parameters:{},namespace:{name:"mcp__docs"},exposure:"codemode"} as unknown as ToolDefinition;
  const definitions=mcp?[definition]:[];
  const context={cwd,hasUI:false,tools:definitions,sessionManager:{getBranch:()=>[],getSessionId:()=>"spec-native",getLeafId:()=>"root"}} as unknown as ExtensionContext;
  const registry=new ActionRegistry();
  const descriptor={name:mcp?"docs.read":"read",description:"Read",inputSchema:{type:"object"},risk:mcp?"network" as const:"read" as const,...(mcp?{namespace:"docs",annotations:{readOnlyHint:true}}:{effect:{kind:"none" as const}})};
  const invoke=vi.fn(async (_name, _args, ctx)=>ctx.nativeToolResult?{native:true}:"fabric");
  registry.register({name:mcp?"mcp":"pi",description:"Fixture",async list(){return [descriptor];},async describe(name){return name===descriptor.name?descriptor:undefined;},invoke});
  const mutation={name:"transition",description:"Write",inputSchema:{type:"object"},risk:"write" as const};
  registry.register({name:"state",description:"Write",async list(){return [mutation];},async describe(){return mutation;},async invoke(){return {};}});
  const service=new FabricExecutionService(registry,config);
  const speculation=new RuntimeStateSpeculation(registry,()=>config.speculation,()=>undefined,()=>config.approvals[mcp?"network":"read"]==="allow","typescript",()=>definitions,()=>profile==="native");
  speculation.tap!.setScannerFactory(()=>new LiteralCallScanner(name=>speculativeNativeRef(name,definitions,nativeMcpIdentity)));
  cleanups.push(()=>{speculation.reset();rmSync(cwd,{recursive:true,force:true});});
  const args=JSON.stringify({path});
  const native=`tools.${mcp?"mcp__docs__read":"read"}(${args})`;
  const ordinary=`${ref}(${args})`;
  const stream=(code:string)=>{
    for(const type of ["toolcall_start","toolcall_delta"] as const) speculation.tap!.handleMessageUpdate({assistantMessageEvent:{type,contentIndex:0,delta:JSON.stringify({code}).slice(0,-2),partial:{content:[{type:"toolCall",name:"fabric_exec",id:"stream"}]}}} as MessageUpdateEvent,context);
  };
  const run=(code:string)=>service.execute({code,context,signal:undefined,parentToolCallId:"stream",onPartial(){}});
  return {config,invoke,speculation,stream,run,native,ordinary,ref,path};
}

describe.each(["additive","native"] as const)("%s native sPTC",profile=>{
  it.each([false,true])("warms native aliases and separates result projections (MCP=%s)",async mcp=>{
    const f=fixture(profile,mcp);
    const code=`return await Promise.all([${f.ordinary},${f.native}]);`;
    f.stream(code);
    await vi.waitFor(()=>expect(f.invoke).toHaveBeenCalledTimes(2));
    const result=await f.run(code);
    expect(result.success,result.error??JSON.stringify(result.typeErrors)).toBe(true);
    expect(result.value).toEqual(["fabric",{native:true}]);
    expect(f.invoke).toHaveBeenCalledTimes(2);
    expect(result.audits.filter(a=>a.ref===f.ref).every(a=>a.speculated)).toBe(true);
  });
  it.each(["reset","mutation","freshness","error","deny"] as const)("rechecks native reads after %s",async mode=>{
    const f=fixture(profile);
    if(mode==="error") f.invoke.mockRejectedValueOnce(new Error("warm failure"));
    const code=`return await ${f.native};`;
    f.stream(code); await vi.waitFor(()=>expect(f.invoke).toHaveBeenCalledOnce());
    if(mode==="reset") f.speculation.reset();
    if(mode==="freshness") writeFileSync(f.path,"changed length");
    if(mode==="deny") f.config.approvals.read="deny";
    const result=await f.run((mode==="mutation"?"await state.transition({});":"")+code);
    expect(result.success).toBe(mode!=="deny");
    expect(f.invoke).toHaveBeenCalledTimes(mode==="deny"?1:2);
    expect(result.audits.find(a=>a.ref===f.ref)?.speculated).not.toBe(true);
  });
  it("does not serve a native warm result to the Fabric view",async()=>{
    const f=fixture(profile); f.stream(`return await ${f.native};`);
    await vi.waitFor(()=>expect(f.invoke).toHaveBeenCalledOnce());
    const result=await f.run(`return await ${f.ordinary};`);
    expect(result.value).toBe("fabric"); expect(f.invoke).toHaveBeenCalledTimes(2);
    expect(result.audits.find(a=>a.ref===f.ref)?.speculated).not.toBe(true);
  });
});

it.each(['const tools = {}; tools.read({path:"x"});','const alias = nativeTools; nativeTools.read({path:"x"});','tools.read = () => {}; tools.read({path:"x"});','tools.read({path: dynamic});','nativeTools.bash({command:"true"});'])("keeps unsafe/ambiguous native candidates conservative: %s",code=>{
  const calls=new LiteralCallScanner().push(code);
  if(code.includes("bash")) expect(calls.every(c=>c.ref==="pi.bash")).toBe(true);
  else expect(calls).toEqual([]);
});
