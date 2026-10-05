import { describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ loads: 0, launches: 0 }));
vi.mock("../src/agents/transports/placement.js", () => {
  calls.loads++;
  return { launchPlacedTask: async () => {
    calls.launches++;
    return { kind: "process", isAlive: async () => false, stop: async () => {} };
  } };
});
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { normalizeAgentPlacement } from "../src/agents/placement-config.js";

describe("placement import/registration/idle boundary", () => {
  it("does not load or launch the optional adapter until first eligible use", async () => {
    const placement=normalizeAgentPlacement({default:"remote",command:["fake","{id}"],resultDirectory:"/unused/{id}",cancelCommand:["fake","--cancel","{id}"]})!;
    const transport=new ProcessTransport(undefined,placement);
    expect(await transport.available()).toBe(true);
    await new Promise(resolve=>setTimeout(resolve,10));
    expect(calls).toEqual({loads:0,launches:0});
    const handle=await transport.launch({id:"first-use",name:"probe",cwd:"/unused",workerPath:"/unused",workerArguments:[]});
    expect(calls).toEqual({loads:1,launches:1});
    expect(handle.kind).toBe("process");
    await handle.stop();
  });
});
