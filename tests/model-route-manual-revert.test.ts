import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

let root: string;
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "manual-route-revert-")); });
afterEach(() => { fs.rmSync(root, { recursive: true, force: true }); });

const fresh = (liveClasses: string[]) => {
  const probe = spawnSync("bun", ["-e", `
    import { prepareModelRoute } from ${JSON.stringify(path.resolve("src/agents/model-route-prepare.ts"))};
    const decision = await prepareModelRoute({routeClass:"status-groom",protected:false,pinModel:"test/sol",pinThinking:"high",parentSessionId:"fresh-main",
      registry:{getAvailable:()=>[{provider:"test",id:"sol"},{provider:"test",id:"luna"}]},aliases:{},
      config:{liveClasses:${JSON.stringify(liveClasses)},shadowCandidates:[{model:"test/luna",effort:"medium"}]},assertModelAllowed(){},
      evaluate:async()=>({model:"jev",answers:{route:{type:"choice",choice:"candidate-1",confidence:.95,probabilities:{"candidate-0":.05,"candidate-1":.95}}},usage:{input_tokens:1,output_tokens:1}})});
    console.log(JSON.stringify(decision));`], { encoding: "utf8", env: { ...process.env, PI_CODING_AGENT_DIR: root }, timeout: 30000 });
  expect(probe.status, probe.stderr).toBe(0);
  return JSON.parse(probe.stdout.trim());
};

describe("manual revert in a fresh owner", () => {
  it("uses only trusted class opt-in, without reading a historical quality journal", () => {
    fs.mkdirSync(path.join(root, "fabric"), { mode: 0o700 });
    // Quality reporting/recovery is not part of this release. Even a broken old
    // quality-only sink cannot control LIVE admission or manual rollback.
    fs.writeFileSync(path.join(root, "fabric/model-routing-quality.jsonl"), "not-json\n", { mode: 0o600 });
    expect(fresh(["status-groom"])).toMatchObject({ model: "test/luna", effort: "medium", mode: "live", reasonCode: "live-choice" });
    expect(fresh([])).toMatchObject({ mode: "shadow", reasonCode: "shadow-choice", pin: { model: "test/sol", effort: "high" } });
    expect(fresh(["status-groom"])).toMatchObject({ mode: "live", reasonCode: "live-choice" });
  });
});
