import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { resolveFabricIdentity } from "../src/main-agent.js";

const workerPath = path.resolve("dist/worker.js");
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const probe = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-spawner-worker-"));
  roots.push(root);
  const piBinary = path.join(root, "probe-pi.mjs");
  fs.writeFileSync(piBinary, `
    process.stdin.resume();
    const keys = ['PI_FABRIC_ACTOR_ID','PI_FABRIC_ACTOR_NAME','PI_FABRIC_ACTOR_SESSION_FILE',
      'PI_FABRIC_PARENT_RUN','PI_FABRIC_MAIN_AGENT_ID','PI_FABRIC_SPAWNER_ID',
      'PI_FABRIC_SPAWNER_KIND','PI_FABRIC_SPAWNER_RUN','PI_FABRIC_REPLY_SCHEMA_FILE',
      'PI_FABRIC_REPLY_FILE','PI_FABRIC_REPLY_HOOK','SMARTY_ROLE','SMARTY_AGENT_NAME'];
    const report = Object.fromEntries(keys.map(key => [key, process.env[key] || '']));
    console.log(JSON.stringify({type:'message_end',message:{role:'assistant',content:JSON.stringify(report)}}));
    console.log(JSON.stringify({type:'agent_settled'}));
    process.exit(0);
  `);
  const manager = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath, piBinary, runRoot: path.join(root, "runs"),
    mainAgentId: "session:root-main",
    ...(process.env.PI_FABRIC_ACTOR_ID ? { identityId: process.env.PI_FABRIC_ACTOR_ID } : {}),
  });
  managers.push(manager);
  vi.stubEnv("SMARTY_AGENT_NAME", "parent-lead");
  const result = await manager.run({ task: "report synthetic binding", transport: "process" });
  expect(process.env.SMARTY_AGENT_NAME).toBe("parent-lead");
  expect(result.status, result.error).toBe("completed");
  const environment = JSON.parse(result.text) as NodeJS.ProcessEnv;
  expect(environment.SMARTY_AGENT_NAME).toBe("");
  return { result, environment };
};

describe.skipIf(!fs.existsSync(workerPath))("#2643 actual worker spawner environment", () => {
  it.each(["session", "durable"] as const)("keeps a %s actor child's own identity while binding replies to the actor + run", async (residency) => {
    const actorId = "b".repeat(32);
    const actorRun = "a".repeat(32);
    vi.stubEnv("PI_FABRIC_ACTOR_ID", actorId);
    vi.stubEnv("PI_FABRIC_ACTOR_NAME", `${residency}-reviewer`);
    vi.stubEnv("PI_FABRIC_ACTOR_SESSION_FILE", "/synthetic/actor-session.jsonl");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", actorRun);
    vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "session:root-main");
    vi.stubEnv("PI_FABRIC_REPLY_SCHEMA_FILE", "/synthetic/actor-reply-schema");
    vi.stubEnv("PI_FABRIC_REPLY_FILE", "/synthetic/actor-reply");
    vi.stubEnv("PI_FABRIC_REPLY_HOOK", "/synthetic/actor-reply-hook");
    const { result, environment } = await probe();
    expect(result.spawner).toEqual({ id: actorId, kind: "actor", runId: actorRun });
    expect(environment).toMatchObject({
      PI_FABRIC_ACTOR_ID: "", PI_FABRIC_ACTOR_NAME: "", PI_FABRIC_ACTOR_SESSION_FILE: "",
      PI_FABRIC_PARENT_RUN: result.id, PI_FABRIC_MAIN_AGENT_ID: "session:root-main",
      PI_FABRIC_SPAWNER_ID: actorId, PI_FABRIC_SPAWNER_KIND: "actor", PI_FABRIC_SPAWNER_RUN: actorRun,
      PI_FABRIC_REPLY_SCHEMA_FILE: "", PI_FABRIC_REPLY_FILE: "", PI_FABRIC_REPLY_HOOK: "",
      SMARTY_ROLE: "task-agent",
    });
    expect(resolveFabricIdentity("child-session", environment)).toMatchObject({
      identity: { id: result.id, kind: "agent" }, mainAgentId: "session:root-main",
    });
  });

  it("binds a Main child to Main and a task child's child to that task run", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
    const main = await probe();
    expect(main.result.spawner).toEqual({ id: "session:root-main", kind: "main" });
    vi.stubEnv("PI_FABRIC_PARENT_RUN", main.result.id);
    const nested = await probe();
    expect(nested.result.spawner).toEqual({ id: main.result.id, kind: "agent", runId: main.result.id });
    expect(nested.environment.PI_FABRIC_SPAWNER_ID).toBe(main.result.id);
    expect(nested.environment.PI_FABRIC_MAIN_AGENT_ID).toBe("session:root-main");
  });
});
