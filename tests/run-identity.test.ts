import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { writePolicyDenial } from "../src/agents/write-guard.js";

// Trust-boundary regression: production manager -> compiled worker -> execution child.
// Only the model binary is replaced. No broker, credentials or external writes.
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});

describe("run identity across Main and residency owners", () => {
  it.each(["main", "residency"] as const)("exports the manager's receipt for %s actor and task runs", async kind => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "run-identity-")));
    const cwd = path.join(root, "work");
    const mesh = path.join(root, "mesh");
    const scratch = path.join(root, "scratch");
    for (const directory of [cwd, mesh, scratch]) fs.mkdirSync(directory, { mode: 0o700 });
    const main = "session:run-identity-main";
    const digest = createHash("sha256").update(main).digest("hex");
    const runRoot = kind === "main" ? path.join(root, "pi-fabric-runs-main") : path.join(mesh, "residency", digest, "runs");
    const binary = path.join(root, "pi.mjs");
    fs.writeFileSync(binary, `#!/usr/bin/env node
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
process.env.PI_SESSION_ID = 'run-identity-native-session';
const emit = event => process.stdout.write(JSON.stringify(event)+'\\n');
emit({type:'fabric_runner_session',runId:process.env.PI_FABRIC_PARENT_RUN,sessionId:process.env.PI_SESSION_ID});
let buffer='';
process.stdin.on('data', chunk => {
  buffer += chunk;
  while (buffer.includes('\\n')) {
    const end=buffer.indexOf('\\n'), frame=JSON.parse(buffer.slice(0,end)); buffer=buffer.slice(end+1);
    if (frame.type!=='prompt') continue;
    const file=process.env.PI_FABRIC_RUN_IDENTITY;
    const identity=JSON.parse(fs.readFileSync(file,'utf8'));
    const status=JSON.parse(fs.readFileSync(identity.runDirectory+'/status.json','utf8'));
    const info=fs.lstatSync(file);
    let consumer=null;
    if (process.env.FABRIC_RUN_IDENTITY_CONSUMER) {
      const checked=spawnSync('python3',['-c',
        'import importlib.machinery,importlib.util,os; from pathlib import Path; '+
        'loader=importlib.machinery.SourceFileLoader("review_identity",os.environ["FABRIC_RUN_IDENTITY_CONSUMER"]); '+
        'module=importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name,loader)); '+
        'loader.exec_module(module); module._publish_run_identity(Path(os.environ["TMPDIR"]),Path(os.environ["PI_FABRIC_MESH_ROOT"]))'
      ],{encoding:'utf8'});
      consumer={code:checked.status,error:checked.stderr};
    }
    const text=JSON.stringify({file,identity,status,consumer,mode:info.mode&511,uid:info.uid,links:info.nlink,
      run:process.env.PI_FABRIC_AGENT_RUN_DIR, id:process.env.PI_FABRIC_PARENT_RUN,
      policy:JSON.parse(process.env.PI_FABRIC_WRITE_POLICY),tmp:process.env.TMPDIR});
    emit({type:'agent_start'});
    emit({type:'message_end',message:{role:'assistant',content:[{type:'text',text}],stopReason:'stop'}});
    emit({type:'agent_settled'});
  }
});
process.stdin.on('end',()=>process.exit(0));
`, { mode: 0o700 });
    vi.stubEnv("PI_FABRIC_RUN_IDENTITY", "/inherited/other-run/run-identity.json");
    const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0,
      timeoutMs: 60_000, retainRuns: false, sessionExport: false, nice: 19 }, {
      workerPath: path.resolve("dist/worker.js"), piBinary: binary, runRoot,
      mainAgentId: main, identityId: main, hostId: kind === "residency" ? `resident:${digest.slice(0, 24)}` : main,
      meshRoot: mesh, fullCodeMode: false,
      shortTmpdirSource: () => ({ tmpdir: scratch, budgetBytes: 4096 }),
    });
    managers.push(manager);
    for (const actorId of ["review-contract", undefined]) {
      const result = await manager.run({ task: "Read native run identity", transport: "process", extensions: false,
        ...(actorId ? { actorId } : {}) });
      expect(result.status, result.error).toBe("completed");
      const report = JSON.parse(result.text);
      if (process.env.FABRIC_RUN_IDENTITY_CONSUMER) expect(report.consumer, report.consumer?.error).toMatchObject({ code: 0 });
      const run = manager.runDirectory(result.id)!;
      expect(report.file).toBe(path.join(run, "run-identity.json"));
      expect(report.run).toBe(run);
      expect(report.id).toBe(result.id);
      expect(report.identity).toMatchObject({ format: 1, runId: result.id, runDirectory: run,
        runRoot, mainAgentId: main, meshRoot: mesh,
        owner: { kind, id: kind === "residency" ? `resident:${digest.slice(0, 24)}` : main },
        supervisor: { pid: process.pid, ...(process.platform === "linux" ? { processStartTime: processStartTime(process.pid) } : {}) },
      });
      expect(report.identity.actorId).toBe(actorId);
      expect(report.identity.createdAt).toBeGreaterThan(0);
      expect(report.status).toMatchObject({ id: result.id, status: "running", mainAgentId: main });
      expect(report.identity.temporaryDirectory).toBe(report.tmp);
      expect(report.policy.writableRoots).toContain(report.tmp);
      expect(report.uid).toBe(process.getuid?.() ?? report.uid);
      expect(report.links).toBe(1);
      if (process.platform !== "win32") expect(report.mode).toBe(0o600);
      for (const tool of ["write", "edit"]) {
        expect(writePolicyDenial(report.policy, tool, { path: report.file }, cwd)).toContain("outside writable roots");
      }
      // New metadata must not permanently veto the existing run cleanup path.
      await manager.cleanup(result.id);
      expect(fs.existsSync(run)).toBe(false);
    }
  }, 180_000);
});
