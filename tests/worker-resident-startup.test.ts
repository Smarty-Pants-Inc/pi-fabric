import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { rmTempSync } from "./fixtures/temp-cleanup.js";

const roots: string[] = [], managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) rmTempSync(root);
});
const directory = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-probe-")); roots.push(root); return root; };
describe.skipIf(!fs.existsSync("dist/worker.js"))("resident startup worker loaded-extension fence", () => {
  it.each(["ok", "missing", "nonce", "run", "extension", "protocol"])("requires the exact positive extension ACK: %s", async mode => {
    const root = directory();
    vi.stubEnv("RESIDENT_PROBE_SCENARIO", mode);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 15_000 }, {
      workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("tests/fixtures/resident-probe-pi.mjs"),
      fabricExtensionPath: path.resolve("dist/index.js"), runRoot: path.join(root, "runs"),
    }); managers.push(manager);
    const result = await manager.run({ task: "never infer", transport: "process", extensions: true, recursive: true, tools: [], residentStartupProbe: true });
    expect(result.status, result.error).toBe(mode === "ok" ? "completed" : "failed");
    if (mode === "ok") expect(result.text).toBe("resident worker startup verified");
    else expect(result.error).toMatch(/readiness|acknowledge/);
    const events = fs.readFileSync(path.join(root, "runs", result.id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    const argv = events.find(e => e.type === "probe_argv")?.args;
    expect(argv).toEqual(expect.arrayContaining(["--no-extensions", "--no-context-files"]));
    expect(events.filter(e => e.type === "probe_received").some(e => e.frame.type === "prompt")).toBe(false);
  }, 25_000);

  it("fails when real Pi swallows an explicit extension session_start exception", async () => {
    const root = directory(), profile = path.join(root, "profile"), extension = path.join(root, "broken.mjs");
    fs.mkdirSync(profile); fs.writeFileSync(path.join(profile, "settings.json"), "{}");
    fs.writeFileSync(extension, 'export default function(pi){ pi.on("session_start", () => { throw new Error("2615 explicit extension activation failure"); }); }');
    vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 20_000 }, {
      workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
      fabricExtensionPath: extension, runRoot: path.join(root, "runs"),
    }); managers.push(manager);
    const result = await manager.run({ task: "never infer", transport: "process", extensions: true, recursive: true, tools: [], residentStartupProbe: true });
    expect(result.status).toBe("failed");
    expect(result.error).toContain("did not acknowledge");
    const log = fs.readFileSync(path.join(root, "runs", result.id, "events.jsonl"), "utf8");
    expect(log).not.toContain('"type":"prompt"');
    expect(result.toolCalls).toBe(0);
  }, 30_000);
});
