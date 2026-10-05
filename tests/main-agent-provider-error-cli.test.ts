import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";

type RpcEvent = Parameters<Parameters<RpcClient["onEvent"]>[0]>[0];
const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = path.join(repo, "dist/index.js");
const cli = process.env.FABRIC_4012_PI_CLI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check: () => boolean, timeout = 20_000) => {
  const deadline = Date.now() + timeout;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("provider recovery did not settle before deadline");
    await sleep(25);
  }
};

// Artifact-backed native RPC proof, no provider credentials or remote transport.
describe.skipIf(!fs.existsSync(entry))("Main provider error recovery (#4012)", () => {
  it.each(["FAIL_ONCE", "FAIL_TWICE", "ABORT", "NATIVE_RETRY", "OVERFLOW", "RESET", "COMPACT_FAIL_ONCE", "COMPACT_FAIL_TWICE"] as const)("bounds %s recovery and parent reporting", async scenario => {
    const delayedCompaction = scenario.startsWith("COMPACT_");
    const failureTwice = scenario.endsWith("FAIL_TWICE");
    const recoverable = ["FAIL_ONCE", "FAIL_TWICE", "RESET"].includes(scenario) || delayedCompaction;
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-provider-error-4012-"));
    const clients: RpcClient[] = [];
    const events: Record<string, RpcEvent[]> = { parent: [], lane: [] };
    const make = (name: string, lead?: string) => {
      const cwd = path.join(root, name);
      const agentDir = path.join(cwd, "agent");
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
        retry: { enabled: scenario === "NATIVE_RETRY", maxRetries: 1, baseDelayMs: 10 }, compaction: { enabled: false, keepRecentTokens: delayedCompaction ? 1 : 16_384 },
      }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
        executor: { kernel: "typescript" }, fullCodeMode: false,
        mesh: { enabled: true, announce: true, followUpFlushMs: 120_000 },
        mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
        compaction: { engine: "pi", ...(delayedCompaction && name === "lane"
          ? { tokenThresholds: { "provider-error-4012/faux-1": 6_000 } } : {}) }, entropy: { compile: false }, speculation: { enabled: false },
      }));
      const client = new RpcClient({ cliPath: cli, cwd, provider: "provider-error-4012", model: "faux-1",
        env: {
          PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"),
          PI_FABRIC_MESH_ROOT: path.join(root, "mesh"), PI_FABRIC_PROJECT_ROOT: cwd,
          PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), FABRIC_4012_PROBE_DIR: cwd,
          HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1",
          ...(lead ? { SMARTY_LEAD_SESSION: lead } : {}),
        },
        args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
          "--approve", "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(cwd, "sessions"),
          "-e", entry, "-e", path.join(repo, "tests/fixtures/main-provider-error-4012.ts")],
      });
      client.onEvent(event => events[name]!.push(event));
      clients.push(client);
      return client;
    };
    const starts = () => events.lane!.filter(event => event.type === "agent_start").length;
    const settled = () => events.lane!.filter(event => event.type === "agent_settled").length;
    const attempts = () => fs.readFileSync(path.join(root, "lane", "attempts.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    try {
      const parent = make("parent");
      await parent.start();
      await wait(() => fs.existsSync(path.join(root, "parent", "ready.json")));
      await parent.promptAndWait("WARM", undefined, 30_000);
      const lead = JSON.parse(fs.readFileSync(path.join(root, "parent", "ready.json"), "utf8")).id;
      const lane = make("lane", lead);
      await lane.start();
      await wait(() => fs.existsSync(path.join(root, "lane", "ready.json")));
      // A prior turn makes native compaction meaningful; the failing turn's
      // large input crosses Fabric's threshold, with Pi auto-compaction off.
      if (delayedCompaction) {
        await lane.promptAndWait("WARM", undefined, 30_000);
        expect(events.lane!.some(event => event.type === "compaction_start")).toBe(false);
      }
      await lane.prompt(delayedCompaction ? `${scenario}\n${"history ".repeat(2_000)}` : scenario === "RESET" ? "FAIL_ONCE" : scenario);
      if (scenario === "ABORT") {
        await wait(() => events.lane!.some(event => event.type === "message_update" && event.assistantMessageEvent.type === "text_delta"));
        await lane.abort();
      }
      const expectedSettles = (recoverable ? 2 : 1) + (delayedCompaction ? 1 : 0);
      await wait(() => settled() >= expectedSettles);
      if (scenario === "RESET") {
        await lane.prompt("FAIL_ONCE");
        await wait(() => settled() === 4);
      }
      await sleep(1_500); // A third attempt would already have fired at the 1s backoff.
      const messages = await lane.getMessages();
      const blocked = messages.filter(message => message.role === "custom" && message.customType === "pi-fabric-provider-blocked");
      const retries = messages.filter(message => message.role === "custom" && message.customType === "pi-fabric-provider-retry");
      const reports = (await parent.getMessages()).filter(message => message.role === "custom" && message.customType === "pi-fabric-agent-message" && JSON.stringify(message).includes("BLOCKED: provider error:"));
      const fabricRetries = scenario === "RESET" ? 2 : recoverable ? 1 : 0;
      if (scenario === "NATIVE_RETRY") {
        expect(events.lane!.filter(event => event.type === "auto_retry_start")).toHaveLength(1);
        expect(events.lane!.filter(event => event.type === "auto_retry_end")).toMatchObject([{ success: true }]);
      }
      expect(retries).toHaveLength(fabricRetries);
      expect(blocked).toHaveLength(failureTwice ? 1 : 0);
      expect(reports).toHaveLength(failureTwice ? 1 : 0);
      expect(starts()).toBe(scenario === "NATIVE_RETRY" ? 2 : scenario === "RESET" ? 4 : expectedSettles);
      expect(attempts()).toHaveLength(scenario === "RESET" ? 4 : recoverable || scenario === "NATIVE_RETRY" ? 2 : 1);
      expect(await lane.getState()).toMatchObject({ isStreaming: false });
      if (delayedCompaction) {
        const compactions = fs.readFileSync(path.join(root, "lane", "compactions.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
        const start = compactions.find(record => record.phase === "start")!;
        const backoff = compactions.find(record => record.phase === "backoff")!;
        const complete = compactions.find(record => record.phase === "complete")!;
        expect(backoff).toMatchObject({ idle: false });
        expect(backoff.at - start.at).toBeGreaterThan(1_000);
        expect(complete.at - start.at).toBeGreaterThan(1_500);
        expect(attempts()[1].at).toBeGreaterThanOrEqual(complete.at);
        expect(messages.some(message => message.role === "compactionSummary")).toBe(true);
      }
      if (failureTwice) {
        expect(blocked[0]).toMatchObject({ content: "BLOCKED: provider error: stream error: stream disconnected before completion", display: true });
        expect(JSON.stringify(reports[0])).not.toContain("second diagnostic line");
      } else if (scenario === "ABORT") {
        expect(messages.at(-1)).toMatchObject({ role: "assistant", stopReason: "aborted" });
      } else if (scenario !== "OVERFLOW") {
        expect(await lane.getLastAssistantText()).toBe("recovered turn");
      }
    } finally {
      const evidence = process.env.FABRIC_4012_EVIDENCE_DIR && path.join(process.env.FABRIC_4012_EVIDENCE_DIR, scenario);
      if (evidence) {
        fs.mkdirSync(evidence, { recursive: true });
        fs.writeFileSync(path.join(evidence, "rpc-events.json"), JSON.stringify(events, null, 2));
        for (const [i, name] of ["parent", "lane"].entries()) {
          fs.writeFileSync(path.join(evidence, `${name}-stderr.log`), clients[i]?.getStderr() ?? "");
          const readyFile = path.join(root, name, "ready.json");
          if (fs.existsSync(readyFile)) {
            const ready = JSON.parse(fs.readFileSync(readyFile, "utf8"));
            if (fs.existsSync(ready.sessionFile)) fs.copyFileSync(ready.sessionFile, path.join(evidence, `${name}-session.jsonl`));
          }
        }
        const attemptsFile = path.join(root, "lane", "attempts.jsonl");
        if (fs.existsSync(attemptsFile)) fs.copyFileSync(attemptsFile, path.join(evidence, "attempts.jsonl"));
        const compactionsFile = path.join(root, "lane", "compactions.jsonl");
        if (fs.existsSync(compactionsFile)) fs.copyFileSync(compactionsFile, path.join(evidence, "compactions.jsonl"));
      }
      await Promise.all(clients.map(async client => { await client.abort().catch(() => undefined); await client.stop(); }));
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
