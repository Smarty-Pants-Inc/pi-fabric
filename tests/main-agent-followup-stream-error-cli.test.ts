import { createHash } from "node:crypto";
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

// Explicit artifact proof: build first, then FABRIC_4012_REAL_CLI=1 bunx vitest run <this file>.
// No remote bridge, credentials, agent spawning, or mocked transport.
describe.runIf(process.env.FABRIC_4012_REAL_CLI === "1")("native RPC followUp after a model stream error (#4012)", () => {
  it.each(["followUp", "mailbox"] as const)("processes later %s delivery without another receiver prompt", async (delivery) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stream-error-4012-"));
    const clients: RpcClient[] = [];
    const events: Record<string, RpcEvent[]> = { receiver: [], sender: [] };
    const states: Record<string, unknown> = {};
    const make = (name: string) => {
      const cwd = path.join(root, name);
      const agentDir = path.join(cwd, "agent");
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
        retry: { enabled: false }, compaction: { enabled: false },
      }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
        executor: { kernel: "typescript" }, fullCodeMode: false,
        mesh: { enabled: true, announce: true, followUpFlushMs: 120_000 },
        mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
        compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
      }));
      const client = new RpcClient({ cliPath: cli, cwd, provider: "stream-error-4012", model: "faux-1",
        env: {
          PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"),
          PI_FABRIC_MESH_ROOT: path.join(root, "mesh"), PI_FABRIC_PROJECT_ROOT: cwd,
          PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), FABRIC_4012_PROBE_DIR: cwd,
          HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1", PI_FABRIC_INBOX_WAKE_MS: "100",
        },
        args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
          "--approve", "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(cwd, "sessions"),
          "-e", entry, "-e", path.join(repo, "tests/fixtures/followup-stream-error-4012.ts")],
      });
      client.onEvent(event => events[name]!.push(event));
      clients.push(client);
      return client;
    };
    const wait = async (check: () => boolean, timeout = 15_000) => {
      const deadline = Date.now() + timeout;
      while (!check()) {
        if (Date.now() > deadline) throw new Error("native followUp not processed before deadline");
        await new Promise(resolve => setTimeout(resolve, 25));
      }
    };
    try {
      const receiver = make("receiver");
      const sender = make("sender");
      await Promise.all(clients.map(client => client.start()));
      await wait(() => ["receiver", "sender"].every(name => fs.existsSync(path.join(root, name, "ready.json"))));
      const ready = JSON.parse(fs.readFileSync(path.join(root, "receiver", "ready.json"), "utf8"));
      const initial = await receiver.promptAndWait("FAIL_STREAM", undefined, 30_000);
      expect(initial.some(event => event.type === "message_update" &&
        event.assistantMessageEvent.type === "text_delta" && event.assistantMessageEvent.delta.includes("partial"))).toBe(true);
      expect(initial.some(event => event.type === "message_end" && event.message.role === "assistant" && event.message.stopReason === "error")).toBe(true);
      states.afterError = await receiver.getState();
      expect(states.afterError).toMatchObject({ isStreaming: false });
      // The bounded automatic recovery is separate from the later addressed mailbox wake.
      await wait(() => events.receiver!.filter(event => event.type === "agent_settled").length === 2);
      const sent = await sender.promptAndWait(`${delivery === "followUp" ? "SEND" : "MAILBOX"} ${JSON.stringify({ id: ready.id, message: "WAKE_4012 after disconnect" })}`, undefined, 30_000);
      const receipt = sent.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
      expect(receipt).toMatchObject({ isError: false });
      if (delivery === "followUp") expect(JSON.stringify(receipt)).toContain('"acknowledged":true');
      else expect(JSON.stringify(receipt)).toContain('"topic":"fleet.work.pi-fabric.4012"');
      states.afterAcknowledgment = await receiver.getState();
      await wait(() => events.receiver!.filter(event => event.type === "agent_settled").length === 3, 70_000);
      expect(events.receiver!.filter(event => event.type === "agent_start")).toHaveLength(3);
      expect(await receiver.getLastAssistantText()).toBe("later followUp processed");
      const messages = await receiver.getMessages();
      expect(messages.filter(message => message.role === "custom" && message.customType === (delivery === "followUp" ? "pi-fabric-agent-message" : "pi-fabric-inbox"))).toHaveLength(1);
      expect(fs.readFileSync(path.join(root, "receiver", "provider-context.jsonl"), "utf8")).toContain("WAKE_4012 after disconnect");
      expect(fs.readFileSync(ready.sessionFile, "utf8")).toContain("WAKE_4012 after disconnect");
      states.afterProcessing = await receiver.getState();
    } finally {
      // Capture evidence before native shutdown; no child may outlive this test.
      for (const [i, name] of ["receiver", "sender"].entries()) {
        if (clients[i]) {
          states[`${name}Entries`] = await clients[i]!.getEntries().catch(() => undefined);
          states[`${name}Final`] = await clients[i]!.getState().catch(() => undefined);
        }
      }
      await Promise.all(clients.map(async client => { await client.abort().catch(() => undefined); await client.stop(); }));
      const dest = process.env.FABRIC_4012_EVIDENCE_DIR && path.join(process.env.FABRIC_4012_EVIDENCE_DIR, delivery);
      if (dest) {
        fs.mkdirSync(dest, { recursive: true });
        fs.writeFileSync(path.join(dest, "rpc-events.json"), JSON.stringify(events, null, 2));
        fs.writeFileSync(path.join(dest, "states-and-entries.json"), JSON.stringify(states, null, 2));
        fs.writeFileSync(path.join(dest, "artifact.json"), JSON.stringify({ cli: fs.realpathSync(cli), entry,
          sha256: createHash("sha256").update(fs.readFileSync(entry)).digest("hex") }, null, 2));
        for (const [i, name] of ["receiver", "sender"].entries()) {
          fs.writeFileSync(path.join(dest, `${name}-stderr.log`), clients[i]?.getStderr() ?? "");
          const readyFile = path.join(root, name, "ready.json");
          if (fs.existsSync(readyFile)) {
            const ready = JSON.parse(fs.readFileSync(readyFile, "utf8"));
            if (fs.existsSync(ready.sessionFile)) fs.copyFileSync(ready.sessionFile, path.join(dest, `${name}-session.jsonl`));
          }
        }
        const context = path.join(root, "receiver", "provider-context.jsonl");
        if (fs.existsSync(context)) fs.copyFileSync(context, path.join(dest, "provider-context.jsonl"));
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 130_000);
});
