import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";

const repo = fileURLToPath(new URL("../", import.meta.url));
type Event = Parameters<Parameters<RpcClient["onEvent"]>[0]>[0];
// Explicit compiled real-host probe; no agent spawning or external model/credentials.
describe.runIf(process.platform === "linux" && process.env.FABRIC_7452_REAL_CLI === "1")("compiled fabric_exec interrupt-priority send (#7452)", () => {
  it.each(["bash", "fabric_exec"] as const)("preempts receiver %s through agents.send over the real mesh and starts HOLD next", async tool => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-interrupt-cli-"));
    const clients: RpcClient[] = [];
    const events: Record<string, Event[]> = { receiver: [], sender: [] };
    let run: Promise<void> | undefined;
    const make = (name: string, interruptFrom: string[] = []) => {
      const cwd = path.join(root, name);
      const agentDir = path.join(cwd, "agent");
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ executor: { kernel: "typescript" }, fullCodeMode: name === "receiver" && tool === "fabric_exec",
        mesh: { enabled: true, announce: true, followUpFlushMs: 10 },
        agents: { interruptFrom },
        mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
        compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
      }));
      const client = new RpcClient({ cliPath: path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), cwd,
        provider: "interrupt-7452", model: "faux-1", env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"),
          PI_FABRIC_MESH_ROOT: path.join(root, "mesh"), PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"),
          FABRIC_7452_GATE_DIR: cwd, HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1",
          PI_FABRIC_TASK_PROCESS_CHILD: "", PI_FABRIC_PARENT_RUN: "", PI_FABRIC_SPAWNER_ID: "", PI_FABRIC_MAIN_AGENT_ID: "", PI_FABRIC_ACTOR_ID: "",
        }, args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve", "--thinking", "off",
          "--session-dir", path.join(cwd, "sessions"), "-e", path.join(repo, "dist/index.js"), "-e", path.join(repo, "tests/fixtures/interrupt-cli-7452.ts")],
      });
      client.onEvent(event => events[name]!.push(event));
      clients.push(client);
      return client;
    };
    const exists = (name: string, file: string) => fs.existsSync(path.join(root, name, file));
    const wait = async (check: () => boolean) => vi.waitFor(() => expect(check()).toBe(true), { timeout: 15_000, interval: 10 });
    try {
      const sender = make("sender");
      await sender.start();
      await wait(() => exists("sender", "ready.json"));
      const senderReady = JSON.parse(fs.readFileSync(path.join(root, "sender", "ready.json"), "utf8"));
      const receiver = make("receiver", [senderReady.id]);
      await receiver.start();
      await wait(() => exists("receiver", "ready.json"));
      const ready = JSON.parse(fs.readFileSync(path.join(root, "receiver", "ready.json"), "utf8"));
      run = receiver.prompt(`WORK ${tool}`);
      void run.catch(() => undefined);
      await wait(() => exists("receiver", "entered"));
      // RPC prompt acknowledges admission, not completion. Join sender settlement so
      // cleanup cannot cancel agents.send before its owner acknowledgement arrives.
      await sender.promptAndWait(`SEND ${JSON.stringify({ id: ready.id, message: "HOLD: do not start the change", priority: "interrupt" })}`, undefined, 15_000);
      await wait(() => events.receiver!.filter(event => event.type === "agent_end").length === 2);
      await run;
      await wait(() => exists("receiver", "abort.json"));
      const aborted = JSON.parse(fs.readFileSync(path.join(root, "receiver", "abort.json"), "utf8"));
      expect(aborted.exited).toBe(true);
      expect(aborted.nativeAbortToProcessExitMs).toBeLessThan(1000);
      expect(exists("receiver", "completed")).toBe(false);
      const messages = await receiver.getMessages();
      const hold = messages.filter(message => message.role === "custom" && message.customType === "pi-fabric-agent-message");
      expect(hold).toHaveLength(1);
      expect(JSON.stringify(hold[0])).toContain("HOLD: do not start the change");
      expect(events.receiver!.filter(event => event.type === "tool_execution_start" && event.toolName === tool)).toHaveLength(1);
      const ended = events.receiver!.find(event => event.type === "tool_execution_end" && event.toolName === tool)!;
      expect((ended as { isError: boolean }).isError).toBe(true);
      expect(events.receiver!.filter(event => event.type === "agent_start")).toHaveLength(2);
      const context = fs.readFileSync(path.join(root, "receiver", "provider-context.jsonl"), "utf8");
      expect(context).toContain("HOLD: do not start the change");
      expect(JSON.parse(context.trim().split("\n").at(-1)!).at(-1).role).toBe("user");
      const persisted = fs.readFileSync(ready.sessionFile, "utf8");
      expect(persisted).toContain("HOLD: do not start the change");
      await wait(() => events.sender!.some(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec"));
      const ack = events.sender!.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec")!;
      expect(JSON.stringify(ack)).toContain('"acknowledged":true');
      expect(JSON.stringify(ack)).toContain('"ref":"agents.send"');
    } finally {
      await Promise.all(clients.map(client => client.abort().catch(() => undefined)));
      await run?.catch(() => undefined);
      await Promise.all(clients.map(client => client.stop()));
      if (process.env.FABRIC_7452_EVIDENCE_DIR) {
        const dest = path.join(process.env.FABRIC_7452_EVIDENCE_DIR, "cli", tool);
        fs.mkdirSync(dest, { recursive: true });
        fs.writeFileSync(path.join(dest, "rpc-events.json"), JSON.stringify(events, null, 2));
        for (const [index, name] of ["sender", "receiver"].entries()) {
          fs.writeFileSync(path.join(dest, `${name}-stderr.log`), clients[index]?.getStderr() ?? "");
          for (const file of ["provider-context.jsonl", "abort.json"]) if (exists(name, file)) fs.copyFileSync(path.join(root, name, file), path.join(dest, `${name}-${file}`));
          if (exists(name, "ready.json")) {
            const ready = JSON.parse(fs.readFileSync(path.join(root, name, "ready.json"), "utf8"));
            if (fs.existsSync(ready.sessionFile)) fs.copyFileSync(ready.sessionFile, path.join(dest, `${name}-session.jsonl`));
          }
        }
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
