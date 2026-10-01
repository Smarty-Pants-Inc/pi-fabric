import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";

// Derive the event from the public callback API; older Pi versions do not export its name.
type RpcAgentSessionEvent = Parameters<Parameters<RpcClient["onEvent"]>[0]>[0];

const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = path.join(repo, "dist/index.js");
const bridgeEntry = path.join(repo, "bin/mesh-bridge");
const fixture = path.join(repo, "tests/fixtures/followup-cli-754.ts");
const cli = path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");

// This artifact proof requires a fresh build and a Pi host exposing isPromptPending.
// Run explicitly: FABRIC_754_REAL_CLI=1 bunx vitest run tests/main-agent-followup-cli.test.ts
// Ordinary source tests remain independent of dist and older supported hosts.
describe.runIf(process.env.FABRIC_754_REAL_CLI === "1")("compiled Fabric public followUp through the real Pi CLI and bridge (#754)", () => {
  it.each(["success", "handled", "validation"])("receives HANDOFF before correction after %s preflight", async (mode) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cli-754-"));
    const clients: RpcClient[] = [];
    let bridge: ChildProcess | undefined;
    let prompt: Promise<void> | undefined;
    let bridgeLog = "";
    const records: Record<string, RpcAgentSessionEvent[]> = { receiver: [], sender: [] };
    const entries: Record<string, unknown> = {};
    const makeClient = (name: string) => {
      const cwd = path.join(root, name);
      const agentDir = path.join(cwd, "agent");
      fs.mkdirSync(agentDir, { recursive: true });
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
        compaction: { enabled: false }, retry: { enabled: false },
      }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
        executor: { kernel: "typescript" }, fullCodeMode: false,
        mesh: { enabled: true, announce: true, followUpFlushMs: 10 },
        mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
        compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
      }));
      const client = new RpcClient({ cliPath: cli, cwd, provider: "followup-754", model: "faux-1",
        env: {
          PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"),
          PI_FABRIC_MESH_ROOT: path.join(cwd, "mesh"), PI_FABRIC_PROJECT_ROOT: cwd,
          PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), FABRIC_754_GATE_DIR: cwd,
          HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1",
        },
        args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
          "--approve", "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(cwd, "sessions"),
          "-e", entry, "-e", fixture],
      });
      client.onEvent(event => records[name]!.push(event));
      clients.push(client);
      return client;
    };
    const exists = (name: string, file: string) => fs.existsSync(path.join(root, name, file));
    const release = (file: string) => fs.writeFileSync(path.join(root, "receiver", `${file}-release`), "1");
    const wait = (check: () => boolean | Promise<boolean>) => vi.waitFor(async () => {
      expect(await check()).toBe(true);
    }, { timeout: 15_000, interval: 25 });
    try {
      const receiver = makeClient("receiver");
      const sender = makeClient("sender");
      await Promise.all(clients.map(client => client.start()));
      await wait(() => exists("receiver", "ready") && exists("sender", "ready"));
      const ready = JSON.parse(fs.readFileSync(path.join(root, "receiver", "ready"), "utf8"));
      expect(ready.preflight).toBe(true); // No skipped candidate proof.
      bridge = spawn(process.execPath, [bridgeEntry, "run", "--mesh", path.join(root, "sender", "mesh"),
        "--name", "sender", "--remote", "receiver", "--cursor", path.join(root, "cursor.json"),
        "--", process.execPath, bridgeEntry, "agent", "--mesh", path.join(root, "receiver", "mesh"), "--peer", "sender"],
      { stdio: ["ignore", "ignore", "pipe"] });
      bridge.stderr!.on("data", chunk => { bridgeLog += String(chunk); });
      await wait(() => bridgeLog.includes("linked sender <-> receiver"));
      // Wait for actual mirrored presence, not just a connected transport.
      await wait(() => {
        const file = path.join(root, "sender", "mesh", "state.json");
        return fs.existsSync(file) && fs.readFileSync(file, "utf8").includes(ready.id);
      });
      // A malformed attachment fails Pi's preflight validation after its input handlers,
      // before agent_start. Subsequent custom-message input has no malformed attachment.
      prompt = receiver.prompt(`preflight-${mode}`, mode === "validation"
        ? [{ type: "image", mimeType: "image/png", data: null } as never] : undefined);
      void prompt.catch(() => undefined); // Cleanup owns an unfinished RPC prompt on assertion failure.
      await wait(() => exists("receiver", "preflight-entered"));
      expect(JSON.parse(fs.readFileSync(path.join(root, "receiver", "preflight-state"), "utf8")))
        .toEqual({ idle: true, pending: true });
      const send = async (message: string) => {
        const events = await sender.promptAndWait(`SEND ${JSON.stringify({ id: ready.id, message })}`, undefined, 15_000);
        const result = events.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
        expect(result).toMatchObject({ isError: false });
        const details = (result as { result: { details: { result: unknown } } }).result.details;
        expect(JSON.stringify(details)).toContain('"acknowledged":true');
        expect(JSON.stringify(details)).toContain('"routed":"mesh"');
      };
      const handoffText = `HANDOFF proof ${"x".repeat(3000)}`;
      await send(handoffText);
      expect(records.receiver!.filter(event => event.type === "agent_start")).toHaveLength(0);
      if (mode !== "success") await send("checksum correction");
      release("preflight");
      if (mode === "success") {
        await wait(() => exists("receiver", "tool-entered"));
        await send("checksum correction");
        release("tool");
      }
      await prompt; // RpcClient.prompt discards the RPC success/error envelope; verify admission below.
      // Handled/invalid input has NO turn_end/settle to rescue admission. With no extra input or
      // deliveries, the acknowledged HANDOFF must cause its own receiving run.
      await wait(() => records.receiver!.some(event => event.type === "agent_settled"));
      const messages = await receiver.getMessages();
      const originalPrompt = messages.some(message => message.role === "user" &&
        JSON.stringify(message.content).includes(`preflight-${mode}`));
      expect(originalPrompt).toBe(mode === "success");
      if (mode === "validation") expect(exists("receiver", "before-start")).toBe(true);
      expect(records.receiver!.filter(event => event.type === "agent_start")).toHaveLength(1);
      const received = messages.filter(message => message.role === "custom" &&
        message.customType === "pi-fabric-agent-message");
      const items = received.flatMap(message => {
        const details = (message as { details: { id: string; items?: Array<{ id: string }> } }).details;
        return details.items ?? [details];
      });
      expect(items).toHaveLength(2);
      const content = received.map(message => (message as { content: string }).content).join("\n");
      expect(content).toContain(handoffText);
      expect(content.indexOf(handoffText)).toBeLessThan(content.indexOf("checksum correction"));
      const receipts = records.sender!.filter(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
      // Exact identity and FIFO against the actual public followUp ACKs, not inferred text.
      const acknowledged = receipts.map(event => {
        const details = (event as { result: { details: { audits: Array<{ result: { messageId: string } }> } } }).result.details;
        return details.audits[0]!.result.messageId;
      });
      expect(items.map(item => item.id)).toEqual(acknowledged);
      // Persisted native receiving-session evidence, not just controller handoff to Pi.
      const persisted = fs.readFileSync(ready.sessionFile, "utf8");
      for (const item of items) expect(persisted).toContain(item.id);
      const context = fs.readFileSync(path.join(root, "receiver", "provider-context.jsonl"), "utf8");
      expect(context).toContain(handoffText);
      expect(context).toContain("checksum correction");
      entries.receiver = await receiver.getEntries();
      entries.sender = await sender.getEntries();
    } finally {
      release("preflight"); release("tool");
      await Promise.all(clients.map(client => client.abort().catch(() => undefined)));
      await prompt?.catch(() => undefined);
      await Promise.all(clients.map(async client => {
        await client.stop(); // Native RPC client; real subprocesses only, never Popen mocks.
      }));
      if (bridge && Number.isSafeInteger(bridge.pid) && bridge.pid! > 1 && bridge.exitCode === null && bridge.signalCode === null) {
        const exited = new Promise<void>(resolve => bridge!.once("exit", () => resolve()));
        bridge.kill("SIGTERM");
        const timer = setTimeout(() => bridge!.kill("SIGKILL"), 5_000);
        await exited;
        clearTimeout(timer);
      }
      const evidence = process.env.FABRIC_754_EVIDENCE_DIR;
      if (evidence) {
        const dest = path.join(evidence, mode);
        fs.mkdirSync(dest, { recursive: true });
        fs.writeFileSync(path.join(dest, "rpc-events.json"), JSON.stringify(records, null, 2));
        fs.writeFileSync(path.join(dest, "entries.json"), JSON.stringify(entries, null, 2));
        for (const name of ["receiver", "sender"]) {
          const state = path.join(root, name, "mesh", "state.json");
          if (fs.existsSync(state)) fs.copyFileSync(state, path.join(dest, `${name}-mesh-state.json`));
        }
        fs.writeFileSync(path.join(dest, "bridge.log"), bridgeLog);
        const dist = path.join(repo, "dist");
        const files = (fs.readdirSync(dist, { recursive: true }) as string[]).filter(file => /\.(?:mjs|js)$/.test(file)).sort();
        fs.writeFileSync(path.join(dest, "artifact.json"), JSON.stringify({ entry, cli: fs.realpathSync(cli),
          revision: execFileSync("git", ["rev-parse", "HEAD"], { cwd: repo, encoding: "utf8" }).trim(),
          bundleHashes: Object.fromEntries(files.map(file => [file,
            createHash("sha256").update(fs.readFileSync(path.join(dist, file))).digest("hex")])),
        }, null, 2));
        for (const name of ["receiver", "sender"]) {
          fs.writeFileSync(path.join(dest, `${name}-stderr.log`), clients[name === "receiver" ? 0 : 1]?.getStderr() ?? "");
          const readyFile = path.join(root, name, "ready");
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
  }, 60_000);
});
