import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";

type Event = Parameters<Parameters<RpcClient["onEvent"]>[0]>[0];
const repo = path.resolve(".");
it.runIf(process.env.FABRIC_4313_REAL_PI === "1")("installed Pi: A followUp -> B native rotation -> B' once and A reroute receipt", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-real-4313-"));
  const evidence = process.env.FABRIC_4313_EVIDENCE_DIR!;
  const cli = process.env.FABRIC_4313_PI_CLI!;
  if (!cli || !evidence) throw new Error("Set FABRIC_4313_PI_CLI and FABRIC_4313_EVIDENCE_DIR to the installed host and retained output");
  const clients: RpcClient[] = [];
  const events: Record<string, Event[]> = { A: [], B: [] };
  const mesh = path.join(root, "mesh");
  const ready = (name: string) => JSON.parse(fs.readFileSync(path.join(root, name, "ready"), "utf8"));
  const wait = async (check: () => boolean | Promise<boolean>, timeout = 30_000) => vi.waitFor(async () => expect(await check()).toBe(true), { timeout, interval: 50 });
  const make = (name: string) => {
    const cwd = path.join(root, name), agentDir = path.join(cwd, "agent"); fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      executor: { kernel: "typescript" }, fullCodeMode: false,
      mesh: { enabled: true, announce: true, followUpFlushMs: 120_000 },
      mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
      compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
    }));
    const client = new RpcClient({ cliPath: cli, cwd, provider: "stall-4313", model: "faux-1",
      env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"),
        PI_FABRIC_MESH_ROOT: mesh, PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"),
        FABRIC_4313_GATE_DIR: cwd, HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1", PI_FABRIC_INBOX_WAKE_MS: "1000" },
      args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve", "--no-builtin-tools",
        "--thinking", "off", "--session-dir", path.join(cwd, "sessions"), "-e", path.join(repo, "dist/index.js"), "-e", path.join(repo, "tests/fixtures/stall-alarms-pi.ts")],
    });
    client.onEvent(event => events[name]!.push(event)); clients.push(client); return client;
  };
  let hold: Promise<void> | undefined;
  const identities: Record<string, unknown> = {};
  let outcome: unknown;
  try {
    const A = make("A"), B = make("B");
    await Promise.all(clients.map(client => client.start()));
    await wait(() => fs.existsSync(path.join(root, "A", "ready")) && fs.existsSync(path.join(root, "B", "ready")));
    identities.A = ready("A"); identities.B = ready("B");
    const old = ready("B");
    hold = B.prompt("HOLD"); void hold.catch(() => undefined);
    await wait(() => fs.existsSync(path.join(root, "B", "holding")));
    const carrierText = "smarty-dev#4313 ORIGINAL-CARRIER from A for rotating B";
    const resultEvents = await A.promptAndWait(`SEND ${JSON.stringify({ id: old.id, message: carrierText })}`, undefined, 30_000);
    const ended = resultEvents.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec") as any;
    expect(ended).toMatchObject({ isError: false });
    const publicResult = ended.result.details.audits[0].result;
    expect(publicResult).toMatchObject({ queued: true, acknowledged: true });
    const oldJournal = path.join(mesh, "main-followups", `${old.id.slice("session:".length)}.json`);
    const journal = JSON.parse(fs.readFileSync(oldJournal, "utf8"));
    expect(journal.items.map((item: any) => item.id)).toEqual([publicResult.messageId]);
    expect(fs.readFileSync(old.sessionFile, "utf8")).not.toContain(carrierText);
    fs.mkdirSync(evidence, { recursive: true });
    fs.copyFileSync(oldJournal, path.join(evidence, "B-before-rotation-inbox.json"));
    const replacement = await B.newSession(); expect(replacement.cancelled).toBe(false);
    await hold;
    await wait(() => ready("B").id !== old.id);
    const successor = ready("B"); identities.successor = successor;
    await wait(async () => (await B.getMessages()).some(message => message.role === "custom" && message.customType === "pi-fabric-agent-message"), 40_000);
    await wait(() => fs.existsSync(successor.sessionFile) && fs.readFileSync(successor.sessionFile, "utf8").includes(carrierText));
    const carrierEntries = (await B.getEntries()).entries.filter((entry: any) => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message");
    const items = carrierEntries.flatMap((entry: any) => entry.details.items ?? [entry.details]);
    expect(items.map((item: any) => item.id)).toEqual([publicResult.messageId]);
    // Real passive root inbox receipt reaches A, not merely a mesh publication/ACK.
    const text = `rerouted: ${old.id} -> ${successor.id}`;
    await wait(async () => (await A.getMessages()).some(message => {
      const content = JSON.stringify(message);
      return content.includes("rerouted:") && content.includes(old.id) && content.includes(successor.id);
    }), 100_000);
    const before = carrierEntries.length;
    await B.promptAndWait("verify no duplicate on another boundary", undefined, 30_000);
    const after = (await B.getEntries()).entries.filter((entry: any) => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message");
    expect(after).toHaveLength(before);
    expect(fs.readFileSync(old.sessionFile, "utf8")).not.toContain(carrierText);
    const files = (fs.readdirSync(path.join(repo, "dist"), { recursive: true }) as string[]).filter(file => /\.(js|mjs)$/.test(file)).sort();
    outcome = { passed: true, messageId: publicResult.messageId, sender: ready("A").id, oldRoot: old.id, successorRoot: successor.id,
      nativeCarrierCount: items.length, senderReceipt: text, installedCli: fs.realpathSync(cli),
      head: execFileSync("git", ["rev-parse", "HEAD"], { cwd: repo, encoding: "utf8" }).trim(),
      bundleHashes: Object.fromEntries(files.map(file => [file, createHash("sha256").update(fs.readFileSync(path.join(repo, "dist", file))).digest("hex")])),
    };
    fs.writeFileSync(path.join(evidence, "A-entries.json"), JSON.stringify(await A.getEntries(), null, 2));
    fs.writeFileSync(path.join(evidence, "successor-entries.json"), JSON.stringify(await B.getEntries(), null, 2));
  } finally {
    await Promise.all(clients.map(client => client.abort().catch(() => undefined)));
    await hold?.catch(() => undefined);
    await Promise.all(clients.map(client => client.stop()));
    fs.mkdirSync(evidence, { recursive: true });
    fs.writeFileSync(path.join(evidence, "outcome.json"), JSON.stringify(outcome ?? { passed: false }, null, 2));
    fs.writeFileSync(path.join(evidence, "identities.json"), JSON.stringify(identities, null, 2));
    fs.writeFileSync(path.join(evidence, "rpc-events.json"), JSON.stringify(events, null, 2));
    if (fs.existsSync(mesh)) fs.cpSync(mesh, path.join(evidence, "mesh"), { recursive: true });
    for (let index = 0; index < clients.length; index++) {
      const name = index === 0 ? "A" : "B";
      fs.writeFileSync(path.join(evidence, `${name}-stderr.log`), clients[index]!.getStderr());
      for (const file of ["sessions.jsonl", "provider-context.jsonl"]) {
        const from = path.join(root, name, file); if (fs.existsSync(from)) fs.copyFileSync(from, path.join(evidence, `${name}-${file}`));
      }
      const sessions = path.join(root, name, "sessions"); if (fs.existsSync(sessions)) fs.cpSync(sessions, path.join(evidence, `${name}-sessions`), { recursive: true });
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 180_000);
