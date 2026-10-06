import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { residentRoot } from "../src/residency/protocol.js";

// Explicit compiled-artifact receipt: FABRIC_PRESENCE_REAL_CLI=1 bunx vitest run tests/residency-presence-cli.test.ts
it.runIf(process.env.FABRIC_PRESENCE_REAL_CLI === "1")("renews both durable actor scopes after native Main exit and recovers from real >timeout mesh contention", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-cli-"));
  const evidence = process.env.FABRIC_PRESENCE_EVIDENCE_DIR;
  const cli = path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
  const entry = path.resolve("dist/index.js");
  const agentDir = path.join(root, "agent"), meshRoot = path.join(root, "mesh");
  fs.mkdirSync(agentDir, { recursive: true });
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
    executor: { kernel: "typescript" }, fullCodeMode: true,
    agents: { nice: 19, budgetUsd: 0 }, mesh: { enabled: true, announce: true, lockProtocol: 2, actorPollMs: 50 },
    mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
    compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
  }));
  const traceFile = path.join(root, "lock-trace.jsonl");
  const events: unknown[] = [], receipts: Record<string, unknown> = {};
  const clients: RpcClient[] = [];
  let owner: { pid: number; hostId: string; fabricExtensionPath?: string; handover?: { launcher: { pid: number } } } | undefined;
  let holder: ChildProcess | undefined, holderExit: Promise<void> | undefined;
  let holderOutput = "";
  const wait = async (predicate: () => boolean, timeout = 20000) => vi.waitFor(() => expect(predicate()).toBe(true), { timeout, interval: 50 });
  const makeClient = (session?: string) => {
    const client = new RpcClient({ cliPath: cli, cwd: root, provider: "presence-proof", model: "faux-1",
      env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
        PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, "runs"),
        PI_FABRIC_PI_BINARY: cli, PI_FABRIC_NODE_BINARY: process.execPath, PI_OFFLINE: "1",
        PRESENCE_PROOF_ROOT: root, PRESENCE_LOCK_TRACE: traceFile,
        PI_FABRIC_COMMIT_TRACE: path.join(root, "commits.jsonl"),
        NODE_OPTIONS: `--require=${path.resolve("tests/fixtures/presence-lock-observer.cjs")}`,
        HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "" },
      args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
        "--approve", "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(root, "sessions"),
        ...(session ? ["--session", session] : []), "-e", entry, "-e", path.resolve("tests/fixtures/presence-cli-provider.ts")],
    });
    client.onEvent(event => events.push(event)); clients.push(client); return client;
  };
  const proofTurn = async (client: RpcClient, prompt: string) => {
    const result = await client.promptAndWait(prompt, undefined, 60000);
    const tool = result.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
    expect(tool).toMatchObject({ isError: false });
    return tool;
  };
  const alive = (pid: number) => {
    try {
      if (process.platform === "linux" && /\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8"))) return false;
      process.kill(pid, 0); return true;
    } catch { return false; }
  };
  let configDir = "";
  try {
    const main = makeClient(); await main.start();
    await wait(() => fs.existsSync(path.join(root, "main-ready.json")));
    const ready = JSON.parse(fs.readFileSync(path.join(root, "main-ready.json"), "utf8"));
    receipts.created = await proofTurn(main, "CREATE_PRESENCE");
    configDir = residentRoot(meshRoot, ready.rootId);
    owner = JSON.parse(fs.readFileSync(path.join(configDir, "owner.json"), "utf8"));
    expect(owner!.fabricExtensionPath).toBe(entry);
    const participantFiles = () => {
      const state = JSON.parse(fs.readFileSync(path.join(meshRoot, "state.json"), "utf8"));
      return Object.entries(state.entries as Record<string, { value: { id: string; name?: string; scope?: string } }>)
        .filter(([key]) => key.startsWith("actors/"))
        .map(([, entry]) => entry.value).filter(value => value.name?.startsWith("presence-proof-"));
    };
    const actors = participantFiles();
    expect(actors).toHaveLength(12);
    expect(new Set(actors.map(actor => actor.scope))).toEqual(new Set(["session", "project"]));
    receipts.actors = actors;
    await main.stop();
    expect(alive(ready.pid)).toBe(false);
    const before = readHostLease(meshRoot, owner!.hostId)!.updatedAt;
    const actorsRenewedAfter = (stamp: number) => actors.every(actor => {
      const key = "topology/participants/" + createHash("sha256").update(actor.id).digest("hex");
      return (readParticipantFile(meshRoot, key)?.updatedAt ?? 0) > stamp;
    });
    await wait(() => (readHostLease(meshRoot, owner!.hostId)?.updatedAt ?? 0) > before && actorsRenewedAfter(before));
    const renewed = readHostLease(meshRoot, owner!.hostId)!;
    for (const actor of actors) {
      const key = "topology/participants/" + createHash("sha256").update(actor.id).digest("hex");
      expect(readParticipantFile(meshRoot, key)?.updatedAt).toBeGreaterThan(before);
    }
    const participantReceipts = () => actors.map(actor => ({ id: actor.id, scope: actor.scope,
      participant: readParticipantFile(meshRoot, "topology/participants/" + createHash("sha256").update(actor.id).digest("hex")) }));
    receipts.owner = owner;
    receipts.afterMainExit = { mainPid: ready.pid, residentPid: owner!.pid, before, renewed, participants: participantReceipts() };
    console.info(`native Main exited pid=${ready.pid}; resident pid=${owner!.pid} renewed host and all 12 actor participants across project/session scopes`);
    // Real public restoration in another native Main of the same persisted session.
    const restoredMain = makeClient(ready.sessionFile); await restoredMain.start();
    receipts.restored = await proofTurn(restoredMain, "RESTORE_PRESENCE");
    const restoration = receipts.restored as { result: { content: Array<{ type: string; text?: string }> } };
    const restored = JSON.parse(restoration.result.content.filter(item => item.type === "text").map(item => item.text).join("")).restored as Array<{ id: string; scope: string }>;
    expect(restored.map(actor => actor.id).sort()).toEqual(actors.map(actor => actor.id).sort());
    expect(new Set(restored.map(actor => actor.scope))).toEqual(new Set(["session", "project"]));
    await restoredMain.stop();
    const heldFile = path.join(root, "lock-held.json");
    const holdCode = `import fs from 'node:fs'; import { MeshStore } from ${JSON.stringify(path.resolve("dist/mesh.js"))};
      const store = new MeshStore(${JSON.stringify(meshRoot)}, 65536, 1000, { lockProtocol: 2 });
      await store.exclusive(() => { const start = Date.now(); fs.writeFileSync(${JSON.stringify(heldFile)}, JSON.stringify({start,pid:process.pid}));
        console.log('actual MeshStore lock acquired; holding 16000 ms (>10000 ms timeout) pid=' + process.pid);
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 16000);
        console.log('actual MeshStore lock released after ' + (Date.now()-start) + ' ms'); });`;
    receipts.holderCommand = [process.execPath, "--input-type=module", "-e", holdCode];
    holder = spawn(process.execPath, ["--input-type=module", "-e", holdCode], { stdio: ["ignore", "pipe", "pipe"] });
    holder.stdout!.on("data", data => { holderOutput += String(data); });
    holder.stderr!.on("data", data => { holderOutput += String(data); });
    holderExit = new Promise<void>((resolve, reject) => { holder!.once("error", reject); holder!.once("close", () => resolve()); });
    await wait(() => fs.existsSync(heldFile));
    const held = JSON.parse(fs.readFileSync(heldFile, "utf8"));
    const stale = readHostLease(meshRoot, owner!.hostId)!.updatedAt;
    await holderExit;
    expect(holder.exitCode).toBe(0);
    const releasedAt = Date.now();
    const traces = fs.readFileSync(traceFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
    const during = traces.filter(trace => trace.pid === owner!.pid && trace.at >= held.start && trace.at <= releasedAt);
    const acquisitions = [...new Set(during.map(trace => trace.token))];
    expect(acquisitions.length).toBeGreaterThan(0);
    expect(acquisitions.length).toBeLessThanOrEqual(2); // shared heartbeat only, never one retry per actor
    // The last syscall sample may precede the absolute deadline by a millisecond.
    // Require the real typed timeout receipt, not a flaky span of sampled timestamps.
    const timeoutLog = fs.readFileSync(path.join(configDir, "child-stderr.log"), "utf8");
    expect(timeoutLog).toContain("FABRIC_MESH_LOCK_TIMEOUT");
    expect(releasedAt - held.start).toBeGreaterThan(10000);
    await wait(() => (readHostLease(meshRoot, owner!.hostId)?.updatedAt ?? 0) > stale && actorsRenewedAfter(stale));
    const recovered = readHostLease(meshRoot, owner!.hostId)!;
    receipts.realContention = { held, releasedAt, heldMs: releasedAt - held.start, timedOut: true,
      lockWaitSamples: during.length, distinctAcquisitions: acquisitions,
      actorCount: actors.length, before: stale, recovered, participants: participantReceipts() };
    console.info(holderOutput.trim());
    console.info(`real lock contention: ${during.length} syscall samples, ${acquisitions.length} shared acquisition(s), 12 actors, recovery lease=${recovered.updatedAt}`);
    for (const actor of actors) {
      const key = "topology/participants/" + createHash("sha256").update(actor.id).digest("hex");
      expect(readParticipantFile(meshRoot, key)?.updatedAt).toBeGreaterThan(stale);
    }
  } finally {
    await Promise.all(clients.map(client => client.stop().catch(() => undefined)));
    // A failed creation assertion may still have launched a resident; always recover its own-root receipt.
    if (!owner && fs.existsSync(path.join(root, "main-ready.json"))) {
      const ready = JSON.parse(fs.readFileSync(path.join(root, "main-ready.json"), "utf8"));
      configDir = residentRoot(meshRoot, ready.rootId);
      const ownerFile = path.join(configDir, "owner.json");
      if (fs.existsSync(ownerFile)) owner = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
    }
    if (holder?.exitCode === null && holder.signalCode === null) holder.kill("SIGTERM");
    await holderExit?.catch(() => undefined);
    if (owner) {
      const pid = owner.handover?.launcher.pid ?? owner.pid;
      if (alive(pid)) process.kill(pid, "SIGTERM");
      await wait(() => !alive(pid) && !alive(owner!.pid), 15000).catch(async () => {
        for (const target of [pid, owner!.pid]) if (alive(target)) process.kill(target, "SIGKILL");
        await wait(() => !alive(pid) && !alive(owner!.pid), 5000);
      });
      receipts.cleanup = { launcherExited: !alive(pid), residentExited: !alive(owner.pid) };
    }
    if (evidence) {
      fs.mkdirSync(evidence, { recursive: true });
      fs.writeFileSync(path.join(evidence, "receipts.json"), JSON.stringify(receipts, null, 2));
      fs.writeFileSync(path.join(evidence, "rpc-events.json"), JSON.stringify(events, null, 2));
      fs.writeFileSync(path.join(evidence, "holder.log"), holderOutput);
      for (const file of ["lock-trace.jsonl", "commits.jsonl"]) if (fs.existsSync(path.join(root, file))) fs.copyFileSync(path.join(root, file), path.join(evidence, file));
      if (configDir && fs.existsSync(configDir)) fs.cpSync(configDir, path.join(evidence, "resident"), { recursive: true });
      fs.writeFileSync(path.join(evidence, "main-stderr.log"), clients.map(client => client.getStderr()).join("\n"));
      const bundles = fs.readdirSync("dist", { recursive: true }).filter(file => /\.(?:mjs|js)$/.test(String(file))).map(String).sort();
      fs.writeFileSync(path.join(evidence, "artifact.json"), JSON.stringify({ entry, cli: fs.realpathSync(cli),
        revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
        hashes: Object.fromEntries(bundles.map(file => [file, createHash("sha256").update(fs.readFileSync(path.join("dist", file))).digest("hex")])) }, null, 2));
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 180000);
