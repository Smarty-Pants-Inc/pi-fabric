import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../../../src/agents/manager.js";
import { AgentCompletionInbox } from "../../../src/agents/completion-inbox.js";
import { CompletionJournal, type CompletionRecipient } from "../../../src/agents/completion-journal.js";
import { DEFAULT_FABRIC_CONFIG } from "../../../src/config.js";
import { MeshStore } from "../../../src/mesh/store.js";
import { MainAgentController } from "../../../src/main-agent.js";
import { ParticipantDirectory } from "../../../src/topology/participant-directory.js";
import { ResidencyClient } from "../../../src/residency/client.js";
import { residentRoot, type ResidentHostConfig } from "../../../src/residency/protocol.js";

/** Native Pi fixture, not a custody/admission simulator. Compose the existing
 * classes at their public seams; launch the compiled worker and the production
 * resident launcher. Deliberately hold parent intake before going offline.
 * No B5 completion ENVELOPE crosses a machine; remote placement can carry
 * result data back, but these workers are pinned needs:["local"]. No transport
 * or completion protocol is invented. This base is FILE ONLY;
 * native-boundaries.test.ts separately reports NATS as blocked, never green.
 */
export default function completionParent(pi: ExtensionAPI): void {
  const root = process.env.FABRIC_NATIVE_BOUNDARY_ROOT!;
  const repo = fileURLToPath(new URL("../../../", import.meta.url));
  const meshRoot = path.join(root, "mesh");
  const output = (name: string, value: unknown) => fs.writeFileSync(path.join(root, name), JSON.stringify(value));
  const append = (name: string, value: unknown) => fs.appendFileSync(path.join(root, name), JSON.stringify(value) + "\n");
  let manager: AgentManager;
  let inbox: AgentCompletionInbox;
  let journal: CompletionJournal;
  let participants: ParticipantDirectory;
  let client: ResidencyClient;
  let mesh: MeshStore;
  let recipient: CompletionRecipient;

  // Observe real stable-storage operations; do not replace any custody decision.
  // Record at the archive rename, BEFORE it happens, that the receipt body and
  // its containing namespace have already passed the actual fsync calls.
  let ioSequence = 0;
  const syncedInodes = new Map<string, number>(); const syncedDirectories = new Map<string, number>();
  const receiptRenames = new Map<string, number>();
  const inode = (stat: fs.Stats) => `${stat.dev}:${stat.ino}`;
  const sync = fs.fsyncSync; const rename = fs.renameSync;
  fs.fsyncSync = fd => {
    sync(fd); // A failed real barrier never becomes an observation.
    let file: string;
    try { file = fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return; }
    if (!file.startsWith(meshRoot + path.sep)) return;
    const stat = fs.fstatSync(fd); const sequence = ++ioSequence;
    if (stat.isDirectory()) syncedDirectories.set(file, sequence);
    else syncedInodes.set(inode(stat), sequence);
  };
  fs.renameSync = (source, target) => {
    const destination = String(target);
    if (path.dirname(destination) === path.join(meshRoot, "agent-completions", "receipts")) {
      syncedDirectories.delete(path.dirname(destination)); // Require the POST-rename namespace barrier.
      rename(source, target);
      receiptRenames.set(destination, ++ioSequence);
      return;
    }
    if (path.dirname(destination) === path.join(meshRoot, "agent-completions", "archive")) {
      const receiptFile = path.join(meshRoot, "agent-completions", "receipts", path.basename(destination));
      const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8"));
      const bodySyncSequence = syncedInodes.get(inode(fs.statSync(receiptFile))) ?? 0;
      const receiptRenameSequence = receiptRenames.get(receiptFile) ?? 0;
      const directorySyncSequence = syncedDirectories.get(path.dirname(receiptFile)) ?? 0;
      const archiveAttemptSequence = ++ioSequence;
      append("completion-order.jsonl", { pid: process.pid, event: "before-archive", receipt,
        bodySyncSequence, receiptRenameSequence, directorySyncSequence, archiveAttemptSequence,
        bodySynced: bodySyncSequence > 0, directorySynced: directorySyncSequence > receiptRenameSequence,
        sourcePresentBeforeArchive: fs.existsSync(String(source)),
        claimPresentBeforeArchive: mesh.get(`residency/completion-claims/${path.basename(destination, ".json")}`, { fresh: true }) !== undefined });
    }
    rename(source, target);
  };

  pi.on("session_start", async (_event, ctx) => {
    const sessionId = ctx.sessionManager.getSessionId();
    const addressFile = path.join(root, `completion-parent-${sessionId}.json`);
    recipient = fs.existsSync(addressFile) ? JSON.parse(fs.readFileSync(addressFile, "utf8")) : {
      rootId: `session:${sessionId}`, sessionId, projectRoot: root, cwd: root,
      name: "native completion parent", startedAt: Date.now(),
    };
    if (recipient.sessionId !== sessionId || recipient.rootId !== `session:${sessionId}`) throw new Error("Native resume address changed");
    output(path.basename(addressFile), recipient);
    mesh = new MeshStore(meshRoot, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents);
    // Empty directories only: let the harness arm fs.watch before the producer
    // writes its first actual candidate/envelope/receipt, with no polling race.
    for (const name of ["", "attempts", "receipts", "archive"]) fs.mkdirSync(path.join(meshRoot, "agent-completions", name), { recursive: true, mode: 0o700 });
    const identity = { id: recipient.rootId, sessionId, name: recipient.name, kind: "main" as const };
    participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: identity.name, status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"], cwd: root, projectRoot: root,
      sessionId, startedAt: recipient.startedAt, updatedAt: Date.now(), controlProtocol: "v1" }]);
    await participants.start();
    inbox = new AgentCompletionInbox(pi, ctx);
    journal = new CompletionJournal(meshRoot, recipient, participants, mesh, (result, delivered) => {
      append("completion-enqueued.jsonl", { pid: process.pid, sessionId, result });
      inbox.enqueue(result, () => {
        delivered(); // Actual consumeCompletion, fsync barriers, archive and claim retirement.
        append("completion-delivered.jsonl", { pid: process.pid, sessionId, id: result.id });
      });
    });
    const agents = { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, timeoutMs: 60_000,
      retainRuns: true, sessionExport: false, nice: 19 };
    manager = new AgentManager(root, agents, { workerPath: path.join(repo, "dist/worker.js"),
      piBinary: process.env.FABRIC_NATIVE_PI_CLI!, fabricExtensionPath: path.join(repo, "dist/index.js"),
      runRoot: path.join(root, "session-runs"), fullCodeMode: false, meshRoot, projectRoot: root,
      mainAgentId: recipient.rootId, fabricSessionId: sessionId, completionRecipient: recipient,
      onSettled: result => journal.save(result),
    });
    const config: ResidentHostConfig = { format: 1, rootId: recipient.rootId, sessionId, cwd: root, projectRoot: root,
      mainName: recipient.name, mainStartedAt: recipient.startedAt, meshRoot, actorRoot: path.join(meshRoot, "actors"),
      sessionActorRoot: path.join(meshRoot, "actors", sessionId), residencyRoot: residentRoot(meshRoot, recipient.rootId),
      fullCodeMode: false, agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh }, retention: DEFAULT_FABRIC_CONFIG.retention,
      watchdog: { enabled: false }, workerPath: path.join(repo, "dist/worker.js"), fabricExtensionPath: path.join(repo, "dist/index.js"),
      piBinary: process.env.FABRIC_NATIVE_PI_CLI!, claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "boundary-offline", id: "offline", name: "Offline boundary" }], aliases: {}, defaultModel: "boundary-offline/offline" },
    };
    client = new ResidencyClient({ config, mesh, participants,
      mainAgent: new MainAgentController(pi, recipient.rootId, true, root, sessionId),
      hostPath: path.join(repo, "dist/residency/launcher.js"),
    });
    // Do not start client delivery/watchdog: the explicitly composed parent
    // journal above owns intake, and the native launcher owns durable execution.
    if (process.env.FABRIC_NATIVE_COMPLETION_INTAKE === "live") await journal.drain();
    output("completion-main-ready.json", { pid: process.pid, sessionId, recipient, config });
  });
  pi.registerCommand("native-completion-spawn", { description: "Launch a source-real completion producer", handler: async args => {
    const request = { task: "Return the native E4 child result.", model: "boundary-offline/offline", thinking: "off" as const,
      needs: ["local"], transport: "process" as const, extensions: false, tools: [] };
    const handle = args.trim() === "durable" ? await client.spawnAgent({ ...request, residency: "durable" })
      : await manager.spawn(request);
    const run = args.trim() === "durable" ? path.join(residentRoot(meshRoot, recipient.rootId), "runs", handle.id)
      : path.join(root, "session-runs", handle.id);
    output("completion-spawned.json", { pid: process.pid, handle, run, recipient });
  } });
  pi.registerCommand("native-completion-drain", { description: "One awaited journal pass; no poll", handler: async () => {
    await journal.drain(); inbox.idleSignal(); output("completion-drained.json", { pid: process.pid });
  } });
  pi.on("session_shutdown", async () => {
    inbox?.close();
    await manager?.close();
    await client?.close();
    await participants?.close();
    mesh?.closeState();
    fs.fsyncSync = sync; fs.renameSync = rename;
  });
}
