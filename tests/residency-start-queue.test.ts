import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { ResidentHostConfig, ResidentCommand } from "../src/residency/protocol.js";
import { newResidentRequestId, RESIDENT_EXPIRING_COMMAND_FORMAT } from "../src/residency/request-expiry.js";

it.each(["pre-publication", "owner-publication"] as const)("Astra F3 preserves accepted unlaunched events and requests through four failed %s host starts, then runs once", async phase => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-start-backlog-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:start-backlog", sessionId: "start-backlog", cwd: process.cwd(), projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5000 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    piModels: { available: [{ provider: "test", id: "visible" }], aliases: {}, defaultModel: "test/visible" },
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  let host = new ResidentHost(config);
  try {
    await host.start();
    const actor = await host.actors.create({ name: "backlog", instructions: "Respond", responseMode: "text", residency: "durable" });
    host.actors.pauseForRelease();
    host.actors.tell(actor.id, "accepted but never launched", undefined);
    await host.close();
    const directory = path.join(config.actorRoot, actor.id);
    const queue = fs.readdirSync(directory).find(file => file.startsWith("queue-") && file.endsWith(".json"))!;
    const queuePath = path.join(directory, queue);
    // Crash/start recovery, not a negotiated clean release. The accepted item
    // still carries its actual launch evidence; changing a release flag is not a run.
    const snapshot = JSON.parse(fs.readFileSync(queuePath, "utf8")); delete snapshot.cleanHandover;
    fs.writeFileSync(queuePath, JSON.stringify(snapshot));
    const requestId = newResidentRequestId();
    const queued: ResidentCommand = { format: RESIDENT_EXPIRING_COMMAND_FORMAT, operation: "actors",
      requestId, rootId: config.rootId, createdAt: Date.now() };
    const requestPath = path.join(config.residencyRoot, "requests", `${requestId}.json`);
    const request = JSON.stringify(queued); fs.writeFileSync(requestPath, request);
    const start = ParticipantDirectory.prototype.start;
    const delayedStart = phase === "owner-publication" ? vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(async function (this: ParticipantDirectory) {
      await start.call(this); await new Promise(resolve => setTimeout(resolve, 400));
    }) : undefined;
    const rename = fs.renameSync;
    const fault = phase === "pre-publication"
      ? vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(() => { throw new Error("injected start failure"); })
      : vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
        if (String(target) === path.join(config.residencyRoot, "owner.json")) throw new Error("injected start failure");
        return rename(source, target);
      });
    try {
      for (let attempt = 1; attempt <= 4; attempt++) {
        host = new ResidentHost(config);
        await expect(host.start()).rejects.toThrow("injected start failure");
        expect(fs.readFileSync(requestPath, "utf8")).toBe(request);
        expect(fs.existsSync(queuePath), `failed start ${attempt} must not consume accepted work`).toBe(true);
        const saved = JSON.parse(fs.readFileSync(queuePath, "utf8"));
        expect(saved.items, `failed start ${attempt} must keep accepted work`).toHaveLength(1);
        expect(saved.items[0].attempts, `failed start ${attempt} launched no worker`).toBe(0);
      }
    } finally { fault.mockRestore(); delayedStart?.mockRestore(); }
    host = new ResidentHost(config); await host.start();
    const responsePath = path.join(config.residencyRoot, "responses", `${requestId}.json`);
    await vi.waitFor(() => expect(fs.existsSync(responsePath)).toBe(true));
    expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: true, requestId, actors: [expect.objectContaining({ id: actor.id })] });
    expect(fs.existsSync(requestPath)).toBe(false);
    await vi.waitFor(() => expect(host.actors.messages(actor.id).filter(m => m.direction === "out" && !m.error)).toHaveLength(1), { timeout: 10000 }).catch(error => { throw new Error(`${String(error)}; status=${JSON.stringify(host.actors.status(actor.id))}; messages=${JSON.stringify(host.actors.messages(actor.id))}`); });
    await new Promise(resolve => setTimeout(resolve, 150));
    expect(host.actors.messages(actor.id).filter(m => m.direction === "out" && !m.error)).toHaveLength(1);
  } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
}, 20000);
