import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { readResidentRequestDecision, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";

/** Uses only baseline public imports, so it can demonstrate fail-before behavior. */
it("maintenance collects an acknowledged terminal generation; its replay never dispatches again", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-replay-"));
  const old = Date.now() - 24 * 60 * 60 * 1_000 - 60_000;
  const requestId = `r1-${old}-${randomUUID()}`;
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:retention-replay", sessionId: "retention-replay", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: root,
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const write = (dir: string, value: unknown) => {
    fs.mkdirSync(path.join(root, dir), { recursive: true });
    fs.writeFileSync(path.join(root, dir, `${requestId}.json`), JSON.stringify(value));
  };
  // A previous host committed once and Main acknowledged its terminal response.
  const id = "a".repeat(32);
  write("decisions", { requestId, state: "committed", requestFormat: 3, operation: "removeActor", id, ownerHostId: residentHostId(config.rootId) });
  write("responses", { format: 1, requestId, ok: true, completedAt: old });
  write("acknowledgements", { format: 1, requestFormat: 3, requestId, completedAt: old, acknowledgedAt: old });
  const host = new ResidentHost(config);
  try {
    await host.start();
    // Wait for budgeted maintenance, not a timer dedicated to collection.
    const collectionDeadline = Date.now() + 2_000;
    while (fs.existsSync(path.join(root, "decisions", `${requestId}.json`)) && Date.now() < collectionDeadline) await new Promise(r => setTimeout(r, 10));
    expect(fs.existsSync(path.join(root, "decisions", `${requestId}.json`))).toBe(false);
    expect(fs.existsSync(path.join(root, "responses", `${requestId}.json`))).toBe(false);
    const mutation = vi.spyOn(host.actors, "remove");
    // Rewriting mutable createdAt cannot rejuvenate an expired request ID.
    write("requests", { format: 3, requestId, rootId: config.rootId, operation: "removeActor", id, createdAt: Date.now() });
    const responsePath = path.join(root, "responses", `${requestId}.json`);
    const responseDeadline = Date.now() + 2_000;
    while (!fs.existsSync(responsePath) && Date.now() < responseDeadline) await new Promise(r => setTimeout(r, 10));
    expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: false, errorCode: "RESIDENT_REQUEST_EXPIRED" });
    expect(mutation).not.toHaveBeenCalled();
    expect(readResidentRequestDecision(root, requestId)).toBeUndefined();
  } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
});
