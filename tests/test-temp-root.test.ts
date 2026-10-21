import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { runSessionTraceArchivePath } from "../src/storage/retention.js";
import { testTempRoot } from "./helpers/test-temp-root.js";

describe("archive-friendly private test roots", () => {
  it("archives a raw session on actor-root close outside process TMPDIR and cleans its owned fixture", async () => {
    const root = testTempRoot("pi-fabric-test-archive-");
    const processTemp = path.resolve(process.env.TMPDIR!);
    const meshRoot = path.join(root, "mesh");
    const actorRoot = path.join(root, "actors");
    const actorDirectory = path.join(actorRoot, "actor-fixture");
    const identity: MeshIdentity = { id: "session:archive-proof", name: "main", kind: "main", sessionId: "archive-proof" };
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("archive-proof", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      actorRoot, persistent: false, rootId: identity.id,
    });
    try {
      fs.mkdirSync(actorDirectory, { recursive: true });
      fs.writeFileSync(path.join(actorDirectory, "session.jsonl"), "raw test session\n");
      const relativeMesh = path.relative(processTemp, meshRoot);
      expect(relativeMesh === "" || (!path.isAbsolute(relativeMesh) && relativeMesh !== ".." && !relativeMesh.startsWith(`..${path.sep}`))).toBe(false);

      await actors.close();

      const archiveName = `actor-root-${createHash("sha256").update(path.resolve(actorRoot)).digest("hex").slice(0, 24)}`;
      const archive = runSessionTraceArchivePath(meshRoot, actorRoot, archiveName);
      expect(fs.existsSync(actorRoot)).toBe(false);
      expect(fs.readFileSync(path.join(archive, "actor-fixture", "session.jsonl"), "utf8")).toBe("raw test session\n");
    } finally {
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
