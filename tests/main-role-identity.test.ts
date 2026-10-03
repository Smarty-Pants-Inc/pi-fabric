import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { fabricHostIdentity, resolveFabricIdentity as hostIdentity } from "../src/host-compatibility.js";
import { resolveFabricIdentity as startupIdentity } from "../src/main-agent-identity.js";
import { rootParticipantName } from "../src/topology/participant-name.js";

// Launch role is display/lookup metadata, never a different owner or root id.
describe("smarty-role Main identity (#3860)", () => {
  it("filters exact names even when the mesh is disabled", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-role-local-"));
    const identity = { id: "session:local", name: "capacity-lead", kind: "main" as const, sessionId: "local" };
    const directory = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 1000), {
      enabled: false, hostId: identity.id, rootId: identity.id, identity,
    });
    try {
      expect(directory.list({ name: "capacity-lead", kinds: ["root"] }))
        .toEqual([expect.objectContaining({ id: identity.id, name: identity.name })]);
      expect(directory.list({ name: "CAPACITY-LEAD" })).toEqual([]);
      expect(directory.list({ name: "absent" })).toEqual([]);
      expect(directory.list({ name: "" })).toEqual([]);
    } finally { await directory.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it.each([
    [undefined, "main"], ["", "main"], ["  ", "main"],
    ["light", "light"], [" capacity-lead@abcdef123456 ", "capacity-lead"],
    ["fabric-v2@abcdef123456", "fabric-v2"], ["bad/name@abcdef123456", "main"],
    ["a".repeat(61), "main"],
  ])("resolves SMARTY_ROLE %j to %j on both identity paths", (role, name) => {
    const environment = { SMARTY_ROLE: role, PI_FABRIC_ROLE: "project-agent" };
    const identity = { id: "session:current", name, kind: "main", sessionId: "current" };
    expect(fabricHostIdentity("current", environment)).toEqual(identity);
    for (const resolve of [hostIdentity, startupIdentity]) {
      expect(resolve("current", environment)).toEqual({ identity, mainAgentId: identity.id });
    }
    expect(rootParticipantName(undefined, environment)).toBe(name);
  });

  it("keeps explicit valid Pi names, name validation, and child identities", () => {
    const environment = { SMARTY_ROLE: "fabric-v2@abcdef123456" };
    expect(rootParticipantName("  custom-lead  ", environment)).toBe("custom-lead");
    expect(rootParticipantName("bad/name", environment)).toBe("fabric-v2");
    for (const resolve of [hostIdentity, startupIdentity]) {
      expect(resolve("child", { ...environment, PI_FABRIC_PARENT_RUN: "run-child",
        PI_FABRIC_AGENT_NAME: "worker", PI_FABRIC_MAIN_AGENT_ID: "session:current" }))
        .toEqual({ identity: { id: "run-child", name: "worker", kind: "agent", sessionId: "child" },
          mainAgentId: "session:current" });
      expect(resolve("actor", { ...environment, PI_FABRIC_ACTOR_ID: "actor-1",
        PI_FABRIC_ACTOR_NAME: "watcher", PI_FABRIC_MAIN_AGENT_ID: "session:current" }))
        .toEqual({ identity: { id: "actor-1", name: "watcher", kind: "actor", sessionId: "actor" },
          mainAgentId: "session:current" });
    }
  });
});
