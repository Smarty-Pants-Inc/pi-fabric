import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  LaunchRecordStore, authorizeLaunchChild, makeLaunchRecord, validateLaunchCensus, validateLaunchRecord,
} from "../src/residency/launch-record.js";
import { nativePlacementPathAvailable } from "../src/agents/placement-config.js";
import { normalizeFabricConfig } from "../src/config.js";

const scratch: string[] = [];
const temp = (): string => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launch-record-")); scratch.push(value); return value; };
afterEach(() => { for (const directory of scratch.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });

const root = () => makeLaunchRecord({ launchSpecId: "root-spec", orgId: "org-a", workId: "work-a", role: "org-agent", profile: "root", sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: "root" } }, "root-a", 1);
const resident = (parent: string, id: string, org = "org-a", work = "work-a") => makeLaunchRecord({ launchSpecId: "resident-spec", orgId: org, workId: work, parentNativeLaunchId: parent, role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident", origin: { class: "background-agent", originRef: id } }, id, 1);
const actor = (parent: string, id: string, org = "org-a", work = "work-a") => makeLaunchRecord({ launchSpecId: "actor-spec", orgId: org, workId: work, parentNativeLaunchId: parent, role: "actor-activation", profile: "actor", sourcePin: "pin", memoryProfile: "actor", origin: { class: "background-agent", originRef: id } }, id, 1);

 describe("resident launch records", () => {
  it("publishes required schema bytes before native creation and can close uncertainty", () => {
    const store = new LaunchRecordStore(temp());
    const record = store.prepare(resident("root-a", "resident-a"));
    expect(record).toMatchObject({ schema: "smarty.launch-record.v1", native_launch_id: "resident-a", unit: "smarty-launch-resident-a-g1.service", fence: { state: "open" }, terminal: null });
    expect(fs.statSync(store.path(record.native_launch_id)).mode & 0o777).toBe(0o600);
    expect(store.transition("resident-a", 1, "creating").fence.state).toBe("creating");
    const uncertain = store.transition("resident-a", 1, "closed-uncertain");
    expect(uncertain.fence.state).toBe("closed-uncertain");
    validateLaunchRecord(JSON.parse(fs.readFileSync(store.path("resident-a"), "utf8")));
    const crashWindow = store.prepare(resident("root-a", "resident-b"));
    expect(store.transition(crashWindow.native_launch_id, crashWindow.generation, "closed-uncertain").fence.state).toBe("closed-uncertain");
  });

  it("requires native identity for closed-started and records terminal exit", () => {
    const store = new LaunchRecordStore(temp());
    store.prepare(resident("root-a", "resident-a"));
    store.transition("resident-a", 1, "creating");
    const started = store.transition("resident-a", 1, "closed-started", { boot_id: "12345678-1234-1234-1234-123456789abc", invocation_id: "1234567890abcdef1234567890abcdef", main_pid_start: "123:456" });
    expect(started.fence.state).toBe("closed-started");
    expect(store.terminal("resident-a", 1, { unit: started.unit, load_state: "loaded", active_state: "inactive",
      invocation_id: started.invocation_id, boot_id: started.boot_id }).terminal?.outcome).toBe("exited");
  });

  it("records a refused duplicate resident as never-started and allows the next admission after exit", async () => {
    const store = new LaunchRecordStore(temp());
    const rootRecord = root(); store.prepare(rootRecord); store.transition(rootRecord.native_launch_id, 1, "creating");
    store.transition(rootRecord.native_launch_id, 1, "closed-started", { boot_id: "12345678-1234-1234-1234-123456789abc",
      invocation_id: "1234567890abcdef1234567890abcdef", main_pid_start: "123:456" });
    const first = await store.admit(resident("root-a", "resident-a"), true);
    store.transition(first.native_launch_id, first.generation, "closed-started", { boot_id: "12345678-1234-1234-1234-123456789abc",
      invocation_id: "abcdef1234567890abcdef1234567890", main_pid_start: "124:457" });
    await expect(store.admit(resident("root-a", "resident-b"), true)).rejects.toThrow("More than one resident");
    expect(store.get("resident-b")).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
    expect(validateLaunchCensus(store.census()).has("resident-b")).toBe(false);
    store.terminal("resident-a", 1, { unit: first.unit, load_state: "loaded", active_state: "inactive",
      invocation_id: "abcdef1234567890abcdef1234567890", boot_id: "12345678-1234-1234-1234-123456789abc" });
    expect((await store.admit(resident("root-a", "resident-c"), true)).fence.state).toBe("creating");
  });

  it("rejects a second resident, cross-root child, and wrong caller", () => {
    const rootA = root();
    const rootB = makeLaunchRecord({ launchSpecId: "root-spec", orgId: "org-b", workId: "work-b", role: "org-agent", profile: "root", sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: "root-b" } }, "root-b", 1);
    const first = resident(rootA.native_launch_id, "resident-a");
    expect(() => validateLaunchCensus([rootA, first, resident(rootA.native_launch_id, "resident-b")])).toThrow("More than one resident");
    const otherRootResident = resident(rootB.native_launch_id, "resident-b", "org-b", "work-b");
    const child = actor(first.native_launch_id, "actor-a", "org-b", "work-b");
    expect(() => validateLaunchCensus([rootA, rootB, first, otherRootResident, child])).toThrow("Cross-root");
    const validChild = actor(first.native_launch_id, "actor-b");
    expect(() => authorizeLaunchChild(rootB.native_launch_id, validChild, [rootA, first, validChild])).toThrow("Unknown launch caller");
  });

  it("rejects unknown schema fields and unavailable native context", () => {
    const value = root() as unknown as Record<string, unknown>;
    value.extra = true;
    expect(() => validateLaunchRecord(value)).toThrow("fields");
    expect(nativePlacementPathAvailable(["/definitely/missing/smarty-launch-context"])).toBe(false);
    expect(() => normalizeFabricConfig({ agents: { placementDecider: {
      command: ["/bin/true"], mode: "enforce", wakeSourceMachine: "host",
    } } })).toThrow("smarty-dev#14903");
  });
});
