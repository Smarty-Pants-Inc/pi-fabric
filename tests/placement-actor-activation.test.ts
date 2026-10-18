import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { buildPlacementRequestDraft, resolvePlacementCallerContext } from "../src/agents/placement-context.js";
import { normalizePlacementDeciderConfig } from "../src/agents/placement-decider-config.js";
import { authorizeLaunchChild, LaunchRecordStore, makeLaunchRecord, type LaunchRecord } from "../src/residency/launch-record.js";

const roots: string[] = [];
const root = (): string => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-placement-")); roots.push(value); return value; };
afterEach(() => { for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true }); });

const started = (store: LaunchRecordStore, record: LaunchRecord): LaunchRecord => {
  store.prepare(record); store.transition(record.native_launch_id, record.generation, "creating");
  return store.transition(record.native_launch_id, record.generation, "closed-started", {
    boot_id: "12345678-1234-1234-1234-123456789abc", invocation_id: "1234567890abcdef1234567890abcdef", main_pid_start: "123:456",
  });
};
const contextBoundary = (directory: string, initial: unknown) => {
  const plan = path.join(directory, "context.json");
  const helper = path.join(directory, "smarty-launch-context");
  const callerPid = path.join(directory, "caller-pid");
  fs.writeFileSync(plan, JSON.stringify(initial));
  fs.writeFileSync(helper, `#!${process.execPath}\nconst fs = require('node:fs'); const callerIndex = process.argv.indexOf('--caller-pid'); if (callerIndex < 0 || !/^\\d+$/.test(process.argv[callerIndex + 1])) process.exit(9); fs.writeFileSync(${JSON.stringify(callerPid)}, process.argv[callerIndex + 1]); const row = JSON.parse(fs.readFileSync(process.argv[2], 'utf8')); delete row.schema; process.stdout.write(JSON.stringify(row));\n`, { mode: 0o700 });
  fs.chmodSync(helper, 0o700);
  const config = normalizePlacementDeciderConfig({ command: [process.execPath], contextCommand: [helper, plan, "--parent-native-launch-id", "forged-parent", "--root-id", "forged-root"], mode: "enforce", wakeSourceMachine: "ryzen1" });
  return { plan, callerPid, config: config! };
};
const rootRecord = (id: string, org: string, work: string, role: "org-agent" | "project-agent" = "org-agent") =>
  makeLaunchRecord({ launchSpecId: "root-spec", orgId: org, workId: work, role, profile: "root", sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: id } }, id, 1);
const residentRecord = (parent: string, id: string, org: string, work: string) =>
  makeLaunchRecord({ launchSpecId: "resident-spec", orgId: org, workId: work, parentNativeLaunchId: parent, role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident", origin: { class: "background-agent", originRef: id } }, id, 1);
const actorRecord = (parent: string, id: string, org: string, work: string) =>
  makeLaunchRecord({ launchSpecId: "actor-spec", orgId: org, workId: work, parentNativeLaunchId: parent, role: "actor-activation", profile: "actor", sourcePin: "pin", memoryProfile: "actor", origin: { class: "background-agent", originRef: id } }, id, 1);

const draftFor = async (config: ReturnType<typeof normalizePlacementDeciderConfig>, actorRoot = "root-a") => {
  const context = await resolvePlacementCallerContext(config, performance.now() + 2_000);
  return { context, draft: buildPlacementRequestDraft(context, {
    launchId: "actor-launch", generation: 1, agentName: "actor-activation", capabilities: [],
    child: { role: "actor-activation", actorActivation: { actorId: "actor-1", rootId: actorRoot }, complexity: "normal", profile: "actor", memoryProfileRef: "actor" },
  }) };
};

describe("actor activation trusted placement context", () => {
  it("admits an activation whose resolved parent is a same-root resident host", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const rootA = started(store, rootRecord("root-a", "org-a", "work-a"));
    const residentA = started(store, residentRecord(rootA.native_launch_id, "resident-a", "org-a", "work-a"));
    const boundary = contextBoundary(directory, residentA);
    const { context, draft } = await draftFor(boundary.config);
    expect(context.callerPid).toBe(process.pid);
    expect(fs.readFileSync(boundary.callerPid, "utf8")).toBe(String(process.pid));
    expect(context.record).toMatchObject({ trust: "trusted", value: { native_launch_id: residentA.native_launch_id } });
    expect(draft.parentNativeLaunchId).toBe(residentA.native_launch_id);
    expect(draft.roleAllowed).toBe(true);
    expect(() => authorizeLaunchChild(residentA.native_launch_id, actorRecord(residentA.native_launch_id, "actor-a", "org-a", "work-a"), [rootA, residentA])).not.toThrow();
  });

  it("refuses a resolved parent from a different root", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const rootB = started(store, rootRecord("root-b", "org-b", "work-b"));
    const residentB = started(store, residentRecord(rootB.native_launch_id, "resident-b", "org-b", "work-b"));
    const boundary = contextBoundary(directory, residentB);
    const { draft } = await draftFor(boundary.config, "root-a");
    expect(draft.parentNativeLaunchId).toBe(residentB.native_launch_id);
    expect(() => authorizeLaunchChild(residentB.native_launch_id, actorRecord(residentB.native_launch_id, "actor-a", "org-a", "work-a"), [rootB, residentB])).toThrow("Cross-root");
  });

  it("refuses a non-resident or unknown trusted context", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const project = started(store, rootRecord("project-a", "org-a", "work-a", "project-agent"));
    const nonResident = contextBoundary(directory, project);
    const nonResidentDraft = await draftFor(nonResident.config);
    expect(nonResidentDraft.draft.roleAllowed).toBe(false);
    fs.writeFileSync(nonResident.plan, JSON.stringify({ nope: true }));
    const unknown = await draftFor(nonResident.config);
    expect(unknown.context.record).toMatchObject({ trust: "missing" });
    expect(unknown.draft.missingFields).toContain("parent.native_launch_id");
    expect(unknown.draft.roleAllowed).toBe(false);
  });

  it("uses the resolved parent rather than forged worker-side actor/root ids", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const rootA = started(store, rootRecord("root-a", "org-a", "work-a"));
    const residentA = started(store, residentRecord(rootA.native_launch_id, "resident-a", "org-a", "work-a"));
    const boundary = contextBoundary(directory, residentA);
    const { draft } = await draftFor(boundary.config, "forged-root-from-worker-arguments");
    expect(draft.parentNativeLaunchId).toBe(residentA.native_launch_id);
    expect(draft.request.assignment).toEqual({ org_id: "org-a", work_id: "work-a" });
    expect(draft.request.interaction.origin_ref).toBe("resident-a");
  });
});
