import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { buildPlacementRequestDraft, placementRoleAllowed, resolvePlacementCallerContext, type PlacementCallerRecord } from "../src/agents/placement-context.js";
import { normalizePlacementDeciderConfig } from "../src/agents/placement-decider-config.js";

const roots: string[] = [];
const root = (): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "placement-context-"));
  roots.push(dir);
  return dir;
};
afterEach(() => { for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

const callerRecord = (): PlacementCallerRecord => ({
  native_launch_id: "native-parent", launch_spec_id: "spec-parent", generation: 2,
  fence: { token: "fence-token", state: "active" }, host: "ryzen2", unit: "pi-agent-native-parent-2.service",
  boot_id: "boot-id", invocation_id: "invocation-id", main_pid_start: "12345",
  org_id: "org-a", work_id: "smarty-dev#8031", parent_native_launch_id: "native-root",
  role: "worktree-agent", profile: "lane-complex", source_pin: "immutable-release", memory_profile: "measured-rss",
  origin: { class: "background-agent", origin_ref: "session:abc" },
});

const resolverConfig = (dir: string, plan: { code?: number; stdout?: string; record?: unknown; delay?: number }) => {
  const script = path.join(dir, "fake-context.cjs");
  const planFile = path.join(dir, "plan.json");
  const pidFile = path.join(dir, "caller-pid.txt");
  fs.writeFileSync(planFile, JSON.stringify(plan));
  fs.writeFileSync(script, `const fs = require('node:fs');
const plan = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
fs.writeFileSync(process.argv[3], process.argv[4] + ':' + process.argv[5]);
const done = () => { if (plan.invalidUtf8) process.stdout.write(Buffer.from([0xff])); else if (plan.stdout !== undefined) process.stdout.write(plan.stdout); else process.stdout.write(JSON.stringify(plan.record)); process.exitCode = plan.code || 0; };
if (plan.delay) setTimeout(done, plan.delay); else done();
`);
  const config = normalizePlacementDeciderConfig({ command: [process.execPath, script], contextCommand: [process.execPath, script, planFile, pidFile], mode: "enforce", wakeSourceMachine: "ryzen1" });
  return { config: config!, pidFile };
};

describe("trusted placement caller context", () => {
  it("passes this process PID (never a request PID) and trusts only a valid resolver record", async () => {
    const dir = root(); const { config, pidFile } = resolverConfig(dir, { record: callerRecord() });
    const result = await resolvePlacementCallerContext(config, performance.now() + 2_000);
    expect(result.record).toEqual({ trust: "trusted", value: callerRecord() });
    expect(result.callerPid).toBe(process.pid);
    expect(result.missingFields).toEqual([]);
    expect(fs.readFileSync(pidFile, "utf8")).toBe(`--caller-pid:${process.pid}`);
  });

  it.each([
    ["nonzero status", { code: 1, record: callerRecord() }, "unavailable"],
    ["missing record field", { record: (() => { const row = callerRecord(); delete (row as Partial<typeof row>).source_pin; return row; })() }, "invalid-response"],
    ["extra field", { record: { ...callerRecord(), prompt: "never accepted" } }, "invalid-response"],
    ["bad origin", { record: { ...callerRecord(), origin: { class: "unknown", origin_ref: "origin" } } }, "invalid-response"],
    ["terminal parent", { record: { ...callerRecord(), terminal: { outcome: "complete", at: "2026-10-18T00:00:00Z" } } }, "invalid-response"],
    ["duplicate key", { stdout: '{"native_launch_id":"a","native_launch_id":"b"}' }, "invalid-response"],
    ["invalid UTF-8", { invalidUtf8: true }, "invalid-response"],
  ] as const)("marks %s context missing rather than deriving identity", async (_label, plan, reason) => {
    const { config } = resolverConfig(root(), plan as { code?: number; stdout?: string; record?: unknown; invalidUtf8?: boolean });
    const result = await resolvePlacementCallerContext(config, performance.now() + 2_000);
    expect(result.record).toEqual({ trust: "missing", reason });
    expect(result.missingFields).toContain("org_id");
    expect(result.callerPid).toBe(process.pid);
  });

  it("marks absent command and expired aggregate deadline as missing", async () => {
    const config = normalizePlacementDeciderConfig({ command: [process.execPath], mode: "enforce", wakeSourceMachine: "ryzen1" });
    expect(await resolvePlacementCallerContext(config, performance.now() + 100)).toMatchObject({
      record: { trust: "missing", reason: "not-configured" },
    });
    const dir = root(); const configured = resolverConfig(dir, { record: callerRecord(), delay: 10_000 }).config;
    const result = await resolvePlacementCallerContext(configured, performance.now() + 1);
    expect(result.record).toEqual({ trust: "missing", reason: "timeout" });
  }, 5_000);

  it("inherits only trusted parent assignment/source/origin and constrains child roles by the two-hop grant", () => {
    const context: Awaited<ReturnType<typeof resolvePlacementCallerContext>> = {
      record: { trust: "trusted", value: callerRecord() }, callerPid: process.pid, missingFields: [],
    };
    const draft = buildPlacementRequestDraft(context, {
      launchId: "host-generated-launch", generation: 1, agentName: "fabric-child-host-generated-launch",
      child: { role: "task-agent", complexity: "complex", profile: "task-complex", memoryProfileRef: "rss-task-complex" },
      model: "provider/model", thinking: "xhigh", capabilities: ["corpus"],
    });
    expect(draft.roleAllowed).toBe(true);
    expect(draft.missingFields).toEqual([]);
    expect(draft.parentNativeLaunchId).toBe("native-parent");
    expect(draft.request).toMatchObject({
      launch_id: "host-generated-launch", generation: 1, agent_name: "fabric-child-host-generated-launch",
      assignment: { org_id: "org-a", work_id: "smarty-dev#8031" }, role: "task-agent", complexity: "complex",
      required_profile: "task-complex", interaction: { class: "background-agent", origin_ref: "session:abc" },
      requirements: { capabilities: ["corpus"], source_pin: "immutable-release", memory_profile_ref: "rss-task-complex" },
      requested: { model: "provider/model", thinking: "xhigh" },
    });
    expect(draft.trust["parent.org_id"]).toBe("trusted");
    expect(draft.trust["parent_native_launch_id"]).toBe("trusted");
    expect(placementRoleAllowed("project-agent", "review-agent")).toBe(true);
    expect(placementRoleAllowed("worktree-agent", "task-agent")).toBe(true);
    expect(placementRoleAllowed("worktree-agent", "review-agent")).toBe(false);
    expect(placementRoleAllowed("task-agent", "task-agent")).toBe(false);
    expect(placementRoleAllowed("resident-host", "actor-activation", { actorId: "actor-1", rootId: "root-1" })).toBe(true);
    expect(placementRoleAllowed("resident-host", "actor-activation")).toBe(false);
    expect(placementRoleAllowed("resident-host", "task-agent", undefined)).toBe(false);
  });

  it("preserves a live-human origin from the trusted parent record", () => {
    const parent = { ...callerRecord(), origin: { class: "live-human" as const, origin_ref: "session:live-main" } };
    const context: Awaited<ReturnType<typeof resolvePlacementCallerContext>> = {
      record: { trust: "trusted", value: parent }, callerPid: process.pid, missingFields: [],
    };
    const draft = buildPlacementRequestDraft(context, {
      launchId: "human-activation", generation: 1, agentName: "fabric-child-human-activation",
      child: { role: "task-agent", complexity: "normal", profile: "task", memoryProfileRef: "pi-normal" },
      capabilities: [],
    });
    expect(draft.request.interaction).toEqual({ class: "live-human", origin_ref: "session:live-main" });
    expect(draft.trust["parent.origin.class"]).toBe("trusted");
  });

  it("marks missing parent/request fields explicitly for shadow and never promotes request text", () => {
    const context = { record: { trust: "missing" as const, reason: "not-configured" as const }, callerPid: process.pid,
      missingFields: ["org_id", "work_id", "role", "profile", "source_pin", "memory_profile"] };
    const draft = buildPlacementRequestDraft(context, { launchId: "launch-id", generation: 3,
      agentName: "fabric-child-launch-id", capabilities: [], model: "provider/model" });
    expect(draft.roleAllowed).toBe(false);
    expect(draft.missingFields).toContain("parent.org_id");
    expect(draft.missingFields).toContain("child.role");
    expect(draft.request.assignment.org_id).toBe("MISSING:parent.org_id");
    expect(draft.request.role).toBe("MISSING:child.role");
    expect(draft.request.requirements.source_pin).toBe("MISSING:parent.source_pin");
    expect(draft.request.agent_name).toBe("fabric-child-launch-id");
    expect(JSON.stringify(draft.request)).not.toContain("task prompt");
  });
});
