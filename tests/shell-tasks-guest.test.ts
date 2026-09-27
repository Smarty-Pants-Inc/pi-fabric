import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";

describe.each(["typescript", "python"] as const)("%s shell task integration", kernel => {
  it("starts a monitor through the sandbox, discovers its handle and stops that same job", async () => {
    const jobs = new FabricShellJobStore(); const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(process.cwd(), undefined, undefined, { shellJobs: jobs, powerShellToolDefinitionFactory: undefined }));
    registry.register(new TasksProvider(jobs));
    const config = normalizeFabricConfig({ fullCodeMode: true, approvals: { read: "allow", execute: "allow" }, executor: { kernel, pythonRuntime: "monty" } });
    const service = new FabricExecutionService(registry, config);
    const context = { cwd: process.cwd(), sessionManager: { getSessionId: () => "guest-test", getSessionFile: () => undefined } } as unknown as ExtensionContext;
    try {
      const code = kernel === "typescript" ? `
const result = await pi.bash({cmd: "printf ready; sleep 8", description: "Guest watch", monitor: {delivery: "ui", timeoutMs: 10000}});
const taskId = result.details.taskId;
const tasks = await tools.call({ref: "tasks.list", args: {}});
const stopped = await tools.call({ref: "tasks.stop", args: {id: taskId}});
return {taskId, tasks, stopped};` : `
r = await pi.bash(command="printf ready; sleep 8", description="Guest watch", monitor={"delivery": "ui", "timeoutMs": 10000})
task_id = r["details"]["taskId"]
items = await tools.call(ref="tasks.list", args={})
stopped = await tools.call(ref="tasks.stop", args={"id": task_id})
return {"taskId": task_id, "tasks": items, "stopped": stopped}`;
      const result = await service.execute({ code, context, signal: undefined, parentToolCallId: "task-probe", onPartial() {} });
      expect(result.success, JSON.stringify(result)).toBe(true);
      expect(result.value).toMatchObject({ taskId: expect.any(String), tasks: [expect.objectContaining({ description: "Guest watch", monitor: expect.objectContaining({ delivery: "ui" }), ownerId: "guest-test" })], stopped: { stopped: true } });
      expect(jobs.list()).toHaveLength(1);
      expect(jobs.live().every(job => job.abort.signal.aborted)).toBe(true);
    } finally { await registry.close(); await jobs.close(); }
  }, 15000);
});
