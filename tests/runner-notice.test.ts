import { describe, expect, it } from "vitest";
import { unregisteredRunnerNotice } from "../src/agents/runner-notice.js";
import { registerAgentRunner, type FabricRunnerCapabilities } from "../src/runners.js";

const NONE: FabricRunnerCapabilities = {
  recursiveFabric: false, steer: false, followUp: false, persistentSessions: false,
  kernels: false, handoff: false, modelDiscovery: false, imageInput: false,
  compaction: false, questions: false, sleep: false, writePolicy: false,
};

describe("unregistered runner notice", () => {
  it("stays quiet for built-in and registered runners", () => {
    expect(unregisteredRunnerNotice("pi", [])).toBeUndefined();
    expect(unregisteredRunnerNotice("pi-durable", [])).toBeUndefined();
    expect(unregisteredRunnerNotice("veda", [])).toBeUndefined();
    expect(unregisteredRunnerNotice("acme-daemon", ["acme-daemon"])).toBeUndefined();
  });

  it("names the runner, suggests a close id, and lists what is registered", () => {
    const notice = unregisteredRunnerNotice("claud", ["acme-daemon"]);
    expect(notice).toContain('agents.runner "claud" is not registered');
    expect(notice).toContain('Did you mean "claude"?');
    expect(notice).toContain("Registered runners: pi, pi-durable, claude, veda, acme-daemon.");
    expect(unregisteredRunnerNotice("acme-deamon", ["acme-daemon"])).toContain('Did you mean "acme-daemon"?');
    expect(unregisteredRunnerNotice("something-else", [])).not.toContain("Did you mean");
  });

  it("reads the same globalThis registry that registerAgentRunner writes", () => {
    const registry = () => (globalThis as Record<symbol, Map<string, unknown> | undefined>)[
      Symbol.for("pi-fabric.runnerRegistry.v1")
    ];
    const unregister = registerAgentRunner({
      kind: "worker",
      id: "notice-probe",
      label: "Notice probe",
      capabilities: NONE,
      launch: ({ fabricWorker }) => ({ workerPath: fabricWorker.workerPath, workerArguments: [...fabricWorker.workerArguments] }),
    });
    try {
      expect(registry()?.has("notice-probe")).toBe(true);
      expect(unregisteredRunnerNotice("notice-probe", registry()!.keys())).toBeUndefined();
    } finally {
      unregister();
    }
  });
});
