import type { Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { describe, expect, it } from "vitest";
import type { AgentHandleInfo, AgentRunRecord } from "../src/agents/types.js";
import type { FabricState } from "../src/fabric-state.js";
import { DashboardDetailRenderer } from "../src/ui/dashboard-detail.js";
import { createDashboardSnapshot } from "../src/ui/snapshot.js";
import type { FabricDashboardSnapshot, FabricUiAgent } from "../src/ui/types.js";
import { heapRunResult } from "./fixtures/manager-heap-probe.js";

// Keep reason: #17018's trust-boundary failure must survive the manager ->
// dashboard projection and be visible in the actual task detail renderer.
// The paired manager test creates the real hard-linked, byte-evicted result.
const stateWith = (records: Array<AgentRunRecord | AgentHandleInfo>): FabricState => ({
  activity: { runs: () => [] },
  agents: {
    listForUi: () => records,
    list: () => { throw new Error("dashboard must use the isolated UI list"); },
  },
  actors: { list: () => [], instructions: () => "", messages: () => [] },
  globalActors: { list: () => [] },
  participantInfos: () => [],
  peerInfos: () => [],
  mainAgentInfo: () => ({
    id: "session:main", name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
    cwd: process.cwd(), sessionId: "main", startedAt: 1, updatedAt: 1, pendingMessages: false, local: true,
  }),
  config: { mesh: { enabled: false } },
  mesh: { list: () => [] },
}) as unknown as FabricState;

const detail = (snapshot: FabricDashboardSnapshot, agent: FabricUiAgent): string => {
  const theme = {
    fg: (_color: string, text: string) => text,
    bg: (_color: string, text: string) => text,
    bold: (text: string) => text,
  } as unknown as Theme;
  const renderer = new DashboardDetailRenderer(
    { requestRender: () => {}, terminal: { rows: 40 } } as unknown as TUI,
    theme, () => snapshot,
    { agentTranscript: undefined, actorTranscript: undefined, codePreviewSettings: undefined, actorDefaultTools: [] },
  );
  return renderer.render(200, snapshot, {
    id: agent.id, label: agent.name, status: agent.status, kind: "agent", value: agent,
  }, {
    view: "summary", scroll: 0, pageAnchor: undefined, transcriptFollowing: true, transcriptToolsExpanded: false,
  }, "", "").lines.join("\n");
};

describe("per-task result integrity errors in the dashboard (#17018)", () => {
  it("keeps legitimate full result payloads and structured values unchanged", () => {
    const healthy = { ...heapRunResult("healthy-full", 64), text: "genuine answer", value: { approved: true } };
    const snapshot = createDashboardSnapshot(stateWith([healthy]), []);
    expect(snapshot.agents).toEqual([expect.objectContaining({
      id: healthy.id, status: "completed", task: healthy.task, text: "genuine answer", value: healthy.value,
      startedAt: healthy.startedAt, updatedAt: healthy.updatedAt, finishedAt: healthy.finishedAt,
    })]);
    expect(snapshot.agents[0]!.value).not.toBe(healthy.value);
    expect(snapshot.agents[0]).not.toHaveProperty("error");
    expect(detail(snapshot, snapshot.agents[0]!)).toContain("genuine answer");
  });

  it.each(["queued", "running", "finishing", "completed", "failed", "stopped", "timed_out"] as const)(
    "shows an unreadable-result %s handle's explicit integrity failure without losing a healthy task or inventing a result", status => {
    const healthy = { ...heapRunResult("healthy-neighbour", 64), text: "genuine neighbour answer", value: { healthy: true } };
    const unreadable: AgentHandleInfo = {
      id: "hard-linked-result", name: "unreadable task", status, runner: "pi", transport: "process", cwd: process.cwd(),
      error: "Result authentication/integrity failure (FABRIC_MISSING_RESULT): no saved terminal result",
    };
    const snapshot = createDashboardSnapshot(stateWith([unreadable, healthy]), []);
    expect(snapshot.agents.find(agent => agent.id === healthy.id)).toMatchObject({ status: "completed", text: healthy.text, value: healthy.value });
    const failedRead = snapshot.agents.find(agent => agent.id === unreadable.id)!;
    expect(failedRead).toMatchObject({ id: unreadable.id, status, error: unreadable.error });
    for (const field of ["task", "text", "value", "usage", "turns", "toolCalls"]) expect(failedRead).not.toHaveProperty(field);
    const rendered = detail(snapshot, failedRead);
    expect(rendered).toContain("Error:");
    expect(rendered).toContain("authentication/integrity failure");
    expect(rendered).toContain("FABRIC_MISSING_RESULT");
    expect(rendered).not.toContain(healthy.text);
  });
});
