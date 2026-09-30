import type { Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { describe, expect, it } from "vitest";
import { DashboardDetailRenderer } from "../src/ui/dashboard-detail.js";
import type { Entity } from "../src/ui/dashboard-model.js";
import type { FabricDashboardSnapshot, FabricUiAgent } from "../src/ui/types.js";

const theme = {
  fg: (_color: string, text: string) => text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
} as unknown as Theme;

const snapshot = (): FabricDashboardSnapshot => ({
  now: 200,
  main: {
    id: "session:main", name: "Main", kind: "main", status: "idle",
    runner: "pi", transport: "host", cwd: "/tmp/project", sessionId: "main",
    startedAt: 100, updatedAt: 200, pendingMessages: false, local: true,
  },
  peers: [{
    id: "session:peer", name: "Peer capture", kind: "peer", status: "idle",
    runner: "pi", transport: "host", cwd: "/tmp/project", sessionId: "peer",
    startedAt: 100, updatedAt: 200, pendingMessages: false, local: false,
  }],
  participants: [{
    format: 1, id: "session:retained", rootId: "session:retained", kind: "root",
    name: "Retained capture", status: "idle", runner: "pi", transport: "host",
    ownerHostId: "host", ownerIdentityId: "session:retained", capabilities: [],
    startedAt: 100, updatedAt: 200, controlProtocol: "v1", local: false, stale: true,
  }],
  agents: [], actors: [], globalActors: [], runs: [], state: [], events: [],
  componentGraph: { components: [], edges: [], cycles: [] },
});

const render = (current: FabricDashboardSnapshot, entity: Entity): string[] => {
  const renderer = new DashboardDetailRenderer(
    { requestRender: () => {}, terminal: { rows: 40 } } as unknown as TUI,
    theme, () => current,
    { agentTranscript: undefined, actorTranscript: undefined, codePreviewSettings: undefined, actorDefaultTools: [] },
  );
  return renderer.render(160, current, entity, {
    view: "summary", scroll: 0, pageAnchor: undefined,
    transcriptFollowing: true, transcriptToolsExpanded: false,
  }, "", "").lines;
};

const agentEntity = (rootId?: string): Entity => {
  const value: FabricUiAgent = {
    id: "agent-1", name: "task capture", status: "running", runner: "pi",
    transport: "process", cwd: "/tmp/project", local: true, ...(rootId ? { rootId } : {}),
  };
  return { id: value.id, label: value.name, status: value.status, kind: "agent", value };
};

describe("dashboard agent run detail root", () => {
  it.each([
    ["session:main", "session:main (Main)"],
    ["session:peer", "session:peer (Peer capture)"],
    ["session:retained", "session:retained (Retained capture)"],
    ["session:unobserved", "session:unobserved"],
    [undefined, "unknown"],
  ])("shows the recorded root %s without guessing from local ownership", (rootId, expected) => {
    const lines = render(snapshot(), agentEntity(rootId));
    expect(lines.filter((line) => line.includes("Root:"))).toHaveLength(1);
    expect(lines.join("\n")).toContain(`Root: ${expected}`);
  });

  it("leaves mesh participant root and ownership fields unchanged", () => {
    const current = snapshot();
    const participant = current.participants![0]!;
    const lines = render(current, {
      kind: "meshParticipant", id: participant.id, label: participant.name,
      status: participant.status,
      value: { id: participant.id, entityId: participant.id, name: participant.name, status: participant.status,
        routes: 0, lastSeenAt: 200, participant },
    });
    const fields = lines.slice(1).map((line) => line.slice(1, -1).trim()).filter((line) => line.includes(":"));
    expect(fields).toEqual([
      "Status: idle", "Scope: project root", "Identity: session:retained", "Root: session:retained",
      "Owner host: host", "Owner identity: session:retained", "Residency: session", "Runner: pi",
      "Transport: host", "Local: no", "Observed routes: 0", `Last activity: ${new Date(200).toLocaleString()}`,
    ]);
  });
});
