import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { createAgentServiceClient, createAgentServiceHandler, createAgentsProvider } from "../src/agents/service-provider.js";
import { AgentService } from "../src/agents/service.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";

// Synthetic replay of the nine-wrong-identifiers failure class in smarty-dev#1612,
// not purported copies of the historical identifiers.
const replay = [
  ["qualified session", "session:01a0cd9c-7c24-72d5-ae80-03d729baf901"],
  ["bare UUIDv7 session", "01a0cd9c-7c24-72d5-ae80-03d729baf902"],
  ["actor", "aaaa1111bbbb2222cccc3333dddd4444"],
  ["run", "bbbb1111cccc2222dddd3333eeee4444"],
  ["full SHA", "commit 1234567890abcdef1234567890abcdef12345678"],
  ["short SHA", "head abc1234"],
  ["backticked SHA", "`def567890abc`"],
  ["comment", "#issuecomment-1234567890"],
  ["pid", "pid 987654"],
] as const;
const issueReferences = [
  ["qualified issue", "Smarty-Pants-Inc/pi-fabric#2175", "Smarty-Pants-Inc/pi-fabric#2175"],
  ["bare issue", "#2175", "#2175"],
  ["issue URL", "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/2175", "Smarty-Pants-Inc/pi-fabric#2175"],
  ["pull URL", "https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2175", "Smarty-Pants-Inc/pi-fabric#2175"],
  ["anchored issue URL", "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/2175#issuecomment-1234567890", "Smarty-Pants-Inc/pi-fabric#2175, comment 1234567890"],
] as const;
const mixedIssueReads = [
  ["#2175", "Smarty-Pants-Inc/pi-fabric#2175"],
  ["#2175", "another/repository#2175"],
  ["Smarty-Pants-Inc/pi-fabric#2175", "#2175"],
  ["Smarty-Pants-Inc/pi-fabric#2175", "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/2175"],
  ["Smarty-Pants-Inc/pi-fabric#2175", "https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2175"],
  ["https://github.com/Smarty-Pants-Inc/pi-fabric/issues/2175", "Smarty-Pants-Inc/pi-fabric#2175"],
  ["https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2175", "#2175"],
] as const;
// Only comment and pid accept hash separators in the existing legacy parser.
const legacyHashReferences = [
  ["comment #1234567890", "comment 1234567890", "1234567890"],
  ["comment#1234567890", "comment 1234567890", "1234567890"],
  ["COMMENT #123456789", "comment 123456789", "123456789"],
  ["comment\t#1234567890", "comment 1234567890", "1234567890"],
  ["comment:#1234567890", "comment 1234567890", "1234567890"],
  ["comment=#1234567890", "comment 1234567890", "1234567890"],
  ["comment-#1234567890", "comment 1234567890", "1234567890"],
  ["comment #=#1234567890", "comment 1234567890", "1234567890"],
  ["pid #987654", "pid 987654", "987654"],
  ["pid#987654", "pid 987654", "987654"],
  ["PID #123", "pid 123", "123"],
  ["pid\t#987654", "pid 987654", "987654"],
  ["pid:#987654", "pid 987654", "987654"],
  ["pid=#987654", "pid 987654", "987654"],
  ["pid-#987654", "pid 987654", "987654"],
  ["pid #=#987654", "pid 987654", "987654"],
  ["pid #1", "pid 1", "1"],
  ["pid #12", "pid 12", "12"],
  ["pid #1234567890", "pid 1234567890", "1234567890"],
] as const;
const surfaces = ["legacy.steer", "legacy.followUp", "hosted.steer", "hosted.followUp", "mesh.publish"] as const;
const session = () => SessionManager.inMemory(process.cwd());
const invocation = (manager: SessionManager): FabricInvocationContext => ({
  cwd: process.cwd(), signal: undefined, parentToolCallId: "outer", nestedToolCallId: "nested", update() {},
  extensionContext: { sessionManager: manager } as unknown as ExtensionContext,
});
const read = (manager: SessionManager, text: string, toolName = "read") => manager.appendMessage({
  role: "toolResult", toolCallId: "read-call", toolName, content: [{ type: "text", text }],
  isError: false, timestamp: 1,
});
const harness = (surface: typeof surfaces[number], manager = session(), routeLimit = false) => {
  const sent: string[] = [];
  const accept = (text: string) => {
    if (routeLimit && text.includes("unverified ids:")) throw new Error("Mesh event exceeds 262144 bytes");
    sent.push(text);
  };
  const ack = { queued: true as const, messageId: "ack", routed: "main" as const };
  let provider;
  if (surface.startsWith("legacy.")) {
    type Ports = ConstructorParameters<typeof AgentsProvider>;
    provider = new AgentsProvider({} as Ports[0], {
      identity: { id: "session:sender", name: "Sender", kind: "main" },
    } as Ports[1], {} as Ports[2], {
      id: "main", local: true, matches: (id: string) => id === "main",
      deliverAgent: ({ message }: { message: string }) => { accept(message); return ack; },
    } as unknown as Ports[3], { get: () => undefined } as unknown as Ports[4], undefined, {} as Ports[6]);
  } else if (surface.startsWith("hosted.")) {
    provider = createAgentsProvider(createAgentServiceClient(async (_action, args) => {
      accept(String(args.message)); return ack;
    }, { steer: true, followUp: true }));
  } else {
    type Ports = ConstructorParameters<typeof MeshProvider>;
    provider = new MeshProvider({ publish: async (args: { text: string }) => {
      accept(args.text); return { sequence: 1, ...args };
    } } as unknown as Ports[0], { id: "session:sender", name: "Sender", kind: "main" }, {} as Ports[2]);
  }
  const context = invocation(manager);
  const action = surface.split(".")[1]!;
  return { sent, manager, context, provider, send: (text: string) => provider.invoke(action,
    action === "publish" ? { topic: "team", text, data: { untouched: true } } : { id: "main", message: text, ...(!surface.startsWith("hosted.") ? { data: { untouched: true } } : {}) }, context) };
};

describe("legacy routes and durable mesh delivery", () => {
  it.each(["steer", "followUp"] as const)("preserves notices and data on every %s routing branch", async action => {
    type Ports = ConstructorParameters<typeof AgentsProvider>;
    const sent: Array<{ route: string; message: string; data: unknown }> = [];
    const deliver = (route: string, message: string, data: unknown) => {
      sent.push({ route, message, data }); return { queued: true as const, messageId: route, routed: "local" as const };
    };
    const remote = (id: string, kind: FabricParticipantInfo["kind"], legacy = false) => ({ id, kind, local: false,
      capabilities: ["steer", "followUp"], ownerHostId: "owner", ownerIdentityId: "owner-id", controlProtocol: legacy ? "legacy" : "v1" }) as FabricParticipantInfo;
    const participants = [remote("remote-root", "root"), remote("remote-child", "agent"), remote("remote-actor", "actor"), remote("legacy-actor", "actor", true), remote("legacy-root", "root", true)];
    const provider = new AgentsProvider({
      status: (id: string) => { if (id !== "child") throw new Error(`Unknown Fabric agent: ${id}`); return { id, name: "Child" }; },
      steer: (_id: string, message: string, data: unknown) => deliver("child", message, data),
      followUp: (_id: string, message: string, data: unknown) => deliver("child", message, data),
    } as unknown as Ports[0], {
      identity: { id: "sender", name: "Sender", kind: "main" }, validateDirectMessage() {},
      status: (id: string) => { if (id !== "actor") throw new Error(`Unknown Fabric actor: ${id}`); return { id, name: "Actor", runner: "pi" }; },
      tell: (_id: string, message: string, data: unknown) => deliver("actor", message, data),
      steerRemote: (_id: string, message: string, _kind: string, data: unknown) => deliver("legacy-actor", message, data),
    } as unknown as Ports[1], {} as Ports[2], {
      id: "main", local: true, matches: (id: string) => id === "main",
      deliverAgent: ({ message, data }: { message: string; data: unknown }) => deliver("main", message, data),
    } as unknown as Ports[3], { get: (id: string) => participants.find(p => p.id === id) } as unknown as Ports[4], {
      request: async (_host: string, id: string, _kind: string, args: { message: string; data: unknown }) => deliver(id, args.message, args.data),
    } as unknown as Ports[5], {} as Ports[6]);
    const context = invocation(session());
    const data = { untouched: "head def5678" };
    for (const id of ["main", "child", "actor", "remote-root", "remote-child", "remote-actor", "legacy-actor"]) {
      expect(await provider.invoke(action, { id, message: "head abc1234", data }, context)).toHaveProperty("notice", "unverified ids: abc1234");
    }
    await expect(provider.invoke(action, { id: "legacy-root", message: "head abc1234", data }, context)).rejects.toThrow("no control channel");
    expect(sent.map(({ route }) => route)).toEqual(["main", "child", "actor", "remote-root", "remote-child", "remote-actor", "legacy-actor"]);
    expect(sent.every(item => item.message === "head abc1234\n\nunverified ids: abc1234" && item.data === data)).toBe(true);
    const bypass = await provider.routeMessage("main", "head def5678", data, action); // Host lifecycle; no model sender context.
    expect(bypass).not.toHaveProperty("notice");
    expect(sent.at(-1)?.message).toBe("head def5678");
  });

  it("persists the mesh marker, leaves target/data alone, and accepts a later actual mesh read", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "id-mesh-"));
    try {
      type Ports = ConstructorParameters<typeof MeshProvider>;
      const store = new MeshStore(root, 64 * 1024, 100);
      const provider = new MeshProvider(store, { id: "sender", name: "Sender", kind: "main" }, {} as Ports[2]);
      const manager = session();
      const context = invocation(manager);
      const data = { untouched: "head def5678" };
      const receipt = await provider.invoke("publish", { topic: "team", to: "unknown-target", text: "head abc1234", data }, context);
      expect(receipt).toHaveProperty("notice", "unverified ids: abc1234");
      const events = await provider.invoke("read", { topic: "team" }, context);
      expect(events).toEqual([expect.objectContaining({ text: "head abc1234\n\nunverified ids: abc1234", to: "unknown-target", data })]);
      expect((events as object[])[0]).not.toHaveProperty("notice"); // Notice is receipt-only; marker is durable text.
      read(manager, JSON.stringify(receipt), "fabric_exec");
      expect(await provider.invoke("publish", { topic: "team", text: "head abc1234" }, context)).toHaveProperty("notice");
      read(manager, JSON.stringify(events), "fabric_exec");
      expect(await provider.invoke("publish", { topic: "team", text: "head abc1234" }, context)).not.toHaveProperty("notice");
      expect(await provider.invoke("publish", { topic: "team", data }, context)).not.toHaveProperty("notice");
      expect(await provider.invoke("publish", { topic: "team", text: "Ready", data }, context)).not.toHaveProperty("notice");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("hosted service end-to-end identifier delivery", () => {
  it.each(["steer", "followUp"] as const)("annotates hosted %s through both child and peer delivery ports", async action => {
    const messages: Array<{ route: string; message: string }> = [];
    let entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const peer = { id: "peer", name: "Peer", kind: "root" as const, status: "idle", capabilities: ["steer", "followUp"] as Array<"steer" | "followUp"> };
    const service = new AgentService({ rootId: "root", port: {
      execute: request => new Promise(resolve => {
        request.signal.addEventListener("abort", () => resolve({ status: "stopped" }), { once: true });
        entered();
      }),
      steer: async ({ message }) => { messages.push({ route: "child", message }); },
      followUp: async ({ message }) => { messages.push({ route: "child", message }); },
    }, topology: {
      self: () => peer, sessions: () => [peer], peers: () => [peer],
      deliver: async ({ message }) => { messages.push({ route: "peer", message }); return peer; },
    } });
    try {
      const client = createAgentServiceClient(createAgentServiceHandler(service, "root"), service.capabilities);
      const provider = createAgentsProvider(client);
      const child = await client.spawn({ task: "synthetic port only, no model/process" });
      await ready;
      const manager = session();
      for (const id of [child.id, peer.id]) {
        expect(await provider.invoke(action, { id, message: "head abc1234" }, invocation(manager)))
          .toHaveProperty("notice", "unverified ids: abc1234");
      }
      expect(messages).toEqual(["child", "peer"].map(route => ({ route, message: "head abc1234\n\nunverified ids: abc1234" })));
      read(manager, "abc1234fedcba9876543210123456789abcdefff", "bash");
      expect(await provider.invoke(action, { id: peer.id, message: "head abc1234" }, invocation(manager))).not.toHaveProperty("notice");
      expect(messages.at(-1)).toEqual({ route: "peer", message: "head abc1234" });
      expect(await provider.invoke("peers", {}, invocation(manager))).toEqual([peer]);
    } finally { await service.close(); }
  });

  it("does not broaden hosted capabilities or hide real delivery failures", async () => {
    const dispatch = vi.fn(async () => { throw new Error("delivery failed"); });
    const manager = session();
    const unsupported = createAgentsProvider(createAgentServiceClient(dispatch));
    await expect(unsupported.invoke("followUp", { id: "main", message: "head abc1234" }, invocation(manager)))
      .rejects.toThrow("Unsupported hosted agents action");
    expect(dispatch).not.toHaveBeenCalled();
    const enabled = createAgentsProvider(createAgentServiceClient(dispatch, { followUp: true }));
    await expect(enabled.invoke("followUp", { id: "main", message: "head abc1234" }, invocation(manager))).rejects.toThrow("delivery failed");
    expect(dispatch).toHaveBeenCalledWith("followUp", { id: "main", message: "head abc1234\n\nunverified ids: abc1234" }, undefined);
  });
});

// Count actual regex yields and span-position reads, not elapsed time. The
// test fuse stops the old quadratic loop deterministically before it hangs CI.
const countedMatchWork = async (label: string, run: () => Promise<void>) => {
  const original = String.prototype.matchAll;
  const counts = { matches: 0, positions: 0, total: 0 };
  const tick = (kind: "matches" | "positions") => {
    counts[kind]++;
    if (++counts.total > 120_000) throw new Error("Test match-work fuse exceeded");
  };
  const spy = vi.spyOn(String.prototype, "matchAll").mockImplementation(function (this: string, regex: RegExp) {
    const matches = original.call(this, regex);
    return (function* () {
      for (const match of matches) {
        tick("matches");
        const index = match.index;
        Object.defineProperty(match, "index", { get: () => { tick("positions"); return index; } });
        yield match;
      }
    })() as RegExpStringIterator<RegExpExecArray>;
  });
  try { await run(); } finally {
    spy.mockRestore();
    console.info("MATCH_WORK " + JSON.stringify({ label, ...counts }));
  }
  return counts;
};

describe.each(surfaces)("unverified identifier annotations: %s", surface => {
  it.each(["outgoing", "history"] as const)("round-3 repeated legacy spans: %s is linear and delivered", async mode => {
    const h = harness(surface);
    const repeated = "comment #123456789 ".repeat(10_000);
    if (mode === "history") read(h.manager, repeated);
    const text = mode === "outgoing" ? repeated : "#123456789";
    const notice = mode === "outgoing" ? "unverified ids: comment 123456789" : "unverified ids: #123456789";
    const counts = await countedMatchWork(`${surface} repeated ${mode}`, async () => {
      const result = await h.send(text);
      expect(h.sent).toEqual([`${text}\n\n${(result as { notice?: string }).notice}`]);
      expect(result).toHaveProperty("notice", notice);
    });
    expect(counts.total).toBeLessThanOrEqual(120_000);
  });

  it.each(["outgoing", "history", "aggregate history"] as const)("round-3 match budget: %s fails open and delivers", async mode => {
    const h = harness(surface);
    const repeated = "comment #123456789 ".repeat(mode === "outgoing" ? 13_500 : 10_000);
    if (mode === "history") read(h.manager, repeated.repeat(10));
    if (mode === "aggregate history") { read(h.manager, repeated); read(h.manager, repeated); }
    const text = mode === "outgoing" ? repeated : "#123456789";
    const counts = await countedMatchWork(`${surface} budget ${mode}`, async () => {
      const notice = "unverified ids: check failed";
      expect(await h.send(text)).toHaveProperty("notice", notice);
      expect(h.sent).toEqual([`${text}\n\n${notice}`]);
    });
    expect(counts.total).toBeLessThanOrEqual(120_000);
  });
  it.each(replay)("delivers and reports the synthetic wrong %s", async (_kind, text) => {
    const h = harness(surface);
    const result = await h.send(text) as { notice?: string };
    expect(result.notice).toMatch(/^unverified ids: .+/);
    expect(result.notice).not.toContain("check failed");
    expect(h.sent).toEqual([`${text}\n\n${result.notice}`]);
    expect(result.notice?.split("\n")).toHaveLength(1);
  });

  it.each(issueReferences)("flags an unread GitHub %s and clears it after a read", async (_kind, text, display) => {
    const h = harness(surface);
    const notice = `unverified ids: ${display}`;
    expect(await h.send(text)).toHaveProperty("notice", notice);
    expect(h.sent).toEqual([`${text}\n\n${notice}`]);
    read(h.manager, text);
    expect(await h.send(text)).not.toHaveProperty("notice");
    expect(h.sent.at(-1)).toBe(text);
  });

  it.each(legacyHashReferences.flatMap(([text, display, value]) =>
    [false, true].map(alreadyRead => ({ text, display, value, alreadyRead }))))(
    "round-2 legacy hash: $text alreadyRead=$alreadyRead belongs only to its own class", async ({ text, display, value, alreadyRead }) => {
      const h = harness(surface);
      if (alreadyRead) read(h.manager, JSON.stringify({ id: Number(value), pid: Number(value) }));
      const result = await h.send(text);
      if (alreadyRead) {
        expect(result).not.toHaveProperty("notice");
        expect(h.sent).toEqual([text]);
      } else {
        const notice = `unverified ids: ${display}`;
        expect(result).toHaveProperty("notice", notice);
        expect(h.sent).toEqual([`${text}\n\n${notice}`]);
      }
    });

  it.each(legacyHashReferences.filter(([, , value]) => value.length >= 3))(
    "round-2 legacy read: %s is not independent issue evidence", async (evidence, _display, value) => {
      const h = harness(surface);
      read(h.manager, evidence);
      const text = `#${value}; owner/repo#${value}; https://github.com/owner/repo/issues/${value}`;
      const notice = `unverified ids: #${value}, owner/repo#${value}`;
      expect(await h.send(text)).toHaveProperty("notice", notice);
      expect(h.sent).toEqual([`${text}\n\n${notice}`]);
      // A real reference later in the same read must still count.
      read(h.manager, `${evidence}; #${value}`);
      expect(await h.send(text)).not.toHaveProperty("notice");
      expect(h.sent.at(-1)).toBe(text);
    });

  it("round-2 real issues: class-named qualified repos and unsupported hash labels remain issues", async () => {
    const h = harness(surface);
    const text = "owner/comment#1234567890; owner/pid#987654; comment #2175; actor #2176; run #2177; session #2178; sha #2179; commit #2180; head #2181; base #2182; revision #2183; rev #2184";
    const notice = "unverified ids: owner/comment#1234567890, comment 1234567890, owner/pid#987654, pid 987654, #2175, #2176, #2177, #2178, #2179, #2180, #2181, #2182, #2183, #2184";
    expect(await h.send(text)).toHaveProperty("notice", notice);
    expect(h.sent).toEqual([`${text}\n\n${notice}`]);
  });
  it.each(mixedIssueReads)("normalizes mixed GitHub forms: outgoing %s, read %s", async (text, evidence) => {
    const h = harness(surface);
    read(h.manager, evidence);
    expect(await h.send(text)).not.toHaveProperty("notice");
    expect(h.sent).toEqual([text]);
  });

  it("round-1 omission: explains the missing recipient marker and logs each occurrence", async () => {
    const log = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const h = harness(surface, session(), true);
      const text = "o/pi-fabric#179";
      const omitted = "unverified ids: o/pi-fabric#179 (recipient marker omitted: message at the route size limit)";
      expect(await h.send(text)).toHaveProperty("notice", omitted);
      expect(await h.send(text)).toHaveProperty("notice", omitted);
      expect(h.sent).toEqual([text, text]); // One unmarked delivery per occurrence.
      expect(log).toHaveBeenCalledTimes(2);
      const rows = log.mock.calls.map(([line]) => JSON.parse(String(line).slice("[pi-fabric] ".length)));
      const route = surface === "mesh.publish" ? surface : `agents.${surface.split(".")[1]}`;
      expect(rows).toEqual([
        { event: "recipient-marker-omitted", route, count: expect.any(Number) },
        { event: "recipient-marker-omitted", route, count: rows[0].count + 1 },
      ]);
      const normal = harness(surface);
      expect(await normal.send(text)).toHaveProperty("notice", "unverified ids: o/pi-fabric#179");
      expect(await normal.send("Ready")).not.toHaveProperty("notice");
      expect(log).toHaveBeenCalledTimes(2); // No count/log for normal marked or no-ID delivery.
    } finally { log.mockRestore(); }
  });

  it("round-1 repository-only: flags unread references and clears finalized reads", async () => {
    for (const text of ["smarty-dev#2175", "pi-fabric#179", "x#123"]) {
      const h = harness(surface);
      const notice = `unverified ids: ${text}`;
      expect(await h.send(text)).toHaveProperty("notice", notice);
      expect(h.sent).toEqual([`${text}\n\n${notice}`]);
      read(h.manager, text);
      expect(await h.send(text)).not.toHaveProperty("notice");
      expect(h.sent.at(-1)).toBe(text);
    }
  });

  it("round-1 repository-only: normalizes reads across bare, qualified and URL forms", async () => {
    const forms = ["#2175", "pi-fabric#2175", "Smarty-Pants-Inc/pi-fabric#2175",
      "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/2175",
      "https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2175"];
    for (const text of forms) for (const evidence of forms) {
      const h = harness(surface);
      read(h.manager, evidence);
      expect(await h.send(text), `${text} after ${evidence}`).not.toHaveProperty("notice");
      expect(h.sent).toEqual([text]);
    }
  });

  it("round-1 repository-only: preserves repo and qualified owner identity", async () => {
    for (const [text, evidence] of [
      ["pi-fabric#2175", "smarty-dev#2175"],
      ["pi-fabric#2175", "Smarty-Pants-Inc/smarty-dev#2175"],
      ["Smarty-Pants-Inc/pi-fabric#2175", "other/pi-fabric#2175"],
      ["Smarty-Pants-Inc/pi-fabric#2175", "https://github.com/other/pi-fabric/pull/2175"],
      ["pi-fabric#2175", "pi-fabric#21750 pi-fabric#2175suffix pi-fabric#2175-thing"],
      ["Smarty-Pants-Inc/pi-fabric#12", "pi-fabric#12 #12"],
    ]) {
      const h = harness(surface);
      read(h.manager, evidence!);
      expect(await h.send(text!)).toHaveProperty("notice", `unverified ids: ${text}`);
      expect(h.sent).toEqual([`${text}\n\nunverified ids: ${text}`]);
    }
    for (const evidence of ["another/pi-fabric#2175", "https://github.com/another/pi-fabric/pull/2175", "smarty-dev#2175"]) {
      const h = harness(surface);
      read(h.manager, evidence);
      const text = evidence.includes("smarty-dev") ? "#2175" : "pi-fabric#2175";
      expect(await h.send(text)).not.toHaveProperty("notice");
      expect(h.sent).toEqual([text]);
    }
  });

  it("does not let an issue send receipt launder an unread GitHub reference", async () => {
    const h = harness(surface);
    const text = "Smarty-Pants-Inc/pi-fabric#2175";
    const first = await h.send(text);
    read(h.manager, JSON.stringify({ text: h.sent[0], ...first as object }), "fabric_exec");
    expect(await h.send(text)).toHaveProperty("notice", `unverified ids: ${text}`);
  });

  it("does not annotate real reads from agents list/peers/create, git or the GitHub API", async () => {
    const manager = session();
    read(manager, 'agents.list: [{"id":"aaaa1111bbbb2222cccc3333dddd4444"}]', "fabric_exec");
    read(manager, 'agents.peers: [{"id":"session:01a0cd9c-7c24-72d5-ae80-03d729baf901"}]', "fabric_exec");
    read(manager, 'agents.create: {"id":"bbbb1111cccc2222dddd3333eeee4444"}', "fabric_exec");
    read(manager, "1234567890abcdef1234567890abcdef12345678\nabc1234fedcba9876543210123456789abcdefff\ndef567890abc1111222233334444555566667777", "bash");
    read(manager, '{"id":1234567890,"pid":987654}', "github_api");
    manager.appendCustomMessageEntry("pi-fabric-agent-message", "received session:01a0cd9c-7c24-72d5-ae80-03d729baf902", true);
    const text = replay.map(([, value]) => value).join("; ");
    const h = harness(surface, manager);
    expect(await h.send(text)).not.toHaveProperty("notice");
    expect(h.sent).toEqual([text]);
  });

  it("leaves no-identifier text untouched without accessing history", async () => {
    const h = harness(surface);
    vi.spyOn(h.manager, "getLeafId").mockImplementation(() => { throw new Error("must not read"); });
    const texts = ["Ready for the review; no identifiers here.", "2175", "#1", "#12", "# Title", "## Heading", "# 2175", "C#12", "F#123", "issue#", "x#1", "x#12", "smarty-dev#12"];
    for (const text of texts) expect(await h.send(text)).not.toHaveProperty("notice");
    expect(h.sent).toEqual(texts);
  });

  it("fails open when the session history check throws", async () => {
    const h = harness(surface);
    vi.spyOn(h.manager, "getEntry").mockImplementation(() => { throw new Error("broken history"); });
    read(h.manager, "unrelated");
    const result = await h.send("head abc1234") as { notice?: string };
    expect(result.notice).toBe("unverified ids: check failed");
    expect(h.sent).toEqual(["head abc1234\n\nunverified ids: check failed"]);
  });

  it("never counts outgoing assistant guesses or annotated send receipts as reads", async () => {
    const h = harness(surface);
    h.manager.appendMessage({ role: "assistant", content: [{ type: "text", text: "head abc1234" },
      { type: "toolCall", id: "guess", name: "fabric_exec", arguments: { code: 'await agents.followUp({id:"main",message:"head abc1234"});' } }],
      api: "openai-responses", provider: "openai", model: "test", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "toolUse", timestamp: 1 });
    const first = await h.send("head abc1234") as { notice?: string };
    read(h.manager, JSON.stringify({ text: h.sent[0], ...first }), "fabric_exec");
    expect(await h.send("head abc1234")).toHaveProperty("notice", "unverified ids: abc1234");
    read(h.manager, "abc1234fedcba9876543210123456789abcdefff", "bash");
    expect(await h.send("head abc1234")).not.toHaveProperty("notice");
  });
});
