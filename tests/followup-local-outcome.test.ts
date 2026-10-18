import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane, type FabricControlCommand } from "../src/topology/control-plane.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import { FileTransport } from "../src/transport/file.js";

// Keep reason: #14867 is a data-safety boundary. A caller must not mistake a
// post-admission failure for permission to resend a new message. Faults run on
// real journal/native append paths, but classification is asserted only at the
// public router boundary; internal carriers may keep their original errors.
const UNCONFIRMED = "FABRIC_DELIVERY_UNCONFIRMED";
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
type Handler = (event: unknown, context: ExtensionContext) => unknown;
const sender: MeshIdentity = { id: "actor:local-proof", name: "Local proof", kind: "actor" };
const roots: string[] = [];
const mains: MainAgentController[] = [];
const planes: FabricControlPlane[] = [];
const transports: FileTransport[] = [];
const stores: MeshStore[] = [];

const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-followup-local-outcome-"));
  roots.push(root);
  return root;
};
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const sendMessage = vi.fn();
  const pi = {
    on: (name: string, handler: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(candidate => candidate !== handler));
    },
    sendMessage,
    sendUserMessage: vi.fn(),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  return { pi, sendMessage };
};
const busy = (idle = false) => ({
  isIdle: () => idle,
  hasPendingMessages: () => false,
  signal: { aborted: false },
  sessionManager: { getEntries: () => [] },
}) as unknown as ExtensionContext;
const router = (main: Ports[2], control?: Ports[4], entries: FabricParticipantInfo[] = []) => {
  const manager = {
    status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); },
    steer: vi.fn(), followUp: vi.fn(), stop: vi.fn(),
  } as unknown as Ports[0];
  const actors = {
    identity: sender,
    status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
    owns: () => false,
    validateDirectMessage: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(),
    steerRemote: vi.fn(), resolveBinding: vi.fn(), resolveActivationBinding: vi.fn(),
  } as unknown as Ports[1];
  const participants = {
    get: (id: string) => entries.find(entry => entry.id === id),
    scheduleRefresh: vi.fn(), writeStalled: () => undefined, lastKnown: () => undefined,
  } as unknown as Ports[3];
  return new AgentMessageRouter(manager, actors, main, participants, control, binding => binding);
};
const localMain = () => {
  const root = tempRoot();
  const journal = path.join(root, "main-followups", "root.json");
  const h = fakePi();
  const main = new MainAgentController(h.pi, "session:local-proof", true, root, "local-proof");
  mains.push(main);
  // Idle Main takes the immediate send path, but still durably journals first.
  main.attachFollowUpDrain(busy(true), 60_000, journal);
  return { journal, h, main, value: router(main) };
};
const fileRoute = () => {
  const mesh = new MeshStore(path.join(tempRoot(), "mesh"), 65_536, 1_000, { stateBackend: "file" });
  stores.push(mesh);
  const transport = new FileTransport(mesh, { machine: "mesh", hostId: sender.id });
  transports.push(transport);
  const control = new FabricControlPlane(mesh, sender, {
    enabled: true, hostId: sender.id, transport, pollMs: 20, acknowledgementTimeoutMs: 1_000,
  });
  planes.push(control);
  const target = {
    id: "session:file-proof", kind: "root", status: "idle", local: false, interactive: true,
    ownerHostId: "host:file-proof", ownerIdentityId: "host:file-proof", ownerIncarnation: "fixture:file-proof",
    controlProtocol: "v1", capabilities: ["steer", "followUp"],
  } as FabricParticipantInfo;
  const main = { id: "session:elsewhere", local: true, interactive: true,
    matches: () => false, deliverAgent: vi.fn() } as unknown as Ports[2];
  return { mesh, transport, target, value: router(main, control, [target]) };
};

// A fulfilled result must never accidentally satisfy an error-only test.
const rejection = async (operation: Promise<unknown>): Promise<unknown> => {
  try { await operation; }
  catch (error) { return error; }
  throw new Error("Expected routeMessage to reject");
};

afterEach(async () => {
  vi.restoreAllMocks();
  for (const main of mains.splice(0)) main.closeFollowUpDrain();
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  await Promise.all(transports.splice(0).map(transport => transport.close()));
  for (const store of stores.splice(0)) store.closeState();
  // Only directories allocated by this file are eligible for deletion.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("followUp local outcome at the router boundary (#14867)", () => {
  it("retains the real Main item id when Pi throws after the durable journal save", async () => {
    const f = localMain();
    const sendFailure = new Error("Pi synchronous send failure after journal save");
    let durableItem: { id: string; message: string } | undefined;
    f.h.sendMessage.mockImplementation(() => {
      const saved = JSON.parse(fs.readFileSync(f.journal, "utf8")) as { items: Array<{ id: string; message: string }> };
      expect(saved.items).toHaveLength(1);
      durableItem = saved.items[0]!;
      expect(durableItem.message).toBe("journal before Pi");
      throw sendFailure;
    });

    const failure = await rejection(f.value.routeMessage(f.main.id, "journal before Pi", undefined, "followUp",
      undefined, { idempotencyKey: "local-postjournal" }));

    expect(f.h.sendMessage).toHaveBeenCalledOnce();
    expect(durableItem?.id).toEqual(expect.any(String));
    expect(failure).toBeInstanceOf(Error);
    expect(failure).not.toBe(sendFailure);
    expect(failure).toMatchObject({ code: UNCONFIRMED, deliveryId: durableItem!.id });
  });

  it("keeps a refused journal save ordinary and never calls Pi before the new item is durable", async () => {
    const f = localMain();
    const saveFailure = new Error("refuse the journal rename before admission");
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === f.journal) throw saveFailure;
      return rename(from, to);
    });

    const failure = await rejection(f.value.routeMessage(f.main.id, "must not reach Pi", undefined, "followUp",
      undefined, { idempotencyKey: "local-presave" }));

    expect(failure).toBeInstanceOf(Error);
    expect(failure).toHaveProperty("message", `Main could not record the message: ${saveFailure.message}`);
    expect(failure).not.toBe(saveFailure);
    expect((failure as Error).cause).toBe(saveFailure);
    expect(failure).not.toHaveProperty("code", UNCONFIRMED);
    expect(f.h.sendMessage).not.toHaveBeenCalled();
    expect(fs.existsSync(f.journal)).toBe(false);
  });

  it("keeps the journal item id and raw cause when rename commits before completion throws", async () => {
    const f = localMain();
    const completionFailure = new Error("journal rename committed before completion failed");
    const rename = fs.renameSync.bind(fs);
    let committed: { items: Array<{ id: string; message: string }> } | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      const result = rename(from, to); // The actual journal replacement commits first.
      if (String(to) === f.journal) {
        committed = JSON.parse(fs.readFileSync(f.journal, "utf8"));
        throw completionFailure;
      }
      return result;
    });

    const failure = await rejection(f.value.routeMessage(f.main.id, "journal survives completion failure", undefined, "followUp",
      undefined, { idempotencyKey: "local-postrename" }));

    expect(committed?.items).toHaveLength(1);
    const item = committed!.items[0]!;
    expect(item).toMatchObject({ id: expect.any(String), message: "journal survives completion failure" });
    expect(failure).toBeInstanceOf(Error);
    expect(failure).toMatchObject({ code: UNCONFIRMED, deliveryId: item.id });
    expect((failure as Error).cause).toBe(completionFailure);
    expect(f.h.sendMessage).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(f.journal, "utf8"))).toEqual(committed);
  });

  it("classifies an accepted-but-stalled Main result without losing its accepted message id", async () => {
    const accepted = { queued: true as const, messageId: "accepted-main-item", routed: "main" as const,
      stalled: true as const, pendingFollowUps: 1, oldestAgeS: 601 };
    const main = { id: "session:stalled-proof", local: true, interactive: true,
      matches: (id: string) => id === "session:stalled-proof", deliverAgent: vi.fn(() => accepted) };
    const value = router(main);

    const failure = await rejection(value.routeMessage(main.id, "already accepted", undefined, "followUp",
      undefined, { idempotencyKey: "local-stalled" }));

    expect(main.deliverAgent).toHaveBeenCalledOnce();
    expect(failure).toBeInstanceOf(Error);
    expect(failure).toMatchObject({ code: UNCONFIRMED, deliveryId: accepted.messageId });
  });

  it("classifies a real FILE post-append fence failure at routeMessage while generic send still reconciles success", async () => {
    const f = fileRoute();
    const completionFailure = new Error("native append completed before the fence threw");
    const publish = f.mesh.publish.bind(f.mesh);
    vi.spyOn(f.mesh, "publish").mockImplementation(input => publish({ ...input,
      fence: commit => {
        const result = input.fence ? input.fence(commit) : commit();
        if (input.topic === "fabric.control.command" && input.kind === "followUp") throw completionFailure;
        return result;
      },
    }));

    const failure = await rejection(f.value.routeMessage(f.target.id, "native committed", undefined, "followUp",
      undefined, { idempotencyKey: "file-postappend" }));
    const commands = f.mesh.read({ topic: "fabric.control.command", limit: 1_000 })
      .filter(event => event.kind === "followUp");
    expect(commands).toHaveLength(1); // Uncertain delivery is never automatically republished.
    const command = commands[0]!.data as FabricControlCommand;
    expect(command.message).toBe("native committed");
    expect(failure).toBeInstanceOf(Error);
    expect(failure).not.toBe(completionFailure);
    expect(failure).toMatchObject({ code: UNCONFIRMED, deliveryId: command.commandId });

    // E1's generic port intentionally has a different success-reconciliation
    // contract; exercise it with the identical real append/fence fault.
    const envelope = { id: "logical-generic-proof", kind: "command" as const,
      to: { machine: "mesh", hostId: f.target.ownerHostId }, targetId: f.target.id,
      operation: "followUp", from: sender, body: { proof: "generic-reconciles" } };
    const receipt = await f.transport.send(envelope);
    const generic = f.mesh.read({ topic: "fabric.control.command", limit: 1_000 })
      .filter(event => (event.data as { proof?: string })?.proof === "generic-reconciles");
    expect(generic).toHaveLength(1);
    expect(generic[0]!.id).not.toBe(envelope.id);
    expect(receipt).toMatchObject({ id: envelope.id, sequence: generic[0]!.sequence, duplicate: false,
      envelope: { id: envelope.id, body: envelope.body } });
  });

  it.each(["admission", "stamp"] as const)("preserves the exact FILE pre-append %s error and appends no event", async stage => {
    const f = fileRoute();
    const refusal = Object.assign(new Error(`refuse native ${stage} before append`), { code: "TEST_PREAPPEND_REFUSAL" });
    const publish = f.mesh.publish.bind(f.mesh);
    vi.spyOn(f.mesh, "publish").mockImplementation(input => {
      if (input.topic !== "fabric.control.command" || input.kind !== "followUp") return publish(input);
      return publish({ ...input, ...(stage === "admission"
        ? { admit: () => { throw refusal; } }
        : { data: () => { throw refusal; } }) });
    });

    const failure = await rejection(f.value.routeMessage(f.target.id, "not admitted", undefined, "followUp",
      undefined, { idempotencyKey: `file-preappend-${stage}` }));

    expect(failure).toBe(refusal);
    expect(failure).not.toHaveProperty("code", UNCONFIRMED);
    expect(f.mesh.read({ limit: 1_000 })).toEqual([]);
  });
});
