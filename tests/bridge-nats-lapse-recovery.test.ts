// Keep: data safety at the carrier/journal boundary; recovery faults are not cheap broker flows.
// Real carrier + file store + atomic journal I/O; scripted delivery/recovery, not a broker.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { createNatsFileLink, NatsCarrierDeferredError } from "../src/mesh/bridge-nats-file.js";
import { MeshStore } from "../src/mesh/store.js";
import { DirectoryNotReadyError } from "../src/transport/types.js";
import { BRIDGE_MACHINE, FakePort, LINK, MemoryLedger, POD_MACHINE, fileLead, linkIdentity, natsLead, route, settle } from "./helpers/bridge-nats-l2-fakes.js";

const WINDOW = 30_000;
const cleanup: Array<() => void> = [];
afterEach(() => { vi.restoreAllMocks(); for (const close of cleanup.splice(0).reverse()) close(); });
const fixture = async (maxInHand = 2) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lapse-recovery-"));
  const cursorPath = path.join(root, "cursor.json"), lapsePath = `${cursorPath}.sender-lease-lapse`;
  let time = Date.now(); const initial = time;
  const store = new MeshStore(path.join(root, "mesh"), 256 * 1024, 1_000, { stateBackend: "file" });
  cleanup.push(() => store.closeState());
  await store.put({ key: "topology/hosts/" + createHash("sha256").update(fileLead.id).digest("hex"), identity: fileLead, value: {
    format: 1, id: fileLead.id, rootId: fileLead.id, identity: fileLead, incarnationToken: "inc-file-lead",
    startedAt: initial - 1_000, updatedAt: initial, expiresAt: initial + 3_600_000,
  } });
  const ledger = new MemoryLedger(), logs: string[] = [];
  const newPort = () => {
    const p = new FakePort();
    for (const [identity, machine, incarnation] of [[natsLead, POD_MACHINE, "inc-moved-lead"],
      [fileLead, BRIDGE_MACHINE, "inc-file-lead"], [linkIdentity, BRIDGE_MACHINE, "inc-link"]] as const) {
      p.routes.set(identity.id, route(identity, machine, incarnation));
    }
    return p;
  };
  let port = newPort();
  const make = () => {
    const l = createNatsFileLink({ store, port, localName: "file", link: LINK, cursorPath, ledger, now: () => time,
      maxInHand, filePresenceTtlMs: 0, authorize: () => ({ ok: true, principal: "carry" }),
      authorizeTopic: () => ({ ok: true, principal: "none" }), log: line => logs.push(line) });
    cleanup.push(() => l.remote.close()); return l;
  };
  let link = make(); await link.remote.latestSequence();
  const lapse = (sender = natsLead) => {
    const live = port.routes.get(sender.id) ?? route(sender, POD_MACHINE);
    port.routes.set(sender.id, { ...live, route: { ...live.route, live: false }, leaseLapsed: true,
      presenceFailure: "expired", remainingAuthorityMs: -1 });
  };
  const envelope = (id: string, sender = natsLead) => ({ id, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id },
    targetId: fileLead.id, operation: "ask", from: sender,
    body: { version: 1, commandId: `cmd-${id}`, targetId: fileLead.id, operation: "ask", ownerIncarnation: "inc-file-lead",
      message: "status?", replyTo: sender.id, requestCreatedAt: initial, requestedAt: initial, deadlineAt: initial + 600_000 } });
  const deliver = async (id: string, sequence: number, publishedAt = time, attempt = 1, sender = natsLead) => {
    const d = port.deliver(envelope(id, sender), POD_MACHINE, sequence);
    // Broker-owned metadata, not envelope.createdAt (which is sender-controlled).
    Object.assign(d, { publishedAt, attempt });
    await settle(); return d;
  };
  const restart = async () => { link.remote.close(); port = newPort(); link = make(); await link.remote.latestSequence(); };
  return { store, ledger, logs, lapsePath, initial, lapse, deliver, restart,
    get link() { return link; }, get port() { return port; }, now: () => time,
    advance: (ms: number) => { time += ms; },
    saved: () => JSON.parse(fs.readFileSync(lapsePath, "utf8")).lapses as Array<{ id: string; startedAt: number; terminalReason?: string }>,
  };
};

describe("NATS carrier bounded lapse recovery", () => {
  it.each(["sender", "directory"])("bounds retained history across restart without wedging later %s gaps", async mode => {
    const f = await fixture(); f.lapse();
    for (let i = 1; i <= 4; i++) {
      if (i === 3) { await f.restart(); f.lapse(); }
      if (mode === "directory") vi.spyOn(f.port, "resolve").mockImplementation(() => { throw new DirectoryNotReadyError("watch gap"); });
      const sender = { ...natsLead, id: `session:00000000-0000-4000-8000-${String(i).padStart(12, "0")}` };
      f.lapse(sender);
      const d = await f.deliver(`terminal-${i}`, i, f.now(), 1, sender);
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      f.advance(WINDOW - 1); await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(d.termed).toEqual([]);
      f.advance(1); await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
      expect(d.termed).toHaveLength(1); expect(d.acked).toBe(0); expect(f.saved().length).toBeLessThanOrEqual(2);
    }
  });

  it("reclaims only broker-proven terminal history, retaining an unconfirmed TERM across restart", async () => {
    const f = await fixture(); f.lapse();
    Object.assign(f.port, { ackFloor: async () => 1 });
    for (let i = 1; i <= 3; i++) {
      await f.deliver(`terminal-${i}`, i);
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      f.advance(WINDOW); await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
    }
    expect(f.saved().map(l => l.id)).toEqual(["terminal-2", "terminal-3"]);
    await f.restart(); // Healthy sender, but sequence 2's TERM has NOT been confirmed.
    const claim = vi.spyOn(f.ledger, "claim"), again = await f.deliver("terminal-2", 2, f.initial + WINDOW, 2);
    await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
    expect(again.termed).toHaveLength(1); expect(claim).not.toHaveBeenCalled();
  });

  it("a full journal with no reclaimable entry uses memory-only retention and never wedges", async () => {
    const f = await fixture(); f.lapse();
    for (let i = 1; i <= 3; i++) {
      const d = await f.deliver(`full-${i}`, i);
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      if (i === 3) {
        expect(f.saved().some(l => l.id === "full-3")).toBe(false);
        f.advance(10_000);
        const current = f.port.routes.get(natsLead.id)!;
        f.port.routes.set(natsLead.id, { ...current, route: { ...current.route, live: true }, leaseLapsed: false });
        const page = await f.link.remote.read(0);
        expect(page.events).toHaveLength(1);
        expect(f.link.remote.retainLeaseLapse(page.events[0]!)).toBe(f.initial + 2 * WINDOW);
        f.lapse(); f.advance(WINDOW - 10_000);
      } else f.advance(WINDOW);
      await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
      expect(d.termed).toHaveLength(1);
    }
    expect(f.saved().map(l => l.id)).toEqual(["full-1", "full-2"]);
    expect(f.logs.filter(line => line.includes("retaining this lapse in memory only"))).toHaveLength(1);
    await f.restart(); f.lapse(); // The overflow record was not durable; broker time still bounds a WAIT.
    const again = await f.deliver("full-3", 3, f.initial + 2 * WINDOW, 2);
    await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] }); expect(again.termed).toHaveLength(1);
  });

  it("unknown healthy old redeliveries admit unchanged, while lapsed ones expire from broker time", async () => {
    const f = await fixture(); f.advance(2 * WINDOW);
    // Same publish age and no history: LIVE presence, not age or attempt, decides normal admission.
    const healthy = await f.deliver("healthy-old", 1, f.initial, 2);
    const page = await f.link.remote.read(0);
    expect(page.events.map(event => event.id)).toEqual(["healthy-old"]); expect(healthy.termed).toEqual([]);
    await f.link.remote.read(page.through);
    const first = await f.deliver("healthy-old-first", 2, f.initial);
    const firstPage = await f.link.remote.read(page.through);
    expect(firstPage.events.map(event => event.id)).toEqual(["healthy-old-first"]); expect(first.termed).toEqual([]);
    await f.link.remote.read(firstPage.through);
    f.lapse(); const expired = await f.deliver("lapsed-old", 3, f.initial, 2);
    await expect(f.link.remote.read(firstPage.through)).resolves.toMatchObject({ events: [] });
    expect(expired.termed).toHaveLength(1);
  });

  it("a missing sidecar cannot renew a redelivery's deadline after restart", async () => {
    const f = await fixture(); f.lapse(); await f.deliver("missing", 1);
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    f.link.remote.close(); fs.renameSync(f.lapsePath, `${f.lapsePath}.saved`);
    f.advance(20_000); await f.restart(); f.lapse();
    const again = await f.deliver("missing", 1, f.initial, 2);
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(f.saved()[0]!.startedAt).toBe(f.initial);
    f.advance(10_000); await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
    expect(again.termed).toHaveLength(1);
  });

  it.each(["before-write", "after-rename"])("retains the same first timestamp after a %s failure", async mode => {
    const f = await fixture(); f.lapse(); const d = await f.deliver("write-failure", 1);
    const realWrite = atomic.writeJsonAtomic, realSync = fs.fsyncSync;
    const fault = mode === "before-write" ? vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file === f.lapsePath) throw Object.assign(new Error("injected pre-write fault"), { code: "ENOSPC" });
      return realWrite(file, value, options);
    }) : vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) throw Object.assign(new Error("injected directory barrier fault"), { code: "EIO" });
      return realSync(fd);
    });
    // The link wraps a non-transient fault in BridgeLinkError (deferTransient); the injected code is kept on its cause.
    const failure = await f.link.remote.read(0).then(() => undefined, (error: unknown) => error as { code?: unknown; cause?: { code?: unknown } });
    expect(failure?.code ?? failure?.cause?.code).toBe(mode === "before-write" ? "ENOSPC" : "EIO");
    fault.mockRestore(); expect(d.acked).toBe(0); expect(d.termed).toEqual([]);
    f.advance(WINDOW); await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
    expect(f.saved()[0]!.startedAt).toBe(f.initial); expect(d.termed).toHaveLength(1);
  });

  it("an unconfirmed expired TERM remains terminal after restart and sender recovery", async () => {
    const f = await fixture(); f.lapse(); const first = await f.deliver("lost-term", 1);
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    f.advance(WINDOW); await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
    expect(first.termed).toHaveLength(1); await f.restart();
    const claim = vi.spyOn(f.ledger, "claim"), again = await f.deliver("lost-term", 1, f.initial, 2);
    await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
    expect(again.termed).toHaveLength(1); expect(claim).not.toHaveBeenCalled();
  });
});
