// Keep reason: LH-AUDIT-001 / smarty-dev#17511, raw NATS lease bucket notices hid changed authority.
// Feed actual KV watch inputs to the production reader/notifier; no source callback is replaced.
import { afterEach, describe, expect, it } from "vitest";
import { NatsBridgePresence } from "../src/mesh/bridge-nats-presence.js";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { advance, hKey, kvWorld, natsLease, pKey, seedNative } from "./helpers/bridge-nats-kv.js";

const MACHINE = "remote-peer", BRIDGE = "local-bridge";
const ID = "session:22222222-2222-4222-8222-222222222222", INCARNATION = "remote-incarnation";
const closes: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of closes.splice(0).reverse()) await close(); });
const record = (): FabricParticipantRecord => ({ format: 1, id: ID, kind: "root", rootId: ID,
  ownerHostId: ID, ownerIdentityId: ID, ownerIncarnation: INCARNATION, name: "peer", status: "idle",
  runner: "pi", transport: "host", capabilities: ["followUp"], controlProtocol: "v1", startedAt: 999_000, updatedAt: 1_000_000 });
const fixture = async () => {
  const world = kvWorld(), clock = { wall: 1_000_000, mono: 50_000 };
  seedNative(world, MACHINE, record(), clock.wall);
  const side = new NatsBridgePresence({ transport: world.transport(BRIDGE, "bridge:local"), link: "nats",
    admit: () => true, now: () => clock.wall, monotonic: () => clock.mono });
  closes.push(() => side.close());
  side.start();
  await side.whenReady();
  expect(side.resolve(ID)?.live).toBe(true);
  const notices: BridgeChange[] = [];
  let resolve: ((kind: BridgeChange) => void) | undefined;
  const unsubscribe = side.subscribeChanges(kind => { notices.push(kind); resolve?.(kind); });
  closes.push(async () => { unsubscribe(); });
  const next = () => new Promise<BridgeChange>(done => { resolve = done; });
  const put = (value: FabricKvHostLease, owner?: string) => {
    const key = hKey(MACHINE, ID), prior = world.leases.values.get(key)!;
    return world.leases.put(key, owner ?? `${value.id}#${value.incarnation}`, value, prior.revision);
  };
  const current = () => side.reader.lease(MACHINE, ID)!.entry.value;
  return { world, clock, side, notices, next, put, current };
};

describe.sequential("canonical NATS lease source classification (LH-AUDIT-001)", () => {
  it("keeps repeated live TTL-only renewals lease-only through the actual production callback", async () => {
    const f = await fixture();
    for (let count = 0; count < 3; count++) {
      advance(f.clock, 1_000);
      const notice = f.next();
      const revision = f.put({ ...f.current(), updatedAt: f.clock.wall, expiresAt: f.clock.wall + 15_000 });
      expect(await notice).toBe("lease");
      expect(f.side.reader.lease(MACHINE, ID)?.entry.revision).toBe(revision);
      expect(f.side.resolve(ID)?.live).toBe(true);
    }
    expect(f.notices).toEqual(["lease", "lease", "lease"]);
  });

  it.each([
    ["identity binding", { identityId: "session:33333333-3333-4333-8333-333333333333" }],
    ["root binding", { rootId: "session:33333333-3333-4333-8333-333333333333" }],
    ["incarnation", { incarnation: "replacement", incarnationToken: "replacement" }],
    ["process incarnation proof", { pidStartTime: "replacement-start" }],
    ["root recheck", { rootRecheck: true }],
  ] satisfies Array<[string, Partial<FabricKvHostLease>]>)("promotes a renewed lease with changed %s to presence", async (_name, mutation) => {
    const f = await fixture(), notice = f.next();
    advance(f.clock, 1_000);
    f.put({ ...f.current(), ...mutation, updatedAt: f.clock.wall, expiresAt: f.clock.wall + 15_000 });
    expect(await notice).toBe("presence");
  });

  it("promotes loss and restoration of the credential-owner binding", async () => {
    const f = await fixture(), lease = f.current();
    let notice = f.next();
    f.put(lease, "foreign-owner");
    expect(await notice).toBe("presence");
    expect(f.side.resolve(ID)?.live).toBe(false);
    notice = f.next();
    f.put(lease, `${ID}#${INCARNATION}`);
    expect(await notice).toBe("presence");
    expect(f.side.resolve(ID)?.live).toBe(true);
  });

  it("promotes the recheck-to-live transition rather than treating its renewal as TTL-only", async () => {
    const f = await fixture();
    let notice = f.next();
    f.put({ ...f.current(), rootRecheck: true });
    await notice;
    expect((await f.side.presence()).partial).toBe(true);
    notice = f.next();
    f.put(natsLease(ID, INCARNATION, f.clock.wall));
    expect(await notice).toBe("presence");
    expect((await f.side.presence()).partial).toBeUndefined();
  });

  it("promotes expiry and recreation, keeping missing-lease observations partial", async () => {
    const f = await fixture();
    let notice = f.next();
    f.world.leases.expire(hKey(MACHINE, ID));
    expect(await notice).toBe("presence");
    expect(await f.side.presence()).toMatchObject({ hosts: [], partial: true });
    notice = f.next();
    f.world.leases.put(hKey(MACHINE, ID), `${ID}#${INCARNATION}`, natsLease(ID, INCARNATION, f.clock.wall), 0);
    expect(await notice).toBe("presence");
    expect(f.side.resolve(ID)?.live).toBe(true);
  });

  it("promotes a live revival even without a broker gone event", async () => {
    const f = await fixture();
    advance(f.clock, 13_001);
    expect(f.side.resolve(ID)?.live).toBe(false);
    const notice = f.next();
    f.put(natsLease(ID, INCARNATION, f.clock.wall));
    expect(await notice).toBe("presence");
    expect(f.side.resolve(ID)?.live).toBe(true);
  });

  it.each(["owner expiry", "receipt authority duration"])("promotes a shortened %s", async bound => {
    const f = await fixture(), lease = f.current(), notice = f.next();
    f.put(bound === "owner expiry" ? { ...lease, expiresAt: lease.expiresAt - 1_000 }
      : { ...lease, updatedAt: lease.updatedAt + 2_000, expiresAt: lease.expiresAt + 1_000 });
    expect(await notice).toBe("presence");
  });

  it("promotes reset and ready uncertainty without reading a gap as withdrawal", async () => {
    const f = await fixture();
    let notice = f.next();
    f.world.leases.gap();
    expect(await notice).toBe("presence");
    expect(f.side.reader.resyncing).toBe(true);
    expect((await f.side.presence()).partial).toBe(true);
    expect(f.side.resolve(ID)?.live).toBe(true); // Old view stays held during resnapshot.
    notice = f.next();
    f.world.leases.resnapshot();
    expect(await notice).toBe("presence");
    expect(f.side.reader.resyncing).toBe(false);
    expect((await f.side.presence()).partial).toBeUndefined();
    notice = f.next();
    advance(f.clock, 1_000);
    f.put(natsLease(ID, INCARNATION, f.clock.wall));
    expect(await notice).toBe("lease");
  });

  it("reports invalid replacement as presence and retains unknown lease evidence", async () => {
    const f = await fixture(), notice = f.next();
    const key = hKey(MACHINE, ID), prior = f.world.leases.values.get(key)!;
    f.world.leases.put(key, prior.owner, { ...f.current(), incarnation: "" }, prior.revision);
    expect(await notice).toBe("presence");
    expect(f.side.reader.lease(MACHINE, ID)).toBeUndefined();
    expect(await f.side.presence()).toMatchObject({ hosts: [], partial: true });
  });

  it("still reports presence-row changes as presence and never wakes on own TTL writes", async () => {
    const f = await fixture(), notice = f.next();
    const key = pKey(MACHINE, ID), prior = f.world.presence.values.get(key)!;
    f.world.presence.put(key, prior.owner, { ...record(), status: "running" }, prior.revision);
    expect(await notice).toBe("presence");
    const own = hKey(BRIDGE, ID);
    f.world.leases.put(own, "own", natsLease(ID, "own", f.clock.wall), 0);
    // An ordered remote renewal is the barrier proving the earlier own write was consumed.
    const barrier = f.next();
    f.put(natsLease(ID, INCARNATION, f.clock.wall));
    expect(await barrier).toBe("lease");
    expect(f.notices).toEqual(["presence", "lease"]);
  });
});
