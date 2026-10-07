import { describe, expect, it } from "vitest";
import { effectiveLiveness } from "../src/topology/liveness.js";
import { hostLiveness, type FabricHostLease } from "../src/topology/host-leases.js";

describe("shared state/file liveness", () => {
  it.each([
    { stored: { updatedAt: 10, expiresAt: 25 }, lease: { updatedAt: 20, expiresAt: 35 }, expected: { updatedAt: 20, expiresAt: 35 } },
    { stored: { updatedAt: 20, expiresAt: 35 }, lease: { updatedAt: 10, expiresAt: 25 }, expected: { updatedAt: 20, expiresAt: 35 } },
    { stored: { updatedAt: 20, expiresAt: 35 }, lease: { updatedAt: 10, expiresAt: 50 }, expected: { updatedAt: 20, expiresAt: 50 } },
    { stored: undefined, lease: { updatedAt: 20, expiresAt: 35 }, expected: { updatedAt: 20, expiresAt: 35 } },
    { stored: { updatedAt: 20, expiresAt: 35 }, lease: undefined, expected: { updatedAt: 20, expiresAt: 35 } },
    { stored: undefined, lease: undefined, expected: { updatedAt: 0, expiresAt: 0 } },
  ])("takes independent maxima, including a publication gap: $expected", ({ stored, lease, expected }) => {
    expect(effectiveLiveness(stored, lease)).toEqual(expected);
  });

  const host = { id: "host", rootId: "root", identity: { id: "writer" }, startedAt: 1, updatedAt: 10, expiresAt: 25 };
  const lease: FabricHostLease = { id: host.id, rootId: host.rootId, identityId: host.identity.id,
    startedAt: host.startedAt, updatedAt: 20, expiresAt: 35 };
  it("accepts a matching modern lease and a pre-capability host lease", () => {
    expect(hostLiveness(new Map([[host.id, lease]]), host)).toEqual({ updatedAt: 20, expiresAt: 35 });
    const { startedAt: _startedAt, ...legacyLease } = lease;
    expect(hostLiveness(new Map([[host.id, legacyLease]]), host))
      .toEqual({ updatedAt: 20, expiresAt: 35 });
  });
  it.each([{ rootId: "other" }, { identityId: "other" }, { startedAt: 2 }])(
    "does not borrow liveness from a different owner/incarnation: %j", patch => {
      expect(hostLiveness(new Map([[host.id, { ...lease, ...patch }]]), host))
        .toEqual({ updatedAt: host.updatedAt, expiresAt: host.expiresAt });
    },
  );
});
