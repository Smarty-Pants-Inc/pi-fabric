import { describe, expect, it, vi } from "vitest";
import {
  PROXY_CONTRACT_CUSTOM_TYPE,
  ProxyContractLedger,
} from "../src/core/proxy-contract.js";

type Entry = {
  id: string;
  parentId: string | null;
  type: string;
  customType?: string;
  details?: { names?: string[] };
  message?: { role: string; content: string };
};

describe("proxy contract session restore", () => {
  it("restores the active ledger from a large history without requesting a full branch", () => {
    const entries: Entry[] = [];
    const byId = new Map<string, Entry>();
    let parentId: string | null = null;
    const count = 5_000;

    for (let index = 0; index < count; index++) {
      const id = `entry-${index}`;
      const entry: Entry = index === 37 || index === 2_501 || index === count - 1
        ? {
            id,
            parentId,
            type: "custom_message",
            customType: PROXY_CONTRACT_CUSTOM_TYPE,
            details: { names: [`captured_${index}`] },
          }
        : {
            id,
            parentId,
            type: "message",
            message: { role: "user", content: `synthetic history row ${index}` },
          };
      entries.push(entry);
      byId.set(id, entry);
      parentId = id;
    }

    // A ledger entry on a sibling branch must not affect the active branch's restore.
    const abandoned: Entry = {
      id: "abandoned-ledger",
      parentId: "entry-1200",
      type: "custom_message",
      customType: PROXY_CONTRACT_CUSTOM_TYPE,
      details: { names: ["abandoned_only"] },
    };
    byId.set(abandoned.id, abandoned);

    const activeBranch = entries;
    const sessionManager = {
      getLeafEntry: vi.fn(() => entries.at(-1)),
      getEntry: vi.fn((id: string) => byId.get(id)),
      getBranch: vi.fn(() => activeBranch),
    };

    const legacy = new ProxyContractLedger();
    legacy.restoreFromEntries(sessionManager.getBranch());
    const expected = legacy.take([
      "captured_37",
      "captured_2501",
      "captured_4999",
      "abandoned_only",
      "not_captured",
    ]);
    sessionManager.getBranch.mockClear();

    const restored = new ProxyContractLedger();
    restored.restoreFromSessionManager(
      sessionManager as unknown as Parameters<ProxyContractLedger["restoreFromSessionManager"]>[0],
    );

    expect(restored.take([
      "captured_37",
      "captured_2501",
      "captured_4999",
      "abandoned_only",
      "not_captured",
    ])).toEqual(expected);
    expect(expected).toEqual(["abandoned_only", "not_captured"]);
    expect(sessionManager.getBranch).not.toHaveBeenCalled();
    expect(sessionManager.getEntry).toHaveBeenCalledTimes(count - 1);
    expect(sessionManager.getLeafEntry).toHaveBeenCalledTimes(1);
  });

  it("resets cleanly on a host without point-entry lookups instead of falling back to getBranch", () => {
    const ledger = new ProxyContractLedger();
    ledger.restoreFromEntries([{
      type: "custom_message",
      customType: PROXY_CONTRACT_CUSTOM_TYPE,
      details: { names: ["old_branch_name"] },
    }]);
    const getBranch = vi.fn(() => [{
      type: "custom_message",
      customType: PROXY_CONTRACT_CUSTOM_TYPE,
      details: { names: ["unread_legacy_name"] },
    }]);

    ledger.restoreFromSessionManager({ getBranch } as unknown as Parameters<ProxyContractLedger["restoreFromSessionManager"]>[0]);

    expect(ledger.take(["old_branch_name", "unread_legacy_name"])).toEqual([
      "old_branch_name",
      "unread_legacy_name",
    ]);
    expect(getBranch).not.toHaveBeenCalled();
  });
});
