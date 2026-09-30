import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { MESSAGE_ID_LIMITS, unverifiedMessageIds } from "../src/coordination/unverified-ids.js";
import { outgoingMessageNotice } from "../src/providers/message-id-notice.js";
import { formatFabricValue } from "../src/ui/structured.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const uuid = "01a0cd9c-7c24-72d5-ae80-03d729baf901";
const actor = "aaaa1111bbbb2222cccc3333dddd4444";
const full = "1234567890abcdef1234567890abcdef12345678";
const session = () => SessionManager.inMemory(process.cwd());
const read = (manager: SessionManager, text: string) => manager.appendMessage({ role: "toolResult", toolCallId: "read", toolName: "bash", content: [{ type: "text", text }], isError: false, timestamp: 1 });
const context = (manager?: SessionManager) => ({ extensionContext: { sessionManager: manager } as unknown as ExtensionContext } as FabricInvocationContext);

describe("conservative identifier classes", () => {
  it.each([
    [`session:${uuid}`, [`session:${uuid}`]],
    [uuid, [uuid]],
    ["session:abcd1234-abcd-4321-8abc-abcdef123456", ["session:abcd1234-abcd-4321-8abc-abcdef123456"]],
    ["abcd1234-abcd-4321-8abc-abcdef123456", []],
    ["01a0abcd-abcd-4321-8abc-abcdef123456", []],
    ["01a0abcd-abcd-7321-7abc-abcdef123456", []],
    [actor, [actor]],
    [`actor:${actor}`, [`actor:${actor}`]],
    [`run:${actor}`, [`run:${actor}`]],
    [full, []],
    [`commit ${full}`, [full]],
    [`head ${actor}`, [actor]],
    ["head abc1234", ["abc1234"]],
    ["sha=abc1234", ["abc1234"]],
    ['"sha": "abc1234"', ["abc1234"]],
    ["COMMIT ABC1234", ["ABC1234"]],
    ["base: abc1234", ["abc1234"]],
    ["revision abc1234", ["abc1234"]],
    ["rev abc1234", ["abc1234"]],
    ["`abc1234`", ["abc1234"]],
    ["```abc1234```", []],
    ["abc1234 is a word; decafed is another word", []],
    ["color abcdef12; sha abc123", []],
    ["sha256 abcdef1234567890; shadow abc1234", []],
    ["head abc1234suffix; head abc1234-thing", []],
    ["comment 123456789", ["comment 123456789"]],
    ["comment1234567890", ["comment 1234567890"]],
    ["#issuecomment-1234567890", ["comment 1234567890"]],
    ["https://github.com/o/r/issues/1#issuecomment-1234567890", ["comment 1234567890"]],
    ["1234567890; #2175; comment 123; #issuecomment-12345678901", []],
    ["pid987654", ["pid 987654"]],
    ["pid=987654", ["pid 987654"]],
    ["pid 987654", ["pid 987654"]],
    ["pid 987654x; rapid987654; pid -oops", []],
  ] as const)("recognizes %s", (text, expected) => expect(unverifiedMessageIds(text, session())).toEqual(expected));

  it("deduplicates canonical ids in first-mention order", () => {
    expect(unverifiedMessageIds(`pid 8; head abc1234; sha=abc1234; session:${uuid}; ${uuid}; comment1234567890; #issuecomment-1234567890`, session()))
      .toEqual(["pid 8", "abc1234", `session:${uuid}`, "comment 1234567890"]);
  });
});

describe("actual sender-session read evidence", () => {
  it.each(["pi-fabric-agent-message", "pi-fabric-inbox", "pi-fabric-records", "other-received-message"])("counts received %s content", customType => {
    const manager = session();
    manager.appendCustomMessageEntry(customType, `received ${uuid}; actor ${actor}; commit ${full}; #issuecomment-1234567890; pid 987654`, true);
    expect(unverifiedMessageIds(`session:${uuid}; ${actor}; head ${full.slice(0, 7)}; comment1234567890; pid987654`, manager)).toEqual([]);
  });

  it("counts incoming Pi user-role steering/followUp and message-role custom content", () => {
    const manager = session();
    manager.appendMessage({ role: "user", content: uuid, timestamp: 1 });
    manager.appendMessage({ role: "custom", customType: "received", content: actor, display: true, timestamp: 2 });
    expect(unverifiedMessageIds(`session:${uuid}; ${actor}`, manager)).toEqual([]);
  });

  it("does not count hidden metadata, compaction or branch summaries", () => {
    const manager = session();
    const id = manager.appendCustomEntry("registry", { sha: full });
    manager.appendCustomMessageEntry("received", "ordinary content", true, { sha: full });
    manager.appendCompaction(`head ${full}`, id, 1);
    manager.appendMessage({ role: "toolResult", toolCallId: "read", toolName: "read", content: [], details: { sha: full }, isError: false, timestamp: 1 });
    expect(unverifiedMessageIds(`head ${full}`, manager)).toEqual([full]);
  });

  it("does not count self-delivered guesses", () => {
    const manager = session();
    manager.appendCustomMessageEntry("pi-fabric-agent-message", `head ${full}`, true, { from: { id: `session:${manager.getSessionId()}` } });
    expect(unverifiedMessageIds(`head ${full}`, manager)).toEqual([full]);
  });

  it("accepts only token boundaries and valid SHA abbreviations, not a different actor prefix", () => {
    const manager = session();
    read(manager, `abc1234fff12341234123412341234123412341234ff ${actor}suffix x${uuid}`);
    expect(unverifiedMessageIds(`head abc1234; ${actor}; ${uuid}`, manager)).toEqual(["abc1234", actor, uuid]);
    read(manager, `abc1234${"0".repeat(25)}`); // 32-hex actor, not SHA proof
    expect(unverifiedMessageIds("head abc1234", manager)).toEqual(["abc1234"]);
    read(manager, `abc1234${"0".repeat(33)}`); // 40 hex
    expect(unverifiedMessageIds("head abc1234", manager)).toEqual([]);
    expect(unverifiedMessageIds(`head abc1234${"0".repeat(25)}`, manager)).toEqual([]);
    expect(unverifiedMessageIds(`abc1234${"0".repeat(25)}`, session())).toEqual([`abc1234${"0".repeat(25)}`]);
  });

  it.each(["json", "yaml", "auto"] as const)("keeps independent reads next to %s send receipts", format => {
    const manager = session();
    const receipt = { sequence: 1, topic: "team", text: "head abc1234\n\nunverified ids: abc1234", notice: "unverified ids: abc1234" };
    read(manager, formatFabricValue({ receipt, peers: [{ id: actor }], git: "abc1234fedcba9876543210123456789abcdefff\n" }, format).text);
    expect(unverifiedMessageIds(`head abc1234; ${actor}`, manager)).toEqual([]);
    const onlyReceipt = session();
    read(onlyReceipt, formatFabricValue(receipt, format).text);
    expect(unverifiedMessageIds("head abc1234", onlyReceipt)).toEqual(["abc1234"]);
  });

  it("accepts received/tool-read marked text, but not direct send-only Fabric output", () => {
    const manager = session();
    read(manager, "head abc1234\n\nunverified ids: abc1234"); // Actual separate read, not a receipt object.
    expect(unverifiedMessageIds("head abc1234", manager)).toEqual([]);
    const sender = session();
    sender.appendMessage({ role: "toolResult", toolCallId: "send", toolName: "fabric_exec", content: [{ type: "text", text: actor }],
      details: { trace: { operations: [{ ref: "agents.followUp" }] } }, isError: false, timestamp: 1 });
    expect(unverifiedMessageIds(actor, sender)).toEqual([actor]);
  });

  it.each([true, false])("excludes only self-authored blocks in batched followUps (self first: %s)", selfFirst => {
    const manager = session();
    const own = `session:${manager.getSessionId()}`;
    const peer = `session:${uuid}`;
    const items = [{ from: { id: own }, text: "head abc1234" }, { from: { id: peer }, text: `head ${full}` }];
    if (!selfFirst) items.reverse();
    manager.appendCustomMessageEntry("pi-fabric-agent-message", items.map(item =>
      `<fabric-agent-message from_id="${item.from.id}">\n${item.text}\n</fabric-agent-message>`).join("\n\n"), true,
      { from: items[0]!.from, items: items.map(({ from }) => ({ from })) });
    expect(unverifiedMessageIds(`head abc1234; head ${full}`, manager)).toEqual(["abc1234"]);
  });

  it("counts a SHA-qualified 32-hex read without mistaking a raw actor prefix for a SHA", () => {
    const manager = session();
    read(manager, `head ${actor}`);
    expect(unverifiedMessageIds(`head ${actor.slice(0, 7)}`, manager)).toEqual([]);
    expect(unverifiedMessageIds(`head ${actor.slice(0, 7)}`, (() => { const m = session(); read(m, actor); return m; })())).toEqual([actor.slice(0, 7)]);
  });

  it.each(["json", "yaml"] as const)("filters receipt elements in %s arrays without dropping sibling reads", format => {
    const manager = session();
    const receipt = { queued: true, messageId: "ack", notice: "unverified ids: abc1234" };
    read(manager, formatFabricValue([receipt, { peers: [actor] }, receipt], format).text);
    expect(unverifiedMessageIds(`head abc1234; ${actor}`, manager)).toEqual(["abc1234"]);
  });

  it("does not turn clipped receipt content into evidence when its notice is beyond the cap", () => {
    const manager = session();
    read(manager, JSON.stringify({ sequence: 1, text: `head ${full} ${" ".repeat(MESSAGE_ID_LIMITS.historyBytes)}`, notice: `unverified ids: ${full}` }));
    expect(unverifiedMessageIds(`head ${full}`, manager)).toEqual([full]);
  });

  it("excludes self-authored actor/run delivery using the authenticated provider identity", () => {
    const manager = session();
    manager.appendCustomMessageEntry("pi-fabric-agent-message", `head ${full}`, true, { from: { id: actor } });
    expect(unverifiedMessageIds(`head ${full}`, manager, actor)).toEqual([full]);
  });

  it("only walks the current sender branch via indexed APIs", () => {
    const manager = session();
    const root = manager.appendCustomEntry("start", {});
    read(manager, full);
    manager.branch(root);
    manager.appendCustomEntry("different-branch", {});
    vi.spyOn(manager, "getEntries").mockImplementation(() => { throw new Error("no full-history copy"); });
    vi.spyOn(manager, "getBranch").mockImplementation(() => { throw new Error("no full-branch walk"); });
    expect(unverifiedMessageIds(`head ${full}`, manager)).toEqual([full]);
  });

  it("bounds history by entry count and UTF-8 bytes", () => {
    const manager = session();
    read(manager, full);
    for (let i = 0; i < MESSAGE_ID_LIMITS.entries; i++) manager.appendCustomEntry("noise", {});
    const getEntry = vi.spyOn(manager, "getEntry");
    expect(unverifiedMessageIds(`head ${full}`, manager)).toEqual([full]);
    expect(getEntry).toHaveBeenCalledTimes(MESSAGE_ID_LIMITS.entries);
    const unicode = session();
    read(unicode, full);
    read(unicode, "界".repeat(MESSAGE_ID_LIMITS.historyBytes / 2));
    expect(unverifiedMessageIds(`head ${full}`, unicode)).toEqual([full]);
  });

  it("does not invent a read from an identifier clipped at the byte cap", () => {
    const manager = session();
    read(manager, " ".repeat(MESSAGE_ID_LIMITS.historyBytes - 10) + "12345678901");
    expect(unverifiedMessageIds("comment1234567890", manager)).toEqual(["comment 1234567890"]);
  });

  it("bounds aggregate trace metadata work, failing open instead of walking a million refs", async () => {
    const manager = session();
    for (let i = 0; i < 2; i++) manager.appendMessage({ role: "toolResult", toolCallId: `send${i}`, toolName: "fabric_exec",
      content: [{ type: "text", text: "head abc1234" }], isError: false, timestamp: i,
      details: { trace: { operations: Array.from({ length: MESSAGE_ID_LIMITS.metadataItems / 2 + 1 }, () => ({ ref: "agents.followUp" })) } } });
    expect(await outgoingMessageNotice("head abc1234", context(manager))).toHaveProperty("notice", "unverified ids: check failed");
  });

  it("bounds content-block iteration even for empty blocks", () => {
    const manager = session();
    manager.appendMessage({ role: "toolResult", toolCallId: "read", toolName: "read", content: [
      ...Array.from({ length: MESSAGE_ID_LIMITS.contentBlocks }, () => ({ type: "text" as const, text: "" })),
      { type: "text", text: full },
    ], isError: false, timestamp: 1 });
    expect(unverifiedMessageIds(`head ${full}`, manager)).toEqual([full]);
  });
});

describe("fail-open notices and fast path", () => {
  it("returns the check-failed notice when sender history is unavailable or corrupt", async () => {
    expect(await outgoingMessageNotice("head abc1234", context())).toEqual({ text: "head abc1234\n\nunverified ids: check failed", notice: "unverified ids: check failed" });
    const manager = session();
    read(manager, "unrelated");
    const entry = manager.getLeafEntry()!;
    vi.spyOn(manager, "getEntry").mockReturnValue({ ...entry, parentId: entry.id });
    expect(await outgoingMessageNotice("head abc1234", context(manager))).toHaveProperty("notice", "unverified ids: check failed");
  });

  it("fails open on message/identifier budget overflow", async () => {
    const oversized = `head abc1234 ${"界".repeat(MESSAGE_ID_LIMITS.messageBytes / 2)}`;
    expect(await outgoingMessageNotice(oversized, context(session()))).toHaveProperty("notice", "unverified ids: check failed");
    const ids = Array.from({ length: MESSAGE_ID_LIMITS.identifiers + 1 }, (_, i) => `pid ${i + 1}`).join("; ");
    expect(await outgoingMessageNotice(ids, context(session()))).toHaveProperty("notice", "unverified ids: check failed");
  });

  it("no ids means no history API access, including ordinary short prose hex", async () => {
    const manager = session();
    vi.spyOn(manager, "getLeafId").mockImplementation(() => { throw new Error("never read"); });
    for (const text of ["Ready to review", "decafed abc1234 is prose", "color abcdef12", "comment 12", "pid nope"]) {
      expect(await outgoingMessageNotice(text, context(manager))).toEqual({ text });
    }
  });
});
