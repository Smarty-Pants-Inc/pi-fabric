import type { ContextEvent } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { MISSING_TOOL_RESULT_TEXT, repairToolResultPairing } from "../src/compaction/orphan-repair.js";

type Message = ContextEvent["messages"][number];

const user = (text: string): Message => ({ role: "user", content: text, timestamp: 1 }) as Message;
const assistant = (calls: string[], stopReason = "toolUse"): Message => ({
  role: "assistant",
  content: [
    { type: "text", text: "working" },
    ...calls.map((id) => ({ type: "toolCall", id, name: `tool-${id}`, arguments: {} })),
  ],
  stopReason,
  timestamp: 1,
}) as unknown as Message;
const result = (id: string): Message => ({
  role: "toolResult",
  toolCallId: id,
  toolName: `tool-${id}`,
  content: [{ type: "text", text: `result ${id}` }],
  isError: false,
  timestamp: 1,
}) as Message;
const custom = (): Message => ({ role: "custom", customType: "note", content: "n", display: false, timestamp: 1 }) as unknown as Message;

const shape = (messages: Message[]): string[] => messages.map((message) => {
  const view = message as { role: string; toolCallId?: string; content?: unknown };
  if (view.role === "toolResult") {
    const text = (view.content as Array<{ text: string }>)[0]!.text;
    return text === MISSING_TOOL_RESULT_TEXT ? `missing:${view.toolCallId}` : `result:${view.toolCallId}`;
  }
  return view.role;
});

describe("repairToolResultPairing", () => {
  it("returns the same array when pairing is intact", () => {
    const messages = [user("go"), assistant(["a", "b"]), result("a"), result("b"), assistant([]), user("next")];
    const repaired = repairToolResultPairing(messages);
    expect(repaired.messages).toBe(messages);
    expect(repaired).toMatchObject({ dropped: 0, inserted: 0 });
  });

  it("drops a tool result whose call is not in the preceding assistant", () => {
    const messages = [user("go"), result("ghost"), assistant(["a"]), result("a"), result("b")];
    const repaired = repairToolResultPairing(messages);
    expect(shape(repaired.messages)).toEqual(["user", "assistant", "result:a"]);
    expect(repaired.dropped).toBe(2);
    expect(shape(messages)).toEqual(["user", "result:ghost", "assistant", "result:a", "result:b"]);
  });

  it("drops a duplicate result and a result answering an earlier assistant", () => {
    const messages = [assistant(["a"]), result("a"), result("a"), assistant(["b"]), result("a"), result("b")];
    const repaired = repairToolResultPairing(messages);
    expect(shape(repaired.messages)).toEqual(["assistant", "result:a", "assistant", "result:b"]);
    expect(repaired.dropped).toBe(2);
  });

  it("inserts synthetic error results for a non-final assistant after the results that arrived", () => {
    const messages = [assistant(["a", "b", "c"]), result("a"), custom(), user("interrupt")];
    const repaired = repairToolResultPairing(messages);
    expect(shape(repaired.messages)).toEqual([
      "assistant", "result:a", "missing:b", "missing:c", "custom", "user",
    ]);
    expect(repaired.inserted).toBe(2);
    const synthetic = repaired.messages[2] as { isError: boolean; toolName: string };
    expect(synthetic).toMatchObject({ isError: true, toolName: "tool-b" });
  });

  it("repairs a trailing window when results follow the assistant but some are missing", () => {
    const messages = [user("go"), assistant(["a", "b"]), result("b")];
    expect(shape(repairToolResultPairing(messages).messages)).toEqual([
      "user", "assistant", "result:b", "missing:a",
    ]);
  });

  it("leaves a final assistant with unanswered calls alone", () => {
    const messages = [user("go"), assistant(["a"])];
    expect(repairToolResultPairing(messages).messages).toBe(messages);
  });

  it("never synthesizes results for errored or aborted assistants Pi will not replay", () => {
    const messages = [assistant(["a"], "aborted"), user("retry"), assistant(["b"], "error"), user("again")];
    expect(repairToolResultPairing(messages).messages).toBe(messages);
  });
});
