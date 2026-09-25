import { describe, expect, it } from "vitest";
import { validateAgentResult } from "../src/agents/result.js";

// The actor directive schema (src/actors/manager.ts).
const directive = {
  type: "object",
  properties: { action: { type: "string", enum: ["silent", "message", "stop"] }, message: { type: "string" }, data: {} },
  required: ["action"],
  additionalProperties: false,
};
type Result = { status: string; text: string; value?: unknown; error?: string };
const validate = (text: string, value?: unknown) =>
  validateAgentResult<Result>({ status: "completed", text, ...(value === undefined ? {} : { value }) }, directive);

// smarty-dev#576: supervisors wrote prose before their final JSON object, and the parser took
// the first brace or bracket in the prose instead.
describe("structured agent results", () => {
  it("accept a reply that is one JSON value, or has one code fence around it", () => {
    for (const text of [
      '{"action":"silent"}', '  \n{"action": "silent"}\n', '```json\n{"action":"silent"}\n```', '```\n{"action":"silent"}\n```',
      'Here it is:\n```json\n{"action":"silent"}\n```',            // a fence marks the value explicitly
      '```JSON \n{"action":"silent"}\n```',
    ]) {
      expect(validate(text)).toMatchObject({ status: "completed", value: { action: "silent" } });
    }
    expect(validate("ignored", { action: "message", message: "hi" })).toMatchObject({ status: "completed" });
  });

  it("reject unfenced text around the value instead of searching it, with a reason that says so", () => {
    const replies = [
      // Incidental empty braces in the prose, then the intended object (the retro's case).
      'The check ran with {} options and found nothing new.\n{"action":"silent"}',
      // A bracketed bot name in the prose.
      'smarty-agents[bot] posted the install line; nothing to steer.\n{"action":"silent"}',
      // One valid object after prose: still a contract violation, not a silent result.
      'Nothing to do.\n{"action":"silent"}',
      '{"action":"silent"}\nDone.',
      '{"action":"silent"}\n{"action":"message","message":"x"}',
      // Two fences: which one is the result is ambiguous.
      '```json\n{"action":"message","message":"x"}\n```\nor\n```json\n{"action":"silent"}\n```',
      'Fenced, but not JSON:\n```json\n{action: silent}\n```',
      // review/astra on #56: a second fence with another tag counts too.
      '```json\n{"action":"silent"}\n```\nThen:\n```text\nposted the install line\n```',
      '```json\n{"action":"silent"}\n```\n```typescript\nconst x = 1;\n```',
      // One fence, but not a JSON one.
      '```ts\n{"action":"silent"}\n```',
    ];
    for (const text of replies) {
      const result = validate(text);
      expect(result.status, text).toBe("failed");
      expect(result.error).toMatch(/^Structured agent output was invalid: the reply must be one JSON value, or have one code fence around it \(/);
      expect(result).not.toHaveProperty("value");
    }
  });

  it("still reject a JSON value that does not match the schema", () => {
    expect(validate('{"action":"shout"}')).toMatchObject({ status: "failed", error: expect.stringMatching(/^Structured agent output was invalid: /) });
    expect(validate("{}").error).not.toContain("code fence");
  });
});
