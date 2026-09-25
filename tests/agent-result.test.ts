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

  // Fabric asks directive actors to finish with the object, and supervisors write a sentence
  // first: pi-fabric#56 rejected that and failed 7 of 27 activations after its install.
  it("accept commentary followed by one final JSON value on its own line", () => {
    const replies = [
      // Live supervisor replies after #56 (B8, 05:40Z).
      'PR #666 is ci-admission-owner\'s follow-up for #432, so no steer is needed.\n\n{"action":"silent"}',
      'Mergify bot status comment, author association NONE — not a request.\n\n{"action":"silent"}',
      // The #576 retro's cases: braces or a bracketed name in the prose are never the value.
      'The check ran with {} options and found nothing new.\n{"action":"silent"}',
      'smarty-agents[bot] posted the install line; nothing to steer.\n{"action":"silent"}',
      // A final value that spans lines.
      'Steer:\n{\n  "action": "message",\n  "message": "Rebase #57."\n}',
    ];
    expect(validate(replies[0]!)).toMatchObject({ status: "completed", value: { action: "silent" } });
    for (const text of replies.slice(1, 4)) expect(validate(text), text).toMatchObject({ status: "completed", value: { action: "silent" } });
    expect(validate(replies[4]!)).toMatchObject({ status: "completed", value: { action: "message", message: "Rebase #57." } });
  });

  it("reject a reply without one final value, with a reason that says so", () => {
    const replies = [
      '{"action":"silent"}\nDone.',                                  // something follows the value
      '{"action":"message","message":"x"}\n{"action":"silent"}',      // two values: ambiguous
      'Either\n{"action":"message","message":"x"}\nor\n{"action":"silent"}',
      'Result: {"action":"silent"}',                                 // not on its own line
      'Nothing to do.\n{"action": silent}',                          // the final value is not JSON
      'No JSON here at all.',
      '```json\n{"action":"message","message":"x"}\n```\nor\n```json\n{"action":"silent"}\n```',
      'Fenced, but not JSON:\n```json\n{action: silent}\n```',
      '```json\n{"action":"silent"}\n```\nThen:\n```text\nposted the install line\n```',
      '```json\n{"action":"silent"}\n```\n```typescript\nconst x = 1;\n```',
      '```ts\n{"action":"silent"}\n```',
    ];
    for (const text of replies) {
      const result = validate(text);
      expect(result.status, text).toBe("failed");
      expect(result.error).toMatch(/^Structured agent output was invalid: the reply must end with one JSON value on its own line, or have one code fence around it \(/);
      expect(result).not.toHaveProperty("value");
    }
  });

  it("still reject a JSON value that does not match the schema", () => {
    expect(validate('{"action":"shout"}')).toMatchObject({ status: "failed", error: expect.stringMatching(/^Structured agent output was invalid: /) });
    expect(validate('Steering.\n{"action":"shout"}')).toMatchObject({ status: "failed" });
    expect(validate("{}").error).not.toContain("code fence");
  });
});
