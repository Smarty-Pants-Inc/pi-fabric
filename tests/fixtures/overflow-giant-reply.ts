// Test-only tools, not a provider/recovery replacement. The real CLI owns persistence and retry.
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

export const OVERFLOW_GIANT_BYTES = 3 * 1024 * 1024;
export const OVERFLOW_GIANT_BEGIN = "OVERFLOW_RAW_GIANT_BEGIN\n";
export const OVERFLOW_GIANT_END = "\nOVERFLOW_RAW_GIANT_END";
export const OVERFLOW_SMALL_TEXT = "OVERFLOW_SMALL_RESULT_MUST_STAY_VERBATIM";
export const overflowGiantText = (bytes = OVERFLOW_GIANT_BYTES): string => OVERFLOW_GIANT_BEGIN + "G".repeat(
  bytes - OVERFLOW_GIANT_BEGIN.length - OVERFLOW_GIANT_END.length,
) + OVERFLOW_GIANT_END;

export default function (pi: ExtensionAPI): void {
  pi.registerTool({
    name: "giant_reply", label: "Giant reply", description: "Return a real oversized offline tool reply (3 MiB by default)",
    parameters: Type.Object({ bytes: Type.Optional(Type.Integer({ minimum: 1024, maximum: OVERFLOW_GIANT_BYTES })) }),
    async execute(_id, args) {
      return { content: [{ type: "text", text: overflowGiantText(args.bytes) }], details: { fixture: "overflow-giant-reply" } };
    },
  });
  pi.registerTool({
    name: "small_reply", label: "Small reply", description: "Return the small-result preservation counterexample",
    parameters: Type.Object({}),
    async execute() {
      return { content: [{ type: "text", text: OVERFLOW_SMALL_TEXT }], details: { fixture: "overflow-small-reply" } };
    },
  });
}
