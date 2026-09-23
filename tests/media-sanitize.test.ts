import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import {
  isImageContent,
  looksLikeBase64,
  sanitizeFabricMediaText,
  sanitizeFabricMediaValue,
} from "../src/core/media-sanitize.js";
import { FabricExecutionService } from "../src/execution-service.js";

const payload = "A".repeat(4_096);
const image = { type: "image", data: payload, mimeType: "image/png" };

describe("media sanitize", () => {
  it("leaves short text untouched and collapses base64 runs", () => {
    expect(sanitizeFabricMediaText("short")).toBe("short");
    expect(sanitizeFabricMediaText(`prefix ${payload} suffix`)).toBe(
      "prefix [omitted base64: 4096 chars] suffix",
    );
  });

  it("recognizes base64 payloads and image blocks", () => {
    expect(looksLikeBase64(payload)).toBe(true);
    expect(looksLikeBase64("data:image/png;base64," + payload)).toBe(true);
    expect(looksLikeBase64("A".repeat(1_023))).toBe(false);
    expect(isImageContent(image)).toBe(true);
    expect(isImageContent({ type: "image", mimeType: "image/png" })).toBe(false);
  });

  it("hoists images out of values into the side channel", () => {
    const { value, images, media } = sanitizeFabricMediaValue({
      terminate: true,
      nested: [{ ...image }, { note: "keep" }],
    });

    expect(value).toEqual({
      terminate: true,
      nested: [
        { type: "image", mediaIndex: 0, mimeType: "image/png", redacted: true },
        { note: "keep" },
      ],
    });
    expect(images).toEqual([{ type: "image", data: payload, mimeType: "image/png" }]);
    expect(media).toEqual([{ type: "image", mediaIndex: 0, mimeType: "image/png", redacted: true }]);
  });

  it("deduplicates repeated payloads and sanitizes loose base64 strings", () => {
    const { value, images } = sanitizeFabricMediaValue({ a: { ...image }, b: { ...image } });

    expect(images).toHaveLength(1);
    expect(value).toEqual({
      a: { type: "image", mediaIndex: 0, mimeType: "image/png", redacted: true },
      b: { type: "image", mediaIndex: 0, mimeType: "image/png", redacted: true },
    });
    expect(sanitizeFabricMediaValue({ blob: payload }).value).toEqual({
      blob: "[omitted base64: 4096 chars]",
    });
  });

  it("preserves non-plain objects, primitives, and repeated references", () => {
    const when = new Date(0);
    const shared = { note: "same" };
    const { value } = sanitizeFabricMediaValue({ when, shared, again: shared, n: 3, missing: null });

    expect(value).toMatchObject({ n: 3, missing: null });
    expect((value as { when: unknown }).when).toBe(when);
    expect((value as { again: unknown }).again).toBe(shared);
  });
});

describe("fabric_exec media channels", () => {
  const run = async (code: string) => {
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.fullCodeMode = false;
    const service = new FabricExecutionService(new ActionRegistry(), config);
    return service.execute({
      code,
      signal: undefined,
      parentToolCallId: "media-sanitize",
      context: { cwd: process.cwd(), hasUI: false } as ExtensionContext,
      onPartial() {},
    });
  };

  it("keeps raw base64 out of logs and the result value", async () => {
    const result = await run(
      `const data = "A".repeat(4096);
       const image = { type: "image", data, mimeType: "image/png" };
       print(image);
       return { nested: [image, "done"] };`,
    );

    expect(result.success).toBe(true);
    const logs = result.logs.join("\n");
    expect(logs).toContain("[omitted base64");
    expect(logs).not.toContain(payload);
    expect(JSON.stringify(result.value)).not.toContain(payload);
    expect(result.value).toEqual({
      nested: [
        { type: "image", mediaIndex: 0, mimeType: "image/png", redacted: true },
        "done",
      ],
    });
    expect(result.media).toEqual([{ type: "image", data: payload, mimeType: "image/png" }]);
  });
});
