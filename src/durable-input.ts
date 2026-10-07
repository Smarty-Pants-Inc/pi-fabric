import type { UserInput } from "@earendil-works/pi-durable";
import type { FabricHostedRunContext } from "./agents/runner-registry.js";

const MAX_IMAGE_CHARS = 16_777_216;
const MAX_SCHEMA_CHARS = 262_144;

/** Snapshot request data before an asynchronous admission; never retain caller-owned image/schema objects. */
export function durableInput(context: Pick<FabricHostedRunContext, "task" | "images" | "schema" | "systemPrompt">, acceptsImages: boolean) {
  const images = context.images?.map(image => {
    if (!image || image.type !== "image" || typeof image.mimeType !== "string" ||
      !/^image\/(png|jpeg|gif|webp)$/.test(image.mimeType) || typeof image.data !== "string" ||
      image.data.length === 0 || image.data.length > MAX_IMAGE_CHARS ||
      image.data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(image.data)) {
      throw new Error("Pi durable image input requires a base64 PNG, JPEG, GIF or WebP block");
    }
    return { type: "image" as const, data: image.data, mimeType: image.mimeType };
  });
  if (images?.length) {
    if (!acceptsImages) throw new Error("Selected Pi durable model does not accept image input");
    if (images.length > 32 || images.reduce((sum, image) => sum + image.data.length, 0) > MAX_IMAGE_CHARS) {
      throw new Error("Pi durable image input exceeds 32 images or 16777216 base64 characters");
    }
  }
  let schema: Record<string, unknown> | undefined;
  if (context.schema !== undefined) {
    if (!context.schema || typeof context.schema !== "object" || Array.isArray(context.schema)) {
      throw new Error("Pi durable output schema must be a JSON object");
    }
    const json = JSON.stringify(context.schema);
    if (json.length > MAX_SCHEMA_CHARS) throw new Error("Pi durable output schema exceeds 262144 characters");
    schema = JSON.parse(json) as Record<string, unknown>;
  }
  const content: UserInput = images?.length
    ? [{ type: "text", text: context.task }, ...images]
    : context.task;
  const instructions = [context.systemPrompt ?? "", ...(schema ? [
    `Your final response must contain only JSON matching this schema, without Markdown fences:\n${JSON.stringify(schema)}`,
  ] : [])].filter(Boolean).join("\n\n");
  return { content, instructions, schema, images };
}
