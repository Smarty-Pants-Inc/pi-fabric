import { createHash } from "node:crypto";
import type { ImageContent } from "@earendil-works/pi-ai";

/**
 * Media sanitization for every channel that serializes guest or provider data
 * into text: fabric_exec logs, results, and error text, and actor host-event
 * payloads. Worker event lines are not sanitized: one past the line cap fails the
 * run with a bounded raw prefix as evidence.
 *
 * Raw image bytes stay available out of band (see the images side channel), so a
 * multimodal model can still receive them while text channels receive a
 * descriptor. The rule used to be implemented only for actor host events, so a
 * guest that printed or returned a nested pi.read image could push hundreds of
 * KB of base64 into one serialized line and break the run.
 */

/** Descriptor that replaces an image payload in a text channel. */
export interface FabricSanitizedMediaDescriptor {
  type: "image";
  mediaIndex: number;
  mimeType: string;
  redacted: true;
}

export interface SanitizedFabricMedia {
  value: unknown;
  /** Raw payloads, indexed by mediaIndex in the descriptors. */
  images: ImageContent[];
  media: FabricSanitizedMediaDescriptor[];
}

/** A run of base64-alphabet characters long enough to be a binary payload. */
const BASE64_RUN = /[A-Za-z0-9+/=_-]{1024,}/g;

const BASE64_ALPHABET = /^[A-Za-z0-9+/=_\r\n-]+$/;

const omittedMarker = (length: number): string =>
  "[omitted base64: " + String(length) + " chars]";

const isPlainObject = (value: object): boolean => {
  const prototype = Object.getPrototypeOf(value) as object | null;
  return prototype === Object.prototype || prototype === null;
};

/** True when an entire string is a base64 payload (or a base64 data URL). */
export const looksLikeBase64 = (value: string): boolean => {
  if (value.startsWith("data:") && value.includes(";base64,")) return true;
  if (value.length < 1_024 || value.length % 4 !== 0) return false;
  return BASE64_ALPHABET.test(value);
};

/** True for the Pi multimodal block shape {type: "image", data, mimeType}. */
export const isImageContent = (value: unknown): value is ImageContent =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  (value as { type?: unknown }).type === "image" &&
  typeof (value as { data?: unknown }).data === "string" &&
  typeof (value as { mimeType?: unknown }).mimeType === "string";

/**
 * Replace base64 runs inside free text with a bounded marker. Short strings are
 * returned untouched, so the hot event-line path pays only a length check.
 */
export const sanitizeFabricMediaText = (text: string): string => {
  if (text.length < 1_024) return text;
  return text.replace(BASE64_RUN, (run) => omittedMarker(run.length));
};

/**
 * Recursively replace image payloads and long base64 strings in a value.
 *
 * Images are deduplicated by content hash and hoisted into the images side
 * channel, and their text representation becomes a descriptor. Non-plain objects
 * and repeated references are returned unchanged: rebuilding them would drop
 * class identity, which matters because fabric_exec inspects result values for
 * control fields such as terminate.
 */
export const sanitizeFabricMediaValue = (value: unknown): SanitizedFabricMedia => {
  const images: ImageContent[] = [];
  const media: FabricSanitizedMediaDescriptor[] = [];
  const indexes = new Map<string, number>();
  const seen = new WeakSet<object>();

  const walk = (node: unknown): unknown => {
    if (typeof node === "string") {
      if (looksLikeBase64(node)) return omittedMarker(node.length);
      return sanitizeFabricMediaText(node);
    }
    if (node === null || typeof node !== "object") return node;
    if (isImageContent(node)) {
      const sha256 = createHash("sha256")
        .update(node.mimeType)
        .update("\0")
        .update(node.data)
        .digest("hex");
      let mediaIndex = indexes.get(sha256);
      if (mediaIndex === undefined) {
        mediaIndex = images.length;
        indexes.set(sha256, mediaIndex);
        images.push({ type: "image", data: node.data, mimeType: node.mimeType });
        media.push({ type: "image", mediaIndex, mimeType: node.mimeType, redacted: true });
      }
      return { type: "image", mediaIndex, mimeType: node.mimeType, redacted: true };
    }
    if (seen.has(node)) return node;
    seen.add(node);
    if (Array.isArray(node)) return node.map(walk);
    if (!isPlainObject(node)) return node;
    const rebuilt: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(node)) rebuilt[key] = walk(nested);
    return rebuilt;
  };

  return { value: walk(value), images, media };
};
