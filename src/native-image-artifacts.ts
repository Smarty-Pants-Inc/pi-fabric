import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ImageContent } from "@earendil-works/pi-ai";

/** Host-owned artifacts, never guest-selected paths. Loaded only when an image is emitted. */
export async function persistNativeImages(images: ImageContent[]): Promise<string[]> {
  if (!images.length) return [];
  const directory = await mkdtemp(join(tmpdir(), "pi-fabric-images-"));
  const extensions: Record<string, string> = {"image/png":"png", "image/jpeg":"jpg", "image/gif":"gif", "image/webp":"webp"};
  return Promise.all(images.map(async (image, index) => {
    const path = join(directory, `${index + 1}.${extensions[image.mimeType] ?? "bin"}`);
    await writeFile(path, Buffer.from(image.data, "base64"), {mode: 0o600});
    return `Image saved to: ${path}`;
  }));
}
