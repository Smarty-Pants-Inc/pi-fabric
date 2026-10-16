// ponytail: V8 slices retain their parent strings; copy the bounded preview.
export const detachedPreview = (text: string, max: number): string =>
  Buffer.from(text.slice(0, Math.max(0, max)), "utf16le").toString("utf16le");
