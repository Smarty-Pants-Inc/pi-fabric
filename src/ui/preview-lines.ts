// Adapted from pi-code-previews; see THIRD_PARTY_NOTICES.md.

export type PreviewLineEntry<T> =
  | { kind: "line"; line: T; index: number }
  | { kind: "hidden"; hidden: number };

// The utility implementation consumes this existing scanner through the public
// module, so the compatibility path shares its actual runtime ownership.
export function forEachRawTextLine(text: string, callback: (line: string) => void): void {
  let start = 0;
  while (start <= text.length) {
    const newline = text.indexOf("\n", start);
    if (newline < 0) {
      callback(text.slice(start));
      break;
    }
    callback(text.slice(start, newline));
    start = newline + 1;
  }
}

export { countContentLines, selectPreviewTextLines } from "../util.js";
