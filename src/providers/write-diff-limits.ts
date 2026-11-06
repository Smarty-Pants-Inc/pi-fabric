// Pure write-diff size guards shared by the write-preview tool wrapper and the
// dashboard's core-tool-render. Keep this module free of imports from
// "@earendil-works/pi-coding-agent": it is part of the lazily imported
// dashboard graph, which cannot resolve pi's host package in managed
// installs (issue #13).

const configuredMaxBytes = Number.parseInt(
  process.env.CODE_PREVIEW_MAX_WRITE_DIFF_BYTES ?? "",
  10,
);
export const MAX_WRITE_DIFF_BYTES =
  Number.isFinite(configuredMaxBytes) && configuredMaxBytes > 0
    ? configuredMaxBytes
    : 200_000;

export {
  writeContentForPreview,
  shouldSkipWriteDiffBytes,
  shouldSkipWriteDiffComplexity,
} from "../util.js";
