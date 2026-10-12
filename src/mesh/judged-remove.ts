import fs from "node:fs";
import { verifiedRemove } from "../storage/verified-remove.js";

/**
 * smarty-dev#8907: how the mesh-internal deleters judge a file and then remove it. These are census records,
 * stats temporaries, dedupe receipts, migration and prepared temporaries, and abandoned temps.
 *
 * `readJudged` reads a file through ONE descriptor and returns its fstat, so the content that was judged belongs to
 * exactly that inode. `removeJudged` then removes only that inode, through verifiedRemove (smarty-dev#7916): a file
 * swapped in between the judgment and the delete is left alone. Off Linux, main's path-based removal runs unchanged
 * (the #7858 fallback). This adds no remove primitive; it is the shared call site.
 */
export const readJudged = (file: string): { text: string; stat: fs.Stats } => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  try { return { stat: fs.fstatSync(fd), text: fs.readFileSync(fd, "utf8") }; } finally { fs.closeSync(fd); }
};

/** Removes `file` only while it is still the judged inode; true when it was removed. Never throws. */
export const removeJudged = (file: string, judged: fs.Stats, deleter: string): boolean =>
  verifiedRemove(file, judged, { deleter, fallback: () => { fs.rmSync(file, { force: true }); return true; } }).removed;
