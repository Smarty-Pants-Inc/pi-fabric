/** Compatibility re-export; the active-branch walk now shares the PR #1385 proxy restore implementation. */
export {
  SessionEntryCursor,
  recentSessionEntries,
  walkActiveSessionBranch,
  type SessionEntryCursorManager,
  type SessionEntryRead,
} from "./preview.js";
