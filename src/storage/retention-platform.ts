/** PR #481 is POSIX-only. Windows retains main's retention paths until
 * Smarty-Pants-Inc/smarty-dev#5132. Evaluate at entry points so platform-forced
 * regression probes exercise the same gate as native Windows. */
export const retentionV2Enabled = (): boolean => process.platform !== "win32";
