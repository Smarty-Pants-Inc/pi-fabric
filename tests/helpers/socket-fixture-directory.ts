import fs from "node:fs";

const descriptors = new Set<number>();
/** Keep Unix socket names short without moving any fixture outside TMPDIR.
 * Only socket addresses use the proc-fd alias; durable data paths retain their
 * real filesystem ancestry (procfs itself cannot satisfy fsync barriers).
 */
export function socketFixtureDirectory(directory: string): string {
  if (process.platform !== "linux" || Buffer.byteLength(directory, "utf8") < 64) return directory;
  const fd = fs.openSync(directory, "r");
  descriptors.add(fd);
  return `/proc/${process.pid}/fd/${fd}`;
}

export function closeSocketFixtureDirectories(): void {
  for (const fd of descriptors) fs.closeSync(fd);
  descriptors.clear();
}
