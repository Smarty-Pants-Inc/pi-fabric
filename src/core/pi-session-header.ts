import fs from "node:fs";

export interface PiSessionHeader {
  type: "session";
  id: string;
  cwd: string;
  timestamp: string;
  version?: number;
}

/** One validation rule for actor repair, activation isolation and worker launch. */
export const parsePiSessionHeader = (first: string): PiSessionHeader | undefined => {
  try {
    const header = JSON.parse(first);
    if (header?.type === "session" && typeof header.id === "string" && header.id.length > 0 &&
      typeof header.cwd === "string" && typeof header.timestamp === "string" &&
      Number.isFinite(Date.parse(header.timestamp)) &&
      (header.version === undefined || [1, 2, 3].includes(header.version))) return header;
  } catch { /* Malformed headers are repaired by the actor manager, not accepted as identity. */ }
  return undefined;
};

/** Read only the bounded native header, never a multi-megabyte actor transcript. */
export const readPiSessionHeader = (file: string): PiSessionHeader | undefined => {
  const fd = fs.openSync(file, "r");
  try {
    const buffer = Buffer.alloc(64 * 1024);
    const bytes = fs.readSync(fd, buffer, 0, buffer.length, 0);
    return parsePiSessionHeader(buffer.subarray(0, bytes).toString("utf8").split("\n", 1)[0]!);
  } finally { fs.closeSync(fd); }
};
