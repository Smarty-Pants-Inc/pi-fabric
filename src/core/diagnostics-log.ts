import fs from "node:fs";
import path from "node:path";
import { DIAGNOSTICS_MAX_BYTES, fabricDiagnosticsFile } from "./diagnostics.js";

/** First-use private sink: one active file and one backup, each at most 1 MiB. */
export const appendDiagnostic = (id: string, line: string, bytes: number): void => {
  let fd: number | undefined;
  try {
    const file = fabricDiagnosticsFile(id);
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    fd = fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_APPEND | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1) return;
    fs.fchmodSync(fd, 0o600);
    if (stat.size + bytes > DIAGNOSTICS_MAX_BYTES) {
      fs.closeSync(fd);
      fd = undefined;
      fs.renameSync(file, `${file}.1`);
      fd = fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY, 0o600);
    }
    fs.writeSync(fd, line);
  } catch {
    // A failed diagnostic write must not break the operation or reach the principal's pane.
  } finally {
    if (fd !== undefined) { try { fs.closeSync(fd); } catch {} }
  }
};
