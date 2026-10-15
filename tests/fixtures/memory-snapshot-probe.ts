import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createFileSystemMemorySource } from "../../src/memory/fs-source.js";

/** Data-safety counterexample through public loadSession and real file I/O.
 * The seeded candidate completes its hash, appends here, then opens parsing.
 * The fixed candidate completes hashing only after parsing that same stream.
 * No Vitest dependency, deletes, timers, child processes, or large workloads.
 */
export const probeMemorySnapshot = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-memory-snapshot-"));
  const file = path.join(root, "snapshot.jsonl");
  const initial = [
    { type: "session", id: "race", cwd: "/work" },
    { type: "message", id: "before", parentId: null, message: { role: "user", content: "full original record" } },
  ];
  const appendedRecord = {
    type: "message", id: "after", parentId: "before", message: { role: "user", content: "full appended record" },
  };
  const before = initial.map((record) => JSON.stringify(record)).join("\n") + "\n \n{torn\n";
  const added = JSON.stringify(appendedRecord) + "\n";
  fs.writeFileSync(file, before);
  const source = createFileSystemMemorySource({ id: "race", root });
  const nativeDigest = crypto.Hash.prototype.digest;
  let appended = false;
  crypto.Hash.prototype.digest = (function (this: crypto.Hash, ...args: Parameters<typeof nativeDigest>) {
    const digest = Reflect.apply(nativeDigest, this, args);
    if (!appended) {
      fs.appendFileSync(file, added);
      appended = true;
    }
    return digest;
  }) as typeof nativeDigest;
  let snapshot: Awaited<ReturnType<typeof source.loadSession>> = null;
  try {
    snapshot = await source.loadSession(path.basename(file), {});
  } finally {
    crypto.Hash.prototype.digest = nativeDigest;
  }
  const hasAppend = snapshot?.records.some((record) => record.id === "after") ?? false;
  const representedContent = before + (hasAppend ? added : "");
  const expectedRevision = crypto.createHash("sha256").update(representedContent).digest("hex");
  const expectedRecords = hasAppend ? [...initial, appendedRecord] : initial;
  const fullRecords = JSON.stringify(snapshot?.records) === JSON.stringify(expectedRecords);
  const consistent = snapshot !== null && snapshot?.revision === expectedRevision && fullRecords;
  return {
    consistent,
    appended,
    hasAppend,
    fullRecords,
    revision: snapshot?.revision,
    expectedRevision,
    records: snapshot?.records,
    initial,
    fileAppended: fs.readFileSync(file, "utf8") === before + added,
  };
};

// Parent can bundle this entry with only fs-source substituted, then run the
// same command for both candidates: node <bundle> --run-memory-snapshot-probe.
if (process.argv.includes("--run-memory-snapshot-probe")) {
  const result = await probeMemorySnapshot();
  process.stdout.write(JSON.stringify(result) + "\n");
  process.exitCode = result.consistent && result.appended && result.fileAppended ? 0 : 1;
}
