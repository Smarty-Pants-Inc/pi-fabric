import fs from "node:fs";
import path from "node:path";
import { openRecoveryDurabilityStore } from "../src/recovery/durability-adapter.js";

// A real subprocess exits *inside* the actual barrier, after complete segment fsync
// (raw mode), or after file+namespace fsync of the commit witness (commit mode),
// before createRetained readback returns and before any barrier reply reaches caller.
const directory = process.argv[2]!;
const mode = process.argv[3]!;
const store = await openRecoveryDurabilityStore({
  directory, storeId: "crash-store", sessionId: "session", sessionFile: path.join(directory, "native-never-opened.jsonl"),
  verifyNativeEntry: async () => false,
});
await store.stageControlSeen({ sessionId: "session", deliveryId: "delivery", bytes: Buffer.from("exact retained control\n") });
const open = fs.openSync;
const read = fs.readFileSync;
const close = fs.closeSync;
const descriptors = new Map<number, string>();
fs.openSync = ((file: fs.PathLike, ...rest: unknown[]) => {
  const fd = Reflect.apply(open, fs, [file, ...rest]) as number;
  descriptors.set(fd, String(file));
  return fd;
}) as typeof fs.openSync;
fs.closeSync = ((fd: number) => { descriptors.delete(fd); return close(fd); }) as typeof fs.closeSync;
fs.readFileSync = ((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
  const bytes = Reflect.apply(read, fs, [file, ...rest]);
  const name = typeof file === "number" ? path.basename(descriptors.get(file) ?? "") : "";
  // seq 1 is the session binding; seq 2 is the tested delivery barrier.
  if (name.startsWith(`${mode === "raw" ? "raw" : "commit"}-0000000000000002-`)) process.exit(73);
  return bytes;
}) as typeof fs.readFileSync;
const result = await store.awaitDurableAppend({ sessionId: "session", deliveryId: "delivery", recordKind: "control-seen", deadlineMs: Date.now() + 10_000 });
process.stdout.write(JSON.stringify({ unexpectedReply: result }));
process.exit(74);
