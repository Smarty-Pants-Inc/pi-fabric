import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it } from "vitest";
import { acquireHostActivation } from "../src/agents/transports/host-activation.js";
import { heldHostTokenLocks } from "./helpers/host-activation-lock-snapshot.js";

const roots: string[] = [];
const fds: number[] = [];
afterEach(() => {
  for (const fd of fds.splice(0)) fs.closeSync(fd);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
it.skipIf(process.platform !== "linux")("counts kernel token locks, not processes sharing or successively holding a token", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-snapshot-")); roots.push(root);
  const lease = await acquireHostActivation({ limit: 1, directory: root }, { id: "first" }); fds.push(lease.fd);
  const held = heldHostTokenLocks(root);
  expect(held).toHaveLength(1);
  const snapshot = fs.readFileSync("/proc/locks", "utf8");
  expect(heldHostTokenLocks(root, snapshot)).toEqual(held);
  fs.closeSync(fds.pop()!);
  expect(heldHostTokenLocks(root)).toEqual([]);
  const next = await acquireHostActivation({ limit: 1, directory: root }, { id: "next" }); fds.push(next.fd);
  expect(heldHostTokenLocks(root)).toEqual(held);
});
it.skipIf(process.platform !== "linux")("detects a genuine extra occupied token rather than clipping observations at the configured cap", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-snapshot-extra-")); roots.push(root);
  for (const id of ["one", "two"]) fds.push((await acquireHostActivation({ limit: 2, directory: root }, { id })).fd);
  expect(heldHostTokenLocks(root)).toHaveLength(2);
});
