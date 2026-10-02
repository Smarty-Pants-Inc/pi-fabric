import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const children: ChildProcess[] = [];
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(children.splice(0).map(async child => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const closed = once(child, "close");
    child.kill("SIGKILL"); await closed;
  }));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const probe = async (root: string, session: string, name: string) => {
  const child = spawn("bun", [fileURLToPath(new URL("./fixtures/root-registration-probe.ts", import.meta.url)), root, session, name], { stdio: ["pipe", "pipe", "pipe"] });
  children.push(child);
  const closed = once(child, "close");
  let output = "", errors = "";
  child.stderr!.on("data", chunk => { errors += String(chunk); });
  const result = await new Promise<{ state: string; code?: string; error?: string }>((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error("Private root probe timed out: " + errors)), 10_000);
    child.stdout!.on("data", chunk => {
      output += String(chunk);
      if (!output.includes("\n")) return;
      clearTimeout(deadline);
      try { resolve(JSON.parse(output.split("\n")[0]!)); } catch (error) { reject(error); }
    });
    child.once("error", error => { clearTimeout(deadline); reject(error); });
    void closed.then(() => { clearTimeout(deadline); if (!output.includes("\n")) reject(new Error("Private root probe exited: " + errors)); });
  });
  return { child, closed, result };
};
const mesh = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-root-process-"));
  roots.push(root); return root;
};

describe("real process root ownership (synthetic sessions only)", () => {
  it.each(["name", "native session"])("refuses duplicate %s across processes and permits provably dead takeover", async kind => {
    const root = mesh();
    const first = await probe(root, "synthetic-a", "one");
    expect(first.result).toEqual({ state: "claimed" });
    const duplicate = await probe(root, kind === "name" ? "synthetic-b" : "synthetic-a", kind === "name" ? "one" : "two");
    expect(duplicate.result).toMatchObject({ state: "refused", code: "FABRIC_DUPLICATE_LIVE_ROOT" });
    await duplicate.closed;
    first.child.kill("SIGKILL"); await first.closed;
    const successor = await probe(root, "synthetic-a", "one");
    expect(successor.result).toEqual({ state: "claimed" });
    successor.child.stdin!.end("close\n"); await successor.closed;
    expect(fs.readdirSync(path.join(root, "root-registrations"))).toHaveLength(0);
  });
});
