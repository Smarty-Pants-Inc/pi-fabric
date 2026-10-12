import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { JevFabricServe, JEV_FABRIC_LINE_MAX_BYTES } from "../src/jev-fabric/serve.js";
const roots: string[] = [];
const peers: JevFabricServe[] = [];
afterEach(async () => { await Promise.all(peers.splice(0).map(p => p.close())); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
async function open(mode = "normal") {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "decision-serve-")); roots.push(root);
  const binary = path.join(root, "peer");
  const fixture = fileURLToPath(new URL("./fixtures/decision-serve-peer.mjs", import.meta.url));
  fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fixture}" "$@"\n`, { mode: 0o755 });
  const peer = await JevFabricServe.open(binary, { cwd: root, home: root, timeoutMs: 10_000, env: { ...process.env, DECISION_PEER_MODE: mode } });
  peers.push(peer); return peer;
}
describe.skipIf(process.platform === "win32")("decision JSONL framing and cleanup", () => {
  it("retains a complete line exactly at the 16 MiB UTF-8 ceiling", async () => {
    const peer = await open("max"); const value = await peer.request<string>("large");
    expect(Buffer.byteLength(JSON.stringify({ id: 1, ok: true, result: value }))).toBe(JEV_FABRIC_LINE_MAX_BYTES);
  });
  it.each(["oversize-complete", "oversize-incomplete"])("rejects %s by bytes, not JS characters", async mode => {
    const peer = await open(mode);
    await expect(peer.request("large")).rejects.toThrow("oversized line");
    expect(peer.closed).toBe(true); await peer.exited;
  });
  it("rejects an oversized partial banner instead of leaving open unresolved", async () => {
    await expect(open("oversize-banner")).rejects.toThrow("oversized line");
  });
  it("rejects outgoing oversize before dispatch while leaving the peer usable", async () => {
    const peer = await open();
    await expect(peer.request("too-big", { text: "😀".repeat(JEV_FABRIC_LINE_MAX_BYTES / 4) })).rejects.toThrow("16 MiB");
    expect(await peer.request("echo", { text: "fine" })).toEqual({ op: "echo", bytes: 4 });
    const circular: Record<string, unknown> = {}; circular.self = circular;
    await expect(peer.request("circular", circular)).rejects.toThrow();
  });
  it("handles fragmented UTF-8 and out-of-order responses", async () => {
    const peer = await open();
    expect(await peer.request("unicode")).toBe("😀é漢字");
    expect(await Promise.all([peer.request("first", { delay: 30 }), peer.request("second")])).toEqual([{ op: "first", bytes: 0 }, { op: "second", bytes: 0 }]);
    await expect(peer.request("fail")).rejects.toMatchObject({ code: 22 });
  });
  it.each(["partial", "invalid-utf8", "exit"])("rejects pending requests and cleans up on %s", async mode => {
    const peer = await open(mode);
    await expect(peer.request("echo")).rejects.toThrow(/incomplete|UTF-8|ended/);
    expect(peer.closed).toBe(true);
    await expect(peer.request("later")).rejects.not.toThrow("private-credential");
  });
  it("drains a slow peer and removes abort listeners, including queued cancellation", async () => {
    const peer = await open("slow");
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const large = peer.request("first", { text: "x".repeat(4 * 1024 * 1024) });
    const aborted = peer.request("never-send", {}, controller.signal);
    controller.abort(new Error("cancelled locally"));
    await expect(aborted).rejects.toThrow("cancelled locally");
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
    expect(await large).toEqual({ op: "first", bytes: 4 * 1024 * 1024 });
    expect(await peer.request("last")).toEqual({ op: "last", bytes: 0 });
  });
  it("handles a peer closing stdin without an unhandled stream error", async () => {
    const peer = await open("close-input");
    await expect(peer.request("write", { text: "x".repeat(4 * 1024 * 1024) })).rejects.toThrow(/pipe failed|write failed|ended/);
    await peer.exited;
  });
  it("rejects malformed complete JSON instead of leaving a waiter hanging", async () => {
    const peer = await open("invalid-json");
    await expect(peer.request("echo")).rejects.toThrow("invalid JSONL");
  });
  it("close rejects outstanding waiters and is idempotent", async () => {
    const peer = await open("hang");
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const pending = peer.request("wait", {}, controller.signal);
    const rejected = expect(pending).rejects.toThrow("closed");
    await peer.close(); await rejected; await peer.close();
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
  });
});
