import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { HOSTED_EXIT_FILE, writeConfirmedHostedExit } from "../src/agents/hosted-exit.js";
import { runTreeExitVeto } from "../src/storage/retention.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-hosted-custody-"));
  roots.push(directory);
  const record = { id: "bound-run", runner: "daemon", transport: "hosted", status: "stopped", startedAt: Date.now() - 100 };
  const locator = { daemon: "local", job: record.id };
  const state = { version: 1, runner: record.runner, locator, context: { id: record.id, runDirectory: directory } };
  const status = (patch = {}) => fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ ...record, ...patch }));
  const saveState = () => fs.writeFileSync(path.join(directory, "hosted.json"), JSON.stringify(state));
  status(); saveState();
  const confirm = () => writeConfirmedHostedExit(directory, record, locator, "shutdown");
  return { directory, record, locator, state, status, saveState, confirm };
};

describe("adapter-owned hosted exit custody", () => {
  it("does not turn terminal status or a missing unresolved marker into exit proof", () => {
    const f = fixture();
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("hosted worker exit is unconfirmed");
    expect(runTreeExitVeto(f.directory)).toContain("unconfirmed");
  });
  it("releases only the bound persisted explicit stop confirmation", () => {
    const f = fixture(); f.confirm();
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toBeUndefined();
    f.status({ status: "running" });
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("unconfirmed");
  });
  it.each([{ id: "other" }, { runner: "other" }, { startedAt: 0 }, { outcome: "indeterminate" }, { cleanupPending: true }])(
    "retains changed or uncertain status %j", patch => {
      const f = fixture(); f.confirm(); f.status(patch);
      expect(runTreeExitVeto(f.directory, 0, undefined, true)).toBeDefined();
    },
  );
  it("retains a run whose durable locator changed after the receipt", () => {
    const f = fixture(); f.confirm(); f.state.locator = { daemon: "local", job: "different" }; f.saveState();
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("unconfirmed");
  });
  it("refuses to issue a receipt without the matching locator", () => {
    const f = fixture(); f.state.locator = { daemon: "local", job: "different" }; f.saveState();
    expect(f.confirm).toThrow("matching durable locator");
    expect(fs.existsSync(path.join(f.directory, HOSTED_EXIT_FILE))).toBe(false);
  });
  it("cannot transfer custody release by copying another run's receipt", () => {
    const first = fixture(); first.confirm(); const other = fixture();
    fs.copyFileSync(path.join(first.directory, HOSTED_EXIT_FILE), path.join(other.directory, HOSTED_EXIT_FILE));
    expect(runTreeExitVeto(other.directory, 0, undefined, true)).toContain("unconfirmed");
  });
  it("retains malformed receipts", () => {
    const f = fixture(); f.confirm();
    fs.writeFileSync(path.join(f.directory, HOSTED_EXIT_FILE), "{torn");
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("unconfirmed");
  });
  it.skipIf(process.platform === "win32")("retains hardlinked and symlinked receipts", () => {
    const f = fixture(); f.confirm(); const receipt = path.join(f.directory, HOSTED_EXIT_FILE);
    const other = path.join(f.directory, "linked");
    fs.linkSync(receipt, other);
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("unconfirmed");
    fs.unlinkSync(other);
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toBeUndefined();
    fs.renameSync(receipt, other); fs.symlinkSync(other, receipt);
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("unconfirmed");
  });
  it("does not release a live native descendant or an unknown descendant", () => {
    const f = fixture(); f.confirm(); const child = path.join(f.directory, "nested", "child");
    fs.mkdirSync(child, { recursive: true });
    fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "stopped", transport: "process", sessionId: String(process.pid) }));
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("worker may still be running");
    fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "stopped", transport: "process" }));
    expect(runTreeExitVeto(f.directory, 0, undefined, true)).toContain("unknown descendant identity");
  });
});
