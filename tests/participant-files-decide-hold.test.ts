import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import * as pf from "../src/topology/participant-files.js";

// smarty-dev#13645 audit N1: writeParticipantFileIf must decide under the SAME key hold that writes.
// `decide` reads authority outside the file (a #7817 Main publication fence, registry lineage, lease
// liveness), so a decision made under an earlier, released hold must never be written later, even
// when the file's bytes did not change in between.
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const scratch = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "pf-decide-")); roots.push(root); return root; };
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const NOW = Date.UTC(2100, 0, 1);
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe("writeParticipantFileIf decides under the hold that writes (smarty-dev#13645 N1)", () => {
  it("re-decides an identity-changing update after its external input changed between key holds", async () => {
    const root = scratch();
    const mesh = { root, custody: async <T>(operation: () => T) => operation() };
    const id = "session:decide-00000000";
    const k = key("topology/participants/", id);
    const entry = (name: string) => ({ key: k, version: 1, updatedAt: NOW, updatedBy: { id, name: "x", kind: "main" as const },
      value: { format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id, kind: "root", name, sessionId: "s1" } });
    pf.writeParticipantFile(root, entry("old"));
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const inCustody = new Promise<void>(resolve => { entered = resolve; });
    // A mirror's custody pass holds the participant namespace, so the identity-changing update
    // decides once under its key receipt, releases it and waits for the namespace.
    const other = pf.withParticipantFileDirectoryCustody(mesh, async () => { entered(); await held; },
      new Set(["session:unrelated-0000"]));
    await inCustody;
    let fenced = false;
    let decisions = 0;
    const result = pf.writeParticipantFileIf(mesh, k, () => { decisions += 1; return fenced ? undefined : entry("new"); });
    for (let i = 0; i < 400 && decisions === 0; i++) await sleep(5);
    await sleep(20);
    expect(decisions).toBe(1);
    // The external input changes while the file's bytes stay the same.
    fenced = true;
    release();
    await other;
    expect(await result).toBe(false);
    expect(decisions).toBe(2);
    expect((pf.readParticipantFile(root, k)?.value as { name?: string } | undefined)?.name).toBe("old");
  }, 20_000);
});
