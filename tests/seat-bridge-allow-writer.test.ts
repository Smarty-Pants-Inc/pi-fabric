import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readBridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { applyPlan, planPublish, planRestore, readLinkConfig, rollbackAuthority, sameJson, SEAT_RECEIVE, SEAT_SEND, withConfigLock,
  type SeatEntry } from "../src/seat-bridge/allow-writer.js";

// Synthetic org and link only (smarty-dev#15310): no real principal, machine or session.
export const SEAT_SID = "00000000-0000-4000-8000-0000000000a1";
export const OTHER_SID = "00000000-0000-4000-8000-0000000000b2";
export const LEAD_SID = "00000000-0000-4000-8000-0000000000c3";
export const orgDoc = {
  productOwner: { name: "Alpha" },
  principals: [
    { name: "Alpha", orgInstance: { herdrAgent: "org-alpha", herdrWorkspaceLabel: "Alpha", fabricSession: SEAT_SID } },
    { name: "Bravo", orgInstance: { herdrAgent: "org-bravo", herdrWorkspaceLabel: "Bravo", fabricSession: OTHER_SID } },
  ],
};
export const pins = readBridgeNatsSeatPins(orgDoc);
const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

const base = (allow: Record<string, unknown> | undefined): Record<string, unknown> => ({
  version: 1, name: "hub-a", link: "v2", file: { mesh: "/synthetic/mesh", lockProtocol: 2 }, stateDir: "/synthetic/state",
  nats: { servers: ["tls://nats.example.test:4222"], machine: "hub-a-bridge", credentials: "/synthetic/creds", caFile: "/synthetic/ca.pem",
    custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: ["hub-a-bridge", "sandbox1", "sp-alpha"],
    operatorMachine: "fabric-ops" },
  ...(allow === undefined ? {} : { allow }),
});

/** A synthetic link config on disk (0600, private real directory), as the writer requires. */
export const writeLinkConfig = (allow: Record<string, unknown> | undefined, mutate: (value: Record<string, unknown>) => void = () => {},
  layout = (value: Record<string, unknown>) => `${JSON.stringify(value, null, 2)}\n`): string => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "link-config-")));
  dirs.push(dir);
  fs.chmodSync(dir, 0o700);
  const value = base(allow);
  mutate(value);
  const file = path.join(dir, "v2.json");
  fs.writeFileSync(file, layout(value), { mode: 0o600 });
  return file;
};

const seatFileEntry = { kind: "root", id: `session:${SEAT_SID}`, principal: "Alpha", send: ["followUp"], receive: ["followUp", "cancel"],
  note: "the seat while on v1" };
const otherFileEntry = { kind: "root", id: `session:${OTHER_SID}`, principal: "Bravo", send: ["followUp"], receive: ["followUp", "cancel"] };
const leadEntry = { kind: "root", id: `session:${LEAD_SID}`, send: ["followUp"] };
const supervisorEntry = { kind: "actor", rootId: `session:${LEAD_SID}`, name: "supervisor", send: ["followUp"] };
const want: SeatEntry = { kind: "root", id: `session:${SEAT_SID}`, principal: "Alpha", machine: "sp-alpha", send: [...SEAT_SEND],
  receive: [...SEAT_RECEIVE] };
const seat = { sessionId: SEAT_SID, machine: "sp-alpha" };

describe("the bridge seam's A' config writer (one entry, never a free-form edit)", () => {
  it("MOVES the seat's own file entry to the fixed nats entry; every other value stays identical; returns the re-read post-state", () => {
    const file = writeLinkConfig({ file: [leadEntry, seatFileEntry, supervisorEntry] });
    const before = readLinkConfig(file);
    const plan = planPublish(before, pins, seat);
    expect(plan).toMatchObject({ change: "move", natsEntry: want, fileEntry: { index: 1, entry: seatFileEntry } });
    const post = applyPlan(plan);
    expect(post.sha256).toBe(plan.afterSha256);
    expect(post.value.allow).toEqual({ file: [leadEntry, supervisorEntry], nats: [want] });
    // The post-state the seam reads for reload, route and receipt is the file ON DISK, parsed again.
    expect(post.config.allow).toEqual({ file: [leadEntry, supervisorEntry], nats: [want] });
    const { allow: _a, ...restBefore } = before.value;
    const { allow: _b, ...restAfter } = post.value;
    expect(sameJson(restBefore, restAfter)).toBe(true);
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["v2.json"]);
  });

  it("ADDS the entry when the seat had none; the same entry already there is no change and gives NO rollback authority", () => {
    const file = writeLinkConfig({ file: [leadEntry] });
    const plan = planPublish(readLinkConfig(file), pins, seat);
    expect(plan).toMatchObject({ change: "add", fileEntry: null });
    expect(rollbackAuthority(plan)).not.toBeNull();
    applyPlan(plan);
    const again = planPublish(readLinkConfig(file), pins, seat);
    expect(again).toMatchObject({ change: "none", after: null, natsEntry: null });
    // #1609 review P1-5: a no-op publish hands a rollback nothing to remove...
    expect(rollbackAuthority(again)).toBeNull();
    // ...so the rollback never removes that PRE-EXISTING entry: with no authority it refuses (the operator reconciles).
    const before = fs.readFileSync(file);
    expect(() => planRestore(readLinkConfig(file), pins, SEAT_SID, rollbackAuthority(again)))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    expect(fs.readFileSync(file)).toEqual(before);
  });

  it("refuses a machine outside nats.pathBMachines, the link's own machine, or a session that is no principal's seat", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    const before = readLinkConfig(file);
    expect(() => planPublish(before, pins, { sessionId: SEAT_SID, machine: "org-charlie" })).toThrow(expect.objectContaining({ code: "seat-machine-not-path-b" }));
    expect(() => planPublish(before, pins, { sessionId: SEAT_SID, machine: "hub-a-bridge" })).toThrow(expect.objectContaining({ code: "seat-machine-not-path-b" }));
    expect(() => planPublish(before, pins, { sessionId: LEAD_SID, machine: "sp-alpha" })).toThrow(expect.objectContaining({ code: "seat-principal-unbound" }));
  });

  it("refuses any other entry naming the seat: a different nats entry, or an actor under the seat's root", () => {
    expect(() => planPublish(readLinkConfig(writeLinkConfig({ nats: [{ ...want, machine: "sandbox1" }] })), pins, seat))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    const actor = { kind: "actor", rootId: `session:${SEAT_SID}`, name: "supervisor", send: ["followUp"] };
    expect(() => planPublish(readLinkConfig(writeLinkConfig({ file: [seatFileEntry, actor] })), pins, seat))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
  });

  it("compare-and-swap: a config changed after the plan is never overwritten", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    const plan = planPublish(readLinkConfig(file), pins, seat);
    const edited = `${fs.readFileSync(file, "utf8").trimEnd()} \n`;
    fs.writeFileSync(file, edited);
    expect(() => applyPlan(plan)).toThrow(expect.objectContaining({ code: "link-config-changed" }));
    expect(fs.readFileSync(file, "utf8")).toBe(edited);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["v2.json"]);
  });

  it("#1609 review P1-7: two writers on the shared config serialize on its lock; neither erases the other's entry", async () => {
    const file = writeLinkConfig({ file: [leadEntry, seatFileEntry, otherFileEntry] });
    const other = { sessionId: OTHER_SID, machine: "sandbox1" };
    let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    const order: string[] = [];
    // Writer A reads and plans, then waits at the barrier BEFORE its rename, holding the lock.
    const a = withConfigLock(file, async () => {
      const plan = planPublish(readLinkConfig(file), pins, seat);
      order.push("a-planned");
      await barrier;
      applyPlan(plan);
      order.push("a-renamed");
    });
    await new Promise(resolve => setTimeout(resolve, 50));
    // Writer B starts while A holds the lock: it can only read AFTER A's rename.
    const b = withConfigLock(file, () => {
      order.push("b-read");
      applyPlan(planPublish(readLinkConfig(file), pins, other));
      order.push("b-renamed");
    }, { sleep: ms => new Promise(resolve => setTimeout(resolve, Math.min(ms, 20))) });
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(order).toEqual(["a-planned"]);
    release();
    await Promise.all([a, b]);
    expect(order).toEqual(["a-planned", "a-renamed", "b-read", "b-renamed"]);
    const nats = readLinkConfig(file).value.allow as { nats: Array<{ id: string }> };
    expect(nats.nats.map(entry => entry.id).sort()).toEqual([`session:${SEAT_SID}`, `session:${OTHER_SID}`].sort());
    expect(fs.readdirSync(path.dirname(file))).toEqual(["v2.json"]);
  });

  it("the lock of a dead holder is reclaimed; a live holder is waited for, bounded", async () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    const lock = path.join(path.dirname(file), ".v2.json.fabric-v2.lock");
    fs.writeFileSync(lock, JSON.stringify({ pid: 2 ** 22 + 7, startTime: "1", at: "t" }), { mode: 0o600 });
    expect(await withConfigLock(file, () => "ran")).toBe("ran");
    expect(fs.existsSync(lock)).toBe(false);
    let t = 0;
    const held = withConfigLock(file, () => new Promise(resolve => setTimeout(resolve, 200)));
    await new Promise(resolve => setTimeout(resolve, 20));
    await expect(withConfigLock(file, () => "never", { waitMs: 1_000, now: () => t, sleep: async ms => { t += ms; } }))
      .rejects.toMatchObject({ code: "link-config-busy" });
    await held;
  });

  it("#1609 review P1-8: refuses a symlink in ANY path component, an unsafe ancestor, or an unsafe file", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    // A symlinked ancestor.
    const alias = path.join(path.dirname(path.dirname(file)), `alias-${path.basename(path.dirname(file))}`);
    fs.symlinkSync(path.dirname(file), alias);
    dirs.push(alias);
    expect(() => readLinkConfig(path.join(alias, "v2.json"))).toThrow(expect.objectContaining({ code: "link-config-path-unsafe" }));
    // The file itself a symlink.
    const link = path.join(path.dirname(file), "link.json");
    fs.symlinkSync(file, link);
    expect(() => readLinkConfig(link)).toThrow(expect.objectContaining({ code: "link-config-path-unsafe" }));
    fs.unlinkSync(link);
    // A group-writable directory.
    fs.chmodSync(path.dirname(file), 0o770);
    expect(() => readLinkConfig(file)).toThrow(expect.objectContaining({ code: "link-config-path-unsafe" }));
    fs.chmodSync(path.dirname(file), 0o700);
    // A group-writable file.
    fs.chmodSync(file, 0o620);
    expect(() => readLinkConfig(file)).toThrow(expect.objectContaining({ code: "link-config-unsafe" }));
  });

  it("refuses a link whose state lives in a temporary store (an agent's private temp store, /tmp, /run)", () => {
    for (const stateDir of ["/srv/scratch/agent/smarty-pi-tmp/pi-0000/nats-link-state/v2", "/tmp/nats-link/v2", "/run/user/1000/link"]) {
      expect(() => readLinkConfig(writeLinkConfig({ file: [seatFileEntry] }, value => { value.stateDir = stateDir; })))
        .toThrow(expect.objectContaining({ code: "link-state-dir-temporary" }));
    }
    expect(readLinkConfig(writeLinkConfig({ file: [seatFileEntry] }, value => { value.stateDir = "/home/synthetic/.local/state/nats-link/v2"; }))
      .config.stateDir).toBe("/home/synthetic/.local/state/nats-link/v2");
  });

  it("the test-only durable state directory exempts that EXACT path, through the write and its read-back, and nothing else", () => {
    const stateDir = "/tmp/nats-link/v2";
    const file = writeLinkConfig({ file: [seatFileEntry] }, value => { value.stateDir = stateDir; });
    for (const other of ["/tmp/nats-link", "/tmp/nats-link/v2/", "/tmp/nats-link/v3"]) {
      expect(() => readLinkConfig(file, { durableStateDir: other })).toThrow(expect.objectContaining({ code: "link-state-dir-temporary" }));
    }
    const before = readLinkConfig(file, { durableStateDir: stateDir });
    expect(before.durableStateDir).toBe(stateDir);
    const after = applyPlan(planPublish(before, pins, seat));
    expect(after).toMatchObject({ durableStateDir: stateDir, config: { stateDir } });
    // Without it, the same file refuses again.
    expect(() => readLinkConfig(file)).toThrow(expect.objectContaining({ code: "link-state-dir-temporary" }));
  });

  it("#1609 review P1-6: a rollback while the file still holds publish's bytes puts back the old bytes EXACTLY", () => {
    // An operator's layout (compact JSON, no allow.nats at all): the publish rewrites it; the rollback restores it byte for byte.
    const file = writeLinkConfig({ file: [leadEntry, seatFileEntry, supervisorEntry] }, () => {}, value => `${JSON.stringify(value)}\n`);
    const original = fs.readFileSync(file);
    const published = planPublish(readLinkConfig(file), pins, seat);
    applyPlan(published);
    const restore = planRestore(readLinkConfig(file), pins, SEAT_SID, rollbackAuthority(published));
    expect(restore.change).toBe("revert");
    applyPlan(restore);
    expect(fs.readFileSync(file)).toEqual(original);
    // Already restored: no change.
    expect(planRestore(readLinkConfig(file), pins, SEAT_SID, rollbackAuthority(published)).change).toBe("none");
  });

  it("a rollback after other seats changed the file undoes exactly publish's entry (an add leaves allow.nats absent again)", () => {
    const file = writeLinkConfig({ file: [leadEntry, otherFileEntry] });
    const published = planPublish(readLinkConfig(file), pins, seat);
    applyPlan(published);
    // Another seat moved in between: the file no longer holds publish's bytes.
    applyPlan(planPublish(readLinkConfig(file), pins, { sessionId: OTHER_SID, machine: "sandbox1" }));
    const restore = planRestore(readLinkConfig(file), pins, SEAT_SID, rollbackAuthority(published));
    expect(restore.change).toBe("remove");
    applyPlan(restore);
    expect((readLinkConfig(file).value.allow as { nats: Array<{ id: string }> }).nats.map(entry => entry.id)).toEqual([`session:${OTHER_SID}`]);
    // A seat whose add was the only nats entry: removing it leaves no allow.nats key, as before the publish.
    const lone = writeLinkConfig({ file: [leadEntry] });
    const added = planPublish(readLinkConfig(lone), pins, seat);
    applyPlan(added);
    fs.writeFileSync(lone, `${JSON.stringify(readLinkConfig(lone).value)}\n`);
    const undo = planRestore(readLinkConfig(lone), pins, SEAT_SID, rollbackAuthority(added));
    expect(undo.change).toBe("remove");
    expect(Object.keys(applyPlan(undo).value.allow as object)).toEqual(["file"]);
  });

  it("a rollback refuses a nats entry for the seat that publish did not write", () => {
    const file = writeLinkConfig({ nats: [want] });
    expect(() => planRestore(readLinkConfig(file), pins, SEAT_SID, null)).toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    const other = writeLinkConfig({ file: [leadEntry] });
    const published = planPublish(readLinkConfig(other), pins, seat);
    applyPlan(published);
    const authority = rollbackAuthority(published)!;
    // Someone changed the entry publish wrote: never remove a value publish did not write.
    const changed = readLinkConfig(other).value;
    (changed.allow as { nats: Array<Record<string, unknown>> }).nats[0]!.machine = "sandbox1";
    fs.writeFileSync(other, `${JSON.stringify(changed)}\n`);
    expect(() => planRestore(readLinkConfig(other), pins, SEAT_SID, authority)).toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    // No publish write and the seat on the file side only (v1 never moved): nothing to undo.
    expect(planRestore(readLinkConfig(writeLinkConfig({ file: [seatFileEntry] })), pins, SEAT_SID, null).change).toBe("none");
  });
});
