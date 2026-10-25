import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readBridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { applyPlan, planPublish, planRestore, readLinkConfig, sameJson, SEAT_RECEIVE, SEAT_SEND, type SeatEntry } from "../src/seat-bridge/allow-writer.js";

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
const lane = linkLane();
const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

function linkLane() {
  return {
    base: (allow: Record<string, unknown>) => ({
      version: 1, name: "hub-a", link: "v2", file: { mesh: "/synthetic/mesh", lockProtocol: 2 }, stateDir: "/synthetic/state",
      nats: { servers: ["tls://nats.example.test:4222"], machine: "hub-a-bridge", credentials: "/synthetic/creds", caFile: "/synthetic/ca.pem",
        custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: ["hub-a-bridge", "sandbox1", "org-alpha"],
        operatorMachine: "fabric-ops" },
      allow,
    }),
  };
}

/** A synthetic link config on disk (0600, private dir), as the link's own loader requires. */
export const writeLinkConfig = (allow: Record<string, unknown>, mutate: (value: Record<string, unknown>) => void = () => {}): string => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "link-config-")));
  dirs.push(dir);
  fs.chmodSync(dir, 0o700);
  const value = lane.base(allow) as Record<string, unknown>;
  mutate(value);
  const file = path.join(dir, "v2.json");
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  return file;
};

const seatFileEntry = { kind: "root", id: `session:${SEAT_SID}`, principal: "Alpha", send: ["followUp"], receive: ["followUp", "cancel"],
  note: "the seat while on v1" };
const leadEntry = { kind: "root", id: `session:${LEAD_SID}`, send: ["followUp"] };
const supervisorEntry = { kind: "actor", rootId: `session:${LEAD_SID}`, name: "supervisor", send: ["followUp"] };
const want: SeatEntry = { kind: "root", id: `session:${SEAT_SID}`, principal: "Alpha", machine: "org-alpha", send: [...SEAT_SEND],
  receive: [...SEAT_RECEIVE] };

describe("the bridge seam's A' config writer (one entry, never a free-form edit)", () => {
  it("MOVES the seat's own file entry to the fixed nats entry; every other value stays identical", () => {
    const file = writeLinkConfig({ file: [leadEntry, seatFileEntry, supervisorEntry] });
    const before = readLinkConfig(file);
    const plan = planPublish(before, pins, { sessionId: SEAT_SID, machine: "org-alpha" });
    expect(plan).toMatchObject({ change: "move", natsEntry: want, fileEntry: { index: 1, entry: seatFileEntry } });
    applyPlan(plan);
    const after = readLinkConfig(file);
    expect(after.sha256).toBe(plan.afterSha256);
    expect(after.value.allow).toEqual({ file: [leadEntry, supervisorEntry], nats: [want] });
    const { allow: _a, ...restBefore } = before.value;
    const { allow: _b, ...restAfter } = after.value;
    expect(sameJson(restBefore, restAfter)).toBe(true);
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    // The leftover temp file is never there.
    expect(fs.readdirSync(path.dirname(file))).toEqual(["v2.json"]);
  });

  it("ADDS the entry when the seat had none; the same entry already there is no change (idempotent)", () => {
    const file = writeLinkConfig({ file: [leadEntry] });
    const plan = planPublish(readLinkConfig(file), pins, { sessionId: SEAT_SID, machine: "org-alpha" });
    expect(plan).toMatchObject({ change: "add", fileEntry: null });
    applyPlan(plan);
    const again = planPublish(readLinkConfig(file), pins, { sessionId: SEAT_SID, machine: "org-alpha" });
    expect(again).toMatchObject({ change: "none", after: null });
  });

  it("refuses a machine outside nats.pathBMachines, the link's own machine, or a session that is no principal's seat", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    const before = readLinkConfig(file);
    expect(() => planPublish(before, pins, { sessionId: SEAT_SID, machine: "org-charlie" })).toThrow(expect.objectContaining({ code: "seat-machine-not-path-b" }));
    expect(() => planPublish(before, pins, { sessionId: SEAT_SID, machine: "hub-a-bridge" })).toThrow(expect.objectContaining({ code: "seat-machine-not-path-b" }));
    expect(() => planPublish(before, pins, { sessionId: LEAD_SID, machine: "org-alpha" })).toThrow(expect.objectContaining({ code: "seat-principal-unbound" }));
  });

  it("refuses any other entry naming the seat: a different nats entry, or an actor under the seat's root", () => {
    const other = { ...want, machine: "sandbox1" };
    expect(() => planPublish(readLinkConfig(writeLinkConfig({ nats: [other] })), pins, { sessionId: SEAT_SID, machine: "org-alpha" }))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    const actor = { kind: "actor", rootId: `session:${SEAT_SID}`, name: "supervisor", send: ["followUp"] };
    expect(() => planPublish(readLinkConfig(writeLinkConfig({ file: [seatFileEntry, actor] })), pins, { sessionId: SEAT_SID, machine: "org-alpha" }))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
  });

  it("compare-and-swap: a config changed after the plan is never overwritten", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    const plan = planPublish(readLinkConfig(file), pins, { sessionId: SEAT_SID, machine: "org-alpha" });
    const edited = `${fs.readFileSync(file, "utf8").trimEnd()} \n`;
    fs.writeFileSync(file, edited);
    expect(() => applyPlan(plan)).toThrow(expect.objectContaining({ code: "link-config-changed" }));
    expect(fs.readFileSync(file, "utf8")).toBe(edited);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["v2.json"]);
  });

  it("refuses an unsafe config (group-writable, or a symlink) before reading it", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] });
    fs.chmodSync(file, 0o620);
    expect(() => readLinkConfig(file)).toThrow(expect.objectContaining({ code: "link-config-invalid" }));
    fs.chmodSync(file, 0o600);
    const link = path.join(path.dirname(file), "link.json");
    fs.symlinkSync(file, link);
    expect(() => readLinkConfig(link)).toThrow(expect.objectContaining({ code: "link-config-invalid" }));
  });

  it("refuses a link whose state lives in a temporary store (an agent's private temp store, /tmp, /run)", () => {
    for (const stateDir of ["/srv/scratch/agent/smarty-pi-tmp/pi-0000/nats-link-state/v2", "/tmp/nats-link/v2", "/run/user/1000/link"]) {
      expect(() => readLinkConfig(writeLinkConfig({ file: [seatFileEntry] }, value => { value.stateDir = stateDir; })))
        .toThrow(expect.objectContaining({ code: "link-state-dir-temporary" }));
    }
    expect(readLinkConfig(writeLinkConfig({ file: [seatFileEntry] }, value => { value.stateDir = "/home/synthetic/.local/state/nats-link/v2"; }))
      .config.stateDir).toBe("/home/synthetic/.local/state/nats-link/v2");
  });

  it("a candidate the link itself would refuse (unknown key elsewhere) is never written", () => {
    const file = writeLinkConfig({ file: [seatFileEntry] }, value => { (value.nats as Record<string, unknown>).pathBMachines = ["hub-a-bridge"]; });
    expect(() => planPublish(readLinkConfig(file), pins, { sessionId: SEAT_SID, machine: "org-alpha" }))
      .toThrow(expect.objectContaining({ code: "seat-machine-not-path-b" }));
  });

  it("restore undoes exactly what publish recorded: the nats entry out, the file entry back at its index", () => {
    const file = writeLinkConfig({ file: [leadEntry, seatFileEntry, supervisorEntry] });
    const original = readLinkConfig(file);
    const published = planPublish(original, pins, { sessionId: SEAT_SID, machine: "org-alpha" });
    applyPlan(published);
    const restore = planRestore(readLinkConfig(file), pins, { sessionId: SEAT_SID, natsEntry: published.natsEntry, fileEntry: published.fileEntry });
    expect(restore.change).toBe("restore");
    applyPlan(restore);
    expect(readLinkConfig(file).value.allow).toEqual({ file: [leadEntry, seatFileEntry, supervisorEntry], nats: [] });
    // Already restored: no change.
    expect(planRestore(readLinkConfig(file), pins, { sessionId: SEAT_SID, natsEntry: published.natsEntry, fileEntry: published.fileEntry }).change).toBe("none");
  });

  it("restore refuses a nats entry for the seat that publish did not write (or with no publish record)", () => {
    const file = writeLinkConfig({ nats: [want] });
    expect(() => planRestore(readLinkConfig(file), pins, { sessionId: SEAT_SID, natsEntry: null, fileEntry: null }))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    expect(() => planRestore(readLinkConfig(file), pins, { sessionId: SEAT_SID, natsEntry: { ...want, machine: "sandbox1" }, fileEntry: null }))
      .toThrow(expect.objectContaining({ code: "seat-entry-conflict" }));
    // No publish record and the seat on the file side only (v1 never moved): nothing to undo.
    expect(planRestore(readLinkConfig(writeLinkConfig({ file: [seatFileEntry] })), pins, { sessionId: SEAT_SID, natsEntry: null, fileEntry: null })
      .change).toBe("none");
    // Publish's own entry with no file entry to put back: REMOVE.
    const remove = planRestore(readLinkConfig(file), pins, { sessionId: SEAT_SID, natsEntry: want, fileEntry: null });
    expect(remove.change).toBe("remove");
  });
});
