import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";
import { MeshStore } from "../src/mesh/store.js";

// E1a KEEP: removing archive recovery before control publication loses acknowledged
// history even with the archive ONLINE and the old reservation counter intact.
// Scratch data-safety regression, not the shared real-fleet N20 measurement.
const roots: string[] = [];
const from = { id: "session:e1a", name: "e1a", kind: "main" as const };
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("E1a archive recovery compatibility", () => {
  it.each(["fabric.control.command", "fabric.control.ack"])("recovers an ONLINE legacy suffix before publishing %s", async topic => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-control-"));
    roots.push(root);
    const dir = path.join(root, "archive"), live = path.join(root, "events.jsonl");
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
    const mesh = new MeshStore(root, 4096, 100, { stateBackend: "sqlite" }), archive = new MeshArchive(dir, root);
    expect(mesh.stateBackend).toBe("sqlite");
    const seed = await mesh.publish({ topic: "legacy.history", from, text: "seed" });
    const old = await mesh.publish({ topic: "legacy.history", from, text: "old acknowledged suffix" });
    expect(archive.lookup(old.sequence)).toEqual(old);

    // Legacy power cut: synced archive bytes and counter survive; the live suffix does not.
    // The archive stays ONLINE. This is distinct from the accepted offline + lost-counter limit.
    fs.writeFileSync(live, JSON.stringify(seed) + "\n");
    fs.writeFileSync(path.join(dir, "BOOT"), "previous-boot");
    expect(Number(fs.readFileSync(path.join(root, "sequence"), "utf8"))).toBe(old.sequence);
    const control = await mesh.publish({ topic, from, text: "new control", durable: false });
    expect(control.sequence).toBe(old.sequence + 1);
    expect(mesh.read({ after: seed.sequence })).toEqual([old, control]);
    expect(mesh.nextEventAfter(seed.sequence)).toEqual(old);
    expect(mesh.tail(0, 100).events).toEqual([seed, old, control]);

    // Later ordinary recovery must not strand the old suffix behind the new live horizon.
    const later = await mesh.publish({ topic: "generic.after", from, text: "later" });
    expect(mesh.read({ after: seed.sequence })).toEqual([old, control, later]);
    const restarted = new MeshStore(root, 4096, 100, { stateBackend: "sqlite" });
    expect(restarted.tail(0, 100).events).toEqual([seed, old, control, later]);
    expect(fs.readFileSync(live, "utf8").trim().split("\n").map(line => JSON.parse(line))).toEqual([seed, old, control, later]);
    expect(archive.lookup(old.sequence)).toEqual(old);
    restarted.closeState(); mesh.closeState();
  });
});
