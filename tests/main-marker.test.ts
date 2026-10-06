import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { readMainMarker } from "../src/residency/operator-safety.js";
import { MainProcessMarker, mainMarkerPath } from "../src/residency/main-marker.js";
import * as publication from "../src/residency/main-publication-fence.js";
import { processStartTime } from "../src/residency/process-identity.js";

describe.skipIf(process.platform !== "linux")("Main process markers", () => {
  it("atomically binds, switches roots/meshes under both fences, and removes on clean exit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-markers-"));
    const first = path.join(root, "one"), second = path.join(root, "two");
    const marker = new MainProcessMarker(), birth = processStartTime(process.pid)!;
    const fence = publication.withMainPublicationFence;
    const roots: string[] = [];
    const spy = vi.spyOn(publication, "withMainPublicationFence").mockImplementation(async (mesh, id, write, wait) => {
      roots.push(id);
      return fence(mesh, id, write, wait);
    });
    try {
      await marker.publish(first, "", "");
      expect(readMainMarker(first, process.pid, birth)).toMatchObject({ rootId: "", sessionId: "" });
      await marker.publish(first, "session:one", "one");
      expect(readMainMarker(first, process.pid, birth)).toMatchObject({ rootId: "session:one", sessionId: "one" });
      roots.length = 0;
      await marker.publish(second, "session:two", "two");
      expect(roots.sort()).toEqual(["session:one", "session:two"]);
      expect(fs.existsSync(mainMarkerPath(first, process.pid, birth))).toBe(false);
      expect(readMainMarker(second, process.pid, birth)).toMatchObject({ rootId: "session:two", sessionId: "two" });
      expect(fs.readdirSync(path.join(second, "main-markers"))).toEqual([`${process.pid}-${birth}.json`]);
      marker.close();
      expect(fs.existsSync(mainMarkerPath(second, process.pid, birth))).toBe(false);
      marker.close();
    } finally { spy.mockRestore(); marker.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("a queued marker publication cannot survive clean shutdown", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-marker-close-"));
    const marker = new MainProcessMarker(), birth = processStartTime(process.pid)!;
    try {
      const publishing = marker.publish(root, "session:one", "one");
      marker.close();
      await publishing;
      expect(fs.existsSync(mainMarkerPath(root, process.pid, birth))).toBe(false);
      await expect(marker.publish(root, "session:two", "two")).rejects.toThrow("marker is closed");
    } finally { marker.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("writes its starting marker before the extension registers anything", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-marker-load-"));
    vi.stubEnv("PI_FABRIC_MESH_ROOT", root);
    const birth = processStartTime(process.pid)!;
    const handlers = new Map<string, Array<(event: unknown, context: ExtensionContext) => unknown>>();
    let observed = false;
    const registered = () => {
      const value = readMainMarker(root, process.pid, birth);
      expect(value).toMatchObject({ rootId: "", sessionId: "" });
      observed = true;
    };
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => { registered(); return () => {}; }) },
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), setActiveTools: vi.fn(),
      registerCommand: registered, registerTool: registered, registerMessageRenderer: registered,
      on: (name: string, handler: (event: unknown, context: ExtensionContext) => unknown) => {
        registered(); handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      },
    };
    try {
      await piFabric(pi as unknown as ExtensionAPI);
      expect(observed).toBe(true);
      for (const handler of handlers.get("session_shutdown") ?? []) {
        await handler({ reason: "exit" }, { hasUI: false } as ExtensionContext);
      }
      expect(fs.existsSync(mainMarkerPath(root, process.pid, birth))).toBe(false);
    } finally { vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
