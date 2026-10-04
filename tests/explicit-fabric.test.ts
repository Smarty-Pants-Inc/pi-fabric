import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { claimFabricRegistration, explicitFabricRoots, yieldsToExplicitFabric } from "../src/core/explicit-fabric.js";
import piFabric from "../src/index.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

const fabricPackage = (name = "pi-fabric") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-pkg-"));
  roots.push(root);
  fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ name, pi: { extensions: ["./dist/extension-bootstrap.js"] } }));
  fs.mkdirSync(path.join(root, "dist", "worker"), { recursive: true });
  const entry = path.join(root, "dist", "index.js");
  fs.writeFileSync(entry, "export default () => {};");
  fs.writeFileSync(path.join(root, "dist", "extension-bootstrap.js"), "export default () => {};");
  // The worker loads this hook with -e for every activation-context actor run.
  const hook = path.join(root, "dist", "worker", "activation-window.js");
  fs.writeFileSync(hook, "export default () => {};");
  return { root: fs.realpathSync(root), entry, hook };
};

// smarty-dev#266: a worker's child Pi loads the parent's Fabric with -e and the profile's
// Fabric as a package; two different copies collide on fabric_exec and Pi refuses to start.
describe("explicit Fabric preference", () => {
  it("finds the Fabric packages whose entry or directory -e and --extension load", () => {
    const parent = fabricPackage();
    const other = fabricPackage("some-other-extension");
    const byDirectory = fabricPackage();
    const argv = ["node", "cli.js", "--mode", "rpc", "-e", "/hooks/activation-window.js",
      "--extension", other.entry, "--extension", parent.entry, "-e", byDirectory.root];
    expect(explicitFabricRoots(argv, "/")).toEqual([parent.root, byDirectory.root]);
  });

  it("recognizes both supported compiled aliases with the bootstrap manifest", () => {
    const fabric = fabricPackage();
    expect(explicitFabricRoots(["node", "cli.js", "-e", fabric.entry, "-e", path.join(fabric.root, "dist", "extension-bootstrap.js")], "/")).toEqual([fabric.root]);
  });

  it("deduplicates aliases only on the same host and releases the claim on reload", () => {
    const fabric = fabricPackage();
    const host = () => {
      const listeners = new Set<(data: unknown) => void>();
      const shutdown: Array<() => void> = [];
      // Each alias receives a different API wrapper for the same host bus.
      const api = () => ({ events: {
        emit: (_channel: string, data: unknown) => { for (const listener of listeners) listener(data); },
        on: (_channel: string, listener: (data: unknown) => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
      }, on: (_event: string, handler: () => void) => { shutdown.push(handler); return () => {}; } }) as unknown as ExtensionAPI;
      return { api, shutdown: () => { for (const handler of shutdown) handler(); } };
    };
    const first = host(), second = host();
    expect(claimFabricRegistration(fabric.entry, first.api())).toBe(true);
    expect(claimFabricRegistration(path.join(fabric.root, "dist", "extension-bootstrap.js"), first.api())).toBe(false);
    expect(claimFabricRegistration(fabric.entry, second.api())).toBe(true);
    first.shutdown();
    expect(claimFabricRegistration(fabric.entry, first.api())).toBe(true);
    expect(claimFabricRegistration(fabric.entry, second.api())).toBe(false);
    first.shutdown(); second.shutdown();
  });

  it("ignores Fabric's own hook files and the --extension= form Pi does not load", () => {
    const parent = fabricPackage();
    const profile = fabricPackage();
    // An activation-context actor: the parent's hook, but no parent Fabric. The profile copy must run.
    const hookOnly = ["node", "cli.js", "--mode", "rpc", "-e", parent.hook];
    expect(explicitFabricRoots(hookOnly, "/")).toEqual([]);
    expect(yieldsToExplicitFabric(profile.entry, hookOnly, "/")).toBe(false);
    // Pi's parser reads --extension=PATH as an unknown flag and loads nothing.
    expect(explicitFabricRoots(["node", "cli.js", `--extension=${parent.entry}`], "/")).toEqual([]);
  });

  it("makes a discovered copy yield only to a DIFFERENT explicit Fabric", () => {
    const parent = fabricPackage();
    const profile = fabricPackage();
    const argv = ["node", "cli.js", "--mode", "rpc", "-e", parent.entry];
    expect(yieldsToExplicitFabric(profile.entry, argv, "/")).toBe(true);   // the profile copy steps aside
    expect(yieldsToExplicitFabric(parent.entry, argv, "/")).toBe(false);   // the parent's copy runs
  });

  it("changes nothing without an explicit Fabric", () => {
    const profile = fabricPackage();
    expect(yieldsToExplicitFabric(profile.entry, ["node", "cli.js", "--mode", "rpc"], "/")).toBe(false);
    expect(yieldsToExplicitFabric(profile.entry, ["node", "cli.js", "-e", "/hooks/other.js"], "/")).toBe(false);
  });

  it("resolves relative -e paths from the working directory and follows symlinks", () => {
    const parent = fabricPackage();
    const linkDir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-link-"));
    roots.push(linkDir);
    fs.symlinkSync(parent.root, path.join(linkDir, "fabric"));
    const argv = ["node", "cli.js", "-e", "fabric/dist/index.js"];
    expect(explicitFabricRoots(argv, linkDir)).toEqual([parent.root]);
    expect(yieldsToExplicitFabric(path.join(linkDir, "fabric", "dist", "index.js"), argv, linkDir)).toBe(false);
  });

  it("registers nothing with Pi when it yields", async () => {
    const parent = fabricPackage();
    const touched: string[] = [];
    const pi = new Proxy({}, { get: (_target, key) => { touched.push(String(key)); return () => undefined; } });
    const argv = process.argv;
    process.argv = ["node", "cli.js", "--mode", "rpc", "-e", parent.entry];
    try {
      await piFabric(pi as ExtensionAPI);
    } finally {
      process.argv = argv;
    }
    expect(touched).toEqual([]);
  });
});
