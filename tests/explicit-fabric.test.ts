import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { explicitFabricRoots, yieldsToExplicitFabric } from "../src/core/explicit-fabric.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

const fabricPackage = (name = "pi-fabric") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-pkg-"));
  roots.push(root);
  fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ name }));
  fs.mkdirSync(path.join(root, "dist"));
  const entry = path.join(root, "dist", "index.js");
  fs.writeFileSync(entry, "export default () => {};");
  return { root: fs.realpathSync(root), entry };
};

// smarty-dev#266: a worker's child Pi loads the parent's Fabric with -e and the profile's
// Fabric as a package; two different copies collide on fabric_exec and Pi refuses to start.
describe("explicit Fabric preference", () => {
  it("finds Fabric package roots among -e, --extension and --extension= arguments", () => {
    const parent = fabricPackage();
    const other = fabricPackage("some-other-extension");
    const argv = ["node", "cli.js", "--mode", "rpc", "-e", "/hooks/activation-window.js",
      "--extension", other.entry, `--extension=${parent.entry}`];
    expect(explicitFabricRoots(argv, "/")).toEqual([parent.root]);
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
});
