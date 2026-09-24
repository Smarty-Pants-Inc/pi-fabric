import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const root = path.resolve(import.meta.dirname, "..");
const documents = ["README.md", "docs/provider-capabilities.md", "docs/verified-kernels.md", "AGENTS.md"];

describe("provider documentation", () => {
  it.each(documents)("%s describes supported concepts without experimental or local-only surfaces", relative => {
    const file = path.join(root, relative);
    const source = fs.readFileSync(file, "utf8");
    expect(source).not.toMatch(/\b(?:PoC|prototype|capgoal)\b|capability-(?:cell|goal)|verified\/(?:cell|goal)-provider|proofs\/cell-|FABRIC_BEND/i);
    for (const match of source.matchAll(/\[[^\]]*\]\(([^)]+)\)/g)) {
      const href = match[1]!.split("#", 1)[0]!;
      if (!href || /^[a-z][a-z0-9+.-]*:/i.test(href)) continue;
      expect(fs.existsSync(path.resolve(path.dirname(file), href)), `${relative}: ${href}`).toBe(true);
    }
  });
});
