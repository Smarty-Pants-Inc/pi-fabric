import { describe, expect, it } from "vitest";
import { LiteralCallScanner } from "../src/speculation/scanner.js";
import { TIER_A_SPECULATION_REFS } from "../src/speculation/eligibility.js";

describe("LiteralCallScanner", () => {
  it("recognizes every Tier-A namespace", () => {
    for (const ref of TIER_A_SPECULATION_REFS) {
      expect(new LiteralCallScanner().push(`await ${ref}({});`), ref).toEqual([{ ref, args: {} }]);
    }
  });

  it("does not speculate on a shadowed mesh namespace", () => {
    expect(new LiteralCallScanner().push('const mesh = other; mesh.scheduled({});')).toEqual([]);
  });

  it("detects a single-object literal call", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push('const a = await pi.read({ path: "src/index.ts" });');
    expect(found).toEqual([{ ref: "pi.read", args: { path: "src/index.ts" } }]);
  });

  it("detects zero-argument calls as {}", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push("const s = await schema.status();");
    expect(found).toEqual([{ ref: "schema.status", args: {} }]);
  });

  it("detects nested literal shapes", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push(
      'await pi.grep({ pattern: "TODO", path: "src", ignoreCase: true, context: 2, tags: ["a", 3, null] });',
    );
    expect(found).toEqual([
      {
        ref: "pi.grep",
        args: {
          pattern: "TODO",
          path: "src",
          ignoreCase: true,
          context: 2,
          tags: ["a", 3, null],
        },
      },
    ]);
  });

  it("detects several calls in one program, including Promise.all fan-out", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push(`
      const [a, b] = await Promise.all([
        pi.read({ path: "a.ts" }),
        pi.read({ path: "b.ts" }),
      ]);
      await memory.recall({ query: "speculation" });
    `);
    expect(found.map((c) => c.ref)).toEqual(["pi.read", "pi.read", "memory.recall"]);
  });

  it("builds three-segment MCP refs", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push(
      'const r = await mcp.exa.search({ query: "pi-fabric", numResults: 3 });',
    );
    expect(found).toEqual([
      { ref: "mcp.exa.search", args: { query: "pi-fabric", numResults: 3 } },
    ]);
  });

  it("rejects non-literal arguments without emitting", () => {
    const scanner = new LiteralCallScanner();
    const code = [
      "pi.read({ path: someVar })",
      "pi.read({ path: `prefix-${x}` })",
      "pi.read({ path: paths[0] })",
      "pi.read({ ...base })",
      "pi.read({ path: \"a\", other: compute() })",
    ].join(";\n") + ";";
    expect(scanner.push(code)).toEqual([]);
  });

  it("skips positional/multi-argument calls (normalization lives on the guest bridge)", () => {
    const scanner = new LiteralCallScanner();
    expect(scanner.push('pi.grep("TODO", "src");')).toEqual([]);
    expect(scanner.push('pi.ls("src");')).toEqual([]);
  });

  it("taints a namespace root when the program shadows it", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push(`
      const pi = { read: () => "fake" };
      pi.read({ path: "a.ts" });
      memory.recall({ query: "still fine" });
    `);
    expect(found).toEqual([{ ref: "memory.recall", args: { query: "still fine" } }]);
  });

  it("treats function parameters named like a root as shadowing", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push(`
      const run = (state) => state.get({ key: "k" });
    `);
    expect(found).toEqual([]);
  });

  it("emits each ref+args pair once across incremental pushes", () => {
    const scanner = new LiteralCallScanner();
    expect(scanner.push('const a = pi.read({ path: "a.ts" }')).toEqual([]);
    const first = scanner.push('const a = pi.read({ path: "a.ts" });\nconst b = pi.read({ path: "b.ts" });');
    expect(first.map((c) => c.args.path)).toEqual(["a.ts", "b.ts"]);
    // Re-pushed overlapping prefix must not re-emit a.ts.
    const second = scanner.push(
      'const a = pi.read({ path: "a.ts" });\nconst b = pi.read({ path: "b.ts" });\npi.read({ path: "a.ts" });',
    );
    expect(second).toEqual([]);
  });

  it("does not scan when the appended text cannot complete a call", () => {
    const scanner = new LiteralCallScanner();
    const noop = scanner.push('const x = pi.read({ path: "a.ts"');
    expect(noop).toEqual([]);
  });

  it("detects calls under conditionals (launch is speculative; freshness gates correctness)", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push('if (docs.length > 0) { await pi.read({ path: "d.md" }); }');
    expect(found).toEqual([{ ref: "pi.read", args: { path: "d.md" } }]);
  });

  it("handles negative and fractional numbers", () => {
    const scanner = new LiteralCallScanner();
    const found = scanner.push("await state.get({ key: \"k\", offset: -2, ratio: 0.5 });");
    expect(found).toEqual([{ ref: "state.get", args: { key: "k", offset: -2, ratio: 0.5 } }]);
  });
});
