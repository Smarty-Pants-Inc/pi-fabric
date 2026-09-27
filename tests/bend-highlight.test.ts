import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { stripVTControlCharacters } from "node:util";
import { createHighlighter, type HighlighterCore } from "shiki";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import bend from "../src/ui/languages/bend.js";
import { shikiLanguages } from "../src/ui/shiki-catalog.js";
import {
  configureHighlighting,
  highlightCode,
  highlightFileLines,
  highlightSourceLines,
  initHighlighting,
} from "../src/ui/highlight.js";

const source = [
  "import Base",
  "# Every natural plus zero is unchanged.",
  "law add_zero:",
  "  for x: Nat",
  "  {Nat.add(x, 0n) == x : Nat}",
  "def add_zero(x):",
  "  match x:",
  "    case 0n:",
  "      {==}",
  "    case 1n+p:",
  "      %add_zero(p) : {1n+Nat.add(p, 0n) == 1n+_ : Nat}",
  "      {==}",
].join("\n");

describe("Bend 2 grammar", () => {
  let highlighter: HighlighterCore;
  beforeAll(async () => {
    highlighter = await createHighlighter({ themes: ["dark-plus", "github-light"], langs: bend });
  });
  afterAll(() => highlighter?.dispose());

  it.each([
    ["def Laws.add_zero?(x):", "def", "keyword.declaration.bend"],
    ["def Laws.add_zero?(x):", "Laws.add_zero?", "entity.name.function.bend"],
    ["law add_zero:", "add_zero", "entity.name.function.bend"],
    ["type Shape is Data:", "Shape", "entity.name.type.bend"],
    ["type Shape is Data:", "Data", "support.type.bend"],
    ["for x: Nat", "for", "keyword.control.bend"],
    ["exs y: U32 where P(y)", "exs", "keyword.control.bend"],
    ["exs y: U32 where P(y)", "where", "keyword.control.bend"],
    ["match x:", "match", "keyword.control.bend"],
    ["case Circle{+r}:", "Circle", "constant.other.constructor.bend"],
    ["do IO<Unit>:", "do", "keyword.control.bend"],
    ["return x", "return", "keyword.control.bend"],
    ["@unsafe def f(x):", "@unsafe", "storage.modifier.bend"],
    ["?TODO", "?TODO", "variable.other.hole.bend"],
    ["Kind(&2)", "&2", "constant.numeric.quantity.bend"],
    ["1n+p", "1n", "constant.numeric.bend"],
    ["42", "42", "constant.numeric.bend"],
    ["1.5", "1.5", "constant.numeric.bend"],
    ["0xff", "0xff", "constant.numeric.bend"],
    ["%add_zero(p)", "%", "keyword.operator.bend"],
    ["{==}", "==", "keyword.operator.bend"],
    ["x : A <- m", "<-", "keyword.operator.bend"],
    ["x => x", "=>", "keyword.operator.bend"],
    ["a <&> b", "<&>", "keyword.operator.bend"],
    ["pow2!(20n)", "pow2", "entity.name.function.bend"],
    ["pow2!(20n)", "!", "keyword.operator.bend"],
    ["IO.return(x)", "IO.return", "entity.name.function.bend"],
    ["module.match", "module.match", "variable.other.readwrite.bend"],
    ["# law for 0n", "# law for 0n", "comment.line.number-sign.bend"],
    ['"# law for 0n"', "# law for 0n", "string.quoted.double.bend"],
    ["'c'", "c", "string.quoted.single.bend"],
    [String.raw`"quote: \" # still a string"`, String.raw`\"`, "constant.character.escape.bend"],
    [String.raw`'\n'`, String.raw`\n`, "constant.character.escape.bend"],
  ])("scopes %s → %s", (code, fragment, scope) => {
    const explanations = highlighter.codeToTokensBase(code, {
      lang: "bend", theme: "dark-plus", includeExplanation: true,
    }).flat().flatMap(token => token.explanation ?? []);
    expect(explanations.some(part => part.content === fragment &&
      part.scopes.some(entry => entry.scopeName === scope))).toBe(true);
  });

  it.each(["dark-plus", "github-light"])("preserves source with distinct themed colors (%s)", theme => {
    const tokens = highlighter.codeToTokensBase(source, { lang: "bend", theme });
    expect(tokens.map(line => line.map(token => token.content).join("")).join("\n")).toBe(source);
    expect(new Set(tokens.flat().map(token => token.color)).size).toBeGreaterThan(3);
  });

  it("extends the catalog without mutating Shiki's bundled registry", async () => {
    const { bundledLanguages } = await import("shiki/langs");
    expect(bundledLanguages).not.toHaveProperty("bend");
    expect(shikiLanguages()).toHaveProperty("bend");
    expect(shikiLanguages()).toBe(shikiLanguages());
    expect(shikiLanguages().python).toBe(bundledLanguages.python);
  });
});

describe("Bend preview integration", () => {
  it("lazily highlights snippets, files, and diff source with invalidation", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-fabric-bend-"));
    const path = join(dir, "PROOF.bend");
    writeFileSync(path, source);
    try {
      configureHighlighting("dark-plus", false);
      await initHighlighting("dark-plus", true);
      const invalidate = vi.fn();
      expect(highlightCode(source, "Bend", invalidate)).toBeNull();
      await vi.waitFor(() => expect(invalidate).toHaveBeenCalled(), { timeout: 15_000 });
      const full = highlightCode(source, "bend")!;
      expect(full.map(stripVTControlCharacters).join("\n")).toBe(source);
      expect(full.join("\n")).toContain("\x1b[38;2;");

      await vi.waitFor(() => {
        expect(highlightFileLines(path, "bend", 2, 5, invalidate)).not.toBeNull();
        expect(highlightSourceLines("bend-proof", source.split("\n"), "bend", 2, 5, invalidate)).not.toBeNull();
      }, { timeout: 15_000 });
      for (const lines of [
        highlightFileLines(path, "bend", 2, 5),
        highlightSourceLines("bend-proof", source.split("\n"), "bend", 2, 5),
      ]) {
        expect(lines!.map(line => line.ansi)).toEqual(full.slice(2, 5));
        expect(lines!.map(line => line.raw)).toEqual(source.split("\n").slice(2, 5));
      }
      configureHighlighting("dark-plus", false);
      expect(highlightCode(source, "bend")).toBeNull();
      expect(highlightFileLines(path, "bend", 2, 5)).toBeNull();
    } finally {
      configureHighlighting("dark-plus", false);
      rmSync(dir, { recursive: true, force: true });
    }
  }, 20_000);
});
