import type { LanguageRegistration } from "shiki";

// Bend 2 surface syntax: https://github.com/bendlang/bend/blob/main/guide/GUIDE.md
// Kept independent of Python/Bend 1: laws, quantities, and proof terms are native.
const bend: LanguageRegistration = {
  name: "bend",
  displayName: "Bend",
  scopeName: "source.bend",
  repository: {},
  patterns: [
    { name: "comment.line.number-sign.bend", match: "#.*$" },
    {
      name: "string.quoted.double.bend",
      begin: '"',
      end: '"',
      patterns: [{ name: "constant.character.escape.bend", match: String.raw`\\.` }],
    },
    {
      name: "string.quoted.single.bend",
      begin: "'",
      end: "'|$",
      patterns: [{ name: "constant.character.escape.bend", match: String.raw`\\.` }],
    },
    {
      match: String.raw`\b(def|law)(\s+)([A-Za-z_][\w.]*\??)`,
      captures: {
        1: { name: "keyword.declaration.bend" },
        3: { name: "entity.name.function.bend" },
      },
    },
    {
      match: String.raw`\b(type)(\s+)([A-Za-z_][\w.]*)`,
      captures: {
        1: { name: "keyword.declaration.bend" },
        3: { name: "entity.name.type.bend" },
      },
    },
    { name: "storage.modifier.bend", match: String.raw`@unsafe\b` },
    { name: "variable.other.hole.bend", match: String.raw`\?[A-Za-z_][\w.]*` },
    {
      name: "keyword.control.bend",
      match: String.raw`\b(?:import|as|is|match|case|do|return|for|exs|where)\b`,
    },
    { name: "constant.numeric.quantity.bend", match: String.raw`&[012]\b` },
    {
      name: "constant.numeric.bend",
      match: String.raw`\b(?:0[xX][\da-fA-F_]+|0[bB][01_]+|\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][+-]?\d[\d_]*)?n?)\b`,
    },
    {
      name: "support.type.bend",
      match: String.raw`\b(?:Type|Data|Kind|Quant|Nat|U32|F32|Char|String)\b(?!\.)`,
    },
    {
      name: "entity.name.function.bend",
      match: String.raw`\b[A-Za-z_][\w.]*\??(?=!?\s*\()`,
    },
    {
      name: "constant.other.constructor.bend",
      match: String.raw`\b[A-Za-z_][\w.]*(?=\s*\{)`,
    },
    { name: "entity.name.type.bend", match: String.raw`\b[A-Z][\w.]*` },
    {
      name: "keyword.operator.bend",
      match: String.raw`<&>|<>|->|=>|<-|==|!=|<=|>=|<<|>>|&&|\|\||\+\+|\.[&|^]\.|[-+*/%=&|^!~@<>]`,
    },
    { name: "punctuation.section.bend", match: String.raw`[()\[\]{}]` },
    { name: "punctuation.separator.bend", match: "[,;:]" },
    // Consume whole dotted names so a suffix such as IO.return is not a keyword.
    { name: "variable.other.readwrite.bend", match: String.raw`\b[A-Za-z_][\w.]*\??` },
  ],
};

export default [bend];
