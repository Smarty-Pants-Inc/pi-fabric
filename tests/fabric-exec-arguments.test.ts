import { describe, expect, it } from "vitest";
import { prepareFabricExecArguments, resolveFabricExecPayloads } from "../src/fabric-exec-arguments.js";

describe("prepareFabricExecArguments", () => {
  it("keeps canonical arguments unchanged", () => {
    const input = { code: "return 1;", tokenBudget: 10 };
    expect(prepareFabricExecArguments(input)).toBe(input);
    const withPayloads = { code: "return 1;", payloads: { body: "ok" } };
    expect(prepareFabricExecArguments(withPayloads)).toBe(withPayloads);
  });

  it("wraps a root code string before schema validation", () => {
    expect(prepareFabricExecArguments("return 1;")).toEqual({ code: "return 1;" });
  });

  it("joins all-string code arrays and leaves malformed arrays invalid", () => {
    expect(prepareFabricExecArguments({ code: ["const x = 1;", "return x;"] })).toEqual({
      code: "const x = 1;\nreturn x;",
    });
    const malformed = { code: ["return ", 1] };
    expect(prepareFabricExecArguments(malformed)).toBe(malformed);
  });

  it("omits null optional fields but preserves a null required code", () => {
    expect(prepareFabricExecArguments({
      code: null,
      payloads: null,
      strings: null,
      resultFormat: null,
      tokenBudget: null,
      agentBudget: undefined,
      display: null,
    })).toEqual({ code: null });
  });

  it("canonicalizes display shorthands before execution", () => {
    expect(prepareFabricExecArguments({ code: "return 1;", display: "Probe" })).toEqual({
      code: "return 1;",
      display: { name: "Probe" },
    });
    expect(prepareFabricExecArguments({
      code: "return 1;",
      display: '{"name":"Probe","description":"check"}',
    })).toEqual({
      code: "return 1;",
      display: { name: "Probe", description: "check" },
    });
  });

  it("remaps the strings alias onto payloads", () => {
    expect(prepareFabricExecArguments({
      code: "return π.body;",
      strings: { body: "ok" },
    })).toEqual({
      code: "return π.body;",
      payloads: { body: "ok" },
    });
    expect(prepareFabricExecArguments({
      code: "return π.body;",
      payloads: { body: "canonical" },
      strings: { body: "alias" },
    })).toEqual({
      code: "return π.body;",
      payloads: { body: "canonical" },
    });
  });

  it("parses JSON-object payload maps before schema validation", () => {
    const payload = { lifecycle: "#!/bin/sh\n# inventory" };
    expect(prepareFabricExecArguments({
      code: "return π.lifecycle;",
      payloads: JSON.stringify(payload),
    })).toEqual({
      code: "return π.lifecycle;",
      payloads: payload,
    });
    expect(prepareFabricExecArguments({
      code: "return π.body;",
      strings: JSON.stringify(JSON.stringify({ body: "ok" })),
    })).toEqual({
      code: "return π.body;",
      payloads: { body: "ok" },
    });
  });

  it("leaves malformed payload maps invalid on the canonical key", () => {
    expect(prepareFabricExecArguments({
      code: "return 1;",
      strings: "not-json",
    })).toEqual({
      code: "return 1;",
      payloads: "not-json",
    });
    expect(prepareFabricExecArguments({
      code: "return 1;",
      payloads: '["lifecycle"]',
    })).toEqual({
      code: "return 1;",
      payloads: '["lifecycle"]',
    });
    expect(prepareFabricExecArguments({
      code: "return 1;",
      strings: '{"n":1}',
    })).toEqual({
      code: "return 1;",
      payloads: '{"n":1}',
    });
  });

  // smarty-dev#2340: payloads are literal; placeholder tokens and file references
  // were passed verbatim and the program ran against the token text.
  describe("rejects payloads-only placeholders", () => {
    const literal = "payloads are literal: read the file with a native tool first and pass its content";
    it.each([
      ["__CONTENT__"],
      ["__FILE_BODY__"],
      ["@/tmp/body.md"],
      ["@/"],
      ["file:///tmp/body.md"],
    ])("rejects %j on payloads, JSON-object strings and the strings alias", (value) => {
      const forms = [
        { code: "return π.body;", payloads: { ok: "fine", body: value } },
        { code: "return π.body;", payloads: JSON.stringify({ ok: "fine", body: value }) },
        { code: "return π.body;", strings: { ok: "fine", body: value } },
        { code: "return π.body;", strings: JSON.stringify({ ok: "fine", body: value }) },
      ];
      for (const form of forms) {
        expect(() => prepareFabricExecArguments(form)).toThrow(literal);
        expect(() => prepareFabricExecArguments(form)).toThrow(/"body"/);
        expect(() => prepareFabricExecArguments(form, "python")).toThrow(literal);
        expect(() => resolveFabricExecPayloads(form)).toThrow(literal);
      }
    });

    it("preserves empty strings and ordinary text containing tokens or paths", () => {
      const payloads = {
        empty: "",
        prose: "replace __CONTENT__ with the body",
        mention: "see @/tmp/body.md for details",
        url: "fetch file:///tmp/x then stop",
        twoTokens: "@/a @/b",
        lower: "__content__",
        dunder: "__init__.py",
        scoped: "@scope/pkg",
        relative: "file:relative",
        multiline: "__CONTENT__\nmore",
        paddedPath: "  @/tmp/body.md",
        newlinePath: "@/tmp/body.md\n",
        paddedToken: " __CONTENT__ ",
        newlineToken: "__CONTENT__\n",
        paddedUrl: "file:///tmp/x ",
      };
      const input = { code: "return 1;", payloads };
      expect(prepareFabricExecArguments(input)).toBe(input);
      expect(prepareFabricExecArguments({ code: "return 1;", strings: JSON.stringify(payloads) }))
        .toEqual({ code: "return 1;", payloads });
      expect(resolveFabricExecPayloads({ payloads })).toEqual(payloads);
    });

    it("treats a value with a final newline as literal text, not a single token", () => {
      for (const value of ["@/file\n", "file:///file\n", "__READ__\n", "__READ__\r\n", "@/file\r\n"]) {
        const payloads = { body: value };
        expect(resolveFabricExecPayloads({ payloads })).toEqual(payloads);
        expect(prepareFabricExecArguments({ code: "return 1;", payloads: JSON.stringify(payloads) }))
          .toEqual({ code: "return 1;", payloads });
      }
      expect(() => resolveFabricExecPayloads({ payloads: { body: "@/file" } })).toThrow(/"body"/);
      expect(() => resolveFabricExecPayloads({ payloads: { body: "__READ__" } })).toThrow(/"body"/);
    });
  });
});
