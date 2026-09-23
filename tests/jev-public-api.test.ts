import { describe, expect, it } from "vitest";
import * as jev from "../src/jev.js";

describe("public Jev API", () => {
  it("exports Jev host APIs without concrete connector implementations", () => {
    expect(jev.JevClient).toBeTypeOf("function");
    expect(jev.JevProvider).toBeTypeOf("function");
    expect(jev.JevProgramManager).toBeTypeOf("function");
    expect(jev.JevObservationHost).toBeTypeOf("function");
    expect(Object.keys(jev).filter(name => /browser|harness/i.test(name))).toEqual([]);
  });
});
