import { expect, it } from "vitest";
import { stripAclAdvisory } from "./helpers/acl-advisory.js";

const advisoryPrefix = "[pi-fabric] FABRIC_MESH_STATE_ACL_ADVISORY: ";

it("strips a real-format advisory line with dynamic root and reason", () => {
  const line = `${advisoryPrefix}Fabric mesh SQLite state Windows ACL privacy finding for C:\\mesh\\state: ACL check failed. Continuing with advisory ACL checks. Set PI_FABRIC_MESH_STATE_ACL_ENFORCE=1 to enforce refusal; remove/unset PI_FABRIC_MESH_STATE_ACL_ENFORCE to restore warning-only startup (no ACL changes needed).`;
  expect(stripAclAdvisory(line)).toBe("");
});

it("preserves empty output", () => {
  expect(stripAclAdvisory("")).toBe("");
});

it.each(["\n", "\r\n"] as const)("preserves all surrounding bytes with %j line endings", (eol) => {
  const before = "  earlier diagnostic\t";
  const advisory = `${advisoryPrefix}root=C:\\dynamic\\mesh reason=dynamic`;
  const after = " \tlater diagnostic  ";
  expect(stripAclAdvisory(`${before}${eol}${advisory}${eol}${after}`)).toBe(`${before}${eol}${after}`);
});

it("strips repeated advisory lines, including an EOF line without a final newline", () => {
  const before = "preserved diagnostic";
  const output = `${before}\r\n${advisoryPrefix}first payload\n${advisoryPrefix}final payload`;
  expect(stripAclAdvisory(output)).toBe(`${before}\r\n`);
});

it("preserves near-matches, unrelated errors, and blank or space-only lines", () => {
  const output = [
    ` ${advisoryPrefix}indented`,
    `embedded ${advisoryPrefix}not a line prefix`,
    "[pi-fabric] FABRIC_MESH_STATE_ACL_ADVISORYX: misspelled tag",
    "[pi-fabric] FABRIC_MESH_STATE_ACL_ADVISORY:missing literal space",
    "[pi-fabric] FABRIC_MESH_STATE_ACL_ERROR: unrelated diagnostic",
    "[pi-fabric] FABRIC_MESH_STATE_ACL_UNVERIFIED: no ACL reader",
    "unrelated error: preserve me",
    "",
    " \t ",
  ].join("\r\n");
  expect(stripAclAdvisory(output)).toBe(output);
});
