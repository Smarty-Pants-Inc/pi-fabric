import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const reference = fs.readFileSync(path.resolve(import.meta.dirname, "../docs/turn-provenance.md"), "utf8");
const senderPolicy = reference.split("## Host API compatibility")[0]!;
const gate = reference.split("## Post-install gate")[1] ?? "";

describe("shipped turn-provenance contract (PR #229)", () => {
  it("F1: documents child attribution, unclaimed notices, and capable/legacy completion delivery", () => {
    expect(senderPolicy).not.toMatch(/Host-generated summaries[^\n]*identify the emitting Fabric runtime/u);
    expect(senderPolicy).not.toContain("Only the generated prewalk notices and directives claim Fabric.");
    expect(senderPolicy).toMatch(/Completion and handoff reports identify the originating child/u);
    expect(senderPolicy).toMatch(/unknown child identity[^.]*no claim/u);
    expect(senderPolicy).toMatch(/Participant-free[^.]*skill\/proxy[^.]*reload[^.]*shell[^.]*prewalk[^.]*no claim/u);
    expect(senderPolicy).toMatch(/Genuine actor output[^.]*actor[^.]*host failure alarms[^.]*no claim/u);
    expect(senderPolicy).toMatch(/completion[^.]*sender-homogeneous FIFO messages/u);
    expect(senderPolicy).toMatch(/Legacy hosts retain their existing batching/u);
  });

  it("#3127: extends recovery and the real-entry notice gate to pre-upgrade Main journals", () => {
    const recovery = reference.split("## First receipt and recovery")[1]!.split("## Originating principal")[0]!;
    expect(recovery).toContain('source: "actor-output"');
    expect(recovery).toMatch(/Pre-upgrade resident actor entries[^.]*replay unclaimed/u);
    expect(recovery).toMatch(/already received Pi history is not rewritten/u);
    expect(gate).toContain("https://github.com/Smarty-Pants-Inc/smarty-dev/issues/3127");
    expect(gate).toMatch(/journal-upgrade case[^\n]*no `source` classification/u);
    expect(gate).toMatch(/delete its acknowledged envelope/u);
    expect(gate).toMatch(/before Pi receives it[^.]*first persisted replay receipt[^.]*no Fabric claim/u);
    expect(gate).toMatch(/positively classified[^.]*actor sender[^.]*replay/u);
    expect(gate).toMatch(/before-upgrade journal[^.]*raw Pi entries/u);
  });

  it("F2: adopts a real-entry, owned, time-bounded gate with complete rollback conditions", () => {
    expect(gate).toMatch(/Owner: fabric-v2[^.]*check and rollback/u);
    expect(gate).toMatch(/capable Pi[^.]*Fabric release containing[^.]*installed[^.]*global[^.]*trust/u);
    expect(gate).toMatch(/within 24 hours[^.]*before[^.]*proven/u);
    expect(gate).toMatch(/real Pi CLI\/session/u);
    expect(gate).toMatch(/persisted session entries/u);
    expect(gate).toMatch(/completion[^.]*two different children[^.]*FIFO/u);
    expect(gate).toMatch(/handoff[^.]*returned child id/u);
    expect(gate).toMatch(/participant-free notices[^.]*no Fabric claim/u);
    expect(gate).toContain("https://github.com/Smarty-Pants-Inc/smarty-dev/issues/2775");
    expect(gate).toMatch(/Roll back[^.]*child sender[^.]*absent or wrong[^.]*participant-free notice[^.]*claim/u);
    expect(gate).toMatch(/human-principal claim[^.]*remote-to-native promotion[^.]*authorization change/u);
    expect(gate).toMatch(/fabric-v2[^.]*immediately[^.]*last known-good Fabric release/u);
    expect(gate).toMatch(/missing[^.]*evidence[^.]*gate open/u);
    expect(gate).toMatch(/dev-lead[^.]*three contract questions[^.]*owner hold/u);
  });
});
