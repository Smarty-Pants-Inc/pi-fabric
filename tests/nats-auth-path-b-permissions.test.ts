import { describe, expect, it } from "vitest";
import { NatsPermissionsError, PATH_B_ADVISORY_DURABLE, PATH_B_OPERATOR_MACHINE, pathBRoleTemplate, roleTemplate, roleTemplateWithPathB, type RoleTemplate } from "../src/mesh/nats-auth/permissions.js";

const matches = (pattern: string, subject: string): boolean => {
  const p = pattern.split("."), s = subject.split(".");
  for (let i = 0; i < p.length; i += 1) {
    if (p[i] === ">") return s.length > i;
    if (i >= s.length || (p[i] !== "*" && p[i] !== s[i])) return false;
  }
  return p.length === s.length;
};
const mayPublish = (template: RoleTemplate, subject: string): boolean =>
  template.pub.allow.some(pattern => matches(pattern, subject)) && !template.pub.deny.some(pattern => matches(pattern, subject));
const maySubscribe = (template: RoleTemplate, subject: string): boolean =>
  template.sub.allow.some(pattern => matches(pattern.replaceAll("{{subject()}}", "fixture-user"), subject));

// Keep reason (#4280): pure grant-shape checks catch trust-boundary wildcard regressions before
// account signing. The CLI-to-real-broker test is the real-path proof; these do not replace it.
describe("Path B NATS grants", () => {
  it("keeps agent application and API writes bound to its machine tag", () => {
    const template = pathBRoleTemplate("agent", "ryzen1");
    expect(mayPublish(template, "fabric.in.ryzen2.abcd.cmd.1234.ryzen1")).toBe(true);
    expect(mayPublish(template, "fabric.in.ryzen2.abcd.cmd.1234.ryzen2")).toBe(false);
    expect(mayPublish(template, "fabric.ev.ryzen1.fleet.work.topic")).toBe(false);
    expect(mayPublish(template, "fabric.ev.ryzen2.fleet.work.topic")).toBe(false);
    expect(mayPublish(template, "$JS.API.CONSUMER.MSG.NEXT.FABRIC_IN_ryzen1.rt_x")).toBe(true);
    expect(mayPublish(template, "$JS.API.CONSUMER.MSG.NEXT.FABRIC_IN_ryzen2.rt_x")).toBe(false);
    expect(mayPublish(template, "$JS.ACK.FABRIC_IN_ryzen1.rt_x.1.1")).toBe(true);
    expect(mayPublish(template, "$JS.ACK.FABRIC_IN_ryzen2.rt_x.1.1")).toBe(false);
    expect(mayPublish(template, "$KV.fabric_presence.ryzen1.participant.x")).toBe(true);
    expect(mayPublish(template, "$KV.fabric_presence.ryzen2.participant.x")).toBe(false);
  });

  it("refuses private inbox reads and all runtime stream administration", () => {
    const template = pathBRoleTemplate("agent", "ryzen1");
    for (const subject of [
      "$JS.API.STREAM.MSG.GET.FABRIC_IN_ryzen1",
      "$JS.API.DIRECT.GET.FABRIC_IN_ryzen1",
      "$JS.API.STREAM.DELETE.FABRIC_IN_ryzen1",
      "$JS.API.STREAM.CREATE.FABRIC_IN_ryzen1",
      "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.operator",
    ]) expect(mayPublish(template, subject), subject).toBe(false);
    expect(template.pub.deny).not.toContain("$JS.API.STREAM.MSG.GET.FABRIC_IN_*");
    expect(template.pub.deny).not.toContain("$JS.API.DIRECT.GET.FABRIC_IN_*.>");
    expect(template.sub.allow).toEqual(["_INBOX.{{subject()}}.>"]);
  });

  it("grants the shared runtime role org-scoped service access and reply permissions", () => {
    const org = "smarty-pants-inc";
    const streams = { custodyStream: "TEST_CUSTODY", livenessStream: "TEST_LIVENESS" };
    const runtime = roleTemplateWithPathB("agent", streams, ["ryzen1", "ryzen2"], org);
    const serviceScope = `fabric.svc.${org}.>`;
    expect(runtime.pub.allow).toContain(serviceScope);
    expect(runtime.sub.allow).toContain(serviceScope);
    expect(runtime.allow_responses).toEqual({ max: 16 });
    expect(runtime.pub.allow).toContain("$JS.FC.KV_fabric_presence.>");
    expect(runtime.pub.allow).toContain("$JS.FC.KV_fabric_leases.>");
    for (const host of ["ryzen1", "ryzen2"]) {
      for (const operation of ["snapshot", "history", "input", "model", "abort"]) {
        const subject = `fabric.svc.${org}.${host}.session.service.${operation}`;
        expect(mayPublish(runtime, subject), subject).toBe(true);
        expect(maySubscribe(runtime, subject), subject).toBe(true);
      }
      const events = `fabric.svc.${org}.${host}.session.service.events`;
      expect(mayPublish(runtime, events), events).toBe(true);
      expect(maySubscribe(runtime, events), events).toBe(true);
    }
    expect(mayPublish(runtime, "fabric.svc.other.ryzen1.session.service.input")).toBe(false);
    expect(maySubscribe(runtime, "fabric.svc.other.ryzen1.session.service.input")).toBe(false);
    for (const valid of ["s", "0", "smarty-pants", "a".repeat(63)]) {
      expect(pathBRoleTemplate("agent", "ryzen1", [], valid).pub.allow).toContain(`fabric.svc.${valid}.>`);
    }
    for (const invalid of ["", ".", "smarty.other", "smarty.*", "smarty.>", "*", ">", "Smarty", "smarty_org", "a".repeat(64)]) {
      expect(() => roleTemplateWithPathB("agent", streams, ["ryzen1"], invalid)).toThrow(NatsPermissionsError);
    }
  });

  it("gives the operator metadata and fixed advisory rights without reads, relays or lifecycle authority", () => {
    const template = pathBRoleTemplate("operator", PATH_B_OPERATOR_MACHINE, ["ryzen2"]);
    expect(mayPublish(template, "$JS.API.STREAM.CREATE.FABRIC_IN_ryzen2")).toBe(false);
    expect(mayPublish(template, "$JS.API.STREAM.UPDATE.FABRIC_ADV")).toBe(false);
    expect(mayPublish(template, "$JS.API.STREAM.INFO.FABRIC_IN_ryzen2")).toBe(true);
    expect(mayPublish(template, `$JS.ACK.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}.1.1`)).toBe(false);
    expect(template.pub.deny).toContain("$JS.ACK.>");
    expect(mayPublish(template, `$JS.API.CONSUMER.INFO.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`)).toBe(true);
    expect(mayPublish(template, `$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`)).toBe(true);
    for (const subject of [
      `$JS.API.CONSUMER.CREATE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
      `$JS.API.CONSUMER.DELETE.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`,
      "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.other",
      "$JS.API.CONSUMER.INFO.FABRIC_ADV.other",
      "$JS.ACK.FABRIC_ADV.other.1.1",
    ]) expect(mayPublish(template, subject), subject).toBe(false);
    expect(mayPublish(template, "$JS.API.STREAM.DELETE.FABRIC_IN_ryzen2")).toBe(false);
    expect(mayPublish(template, "$JS.API.STREAM.PURGE.FABRIC_EV")).toBe(false);
    expect(mayPublish(template, "fabric.in.ryzen2.owner.ack.target.fabric-ops")).toBe(false);
    expect(mayPublish(template, "$JS.API.CONSUMER.INFO.FABRIC_IN_ryzen2.rt_owner")).toBe(true);
    expect(mayPublish(template, "$JS.API.STREAM.MSG.GET.FABRIC_IN_ryzen2")).toBe(false);
    expect(mayPublish(template, "$JS.API.STREAM.MSG.GET.KV_fabric_leases")).toBe(false);
    expect(mayPublish(template, "fabric.ev.fabric-ops.fabric.ops.dlq")).toBe(false);
    expect(mayPublish(template, "fabric.in.ryzen4.owner.ack.target.fabric-ops")).toBe(false);
    expect(mayPublish(template, "fabric.in.ryzen2.owner.ack.target.ryzen1")).toBe(false);
    expect(mayPublish(template, "fabric.in.ryzen2.owner.cmd.target.fabric-ops")).toBe(false);
    expect(mayPublish(template, "$JS.API.STREAM.MSG.DELETE.FABRIC_IN_ryzen2")).toBe(false);
    expect(mayPublish(template, "$JS.API.DIRECT.GET.FABRIC_IN_ryzen2")).toBe(false);
  });

  it("reserves all advisory mutation forms for every opted-in nonbootstrap role without narrowing normal maintainer allows", () => {
    const streams = { custodyStream: "TEST_CUSTODY", livenessStream: "TEST_LIVENESS" };
    const reserved = [
      "$JS.API.CONSUMER.CREATE.FABRIC_ADV",
      "$JS.API.CONSUMER.CREATE.FABRIC_ADV.transport_dlq_operator",
      "$JS.API.CONSUMER.CREATE.FABRIC_ADV.transport_dlq_operator.>",
      "$JS.API.CONSUMER.CREATE.FABRIC_ADV.>",
      "$JS.API.CONSUMER.DURABLE.CREATE.FABRIC_ADV.>",
      "$JS.API.CONSUMER.DELETE.FABRIC_ADV.transport_dlq_operator",
      "$JS.API.CONSUMER.DELETE.FABRIC_ADV.>",
      "$JS.API.CONSUMER.PAUSE.FABRIC_ADV.>",
      "$JS.API.CONSUMER.UNPIN.FABRIC_ADV.>",
      "$JS.API.CONSUMER.RESET.FABRIC_ADV.>",
      "$JS.API.CONSUMER.LEADER.STEPDOWN.FABRIC_ADV.>",
      "$JS.API.STREAM.CREATE.FABRIC_ADV",
      "$JS.API.STREAM.UPDATE.FABRIC_ADV",
      "$JS.API.STREAM.DELETE.FABRIC_ADV",
      "$JS.API.STREAM.PURGE.FABRIC_ADV",
      "$JS.API.STREAM.MSG.DELETE.FABRIC_ADV",
      "$JS.API.STREAM.SNAPSHOT.FABRIC_ADV",
      "$JS.API.STREAM.RESTORE.FABRIC_ADV",
      "$JS.API.STREAM.LEADER.STEPDOWN.FABRIC_ADV",
      "$JS.API.STREAM.PEER.REMOVE.FABRIC_ADV",
      "$JS.API.ACCOUNT.STREAM.MOVE.*.FABRIC_ADV",
      "$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.*.FABRIC_ADV",
    ];
    for (const role of ["principal", "agent", "maintainer", "operator"] as const) {
      const template = roleTemplateWithPathB(role, streams, ["ryzen2"]);
      for (const subject of reserved) expect(template.pub.deny, `${role}: ${subject}`).toContain(subject);
      for (const subject of [...reserved.map((pattern) => pattern.replace(".>", ".reader").replace(".*.", ".fixture-account.")), "$JS.API.CONSUMER.CREATE.FABRIC_ADV.transport_dlq_operator.filter", "$JS.API.CONSUMER.DURABLE.CREATE.FABRIC_ADV.transport_dlq_operator"]) {
        expect(mayPublish(template, subject), `${role}: ${subject}`).toBe(false);
      }
    }
    for (const role of ["principal", "maintainer"] as const) {
      const base = roleTemplate(role, streams);
      const opted = roleTemplateWithPathB(role, streams);
      expect(opted.pub.allow).toEqual(base.pub.allow);
      expect(opted.pub.deny).toEqual([...base.pub.deny, ...reserved, "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.>"]);
      for (const pattern of reserved) {
        expect(pattern.split("."), "reservation uses a whole literal stream token").toContain("FABRIC_ADV");
        for (const stream of ["OTHER", "FABRIC_ADV_OTHER"]) {
          const subject = pattern.replace("FABRIC_ADV", stream).replace(".>", ".reader").replace(".*.", ".fixture-account.");
          expect(opted.pub.deny.some((deny) => matches(deny, subject)), `${role}: unrelated ${subject}`).toBe(false);
        }
      }
      expect(opted.sub).toEqual(base.sub);
      expect(mayPublish(opted, "$JS.API.CONSUMER.CREATE.OTHER.reader")).toBe(true);
      expect(mayPublish(opted, "$JS.API.CONSUMER.DELETE.OTHER.reader")).toBe(true);
    }
    expect(roleTemplateWithPathB("break-glass", streams)).toEqual(roleTemplate("break-glass", streams));
  });

  it("reserves state-advancing advisory pulls to the operator's exact durable", () => {
    const streams = { custodyStream: "TEST_CUSTODY", livenessStream: "TEST_LIVENESS" };
    for (const role of ["agent", "principal", "maintainer"] as const) {
      const template = roleTemplateWithPathB(role, streams, ["ryzen2"]);
      expect(template.pub.deny).toContain("$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.>");
      expect(mayPublish(template, `$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`)).toBe(false);
      expect(mayPublish(template, "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.other")).toBe(false);
    }
    const operator = roleTemplateWithPathB("operator", streams, ["ryzen2"]);
    expect(operator.pub.deny).not.toContain("$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.>");
    expect(mayPublish(operator, `$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.${PATH_B_ADVISORY_DURABLE}`)).toBe(true);
    expect(mayPublish(operator, "$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.other")).toBe(false);
  });

  it("rejects unsafe machine literals and keeps template values fresh", () => {
    expect(() => pathBRoleTemplate("agent", "ryzen.1")).toThrow("invalid machine");
    expect(() => pathBRoleTemplate("agent", PATH_B_OPERATOR_MACHINE)).toThrow("invalid machine");
    expect(() => pathBRoleTemplate("agent", "fabric-ops-canary")).toThrow("invalid machine");
    expect(pathBRoleTemplate("operator", "fabric-ops-canary", ["ryzen2"]).pub.allow).toContain("$JS.API.CONSUMER.MSG.NEXT.FABRIC_ADV.transport_dlq_operator");
    expect(() => pathBRoleTemplate("operator", "ryzen1", ["ryzen2"])).toThrow("invalid machine");
    const first = pathBRoleTemplate("agent");
    first.pub.allow.push("fabric.in.>");
    expect(pathBRoleTemplate("agent").pub.allow).not.toContain("fabric.in.>");
    expect(() => pathBRoleTemplate("other" as never, "ryzen1")).toThrow("invalid Path B role");
  });
});
