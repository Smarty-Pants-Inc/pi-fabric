import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { decodeAndVerifyNatsJwt } from "../src/mesh/nats-auth/jwt.js";
import { createNatsAccount, createNatsOperator, mintNatsUser, type NatsMintOutput } from "../src/mesh/nats-auth/mint.js";
import { NatsPermissionsError, PATH_B_ADVISORY_DURABLE, PATH_B_OPERATOR_MACHINE, pathBRoleTemplate, roleTemplate, roleTemplateWithPathB, type RoleTemplate } from "../src/mesh/nats-auth/permissions.js";

const artifact = (output: NatsMintOutput<unknown>, name: string): string =>
  new TextDecoder().decode(output.artifacts.find(entry => entry.name === name)!.bytes).trim();
const fixture = (events?: boolean) => {
  const streams = { custodyStream: "TEST_CUSTODY", livenessStream: "TEST_LIVENESS" };
  const policy = { ...streams, pathBMachines: ["ryzen2"], ...(events === undefined ? {} : { events }), org: "smarty-dev" };
  const operator = createNatsOperator({ operatorName: "events-test" });
  const operatorJwt = artifact(operator, "operator.jwt");
  const account = createNatsAccount({
    ...policy, name: "smarty-dev", operatorJwt, operatorSigningSeed: artifact(operator, "operator-signing.nk"),
    jetstream: { memStorage: 0, diskStorage: 1, streams: 1, consumers: 1 },
  });
  const accountJwt = artifact(account, "account.jwt");
  const input = {
    ...policy, operatorJwt, accountJwt, accountPublicKey: account.metadata.account.publicKey,
    signingSeed: artifact(account, "signing-agent.nk"), role: "agent", host: "ryzen2",
    lane: "test", session: "test", release: "test", meshes: ["0".repeat(32)],
  };
  return { streams, policy, account, accountJwt, input };
};

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
const accountScope = (template: RoleTemplate) => ({
  pub: template.pub,
  sub: template.sub,
  ...(template.allow_responses ? { resp: template.allow_responses } : {}),
  subs: -1,
  data: -1,
  payload: -1,
});

// Keep reason (#4280): pure grant-shape checks catch trust-boundary wildcard regressions before
// account signing. The CLI-to-real-broker test is the real-path proof; these do not replace it.
describe("Path B NATS grants", () => {
  it("adds only the exact opted-in org event grants and preserves default/operator scope", () => {
    const streams = { custodyStream: "TEST_CUSTODY", livenessStream: "TEST_LIVENESS" };
    const machines = ["ryzen2"];
    const base = roleTemplateWithPathB("agent", streams, machines);
    const session = roleTemplateWithPathB("agent", streams, machines, "smarty-dev");
    const event = roleTemplateWithPathB("agent", streams, machines, { events: true, org: "smarty-dev" });
    const grants = [
      "fabric.ev.smarty-dev.>",
      "$JS.API.STREAM.INFO.FABRIC_EV_SMARTY_DEV",
      ...["FABRIC_EV_SMARTY_DEV", "GITHUB_SMARTY_DEV"].flatMap(stream => [
        `$JS.API.CONSUMER.CREATE.${stream}.>`,
        `$JS.API.CONSUMER.INFO.${stream}.>`,
        `$JS.API.CONSUMER.MSG.NEXT.${stream}.>`,
        `$JS.API.CONSUMER.DELETE.${stream}.>`,
        `$JS.API.CONSUMER.DURABLE.CREATE.${stream}.>`,
        `$JS.ACK.${stream}.>`
      ]),
    ];
    expect(new Set(event.pub.allow)).toEqual(new Set([...session.pub.allow, ...grants]));
    expect(event.pub.deny).toEqual(base.pub.deny);
    expect(event.sub).toEqual(session.sub);
    expect(event.allow_responses).toEqual({ max: 16 });
    expect(roleTemplateWithPathB("agent", streams, machines, { events: false, org: "smarty-dev" })).toEqual(session);
    expect(roleTemplateWithPathB("agent", streams, machines)).toEqual(base);
    expect(roleTemplateWithPathB("operator", streams, machines, { events: true, org: "smarty-dev" }))
      .toEqual(roleTemplateWithPathB("operator", streams, machines));
    expect(event.pub.allow).not.toContain("github.>");
    expect(() => roleTemplateWithPathB("agent", streams, machines, { events: true, org: "sys" })).toThrow();
  });

  it("mints the exact event-enabled account scope and refuses mismatched agent issuance", () => {
    for (const events of [undefined, false, true]) {
      const { streams, policy, account, accountJwt, input } = fixture(events);
      const keys = decodeAndVerifyNatsJwt(accountJwt).claims.nats.signing_keys as { role: string; template: ReturnType<typeof roleTemplate> }[];
      const agent = keys.find(key => key.role === "agent")!.template;
      expect(agent).toEqual(accountScope(roleTemplateWithPathB("agent", streams, policy.pathBMachines, policy)));
      expect(keys.find(key => key.role === "operator")!.template)
        .toEqual(accountScope(roleTemplateWithPathB("operator", streams, policy.pathBMachines)));
      expect(mintNatsUser(input).metadata.role).toBe("agent");
      expect(() => mintNatsUser({ ...input, events: events !== true })).toThrow("scope template differs");
      if (events === true) expect(() => mintNatsUser({ ...input, org: "other-org" })).toThrow("scope template differs");
      expect(mintNatsUser({
        ...policy, operatorJwt: input.operatorJwt, accountJwt, accountPublicKey: account.metadata.account.publicKey,
        role: "operator", signingSeed: artifact(account, "signing-operator.nk"), job: "compaction", runId: "test",
      }).metadata.role).toBe("operator");
    }
  });

  // USER.INFO is the trust boundary here; only network I/O is substituted. Existing broker suites prove server expansion.
  it.skipIf(process.platform === "win32")("requires every opted-in event grant while accepting agent permission supersets", async () => {
    const { streams, policy, input } = fixture(true);
    const user = mintNatsUser(input);
    const subject = user.metadata.publicKey;
    const template = roleTemplateWithPathB("agent", streams, policy.pathBMachines, policy);
    const expand = (subjects: string[]) => subjects.map(value => value
      .replaceAll("{{subject()}}", subject).replaceAll("{{tag(machine)}}", "ryzen2").replaceAll("{{tag(mesh)}}", input.meshes[0]!));
    const allowed = expand(template.pub.allow);
    let publish = allowed;
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-events-auth-"));
    const credentials = path.join(root, "user.creds");
    fs.writeFileSync(credentials, user.artifacts[0]!.bytes, { mode: 0o600 });
    const connect = vi.fn(async () => {
      let finish!: () => void;
      const closed = new Promise<void>(resolve => { finish = resolve; });
      return {
        request: async () => ({ data: new TextEncoder().encode(JSON.stringify({ data: { user: subject, permissions: {
          publish: { allow: publish, deny: expand(template.pub.deny) }, subscribe: { allow: expand(template.sub.allow) },
          responses: { max: 16, ttl: 120_000_000_000 },
        } } })) }),
        closed: () => closed,
        status: async function* () {},
        close: async () => { finish(); },
      };
    });
    vi.doMock("@nats-io/transport-node", async importOriginal => ({
      ...await importOriginal<typeof import("@nats-io/transport-node")>(), connect,
    }));
    vi.doMock("@nats-io/jetstream", () => ({ jetstream: () => ({}), jetstreamManager: async () => ({}) }));
    try {
      const { openNatsConnection } = await import("../src/transport/nats/connection.js");
      const baseOptions = { ...streams, pathBMachines: policy.pathBMachines, org: policy.org, credentials, machine: "ryzen2", servers: "tls://localhost:4222" };
      const options = { ...baseOptions, events: true };
      await (await openNatsConnection(options)).close();
      // An event-enabled signed user is a permission superset of the same session-channel policy.
      for (const off of [baseOptions, { ...baseOptions, events: false }]) await (await openNatsConnection(off)).close();
      await expect(openNatsConnection({ ...options, org: "other-org" })).rejects.toThrow("machine tag mismatch");
      publish = allowed.filter(value => value !== "fabric.ev.smarty-dev.>");
      await expect(openNatsConnection(options)).rejects.toThrow("machine tag mismatch");
      publish = [...allowed, "github.>"];
      await (await openNatsConnection(options)).close();
      publish = expand(roleTemplateWithPathB("agent", streams, policy.pathBMachines, { org: policy.org }).pub.allow);
      await expect(openNatsConnection(options)).rejects.toThrow("machine tag mismatch");
    } finally {
      vi.doUnmock("@nats-io/transport-node");
      vi.doUnmock("@nats-io/jetstream");
      vi.resetModules();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

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
