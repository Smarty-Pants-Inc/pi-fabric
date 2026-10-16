import { describe, expect, it } from "vitest";
import { nkeys } from "@nats-io/transport-node";
import { assertSigningKeyRetirable } from "../src/mesh/nats-auth/retirement.js";
import {
  createNatsAccount,
  createNatsOperator,
  mintNatsUser,
  mintNatsBreakGlassUser,
  type NatsMintOutput,
} from "../src/mesh/nats-auth/mint.js";
import { decodeAndVerifyNatsJwt } from "../src/mesh/nats-auth/jwt.js";
import { roleTemplate, roleTemplateWithPathB, type NatsRole } from "../src/mesh/nats-auth/permissions.js";

const clock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };
const machines = ["ryzen2", "ryzen4"];
const streams = { custodyStream: "TEST_CUSTODY", livenessStream: "TEST_LIVENESS" };
const decode = new TextDecoder();
const artifact = (output: NatsMintOutput<unknown>, name: string): string => {
  const value = output.artifacts.find((entry) => entry.name === name);
  if (!value) throw new Error(`missing test artifact ${name}`);
  return decode.decode(value.bytes).trim();
};
const claims = (jwt: string): Record<string, any> => decodeAndVerifyNatsJwt(jwt).claims;

const fixture = () => {
  const operator = createNatsOperator({ operatorName: "test-path-b" }, clock);
  const operatorJwt = artifact(operator, "operator.jwt");
  const account = createNatsAccount({
    name: "test-path-b",
    operatorJwt,
    operatorSigningSeed: artifact(operator, "operator-signing.nk"),
    jetstream: { memStorage: 0, diskStorage: 16 * 1024 * 1024, streams: 16, consumers: 32 },
    ...streams,
    pathBMachines: machines,
  }, clock);
  const common = {
    operatorJwt,
    accountPublicKey: account.metadata.account.publicKey,
    accountJwt: artifact(account, "account.jwt"),
    ...streams,
    pathBMachines: machines,
  };
  return { account, common };
};

// Keep reason (#4280): signing-policy/refusal probes cover the credential trust boundary
// (especially malformed inventory and reserved identities) without issuing invalid live grants.
// CLI-to-real-broker coverage separately proves that valid minted files actually enforce it.
describe("Path B opt-in mint policy", () => {
  it("binds the machine tag and adds runtime grants while keeping legacy state and snapshot grants", () => {
    const { account, common } = fixture();
    const input = {
      ...common,
      signingSeed: artifact(account, "signing-agent.nk"),
      role: "agent",
      host: "ryzen2",
      lane: "test",
      session: "test",
      release: "test",
      meshes: ["0".repeat(32)],
    };
    const output = mintNatsUser(input, clock);
    const creds = artifact(output, "user.creds");
    const jwt = creds.match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([^\s]+)\s*-{3,}END NATS USER JWT-{3,}/)?.[1];
    expect(jwt).toBeDefined();
    expect(claims(jwt!).nats.tags).toContain("machine:ryzen2");
    const accountClaims = claims(common.accountJwt);
    const agent = accountClaims.nats.signing_keys.find((entry: any) => entry.role === "agent");
    expect(agent.template.pub.allow).toContain("fabric.in.*.*.*.*.{{tag(machine)}}");
    expect(agent.template.pub.allow).toContain("FABRIC_MESH_{{tag(mesh)}}.state");
    expect(agent.template.pub.allow).toContain("FABRIC_MESH_{{tag(mesh)}}.snapshot");
    for (const subject of roleTemplate("agent", streams).pub.allow) expect(agent.template.pub.allow).toContain(subject);
    for (const subject of roleTemplate("agent", streams).pub.deny) expect(agent.template.pub.deny).toContain(subject);
    expect(agent.description).toMatch(/^path-b\/[a-f0-9]{64}$/);
    expect(() => mintNatsUser({ ...input, host: "ryzen5" }, clock)).toThrow();
    expect(() => mintNatsUser({ ...input, pathBMachines: ["ryzen2", "ryzen5"] }, clock)).toThrow();
    const { pathBMachines: omitted, ...withoutPolicy } = input;
    expect(omitted).toBeDefined();
    expect(() => mintNatsUser(withoutPolicy, clock)).toThrow();
  });

  it("infers the signed reservation without inventory for principal, normal maintainer and break-glass", () => {
    const { account, common } = fixture();
    const { pathBMachines: inventory, ...legacyCommon } = common;
    expect(inventory).toEqual(machines);
    const issued = [
      mintNatsUser({ ...legacyCommon, role: "principal", signingSeed: artifact(account, "signing-principal.nk"), person: "test", device: "test" }, clock),
      mintNatsBreakGlassUser({ ...legacyCommon, signingSeed: artifact(account, "signing-break-glass.nk"), person: "test", runId: "test", approvalRef: "issue:13871" }, clock),
      mintNatsUser({ ...legacyCommon, role: "maintainer", signingSeed: artifact(account, "signing-maintainer.nk"), job: "migration", runId: "test" }, clock),
    ];
    const scoped = claims(common.accountJwt).nats.signing_keys;
    for (const [index, role] of ["principal", "break-glass", "maintainer"].entries()) {
      const output = issued[index]!;
      expect(output.metadata.role).toBe(role);
      const jwt = artifact(output, "user.creds").match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([^\s]+)\s*-{3,}END NATS USER JWT-{3,}/)?.[1];
      expect(jwt).toBeDefined();
      expect(claims(jwt!).nats.tags).toContain(`role:${role}`);
      expect(claims(jwt!).nats.tags.some((tag: string) => tag.startsWith("machine:"))).toBe(false);
      const key = scoped.find((entry: any) => entry.role === role);
      expect(key.description).toBe("");
      expect(key.template).toEqual({ ...roleTemplateWithPathB(role as NatsRole, streams), subs: -1, data: -1, payload: -1 });
      expect(key.template.pub.allow).toEqual(roleTemplate(role as NatsRole, streams).pub.allow);
    }
    expect(() => mintNatsUser({ ...legacyCommon, role: "maintainer", signingSeed: artifact(account, "signing-maintainer.nk"), job: "compaction", runId: "test", machine: "fabric-ops" }, clock)).toThrow("unsupported field");
    for (const role of ["agent", "operator"]) {
      expect(scoped.find((entry: any) => entry.role === role).description).toMatch(/^path-b\/[a-f0-9]{64}$/);
    }
  });

  it("gives the messaging operator no stream lifecycle or inbox acknowledgement authority", () => {
    const { account, common } = fixture();
    const input = {
      ...common,
      signingSeed: artifact(account, "signing-operator.nk"),
      role: "operator",
      machine: "fabric-ops",
      job: "compaction",
      runId: "test",
    };
    const issued = mintNatsUser(input, clock);
    expect(issued.metadata.role).toBe("operator");
    expect(issued.metadata.signer).toBe(account.metadata.signingKeys.operator);
    expect(issued.metadata.signer).not.toBe(account.metadata.signingKeys.maintainer);
    expect(() => mintNatsUser({ ...input, signingSeed: artifact(account, "signing-maintainer.nk") }, clock)).toThrow("invalid signingSeed");
    const jwt = artifact(issued, "user.creds").match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([^\s]+)\s*-{3,}END NATS USER JWT-{3,}/)?.[1];
    expect(claims(jwt!).nats.tags).toContain("machine:fabric-ops");
    expect(() => mintNatsUser({ ...input, machine: "ryzen1" }, clock)).toThrow();
    const operator = claims(common.accountJwt).nats.signing_keys.find((entry: any) => entry.role === "operator");
    for (const verb of ["CREATE", "UPDATE", "DELETE", "PURGE"]) {
      expect(operator.template.pub.allow.some((subject: string) => subject.startsWith(`$JS.API.STREAM.${verb}.`))).toBe(false);
    }
    expect(operator.template.pub.allow).not.toContain("$JS.ACK.FABRIC_IN_ryzen2.>");
    expect(operator.template.pub.allow.some((subject: string) => subject.startsWith("$JS.ACK."))).toBe(false);
    expect(operator.template.pub.deny).toContain("$JS.ACK.>");
    expect(operator.template.pub.allow.some((subject: string) => /^\$JS\.API\.CONSUMER\.(CREATE|DELETE)\./.test(subject))).toBe(false);
    expect(operator.template.pub.allow).not.toContain("$JS.API.STREAM.MSG.GET.FABRIC_IN_ryzen2");
    expect(operator.template.pub.allow).toContain("$JS.API.CONSUMER.INFO.FABRIC_IN_ryzen2.>");
    expect(operator.template.pub.allow).not.toContain("fabric.in.ryzen2.*.ack.*.{{tag(machine)}}");
    expect(operator.template.pub.allow).not.toContain("fabric.in.ryzen4.*.ack.*.{{tag(machine)}}");
    expect(operator.template.pub.allow).not.toContain("fabric.in.*.*.ack.*.{{tag(machine)}}");
    const { machine: omitted, ...withoutMachine } = input;
    expect(omitted).toBeDefined();
    const defaultJwt = artifact(mintNatsUser(withoutMachine, clock), "user.creds").match(/BEGIN NATS USER JWT-----\s*([^\s]+)/)?.[1];
    expect(claims(defaultJwt!).nats.tags).toContain("machine:fabric-ops");
    const { pathBMachines: omittedInventory, ...withoutInventory } = input;
    expect(omittedInventory).toEqual(machines);
    expect(() => mintNatsUser(withoutInventory, clock)).toThrow("invalid pathBMachines");
    expect(() => mintNatsUser({ ...input, pathBMachines: ["ryzen2", "ryzen5"] }, clock)).toThrow("invalid pathBMachines");
  });

  it("T-OPS-MINT: reserves the whole operator namespace and refuses operators outside it", () => {
    const { account, common } = fixture();
    const input = { ...common, signingSeed: artifact(account, "signing-agent.nk"), role: "agent", lane: "test", session: "test", release: "test", meshes: ["0".repeat(32)] };
    for (const host of ["fabric-ops", "fabric-ops-canary", "fabric-ops-2"]) {
      expect(() => mintNatsUser({ ...input, host }, clock)).toThrow("invalid host");
      expect(() => mintNatsUser({ ...input, host, pathBMachines: [host] }, clock)).toThrow("invalid pathBMachines");
    }
    const operator = { ...common, signingSeed: artifact(account, "signing-operator.nk"), role: "operator", job: "compaction", runId: "test" };
    for (const machine of ["ryzen2", "ryzen1", "fabric-ops-", "fabric-ops-two-parts", null]) {
      expect(() => mintNatsUser({ ...operator, machine }, clock)).toThrow("invalid machine");
    }
    for (const machine of ["fabric-ops", "fabric-ops-canary"]) {
      expect(mintNatsUser({ ...operator, machine }, clock).metadata.role).toBe("operator");
    }
  });

  it("accepts operator inventory for another key's retirement but still enforces its own expiry", () => {
    const { account, common } = fixture();
    const issued = mintNatsUser({ ...common, role: "operator", signingSeed: artifact(account, "signing-operator.nk"), job: "compaction", runId: "retirement" }, clock);
    const otherAccountKey = nkeys.createAccount();
    const credential = { ...issued.metadata, role: "operator" as const };
    try {
      expect(() => assertSigningKeyRetirable(otherAccountKey.getPublicKey(), [credential], clock)).not.toThrow();
      expect(() => assertSigningKeyRetirable(issued.metadata.signer, [credential], clock)).toThrow();
      expect(() => assertSigningKeyRetirable(issued.metadata.signer, [credential], { nowSeconds: () => issued.metadata.expiresAt })).not.toThrow();
    } finally { otherAccountKey.clear(); }
  });

  it("keeps the four non-opted-in scoped keys and their exact templates without an operator artifact", () => {
    const operator = createNatsOperator({ operatorName: "legacy-fixture" }, clock);
    const operatorJwt = artifact(operator, "operator.jwt");
    const account = createNatsAccount({ name: "legacy-fixture", operatorJwt, operatorSigningSeed: artifact(operator, "operator-signing.nk"), jetstream: { memStorage: 0, diskStorage: 1, streams: 1, consumers: 1 }, ...streams }, clock);
    expect(Object.keys(account.metadata.signingKeys)).toEqual(["agent", "principal", "maintainer", "break-glass"]);
    expect(account.artifacts.map((entry) => entry.name)).not.toContain("signing-operator.nk");
    for (const entry of claims(artifact(account, "account.jwt")).nats.signing_keys) {
      expect(entry.description).toBe("");
      expect(entry.template).toEqual({ ...roleTemplate(entry.role, streams), subs: -1, data: -1, payload: -1 });
    }
    expect(() => mintNatsUser({ role: "operator", operatorJwt, accountJwt: artifact(account, "account.jwt"), accountPublicKey: account.metadata.account.publicKey, ...streams, signingSeed: artifact(account, "signing-maintainer.nk"), pathBMachines: machines, job: "compaction", runId: "test" }, clock)).toThrow();
  });

  it("refuses empty, duplicate or unsafe inventory before generating account artifacts", () => {
    const operator = createNatsOperator({ operatorName: "test-path-b" }, clock);
    const input = {
      name: "test-path-b",
      operatorJwt: artifact(operator, "operator.jwt"),
      operatorSigningSeed: artifact(operator, "operator-signing.nk"),
      jetstream: { memStorage: 0, diskStorage: 1, streams: 1, consumers: 1 },
      ...streams,
    };
    for (const pathBMachines of [[], ["ryzen2", "ryzen2"], ["ryzen2.*"], ["{{tag(machine)}}"], ["fabric-ops"], ["fabric-ops-canary"]]) {
      expect(() => createNatsAccount({ ...input, pathBMachines }, clock)).toThrow();
    }
  });
});
