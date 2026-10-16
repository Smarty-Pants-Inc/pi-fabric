import { describe, expect, it } from "vitest";
import {
  createNatsAccount,
  createNatsOperator,
  mintNatsUser,
  mintNatsBreakGlassUser,
  type NatsMintOutput,
} from "../src/mesh/nats-auth/mint.js";
import { decodeAndVerifyNatsJwt } from "../src/mesh/nats-auth/jwt.js";
import { roleTemplate, type NatsRole } from "../src/mesh/nats-auth/permissions.js";

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
  it("binds the machine tag and standalone runtime template without changing legacy templates", () => {
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
    expect(agent.template.pub.allow).not.toContain("FABRIC_MESH_{{tag(mesh)}}.state");
    expect(agent.description).toMatch(/^path-b\/[a-f0-9]{64}$/);
    expect(() => mintNatsUser({ ...input, host: "ryzen5" }, clock)).toThrow();
    expect(() => mintNatsUser({ ...input, pathBMachines: ["ryzen2", "ryzen5"] }, clock)).toThrow();
    const { pathBMachines: omitted, ...withoutPolicy } = input;
    expect(omitted).toBeDefined();
    expect(() => mintNatsUser(withoutPolicy, clock)).toThrow();
  });

  it("keeps principal and break-glass issuance available in an opted-in account", () => {
    const { account, common } = fixture();
    const { pathBMachines: inventory, ...legacyCommon } = common;
    expect(inventory).toEqual(machines);
    const issued = [
      mintNatsUser({ ...legacyCommon, role: "principal", signingSeed: artifact(account, "signing-principal.nk"), person: "test", device: "test" }, clock),
      mintNatsBreakGlassUser({ ...legacyCommon, signingSeed: artifact(account, "signing-break-glass.nk"), person: "test", runId: "test", approvalRef: "issue:13871" }, clock),
    ];
    const scoped = claims(common.accountJwt).nats.signing_keys;
    for (const [index, role] of ["principal", "break-glass"].entries()) {
      const output = issued[index]!;
      expect(output.metadata.role).toBe(role);
      const jwt = artifact(output, "user.creds").match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([^\s]+)\s*-{3,}END NATS USER JWT-{3,}/)?.[1];
      expect(jwt).toBeDefined();
      expect(claims(jwt!).nats.tags).toContain(`role:${role}`);
      const key = scoped.find((entry: any) => entry.role === role);
      expect(key.description).toBe("");
      expect(key.template).toMatchObject(roleTemplate(role as NatsRole, streams));
    }
    for (const role of ["agent", "maintainer"]) {
      expect(scoped.find((entry: any) => entry.role === role).description).toMatch(/^path-b\/[a-f0-9]{64}$/);
    }
  });

  it("gives the messaging operator no stream lifecycle or inbox acknowledgement authority", () => {
    const { account, common } = fixture();
    const input = {
      ...common,
      signingSeed: artifact(account, "signing-maintainer.nk"),
      role: "maintainer",
      machine: "fabric-ops",
      job: "compaction",
      runId: "test",
    };
    const issued = mintNatsUser(input, clock);
    expect(issued.metadata.role).toBe("maintainer");
    const jwt = artifact(issued, "user.creds").match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([^\s]+)\s*-{3,}END NATS USER JWT-{3,}/)?.[1];
    expect(claims(jwt!).nats.tags).toContain("machine:fabric-ops");
    expect(() => mintNatsUser({ ...input, machine: "ryzen1" }, clock)).toThrow();
    const operator = claims(common.accountJwt).nats.signing_keys.find((entry: any) => entry.role === "maintainer");
    for (const verb of ["CREATE", "UPDATE", "DELETE", "PURGE"]) {
      expect(operator.template.pub.allow.some((subject: string) => subject.startsWith(`$JS.API.STREAM.${verb}.`))).toBe(false);
    }
    expect(operator.template.pub.allow).not.toContain("$JS.ACK.FABRIC_IN_ryzen2.>");
    expect(operator.template.pub.allow).toContain("$JS.ACK.FABRIC_ADV.transport_dlq_operator.>");
    expect(operator.template.pub.allow.some((subject: string) => /^\$JS\.API\.CONSUMER\.(CREATE|DELETE)\./.test(subject))).toBe(false);
    expect(operator.template.pub.allow).toContain("$JS.API.STREAM.MSG.GET.FABRIC_IN_ryzen2");
    expect(operator.template.pub.allow).toContain("fabric.in.ryzen2.*.ack.*.{{tag(machine)}}");
    expect(operator.template.pub.allow).toContain("fabric.in.ryzen4.*.ack.*.{{tag(machine)}}");
    expect(operator.template.pub.allow).not.toContain("fabric.in.*.*.ack.*.{{tag(machine)}}");
    const { machine: omitted, ...withoutMachine } = input;
    expect(omitted).toBeDefined();
    expect(() => mintNatsUser(withoutMachine, clock)).toThrow();
  });

  it("T-OPS-MINT: reserves the whole operator namespace and refuses operators outside it", () => {
    const { account, common } = fixture();
    const input = { ...common, signingSeed: artifact(account, "signing-agent.nk"), role: "agent", lane: "test", session: "test", release: "test", meshes: ["0".repeat(32)] };
    for (const host of ["fabric-ops", "fabric-ops-canary", "fabric-ops-2"]) {
      expect(() => mintNatsUser({ ...input, host }, clock)).toThrow("invalid host");
      expect(() => mintNatsUser({ ...input, host, pathBMachines: [host] }, clock)).toThrow("invalid pathBMachines");
    }
    const operator = { ...common, signingSeed: artifact(account, "signing-maintainer.nk"), role: "maintainer", job: "compaction", runId: "test" };
    for (const machine of ["ryzen2", "ryzen1", "fabric-ops-", "fabric-ops-two-parts"]) {
      expect(() => mintNatsUser({ ...operator, machine }, clock)).toThrow("invalid machine");
    }
    for (const machine of ["fabric-ops", "fabric-ops-canary"]) {
      expect(mintNatsUser({ ...operator, machine }, clock).metadata.role).toBe("maintainer");
    }
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
