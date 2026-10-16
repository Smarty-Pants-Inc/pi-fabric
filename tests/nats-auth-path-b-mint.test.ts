import { describe, expect, it } from "vitest";
import {
  createNatsAccount,
  createNatsOperator,
  mintNatsUser,
  type NatsMintOutput,
} from "../src/mesh/nats-auth/mint.js";
import { decodeAndVerifyNatsJwt } from "../src/mesh/nats-auth/jwt.js";

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

  it("gives the messaging operator no stream lifecycle or inbox acknowledgement authority", () => {
    const { account, common } = fixture();
    const input = {
      ...common,
      signingSeed: artifact(account, "signing-maintainer.nk"),
      role: "maintainer",
      machine: "ryzen1",
      job: "compaction",
      runId: "test",
    };
    expect(mintNatsUser(input, clock).metadata.role).toBe("maintainer");
    const operator = claims(common.accountJwt).nats.signing_keys.find((entry: any) => entry.role === "maintainer");
    for (const verb of ["CREATE", "UPDATE", "DELETE", "PURGE"]) {
      expect(operator.template.pub.allow.some((subject: string) => subject.startsWith(`$JS.API.STREAM.${verb}.`))).toBe(false);
    }
    expect(operator.template.pub.allow).not.toContain("$JS.ACK.FABRIC_IN_ryzen2.>");
    expect(operator.template.pub.allow).toContain("$JS.ACK.FABRIC_ADV.>");
    expect(operator.template.pub.allow).toContain("$JS.API.STREAM.MSG.GET.FABRIC_IN_ryzen2");
    const { machine: omitted, ...withoutMachine } = input;
    expect(omitted).toBeDefined();
    expect(() => mintNatsUser(withoutMachine, clock)).toThrow();
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
    for (const pathBMachines of [[], ["ryzen2", "ryzen2"], ["ryzen2.*"], ["{{tag(machine)}}"]]) {
      expect(() => createNatsAccount({ ...input, pathBMachines }, clock)).toThrow();
    }
  });
});
