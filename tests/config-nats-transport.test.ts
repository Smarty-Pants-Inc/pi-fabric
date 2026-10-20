import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG, FabricMeshNatsConfigError, normalizeFabricConfig } from "../src/config.js";
import { childMeshTransportEnvironment } from "../src/agents/child-env.js";

const keys = ["PI_FABRIC_MESH_TRANSPORT", "PI_FABRIC_MESH_HOST_TRANSPORT", "PI_FABRIC_MESH_NATS", "PI_FABRIC_MESH_STATE_BACKEND"] as const;
const saved = new Map<string, string | undefined>();
beforeEach(() => { for (const key of keys) { saved.set(key, process.env[key]); delete process.env[key]; } });
afterEach(() => { for (const key of keys) { const value = saved.get(key); if (value === undefined) delete process.env[key]; else process.env[key] = value; } saved.clear(); });
const nats = () => ({ org: "smarty-pants-inc", operatorMachine: "fabric-ops", machine: "ryzen2", credentials: "/not-read/user.creds",
  servers: ["tls://intel1.tail62219a.ts.net:4222"], custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
  pathBMachines: ["ryzen2", "ryzen4"], tls: { caFile: "/not-read/ca.pem" } });
const config = (value: Record<string, unknown>) => ({ mesh: { transport: "nats", hostTransport: "nats", stateBackend: "file", nats: value } });
const refuses = (value: Record<string, unknown>, field?: string) => {
  expect(() => normalizeFabricConfig(config(value))).toThrow(field ? FabricMeshNatsConfigError : /mesh\.nats/);
  if (field) { try { normalizeFabricConfig(config(value)); } catch (error) { expect(error).toMatchObject({ code: "FABRIC_MESH_TRANSPORT_CONFIG", field }); } }
};

describe.sequential("NATS transport policy and TLS configuration", () => {
  it("normalizes the complete policy without reading credentials or CA and copies mutable inputs", () => {
    const input = nats(), before = structuredClone(input), actual = normalizeFabricConfig(config(input)).mesh.nats;
    expect(actual).toEqual({ replicas: 1, inboxLanes: 8, events: false, provision: false, ephemeralHost: false, ephemeralInactiveSeconds: 3_600, ...input });
    expect(actual).not.toBe(input); expect(actual.servers).not.toBe(input.servers);
    expect(actual.pathBMachines).not.toBe(input.pathBMachines); expect(actual.tls).not.toBe(input.tls);
    expect(input).toEqual(before);
  });
  it("defaults events off and requires a valid org only when the event path is enabled", () => {
    expect(normalizeFabricConfig(config(nats())).mesh.nats).toMatchObject({ events: false, provision: false });
    expect(normalizeFabricConfig(config({ ...nats(), events: true, org: "smarty-pants-inc" })).mesh.nats)
      .toMatchObject({ events: true, org: "smarty-pants-inc", provision: false });
    for (const org of [undefined, "sys", "Upper", "has space", "a".repeat(64)]) {
      const value: Record<string, unknown> = { ...nats(), events: true };
      if (org !== undefined) value.org = org;
      else delete value.org;
      refuses(value, "org");
    }
    for (const field of ["events", "provision"] as const) {
      for (const invalid of ["true", 1, null]) refuses({ ...nats(), [field]: invalid }, field);
    }
  });
  it("normalizes a single server string and applies identical DNS/TLS/IP validation to either shape", () => {
    const server = nats().servers[0]!, input = { ...nats(), servers: server }, before = structuredClone(input);
    const mesh = normalizeFabricConfig(config(input)).mesh;
    expect(mesh.nats.servers).toEqual([server]);
    const child = childMeshTransportEnvironment({ ...mesh, nats: { ...mesh.nats, servers: server } }, {});
    expect(JSON.parse(child.PI_FABRIC_MESH_NATS!).servers).toEqual([server]);
    expect(input).toEqual(before);
    for (const unsafe of ["", "tls://-host:4222", "tls://bad..dns:4222", "tls://127.0.0.1:4222",
      "tls://[::1]:4222", "tls://127.0x0.1:4222", "tls://0x7f.1:4222", "tls://2130706433:4222",
      "tls://0x7f000001:4222", "tls://0177.1:4222", "tls://127.1:4222", "nats://localhost:4222",
      "tls://localhost:0", "tls://localhost:65536", "tls://localhost:4222\n"]) {
      refuses({ ...nats(), servers: unsafe }, "servers");
      refuses({ ...nats(), servers: [unsafe] }, "servers");
    }
  });
  it.each(["operatorMachine", "servers", "custodyStream", "livenessStream", "pathBMachines", "tls"])("typed-refuses missing required %s", field => {
    const value: Record<string, unknown> = nats(); delete value[field]; refuses(value, field);
  });
  it.each(["tls://127.0.0.1:4222", "tls://[::1]:4222", "tls://2130706433:4222", "tls://0x7f000001:4222", "nats://localhost:4222", "tls://localhost:0", "tls://localhost:65536", "tls://localhost:4222\n"])("refuses non-DNS/plaintext/invalid URL %s", server => refuses({ ...nats(), servers: [server] }, "servers"));
  it("refuses mixed-hex, decimal-integer, octal and short IPv4 spellings after canonicalization", () => {
    for (const host of ["127.0x0.1", "0x7f.1", "2130706433", "0177.1", "127.1"])
      refuses({ ...nats(), servers: [`tls://${host}:4222`] }, "servers");
  });
  it("still accepts localhost as a DNS name without attempting credential or CA reads", () => {
    expect(normalizeFabricConfig(config({ ...nats(), servers: ["tls://localhost:4222"] })).mesh.nats.servers)
      .toEqual(["tls://localhost:4222"]);
  });
  it("checks every server, including sparse lists", () => {
    refuses({ ...nats(), servers: [nats().servers[0], "tls://127.0.0.1:4222"] }, "servers");
    refuses({ ...nats(), servers: new Array(1) }, "servers");
  });
  it.each(["allowInsecureLoopback", "verifyMachine"])("never exposes the direct-fixture bypass %s in config", field => refuses({ ...nats(), [field]: false }, field));
  it.each([{ rejectUnauthorized: false }, { checkServerIdentity: "override" }, { servername: "other" }, {}, { caFile: "" }])("refuses unsafe or empty TLS options %j", tls => refuses({ ...nats(), tls }, "tls"));
  it("typed-refuses a relative CA file even when an inline CA is present", () => {
    refuses({ ...nats(), tls: { caFile: "certs/ca.pem" } }, "tls");
    refuses({ ...nats(), tls: { ca: "fixture PEM", caFile: "certs/ca.pem" } }, "tls");
    expect(() => normalizeFabricConfig(config({ ...nats(), tls: { caFile: "certs/ca.pem" } })))
      .toThrow("caFile must be an absolute path");
  });
  it("accepts explicit inline CA, file CA, or both without credential/CA I/O", () => {
    for (const tls of [{ ca: "fixture PEM" }, { caFile: "/not-read/ca.pem" }, { ca: "fixture PEM", caFile: "/not-read/ca.pem" }])
      expect(normalizeFabricConfig(config({ ...nats(), tls })).mesh.nats.tls).toEqual(tls);
  });
  it.each([
    { custodyStream: "" }, { custodyStream: "FABRIC_MESH_private" }, { livenessStream: "fabric_custody" },
    { livenessStream: "WITH SPACE" }, { org: "Smarty-Pants-Inc" }, { org: "two.parts" }, { org: "x*" },
    { pathBMachines: [] }, { pathBMachines: ["ryzen2", "ryzen2"] },
    { pathBMachines: ["fabric-ops"] }, { pathBMachines: ["ryzen2\n"] }, { pathBMachines: new Array(1) },
  ])("refuses malformed or reserved policy %j", change => refuses({ ...nats(), ...change }));
  it("preserves exact ordinary FILE defaults and ignores NATS-only policy", () => {
    const { machine: _machine, ...ignored } = nats();
    expect(normalizeFabricConfig({ mesh: { transport: "file", nats: { ...ignored, servers: ["nats://127.0.0.1:4222"], tls: { rejectUnauthorized: false } } } }).mesh.nats).toEqual(DEFAULT_FABRIC_CONFIG.mesh.nats);
  });
  it("retains only approved FILE rollback machine and hold authority", () => {
    const rollbackHoldUntil = "2030-01-02T03:04:05Z";
    expect(normalizeFabricConfig({ mesh: { transport: "file", nats: { ...nats(), rollbackHoldUntil } } }).mesh.nats)
      .toEqual({ replicas: 1, inboxLanes: 8, events: false, provision: false, ephemeralHost: false, ephemeralInactiveSeconds: 3_600, machine: "ryzen2", rollbackHoldUntil });
  });
  it("propagates the opt-in event mode and org to mesh children", () => {
    const parent = normalizeFabricConfig(config({ ...nats(), events: true })).mesh;
    const child = childMeshTransportEnvironment(parent, {});
    expect(JSON.parse(child.PI_FABRIC_MESH_NATS!)).toMatchObject({ events: true, org: "smarty-pants-inc" });
  });
  it("inherits a whitelisted independent policy snapshot across different child workspace settings", () => {
    const parent = normalizeFabricConfig(config(nats())).mesh, before = structuredClone(parent);
    const environment = { OTHER: "retained", PI_FABRIC_MESH_STATE_BACKEND: "file" };
    const child = childMeshTransportEnvironment(parent, environment);
    const snapshot = JSON.parse(child.PI_FABRIC_MESH_NATS!);
    expect(snapshot).toEqual({ ...nats(), inboxLanes: 8 }); expect(snapshot).not.toHaveProperty("replicas");
    expect(parent).toEqual(before); expect(environment).toEqual({ OTHER: "retained", PI_FABRIC_MESH_STATE_BACKEND: "file" });
    for (const key of keys) if (child[key] !== undefined) process.env[key] = child[key];
    const resolved = normalizeFabricConfig(config({ ...nats(), custodyStream: "WRONG", tls: { caFile: "/wrong" } })).mesh;
    expect(resolved.nats).toEqual(parent.nats); expect(resolved.stateBackend).toBe("file");
  });
});
