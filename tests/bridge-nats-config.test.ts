import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { validBridgeName } from "../src/mesh/bridge.js";
import {
  describeNatsLinkConfig,
  loadNatsLinkConfig,
  natsLinkMeshNatsConfig,
  natsLinkStatePaths,
  NatsLinkConfigError,
  parseNatsLinkConfig,
  preflightNatsLinkConfig,
} from "../src/mesh/bridge-nats-config.js";

// Fictional ids only: pi-fabric is public.
const KATE = "session:00000000-0000-7000-8000-00000000000a";
const PAUL = "session:00000000-0000-7000-8000-00000000000b";
const LEAD = "session:00000000-0000-7000-8000-00000000000c";
const ASTRA = "0000000000000000000000000000000d";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const scratch = (): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "nats-link-config-")));
  roots.push(root);
  return root;
};

const NATS = {
  servers: ["tls://broker.example.test:4222"],
  machine: "ryzen1-bridge",
  credentials: "/srv/secrets/ryzen1-bridge.creds",
  caFile: "/srv/secrets/ca.pem",
  org: "test-org",
  custodyStream: "FABRIC_CUSTODY",
  livenessStream: "FABRIC_LIVENESS",
  pathBMachines: ["ryzen1-bridge", "sandbox1"],
  operatorMachine: "fabric-ops",
};
const FILE = [
  { kind: "root", id: PAUL, principal: "Paul", send: ["followUp"], receive: ["followUp", "cancel"], note: "Paul's org seat" },
  { kind: "actor", rootId: PAUL, name: "review-astra", id: ASTRA, send: ["followUp", "steer"] },
];
const NATS_ALLOW = [
  { kind: "root", id: KATE, principal: "Kate", machine: "sandbox1",
    send: ["followUp", "cancel"], receive: ["followUp", "cancel"], receiveTopics: ["ops.owner", "fleet.work.*"] },
  { kind: "root", id: LEAD, send: ["followUp", "steer", "ask", "cancel"], receive: ["followUp", "steer", "ask"] },
];
const ALLOW = { file: FILE, nats: NATS_ALLOW };
const sample = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  version: 1,
  name: "ryzen1",
  link: "v2",
  file: { mesh: "/srv/fleet/mesh", lockProtocol: 2 },
  stateDir: "/srv/nats-link/v2",
  nats: NATS,
  allow: ALLOW,
  ...overrides,
});
/** A config whose allow section holds one changed copy of an entry from FILE or NATS_ALLOW. */
const withEntry = (side: "file" | "nats", index: number, change: Record<string, unknown>): Record<string, unknown> =>
  sample({ allow: { [side]: [{ ...(side === "file" ? FILE : NATS_ALLOW)[index], ...change }] } });

const refused = (value: unknown, field: string): void => {
  let error: unknown;
  try { parseNatsLinkConfig(value); } catch (caught) { error = caught; }
  expect(error).toBeInstanceOf(NatsLinkConfigError);
  expect((error as NatsLinkConfigError).field).toBe(field);
};

describe("nats-link config", () => {
  it("parses a complete config into L3's normalized allow entries, with least-privilege defaults", () => {
    const config = parseNatsLinkConfig(sample());
    expect(config.callTimeoutMs).toBe(30_000);
    expect(config.logLevel).toBe("info");
    expect(config.nats.events).toBe(false);
    expect(config.nats.inboxLanes).toBe(8);
    // L3's parser: frozen, both lists in one entries list, each entry with its home side and defaults.
    expect(Object.isFrozen(config.allow)).toBe(true);
    expect(config.allow).toEqual({ version: 1, link: "v2", natsMachine: "ryzen1-bridge", entries: [
      { ...FILE[0], side: "file" },
      // An entry that omits receive may only receive followUp.
      { ...FILE[1], side: "file", receive: ["followUp"] },
      ...NATS_ALLOW.map((entry) => ({ ...entry, side: "nats" })),
    ] });
    expect(parseNatsLinkConfig(sample({ allow: { nats: [{ kind: "root", id: KATE }] } })).allow.entries)
      .toEqual([{ kind: "root", id: KATE, side: "nats", send: ["followUp"], receive: ["followUp"] }]);
    expect(config.orgJson).toBeUndefined();
    expect(parseNatsLinkConfig(sample({ orgJson: "/srv/test-org/org.json" })).orgJson).toBe("/srv/test-org/org.json");
    refused(sample({ orgJson: "org.json" }), "orgJson");
    expect(natsLinkStatePaths(config)).toEqual({
      cursor: "/srv/nats-link/v2/cursor.json",
      stallReason: "/srv/nats-link/v2/cursor.json.last-stall-reason",
      ledger: "/srv/nats-link/v2/ledger",
    });
  });

  it("refuses unknown keys at every level, so a misspelt field never falls back to a default", () => {
    refused(sample({ alow: {} }), "config");
    refused(sample({ file: { mesh: "/srv/fleet/mesh", lockprotocol: 2 } }), "file");
    refused(sample({ nats: { ...NATS, credential: "/x" } }), "nats");
    refused(withEntry("file", 0, { recieve: ["followUp"] }), "allow.file[0]");
    refused(withEntry("file", 0, { operations: ["followUp"] }), "allow.file[0]");
    refused(sample({ allow: { files: [] } }), "allow");
    refused(sample({ allow: [] }), "allow");
  });

  it("holds names and paths only: no inline credential or relative path", () => {
    refused(sample({ nats: { ...NATS, credentials: "-----BEGIN NATS USER JWT-----" } }), "nats.credentials");
    refused(sample({ nats: { ...NATS, credentials: "secrets/ryzen1.creds" } }), "nats.credentials");
    refused(sample({ nats: { ...NATS, caFile: "/srv/secrets/../ca.pem" } }), "nats.caFile");
    refused(sample({ stateDir: "/srv/fleet/mesh/link" }), "stateDir");
    refused(sample({ file: { mesh: "relative/mesh" } }), "file.mesh");
  });

  it("checks names, machines and servers as mesh-bridge and mesh.nats do", () => {
    refused(sample({ name: "v2" }), "link");
    refused(sample({ link: "-v2" }), "link");
    refused(sample({ version: 2 }), "version");
    for (const server of ["nats://broker.example.test:4222", "tls://127.0.0.1:4222", "tls://0x7f.1:4222", "tls://broker:0", "tls://a_b:4222"]) {
      refused(sample({ nats: { ...NATS, servers: [server] } }), "nats.servers");
    }
    refused(sample({ nats: { ...NATS, machine: "fabric-ops" } }), "nats.machine");
    refused(sample({ nats: { ...NATS, machine: "epyc1" } }), "nats.pathBMachines");
    refused(sample({ nats: { ...NATS, operatorMachine: "ops" } }), "nats.operatorMachine");
    refused(sample({ nats: { ...NATS, livenessStream: "fabric_custody" } }), "nats.livenessStream");
    refused(sample({ nats: { ...NATS, custodyStream: "FABRIC_MESH_X" } }), "nats.custodyStream");
    const { org: _org, ...noOrg } = NATS;
    expect(parseNatsLinkConfig(sample({ nats: noOrg })).nats.org).toBeUndefined();
    refused(sample({ nats: { ...noOrg, events: true } }), "nats.org");
    // The name rule is mesh-bridge's validBridgeName.
    for (const name of ["ryzen1", "a", "v2.link_1-b", "-a", ".a", "a b", "x".repeat(63), "x".repeat(64), "é", "a\n"]) {
      const ok = (() => { try { parseNatsLinkConfig(sample({ name, link: "zz" })); return true; } catch { return false; } })();
      expect(ok, name).toBe(validBridgeName(name));
    }
  });

  it("produces mesh.nats settings the runtime's own config validator accepts unchanged", () => {
    const config = parseNatsLinkConfig(sample());
    const nats = natsLinkMeshNatsConfig(config);
    const names = ["PI_FABRIC_MESH_TRANSPORT", "PI_FABRIC_MESH_HOST_TRANSPORT", "PI_FABRIC_MESH_NATS"] as const;
    const saved = names.map((name) => [name, process.env[name]] as const);
    for (const name of names) delete process.env[name];
    try {
      const normalized = normalizeFabricConfig({ mesh: { transport: "nats", hostTransport: "nats", nats } }).mesh.nats;
      expect(normalized).toMatchObject({ servers: nats.servers, machine: "ryzen1-bridge", credentials: nats.credentials,
        tls: { caFile: nats.tls!.caFile }, custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
        pathBMachines: ["ryzen1-bridge", "sandbox1"], operatorMachine: "fabric-ops", org: "test-org", events: false, provision: false });
    } finally {
      for (const [name, value] of saved) if (value !== undefined) process.env[name] = value;
    }
  });

  it("keeps each identity listed once and on one side, seats capped and pins sane", () => {
    // An identity lives on one side only, and is listed once.
    refused(sample({ allow: { file: [{ kind: "root", id: KATE }], nats: [{ kind: "root", id: KATE }] } }), "allow.nats[0]");
    refused(sample({ allow: { nats: [NATS_ALLOW[1], { ...NATS_ALLOW[1], send: ["steer"] }] } }), "allow.nats[1]");
    refused(sample({ allow: { file: [FILE[1], { kind: "actor", rootId: PAUL, name: "other", id: ASTRA }] } }), "allow.file[1]");
    refused(sample({ allow: { file: [FILE[1], { kind: "actor", rootId: PAUL, name: "review-astra" }] } }), "allow.file[1]");
    // A principal seat receives followUp and cancel only (BRIDGE_NATS_SEAT_RECEIVE): never steer.
    refused(withEntry("nats", 0, { receive: ["followUp", "steer"] }), "allow.nats[0].receive");
    // stop, setModel and setThinking are received by no destination class; a NATS actor receives nothing.
    refused(withEntry("nats", 1, { receive: ["followUp", "stop"] }), "allow.nats[0].receive");
    refused(sample({ allow: { nats: [{ kind: "actor", rootId: LEAD, name: "helper", receive: ["followUp"] }] } }), "allow.nats[0].receive");
    refused(withEntry("nats", 1, { send: ["followUp", "followUp"] }), "allow.nats[0].send");
    refused(withEntry("nats", 1, { send: ["tell"] }), "allow.nats[0].send");
    refused(withEntry("file", 1, { principal: "Paul" }), "allow.file[0]");
    refused(withEntry("nats", 0, { id: KATE.toUpperCase() }), "allow.nats[0].id");
    refused(withEntry("file", 1, { id: "review-astra" }), "allow.file[0].id");
    refused(withEntry("file", 1, { rootId: "bridge:v2" }), "allow.file[0].rootId");
    refused(withEntry("nats", 1, { id: "bridge:v2" }), "allow.nats[0].id");
    refused(sample({ allow: { nats: [{ kind: "main", id: LEAD }] } }), "allow.nats[0].kind");
    refused(withEntry("nats", 1, { note: "x".repeat(201) }), "allow.nats[0].note");
    // A machine pin is for a NATS identity, on an org machine, never the bridge's own machine (where mirrors live).
    refused(withEntry("nats", 0, { machine: "ryzen1-bridge" }), "allow.nats[0].machine");
    refused(withEntry("nats", 0, { machine: "epyc1" }), "allow.nats[0].machine");
    refused(withEntry("file", 0, { machine: "sandbox1" }), "allow.file[0].machine");
    refused(withEntry("nats", 0, { receiveTopics: ["fleet.*.x"] }), "allow.nats[0].receiveTopics");
    refused(withEntry("nats", 0, { receiveTopics: ["*"] }), "allow.nats[0].receiveTopics");
    refused(withEntry("file", 1, { receiveTopics: ["ops.owner"] }), "allow.file[0].receiveTopics");
    expect(parseNatsLinkConfig(withEntry("nats", 1, { send: [] })).allow.entries[0]!.send).toEqual([]);
    expect(parseNatsLinkConfig(sample({ allow: undefined })).allow.entries).toEqual([]);
  });

  it("describes the config without reading a credential", () => {
    const description = describeNatsLinkConfig(parseNatsLinkConfig(sample()));
    expect(description).toMatchObject({ name: "ryzen1", link: "v2",
      nats: { machine: "ryzen1-bridge", credentials: NATS.credentials } });
    expect(description.allow).toEqual({ file: [PAUL, `actor:${PAUL}/review-astra`], nats: [KATE, LEAD] });
  });

  it("loads only a private regular file and preflights the files start needs", () => {
    const dir = scratch();
    const mesh = path.join(dir, "mesh");
    fs.mkdirSync(mesh);
    fs.writeFileSync(path.join(mesh, "state.json"), "{}");
    const creds = path.join(dir, "ryzen1-bridge.creds");
    const ca = path.join(dir, "ca.pem");
    fs.writeFileSync(creds, "not a real credential", { mode: 0o600 });
    fs.writeFileSync(ca, "not a real ca");
    const value = sample({ file: { mesh }, stateDir: path.join(dir, "state", "v2"), nats: { ...NATS, credentials: creds, caFile: ca } });
    const file = path.join(dir, "link.json");
    fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
    const config = loadNatsLinkConfig(file);
    preflightNatsLinkConfig(config);
    expect(fs.statSync(config.stateDir).mode & 0o777).toBe(0o700);

    expect(() => loadNatsLinkConfig("link.json")).toThrow(NatsLinkConfigError);
    const link = path.join(dir, "alias.json");
    fs.symlinkSync(file, link);
    expect(() => loadNatsLinkConfig(link)).toThrow(/regular file/);
    if (process.geteuid) {
      fs.chmodSync(file, 0o620);
      expect(() => loadNatsLinkConfig(file)).toThrow(/writable/);
      fs.chmodSync(file, 0o600);
      fs.chmodSync(creds, 0o640);
      expect(() => preflightNatsLinkConfig(config)).toThrow(/group or others/);
      fs.chmodSync(creds, 0o600);
      fs.chmodSync(config.stateDir, 0o750);
      expect(() => preflightNatsLinkConfig(config)).toThrow(/private/);
      fs.chmodSync(config.stateDir, 0o700);
    }
    fs.writeFileSync(file, "{ not json", { mode: 0o600 });
    expect(() => loadNatsLinkConfig(file)).toThrow(/JSON/);
    // A wrong mesh path fails; it never becomes a new empty mesh.
    const missing = parseNatsLinkConfig({ ...value, file: { mesh: path.join(dir, "nope") } });
    expect(() => preflightNatsLinkConfig(missing)).toThrow(/mesh directory/);
    expect(fs.existsSync(path.join(dir, "nope"))).toBe(false);
  });
});
