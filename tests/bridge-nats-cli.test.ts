import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runFabricCli } from "../src/cli/index.js";
import { filePresenceView, main, type NatsLinkCliIo, type NatsLinkHandle, type NatsLinkStarter } from "../src/mesh-bridge-nats-cli.js";
import { loadNatsLinkConfig, natsLinkAllowListConfig } from "../src/mesh/bridge-nats-config.js";
import { openNatsLinkCounters } from "../src/mesh/bridge-nats-counters.js";
import { createBridgeNatsIdentityGate, readBridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { NatsRootsUnavailableError, type NatsRootInfo, type NatsRootsSource } from "../src/mesh/bridge-nats-roots.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

/** nats-link start and dry-run refuse on Windows (Linux/macOS only); their full behavior is tested elsewhere. */
const WINDOWS = process.platform === "win32";

// Fictional ids only: pi-fabric is public.
const KATE = "session:00000000-0000-7000-8000-00000000000a";
const PAUL = "session:00000000-0000-7000-8000-00000000000b";
const STRAY = "session:00000000-0000-7000-8000-00000000000e";
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** A fictional org.json whose seats pin Paul's and Kate's fictional sessions (never a real org.json). */
const orgJsonFixture = (dir: string, seats: { paul: string; kate: string } = { paul: PAUL, kate: KATE }): string => {
  const file = path.join(dir, "org.json");
  fs.writeFileSync(file, JSON.stringify({
    productOwner: { name: "Paul" },
    principals: [
      { name: "Paul", orgInstance: { herdrAgent: "org", herdrWorkspaceLabel: "Org", fabricSession: seats.paul.slice(8) } },
      { name: "Kate", orgInstance: { herdrAgent: "org-kate", herdrWorkspaceLabel: "Kate", fabricSession: seats.kate.slice(8) } },
    ],
  }), { mode: 0o600 });
  return file;
};

/** PR 2 additions to the base config: a spoke root, a republishing moved lead, events and the spoke link. */
const SPOKE_ROOT = "session:00000000-0000-7000-8000-00000000000f";
const LEAD = "session:00000000-0000-7000-8000-00000000000d";
const PR2 = {
  relay: { spokes: ["ryzen4"] },
  nats: { events: true, org: "test-org" },
  allow: {
    file: [{ kind: "root", id: SPOKE_ROOT, viaSpoke: "ryzen4", send: ["followUp", "cancel"] }],
    nats: [{ kind: "root", id: LEAD, machine: "sandbox1", republishTopics: ["fleet.work.*"] }],
  },
};

/** A private scratch tree: mesh, state dir, a 0600 fake credential, a fixture org.json and a config naming them. */
const setup = async (extra?: typeof PR2) => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "nats-link-cli-")));
  roots.push(dir);
  const mesh = path.join(dir, "mesh");
  const stateDir = path.join(dir, "state");
  fs.mkdirSync(stateDir, { mode: 0o700 });
  const credentials = path.join(dir, "bridge.creds");
  const caFile = path.join(dir, "ca.pem");
  fs.writeFileSync(credentials, "fixture, not a credential", { mode: 0o600 });
  fs.writeFileSync(caFile, "fixture, not a CA");
  const store = new MeshStore(mesh, 64 * 1024, 1_000);
  // An existing mesh: preflight and the readers refuse a directory that would become a new empty one.
  await store.publish({ topic: "test.setup", kind: "setup", from: { id: "test", name: "test", kind: "main" } });
  const orgJson = orgJsonFixture(dir);
  // A test never reads a real org.json.
  expect(orgJson.startsWith(`${os.homedir()}${path.sep}`)).toBe(false);
  const config = path.join(dir, "link.json");
  fs.writeFileSync(config, JSON.stringify({
    version: 1, name: "ryzen1", link: "v2", file: { mesh }, stateDir, orgJson,
    nats: { servers: ["tls://broker.example.test:4222"], machine: "ryzen1-bridge", credentials, caFile,
      custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: ["ryzen1-bridge", "sandbox1"],
      operatorMachine: "fabric-ops", ...extra?.nats },
    allow: {
      file: [{ kind: "root", id: PAUL, principal: "Paul", receive: ["followUp", "cancel"] }, ...extra?.allow.file ?? []],
      nats: [{ kind: "root", id: KATE, principal: "Kate", machine: "sandbox1", receive: ["followUp", "cancel"] },
        ...extra?.allow.nats ?? []],
    },
    ...(extra ? { relay: extra.relay } : {}),
  }), { mode: 0o600 });
  return { dir, mesh, stateDir, store, config, credentials, orgJson };
};

/** A root and its host record, as the participant directory, or a bridge mirror (remoteHost), writes them. */
const addRoot = async (store: MeshStore, id: string,
  options: { remoteHost?: string; bridgeVia?: string; incarnation?: string; expiresIn?: number } = {}) => {
  const now = Date.now();
  const sessionId = id.slice("session:".length);
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId };
  const remote = { ...(options.remoteHost ? { remoteHost: options.remoteHost } : {}), ...(options.bridgeVia ? { bridgeVia: options.bridgeVia } : {}) };
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + (options.expiresIn ?? 60_000), ...remote,
  } });
  await store.put({ key: `topology/participants/${hash(id)}`, identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: "main", status: "idle", runner: "pi",
    transport: "host", capabilities: ["steer", "followUp", "fabric"], sessionId, startedAt: now, updatedAt: now,
    pendingMessages: false, controlProtocol: "v1", ownerIncarnation: options.incarnation ?? "inc-1", ...remote,
  } });
};

const cursor = (stateDir: string, local = "ryzen1", remote = "v2"): void => fs.writeFileSync(path.join(stateDir, "cursor.json"),
  JSON.stringify({ format: 1, local, remote, toRemote: { after: 3, mark: 1 }, toLocal: { after: 5, mark: 2 } }), { mode: 0o600 });

const run = async (argv: string[], io: NatsLinkCliIo = {}) => {
  let out = "";
  let err = "";
  const code = await main(argv, { out: (text) => { out += text; }, err: (text) => { err += text; }, ...io });
  return { code, out, err, json: (): Record<string, unknown> => JSON.parse(out) as Record<string, unknown> };
};

const command = (store: MeshStore, commandId: string, from: MeshIdentity, to: string) => store.publish({
  topic: "fabric.control.command", kind: "followUp", from, to,
  data: { version: 1, commandId, targetId: to, operation: "followUp", replyTo: from.id, ownerIncarnation: "inc-k",
    requestedAt: Date.now(), message: "hello" },
});
const ack = (store: MeshStore, commandId: string, from: MeshIdentity, to: string, accepted = true) => store.publish({
  topic: "fabric.control.ack", kind: accepted ? "accepted" : "rejected", from, to,
  data: { version: 1, commandId, targetId: from.id, ownerIncarnation: "inc-k", accepted, bridge: { from: "v2", id: `ack:${commandId}` } },
});

describe("nats-link CLI", () => {
  it("refuses bad usage and bad config with exit 2, writing nothing", async () => {
    const { config, dir } = await setup();
    expect((await run([])).code).toBe(2);
    expect((await run(["frobnicate"])).code).toBe(2);
    expect((await run(["status"])).err).toContain("--config is required");
    expect((await run(["status", "--config", config, "--config", config])).code).toBe(2);
    expect((await run(["receipt", "--config", config])).err).toContain("exactly one of --command or --session");
    expect((await run(["receipt", "--config", config, "--session", "nope"])).code).toBe(2);
    expect((await run(["receipt", "--config", config, "--command", "c", "--nats"])).code).toBe(2);
    expect((await run(["status", "--config", "relative.json"])).err).toContain("FABRIC_NATS_LINK_CONFIG");
    fs.writeFileSync(path.join(dir, "bad.json"), JSON.stringify({ version: 1 }), { mode: 0o600 });
    expect((await run(["status", "--config", path.join(dir, "bad.json")])).code).toBe(2);
    expect((await run(["help"])).code).toBe(0);
  });

  it("status reports cursor, stall, mirrors and allow-list state; an unlisted mirror is a problem", async () => {
    const { config, store, stateDir } = await setup();
    const fresh = await run(["status", "--config", config]);
    expect(fresh.code).toBe(1);
    expect(fresh.json().problems).toEqual(["no cursor: the link has never started with this stateDir"]);

    cursor(stateDir);
    await addRoot(store, PAUL);
    await addRoot(store, KATE, { remoteHost: "v2", incarnation: "inc-k" });
    const healthy = await run(["status", "--config", config]);
    expect(healthy.code).toBe(0);
    expect(healthy.json()).toMatchObject({
      ok: true, cursor: { present: true, toRemote: 3, toLocal: 5 }, stall: { reason: null },
      mirrors: [{ id: KATE, remoteHost: "v2", ownerIncarnation: "inc-k", live: true }],
      allow: { file: [{ identity: PAUL, id: PAUL, present: true, live: true, send: ["followUp"], receive: ["followUp", "cancel"] }],
        nats: [{ identity: KATE, id: KATE, present: true, live: true }] },
      problems: [],
    });
    expect(JSON.stringify(healthy.json())).not.toContain("fixture, not a credential");

    await addRoot(store, STRAY, { remoteHost: "v2" });
    fs.writeFileSync(path.join(stateDir, "cursor.json.last-stall-reason"), "mesh-bridge: stall v1 host=ryzen1 peer=v2 direction=both\n");
    const sick = await run(["status", "--config", config]);
    expect(sick.code).toBe(1);
    expect(sick.json().problems).toEqual([
      "stalled: mesh-bridge: stall v1 host=ryzen1 peer=v2 direction=both",
      `mirror not on allow.nats: ${STRAY}`,
    ]);
    cursor(stateDir, "ryzen1", "other");
    expect((await run(["status", "--config", config])).json().problems).toContain("cursor names another link (local/remote differ from name/link)");
  });

  it("status shows the running link's republish and relay counters", async () => {
    const { config, stateDir } = await setup();
    cursor(stateDir);
    const before = (await run(["status", "--config", config])).json();
    expect(before).toMatchObject({ counters: { present: false },
      republish: { enabled: false, senders: [], accepted: 0, duplicate: 0, refused: 0 },
      relay: { spokeRoots: [], toNats: 0, toFile: 0, refused: 0 } });
    const counters = openNatsLinkCounters(stateDir, "v2");
    counters.add("republish.accepted", 2);
    counters.add("republish.refused");
    counters.add("relay.toNats", 3);
    counters.add("control.toFile");
    counters.close();
    const after = await run(["status", "--config", config]);
    expect(after.code).toBe(0);
    expect(after.json()).toMatchObject({
      counters: { present: true, pid: process.pid, values: { "republish.accepted": 2, "relay.toNats": 3, "control.toFile": 1 } },
      republish: { accepted: 2, duplicate: 0, refused: 1 },
      relay: { toNats: 3, toFile: 0, refused: 0 },
      problems: [],
    });
    // A snapshot left by another link in this state directory is a problem, never this link's numbers.
    openNatsLinkCounters(stateDir, "v9").close();
    expect((await run(["status", "--config", config])).json().problems).toContain("counters name another link");
  });

  it("status and receipt find a spoke root as its spoke link's direct mirror, and list republishing senders", async () => {
    const { config, store, stateDir } = await setup(PR2);
    cursor(stateDir);
    // The spoke root as the hub's ryzen4 link mirrors it.
    await addRoot(store, SPOKE_ROOT, { remoteHost: "ryzen4", incarnation: "inc-s" });
    const status = (await run(["status", "--config", config])).json();
    expect(status).toMatchObject({
      allow: { file: [{ identity: PAUL }, { identity: SPOKE_ROOT, id: SPOKE_ROOT, present: true, live: true, viaSpoke: "ryzen4" }],
        nats: [{ identity: KATE }, { identity: LEAD, republishTopics: ["fleet.work.*"] }] },
      republish: { enabled: true, senders: [{ identity: LEAD, topics: ["fleet.work.*"] }] },
      relay: { spokes: ["ryzen4"], spokeRoots: [{ identity: SPOKE_ROOT, viaSpoke: "ryzen4" }] },
      config: { relay: { spokes: ["ryzen4"] } },
    });
    const receipt = await run(["receipt", "--config", config, "--session", SPOKE_ROOT]);
    expect([receipt.code, receipt.json()]).toMatchObject([0, { verdict: "routable", side: "file", viaSpoke: "ryzen4",
      file: { remoteHost: "ryzen4" } }]);
    // Natively on the hub (no spoke mirror), the same id is not the spoke root the allow-list names.
    const native = await setup(PR2);
    await addRoot(native.store, SPOKE_ROOT);
    const wrong = await run(["receipt", "--config", native.config, "--session", SPOKE_ROOT]);
    expect([wrong.code, wrong.json().problems]).toEqual([1, ["file-mesh record is not spoke ryzen4's direct mirror (remoteHost none)"]]);
    expect((await run(["status", "--config", native.config])).json())
      .toMatchObject({ allow: { file: [{ identity: PAUL }, { identity: SPOKE_ROOT, present: false }] } });
  });

  // Stated skip: dry-run refuses on Windows by design (nats-link is Linux/macOS only); that refusal is tested below.
  it.skipIf(WINDOWS)("dry-run takes a spoke root only as its spoke link's direct mirror, never a re-export (L3 audit P0)", async () => {
    const kate: NatsRootInfo = { id: KATE, kind: "root", name: "main", sessionId: KATE.slice(8), rootId: KATE, machine: "sandbox1",
      ownerHostId: KATE, ownerIdentityId: KATE, ownerIncarnation: "inc-k", status: "idle", capabilities: ["followUp"], live: true };
    const spokeRow = async (options: { remoteHost: string; bridgeVia?: string }) => {
      const { config, store } = await setup(PR2);
      await addRoot(store, PAUL);
      await addRoot(store, SPOKE_ROOT, options);
      const status = (await run(["status", "--config", config])).json();
      const dry = await run(["dry-run", "--config", config], { readRoots: async () => [kate] });
      return { status, row: (dry.json().rows as Array<{ key: string; status: string; reason?: string }>).find((row) => row.key === SPOKE_ROOT)! };
    };
    expect((await spokeRow({ remoteHost: "ryzen4" })).row.status).toBe("live");
    // The view keeps a record's stored origin (remoteHost) and holder (bridgeVia) apart, through L3's builder:
    // the same id held BY ryzen4 as a re-export of another link's mirror (origin epyc1) is not ryzen4's spoke
    // root, and L3's gate refuses it as a target.
    const row = { id: SPOKE_ROOT, kind: "root", name: "main", rootId: SPOKE_ROOT, ownerHostId: SPOKE_ROOT, ownerIdentityId: SPOKE_ROOT,
      ownerIncarnation: "inc-s", remoteHost: "epyc1", bridgeVia: "ryzen4", stale: false } as unknown as Parameters<typeof filePresenceView>[0][number];
    const [record] = filePresenceView([row], Date.now()).records(SPOKE_ROOT);
    expect(record).toMatchObject({ remoteHost: "epyc1", bridgeVia: "ryzen4", mirroredBy: "ryzen4", kind: "main", live: true });
    const { config } = await setup(PR2);
    const parsed = natsLinkAllowListConfig(loadNatsLinkConfig(config));
    const gate = createBridgeNatsIdentityGate(parsed, { pins: readBridgeNatsSeatPins(JSON.parse(fs.readFileSync(path.join(path.dirname(config), "org.json"), "utf8"))) });
    const verified = gate.verifyTarget("file", SPOKE_ROOT, filePresenceView([row], Date.now()), { side: "nats", records: () => [] });
    expect(verified.ok).toBe(false);
  });

  it.skipIf(process.platform === "win32")("start with spoke links needs a config file the hub relay accepts as its peer's (0600)", async () => {
    const { config } = await setup(PR2);
    let started = 0;
    const starter: NatsLinkStarter = async () => { started += 1; return { closed: Promise.resolve(), stop: async () => undefined }; };
    fs.chmodSync(config, 0o640);
    const loose = await run(["start", "--config", config], { starter, signal: new AbortController().signal });
    expect([loose.code, started]).toEqual([2, 0]);
    expect(loose.err).toContain("must have no group or other permission bits (0600 or 0400) when relay.spokes is set");
    fs.chmodSync(config, 0o600);
    expect((await run(["start", "--config", config], { starter, signal: new AbortController().signal })).code).toBe(1);
    expect(started).toBe(1);
  });

  it("receipt --command proves one command and its ACK, and never exits 0 on missing or doubled evidence", async () => {
    const { config, store } = await setup();
    const paul: MeshIdentity = { id: PAUL, name: "org", kind: "main" };
    const kate: MeshIdentity = { id: KATE, name: "org-kate", kind: "main" };
    await command(store, "c-ok", paul, KATE);
    await ack(store, "c-ok", kate, PAUL);
    await ack(store, "c-ok", kate, PAUL); // an identical replayed ACK is still one outcome
    await command(store, "c-pending", paul, KATE);
    await command(store, "c-twice", paul, KATE);
    await command(store, "c-twice", paul, KATE);
    await command(store, "c-split", paul, KATE);
    await ack(store, "c-split", kate, PAUL, true);
    await ack(store, "c-split", kate, PAUL, false);

    const ok = await run(["receipt", "--config", config, "--command", "c-ok"]);
    expect(ok.code).toBe(0);
    expect(ok.json()).toMatchObject({ ok: true, verdict: "acknowledged", accepted: true,
      commands: [{ from: { id: PAUL, kind: "main" }, to: KATE, ownerIncarnation: "inc-k", operation: "followUp" }],
      acks: [{ from: { id: KATE }, accepted: true, bridge: { from: "v2" } }, { accepted: true }] });
    for (const [id, verdict] of [["c-pending", "pending"], ["c-twice", "duplicate"], ["c-split", "conflict"], ["c-none", "not-found"]]) {
      const result = await run(["receipt", "--config", config, "--command", id!]);
      expect([result.code, result.json().verdict], id).toEqual([1, verdict]);
    }
    const head = (await run(["receipt", "--config", config, "--command", "c-ok"])).json().scanned as { through: number };
    expect((await run(["receipt", "--config", config, "--command", "c-ok", "--after", String(head.through)])).json().verdict).toBe("not-found");
  });

  it("receipt --session checks both sides' records and the owner incarnation", async () => {
    const { config, store } = await setup();
    await addRoot(store, KATE, { remoteHost: "v2", incarnation: "inc-k" });
    await addRoot(store, PAUL, { incarnation: "inc-p" });
    const kate = (change: Partial<NatsRootInfo> = {}): NatsRootInfo => ({ id: KATE, kind: "root", name: "main", sessionId: KATE.slice(8),
      rootId: KATE, machine: "sandbox1", ownerHostId: KATE, ownerIdentityId: KATE, ownerIncarnation: "inc-k", status: "idle",
      capabilities: ["followUp"], live: true, ...change });
    let read: NatsRootsSource | undefined;
    const roots = (rows: NatsRootInfo[]) => async (source: NatsRootsSource) => { read = source; return rows; };

    const fileOnly = await run(["receipt", "--config", config, "--session", KATE.slice(8)]);
    expect(fileOnly.code).toBe(0);
    expect(fileOnly.json()).toMatchObject({ verdict: "routable", side: "nats", file: { remoteHost: "v2", live: true }, nats: null, handoffs: [] });

    const both = await run(["receipt", "--config", config, "--session", KATE, "--nats"], { readRoots: roots([kate()]) });
    expect(both.code).toBe(0);
    expect(read).toMatchObject({ machine: "ryzen1-bridge", pathBMachines: ["ryzen1-bridge", "sandbox1"] });
    const moved = await run(["receipt", "--config", config, "--session", KATE, "--nats"], { readRoots: roots([kate({ ownerIncarnation: "inc-new" })]) });
    expect([moved.code, moved.json().problems]).toEqual([1, ["owner incarnation differs between the file mesh and NATS"]]);
    const looped = await run(["receipt", "--config", config, "--session", KATE, "--nats"], { readRoots: roots([kate({ machine: "ryzen1-bridge" })]) });
    expect(looped.json().problems).toEqual(["NATS record is under the bridge machine: a mirror, not the native owner"]);
    const twice = await run(["receipt", "--config", config, "--session", KATE, "--nats"], { readRoots: roots([kate(), kate({ machine: "ryzen1-bridge" })]) });
    expect(twice.json().problems).toContain("several NATS presence records answer to this id (ambiguous, unroutable)");
    const down = await run(["receipt", "--config", config, "--session", KATE, "--nats"], {
      readRoots: async () => { throw new NatsRootsUnavailableError("no complete NATS presence snapshot within 10000 ms"); } });
    expect([down.code, down.json().problems]).toEqual([1, ["NATS presence unreadable"]]);

    // A file identity is native on the file mesh and mirrored into KV under the bridge machine.
    const paul = await run(["receipt", "--config", config, "--session", PAUL, "--nats"], {
      readRoots: roots([{ ...kate(), id: PAUL, rootId: PAUL, ownerHostId: PAUL, ownerIdentityId: PAUL, machine: "ryzen1-bridge", ownerIncarnation: "inc-p" }]) });
    expect([paul.code, paul.json().side, paul.json().problems]).toEqual([0, "file", []]);
    const unknown = await run(["receipt", "--config", config, "--session", STRAY]);
    expect([unknown.code, unknown.json().problems]).toEqual([1, ["not on this link's allow-list", "no file-mesh record"]]);
  });

  it("start and dry-run refuse on Windows by name (exit 2, BRIDGE_NATS_UNSUPPORTED_PLATFORM); read-only modes still run", async () => {
    const { config } = await setup();
    const original = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...original, value: "win32" });
    try {
      let started = false;
      const starter: NatsLinkStarter = async () => { started = true; return { closed: new Promise(() => undefined), stop: async () => undefined }; };
      for (const mode of ["start", "dry-run"]) {
        const refused = await run([mode, "--config", config], { starter, signal: new AbortController().signal, readRoots: async () => [] });
        expect(refused.code).toBe(2);
        expect(refused.err).toContain("BRIDGE_NATS_UNSUPPORTED_PLATFORM: nats-link runs only on Linux and macOS");
      }
      expect(started).toBe(false);
      expect((await run(["help"])).code).toBe(0);
    } finally { Object.defineProperty(process, "platform", original); }
  });

  // Stated skip: start and dry-run refuse on Windows by design (the refusal is tested above on every platform).
  it.skipIf(WINDOWS)("start runs the link until stopped (exit 0) and exits 1 when the link ends by itself", async () => {
    const { config, credentials, stateDir } = await setup();
    let stops = 0;
    let seen: { link?: string; signal?: AbortSignal } = {};
    const handle = (closed: Promise<void>): NatsLinkHandle => ({ closed, stop: async () => { stops += 1; } });
    const controller = new AbortController();
    const lock = path.join(stateDir, "link.lock");
    let second: Awaited<ReturnType<typeof run>> | undefined;
    const starter: NatsLinkStarter = async (linkConfig, options) => {
      seen = { link: linkConfig.link, signal: options.signal };
      // While it runs, the link holds its state directory: a second start is refused by name.
      expect(JSON.parse(fs.readFileSync(lock, "utf8"))).toMatchObject({ link: "v2", purpose: "start", pid: process.pid });
      setTimeout(() => controller.abort(), 10);
      return handle(new Promise<void>(() => undefined));
    };
    const running = run(["start", "--config", config], { starter, signal: controller.signal });
    second = await run(["start", "--config", config], { starter: async () => handle(new Promise(() => undefined)),
      signal: new AbortController().signal });
    expect((await running).code).toBe(0);
    expect(seen.link).toBe("v2");
    expect(stops).toBe(1);
    expect(fs.existsSync(lock)).toBe(false);
    expect(second.code).toBe(2);
    expect(second.err).toContain("FABRIC_NATS_LINK_BUSY: link v2 is held by start pid");

    const failed = await run(["start", "--config", config], {
      starter: async () => handle(Promise.reject(new Error("NATS connection lost"))), signal: new AbortController().signal });
    expect(failed.code).toBe(1);
    expect(failed.err).toContain("nats-link: stopped: NATS connection lost");
    const thrown = await run(["start", "--config", config], {
      starter: async () => { throw new Error("relay socket refused"); }, signal: new AbortController().signal });
    expect([thrown.code, thrown.err]).toEqual([1, "nats-link: relay socket refused\n"]);

    if (process.geteuid) {
      fs.chmodSync(credentials, 0o644);
      const unsafe = await run(["start", "--config", config], { starter, signal: new AbortController().signal });
      expect(unsafe.code).toBe(2);
      expect(unsafe.err).toContain("nats.credentials must not be readable by group or others");
    }
  });

  it("withdraw prints its report and exits 1 unless both sides came out clean", async () => {
    const { config, store } = await setup();
    await addRoot(store, KATE, { remoteHost: "v2" });
    const result = await run(["withdraw", "--config", config], {
      withdraw: { open: async () => { throw new Error("broker unreachable"); } } });
    expect(result.code).toBe(1);
    expect(result.json()).toMatchObject({ ok: false, link: "v2", stopped: null, nats: { error: "broker unreachable" },
      file: { removed: [KATE], remaining: [] }, problems: ["NATS withdraw failed"] });
  });

  it.skipIf(WINDOWS)("start loads the real link module and fails closed on unusable credentials", async () => {
    const { config } = await setup();
    // The fixture's user.creds holds no JWT: the link (bridge-nats-link -> L1's port) refuses before any connection.
    const refused = await run(["start", "--config", config], { signal: new AbortController().signal });
    expect(refused.code).toBe(1);
    expect(refused.err).toMatch(/NATS credentials/);
  });

  it.skipIf(WINDOWS)("start refuses, before anything opens, an allow-list that contradicts org.json's principal seats", async () => {
    const { config, dir } = await setup();
    let started = false;
    const starter: NatsLinkStarter = async () => { started = true; return { closed: new Promise(() => undefined), stop: async () => undefined }; };
    // Kate's seat in org.json is another session: the allow entry claiming her seat is refused.
    orgJsonFixture(dir, { paul: PAUL, kate: STRAY });
    const wrongSeat = await run(["start", "--config", config], { starter, signal: new AbortController().signal });
    expect([wrongSeat.code, started]).toEqual([2, false]);
    expect(wrongSeat.err).toContain("BRIDGE_NATS_ALLOW_LIST_INVALID: allow.nats[0].id must equal that principal's org.json orgInstance.fabricSession");
    // A missing org.json fails closed too.
    fs.rmSync(path.join(dir, "org.json"));
    const missing = await run(["start", "--config", config], { starter, signal: new AbortController().signal });
    expect([missing.code, started]).toEqual([2, false]);
    expect(missing.err).toContain("BRIDGE_NATS_SEAT_PINS_INVALID");
  });

  it.skipIf(WINDOWS)("dry-run reports what each allow entry resolves to now, and writes nothing", async () => {
    const { config, store, stateDir } = await setup();
    fs.rmdirSync(stateDir);
    await addRoot(store, PAUL, { incarnation: "inc-p" });
    const kate: NatsRootInfo = { id: KATE, kind: "root", name: "main", sessionId: KATE.slice(8), rootId: KATE, machine: "sandbox1",
      ownerHostId: KATE, ownerIdentityId: KATE, ownerIncarnation: "inc-k", status: "idle", capabilities: ["followUp"], live: true };
    let starts = 0;
    const starter: NatsLinkStarter = async () => { starts += 1; return { closed: new Promise(() => undefined), stop: async () => undefined }; };
    expect((await run(["start", "--config", config, "--dry-run"], { starter, readRoots: async () => [kate] })).code).toBe(2);
    const dry = await run(["dry-run", "--config", config], { starter, readRoots: async () => [kate] });
    expect(dry.code).toBe(0);
    expect(dry.json()).toMatchObject({ v: 1, link: "v2", natsMachine: "ryzen1-bridge", live: 2, refused: 0, rows: [
      { side: "file", key: PAUL, status: "live", id: PAUL, mirrorTo: "nats", principal: "Paul" },
      { side: "nats", key: KATE, status: "live", id: KATE, machine: "sandbox1", mirrorTo: "file", principal: "Kate" },
    ] });
    expect([starts, fs.existsSync(stateDir)]).toEqual([0, false]);
    // Kate not on NATS yet: the row says so and the dry run exits 1.
    const absent = await run(["dry-run", "--config", config], { starter, readRoots: async () => [] });
    expect(absent.code).toBe(1);
    expect((absent.json().rows as Array<{ side: string; status: string }>).find((row) => row.side === "nats")!.status).not.toBe("live");
    const down = await run(["dry-run", "--config", config], { starter,
      readRoots: async () => { throw new NatsRootsUnavailableError("no complete NATS presence snapshot within 10000 ms"); } });
    expect([down.code, down.out]).toEqual([1, ""]);
  });

  it("roots reads NATS presence from the Main's own environment or the link config", async () => {
    const { config } = await setup();
    const env = { PI_FABRIC_MESH_NATS: JSON.stringify({ servers: ["tls://intel1.example.test:4222"], machine: "sandbox1",
      credentials: "/etc/fabric-test/nats/machine.creds", custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
      pathBMachines: ["ryzen1-bridge", "sandbox1"], tls: { caFile: "/etc/fabric-test/nats/ca.pem" } }) };
    const calls: Array<{ source: NatsRootsSource; options: unknown }> = [];
    const readRoots = async (source: NatsRootsSource, options: unknown) => {
      calls.push({ source, options });
      return [{ id: PAUL, live: true }] as unknown as NatsRootInfo[];
    };
    const listed = await run(["roots", "--include-stale", "--kind", "root", "--kind", "actor"], { env, readRoots });
    expect([listed.code, JSON.parse(listed.out)]).toEqual([0, [{ id: PAUL, live: true }]]);
    expect(calls[0]).toMatchObject({ source: { machine: "sandbox1" }, options: { includeStale: true, kinds: ["root", "actor"] } });
    expect((await run(["roots", "--config", config], { env: {}, readRoots })).code).toBe(0);
    expect(calls[1]!.source).toMatchObject({ machine: "ryzen1-bridge", credentials: expect.stringContaining("bridge.creds") });

    expect((await run(["roots"], { env: {}, readRoots })).err).toContain("NATS reader settings missing");
    expect((await run(["roots", "--config", config, "--machine", "x"], { env, readRoots })).code).toBe(2);
    expect((await run(["roots", "--kind", "tell"], { env, readRoots })).code).toBe(2);
    expect((await run(["roots", "--creds", "relative.creds"], { env, readRoots })).code).toBe(2);
    const down = await run(["roots"], { env, readRoots: async () => { throw new NatsRootsUnavailableError("no snapshot"); } });
    expect([down.code, down.out, down.err]).toEqual([1, "", "nats-link: FABRIC_NATS_ROOTS_UNAVAILABLE: no snapshot\n"]);
  });

  it("is reachable as `pi-fabric nats-link`", async () => {
    const write = process.stdout.write;
    let out = "";
    process.stdout.write = ((chunk: string) => { out += chunk; return true; }) as typeof process.stdout.write;
    try {
      expect(await runFabricCli(["nats-link", "help"])).toBe(0);
    } finally {
      process.stdout.write = write;
    }
    expect(out).toContain("nats-link roots");
  });
});
