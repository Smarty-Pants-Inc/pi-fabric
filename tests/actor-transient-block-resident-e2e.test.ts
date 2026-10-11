import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

// smarty-dev#6509 end to end. A real ResidentHost (its ActorManager, AgentManager and participant
// presence) on a temp mesh root runs the real Fabric worker (src/worker.ts), which runs a fake Pi
// RPC child (tests/fixtures/fake-pi-rpc.mjs). Its provider fails with a real transport error (or a
// 401) a set number of times, then succeeds. The cooldown is shortened through the actors config;
// no fake timers or clocks. A separate MeshStore plays the factory router: it reads the published
// actor record and routes the next review request only when that record is not blocked.
const COOLDOWN_MS = 1_500;
const MAX_MS = 4_000;
const TOPIC = "review.requests";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const waitFor = async (predicate: () => boolean, what: string, timeoutMs = 60_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
};

type Block = { code: string; class?: string; since: number; blockedAt?: number; count: number; retryAt?: number; backoffMs?: number;
  classification?: string };
type Presence = { status?: string; activationBlocked?: Block; activationBlockExpired?: Block & { expiredAt: number } } | undefined;

const fixture = (cooldownMs = COOLDOWN_MS) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-6509-e2e-"));
  roots.push(root);
  const piBinary = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
  fs.chmodSync(piBinary, 0o755);
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:review-main", sessionId: "review-main",
    cwd: root, projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(meshRoot, "session:review-main"), fullCodeMode: true,
    agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("src/worker.ts"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary, claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fake", id: "fake-model" }], aliases: {}, defaultModel: "fake/fake-model" },
    // The knob under test: a short cooldown and cap instead of 10 and 60 minutes.
    actors: { ...DEFAULT_FABRIC_CONFIG.actors, transientBlockCooldownMs: cooldownMs, transientBlockMaxMs: MAX_MS },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const router = new MeshStore(meshRoot, 65_536, 100);
  const from: MeshIdentity = { id: "session:router", name: "router", kind: "main", sessionId: "router" };
  const token = randomUUID().replace(/-/g, "");
  closers.push(async () => {
    for (const marker of ["TRANSIENT_FAILS", "AUTH_FAILS"]) fs.rmSync(path.join(os.tmpdir(), `fake-pi-rpc-${marker}-${token}`), { force: true });
  });
  const start = async () => {
    const host = new ResidentHost(config);
    closers.push(() => host.close());
    await host.start();
    return host;
  };
  return { config, router, token, start,
    presence: (id: string): Presence => router.get(`actors/${config.sessionId}/${id}`)?.value as Presence,
    // Factory review routing: skip an actor whose published record is blocked or not live.
    routable: (id: string): boolean => {
      const value = router.get(`actors/${config.sessionId}/${id}`)?.value as Presence;
      // A dormant actor still accepts deliveries: delivery wakes it, so it is routable.
      return value !== undefined && value.activationBlocked === undefined && ["idle", "running", "queued", "dormant"].includes(String(value.status));
    },
    route: (text: string) => router.publish({ topic: TOPIC, from, text }),
  };
};

const createReviewer = (host: ResidentHost) => host.actors.create({ name: "review-route", instructions: "Review each request.",
  responseMode: "text", topics: [TOPIC], coalesce: false, residency: "durable" });

const settled = (host: ResidentHost, id: string) => {
  const status = host.actors.status(id);
  return !status.inFlightRun && !status.preparing && status.queued === 0;
};

describe("transient activation block on a resident host, end to end (smarty-dev#6509)", () => {
  it("a transient provider error blocks; after the configured cooldown the router re-admits it and the probe clears it", async () => {
    const s = fixture();
    const host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked !== undefined, "the transient block to be published");
    const blocked = s.presence(actor.id)!.activationBlocked!;
    expect(blocked).toMatchObject({ code: "provider-transient", class: "transient", count: 1, backoffMs: COOLDOWN_MS,
      retryAt: blocked.blockedAt! + COOLDOWN_MS });
    expect(blocked.classification).toMatch(/^transient: /);
    expect(host.actors.status(actor.id).activationBlocked).toMatchObject({ class: "transient", retryAt: blocked.retryAt });
    expect(s.routable(actor.id)).toBe(false);

    // No fake clock: the real cooldown passes and the host republishes the record unblocked.
    await waitFor(() => s.routable(actor.id), "the cooldown to expire", 20_000);
    expect(Date.now()).toBeGreaterThanOrEqual(blocked.retryAt!);
    expect(s.presence(actor.id)!.activationBlockExpired).toMatchObject({ code: "provider-transient", class: "transient",
      backoffMs: COOLDOWN_MS, expiredAt: blocked.retryAt });

    // The router routes the next request: the probe runs on the real worker, succeeds, and clears the block.
    const failedRun = host.actors.status(actor.id).lastRunId;
    await s.route(`review 2 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => host.actors.status(actor.id).lastRunId !== failedRun && settled(host, actor.id) &&
      host.actors.status(actor.id).activationBlockExpired === undefined, "the probe to complete");
    expect(host.actors.status(actor.id)).not.toHaveProperty("activationBlocked");
    const out = host.actors.messages(actor.id).filter((message) => message.direction === "out");
    expect(out.at(-1)).not.toHaveProperty("error");
    expect(out.at(-1)?.text).toContain("validated actor response");
    await waitFor(() => s.presence(actor.id) !== undefined && s.presence(actor.id)!.activationBlockExpired === undefined, "the clear to be published");
    expect(s.presence(actor.id)).not.toHaveProperty("activationBlocked");
    expect(s.routable(actor.id)).toBe(true);
  }, 120_000);

  it("a transient failure on the probe re-blocks with the backoff doubled; the next probe clears it", async () => {
    const s = fixture();
    const host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=2#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked !== undefined, "the first block");
    expect(s.presence(actor.id)!.activationBlocked).toMatchObject({ class: "transient", count: 1, backoffMs: COOLDOWN_MS });
    await waitFor(() => s.routable(actor.id), "the first cooldown to expire", 20_000);

    await s.route(`review 2 TRANSIENT_FAILS=2#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked?.count === 2, "the failed probe to re-block");
    const second = s.presence(actor.id)!.activationBlocked!;
    expect(second).toMatchObject({ code: "provider-transient", class: "transient", backoffMs: 2 * COOLDOWN_MS,
      retryAt: second.blockedAt! + 2 * COOLDOWN_MS });
    expect(s.routable(actor.id)).toBe(false);
    // The first cooldown is not enough any more.
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, second.blockedAt! + COOLDOWN_MS + 250 - Date.now())));
    expect(s.routable(actor.id)).toBe(false);
    await waitFor(() => s.routable(actor.id), "the doubled cooldown to expire", 20_000);
    expect(Date.now()).toBeGreaterThanOrEqual(second.retryAt!);

    await s.route(`review 3 TRANSIENT_FAILS=2#${s.token}`);
    await waitFor(() => settled(host, actor.id) && host.actors.status(actor.id).activationBlockExpired === undefined &&
      host.actors.status(actor.id).activationBlocked === undefined, "the second probe to clear the block");
    expect(host.actors.messages(actor.id).filter((message) => message.direction === "out").at(-1)).not.toHaveProperty("error");
  }, 120_000);

  it("a hard block (rejected credentials) never expires; only a completed run clears it", async () => {
    const s = fixture();
    const host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 AUTH_FAILS=1#${s.token}`);
    await waitFor(() => s.presence(actor.id)?.activationBlocked !== undefined, "the hard block");
    const blocked = s.presence(actor.id)!.activationBlocked!;
    expect(blocked).toMatchObject({ code: "auth", class: "hard", count: 1 });
    expect(blocked.classification).toMatch(/^permanent: credential or authentication failure/);
    expect(blocked).not.toHaveProperty("retryAt");
    expect(blocked).not.toHaveProperty("backoffMs");
    // Well past any transient cooldown and cap: still blocked, still skipped by the router.
    await new Promise((resolve) => setTimeout(resolve, MAX_MS + COOLDOWN_MS));
    expect(s.routable(actor.id)).toBe(false);
    expect(s.presence(actor.id)).toMatchObject({ activationBlocked: { code: "auth", class: "hard" } });
    expect(host.actors.status(actor.id)).toMatchObject({ activationBlocked: { code: "auth", class: "hard" } });
    expect(host.actors.status(actor.id)).not.toHaveProperty("activationBlockExpired");

    // Fixed (the credentials work again): an explicit operator ask completes and clears it.
    await expect(host.actors.ask(actor.id, `repair probe AUTH_FAILS=1#${s.token}`)).resolves.toBeDefined();
    await waitFor(() => settled(host, actor.id) && host.actors.status(actor.id).activationBlocked === undefined, "the repair run to clear the block");
    await waitFor(() => s.routable(actor.id), "the clear to be published");
  }, 120_000);

  it("the block, its class, time and backoff survive a host restart, then expire and clear", async () => {
    const s = fixture(6_000);
    let host = await s.start();
    const actor = await createReviewer(host);
    await waitFor(() => s.routable(actor.id), "the reviewer to be routable");

    await s.route(`review 1 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => host.actors.status(actor.id).activationBlocked !== undefined && settled(host, actor.id), "the block");
    const blocked = host.actors.status(actor.id).activationBlocked!;
    expect(blocked).toMatchObject({ class: "transient", backoffMs: 6_000, retryAt: blocked.blockedAt! + 6_000 });

    await host.close();
    // The registry record on disk carries the class, the time and the backoff.
    const registry = (fs.readdirSync(s.config.cwd, { recursive: true, encoding: "utf8" }) as string[])
      .filter((file) => path.basename(file) === "actors.json")
      .map((file) => fs.readFileSync(path.join(s.config.cwd, file), "utf8")).find((text) => text.includes(actor.id)) ?? "";
    expect(registry).toContain(`"backoffMs":6000`);
    expect(registry).toContain(`"class":"transient"`);
    expect(registry).toContain(`"blockedAt":${blocked.blockedAt}`);

    host = await s.start();
    expect(Date.now()).toBeLessThan(blocked.retryAt!);
    expect(host.actors.status(actor.id).activationBlocked).toEqual(blocked);
    await waitFor(() => s.presence(actor.id)?.activationBlocked?.retryAt === blocked.retryAt, "the restored block to be published");
    expect(s.routable(actor.id)).toBe(false);
    await waitFor(() => s.routable(actor.id), "the restored cooldown to expire", 30_000);
    expect(Date.now()).toBeGreaterThanOrEqual(blocked.retryAt!);

    await s.route(`review 2 TRANSIENT_FAILS=1#${s.token}`);
    await waitFor(() => settled(host, actor.id) && host.actors.status(actor.id).activationBlockExpired === undefined &&
      host.actors.status(actor.id).activationBlocked === undefined, "the probe after restart to clear the block");
  }, 120_000);
});
