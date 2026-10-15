import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, type RootInboxSession } from "../src/topology/root-inbox.js";

// PI_FABRIC_INBOX_WAKE_COOLDOWN_MS bound, contract level (NOT E2E, Decision C): a misconfigured
// huge cooldown is a bound a live run cannot safely force, so it is exercised here against a real
// MeshStore in a temp dir with an injected clock. The named E2E is post-install: an idle Main gets
// an exact-session agents.followUp within the cooldown and wakes; owner fabric-v2.
// Policy: the env cooldown is capped at 1 h (warned once); 0 is valid (no cooldown); an invalid
// value falls back to the 5 min default. Exact-session work wakes inside any cooldown.
// Fixtures are retained (never deleted here), for local inspection.
const ENV = "PI_FABRIC_INBOX_WAKE_COOLDOWN_MS";
const HOUR_MS = 60 * 60_000;
const DEFAULT_COOLDOWN_MS = 5 * 60_000;
const GRACE_MS = 60_000;
const me: MeshIdentity = { id: "session:me", name: "main", kind: "main", sessionId: "me" };
const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
const ALIAS = "fabric-v2";
const held: RootInboxSession = { holdsBatch: () => true, holdsSteer: () => false };
const idle = () => true;
const saved = process.env[ENV];

const setup = (cooldown: string) => {
  process.env[ENV] = cooldown;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-root-inbox-cooldown-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let offset = 0;
  const clock = { now: () => Date.now() + offset, advance: (ms: number) => { offset += ms; } };
  // No wakeCooldownMs option: the env value is under test.
  const box = new RootInbox(mesh, me, () => [me.id, ALIAS], { now: clock.now });
  const send = (text: string, to: string) =>
    mesh.publish({ topic: "fleet.work.pi-fabric.1", to, kind: "ask", from: peer, text, data: { ref: "x#1", key: text } });
  const texts = (events: MeshEvent[] | undefined) => (events ?? []).map((event) => event.text);
  /** Initial exact-session wake; returns the clock bounds around the recorded wake time. */
  const initialWake = async () => {
    await box.next(held);
    await send("first exact", me.id);
    const before = clock.now();
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["first exact"]);
    return { before, after: clock.now() };
  };
  return { box, clock, send, texts, initialWake };
};

const cooldownWarnings = (warn: { mock: { calls: unknown[][] } }) =>
  warn.mock.calls.filter((call) => /cooldown|PI_FABRIC_INBOX_WAKE_COOLDOWN_MS/i.test(call.map(String).join(" ")));

afterEach(() => {
  if (saved === undefined) delete process.env[ENV];
  else process.env[ENV] = saved;
  vi.restoreAllMocks();
});

describe("RootInbox.wake env cooldown bound", () => {
  it("caps a 1e12 ms cooldown at 1 h, warns once, and still wakes for exact-session work", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { box, clock, send, texts, initialWake } = setup("1e12");
    const woke = await initialWake();
    await send("alias work", ALIAS);
    clock.advance(GRACE_MS + 1_000);
    // Past the grace, inside the cooldown: alias work is cooling, across several reads.
    for (let read = 0; read < 3; read++) {
      expect(await box.wake(held, idle)).toBeUndefined();
      const due = box.wakeDueAt;
      expect(due).toBeDefined();
      expect(due!).toBeGreaterThan(clock.now());
      expect(due!).toBeLessThanOrEqual(woke.after + HOUR_MS);
    }
    expect(cooldownWarnings(warn)).toHaveLength(1);
    // Exact-session work wakes inside the cooldown, with the ordered alias prefix.
    await send("second exact", me.id);
    const second = clock.now();
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["alias work", "second exact"]);
    // The capped cooldown actually ends: later alias work wakes an hour after the last wake.
    await send("late alias", ALIAS);
    clock.advance(GRACE_MS + 1_000);
    expect(await box.wake(held, idle)).toBeUndefined();
    expect(box.wakeDueAt!).toBeLessThanOrEqual(clock.now() + HOUR_MS);
    clock.advance(HOUR_MS - (clock.now() - second) + 1_000);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["late alias"]);
    expect(cooldownWarnings(warn)).toHaveLength(1);
    await box.close();
  });

  it("treats 0 as a valid no-cooldown value without a warning", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { box, clock, send, texts, initialWake } = setup("0");
    await initialWake();
    await send("alias work", ALIAS);
    clock.advance(GRACE_MS + 1_000);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["alias work"]);
    expect(cooldownWarnings(warn)).toHaveLength(0);
    await box.close();
  });

  it.each(["not-a-number", "-1", "Infinity", "", "   "])("falls back to the 5 min default for %j", async (value) => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const { box, clock, send, texts, initialWake } = setup(value);
    const woke = await initialWake();
    await send("alias work", ALIAS);
    clock.advance(GRACE_MS + 1_000);
    expect(await box.wake(held, idle)).toBeUndefined();
    expect(box.wakeDueAt!).toBeGreaterThanOrEqual(woke.before + DEFAULT_COOLDOWN_MS);
    expect(box.wakeDueAt!).toBeLessThanOrEqual(woke.after + DEFAULT_COOLDOWN_MS);
    clock.advance(DEFAULT_COOLDOWN_MS);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["alias work"]);
    await box.close();
  });
});
