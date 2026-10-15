import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { BridgeOwnershipError, MeshBridge, type BridgePresence, type BridgePublish, type BridgeSide } from "../src/mesh/bridge.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";

// Keep reason: smarty-dev#12312's recorded 500-event catch-up made 15,504 source
// holds() calls with one-event receipts, versus 500 with full 32-event receipts.
// Exercise the real bridge, not disk timing: only cursor fixtures touch the filesystem.
const EVENTS = 65; // Two full candidate pages and a singleton; no slow-fsync budget.
const BATCH_MAX = 32;
type Candidate = { event: BridgePublish; held?: string[] };

class RecordedSide implements BridgeSide {
  readonly events: MeshEvent[] = [];
  readonly output: Array<{ event: BridgePublish; sequence: number }> = [];
  readonly candidateSizes: number[] = [];
  readonly holdsCalls: string[] = [];
  receiptLimit = BATCH_MAX;
  holding = true;
  singles = 0;
  onBatchCommitted: (() => void) | undefined;
  #sequence = 0;
  #mirrored: Pick<BridgePresence, "hosts" | "participants"> = { hosts: [], participants: [] };

  constructor(readonly identity: MeshIdentity) {}

  append(to: string, count = EVENTS): void {
    for (let index = 0; index < count; index++) {
      const sequence = ++this.#sequence;
      this.events.push({ id: `${this.identity.id}/${sequence}`, sequence, createdAt: 0,
        topic: "fleet.work.prefix", kind: "ask", from: this.identity, to, text: String(sequence) });
    }
  }

  async latestSequence(): Promise<number> { return this.#sequence; }

  async read(after: number) {
    const events = this.events.filter(event => event.sequence > after).slice(0, BATCH_MAX);
    return { events, through: events.at(-1)?.sequence ?? Math.max(after, this.#sequence) };
  }

  async presence(): Promise<BridgePresence> {
    const expiresAt = Number.MAX_SAFE_INTEGER;
    return { hosts: [{ record: { format: 1, id: this.identity.id, rootId: this.identity.id,
      identity: this.identity, startedAt: 0, updatedAt: 0, expiresAt }, expiresAt }],
    participants: [], reserved: [this.identity.id] };
  }

  async mirror(presence: Pick<BridgePresence, "hosts" | "participants">): Promise<void> {
    this.#mirrored = presence;
  }

  async owned(): Promise<Pick<BridgePresence, "hosts" | "participants">> { return this.#mirrored; }

  holds(id: string): boolean {
    this.holdsCalls.push(id);
    return this.holding && this.#mirrored.hosts.some(host => host.record.id === id);
  }

  #commit(event: BridgePublish, held: string[] = []): { sequence: number } {
    for (const id of held) {
      if (!this.holds(id)) throw new BridgeOwnershipError(`${id} is no longer held`);
    }
    const sequence = ++this.#sequence;
    this.output.push({ event, sequence });
    return { sequence };
  }

  async publish(event: BridgePublish, held?: string[]): Promise<{ sequence: number }> {
    this.singles++;
    return this.#commit(event, held);
  }

  // Capability declaration only: MeshBridge invokes it internally, with no direct
  // batch call or product store/remover in this suite (local deletion guard).
  async publishBatch(candidates: Candidate[]): Promise<Array<{ sequence: number }>> {
    this.candidateSizes.push(candidates.length);
    const committed = candidates.slice(0, this.receiptLimit).map(({ event, held }) => this.#commit(event, held));
    this.onBatchCommitted?.();
    return committed;
  }

  async bridgedIds(after: number) {
    return { ids: this.output.filter(item => item.sequence > after).map(item => item.event.data.bridge.id),
      through: this.#sequence, done: true };
  }
}

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = async (direction: "toRemote" | "toLocal", prefix: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-short-prefix-"));
  roots.push(root);
  const local = new RecordedSide({ id: "session:prefix-local-0000", name: "local", kind: "main" });
  const remote = new RecordedSide({ id: "session:prefix-remote-0000", name: "remote", kind: "main" });
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(root, "cursor.json"), presenceMs: Number.MAX_SAFE_INTEGER });
  await bridge.start(); // Heads are zero; native source events arrive only afterward.
  const source = direction === "toRemote" ? local : remote;
  const target = direction === "toRemote" ? remote : local;
  target.receiptLimit = prefix;
  source.append(target.identity.id);
  return { bridge, local, source, target };
};

const outputIds = (side: RecordedSide) => side.output.map(item => item.event.data.bridge.id);
const sourceIds = (side: RecordedSide) => side.events.map(event => event.id);

for (const direction of ["toRemote", "toLocal"] as const) {
  describe(`recorded short-prefix receipts ${direction} (smarty-dev#12312)`, () => {
    it.each([1, 2, 4, 8, 32])("prefix %s: ordered delivery with fresh holds bounded by EVENTS + 31", async prefix => {
      const { bridge, local, source, target } = await setup(direction, prefix);
      expect(await bridge.step()).toEqual({ toRemote: direction === "toRemote" ? EVENTS : 0,
        toLocal: direction === "toLocal" ? EVENTS : 0, dropped: 0 });
      expect(outputIds(target)).toEqual(sourceIds(source));
      expect(target.output.map(item => item.event.text)).toEqual(source.events.map(event => event.text));
      expect(target.candidateSizes[0]).toBe(BATCH_MAX);
      expect(Math.max(...target.candidateSizes)).toBeLessThanOrEqual(BATCH_MAX);
      expect(target.candidateSizes.slice(1).every(size => size <= prefix)).toBe(true);
      expect(source.holdsCalls.length).toBeLessThanOrEqual(EVENTS + BATCH_MAX - 1);
      if (direction === "toRemote") {
        expect(source.holdsCalls.length).toBeGreaterThanOrEqual(EVENTS);
        expect(source.holdsCalls.every(id => id === target.identity.id)).toBe(true);
      } else {
        // Inbound ownership is admitted only for the committed prefix, not its speculative suffix.
        expect(source.holdsCalls).toEqual([]);
        expect(local.holdsCalls).toEqual(Array.from({ length: EVENTS }, () => source.identity.id));
      }
      expect(await bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
      expect(outputIds(target)).toEqual(sourceIds(source));
    });

    it("a transient prefix 1 stays small for this pass, then the next pass resets to 32", async () => {
      const { bridge, source, target } = await setup(direction, 1);
      target.onBatchCommitted = () => { target.receiptLimit = BATCH_MAX; };
      expect(await bridge.step()).toMatchObject({ [direction]: EVENTS, dropped: 0 });
      expect(target.candidateSizes).toEqual([BATCH_MAX]);
      expect(target.singles).toBe(EVENTS - 1);
      const holdsBefore = source.holdsCalls.length;
      source.append(target.identity.id);
      expect(await bridge.step()).toMatchObject({ [direction]: EVENTS, dropped: 0 });
      expect(target.candidateSizes).toEqual([BATCH_MAX, BATCH_MAX, BATCH_MAX]);
      expect(target.singles).toBe(EVENTS);
      expect(source.holdsCalls.length - holdsBefore).toBe(direction === "toRemote" ? EVENTS : 0);
      expect(outputIds(target)).toEqual(sourceIds(source));
    });
  });
}

it.each([1, 4])("revoking source holds after prefix %s refuses every subsequent outbound event", async prefix => {
  const { bridge, source, target } = await setup("toRemote", prefix);
  target.onBatchCommitted = () => { source.holding = false; };
  expect(await bridge.step()).toEqual({ toRemote: prefix, toLocal: 0, dropped: EVENTS - prefix });
  expect(outputIds(target)).toEqual(sourceIds(source).slice(0, prefix));
  expect(target.candidateSizes).toEqual([BATCH_MAX]);
  expect(source.holdsCalls.length).toBeGreaterThan(BATCH_MAX);
  expect(target.singles).toBe(0);
});

it("a full receipt of a refusal-stopped list keeps 32 candidates later in the same pass (smarty-dev#12416)", async () => {
  const { bridge, source, target } = await setup("toRemote", BATCH_MAX);
  // Admission stops after two events, but the target commits that whole candidate list.
  source.events[2]!.topic = "fabric.control.private";
  const expected = source.events.filter((_, index) => index !== 2);
  expect(await bridge.step()).toEqual({ toRemote: EVENTS - 1, toLocal: 0, dropped: 1 });
  expect(target.candidateSizes).toEqual([2, BATCH_MAX - 3, BATCH_MAX]);
  expect(outputIds(target)).toEqual(expected.map(event => event.id));
  expect(target.output.map(item => item.event.text)).toEqual(expected.map(event => event.text));
  expect(target.output).toHaveLength(EVENTS - 1);
  expect(new Set(outputIds(target)).size).toBe(EVENTS - 1);
  expect(target.singles).toBe(1);
});
