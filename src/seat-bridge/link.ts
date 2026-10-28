import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import type { NatsLinkConfig } from "../mesh/bridge-nats-config.js";
import { natsLinkLockHolder, natsLinkRootsSource } from "../mesh/bridge-nats-ops.js";
import { readNatsRoots, type NatsRootInfo } from "../mesh/bridge-nats-roots.js";
import { natsLinkDryRun, natsLinkStatus, sessionReceipt } from "../mesh-bridge-nats-cli.js";
import { SeamGap } from "../seat-drain/run.js";

/**
 * The bridge seam's reload and read-backs (principal-seat move). The allow-list is read only when the link starts (it is
 * never widened at run time), so a reload is a restart of the link's own service-manager unit, pinned into the
 * installed seam. Every read here is the link's own read-only view: `nats-link status`, `receipt --session --nats`
 * and `roots`, as the link's machine.
 */
export interface LinkDeps {
  /** Tests only: a fake systemctl and fake readers. Production: /usr/bin/systemctl and the link's own readers. */
  systemctl?: string;
  status?: (config: NatsLinkConfig) => Promise<Record<string, unknown>>;
  receipt?: (config: NatsLinkConfig, session: string) => Promise<Record<string, unknown>>;
  roots?: (config: NatsLinkConfig) => Promise<NatsRootInfo[]>;
  holder?: (config: NatsLinkConfig) => { pid: number; startTime?: string | undefined; purpose: string } | undefined;
  /** The link's REAL identity gate over both sides' presence (`nats-link dry-run`). */
  dryRun?: (config: NatsLinkConfig) => Promise<{ rows: ReadonlyArray<Record<string, unknown>> }>;
  /**
   * Tests only: restart an in-process link in place of `systemctl --user restart <unit>` (the real-broker test). The
   * installed bundle's entry passes no deps, so neither its argv nor its env can reach this; a test asserts it.
   */
  restartLink?: (unit: string) => Promise<number | null>;
}

/**
 * The seat's row as the link's own identity gate resolves it right now (bridgeNatsDryRun, with the same org.json seat
 * pins as `start`): `status` is `live`, or the gate's refusal code (an id with two records, live or not, is ambiguous).
 */
export const gateRow = async (config: NatsLinkConfig, sessionId: string, deps: LinkDeps): Promise<Record<string, unknown> | null> => {
  try {
    const report = await (deps.dryRun ?? (c => natsLinkDryRun(c)))(config);
    const row = report.rows.find(entry => entry.key === `session:${sessionId}`);
    return row ? { ...row } : null;
  } catch (error) { return { status: "unreadable", reason: error instanceof Error ? error.message.slice(0, 300) : "dry-run failed" }; }
};

const SYSTEMCTL = "/usr/bin/systemctl";
export const LINK_UNIT = /^[A-Za-z0-9][A-Za-z0-9@._-]{0,120}\.service$/;

/** The fixed client: root-owned, not writable by others, in a root-owned directory chain. */
const fixedSystemctl = (): string => {
  try {
    const real = fs.realpathSync(SYSTEMCTL);
    const stat = fs.statSync(real);
    if (!stat.isFile() || stat.uid !== 0 || (stat.mode & 0o022) !== 0) throw new SeamGap("systemctl-unsafe");
    for (let current = path.dirname(real); ; current = path.dirname(current)) {
      const parent = fs.lstatSync(current);
      if (!parent.isDirectory() || parent.uid !== 0 || (parent.mode & 0o022) !== 0) throw new SeamGap("systemctl-unsafe");
      if (path.dirname(current) === current) break;
    }
    return real;
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    throw new SeamGap("systemctl-missing");
  }
};

/** `systemctl --user restart <unit>`, the seat user's own manager, with a minimal env. */
const restartUnit = (unit: string, uid: number, home: string, deps: LinkDeps, timeoutMs: number): Promise<number | null> =>
  new Promise(resolve => {
    if (!LINK_UNIT.test(unit)) { resolve(null); return; }
    if (deps.restartLink) { deps.restartLink(unit).then(resolve, () => resolve(null)); return; }
    const runtime = `/run/user/${uid}`;
    const child = spawn(deps.systemctl ?? fixedSystemctl(), ["--user", "restart", unit], {
      stdio: ["ignore", "ignore", "ignore"], timeout: Math.max(1_000, Math.floor(timeoutMs)),
      env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C", HOME: home, XDG_RUNTIME_DIR: runtime,
        DBUS_SESSION_BUS_ADDRESS: `unix:path=${runtime}/bus` },
    });
    child.on("error", () => resolve(null));
    child.on("close", code => resolve(code));
  });

export interface ReloadEvidence {
  unit: string;
  before: { pid: number; startTime: string | null } | null;
  after: { pid: number; startTime: string | null } | null;
  restartExit: number | null;
  statusOk: boolean;
  problems: string[];
  waitedMs: number;
}

const holderOf = (config: NatsLinkConfig, deps: LinkDeps) => {
  try {
    const holder = (deps.holder ?? natsLinkLockHolder)(config);
    return holder && holder.purpose === "start" ? { pid: holder.pid, startTime: holder.startTime ?? null } : null;
  } catch { return null; }
};

/**
 * Restart the link and wait until a NEW process holds its lock (another pid or start time) and `nats-link status`
 * reports no problem. Not reloaded in time is a refusal, never assumed done.
 */
export const reloadLink = async (config: NatsLinkConfig, unit: string, seat: { uid: number; home: string }, deps: LinkDeps,
  clock: { now: () => number; sleep: (ms: number) => Promise<void>; deadline: number },
  tolerate: (problem: string) => boolean = () => false): Promise<ReloadEvidence> => {
  const started = clock.now();
  const before = holderOf(config, deps);
  const restartExit = await restartUnit(unit, seat.uid, seat.home, deps, Math.min(60_000, clock.deadline - clock.now()));
  let after: ReloadEvidence["after"] = null;
  let problems: string[] = ["not reloaded"];
  let statusOk = false;
  while (restartExit === 0 && clock.now() < clock.deadline) {
    after = holderOf(config, deps);
    const fresh = after !== null && (before === null || after.pid !== before.pid || after.startTime !== before.startTime);
    if (fresh) {
      try {
        const status = await (deps.status ?? natsLinkStatus)(config);
        const all = Array.isArray(status.problems) ? (status.problems as unknown[]).map(String) : ["status unreadable"];
        // Only a named, expected leftover is tolerated (recorded): the removed seat's own old mirror row. EVERY problem is
        // judged; only the evidence copy is cut to 50.
        statusOk = Array.isArray(status.problems) && all.every(tolerate);
        problems = all.slice(0, 50);
      } catch { problems = ["status unreadable"]; statusOk = false; }
      if (statusOk) break;
    } else problems = ["the link lock does not name a new process"];
    await clock.sleep(1_000);
  }
  return { unit, before, after, restartExit, statusOk, problems, waitedMs: Math.round(clock.now() - started) };
};

/** Each allow entry's `present`/`live` as `nats-link status` shows it (both sides), keyed by identity. */
export const routeTable = async (config: NatsLinkConfig, deps: LinkDeps): Promise<Record<string, { side: string; live: boolean }> | null> => {
  try {
    const status = await (deps.status ?? natsLinkStatus)(config);
    const allow = status.allow as { file?: Array<Record<string, unknown>>; nats?: Array<Record<string, unknown>> } | undefined;
    if (!allow) return null;
    const table: Record<string, { side: string; live: boolean }> = {};
    for (const side of ["file", "nats"] as const) {
      for (const row of allow[side] ?? []) if (typeof row.identity === "string") table[row.identity] = { side, live: row.live === true };
    }
    return table;
  } catch { return null; }
};

/**
 * Every OTHER route the link carried live before the reload must be live again after it (the routes that share the
 * link). Polled until the deadline; the names of the ones still down are returned.
 */
export const otherRoutesBack = async (config: NatsLinkConfig, before: Record<string, { side: string; live: boolean }>, exclude: string,
  deps: LinkDeps, clock: { now: () => number; sleep: (ms: number) => Promise<void>; deadline: number }) => {
  const wanted = Object.entries(before).filter(([identity, row]) => identity !== exclude && row.live).map(([identity]) => identity);
  let down = wanted;
  let after: Record<string, { side: string; live: boolean }> | null = null;
  for (;;) {
    after = await routeTable(config, deps);
    down = after ? wanted.filter(identity => after![identity]?.live !== true) : wanted;
    if (down.length === 0 || clock.now() >= clock.deadline) break;
    await clock.sleep(1_000);
  }
  return { checked: wanted, down, after };
};

/**
 * The seat's route, polled until accepted or the deadline: the link's identity gate row for the seat AND
 * `nats-link receipt --session <sid> --nats` (the records and incarnations on both sides), both read against the
 * config ON DISK now (the caller passes the post-write config).
 */
export const awaitRoutable = async (config: NatsLinkConfig, sessionId: string, deps: LinkDeps,
  clock: { now: () => number; sleep: (ms: number) => Promise<void>; deadline: number },
  accept: (gate: Record<string, unknown> | null, receipt: Record<string, unknown>) => boolean) => {
  let last: Record<string, unknown> | null = null;
  let gate: Record<string, unknown> | null = null;
  for (;;) {
    gate = await gateRow(config, sessionId, deps);
    try { last = await (deps.receipt ?? ((c, s) => sessionReceipt(c, s, { nats: true })))(config, sessionId); }
    catch (error) { last = { ok: false, problems: [error instanceof Error ? error.message.slice(0, 300) : "receipt failed"] }; }
    if (last && accept(gate, last)) return { ok: true, gate, receipt: last };
    if (clock.now() >= clock.deadline) return { ok: false, gate, receipt: last };
    await clock.sleep(1_000);
  }
};

/** Every NATS presence record (live or not) as the link's machine reads it. */
export const natsRecords = async (config: NatsLinkConfig, deps: LinkDeps): Promise<NatsRootInfo[]> => {
  try { return await (deps.roots ?? (c => readNatsRoots(natsLinkRootsSource(c), { includeStale: true, kinds: ["root"] })))(config); }
  catch { throw new SeamGap("nats-presence-unreadable"); }
};
