import { HOST_LEASE_HEARTBEAT_MS, type FabricHostLease } from "../topology/host-leases.js";

export const RESIDENT_WATCHDOG_RENEWAL_MS = HOST_LEASE_HEARTBEAT_MS;
export const RESIDENT_WATCHDOG_INTERVALS = 3;
export interface ResidentZombie { pid: number; processStartTime: string; }

/** Per-attempt observation, never authority over another owner or a release successor. */
export class ResidentLauncherWatchdog {
  readonly #zombies = new Map<string, number>();
  #alarmed = false;
  constructor(readonly renewalMs = RESIDENT_WATCHDOG_RENEWAL_MS, readonly intervals = RESIDENT_WATCHDOG_INTERVALS) {}

  observe(readyAt: number, lease: FabricHostLease | undefined, zombies: readonly ResidentZombie[], now = Date.now()): "stale-lease" | "unreaped-child" | undefined {
    if (this.#alarmed) return;
    const grace = this.renewalMs * this.intervals;
    let reason: "stale-lease" | "unreaped-child" | undefined;
    if (now - (lease?.updatedAt ?? readyAt) >= grace) reason = "stale-lease";
    const present = new Set(zombies.map(row => `${row.pid}:${row.processStartTime}`));
    for (const key of this.#zombies.keys()) if (!present.has(key)) this.#zombies.delete(key);
    for (const key of present) {
      const since = this.#zombies.get(key) ?? now;
      this.#zombies.set(key, since);
      if (now - since >= grace) reason ??= "unreaped-child";
    }
    if (reason) this.#alarmed = true;
    return reason;
  }
}
