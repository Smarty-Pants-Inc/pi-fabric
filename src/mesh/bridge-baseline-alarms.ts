import { emitMeshJournalAlarm } from "../core/atomic-write.js";
import type { MeshIdentity, MeshStore } from "./store.js";
import { meshAlarmRecipient } from "./store.js";

export const LEGACY_BASELINE_ALARM_AFTER_MS = 2 * 60 * 1_000;
export const LEGACY_BASELINE_ALARM_RETRY_MS = 1_000;

export const isLegacyBaselineAlarmEligible = (supportsPartialPresence: boolean | undefined, noBaselineCycles: number): boolean =>
  supportsPartialPresence === false && noBaselineCycles > 0;

export interface BridgeLegacyBaselineAlarm {
  peer: string;
  state: "alarm" | "recovered";
  /** Stable within one continuous no-baseline episode. */
  episodeKey: string;
  sinceMs: number;
  at: number;
}

/** Publish an addressed owner alarm; no recipient keeps the same episode on the journal only. */
export const publishBridgeLegacyBaselineAlarm = async (
  mesh: MeshStore,
  from: MeshIdentity,
  alarm: BridgeLegacyBaselineAlarm,
): Promise<void> => {
  const durationMs = Math.max(0, alarm.at - alarm.sinceMs);
  const text = alarm.state === "recovered"
    ? `Bridge link ${alarm.peer} recovered: a complete legacy presence baseline was acknowledged`
    : `Bridge link ${alarm.peer} has had no acknowledged legacy presence baseline for more than two minutes`;
  const to = meshAlarmRecipient();
  if (!to) {
    emitMeshJournalAlarm({ label: `mesh bridge ${alarm.peer}`, state: alarm.state === "alarm" ? "outage" : "recovered",
      sinceMs: alarm.sinceMs, durationMs, reason: text, source: "no-baseline", peer: alarm.peer });
    return;
  }
  await mesh.publish({ topic: "ops.owner", kind: alarm.state === "alarm" ? "bridge.legacy-baseline.alarm" : "bridge.legacy-baseline.recovered",
    from, to, text, data: { peer: alarm.peer, source: "no-baseline", sinceMs: alarm.sinceMs, durationMs },
    dedupeKey: `bridge-legacy-baseline:${alarm.peer}:${alarm.episodeKey}:${alarm.state}` });
};

interface NoBaselineEpisode {
  sinceMs: number;
  alarmQueued: boolean;
  alarmPublished: boolean;
  retryAt?: number;
}

/** Serializes retries and recovery so an alarm cannot arrive after its recovery. */
export class BridgeLegacyBaselineAlarmEpisode {
  #episode: NoBaselineEpisode | undefined;
  #tail: Promise<void> = Promise.resolve();

  /** Next time the run loop must observe this episode, including a retry after a failed publish. */
  nextAttemptAt(sinceMs: number): number | undefined {
    const episode = this.#episode;
    if (episode?.sinceMs !== sinceMs) return sinceMs + LEGACY_BASELINE_ALARM_AFTER_MS + 1;
    if (episode.alarmQueued || episode.alarmPublished) return undefined;
    return episode.retryAt ?? sinceMs + LEGACY_BASELINE_ALARM_AFTER_MS + 1;
  }

  observe(
    input: { peer: string; sinceMs: number | undefined; at: number },
    publish: (alarm: BridgeLegacyBaselineAlarm) => Promise<void>,
    onError: (error: unknown) => void,
  ): void {
    const reportError = (error: unknown): void => {
      try { onError(error); } catch { /* Reporting must not break the bridge loop. */ }
    };
    const send = (episode: NoBaselineEpisode, state: BridgeLegacyBaselineAlarm["state"]): void => {
      const alarm: BridgeLegacyBaselineAlarm = { peer: input.peer, state,
        episodeKey: String(episode.sinceMs), sinceMs: episode.sinceMs, at: input.at };
      const operation = this.#tail.then(async () => {
        if (state === "recovered" && !episode.alarmPublished) return;
        try {
          await publish(alarm);
          if (state === "alarm") episode.alarmPublished = true;
        } catch (error) {
          if (state === "alarm" && this.#episode === episode) {
            episode.alarmQueued = false;
            episode.retryAt = Math.max(input.at, Date.now()) + LEGACY_BASELINE_ALARM_RETRY_MS;
          }
          reportError(error);
        }
      });
      this.#tail = operation.catch(reportError);
    };

    if (input.sinceMs === undefined) {
      const previous = this.#episode;
      this.#episode = undefined;
      if (previous && (previous.alarmQueued || previous.alarmPublished)) send(previous, "recovered");
      return;
    }
    if (this.#episode && this.#episode.sinceMs !== input.sinceMs) {
      const previous = this.#episode;
      this.#episode = undefined;
      if (previous.alarmQueued || previous.alarmPublished) send(previous, "recovered");
    }
    this.#episode ??= { sinceMs: input.sinceMs, alarmQueued: false, alarmPublished: false };
    const current = this.#episode;
    if (current.alarmQueued || current.alarmPublished || input.at <= current.sinceMs + LEGACY_BASELINE_ALARM_AFTER_MS) return;
    if (current.retryAt !== undefined && input.at < current.retryAt) return;
    current.alarmQueued = true;
    delete current.retryAt;
    send(current, "alarm");
  }
}
