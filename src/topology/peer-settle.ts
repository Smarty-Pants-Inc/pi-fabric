import type { FabricPeerInfo } from "./types.js";

/** Quiet window after a peer's last observed activity before it counts as settled. */
const DEFAULT_PEER_SETTLED_FOR_MS = 3_000;
const PEER_SETTLE_POLL_MS = 500;

/**
 * Derive a Linear-style label prefix from a project path's basename:
 * "pi-queue-steer" -> "PQS", "fabric" -> "FAB". Falls back to "P".
 */
export const peerLabelPrefix = (cwd: string | undefined): string => {
	const base = cwd?.split(/[\\/]/).filter(Boolean).at(-1) ?? "";
	const words = base.split(/[^A-Za-z0-9]+/).filter(Boolean);
	if (words.length === 0) return "P";
	// Single-word projects borrow the first letters ("fabric" -> "FAB"),
	// multi-word projects take word initials ("pi-queue-steer" -> "PQS").
	if (words.length === 1) return words[0]!.slice(0, 3).toUpperCase();
	return words
		.slice(0, 4)
		.map((word) => word[0]!.toUpperCase())
		.join("");
};

/** Snapshot of one root peer session for pickers and status lines. */
export interface FabricPeerCard {
	id: string;
	label: string;
	status: "idle" | "running";
	model?: string;
	cwd?: string;
	startedAt: number;
	updatedAt: number;
	pendingMessages: boolean;
}

/** Labels are chronological, so creation order is simply label minting order. */
export const buildPeerCards = (peers: readonly FabricPeerInfo[]): FabricPeerCard[] =>
	[...peers]
		.sort((left, right) => left.startedAt - right.startedAt || left.id.localeCompare(right.id))
		.map((peer) => ({
			id: peer.id,
			label: peer.label ?? peer.name,
			status: peer.status,
			...(peer.model ? { model: peer.model } : {}),
			...(peer.cwd ? { cwd: peer.cwd } : {}),
			startedAt: peer.startedAt,
			updatedAt: peer.updatedAt,
			pendingMessages: peer.pendingMessages,
		}));

interface PeerSettleProgress {
	waiting: Array<{ label: string; status: "idle" | "running" }>;
}

export type PeerSettleResult = { ok: true } | { ok: false; error: string };

export interface AwaitPeerSettleOptions {
	/** Fresh peer snapshot source (typically FabricRuntimeState.peerInfos()). */
	poll: () => FabricPeerInfo[];
	/** Peer label (case-insensitive) or exact participant id. Omitted means all peers. */
	selector?: string;
	settledForMs?: number;
	pollMs?: number;
	now?: () => number;
	signal?: AbortSignal;
	onUpdate?: (progress: PeerSettleProgress) => void;
	/** A stalled mesh makes peers look departed; report it instead of "settled". */
	stalled?: () => Error | undefined;
	/**
	 * When this host last committed a mesh heartbeat. A vanished peer counts as departed
	 * only after a commit later than its disappearance showed the mesh working.
	 */
	confirmedAt?: () => number | undefined;
}

const matchesSelector = (peer: FabricPeerInfo, selector: string): boolean => {
	const target = selector.trim().toLowerCase();
	if (!target) return false;
	return (
		peer.id.toLowerCase() === target ||
		(peer.label ?? "").toLowerCase() === target ||
		peer.name.toLowerCase() === target
	);
};

interface WatchedPeer {
	id: string;
	label: string;
	running: boolean;
	/** Last time the peer was observed running; undefined when quiet since arming. */
	lastRunningAt: number | undefined;
	settled: boolean;
	/** When the peer was first seen missing; cleared when it reappears. */
	missingSince?: number;
}

/**
 * Wait until every watched peer has settled: only runs that started after
 * arming delay it (an idle-at-arm peer satisfies once the quiet window
 * passes), and a peer vanishing from the mesh counts as settled since it can
 * no longer conflict.
 */
export const awaitPeerSettle = (options: AwaitPeerSettleOptions): Promise<PeerSettleResult> => {
	const now = options.now ?? (() => Date.now());
	const settledFor = Math.max(0, options.settledForMs ?? DEFAULT_PEER_SETTLED_FOR_MS);
	const pollMs = Math.max(10, options.pollMs ?? PEER_SETTLE_POLL_MS);
	const armedAt = now();
	const stalledAtArm = options.stalled?.();
	if (stalledAtArm) return Promise.resolve({ ok: false, error: stalledAtArm.message });
	const initial = options.poll();
	const targets =
		options.selector !== undefined
			? initial.filter((peer) => matchesSelector(peer, options.selector ?? ""))
			: [...initial];
	if (options.selector !== undefined && targets.length === 0) {
		return Promise.resolve({
			ok: false,
			error: `No Fabric peer matches "${options.selector}" on this project mesh`,
		});
	}
	// With confirmedAt, a snapshot is complete only once this host has committed a heartbeat
	// after arming: before that it can already omit a peer that lapsed behind a stalled lock.
	const confirmedSinceArm = (): boolean => {
		const confirmed = options.confirmedAt?.();
		return confirmed === undefined || confirmed > armedAt;
	};
	if (targets.length === 0 && confirmedSinceArm()) return Promise.resolve({ ok: true });

	const watched = new Map<string, WatchedPeer>();
	const watch = (peer: FabricPeerInfo, at: number): void => {
		watched.set(peer.id, {
			id: peer.id,
			label: peer.label ?? peer.name,
			running: peer.status === "running",
			lastRunningAt: peer.status === "running" ? at : undefined,
			settled: false,
		});
	};
	for (const peer of targets) watch(peer, armedAt);

	return new Promise<PeerSettleResult>((resolve) => {
		let timer: ReturnType<typeof setInterval> | undefined;
		let done = false;
		const finish = (result: PeerSettleResult): void => {
			if (done) return;
			done = true;
			if (timer) clearInterval(timer);
			options.signal?.removeEventListener("abort", onAbort);
			resolve(result);
		};
		const onAbort = (): void => finish({ ok: false, error: "cancelled" });
		const tick = (): void => {
			// A timer callback must never throw: an uncaught error exits interactive Pi.
			try {
				poll();
			} catch (error) {
				finish({ ok: false, error: error instanceof Error ? error.message : String(error) });
			}
		};
		const poll = (): void => {
			const stalled = options.stalled?.();
			if (stalled) {
				finish({ ok: false, error: stalled.message });
				return;
			}
			const confirmed = confirmedSinceArm();
			const snapshot = options.poll();
			const byId = new Map(snapshot.map((peer) => [peer.id, peer] as const));
			const current = now();
			if (!confirmed) {
				// Peers the arming snapshot missed, seen before the first confirming commit.
				for (const peer of snapshot) {
					if (watched.has(peer.id)) continue;
					if (options.selector !== undefined && !matchesSelector(peer, options.selector)) continue;
					watch(peer, current);
				}
			}
			for (const entry of watched.values()) {
				if (entry.settled) continue;
				const peer = byId.get(entry.id);
				if (!peer) {
					entry.missingSince ??= current;
					const confirmed = options.confirmedAt?.();
					if (confirmed === undefined || confirmed > entry.missingSince) entry.settled = true;
					continue;
				}
				delete entry.missingSince;
				entry.label = peer.label ?? peer.name;
				const running = peer.status === "running";
				if (running) {
					entry.lastRunningAt = current;
					entry.running = true;
					continue;
				}
				entry.running = false;
				const quietSince = entry.lastRunningAt ?? armedAt;
				if (current - quietSince >= settledFor) entry.settled = true;
			}
			const waiting = [...watched.values()]
				.filter((entry) => !entry.settled)
				.map((entry) => ({ label: entry.label, status: entry.running ? "running" : "idle" }) as const);
			if (waiting.length === 0 && confirmed) {
				options.onUpdate?.({ waiting });
				finish({ ok: true });
				return;
			}
			options.onUpdate?.({ waiting });
		};
		if (options.signal) {
			if (options.signal.aborted) {
				finish({ ok: false, error: "cancelled" });
				return;
			}
			options.signal.addEventListener("abort", onAbort, { once: true });
		}
		timer = setInterval(tick, pollMs);
		tick();
	});
};
