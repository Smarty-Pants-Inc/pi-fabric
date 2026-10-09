import { childProcessEnvironment } from "../core/atomic-write.js";
import { execFile } from "node:child_process";

// Record admission gate (smarty-dev#754, contract C2): refuse new records while the gap-free,
// off-host recoverable WAL frontier lags the insert position by more than refuseSeconds.

const DEFAULT_SEGMENT_SIZE = 16 * 1024 * 1024;
const LSN_PATTERN = /^([0-9A-Fa-f]{1,8})\/([0-9A-Fa-f]{1,8})$/;
const SEGMENT_PATTERN = /^[0-9A-Fa-f]{24}$/;
const MAX_LSN = (1n << 64n) - 1n;

export const lsnToBigInt = (lsn: string): bigint => {
	const match = LSN_PATTERN.exec(lsn);
	if (!match) throw new Error(`malformed LSN: ${JSON.stringify(lsn)}`);
	return (BigInt(`0x${match[1]}`) << 32n) | BigInt(`0x${match[2]}`);
};

export const bigIntToLsn = (value: bigint): string => {
	if (value < 0n || value > MAX_LSN) throw new Error(`LSN out of range: ${value}`);
	return `${(value >> 32n).toString(16).toUpperCase()}/${(value & 0xffffffffn).toString(16).toUpperCase()}`;
};

const checkSegmentSize = (segmentSize: number): bigint => {
	if (!Number.isSafeInteger(segmentSize) || segmentSize <= 0 || 0x100000000 % segmentSize !== 0) {
		throw new Error(`invalid WAL segment size: ${segmentSize}`);
	}
	return BigInt(segmentSize);
};

export const walSegmentStartLsn = (segment: string, segmentSize: number = DEFAULT_SEGMENT_SIZE): bigint => {
	const size = checkSegmentSize(segmentSize);
	if (!SEGMENT_PATTERN.test(segment)) throw new Error(`malformed WAL segment name: ${JSON.stringify(segment)}`);
	const logId = BigInt(`0x${segment.slice(8, 16)}`);
	const segNo = BigInt(`0x${segment.slice(16, 24)}`);
	if (segNo >= 0x100000000n / size) throw new Error(`WAL segment number out of range: ${segment}`);
	return logId * 0x100000000n + segNo * size;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
	typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * End (exclusive) of the contiguous FOUND prefix in a `wal-g wal-verify integrity --json` report.
 * Any non-FOUND range or a hole between ranges stops the prefix; later FOUND ranges do not count.
 */
export const walgFrontier = (report: unknown, segmentSize: number = DEFAULT_SEGMENT_SIZE): string | undefined => {
	try {
		const integrity = isRecord(report) && isRecord(report.integrity) ? report.integrity : report;
		if (!isRecord(integrity) || !Array.isArray(integrity.details)) return undefined;
		const size = BigInt(segmentSize);
		let end: bigint | undefined;
		// ponytail: ranges are taken in report order (WAL-G emits them ascending); a range that
		// does not start at or before the previous end is a hole and stops the prefix.
		for (const range of integrity.details) {
			if (!isRecord(range) || range.status !== "FOUND") break;
			if (typeof range.start_segment !== "string" || typeof range.end_segment !== "string") break;
			const start = walSegmentStartLsn(range.start_segment, segmentSize);
			if (end !== undefined && start > end) break;
			const rangeEnd = walSegmentStartLsn(range.end_segment, segmentSize) + size;
			if (rangeEnd < start) break;
			end = end === undefined || rangeEnd > end ? rangeEnd : end;
		}
		return end === undefined ? undefined : bigIntToLsn(end);
	} catch {
		return undefined;
	}
};

export interface ArchiveFrontierProvider {
	readonly name: string;
	frontier(signal?: AbortSignal): Promise<string | undefined>;
}

export interface CommandRunner {
	(
		command: readonly string[],
		options: { env?: Record<string, string>; timeoutMs: number; signal?: AbortSignal },
	): Promise<{ stdout: string; stderr: string; code: number | null }>;
}

const execFileRunner: CommandRunner = (command, options) =>
	new Promise((resolve) => {
		const [file, ...args] = command;
		if (!file) {
			resolve({ stdout: "", stderr: "empty command", code: null });
			return;
		}
		execFile(
			file,
			args,
			{
				// ponytail: env extends the process environment so PATH/HOME still reach wal-g.
				env: childProcessEnvironment(options.env ? { ...process.env, ...options.env } : process.env),
				timeout: options.timeoutMs,
				maxBuffer: 16 * 1024 * 1024,
				encoding: "utf8",
				shell: false,
				...(options.signal ? { signal: options.signal } : {}),
			},
			(error, stdout, stderr) => {
				const code = error ? (typeof error.code === "number" ? error.code : null) : 0;
				resolve({ stdout, stderr: stderr || (error ? error.message : ""), code });
			},
		);
	});

export class WalGFrontierProvider implements ArchiveFrontierProvider {
	private readonly env: Record<string, string> | undefined;
	private readonly timeoutMs: number;
	private readonly segmentSize: number;
	private readonly run: CommandRunner;

	constructor(
		readonly name: string,
		private readonly command: readonly string[],
		options: { env?: Record<string, string>; timeoutMs?: number; segmentSize?: number; run?: CommandRunner } = {},
	) {
		this.env = options.env;
		this.timeoutMs = options.timeoutMs ?? 60_000;
		this.segmentSize = options.segmentSize ?? DEFAULT_SEGMENT_SIZE;
		checkSegmentSize(this.segmentSize);
		this.run = options.run ?? execFileRunner;
	}

	async frontier(signal?: AbortSignal): Promise<string | undefined> {
		const result = await this.run(this.command, {
			timeoutMs: this.timeoutMs,
			...(this.env ? { env: this.env } : {}),
			...(signal ? { signal } : {}),
		});
		let report: unknown;
		try {
			report = JSON.parse(result.stdout);
		} catch {
			throw new Error(
				`${this.name}: wal-verify ${result.code === 0 ? "printed no JSON" : `exited ${result.code}`}: ${result.stderr.slice(0, 500)}`,
			);
		}
		return walgFrontier(report, this.segmentSize);
	}
}

export interface AdmissionTargetStatus {
	name: string;
	frontier?: string;
	checkedAt?: number;
	error?: string;
}

export interface AdmissionStatus {
	state: "ok" | "alarm" | "refuse" | "disabled";
	lagSeconds: number;
	frontier?: string;
	insertLsn?: string;
	targets: AdmissionTargetStatus[];
	checkedAt: number;
	alarmSeconds: number;
	refuseSeconds: number;
}

export class RecordArchiveLaggingError extends Error {
	readonly code = "RECORD_ARCHIVE_LAGGING";
	readonly retryable = true;
	constructor(readonly lagSeconds: number) {
		super(`record archive lagging ${lagSeconds} s; retry with the same key`);
		this.name = "RecordArchiveLaggingError";
	}
}

interface Sample {
	at: number;
	lsn: bigint;
}

export class AdmissionGate {
	private readonly providers: readonly ArchiveFrontierProvider[];
	private readonly alarmSeconds: number;
	private readonly refuseSeconds: number;
	private readonly now: () => number;
	private readonly onStatus: ((status: AdmissionStatus, previous: AdmissionStatus | undefined) => void) | undefined;
	private readonly maxSamples: number;
	private readonly targets: AdmissionTargetStatus[];
	private samples: Sample[] = [];
	private last: AdmissionStatus | undefined;
	private refreshes = 0;

	constructor(options: {
		providers: readonly ArchiveFrontierProvider[];
		alarmSeconds?: number;
		refuseSeconds?: number;
		now?: () => number;
		onStatus?: (status: AdmissionStatus, previous: AdmissionStatus | undefined) => void;
		maxSamples?: number;
	}) {
		this.providers = options.providers;
		this.alarmSeconds = options.alarmSeconds ?? 120;
		this.refuseSeconds = options.refuseSeconds ?? 300;
		this.now = options.now ?? Date.now;
		this.onStatus = options.onStatus;
		this.maxSamples = Math.max(2, options.maxSamples ?? 1000);
		this.targets = this.providers.map((provider) => ({ name: provider.name }));
	}

	get enabled(): boolean {
		return this.providers.length > 0;
	}

	/** Whether any refresh has completed (successful or not). */
	get refreshed(): boolean {
		return this.refreshes > 0;
	}

	/** The freshest target's last good frontier (the max LSN), or undefined. */
	frontier(): string | undefined {
		let frontier: bigint | undefined;
		for (const target of this.targets) {
			if (target.frontier === undefined) continue;
			const value = lsnToBigInt(target.frontier);
			if (frontier === undefined || value > frontier) frontier = value;
		}
		return frontier === undefined ? undefined : bigIntToLsn(frontier);
	}

	async refresh(signal?: AbortSignal): Promise<AdmissionStatus | undefined> {
		const results = await Promise.allSettled(this.providers.map((provider) => provider.frontier(signal)));
		const at = this.now();
		results.forEach((result, index) => {
			const target = this.targets[index];
			if (!target) return;
			let error: string;
			if (result.status === "fulfilled") {
				if (result.value !== undefined) {
					try {
						lsnToBigInt(result.value);
						// ponytail: a lower frontier replaces a higher one (archive lost); fail-closed.
						target.frontier = result.value;
						target.checkedAt = at;
						delete target.error;
						return;
					} catch (cause) {
						error = cause instanceof Error ? cause.message : String(cause);
					}
				} else {
					error = "no recoverable WAL frontier";
				}
			} else {
				error = result.reason instanceof Error ? result.reason.message : String(result.reason);
			}
			target.error = error;
		});
		this.refreshes++;
		// ponytail: re-evaluate against the last insert position so a new frontier (or a stall)
		// shows up without waiting for the next append; oldestUncoveredAt is per-append and dropped.
		if (this.last?.insertLsn !== undefined) this.evaluate({ insertLsn: this.last.insertLsn });
		return this.last;
	}

	sample(insertLsn: string, at: number = this.now()): void {
		const lsn = lsnToBigInt(insertLsn);
		const newest = this.samples.at(-1);
		// Monotone: keep the earliest time a position was seen; ignore positions that go backwards.
		if (newest && (lsn <= newest.lsn || at < newest.at)) return;
		this.samples.push({ at, lsn });
		if (this.samples.length > this.maxSamples) {
			// ponytail: merge the two oldest samples keeping the older time with the newer LSN, so a
			// full ring over-estimates lag (fail-closed) instead of forgetting the oldest write time.
			const [oldest, next] = this.samples;
			if (oldest && next) next.at = oldest.at;
			this.samples.shift();
		}
	}

	evaluate(input: { insertLsn: string; oldestUncoveredAt?: number }): AdmissionStatus {
		const now = this.now();
		const insert = lsnToBigInt(input.insertLsn);
		this.sample(input.insertLsn, now);
		let frontier: bigint | undefined;
		for (const target of this.targets) {
			if (target.frontier === undefined) continue;
			const value = lsnToBigInt(target.frontier);
			if (frontier === undefined || value > frontier) frontier = value;
		}
		if (frontier !== undefined) {
			const covered = frontier;
			this.samples = this.samples.filter((sample) => sample.lsn > covered);
		}
		let lagSeconds = 0;
		if (this.enabled && (frontier === undefined || frontier < insert)) {
			const candidates = [input.oldestUncoveredAt, this.samples[0]?.at].filter(
				(value): value is number => value !== undefined && Number.isFinite(value),
			);
			if (candidates.length > 0) lagSeconds = Math.max(0, Math.floor((now - Math.min(...candidates)) / 1000));
		}
		const state: AdmissionStatus["state"] = !this.enabled
			? "disabled"
			: lagSeconds > this.refuseSeconds
				? "refuse"
				: lagSeconds >= this.alarmSeconds
					? "alarm"
					: "ok";
		const status: AdmissionStatus = {
			state,
			lagSeconds,
			insertLsn: input.insertLsn,
			targets: this.targets.map((target) => ({ ...target })),
			checkedAt: now,
			alarmSeconds: this.alarmSeconds,
			refuseSeconds: this.refuseSeconds,
			...(frontier !== undefined ? { frontier: bigIntToLsn(frontier) } : {}),
		};
		const previous = this.last;
		this.last = status;
		this.onStatus?.(status, previous);
		return status;
	}

	check(input: { insertLsn: string; oldestUncoveredAt?: number }): AdmissionStatus {
		const status = this.evaluate(input);
		if (status.state === "refuse") throw new RecordArchiveLaggingError(status.lagSeconds);
		return status;
	}

	status(): AdmissionStatus | undefined {
		return this.last;
	}
}
