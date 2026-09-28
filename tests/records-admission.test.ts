import { describe, expect, it } from "vitest";
import {
	AdmissionGate,
	type AdmissionStatus,
	type ArchiveFrontierProvider,
	type CommandRunner,
	RecordArchiveLaggingError,
	WalGFrontierProvider,
	bigIntToLsn,
	lsnToBigInt,
	walSegmentStartLsn,
	walgFrontier,
} from "../src/records/admission.js";

const MiB = 1024 * 1024;
const seg = (tli: number, logId: number, segNo: number) =>
	[tli, logId, segNo].map((n) => n.toString(16).toUpperCase().padStart(8, "0")).join("");
const range = (start: string, end: string, status = "FOUND") => ({
	timeline_id: 1,
	start_segment: start,
	end_segment: end,
	segments_count: 1,
	status,
});

describe("LSN and segment math", () => {
	it("round-trips pg_lsn text", () => {
		for (const lsn of ["0/0", "0/1", "16/B374D848", "FFFFFFFF/FFFFFFFF", "1/0"]) {
			expect(bigIntToLsn(lsnToBigInt(lsn))).toBe(lsn);
		}
		expect(lsnToBigInt("16/b374d848")).toBe(0x16b374d848n);
		expect(bigIntToLsn(0x100000000n)).toBe("1/0");
		for (const bad of ["", "0", "0/", "/0", "G/0", "123456789/0", "0/0/0", " 0/0"]) {
			expect(() => lsnToBigInt(bad)).toThrow(/malformed LSN/);
		}
		expect(() => bigIntToLsn(-1n)).toThrow();
	});

	it("computes segment start LSNs for 16 MiB and other sizes", () => {
		expect(walSegmentStartLsn(seg(1, 0, 1))).toBe(0x1000000n);
		expect(walSegmentStartLsn(seg(1, 2, 0xff))).toBe(0x2ff000000n);
		expect(() => walSegmentStartLsn(seg(1, 0, 0x100))).toThrow(/out of range/);
		expect(walSegmentStartLsn(seg(1, 1, 3), 64 * MiB)).toBe(0x100000000n + 3n * 64n * BigInt(MiB));
		expect(() => walSegmentStartLsn(seg(1, 0, 0x40), 64 * MiB)).toThrow(/out of range/);
		expect(walSegmentStartLsn(seg(1, 0, 0x3ff), 4 * MiB)).toBe(0x3ffn * 4n * BigInt(MiB));
		expect(() => walSegmentStartLsn("nope")).toThrow(/malformed/);
		expect(() => walSegmentStartLsn(seg(1, 0, 0), 3 * MiB)).toThrow(/segment size/);
	});
});

describe("walgFrontier", () => {
	it("returns the end of an all-FOUND chain", () => {
		const report = { integrity: { status: "OK", details: [range(seg(1, 0, 1), seg(1, 0, 9))] } };
		expect(walgFrontier(report)).toBe("0/A000000");
	});

	it("stops before a MISSING_LOST middle range even with newer FOUND ranges and WARNING status", () => {
		const report = {
			integrity: {
				status: "WARNING",
				details: [
					range(seg(1, 0, 1), seg(1, 0, 4)),
					range(seg(1, 0, 5), seg(1, 0, 6), "MISSING_LOST"),
					range(seg(1, 0, 7), seg(1, 0, 0x20)),
				],
			},
		};
		expect(walgFrontier(report)).toBe("0/5000000");
	});

	it("does not count FOUND ranges after a hole between ranges", () => {
		const report = { details: [range(seg(1, 0, 1), seg(1, 0, 2)), range(seg(1, 0, 5), seg(1, 0, 6))] };
		expect(walgFrontier(report)).toBe("0/3000000");
	});

	it("stops at a MISSING_UPLOADING tail", () => {
		const report = {
			integrity: {
				status: "WARNING",
				details: [
					range(seg(1, 0, 1), seg(1, 0, 0xff)),
					range(seg(1, 1, 0), seg(1, 1, 2)),
					range(seg(1, 1, 3), seg(1, 1, 3), "MISSING_UPLOADING"),
				],
			},
		};
		expect(walgFrontier(report)).toBe("1/3000000");
	});

	it("accepts the top-level shape and a non-16MiB segment size", () => {
		const details = [range(seg(1, 0, 1), seg(1, 0, 3))];
		expect(walgFrontier({ status: "OK", details })).toBe("0/4000000");
		expect(walgFrontier({ integrity: { status: "OK", details } })).toBe("0/4000000");
		expect(walgFrontier({ status: "OK", details }, 64 * MiB)).toBe("0/10000000");
	});

	it("returns undefined for empty, garbage, or a leading gap", () => {
		for (const report of [
			undefined,
			null,
			"x",
			42,
			{},
			{ integrity: { details: [] } },
			{ integrity: { details: "no" } },
			{ details: [{ status: "FOUND", start_segment: "zz", end_segment: "zz" }] },
			{ details: [range(seg(1, 0, 1), seg(1, 0, 2), "MISSING_DELAYED"), range(seg(1, 0, 3), seg(1, 0, 4))] },
		]) {
			expect(walgFrontier(report)).toBeUndefined();
		}
	});
});

class FakeProvider implements ArchiveFrontierProvider {
	constructor(
		readonly name: string,
		public value: string | undefined | Error,
	) {}
	async frontier(): Promise<string | undefined> {
		if (this.value instanceof Error) throw this.value;
		return this.value;
	}
}

const setup = (values: Array<string | undefined | Error>, options: { maxSamples?: number } = {}) => {
	let now = 1_000_000;
	const calls: Array<[AdmissionStatus, AdmissionStatus | undefined]> = [];
	const providers = values.map((value, index) => new FakeProvider(`t${index}`, value));
	const gate = new AdmissionGate({
		providers,
		now: () => now,
		onStatus: (status, previous) => calls.push([status, previous]),
		...options,
	});
	return { gate, providers, calls, advance: (seconds: number) => (now += seconds * 1000), at: () => now };
};

describe("AdmissionGate", () => {
	it("is ok under 2 min, alarm at exactly 120 s, alarm at 300 s, refuse at 301 s", async () => {
		const { gate, advance } = setup(["0/1000"]);
		await gate.refresh();
		expect(gate.evaluate({ insertLsn: "0/2000" }).state).toBe("ok");
		advance(119);
		expect(gate.evaluate({ insertLsn: "0/3000" })).toMatchObject({ state: "ok", lagSeconds: 119, frontier: "0/1000" });
		advance(1);
		expect(gate.evaluate({ insertLsn: "0/4000" })).toMatchObject({ state: "alarm", lagSeconds: 120 });
		advance(180);
		expect(gate.check({ insertLsn: "0/5000" })).toMatchObject({ state: "alarm", lagSeconds: 300 });
		advance(1);
		let caught: unknown;
		try {
			gate.check({ insertLsn: "0/6000" });
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(RecordArchiveLaggingError);
		const error = caught as RecordArchiveLaggingError;
		expect(error.message).toBe("record archive lagging 301 s; retry with the same key");
		expect(error.code).toBe("RECORD_ARCHIVE_LAGGING");
		expect(error.retryable).toBe(true);
		expect(error.lagSeconds).toBe(301);
		expect(gate.status()).toMatchObject({ state: "refuse", lagSeconds: 301 });
	});

	it("reports lag 0 once the frontier reaches the insert position", async () => {
		const { gate, providers, advance } = setup(["0/1000"]);
		await gate.refresh();
		gate.evaluate({ insertLsn: "0/2000", oldestUncoveredAt: 0 });
		advance(1000);
		providers[0]!.value = "0/2000";
		const refreshed = await gate.refresh();
		expect(refreshed).toMatchObject({ state: "ok", lagSeconds: 0, frontier: "0/2000" });
		expect(gate.evaluate({ insertLsn: "0/2000", oldestUncoveredAt: 0 })).toMatchObject({ state: "ok", lagSeconds: 0 });
	});

	it("counts the fresher of two targets (both orders)", async () => {
		for (const order of [["0/1000", "0/9000"], ["0/9000", "0/1000"]]) {
			const { gate, advance } = setup(order);
			gate.sample("0/1800");
			advance(600);
			gate.sample("0/9800");
			advance(30);
			await gate.refresh();
			expect(gate.evaluate({ insertLsn: "0/A000" })).toMatchObject({ state: "ok", lagSeconds: 30, frontier: "0/9000" });
		}
	});

	it("keeps a failing provider's last good frontier and records the error", async () => {
		const { gate, providers, advance } = setup(["0/5000", "0/1000"]);
		await gate.refresh();
		const goodAt = gate.evaluate({ insertLsn: "0/6000" }).checkedAt;
		providers[0]!.value = new Error("wal-g exploded");
		providers[1]!.value = undefined;
		advance(10);
		await expect(gate.refresh()).resolves.toBeDefined();
		const status = gate.evaluate({ insertLsn: "0/7000" });
		expect(status.frontier).toBe("0/5000");
		expect(status.targets).toEqual([
			{ name: "t0", frontier: "0/5000", checkedAt: goodAt, error: "wal-g exploded" },
			{ name: "t1", frontier: "0/1000", checkedAt: goodAt, error: "no recoverable WAL frontier" },
		]);
		expect(status.lagSeconds).toBe(10);
	});

	it("grows lag from the oldest sample or oldestUncoveredAt when no provider ever succeeded", async () => {
		const { gate, advance, at } = setup([new Error("down")]);
		expect(await gate.refresh()).toBeUndefined();
		gate.sample("0/100");
		advance(200);
		expect(gate.evaluate({ insertLsn: "0/200" })).toMatchObject({ state: "alarm", lagSeconds: 200 });
		expect(gate.status()?.frontier).toBeUndefined();
		expect(gate.evaluate({ insertLsn: "0/300", oldestUncoveredAt: at() - 400_000 })).toMatchObject({
			state: "refuse",
			lagSeconds: 400,
		});
		advance(101);
		expect(gate.evaluate({ insertLsn: "0/400" })).toMatchObject({ state: "refuse", lagSeconds: 301 });
	});

	it("uses the oldest sample above the frontier, not older covered samples", async () => {
		const { gate, advance } = setup(["0/150"]);
		gate.sample("0/100");
		advance(500);
		gate.sample("0/200");
		advance(60);
		await gate.refresh();
		expect(gate.evaluate({ insertLsn: "0/300" }).lagSeconds).toBe(60);
	});

	it("keeps the oldest write time when the sample ring is full", async () => {
		const { gate, advance } = setup(["0/0"], { maxSamples: 2 });
		await gate.refresh();
		for (let index = 1; index <= 10; index += 1) {
			gate.sample(bigIntToLsn(BigInt(index * 0x100)));
			advance(50);
		}
		expect(gate.evaluate({ insertLsn: "0/F00" }).lagSeconds).toBe(500);
	});

	it("passes the previous status to onStatus", async () => {
		const { gate, calls } = setup(["0/1000"]);
		await gate.refresh();
		const first = gate.evaluate({ insertLsn: "0/2000" });
		const second = gate.evaluate({ insertLsn: "0/3000" });
		expect(calls).toEqual([
			[first, undefined],
			[second, first],
		]);
	});

	it("is disabled without providers and never refuses", () => {
		const gate = new AdmissionGate({ providers: [], now: () => 0 });
		expect(gate.enabled).toBe(false);
		gate.sample("0/1", -1_000_000);
		expect(gate.check({ insertLsn: "0/2", oldestUncoveredAt: -1_000_000 })).toMatchObject({
			state: "disabled",
			lagSeconds: 0,
		});
	});
});

describe("WalGFrontierProvider", () => {
	const argv = ["wal-g", "--config", "/etc/walg-m4.yaml", "wal-verify", "integrity", "--json"] as const;
	const report = JSON.stringify({ integrity: { status: "OK", details: [range(seg(1, 0, 1), seg(1, 0, 2))] } });

	it("passes argv and env through, and parses the JSON report", async () => {
		const seen: Array<Parameters<CommandRunner>> = [];
		const run: CommandRunner = async (command, options) => {
			seen.push([command, options]);
			return { stdout: report, stderr: "", code: 0 };
		};
		const provider = new WalGFrontierProvider("m4", argv, { env: { WALG_X: "1" }, timeoutMs: 1234, run });
		expect(provider.name).toBe("m4");
		expect(await provider.frontier()).toBe("0/3000000");
		expect(seen).toEqual([[argv, { env: { WALG_X: "1" }, timeoutMs: 1234 }]]);
		const sized = new WalGFrontierProvider("m4", argv, { segmentSize: 64 * MiB, run });
		expect(await sized.frontier()).toBe("0/C000000");
		expect(seen[1]?.[1]).toEqual({ timeoutMs: 60_000 });
	});

	it("throws with stderr on nonzero exit with unparsable output", async () => {
		const stderr = `ERROR: storage unreachable ${"x".repeat(1000)}`;
		const run: CommandRunner = async () => ({ stdout: "", stderr, code: 1 });
		const error = (await new WalGFrontierProvider("m4", argv, { run }).frontier().catch((cause: unknown) => cause)) as Error;
		expect(error).toBeInstanceOf(Error);
		expect(error.message).toContain("exited 1");
		expect(error.message).toContain("ERROR: storage unreachable");
		expect(error.message).toContain(stderr.slice(0, 500));
		expect(error.message).not.toContain(stderr.slice(0, 501));
	});

	it("runs a real command through the default execFile runner without a shell", async () => {
		const provider = new WalGFrontierProvider("echo", [process.execPath, "-e", `process.stdout.write(${JSON.stringify(report)})`]);
		expect(await provider.frontier()).toBe("0/3000000");
		const failing = new WalGFrontierProvider("fail", [process.execPath, "-e", "console.error('boom $HOME'); process.exit(3)"]);
		await expect(failing.frontier()).rejects.toThrow(/exited 3: boom \$HOME/);
	});
});
