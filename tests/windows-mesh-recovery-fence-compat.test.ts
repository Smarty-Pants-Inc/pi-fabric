import type fs from "node:fs";
import { createHash } from "node:crypto";
import { expect, it } from "vitest";
import { encodeMeshOwnerRecoveryRequest, windowsMeshRecoveryFence } from "../src/storage/windows-mesh-recovery.js";

// Pure recorded ABI regression, not native Windows evidence. No filesystem or child operations.
const record = "11111111-2222-3333-4444-555555555555\n42\n123\n456\nrecorded-namespace\nnonce\n";
const stat = (directory: boolean, dev: bigint, ino: bigint): fs.BigIntStats => ({
  dev, ino, size: directory ? 0n : BigInt(Buffer.byteLength(record)), mtimeNs: 123_456_700n,
  isDirectory: () => directory, isFile: () => !directory, isSymbolicLink: () => false,
} as fs.BigIntStats);
const digest = (preimage: string): string => createHash("sha256").update(preimage).digest("hex");

it("keeps legacy Number-Stats F names but exact original bigint S/full R in framed C requests", () => {
  const cases = [
    { lock: "C:\\mesh\\.lock", dev: 9_007_199_254_740_993n, ino: 9_007_199_254_740_995n,
      decimal: "9007199254740992:9007199254740996", hex: ["20000000000001", "20000000000003"], rounded: true },
    { lock: "D:\\mesh\\custody.lock", dev: 9_007_199_254_740_993n, ino: 9_007_199_254_740_995n,
      decimal: "9007199254740992:9007199254740996", hex: ["20000000000001", "20000000000003"], rounded: true },
    { lock: "C:\\mesh\\.lock", dev: 123n, ino: 456n,
      decimal: "123:456", hex: ["7b", "1c8"], rounded: false },
  ];
  for (const sample of cases) {
    const directory = stat(true, sample.dev, sample.ino);
    const owner = stat(false, 9_007_199_254_740_997n, 0xffff_ffff_ffff_ffffn);
    // Empty R is the E name; nonempty R hashes every original byte, including the final newline.
    for (const fullR of ["", record]) {
      const expected = `${sample.lock}.dead.${digest(`${sample.decimal}:${fullR}`)}`;
      expect(windowsMeshRecoveryFence(sample.lock, directory, fullR)).toBe(expected);
      expect(expected.slice(`${sample.lock}.dead.`.length)).toMatch(/^[0-9a-f]{64}$/);
      const exactBigintName = `${sample.lock}.dead.${digest(`${directory.dev}:${directory.ino}:${fullR}`)}`;
      expect(expected === exactBigintName).toBe(!sample.rounded);
    }
    expect(windowsMeshRecoveryFence(sample.lock, directory, record.trimEnd()))
      .not.toBe(windowsMeshRecoveryFence(sample.lock, directory, record));

    const message = encodeMeshOwnerRecoveryRequest(sample.lock, record, directory, owner, 1234);
    expect(message.subarray(0, 5).toString("ascii")).toBe("FMRC1");
    const fields: string[] = [];
    let offset = 5;
    while (offset < message.length) {
      const lengthHex = message.subarray(offset, offset + 8).toString("ascii");
      expect(lengthHex).toMatch(/^[0-9a-f]{8}$/);
      const length = Number.parseInt(lengthHex, 16);
      offset += 8;
      expect(offset + length).toBeLessThanOrEqual(message.length);
      fields.push(message.subarray(offset, offset + length).toString("utf8"));
      offset += length;
    }
    expect(offset).toBe(message.length);
    expect(fields).toEqual([
      `\\\\?\\${sample.lock}`,
      `\\\\?\\${sample.lock}.dead.${digest(`${sample.decimal}:${record}`)}`,
      ...sample.hex, "20000000000005", "ffffffffffffffff",
      (123_456_700n / 100n + 116_444_736_000_000_000n).toString(16),
      BigInt(Buffer.byteLength(record)).toString(16), record, "1234",
    ]);
  }
});
