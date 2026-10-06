import { encodeKitty } from "@earendil-works/pi-tui";
import * as tui from "@earendil-works/pi-tui";
import { describe, expect, it, vi } from "vitest";
import { hideKittyImages, KittyViewport } from "../src/ui/kitty-viewport.js";

vi.mock("@earendil-works/pi-tui", async original => {
  const host = await original<typeof import("@earendil-works/pi-tui")>();
  return { ...host, getPngDimensions: vi.fn(host.getPngDimensions) };
});

const sliceKittyViewport = (source: readonly string[], start: number, end: number) => new KittyViewport().slice(source, start, end).lines;
const kittyImageRows = (source: readonly string[]) => new KittyViewport().slice(source, 0, source.length).imageRows;

// Only the PNG header is consumed by the cropper; the large suffix exercises
// chunk preservation without decoding or allocating a bitmap.
const png = Buffer.alloc(24);
png.writeUInt32BE(0x89504e47, 0);
png.writeUInt32BE(80, 16);
png.writeUInt32BE(160, 20);
const payload = png.toString("base64") + "A".repeat(9000);
const image = encodeKitty(payload, { imageId: 42, rows: 8, columns: 8, moveCursor: false });
const source = ["before", image, ...Array<string>(7).fill(""), "after", "tail"];
const controls = (line: string) => Object.fromEntries(/\x1b_G([^;]*);/.exec(line)![1]!.split(",").map((part) => part.split("=")));
const data = (line: string) => [...line.matchAll(/\x1b_G[^;]*;([^\x1b]*)\x1b\\/g)].map((match) => match[1]).join("");

describe("Kitty conversation viewport", () => {
  it("crops top, bottom and both edges without rescaling or changing transmissions", () => {
    for (const [start, end, y, h, r] of [[1, 5, 0, 80, 4], [4, 9, 60, 100, 5], [4, 6, 60, 40, 2]]) {
      const result = sliceKittyViewport(source, start!, end!);
      expect(controls(result[0]!)).toMatchObject({ i: "42", y: String(y), h: String(h), r: String(r), c: "8", C: "1", m: "1" });
      expect(data(result[0]!)).toBe(payload);
      expect(result.slice(1).every((line) => line === "")).toBe(true);
    }
    expect(source[1]).toBe(image);
    expect(sliceKittyViewport(source, 0, source.length)).toEqual(source);
    expect(sliceKittyViewport(source, 9, 11)).toEqual(["after", "tail"]);
  });

  it("covers every scroll position and short/tall viewport without stale images", () => {
    for (const height of [1, 2, 5, 8, 30]) for (let start = 0; start <= source.length; start++) {
      const result = sliceKittyViewport(source, start, start + height);
      const overlap = Math.max(0, Math.min(9, start + height) - Math.max(1, start));
      const images = result.filter((line) => line.includes("\x1b_G"));
      expect(images).toHaveLength(overlap ? 1 : 0);
      if (overlap) expect(Number(controls(images[0]!).r)).toBe(overlap);
      expect(kittyImageRows(result).size).toBe(overlap);
    }
  });

  it("handles multiple images and leaves text/other protocols unchanged", () => {
    const result = sliceKittyViewport([...source.slice(0, 9), image, ...source.slice(2)], 7, 12);
    expect(result.filter((line) => line.includes("\x1b_G"))).toHaveLength(2);
    expect([...kittyImageRows(result)]).toEqual([0, 1, 2, 3, 4]);
    const text = ["plain", "\x1b[31mred\x1b[0m", "\x1b]1337;File=abc\x07", ""];
    expect(sliceKittyViewport(text, 1, 4)).toEqual(text.slice(1, 4));
    expect(sliceKittyViewport(["\x1b_Ga=T,f=100,r=9;invalid\x1b\\", ""], 0, 1)).toEqual([""]);
  });

  it("indexes retained history once and reads only visible rows while scrolling", () => {
    let reads = 0;
    const retained = new Proxy(Object.freeze(["old", ...Array<string>(100_000).fill("")]), {
      get(target, key, receiver) {
        if (typeof key === "string" && /^\d+$/.test(key)) reads++;
        return Reflect.get(target, key, receiver);
      },
    });
    const viewport = new KittyViewport();
    viewport.slice(retained, 90_000, 90_030);
    reads = 0;
    for (let i = 0; i < 30; i++) expect(viewport.slice(retained, 90_000 + i, 90_030 + i).lines).toHaveLength(30);
    expect(reads).toBe(30 * 30);
    viewport.clear();
    reads = 0;
    viewport.slice(retained, 90_000, 90_030);
    expect(reads).toBeGreaterThan(100_000);
  });

  it("decodes only a PNG header once per retained image and bounds cropped payload retention", () => {
    const decode = vi.mocked(tui.getPngDimensions);
    decode.mockClear();
    const viewport = new KittyViewport();
    try {
      const first = viewport.slice(source, 3, 5);
      first.lines.fill("caller mutation");
      for (let i = 0; i < 30; i++) {
        const cropped = viewport.slice(source, 3, 5);
        expect(cropped.lines[0]).toContain("i=42");
        expect(cropped.imageRows.size).toBe(2);
      }
      viewport.slice(source, 4, 6);
      expect(decode).toHaveBeenCalledTimes(1);
      expect(decode.mock.calls[0]![0]).toHaveLength(32);
      const many = Array.from({ length: 5 }, (_, id) => [encodeKitty(payload, { imageId: id + 1, rows: 8 }), ...Array<string>(7).fill("")]).flat();
      const cache = viewport as unknown as { crops: Map<unknown, unknown>; cropBytes: number };
      for (let i = 0; i < 5; i++) {
        viewport.slice(many, i * 8 + 1, i * 8 + 3);
        expect(cache.crops.size).toBeLessThanOrEqual(2);
        expect(cache.cropBytes).toBeLessThanOrEqual(16 * 1024 * 1024);
      }
      const huge = [encodeKitty(png.toString("base64") + "A".repeat(8 * 1024 * 1024), { imageId: 99, rows: 2 }), ""];
      viewport.slice(huge, 1, 2);
      expect(cache.crops.size).toBe(0); // over-budget crops are rendered, not retained
      expect(cache.cropBytes).toBe(0);
      viewport.clear();
      expect(cache.crops.size).toBe(0);
    } finally { decode.mockClear(); }
  });

  it("reindexes replacement frames and never lets caller painting mutate retained history", () => {
    const viewport = new KittyViewport();
    viewport.slice(source, 3, 5).lines.fill("paint");
    expect(viewport.slice(source, 3, 5).lines[0]).toContain("i=42");
    const replaced = Array<string>(source.length).fill("new text");
    expect(viewport.slice(replaced, 3, 5)).toEqual({ lines: ["new text", "new text"], imageRows: new Set() });
    expect(viewport.slice(source, 3, 5).imageRows.size).toBe(2);
  });

  it("leaves offscreen images intact and filters a placement anchored above the viewport", () => {
    const history = [image, ...Array<string>(7).fill(""), ...Array<string>(100).fill("history"), image, ...Array<string>(7).fill("")];
    const hidden = hideKittyImages(history, 4);
    expect(hidden).not.toBe(history);
    expect(hidden[0]).toBe(image);
    expect(hidden[108]).toBe("");
    expect(history[108]).toBe(image);
    expect(hideKittyImages(history, 0)).toBe(history);
    const text = Array<string>(100_000).fill("text");
    let reads = 0;
    const proxy = new Proxy(text, { get(target, key, receiver) {
      if (typeof key === "string" && /^\d+$/.test(key)) reads++;
      return Reflect.get(target, key, receiver);
    } });
    expect(hideKittyImages(proxy, 30)).toBe(proxy);
    expect(reads).toBe(31);
    text[text.length - 1] = image; // mutable background arrays are deliberately not cached
    expect(hideKittyImages(proxy, 30).at(-1)).toBe("");
  });
});
