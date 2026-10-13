// Run with bun: the benchmark exercises the same source modules as the tests.
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { encodeKitty, Text, TuiMainScreen } from "@earendil-works/pi-tui";
import { KittyViewport } from "../src/ui/kitty-viewport.ts";
import { retainImageSafeOverlays } from "../src/ui/image-overlays.ts";

const png = Buffer.alloc(24);
png.writeUInt32BE(0x89504e47, 0);
png.writeUInt32BE(800, 16);
png.writeUInt32BE(1600, 20);
const payload = png.toString("base64") + "A".repeat(4 * 1024 * 1024);
const image = encodeKitty(payload, { imageId: 42, rows: 40, columns: 40, moveCursor: false });
const imageSource = [image, ...Array(39).fill("")];
const blankSource = ["history", ...Array(100_000).fill("")];
const imageViewport = new KittyViewport();
const blankViewport = new KittyViewport();
const textSource = Array.from({ length: 30_000 }, (_, i) => `history ${i} ${"x".repeat(60)}`);
const terminal = { columns: 100, rows: 30, write() {}, hideCursor() {}, showCursor() {}, stop() {} };
const tui = new TuiMainScreen(terminal);
const overlay = tui.showOverlay(new Text("Dashboard"), { row: 2, width: 90 });
const native = tui.compositeOverlays.bind(tui);
const release = retainImageSafeOverlays(tui);
const protectedComposite = tui.compositeOverlays.bind(tui);
function measure(run, iterations) {
  for (let i = 0; i < 10; i++) run(i);
  const samples = [];
  for (let i = 0; i < iterations; i++) {
    const before = performance.now();
    run(i);
    samples.push(performance.now() - before);
  }
  samples.sort((a, b) => a - b);
  return { medianMs: +samples[Math.floor(samples.length / 2)].toFixed(4), p95Ms: +samples[Math.floor(samples.length * 0.95)].toFixed(4), samples: iterations };
}
try {
  const cases = {
    unchanged4MiBImageCrop: () => {
      assert.equal(imageViewport.slice(imageSource, 3, 23).imageRows.size, 20);
    },
    scrolling4MiBImageCrop: (i) => {
      assert.equal(imageViewport.slice(imageSource, 1 + i % 10, 21 + i % 10).imageRows.size, 20);
    },
    scrollBlank100kHistory: (i) => {
      const start = 99_000 + i % 100;
      assert.equal(blankViewport.slice(blankSource, start, start + 30).lines.length, 30);
    },
    nativeTextOverlay30k: () => { native(textSource, 100, 30); },
    protectedTextOverlay30k: () => { protectedComposite(textSource, 100, 30); },
  };
  console.log(JSON.stringify({ runtime: { node: process.version, bun: process.versions.bun }, cases: Object.fromEntries(Object.entries(cases).map(([name, run]) => [name, measure(run, name.includes("4MiB") ? 60 : 120)])) }, null, 2));
} finally { overlay.hide(); release(); tui.stop(); }
