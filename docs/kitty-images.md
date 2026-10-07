# Kitty images in Fabric conversations and overlays

## Other Pi extensions examined

These are pinned source snapshots of the public repositories' HEADs at review time,
not claims that every terminal behaves identically:

- [pi-traceline's peek pager](https://github.com/tmustier/pine-of-glass/blob/6e7dd5fd613198fee9fff71df1bc45a579030cce/extensions/pi-traceline/drill-pager.ts#L125-L179)
  caches rendered bodies and Image components. Images are atomic: a partly visible
  block stays a placeholder to avoid painting over the pager's chrome. It
  deletes its own image IDs on disposal. This is a safe alternative to cropping,
  but would make Fabric's partially scrolled images disappear by design.
- [screenshots-picker](https://github.com/thegalexc/pi-extensions-oss/blob/d9d248ce7fbdeff362ef88394fa8372cf9220815/extensions/screenshots-picker/index.ts#L1113-L1255)
  uses a stable image ID, cached thumbnails/dimensions, and explicit source-pixel
  crops. Thumbnail changes delete the previous placement; its zoom inspector
  deletes/retransmits on each render to prevent stacking.
- [pi-craft preview](https://github.com/TudeOrangBiasa/pi-craft/blob/a868030b50ac49c4b4c7cb1dabfd194ed0b63816/extensions/preview/index.ts#L2260-L2300)
  tracks IDs per page and explicitly deletes those IDs when rebuilding/closing.
- [codex-ui-gallery](https://github.com/gwelinder/pi-extension-pack/blob/f7a35faf47e66a636a651162a1dc5cd8268f714f/extensions/codex-ui-gallery/index.ts#L219-L252)
  deletes its owned image on zoom/close. It also exposes a manual
  `codex-gallery-clear` command that deletes all terminal images.

None of these inspected implementations provided a reusable public hook to fix
Pi's base-conversation/overlay compositor. We retain a scoped Pi 1.0 compatibility
adapter without installing a permanent global patch or copying an extension's
out-of-band delete loop. The adapter is active only during Fabric custom UI,
reference-counted across stacked views, and restored even after factory failures.

## Fabric's approach

- Retain native Image components and IDs. Crop using `y`, `h`, and `r`; preserve
  transmission chunks, image data, and the original retained transcript.
- Keep reserved image rows empty. Text selection, scrollbars, and overlay padding
  must not trigger line-clears through a placement after it has been drawn.
- Suppress background image commands before native compositing. Preserve images
  wholly outside the visible terminal; do not delete scrollback to mask a
  panel. The native TUI still owns differential deletion, uploads, and restoration.
- Do **not** manually delete all images each frame: this can invalidate Pi's
  fullscreen upload cache and cause flicker or expensive retransmissions. Pi's
  regular renderer may still retransmit on scroll/full redraw; this extension
  does not replace its upload policy.

## Performance contracts

`src/ui/conversation-render.ts` keeps the same immutable retained frame while
native/dynamic row output is unchanged. Dynamic components still run and can
update every frame; their output arrays are snapshotted to detect in-place
mutation. Ordinary renderer callers receive defensive copies. Only the
conversation view explicitly borrows the readonly frame.

`src/ui/kitty-viewport.ts` indexes image spans once per retained frame. Warm
scrolling uses binary search and a viewport-sized slice, not a backward scan
through retained blank history. Cropping parses only the first 32 base64
characters of a PNG header, once per indexed image, and never decodes pixels.
The cache holds at most two cropped transmissions and 16 MiB of estimated UTF-16
string storage; oversized crops are rendered without retention. Replacement
frames, resize, and disposal release the previous index/cache.

Overlay filtering checks the visible rows and, when needed, the preceding blank
reservation run for an image spanning the viewport edge. It returns text-only
background arrays unchanged. Background arrays are not identity-cached because
third-party components may mutate them in place.

Run the deterministic correctness/performance guards:

```sh
bunx vitest run tests/kitty-viewport.test.ts tests/conversation-kitty.test.ts tests/image-overlays.test.ts tests/conversation-render-cache.test.ts
bun run benchmark:kitty
```

The benchmark reports warm median/p95 latency for 4 MiB image crops, scrolling
100,000 blank history rows, and native versus protected overlays over 30,000
text rows. These are component microbenchmarks, **not terminal latency or whole
session startup**. Use same-machine before/after runs; no timing threshold is
used in CI. Tests instead assert bounded row reads/cache bytes, stable retained
frames, no idle graphics commands, and native fullscreen upload-cache reuse.

Observed on the same macOS arm64 machine with Bun 1.4.2, comparing the initial
uncached image fix with the retained-frame implementation (warm medians, ms):

| Workload | Before | After |
| --- | ---: | ---: |
| Unchanged 4 MiB crop | 0.2079 | 0.0022 |
| Scrolling 4 MiB crop | 0.1895 | 0.0041 |
| Scrolling 100,000 blank rows | 0.5903 | 0.0002 |
| Protected overlay over 30,000 text rows | 0.6819 | 0.0929 |

The separate native-overlay control varies with JIT/GC and host load; these
numbers demonstrate removal of repeated work, not a promised UI frame rate.
