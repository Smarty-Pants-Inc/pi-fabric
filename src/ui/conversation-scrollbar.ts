import type { Theme } from "@earendil-works/pi-coding-agent";
import { ScrollView, compositeTuiLine, sliceByColumn, stripTerminalSequences, visibleWidth, type ScrollViewScrollbar, type TuiMouseEvent } from "@earendil-works/pi-tui";

const graphemes = new Intl.Segmenter(undefined, { granularity: "grapheme" });

/** Pi 1.0 layout.ts scrollbar painting and tui-alt-screen.ts pointer geometry,
 * adapted to the participant overlay (native scrollbar hit testing skips overlays).
 * ScrollView owns the native visibility timer; conversation state owns history. */
export class ConversationScrollbar {
  private readonly view: ScrollView;
  private width = 0;
  private top = 0;
  private contentHeight = 0;
  private grabOffset: number | undefined;

  constructor(theme: Theme, mode: ScrollViewScrollbar = "auto") {
    this.view = new ScrollView({ render: () => [], invalidate: () => {} }, {
      follow: "end", scrollbar: mode,
      scrollbarTrackStyle: (text) => theme.fg("scrollbarTrack", text),
      scrollbarThumbStyle: (text) => theme.fg("scrollbarThumb", text),
    });
  }

  contentWidth(width: number): number { return this.view.getContentWidth(width); }
  get visible(): boolean { return this.view.isScrollbarVisible; }

  sync(width: number, top: number, height: number, contentHeight: number, scroll: number, following: boolean, requestRender: () => void): void {
    this.width = width;
    this.top = top;
    this.contentHeight = contentHeight;
    this.view.updateLayout(contentHeight, height, requestRender);
    this.view.scrollTo(scroll, { disableFollow: !following });
  }

  reset(): void {
    this.grabOffset = undefined;
    this.view.updateLayout(0, 0, () => {});
    this.view.setScrollbarActive(false);
    this.view.scrollToEnd();
  }

  private geometry(includeHidden = false) {
    const height = this.view.viewportHeight;
    if (this.width <= 0 || height <= 0 || (!this.view.isScrollbarVisible &&
      !(includeHidden && this.view.scrollbar === "auto" && this.contentHeight > height))) return undefined;
    const thumbHeight = Math.max(Math.min(2, height), Math.min(height, Math.round(height * height / this.contentHeight)));
    const maxScroll = Math.max(0, this.contentHeight - height);
    const thumbTop = maxScroll === 0 ? 0 : Math.round(this.view.scrollTop / maxScroll * (height - thumbHeight));
    return { height, thumbHeight, thumbTop, maxScroll };
  }

  handleMouse(event: TuiMouseEvent, scrollTo: (position: number) => void): boolean {
    const geometry = this.geometry(true);
    const inTrack = !!geometry && event.x === this.width - 1 && event.y >= this.top && event.y < this.top + geometry.height;
    if (this.grabOffset !== undefined) {
      if (event.type === "release") {
        this.grabOffset = undefined;
        this.view.setScrollbarActive(inTrack);
      } else if (geometry) this.move(event.y, geometry, scrollTo);
      return true;
    }
    const visible = this.view.isScrollbarVisible;
    this.view.setScrollbarActive(inTrack);
    if (!geometry || !inTrack || !visible || event.type !== "press" || event.button !== "left") return false;
    const row = event.y - this.top;
    const onThumb = row >= geometry.thumbTop && row < geometry.thumbTop + geometry.thumbHeight;
    this.grabOffset = onThumb ? row - geometry.thumbTop : Math.floor(geometry.thumbHeight / 2);
    if (!onThumb) this.move(event.y, geometry, scrollTo);
    return true;
  }

  private move(y: number, geometry: { height: number; thumbHeight: number; maxScroll: number }, scrollTo: (position: number) => void): void {
    const maxOffset = geometry.height - geometry.thumbHeight;
    const offset = Math.max(0, Math.min(maxOffset, y - this.top - this.grabOffset!));
    scrollTo(maxOffset === 0 ? 0 : Math.round(offset / maxOffset * geometry.maxScroll));
  }

  paint(lines: string[], imageRows?: ReadonlySet<number>): void {
    const geometry = this.geometry();
    if (!geometry) return;
    for (let row = 0; row < geometry.height; row++) {
      if (imageRows?.has(row)) continue;
      let line = lines[row] ?? "";
      if (/\x1b(?:_G|\]1337;File=|P)/.test(line)) continue;
      if (this.view.scrollbar === "always" && this.width > 1 && row < this.contentHeight - this.view.scrollTop) {
        line = compositeTuiLine("", line, 0, this.width - 1, this.width);
      }
      const thumb = row >= geometry.thumbTop && row < geometry.thumbTop + geometry.thumbHeight;
      const glyph = thumb ? this.view.scrollbarThumbStyle(this.view.isScrollbarActive ? "█" : "┃") : this.view.scrollbarTrackStyle("│");
      const column = this.width - 1;
      let start = column;
      let end = column + 1;
      let position = 0;
      for (const { segment } of graphemes.segment(stripTerminalSequences(line))) {
        const next = position + visibleWidth(segment);
        if (position <= column && column < next) { start = position; end = next; break; }
        position = next;
      }
      const before = sliceByColumn(line, 0, start, true);
      const target = sliceByColumn(line, start, end - start, true);
      const prefix = target.match(/^(?:\x1b\[[0-9;]*m|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\))*/)?.[0] ?? "";
      const background = this.view.scrollbar === "always" ? "" : activeBackground(prefix);
      lines[row] = before + " ".repeat(Math.max(0, start - visibleWidth(before))) + "\x1b[0m\x1b]8;;\x07" + background +
        " ".repeat(column - start) + glyph + " ".repeat(end - column - 1) +
        sliceByColumn(line, end, Math.max(0, this.width - end), true);
    }
  }
}

function activeBackground(prefix: string): string {
  let background = "";
  for (const match of prefix.matchAll(/\x1b\[([0-9;]*)m/g)) {
    const parts = match[1]!.split(";").map(Number);
    for (let i = 0; i < parts.length; i++) {
      const code = parts[i]!;
      if (code === 0 || code === 49) background = "";
      else if ((code >= 40 && code <= 47) || (code >= 100 && code <= 107)) background = `\x1b[${code}m`;
      else if (code === 38 || code === 48 || code === 58) {
        const size = parts[i + 1] === 2 ? 5 : parts[i + 1] === 5 ? 3 : 1;
        if (code === 48) background = `\x1b[${parts.slice(i, i + size).join(";")}m`;
        i += size - 1;
      }
    }
  }
  return background;
}
