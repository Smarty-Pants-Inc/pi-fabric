import {
  Editor,
  Loader,
  truncateToWidth,
  visibleWidth,
  type LoaderIndicatorOptions,
  type TUI,
} from "@earendil-works/pi-tui";

/** Streaming status rendered inside an editor's top border. Mirrors Pi's
 *  `WorkingStatusIndicator`: braille frames at 80 ms, plus the same
 *  `renderInBorder` projections `CustomEditor` uses to place them. */
export class BorderWorkingIndicator extends Loader {
  constructor(ui: TUI, colorFn: (text: string) => string, message = "Working", indicator?: LoaderIndicatorOptions) {
    // Pi's embedded indicator paints the spinner and the label with one color:
    // the editor border color, not accent/muted.
    super(ui, colorFn, colorFn, message, indicator);
  }

  renderInBorder(width: number): string {
    const line = this.render(width + 2)[1] ?? "";
    return truncateToWidth(line.startsWith(" ") ? line.slice(1).trimEnd() : line.trimEnd(), width, "");
  }

  renderSpinnerInBorder(width: number): string {
    return truncateToWidth(this.getRenderedIndicator(), width, "");
  }
}

/** Editor that carries the streaming status in its top border, the way Pi's
 *  `CustomEditor` does when `embedWorkingStatus` is on. Pi's host owns the
 *  default editor and opts in there; the Fabric preview owns this editor, so it
 *  opts in by construction. Placement rules (full status, spinner-only when it
 *  cannot fit, and the `↑ N more` scroll label) match Pi's border exactly, and
 *  the status is painted with the editor's own border color — the thinking-level
 *  color, as Pi's embedded indicator is. */
export class BorderStatusEditor extends Editor {
  private workingStatusIndicator: BorderWorkingIndicator | undefined;
  /** Render-time label; width includes its padding. No extra editor rows. */
  topBorderBadge: ((width: number) => string) | undefined;

  setWorkingStatusIndicator(indicator: BorderWorkingIndicator | undefined): void {
    this.workingStatusIndicator = indicator;
  }

  protected override renderTopBorder(width: number, hiddenLineCount: number): string {
    if (!this.topBorderBadge) return this.renderStatusTopBorder(width, hiddenLineCount);
    // Reserve native status/overflow space before asking for a compact target.
    // The base editor still owns content, cursor markers, focus and hit testing.
    const status = this.workingStatusIndicator?.renderInBorder(width) ?? "";
    const reserve = (visibleWidth(status) > 0 ? 6 : 1) +
      (hiddenLineCount > 0 ? visibleWidth(` ↑ ${hiddenLineCount} more `) + 3 : 0);
    const budget = Math.max(0, width - reserve - 2);
    const badge = budget >= 3 ? this.topBorderBadge(budget) : "";
    const badgeWidth = visibleWidth(badge);
    if (!badgeWidth || badgeWidth > budget) return this.renderStatusTopBorder(width, hiddenLineCount);
    return this.renderStatusTopBorder(width - badgeWidth - 2, hiddenLineCount) +
      badge + this.borderColor("──");
  }

  private renderStatusTopBorder(width: number, hiddenLineCount: number): string {
    const indicator = this.workingStatusIndicator;
    if (!indicator || width <= 0) return super.renderTopBorder(width, hiddenLineCount);
    let status = indicator.renderInBorder(Math.max(1, width - 5));
    let statusWidth = visibleWidth(status);
    if (statusWidth === 0) return super.renderTopBorder(width, hiddenLineCount);
    const overflowLabel = hiddenLineCount > 0 ? ` ↑ ${hiddenLineCount} more ` : undefined;
    const overflowLabelWidth = overflowLabel ? visibleWidth(overflowLabel) : 0;
    const overflowStart = Math.floor((width - overflowLabelWidth) / 2);
    const canFitOverflow = (): boolean =>
      overflowLabel !== undefined &&
      overflowLabelWidth + 2 <= width &&
      overflowStart - (3 + statusWidth + 1) >= 1;
    if (overflowLabel && !canFitOverflow()) {
      status = indicator.renderSpinnerInBorder(width);
      statusWidth = visibleWidth(status);
    }
    if (canFitOverflow()) {
      const leftBlockWidth = 3 + statusWidth + 1;
      return this.borderColor("── ") +
        status +
        this.borderColor(
          ` ${"─".repeat(overflowStart - leftBlockWidth)}${overflowLabel}${"─".repeat(width - overflowStart - overflowLabelWidth)}`,
        );
    }
    if (width >= statusWidth + 5) {
      return this.borderColor("── ") + status + this.borderColor(` ${"─".repeat(width - statusWidth - 4)}`);
    }
    status = indicator.renderSpinnerInBorder(width);
    statusWidth = visibleWidth(status);
    const prefixWidth = Math.min(3, Math.max(0, width - statusWidth));
    return this.borderColor("─".repeat(prefixWidth)) +
      status +
      this.borderColor("─".repeat(Math.max(0, width - prefixWidth - statusWidth)));
  }
}
