import type { Component } from "@earendil-works/pi-tui";

const SPINNER_INTERVAL_MS = 250;
const SPINNER_FRAMES = ["◐", "◓", "◑", "◒"] as const;
// Tool renderers get width, not screen coordinates. Leave conservative room
// for the editor, footer and shell padding; this is a tall-preview guard, not
// exact visibility tracking for arbitrary widgets or older transcript cards.
const TOOL_VIEWPORT_RESERVED_ROWS = 12;

export interface AnimationViewportState {
  renderedRows?: number;
}

export const animationFitsViewport = (
  state: AnimationViewportState | undefined,
  terminalRows = process.stdout.rows,
): boolean =>
  state?.renderedRows === undefined || !Number.isFinite(terminalRows) || terminalRows <= 0 ||
  state.renderedRows + TOOL_VIEWPORT_RESERVED_ROWS <= terminalRows;

export const observeAnimationRows = (
  component: Component,
  state: AnimationViewportState,
): Component => ({
  render(width) {
    const lines = component.render(width);
    state.renderedRows = lines.length;
    return lines;
  },
  invalidate() { component.invalidate(); },
});

export interface SpinnerTimerState extends AnimationViewportState {
  timer?: ReturnType<typeof setTimeout>;
  frame?: string;
}

export const spinnerFrame = (now = Date.now()): string =>
  SPINNER_FRAMES[Math.floor(now / SPINNER_INTERVAL_MS) % SPINNER_FRAMES.length]!;

const armSpinner = (state: SpinnerTimerState, invalidate: () => void, now: number): void => {
  if (state.timer) return;
  const delay = SPINNER_INTERVAL_MS - (now % SPINNER_INTERVAL_MS);
  state.timer = setTimeout(() => {
    delete state.timer;
    if (animationFitsViewport(state)) invalidate();
    // Poll only the dimensions while paused: no renderer/highlighter work, and
    // a resize can resume animation without a tool-output event.
    else armSpinner(state, invalidate, Date.now());
  }, delay);
  state.timer.unref?.();
};

export const updateSpinner = (
  state: SpinnerTimerState,
  active: boolean,
  invalidate: () => void,
  now = Date.now(),
): string => {
  if (!active) {
    if (state.timer) clearTimeout(state.timer);
    delete state.timer;
    return spinnerFrame(now);
  }
  if (state.frame === undefined || animationFitsViewport(state)) state.frame = spinnerFrame(now);
  armSpinner(state, invalidate, now);
  return state.frame;
};
