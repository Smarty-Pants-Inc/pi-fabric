import { afterEach, describe, expect, it, vi } from "vitest";
import { animationFitsViewport, observeAnimationRows, spinnerFrame, type AnimationViewportState, type SpinnerTimerState, updateSpinner } from "../src/ui/spinner.js";

describe("spinner", () => {
  afterEach(() => vi.useRealTimers());

  it("uses the widget frame sequence at 250ms intervals", () => {
    expect([0, 250, 500, 750, 1_000].map(spinnerFrame)).toEqual(["◐", "◓", "◑", "◒", "◐"]);
  });

  it("reserves space below the preview without guessing when terminal height is unknown", () => {
    expect(animationFitsViewport({ renderedRows: 18 }, 30)).toBe(true);
    expect(animationFitsViewport({ renderedRows: 19 }, 30)).toBe(false);
    expect(animationFitsViewport({ renderedRows: 40 }, 80)).toBe(true);
    expect(animationFitsViewport({ renderedRows: 40 }, Number.NaN)).toBe(true);
    expect(animationFitsViewport({ renderedRows: 40 }, 0)).toBe(true);
    expect(animationFitsViewport(undefined, 30)).toBe(true);
  });

  it("measures rendered rows after wrapping and forwards invalidation", () => {
    const state: AnimationViewportState = {};
    const child = {
      render: (width: number) => width < 20 ? ["first", "second"] : ["first second"],
      invalidate: vi.fn(),
    };
    const component = observeAnimationRows(child, state);
    expect(component.render(10)).toEqual(["first", "second"]);
    expect(state.renderedRows).toBe(2);
    expect(component.render(40)).toEqual(["first second"]);
    expect(state.renderedRows).toBe(1);
    component.invalidate();
    expect(child.invalidate).toHaveBeenCalledOnce();
  });

  it("freezes the last frame without invalidating, resumes after resize, and cancels its paused timer", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const originalRows = Object.getOwnPropertyDescriptor(process.stdout, "rows");
    let rows = 30;
    Object.defineProperty(process.stdout, "rows", { configurable: true, get: () => rows });
    try {
      const invalidate = vi.fn();
      const state: SpinnerTimerState = { renderedRows: 2 };
      expect(updateSpinner(state, true, invalidate)).toBe("◐");
      vi.advanceTimersByTime(250);
      expect(updateSpinner(state, true, invalidate)).toBe("◓");
      state.renderedRows = 40;
      vi.advanceTimersByTime(1_000);
      expect(invalidate).toHaveBeenCalledOnce();
      expect(updateSpinner(state, true, invalidate)).toBe("◓");
      expect(vi.getTimerCount()).toBe(1);
      rows = 80;
      vi.advanceTimersByTime(250);
      expect(invalidate).toHaveBeenCalledTimes(2);
      expect(updateSpinner(state, true, invalidate)).toBe("◑");
      rows = 30;
      vi.advanceTimersByTime(250);
      updateSpinner(state, false, invalidate);
      expect(vi.getTimerCount()).toBe(0);
      vi.advanceTimersByTime(1_000);
      expect(invalidate).toHaveBeenCalledTimes(2);
    } finally {
      if (originalRows) Object.defineProperty(process.stdout, "rows", originalRows);
      else Reflect.deleteProperty(process.stdout, "rows");
    }
  });

  it("keeps only one timer active and stops invalidating after completion", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const invalidate = vi.fn();
    const state: SpinnerTimerState = {};

    expect(updateSpinner(state, true, invalidate)).toBe("◐");
    const timer = state.timer;
    expect(timer).toBeDefined();
    expect(updateSpinner(state, true, invalidate)).toBe("◐");
    expect(state.timer).toBe(timer);

    vi.advanceTimersByTime(250);
    expect(invalidate).toHaveBeenCalledOnce();
    expect(state.timer).toBeUndefined();
    expect(updateSpinner(state, true, invalidate)).toBe("◓");

    updateSpinner(state, false, invalidate);
    expect(state.timer).toBeUndefined();
    vi.advanceTimersByTime(1_000);
    expect(invalidate).toHaveBeenCalledOnce();
  });
});
