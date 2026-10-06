import type { Terminal } from "@earendil-works/pi-tui";

export const PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j5V8AAAAASUVORK5CYII=";
export class KittyTerminal implements Terminal {
  columns = 80;
  rows = 24;
  kittyProtocolActive = true;
  output = "";
  input: (data: string) => void = () => {};
  readonly placements = new Set<number>();
  private pending = "";
  start(onInput: (data: string) => void): void { this.input = onInput; }
  stop(): void { this.input = () => {}; }
  async drainInput(): Promise<void> {}
  write(data: string): void {
    this.output += data;
    this.pending += data;
    for (;;) {
      const start = this.pending.indexOf("\x1b_G");
      if (start < 0) { this.pending = ""; break; }
      const end = this.pending.indexOf("\x1b\\", start);
      if (end < 0) { this.pending = this.pending.slice(start); break; }
      const controls = Object.fromEntries(this.pending.slice(start + 3, end).split(";")[0]!.split(",").map((part) => part.split("=")));
      const id = Number(controls.i);
      if ((controls.a === "T" || controls.a === "p") && id) this.placements.add(id);
      if (controls.a === "d") {
        if (controls.d?.toLowerCase() === "a") this.placements.clear();
        if (controls.d?.toLowerCase() === "i") this.placements.delete(id);
      }
      this.pending = this.pending.slice(end + 2);
    }
  }
  moveBy(): void {}
  hideCursor(): void {}
  showCursor(): void {}
  clearLine(): void {}
  clearFromCursor(): void {}
  clearScreen(): void {}
  setTitle(): void {}
  setProgress(): void {}
}
