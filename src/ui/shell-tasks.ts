import type { Theme } from "@earendil-works/pi-coding-agent";
import { SelectList, getKeybindings, truncateToWidth, type Component } from "@earendil-works/pi-tui";
import type { FabricShellJobInfo, FabricShellJobStore } from "../core/shell-jobs.js";
import { formatDuration, safeText, wrapPlainText } from "./format.js";

export interface ShellTasksOptions {
  jobs: FabricShellJobStore;
  theme: Theme;
  done: () => void;
  requestRender: () => void;
  rows: () => number;
  id?: string;
}

/** Lazy inspector; the controller owns lifetime, this view owns no Pi runtime. */
export class ShellTasksView implements Component {
  #list: SelectList | undefined;
  #jobs: FabricShellJobInfo[] = [];
  #id: string | undefined;
  #output = "";
  #scroll = 0;
  #stopArmed = 0;
  #reading = false;
  #closed = false;
  #timer: ReturnType<typeof setInterval>;
  #unsubscribe: () => void;

  constructor(readonly options: ShellTasksOptions) {
    this.#id = options.id;
    this.#refresh();
    if (!this.#id && this.#jobs.length === 1) this.#id = this.#jobs[0]!.id;
    this.#unsubscribe = options.jobs.subscribe(() => this.#refresh());
    this.#timer = setInterval(() => this.#refresh(), 1000);
    this.#timer.unref?.();
    void this.#read();
  }

  #refresh(): void {
    if (this.#closed) return;
    const selected = this.#list?.getSelectedItem()?.value;
    this.#jobs = this.options.jobs.list().filter(job => job.finishedAt === undefined || job.spilledAt !== undefined)
      .sort((a, b) => Number(a.finishedAt !== undefined) - Number(b.finishedAt !== undefined) || b.startedAt - a.startedAt);
    const { theme } = this.options;
    const now = Date.now();
    this.#list = new SelectList(this.#jobs.map(job => ({ value: job.id,
      label: `${job.id.slice(0, 8)} ${job.stopping ? "stopping" : job.status} ${formatDuration((job.finishedAt ?? now) - job.startedAt) || "0s"}`,
      description: safeText(job.description ?? job.command).slice(0, 240),
    })), Math.max(1, Math.min(12, this.options.rows() - 5)), {
      selectedPrefix: text => theme.fg("accent", text), selectedText: text => theme.fg("accent", text),
      description: text => theme.fg("muted", text), scrollInfo: text => theme.fg("dim", text), noMatch: text => theme.fg("dim", text),
    });
    const index = this.#jobs.findIndex(job => job.id === selected);
    if (index >= 0) this.#list.setSelectedIndex(index);
    this.#list.onSelect = item => { this.#id = item.value; this.#output = ""; this.#scroll = 0; this.#stopArmed = 0; void this.#read(); this.options.requestRender(); };
    this.#list.onCancel = () => this.options.done();
    void this.#read();
    this.options.requestRender();
  }

  async #read(): Promise<void> {
    const id = this.#id;
    if (!id || this.#reading || this.#closed) return;
    const job = this.options.jobs.get(id);
    if (!job) { this.#output = "Task no longer retained."; return; }
    this.#reading = true;
    try {
      const output = await job.outputText();
      if (!this.#closed && this.#id === id) this.#output = output;
    } catch (error) {
      if (!this.#closed && this.#id === id) this.#output = `Output unavailable: ${safeText(error instanceof Error ? error.message : error)}`;
    } finally { this.#reading = false; if (!this.#closed) this.options.requestRender(); }
  }

  handleInput(data: string): void {
    if (!this.#id) { this.#list?.handleInput(data); return; }
    const keys = getKeybindings();
    if (keys.matches(data, "tui.select.cancel")) { this.#id = undefined; this.#stopArmed = 0; this.#refresh(); return; }
    if (data === "x") {
      const now = Date.now();
      const job = this.options.jobs.get(this.#id);
      if (job && !job.finished) {
        if (this.#stopArmed && now - this.#stopArmed < 3000) { job.stop(); this.#stopArmed = 0; }
        else this.#stopArmed = now;
      }
    } else {
      this.#stopArmed = 0;
      if (keys.matches(data, "tui.select.up")) this.#scroll++;
      if (keys.matches(data, "tui.select.down")) this.#scroll = Math.max(0, this.#scroll - 1);
      if (keys.matches(data, "tui.select.pageUp")) this.#scroll += 10;
      if (keys.matches(data, "tui.select.pageDown")) this.#scroll = Math.max(0, this.#scroll - 10);
      if (data === "G") this.#scroll = 0;
    }
    this.options.requestRender();
  }

  render(width: number): string[] {
    if (width <= 0) return [];
    const { theme } = this.options;
    const height = Math.max(1, Math.floor(this.options.rows() * 0.8));
    let rows = [theme.fg("accent", "Fabric · shell tasks")];
    let hint = "↑↓ select · enter inspect · esc close";
    if (!this.#id) rows.push(...(this.#jobs.length ? this.#list!.render(width) : ["No background shell tasks."]));
    else {
      const job = this.options.jobs.get(this.#id)?.info();
      hint = this.#stopArmed && Date.now() - this.#stopArmed < 3000 ? "Press x again to stop this task · esc back" : "↑↓ scroll output · G tail · x twice stop · esc tasks";
      if (!job) rows.push("Task no longer retained.");
      else {
        const now = Date.now();
        rows.push(`${job.id} · ${job.stopping ? "stopping" : job.status}${job.exitCode !== undefined ? ` · exit ${job.exitCode}` : ""}`);
        rows.push(`Elapsed ${formatDuration((job.finishedAt ?? now) - job.startedAt) || "0s"} · ${job.lastOutputAt ? `last output ${formatDuration(now - job.lastOutputAt) || "0s"} ago` : "no output yet"}${job.pid ? ` · pid ${job.pid}` : ""}`);
        if (job.description) rows.push(safeText(job.description));
        rows.push(`cwd: ${safeText(job.cwd ?? "unknown")}`);
        const commandRows = wrapPlainText(`Command: ${job.command.slice(0, 12000)}`, width, 5);
        rows.push(...commandRows.slice(0, 4));
        if (commandRows.length > 4 || job.command.length > 12000) rows.push("[Command preview clipped; tasks.get returns full text]");
        rows.push(`Log: ${safeText(job.logPath ?? "available after backgrounding")}`);
        if (job.monitor) {
          rows.push(`Monitor: ${job.monitor.delivery === "wake" ? "wake owning agent" : "UI only"} · deadline ${formatDuration(job.monitor.timeoutMs)} · ${job.eventCount} events`);
          if (job.lastEvent) rows.push(`Latest event: ${safeText(job.lastEvent.lines.at(-1))}`);
        }
        rows.push(theme.fg("dim", "Bounded output tail (not a full archive):"));
        const output = this.#output.split("\n").map(safeText);
        const available = Math.max(1, height - rows.length - 1);
        this.#scroll = Math.min(this.#scroll, Math.max(0, output.length - available));
        const end = output.length - this.#scroll;
        rows.push(...output.slice(Math.max(0, end - available), end));
      }
    }
    rows = rows.slice(0, height - 1);
    rows.push(theme.fg("dim", hint));
    return rows.map(row => truncateToWidth(row, width));
  }

  invalidate(): void { this.#list?.invalidate(); }
  dispose(): void {
    if (this.#closed) return;
    this.#closed = true;
    clearInterval(this.#timer);
    this.#unsubscribe();
  }
}
