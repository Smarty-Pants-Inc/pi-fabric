import { stripVTControlCharacters } from "node:util";
import type { Theme } from "@earendil-works/pi-coding-agent";
import { getKeybindings, truncateToWidth, visibleWidth, wrapTextWithAnsi, type Component, type KeybindingsManager } from "@earendil-works/pi-tui";
import type { FabricShellJobInfo, FabricShellJobStore } from "../core/shell-jobs.js";
import { formatDuration, padToWidth, safeText, wrapPlainText } from "./format.js";
import { spinnerFrame } from "./spinner.js";

export interface ShellTasksOptions {
  jobs: FabricShellJobStore;
  theme: Theme;
  done: () => void;
  requestRender: () => void;
  rows: () => number;
  keys?: KeybindingsManager;
  id?: string;
}

const groupNames = ["Active", "Needs attention", "Finished"] as const;
const taskGroup = (job: FabricShellJobInfo): number => job.finishedAt === undefined ? 0
  : job.status === "failed" || job.status === "timed_out" ? 1 : 2;
const elapsed = (job: FabricShellJobInfo): string => formatDuration((job.finishedAt ?? Date.now()) - job.startedAt) || "0s";
const outputLine = (line: string): string => stripVTControlCharacters(line)
  .replace(/\t/g, "  ").replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/g, "");

/** Lazy inspector; the controller owns lifetime, this view owns no Pi runtime. */
export class ShellTasksView implements Component {
  #jobs: FabricShellJobInfo[] = [];
  #selected: string | undefined;
  #listStart = 0;
  #pageSize = 1;
  #id: string | undefined;
  #output = "";
  #scroll = 0;
  #stopArmed: number | undefined;
  #reading = false;
  #readAgain = false;
  #closed = false;
  #timer: ReturnType<typeof setInterval> | undefined;
  #unsubscribe: () => void;

  constructor(readonly options: ShellTasksOptions) {
    this.#id = options.id;
    this.#refresh();
    if (!this.#id && this.#jobs.length === 1) this.#id = this.#jobs[0]!.id;
    this.#selected = this.#id ?? this.#selected;
    this.#unsubscribe = options.jobs.subscribe(event => {
      this.#refresh();
      if (event.job.id === this.#id) void this.#read();
    });
    void this.#read();
  }

  #refresh(): void {
    if (this.#closed) return;
    this.#jobs = this.options.jobs.list().filter(job => job.finishedAt === undefined || job.spilledAt !== undefined)
      .sort((a, b) => taskGroup(a) - taskGroup(b) || b.startedAt - a.startedAt);
    if (!this.#jobs.some(job => job.id === this.#selected)) this.#selected = this.#jobs[0]?.id;
    const active = this.#jobs.some(job => job.finishedAt === undefined);
    // Store events update metadata. Frames sample only the live in-memory tail, never disk logs.
    if (active && !this.#timer) {
      this.#timer = setInterval(() => this.options.requestRender(), 250);
      this.#timer.unref?.();
    } else if (!active && this.#timer) {
      clearInterval(this.#timer);
      this.#timer = undefined;
    }
    this.options.requestRender();
  }

  async #read(): Promise<void> {
    const id = this.#id;
    if (!id || this.#closed) return;
    if (this.#reading) { this.#readAgain = true; return; }
    const job = this.options.jobs.get(id);
    if (!job) { this.#output = "Task no longer retained."; return; }
    this.#reading = true;
    try {
      const output = await job.outputText();
      if (!this.#closed && this.#id === id) this.#output = output;
    } catch (error) {
      if (!this.#closed && this.#id === id) this.#output = `Output unavailable: ${safeText(error instanceof Error ? error.message : error)}`;
    } finally {
      this.#reading = false;
      if (!this.#closed) {
        this.options.requestRender();
        if (this.#readAgain) { this.#readAgain = false; void this.#read(); }
      }
    }
  }

  handleInput(data: string): void {
    if (this.#closed) return;
    const keys = this.options.keys ?? getKeybindings();
    if (keys.matches(data, "tui.select.cancel")) {
      if (!this.#id) { this.dispose(); this.options.done(); return; }
      this.#id = undefined; this.#stopArmed = undefined; this.#refresh(); return;
    }
    if (!this.#id) {
      const index = Math.max(0, this.#jobs.findIndex(job => job.id === this.#selected));
      let next = index;
      if (keys.matches(data, "tui.select.up")) next = index === 0 ? this.#jobs.length - 1 : index - 1;
      else if (keys.matches(data, "tui.select.down")) next = index === this.#jobs.length - 1 ? 0 : index + 1;
      else if (keys.matches(data, "tui.select.pageUp")) next = Math.max(0, index - this.#pageSize);
      else if (keys.matches(data, "tui.select.pageDown")) next = Math.min(this.#jobs.length - 1, index + this.#pageSize);
      else if (keys.matches(data, "tui.select.confirm") && this.#selected) {
        this.#id = this.#selected; this.#output = ""; this.#scroll = 0; this.#stopArmed = undefined;
        void this.#read();
      }
      this.#selected = this.#jobs[next]?.id;
    } else if (data === "x") {
      const now = Date.now();
      const job = this.options.jobs.get(this.#id);
      if (job && !job.finished) {
        if (this.#stopArmed !== undefined && now - this.#stopArmed < 3000) { job.stop(); this.#stopArmed = undefined; }
        else this.#stopArmed = now;
      }
    } else {
      this.#stopArmed = undefined;
      if (keys.matches(data, "tui.select.up")) this.#scroll++;
      if (keys.matches(data, "tui.select.down")) this.#scroll = Math.max(0, this.#scroll - 1);
      if (keys.matches(data, "tui.select.pageUp")) this.#scroll += this.#pageSize;
      if (keys.matches(data, "tui.select.pageDown")) this.#scroll = Math.max(0, this.#scroll - this.#pageSize);
      if (data === "G") this.#scroll = 0;
    }
    this.options.requestRender();
  }

  #status(job: FabricShellJobInfo): string {
    const { theme } = this.options;
    const active = job.finishedAt === undefined;
    const color = job.stopping ? "warning" : active ? "accent" : taskGroup(job) === 1 ? "error" : job.status === "exited" ? "success" : "dim";
    const glyph = active ? spinnerFrame() : taskGroup(job) === 1 ? "✗" : job.status === "exited" ? "✓" : "■";
    return theme.fg(color, `${glyph} ${job.stopping ? "stopping" : job.status}`);
  }

  #section(label: string, width: number): string {
    const title = truncateToWidth(` ${label} `, width);
    return this.options.theme.fg("borderMuted", title + "─".repeat(Math.max(0, width - visibleWidth(title))));
  }

  #overview(width: number, height: number): string[] {
    const { theme } = this.options;
    if (!this.#jobs.length) return [theme.fg("muted", "No background shell tasks."), theme.fg("dim", "Detached commands and monitors appear here.")].slice(0, height);
    const selected = Math.max(0, this.#jobs.findIndex(job => job.id === this.#selected));
    const counts = groupNames.map((_, group) => this.#jobs.filter(job => taskGroup(job) === group).length);
    const compact = height < 4;
    const rowSize = compact ? 1 : 2;
    const window = (start: number): { rows: string[]; end: number } => {
      const rows: string[] = [];
      let previousGroup = -1, end = start;
      for (let index = start; index < this.#jobs.length; index++) {
        const job = this.#jobs[index]!;
        const group = taskGroup(job);
        const heading = !compact && group !== previousGroup;
        if (rows.length + rowSize + Number(heading) > height) break;
        if (heading) rows.push(this.#section(`${groupNames[group]} · ${counts[group]}`, width));
        const chosen = job.id === this.#selected;
        const title = safeText(job.description || job.command) || "Shell task";
        const primary = `${chosen ? theme.fg("accent", "›") : " "} ${this.#status(job)}  ${theme.fg(chosen ? "accent" : "text", title)}`;
        rows.push(chosen ? theme.bg("selectedBg", padToWidth(primary, width)) : truncateToWidth(primary, width));
        if (!compact) {
          const monitor = job.monitor ? ` · monitor:${job.monitor.delivery}` : "";
          const exit = job.exitCode != null ? ` · exit ${job.exitCode}` : "";
          rows.push(theme.fg("dim", `  ${job.id.slice(0, 8)} · ${job.tool} · ${elapsed(job)}${exit}${monitor}`));
        }
        previousGroup = group; end = index + 1;
      }
      return { rows, end };
    };
    this.#listStart = Math.min(this.#listStart, selected);
    let page = window(this.#listStart);
    while (page.end <= selected && this.#listStart < selected) page = window(++this.#listStart);
    this.#pageSize = Math.max(1, page.end - this.#listStart);
    return page.rows;
  }

  #detail(job: FabricShellJobInfo, width: number, height: number): string[] {
    const { theme } = this.options;
    const meta = [
      theme.fg("accent", safeText(job.description || job.command)),
      theme.fg("dim", `${job.lastOutputAt !== undefined ? `last output ${formatDuration(Date.now() - job.lastOutputAt) || "0s"} ago` : "no output yet"}${job.pid ? ` · pid ${job.pid}` : ""}`),
      theme.fg("muted", `cwd: ${safeText(job.cwd ?? "unknown")}`),
      theme.fg("dim", `Log: ${safeText(job.logPath ?? "available after backgrounding")}`),
    ];
    if (job.durable) meta.push(theme.fg("muted", `Durable: jev-fabric ${job.durable.jobId ? `job ${safeText(job.durable.jobId)}` : "job starting"}${job.durable.adopted ? " · reattached" : ""} · survives Pi exit`));
    if (job.monitor) {
      meta.push(theme.fg("muted", `Monitor: ${job.monitor.delivery === "wake" ? "wake owning agent" : "UI only"} · deadline ${formatDuration(job.monitor.timeoutMs)} · ${job.eventCount} events`));
      if (job.lastEvent) meta.push(theme.fg("dim", `Latest event: ${safeText(job.lastEvent.lines.at(-1))}`));
    }
    const rows = meta.slice(0, Math.max(0, height - 6));
    if (height >= 5) {
      rows.push(this.#section("Command", width));
      const commandSpace = Math.max(1, Math.min(3, height - rows.length - 4));
      const command = wrapPlainText(job.command.slice(0, 12000), width, commandSpace + 1);
      const clipped = command.length > commandSpace || job.command.length > 12000;
      rows.push(...command.slice(0, commandSpace).map(line => theme.fg("muted", line)));
      if (clipped) rows[rows.length - 1] = theme.fg("dim", truncateToWidth("… command clipped · tasks.get for full text", width));
    }
    const available = Math.max(1, height - rows.length - 1);
    // append() intentionally emits no store event for every output chunk.
    const text = job.finishedAt === undefined ? this.options.jobs.get(job.id)?.snapshotText(8000) ?? "" : this.#output;
    const output = text ? text.split("\n").flatMap(line => wrapTextWithAnsi(outputLine(line), width)) : [job.finishedAt === undefined ? "Waiting for output…" : "No output captured."];
    this.#scroll = Math.min(this.#scroll, Math.max(0, output.length - available));
    const end = output.length - this.#scroll;
    const start = Math.max(0, end - available);
    this.#pageSize = available;
    rows.push(this.#section(`Output · ${this.#scroll ? `${start + 1}–${end}/${output.length}` : "tail"} · bounded`, width));
    rows.push(...output.slice(start, end).map(line => theme.fg("toolOutput", line)));
    return rows.slice(0, height);
  }

  render(width: number): string[] {
    if (width <= 0) return [];
    const { theme } = this.options;
    const height = Math.max(1, Math.floor(this.options.rows() * 0.8));
    const job = this.#id ? this.options.jobs.get(this.#id)?.info() : undefined;
    const armed = job?.finishedAt === undefined && this.#stopArmed !== undefined && Date.now() - this.#stopArmed < 3000;
    const hint = !this.#id ? "↑↓ select · enter inspect · esc close"
      : armed ? "Press x again to stop this task · esc back"
      : width < 70 ? `↑↓ · G tail${job?.finishedAt === undefined ? " · x×2 stop" : ""} · esc` : `↑↓/pg scroll · G tail${job?.finishedAt === undefined ? " · x twice stop" : ""} · esc tasks`;
    if (width < 8 || height < 7) return ["Fabric · shell tasks", hint].slice(0, height).map(row => truncateToWidth(row, width));
    const inner = width - 4;
    const border = (text: string) => theme.fg("borderMuted", text);
    const row = (text: string) => `${border("│")} ${padToWidth(text, inner)} ${border("│")}`;
    const title = truncateToWidth(this.#id ? `Fabric · task ${this.#id}` : "Fabric · shell tasks", width - 6);
    const top = border("╭─") + ` ${theme.fg("accent", title)} ` + border("─".repeat(Math.max(0, width - visibleWidth(title) - 6)) + "─╮");
    const counts = groupNames.map((name, group) => `${this.#jobs.filter(task => taskGroup(task) === group).length} ${name.toLowerCase()}`);
    const index = this.#jobs.findIndex(task => task.id === this.#selected);
    const summary = this.#id ? job ? `${this.#status(job)} · Elapsed ${elapsed(job)}${job.exitCode !== undefined ? ` · exit ${job.exitCode}` : ""}` : "Task no longer retained."
      : `${counts.join(" · ")}${index >= 0 ? ` · ${index + 1}/${this.#jobs.length}` : ""}`;
    const bodyHeight = height - 6;
    const body = this.#id ? job ? this.#detail(job, inner, bodyHeight) : ["Task no longer retained."] : this.#overview(inner, bodyHeight);
    while (body.length < bodyHeight) body.push("");
    const separator = border(`├${"─".repeat(width - 2)}┤`);
    return [top, row(summary), separator, ...body.map(row), separator, row(theme.fg(armed ? "warning" : "dim", hint)), border(`╰${"─".repeat(width - 2)}╯`)];
  }

  invalidate(): void { /* Theme and width are evaluated on every render. */ }
  dispose(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    this.#unsubscribe();
  }
}
