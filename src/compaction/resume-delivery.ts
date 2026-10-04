// Shared, host-free wire contract for the continuation journal and RPC worker.
export const COMPACT_RESUME_ENTRY_TYPE = "fabric-compact-resume";
export const compactResumeMessage = ({ id, resume }: { id: string; resume: string }): string =>
  `Resume after compaction: ${resume}\n\n[Fabric continuation: ${id}]`;
export const compactResumeMessageId = (text: string): string | undefined =>
  /\n\n\[Fabric continuation: ([0-9a-f-]{36})\]$/.exec(text)?.[1];

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;

// appendEntry emits entry_appended before the public settled frame. A
// sendUserMessage deferred by Pi does not emit queue_update yet, so that frame
// alone is not permission for a one-shot worker to close the child's stdin.
export class CompactResumeDelivery {
  readonly #pending = new Set<string>();
  get pending(): boolean { return this.#pending.size > 0; }
  reset(): void { this.#pending.clear(); }

  observe(event: Record<string, unknown>): void {
    const entry = event.type === "entry_appended" ? record(event.entry) : undefined;
    if (entry?.type === "custom" && entry.customType === COMPACT_RESUME_ENTRY_TYPE) {
      const data = record(entry.data);
      if (typeof data?.id !== "string") return;
      if (data.state === "pending" && typeof data.resume === "string" && data.resume.trim()) this.#pending.add(data.id);
      else if (data.state === "cancelled") this.#pending.delete(data.id);
    }
    const message = entry?.type === "message" ? record(entry.message)
      : ["message_start", "message_end"].includes(String(event.type)) ? record(event.message) : undefined;
    if (message?.role !== "user") return;
    const text = typeof message.content === "string" ? message.content : Array.isArray(message.content)
      ? message.content.map(part => record(part)).filter(part => part?.type === "text").map(part => part!.text).join("\n") : "";
    const id = compactResumeMessageId(text);
    if (id) this.#pending.delete(id);
  }
}
