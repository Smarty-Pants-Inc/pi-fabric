// Pi repeats the entire history in agent_end.messages and the current tool
// results in turn_end.toolResults. These top-level fields are not consumed by
// the worker or transcript reader; authoritative messages arrive separately.
// Elide them *before* buffering a JSONL record, not after JSON.parse. This keeps
// image-heavy sessions bounded without dropping lifecycle flags after history.
// This is a lexical projection, not a JSON validator. Retained JSON is parsed
// by the worker. Discarded values are never accumulated; key tokens are bounded.
export class PiEventProjection {
  #depth = 0;
  #inString = false;
  #escaped = false;
  #expectKey = false;
  #readingKey = false;
  #keyToken = "";
  #key = "";
  #awaitingValue = false;
  #skipping = false;

  write(text: string): string {
    const parts: string[] = [];
    let start = 0;
    for (let i = 0; i < text.length; i++) {
      const char = text[i]!;
      // Only LF frames records. CR is retained for the worker's CRLF handling;
      // U+2028/U+2029 and escaped newlines are ordinary JSON string contents.
      if (char === "\n") {
        if (this.#skipping) {
          // An incomplete discarded value must not manufacture a valid event.
          parts.push("!\n");
          start = i + 1;
        }
        this.#reset();
        continue;
      }
      if (this.#inString) {
        if (this.#readingKey && this.#keyToken.length < 256) this.#keyToken += char;
        if (this.#escaped) {
          this.#escaped = false;
        } else if (char === "\\") {
          this.#escaped = true;
        } else if (char === '"') {
          this.#inString = false;
          if (this.#readingKey) {
            try {
              this.#key = JSON.parse(this.#keyToken) as string;
            } catch {
              this.#key = "";
            }
            this.#readingKey = false;
          }
        }
        continue;
      }
      if (this.#depth === 1 && this.#awaitingValue && !/[ \t\r]/.test(char)) {
        this.#awaitingValue = false;
        if (char === "[" && (this.#key === "messages" || this.#key === "toolResults")) {
          parts.push(text.slice(start, i), "[]");
          this.#skipping = true;
        }
      }
      if (char === '"') {
        this.#inString = true;
        this.#readingKey = this.#depth === 1 && this.#expectKey;
        if (this.#readingKey) {
          this.#keyToken = '"';
          this.#expectKey = false;
        }
      } else if (char === "{" || char === "[") {
        this.#depth++;
        if (this.#depth === 1 && char === "{") this.#expectKey = true;
      } else if (char === "}" || char === "]") {
        this.#depth--;
        if (this.#skipping && this.#depth === 1 && char === "]") {
          this.#skipping = false;
          start = i + 1;
        }
      } else if (this.#depth === 1 && char === ":") {
        this.#awaitingValue = true;
      } else if (this.#depth === 1 && char === ",") {
        this.#expectKey = true;
        this.#key = "";
      }
    }
    if (!this.#skipping) parts.push(text.slice(start));
    return parts.join("");
  }

  // A child may exit without LF. Do not let a truncated discarded array look
  // complete to the worker's final JSON.parse.
  end(): string {
    const incomplete = this.#skipping ? "!" : "";
    this.#reset();
    return incomplete;
  }

  #reset(): void {
    this.#depth = 0;
    this.#inString = false;
    this.#escaped = false;
    this.#expectKey = false;
    this.#readingKey = false;
    this.#keyToken = "";
    this.#key = "";
    this.#awaitingValue = false;
    this.#skipping = false;
  }
}
