// Pi repeats the entire history in agent_end.messages and the current tool
// results in turn_end.toolResults. These top-level fields are not consumed by
// the worker or transcript reader; authoritative messages arrive separately.
// Elide them *before* buffering a JSONL record, not after JSON.parse. This keeps
// image-heavy sessions bounded without dropping lifecycle flags after history.
// This is a lexical projection, not a JSON validator. Retained JSON is parsed
// by the worker. Discarded values are never accumulated; key tokens are bounded.
export class PiEventProjection {
  #history = new HistoryProjection();
  #images = new ImageDataProjection();

  write(text: string): string {
    return this.#images.write(this.#history.write(text));
  }

  end(): string {
    return this.#images.write(this.#history.end()) + this.#images.end();
  }
}

interface Frame {
  object: boolean;
  image: boolean;
}

// smarty-dev#1907: a child `read` of a PNG returns the image as base64 inside
// tool_execution_end and the toolResult message_end. The child's own session
// keeps the image for its model; the parent needs only the fact of it. Replace
// `"data":"<base64>"` of any `{"type":"image",...}` object at any depth with
// `"elided":true,"bytes":<decoded size>` while streaming, so the line never
// buffers the image. Every image is stubbed, whatever its size.
// ponytail: lexical, not a JSON parser. It stubs only when "type":"image"
// precedes "data" in the object, which is Pi's field order ({type, data,
// mimeType}). Another order passes through to the worker's line cap, which
// drops that one event with a warning. Only a key token is ever held.
class ImageDataProjection {
  #stack: Frame[] = [];
  #inString = false;
  #escaped = false;
  #role: "key" | "type" | "data" | "other" = "other";
  #token = "";
  #key = "";
  #expectKey = false;
  #awaitingValue = false;
  // A key of an image object is held until it is known not to be "data".
  #holding = false;
  #held = "";
  #dropping = false;
  #dataChars = 0;
  #padding = 0;

  write(text: string): string {
    const parts: string[] = [];
    let start = 0;
    for (let i = 0; i < text.length; i++) {
      const char = text[i]!;
      if (char === "\n") {
        if (this.#holding) parts.push(this.#held, text.slice(start, i));
        else if (!this.#dropping) parts.push(text.slice(start, i));
        // An incomplete stubbed value must not manufacture a valid event.
        if (this.#dropping) parts.push("!");
        start = i;
        this.#reset();
        continue;
      }
      const top = this.#stack[this.#stack.length - 1];
      if (this.#inString) {
        if (this.#role === "data") {
          if (this.#escaped) this.#escaped = false;
          else if (char === "\\") this.#escaped = true;
          else if (char === '"') {
            this.#inString = false;
            this.#dropping = false;
            const bytes = Math.max(0, Math.floor(((this.#dataChars + this.#padding) * 3) / 4) - this.#padding);
            parts.push(`"elided":true,"bytes":${bytes}`);
            start = i + 1;
            continue;
          }
          if (char === "=") this.#padding++;
          else if (char !== "\\") this.#dataChars++;
          continue;
        }
        if (this.#role !== "other" && this.#token.length < 256) this.#token += char;
        if (this.#escaped) this.#escaped = false;
        else if (char === "\\") this.#escaped = true;
        else if (char === '"') {
          this.#inString = false;
          let value = "";
          try {
            value = this.#role === "other" ? "" : (JSON.parse(this.#token) as string);
          } catch {
            value = "";
          }
          if (this.#role === "key") {
            this.#key = value;
            if (this.#holding && value !== "data") this.#flush(parts);
          } else if (this.#role === "type" && top?.object && value === "image") top.image = true;
        }
        continue;
      }
      if (char === '"') {
        this.#inString = true;
        this.#token = '"';
        if (top?.object && this.#expectKey) {
          this.#role = "key";
          this.#expectKey = false;
          if (top.image) {
            parts.push(text.slice(start, i));
            start = i;
            this.#holding = true;
            this.#held = "";
          }
        } else if (top?.object && this.#awaitingValue && this.#key === "type") this.#role = "type";
        else if (top?.object && this.#awaitingValue && this.#key === "data" && this.#holding) {
          this.#role = "data";
          this.#dataChars = 0;
          this.#padding = 0;
          this.#holding = false;
          this.#held = "";
          this.#dropping = true;
        } else this.#role = "other";
        this.#awaitingValue = false;
        continue;
      }
      if (/[ \t\r]/.test(char)) continue;
      if (char === ":") {
        if (top?.object) this.#awaitingValue = true;
        continue;
      }
      // Any other token starts or ends a value: a held non-string data value is kept.
      if (this.#holding) this.#flush(parts);
      this.#awaitingValue = false;
      if (char === "{") {
        this.#stack.push({ object: true, image: false });
        this.#expectKey = true;
        this.#key = "";
      } else if (char === "[") {
        this.#stack.push({ object: false, image: false });
      } else if (char === "}" || char === "]") {
        this.#stack.pop();
        this.#expectKey = false;
      } else if (char === "," && top?.object) {
        this.#expectKey = true;
        this.#key = "";
      }
    }
    if (this.#holding) this.#held += text.slice(start);
    else if (!this.#dropping) parts.push(text.slice(start));
    return parts.join("");
  }

  end(): string {
    const rest = this.#holding ? this.#held : this.#dropping ? "!" : "";
    this.#reset();
    return rest;
  }

  // Held output is emitted as is. The caller's `start` already points past it.
  #flush(parts: string[]): void {
    parts.push(this.#held);
    this.#held = "";
    this.#holding = false;
  }

  #reset(): void {
    this.#stack = [];
    this.#inString = false;
    this.#escaped = false;
    this.#role = "other";
    this.#token = "";
    this.#key = "";
    this.#expectKey = false;
    this.#awaitingValue = false;
    this.#holding = false;
    this.#held = "";
    this.#dropping = false;
    this.#dataChars = 0;
    this.#padding = 0;
  }
}

class HistoryProjection {
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
