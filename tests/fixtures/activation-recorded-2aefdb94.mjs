// Rebuild the sanitized replay JSONL from tests/fixtures/activation-recorded-2aefdb94.json (byte-identical).
// Usage: node activation-recorded-2aefdb94.mjs FIXTURE.json [OUT.jsonl]   (also exports buildReplay(fixture) -> string)
import { readFileSync, writeFileSync } from "node:fs";
import { brotliDecompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

const unbr = (b64) => brotliDecompressSync(Buffer.from(b64, "base64")).toString("utf8");

export function buildReplay(fx) {
  const { shapes, shared, entries } = JSON.parse(unbr(fx.residual.structure));
  const ws = unbr(fx.residual.whitespace).split("\0");
  const ser = (v) => {
    if (v === null || typeof v === "boolean") return String(v);
    if (typeof v === "number") { if (!Number.isSafeInteger(v)) throw new Error(`bare ${v}`); return String(v); }
    if (typeof v === "string") {
      if (!v.startsWith("~")) return JSON.stringify(v);
      const [tag, rest] = [v[1], v.slice(2)];
      if (tag === "=") return JSON.stringify(`~${rest}`);
      if (tag === "x" || tag === "A") return JSON.stringify(tag.repeat(Number(rest)));
      if (tag === "n") return rest; // exact Python float lexeme
      if (tag === "w") {
        const [id, len, nl, tab, cr, sp] = rest.split(",").map(Number);
        const s = ws[id];
        const n = (c) => s.split(c).length - 1;
        if (s.length !== len || n("\n") !== nl || n("\t") !== tab || n("\r") !== cr || n(" ") !== sp) throw new Error(`whitespace ${id}`);
        return JSON.stringify(s);
      }
      throw new Error(`tag ${v.slice(0, 20)}`);
    }
    if (v[0] === "L") return `[${v.slice(1).map(ser).join(",")}]`;
    if (v[0] === "R") return ser(shared[v[1]]);
    if (v[0] === "S") return JSON.stringify(ser(v[1]));
    const keys = shapes[v[0]];
    if (keys.length !== v.length - 1) throw new Error(`shape ${v[0]}`);
    return `{${keys.map((k, i) => `${JSON.stringify(k)}:${ser(v[i + 1])}`).join(",")}}`;
  };
  if (entries.length !== fx.rows.length) throw new Error("entry count");
  const lines = entries.map((e, i) => {
    const line = ser(e);
    const [type, role, bt, bl, sl, usage, lineBytes] = fx.rows[i];
    const o = JSON.parse(line);
    const m = o.message && typeof o.message === "object" ? o.message : {};
    const c = m.content;
    const got = [[], [], []];
    if (typeof c === "string") got[0].push("string"), got[1].push(c.length);
    else if (Array.isArray(c)) for (const b of c) {
      got[0].push(b.type);
      got[1].push(b.type === "text" ? b.text.length : b.type === "thinking" ? b.thinking.length : b.type === "toolCall" ? JSON.stringify(b.arguments).length : null);
      if (typeof b.thinkingSignature === "string") got[2].push(b.thinkingSignature.length);
    }
    let u = usage;
    if (u !== null && !u.startsWith('"usage"')) {
      const x = u.split(",");
      u = `"usage":{${fx.usageKeys.slice(0, 6).map((k, j) => `"${k}":${x[j]}`).join(",")},"cost":{${fx.usageKeys.slice(6).map((k, j) => `"${k.slice(5)}":${x[j + 6]}`).join(",")}}}`;
    }
    const ok = o.type === type && (m.role ?? null) === role && JSON.stringify(got) === JSON.stringify([bt, bl, sl]) &&
      Buffer.byteLength(line) + 1 === lineBytes && ("usage" in m ? u !== null && line.includes(u) : u === null);
    if (!ok) throw new Error(`row ${i} does not match rebuilt entry`);
    return `${line}\n`;
  });
  return lines.join("");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const out = buildReplay(JSON.parse(readFileSync(process.argv[2], "utf8")));
  if (process.argv[3]) writeFileSync(process.argv[3], out);
  console.log(`entries ${out.split("\n").length - 1} bytes ${Buffer.byteLength(out)} sha256 ${createHash("sha256").update(out).digest("hex")}`);
}
