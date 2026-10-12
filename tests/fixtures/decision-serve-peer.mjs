#!/usr/bin/env node
// Offline JSONL peer. Never performs inference, credential resolution or networking.
const mode = process.env.DECISION_PEER_MODE;
const max = 16 * 1024 * 1024;
const send = value => process.stdout.write(JSON.stringify(value) + "\n");
if (mode === "oversize-banner") process.stdout.write("x".repeat(max + 1));
else send({ ready: { protocol: 2, version: "test", features: ["decisions", "decision-targets"] } });
process.stdout.on("error", () => process.exit(0));
if (mode === "close-input") { process.stdin.destroy(); setTimeout(() => process.exit(0), 200); }
let buffer = "";
const attach = () => {
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", chunk => {
    buffer += chunk;
    let index;
    while ((index = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, index); buffer = buffer.slice(index + 1);
      const r = JSON.parse(line);
      if (mode === "exit") { process.stderr.write("private-credential-must-not-leak\n"); process.exit(4); }
      if (mode === "hang") continue;
      if (mode === "invalid-json") { process.stdout.write("not-json\n"); continue; }
      if (mode === "partial") { process.stdout.write('{"id":'); process.exit(0); }
      if (mode === "invalid-utf8") { process.stdout.write(Buffer.from([0xc3, 0x28, 10])); continue; }
      if (mode === "oversize-complete" || mode === "oversize-incomplete") {
        process.stdout.write(JSON.stringify({ id: r.id, ok: true, result: "😀".repeat(max / 4) }) + (mode === "oversize-complete" ? "\n" : ""));
        continue;
      }
      if (mode === "max") {
        const prefix = JSON.stringify({ id: r.id, ok: true, result: "" });
        const result = "x".repeat(max - Buffer.byteLength(prefix));
        send({ id: r.id, ok: true, result }); continue;
      }
      if (r.op === "fail") { send({ id: r.id, ok: false, error: { code: 22, message: "offline failure" } }); continue; }
      if (r.op === "unicode") {
        const bytes = Buffer.from(JSON.stringify({ id: r.id, ok: true, result: "😀é漢字" }) + "\n");
        for (let i = 0; i < bytes.length; i++) process.stdout.write(bytes.subarray(i, i + 1));
        continue;
      }
      const result = { op: r.op, bytes: Buffer.byteLength(r.text ?? "") };
      setTimeout(() => send({ id: r.id, ok: true, result }), r.delay ?? 0);
    }
  });
  process.stdin.on("end", () => process.exit(0));
};
if (mode === "slow") setTimeout(attach, 100); else attach();
