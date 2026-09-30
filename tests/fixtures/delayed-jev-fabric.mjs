#!/usr/bin/env node
// Transparent protocol gate for a real backend: only delivery of a completed
// spawn response is delayed. No child, process group, or stop call is mocked.
import { spawn } from "node:child_process";
import fs from "node:fs";
const [binary, ...args] = process.argv.slice(2);
const backend = spawn(binary, args, { env: process.env, stdio: ["pipe", "pipe", "pipe"] });
const operations = new Map();
let input = "", output = "", exited = false;
process.stdin.on("data", chunk => {
  input += chunk.toString();
  let newline;
  while ((newline = input.indexOf("\n")) >= 0) {
    const line = input.slice(0, newline); input = input.slice(newline + 1);
    try { const request = JSON.parse(line); operations.set(request.id, request.op); } catch {}
    backend.stdin.write(line + "\n");
  }
});
backend.stdout.on("data", chunk => {
  output += chunk.toString();
  let newline;
  while ((newline = output.indexOf("\n")) >= 0) {
    const line = output.slice(0, newline); output = output.slice(newline + 1);
    let response; try { response = JSON.parse(line); } catch {}
    if (response?.ok && operations.get(response.id) === "spawn" && process.env.JEV_TEST_SPAWN_READY_FILE) {
      const ready = process.env.JEV_TEST_SPAWN_READY_FILE;
      fs.writeFileSync(ready, JSON.stringify(response.result));
      const gate = setInterval(() => {
        if (exited || fs.existsSync(ready + ".release")) {
          clearInterval(gate);
          if (!exited) process.stdout.write(line + "\n");
        }
      }, 10);
    } else process.stdout.write(line + "\n");
  }
});
backend.stderr.pipe(process.stderr);
backend.stdin.on("error", () => {});
process.stdin.on("end", () => backend.stdin.end());
backend.once("error", error => { process.stderr.write(error.message); process.exit(1); });
backend.once("close", code => { exited = true; process.exit(code ?? 1); });
