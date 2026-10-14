#!/usr/bin/env python3
"""Offline real Pi TUI + built Fabric + a second process holding its mesh lock (#5536).

No model turn, credentials, inherited profile, shared mesh or service changes. A readiness
pipe is observation only; the warning comes from Fabric's real participant heartbeat.
Run after a build: python3 scripts/probe-main-diagnostics.py --out <owned evidence dir>
"""
import argparse
import errno
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time


def hold_lock(root: Path):
    lock = root / ".lock"
    lock.mkdir(mode=0o700)
    (lock / "owner").write_text(f"diagnostics-proof\n{os.getpid()}\n{int(time.time() * 1000)}\n")
    print(json.dumps({"held": True, "pid": os.getpid()}), flush=True)
    sys.stdin.buffer.read(1)  # parent releases this owned holder once it has observed the warning
    shutil.rmtree(lock)


def run(out: Path, repo: Path, cli: Path, entry: Path):
    out.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="fabric-main-diagnostics-"))
    home, project, agent, tmp, mesh = (root / n for n in ("home", "project", "agent", "tmp", "mesh"))
    for directory in (home, project, agent, tmp, mesh):
        directory.mkdir(mode=0o700)
    (agent / "settings.json").write_text(json.dumps({"retry": {"enabled": False}, "compaction": {"enabled": False}}))
    (agent / "fabric.json").write_text(json.dumps({
        "fullCodeMode": False, "mesh": {"enabled": True, "announce": True},
        "mcp": {"enabled": False}, "jev": {"enabled": False}, "memory": {"enabled": False},
        "compaction": {"engine": "pi"}, "entropy": {"compile": False}, "speculation": {"enabled": False},
    }))
    (agent / "fabric/logs").mkdir(parents=True, mode=0o700)
    ready_read, ready_write = os.pipe()
    fixture = root / "observe.ts"
    fixture.write_text('''import fs from "node:fs";
export default function (pi) {
  const report = data => fs.writeSync(Number(process.env.FABRIC_DIAGNOSTICS_PROOF_FD), JSON.stringify(data) + "\\n");
  pi.on("session_start", (_event, ctx) => report({ ready: true, pid: process.pid,
    hasUI: ctx.hasUI, mode: ctx.mode, fabric: pi.getAllTools().some(tool => tool.name === "fabric_exec") }));
  pi.registerCommand("diagnostics-proof-finish", {
    description: "Confirm the TUI editor still accepts a command, then exit this private proof",
    handler: async (_args, ctx) => { report({ editorCommand: true }); ctx.shutdown(); },
  });
}
''')
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 140, 0, 0))
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "TERM": "xterm-256color",
        "HOME": str(home), "USERPROFILE": str(home), "TMPDIR": str(tmp), "TEMP": str(tmp), "TMP": str(tmp),
        "PI_CODING_AGENT_DIR": str(agent), "PI_OFFLINE": "1", "PI_FABRIC_MESH_ROOT": str(mesh),
        "PI_FABRIC_AGENT_DIR": str(root / "exports"), "PI_FABRIC_RUN_ROOT": str(root / "runs"),
        "PI_FABRIC_PROJECT_ROOT": str(project), "FABRIC_DIAGNOSTICS_PROOF_FD": str(ready_write),
    }
    command = ["node", str(cli), "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes",
               "--no-context-files", "--approve", "--no-builtin-tools", "--offline", "--thinking", "off",
               "--session-dir", str(root / "sessions"), "-e", str(entry), "-e", str(fixture)]
    child = subprocess.Popen(command, cwd=project, env=env, stdin=slave, stdout=slave, stderr=slave,
                             pass_fds=(ready_write,), start_new_session=True)
    os.close(slave)
    os.close(ready_write)
    raw = bytearray()
    reports = bytearray()
    holder = None
    ready = None
    log = agent / "fabric/logs/diagnostics.log"
    watcher = None
    warning = None
    editor_ack = False
    started = time.monotonic()
    try:
        # Linux inotify carries log events without a readiness/poll loop.
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        watcher = libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if watcher < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1")
        # Observe the private log directory; a real heartbeat diagnostic creates/appends its log.
        deadline = started + 90
        finishing = False
        while time.monotonic() < deadline:
            readers = [master, ready_read]
            if ready is not None:
                readers.append(watcher)
            readable, _, _ = select.select(readers, [], [], max(0, deadline - time.monotonic()))
            if not readable:
                raise TimeoutError("No native event before proof deadline")
            if master in readable:
                try:
                    chunk = os.read(master, 65536)
                except OSError as error:
                    if error.errno == errno.EIO:
                        chunk = b""
                    else:
                        raise
                raw.extend(chunk)
                if b"mesh lock timeout" in raw or b"FABRIC_MESH_LOCK_TIMEOUT" in raw:
                    raise AssertionError("Original mesh lock-timeout warning bytes reached the real TUI stream")
                # Pi's terminal startup asks for cursor position; answer as an ordinary terminal.
                if b"\x1b[6n" in chunk:
                    os.write(master, b"\x1b[1;1R")
                if not chunk:
                    break
            if ready_read in readable:
                chunk = os.read(ready_read, 65536)
                reports.extend(chunk)
                if not chunk and child.poll() is not None:
                    break
                records = [json.loads(line) for line in reports.splitlines() if line.startswith(b"{")]
                if ready is None:
                    ready = next((r for r in records if r.get("ready")), None)
                    if ready is not None:
                        if ready.get("mode") != "tui" or not ready.get("hasUI") or not ready.get("fabric"):
                            raise AssertionError(f"Not a real Fabric Main TUI: {ready}")
                        if libc.inotify_add_watch(watcher, os.fsencode(log.parent), 0x00000002 | 0x00000100 | 0x00000080) < 0:
                            raise OSError(ctypes.get_errno(), "inotify_add_watch")
                        holder = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--hold", str(mesh)],
                                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
                        held = json.loads(holder.stdout.readline())
                        if not held.get("held"):
                            raise AssertionError(f"No live lock holder: {held}")
                editor_ack = any(r.get("editorCommand") for r in records)
            if ready is not None and watcher in readable:
                os.read(watcher, 65536)
                text = log.read_text()
                warning = next((line for line in text.splitlines() if "participant heartbeat/change refresh: mesh lock timeout; retrying" in line), None)
                if warning and not finishing:
                    finishing = True
                    holder.stdin.write(b"x")
                    holder.stdin.flush()
                    holder.wait(timeout=10)
                    # Exercise the actual editor's slash-command input after the warning.
                    os.write(master, b"/diagnostics-proof-finish\r")
            if editor_ack:
                # Continue draining the PTY until native exit, preserving any shutdown writes too.
                deadline = min(deadline, time.monotonic() + 20)
        child.wait(timeout=10)
        (out / "tui-output.bin").write_bytes(raw)
        (out / "host-observations.jsonl").write_bytes(reports)
        if log.exists():
            shutil.copyfile(log, out / "diagnostics.log")
        summary = {"command": command, "cwd": str(project), "privateRoot": str(root), "ready": ready,
                   "exitCode": child.returncode, "editorCommandAccepted": editor_ack,
                   "tuiBytes": len(raw), "warningBytesInTui": b"mesh lock timeout" in raw or b"FABRIC_MESH_LOCK_TIMEOUT" in raw,
                   "warningInLog": warning, "seconds": round(time.monotonic() - started, 3), "spend": 0}
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        if child.returncode != 0 or not editor_ack or not warning or summary["warningBytesInTui"]:
            raise AssertionError(json.dumps(summary, indent=2))
        print("REAL_PI_MAIN_DIAGNOSTICS: PASS " + json.dumps(summary))
    finally:
        (out / "tui-output.bin").write_bytes(raw)
        (out / "host-observations.jsonl").write_bytes(reports)
        if log.exists():
            shutil.copyfile(log, out / "diagnostics.log")
        if holder is not None and holder.poll() is None:
            holder.stdin.close()
            holder.wait(timeout=10)
        if child.poll() is None:
            child.send_signal(signal.SIGTERM)
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
        os.close(master)
        os.close(ready_read)
        if watcher is not None:
            os.close(watcher)
        shutil.rmtree(root)  # only this function's own mktemp root


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hold", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--pi-cli", type=Path)
    parser.add_argument("--entry", type=Path, help="Alternate built Fabric entry for the main negative control")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    if args.hold:
        hold_lock(args.hold)
    else:
        if not args.out:
            parser.error("--out is required")
        run(args.out, repo, (args.pi_cli or repo / "node_modules/@earendil-works/pi-coding-agent/dist/cli.js").resolve(),
            (args.entry or repo / "dist/index.js").resolve())
