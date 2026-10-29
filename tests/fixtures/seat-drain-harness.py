"""Runs a seam exactly as A's seam caller does: the verified executable by its open
descriptor, the held seat lock passed as D3_SEAT_LOCK_FD, and the fixed minimal environment.

usage: seat-drain-harness.py RUN EXE PHASE [nolock] [home=DIR]   (request on stdin; one JSON result line on stdout)
home=DIR adds HOME=DIR to that environment (a test proving the seam never falls back to a host file under HOME).
"""
import fcntl
import json
import os
import subprocess
import sys

run_dir, exe, phase = sys.argv[1:4]
flags = sys.argv[4:]
home = [flag[len("home="):] for flag in flags if flag.startswith("home=")]
lock = os.open(os.path.join(run_dir, "seat.lock"), os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
if "nolock" not in flags:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
fd = os.open(exe, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
request = sys.stdin.buffer.read()
env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "D3_SEAT_LOCK_FD": str(lock)}
if home:
    env["HOME"] = home[-1]
os.set_inheritable(lock, True)
try:
    out = subprocess.run(["/proc/self/fd/" + str(fd), "--run-dir", run_dir, "--phase", phase], input=request,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, pass_fds=(fd, lock), timeout=120,
                         check=False, close_fds=True)
    print(json.dumps({"code": out.returncode, "stdout": out.stdout.decode(), "stderr": out.stderr.decode()}))
except subprocess.TimeoutExpired:
    print(json.dumps({"code": None, "stdout": "", "stderr": "timeout"}))
