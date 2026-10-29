#!/usr/bin/python3
"""Test-only real Linux record-lock probe and owned, deferred-reap supervisor.

No production implementation lives here. `set-inherited` is an oracle control,
not the candidate guard. Every pathname supplied by tests is inside mkdtemp.
Expected skip: non-Linux / non-64-bit supported ABI; real PID namespace denial.
No unlink/rmtree, shell invocation, process-name signalling, or trace cleanup.
"""
import ctypes
import errno
import fcntl
import json
import os
import platform
import selectors
import signal
import subprocess
import sys
import time

DEADLINE = 8.0
ENV = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}


def emit(value):
    print(json.dumps(value, separators=(",", ":")), flush=True)


class Flock(ctypes.Structure):
    _fields_ = [("l_type", ctypes.c_short), ("l_whence", ctypes.c_short),
                ("l_start", ctypes.c_longlong), ("l_len", ctypes.c_longlong),
                ("l_pid", ctypes.c_int)]


def require_abi():
    # Linux asm-generic/x86-64 64-bit struct flock; refuse every unverified ABI.
    if (sys.platform != "linux" or platform.machine() not in ("x86_64", "aarch64")
            or ctypes.sizeof(ctypes.c_void_p) != 8 or ctypes.sizeof(Flock) != 32
            or [getattr(Flock, name).offset for name, _ in Flock._fields_] != [0, 2, 8, 16, 24]
            or not hasattr(fcntl, "F_OFD_SETLK") or not hasattr(fcntl, "F_OFD_GETLK")):
        raise RuntimeError("unsupported Linux struct flock ABI / named OFD constants")


def record(fd, command):
    require_abi()
    lock = Flock(fcntl.F_WRLCK, os.SEEK_SET, 0, 0, 0)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.fcntl.restype = ctypes.c_int
    result = libc.fcntl(ctypes.c_int(fd), ctypes.c_int(command), ctypes.byref(lock))
    if result == -1:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    return lock


def open_fixed(file):
    # The tests create the file first: the oracle never creates/replaces it.
    return os.open(file, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)


def identity(fd):
    stat = os.fstat(fd)
    return {"dev": str(stat.st_dev), "ino": str(stat.st_ino)}


def try_lock(file, kind):
    fd = open_fixed(file)
    try:
        try:
            if kind == "flock":
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                record(fd, fcntl.F_OFD_SETLK if kind == "ofd" else fcntl.F_SETLK)
            return {"status": "acquired", **identity(fd)}
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN):
                return {"status": "blocked", "errno": error.errno, **identity(fd)}
            if error.errno in (errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP):
                return {"status": "unsupported", "errno": error.errno, **identity(fd)}
            raise
    finally:
        os.close(fd)


def query(fd):
    lock = record(fd, fcntl.F_OFD_GETLK)
    return {"status": "unlocked" if lock.l_type == fcntl.F_UNLCK else "conflict",
            "type": lock.l_type, "writeType": fcntl.F_WRLCK, "pid": lock.l_pid,
            "start": lock.l_start, "len": lock.l_len, **identity(fd)}


def process_proof(pid):
    with open("/proc/%d/stat" % pid, encoding="ascii") as handle:
        stat = handle.read()
    fields = stat[stat.rfind(")") + 2:].split()
    return {"pid": pid, "state": fields[0], "pidStartTime": fields[19],
            "pidNamespace": os.readlink("/proc/%d/ns/pid" % pid)}


def read_line(fd, timeout=DEADLINE):
    # os.read avoids TextIO read-ahead hiding a second line from select().
    end = time.monotonic() + timeout
    data = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(fd, selectors.EVENT_READ)
        while b"\n" not in data:
            remaining = end - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise TimeoutError("bounded readiness/control pipe timed out")
            byte = os.read(fd, 1)
            if not byte:
                raise EOFError("readiness/control pipe closed")
            data.extend(byte)
            if len(data) > 65536:
                raise RuntimeError("oversized fixture message")
    return data.decode("utf-8").strip()


def deny_ofd():
    """Only in the owned child: real kernel EINVAL, inherited through exec/helpers.

    Deny both named OFD commands, not flock or POSIX F_SETLK. A candidate falling
    back to either weaker lock is therefore observed as an erroneous READY.
    """
    require_abi()
    syscall = 72 if platform.machine() == "x86_64" else 25

    class Filter(ctypes.Structure):
        _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte),
                    ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint32)]

    class Program(ctypes.Structure):
        _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(Filter))]

    # seccomp_data.nr at 0; args[1] low word at 24 (supported little-endian ABIs).
    if sys.byteorder != "little":
        raise RuntimeError("unsupported seccomp byte order")
    filters = (Filter * 8)(
        Filter(0x20, 0, 0, 0),                 # LD nr
        Filter(0x15, 0, 5, syscall),           # not fcntl -> ALLOW
        Filter(0x20, 0, 0, 24),                # LD arg1 (command)
        Filter(0x15, 2, 0, fcntl.F_OFD_SETLK),
        Filter(0x15, 1, 0, fcntl.F_OFD_GETLK),
        Filter(0x06, 0, 0, 0x7FFF0000),        # ALLOW other fcntl
        Filter(0x06, 0, 0, 0x00050000 | errno.EINVAL),
        Filter(0x06, 0, 0, 0x7FFF0000),
    )
    libc = ctypes.CDLL(None, use_errno=True)
    program = Program(len(filters), filters)
    if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.prctl(22, 2, ctypes.byref(program), 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "test-only seccomp unavailable")


def bounded_waitid(pid, flags):
    # waitid is event-driven but still needs a hard deadline (e.g. D-state child).
    def expired(_signum, _frame):
        raise TimeoutError("bounded waitid timed out")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, DEADLINE)
    try:
        return os.waitid(os.P_PID, pid, flags | os.WNOWAIT)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def supervise(argv, sandbox=False):
    # The supervisor never opens the guard file, and never holds its description.
    child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=None, env=ENV, close_fds=True,
                             preexec_fn=deny_ofd if sandbox else None)
    reaped = False
    try:
        ready = json.loads(read_line(child.stdout.fileno()))
        emit({"event": "CHILD", "child": ready, "outer": process_proof(child.pid)})
        # Explicit controls; no background polling/reaping, including after SIGKILL.
        while True:
            command = read_line(sys.stdin.fileno())
            if command == "STOP":
                os.kill(child.pid, signal.SIGSTOP)
                # waitid acknowledges the stopped state without consuming exit.
                bounded_waitid(child.pid, os.WSTOPPED)
                emit({"event": "STOPPED", **process_proof(child.pid)})
            elif command == "CONT":
                os.kill(child.pid, signal.SIGCONT)
                emit({"event": "CONTINUED", **process_proof(child.pid)})
            elif command == "KILL":
                os.kill(child.pid, signal.SIGKILL)
                bounded_waitid(child.pid, os.WEXITED)
                emit({"event": "ZOMBIE", **process_proof(child.pid)})
            elif command == "TERM":
                os.kill(child.pid, signal.SIGTERM)
                exited = bounded_waitid(child.pid, os.WEXITED)
                emit({"event": "TERMINATED", "exitStatus": exited.si_status,
                      "exitKind": exited.si_code, **process_proof(child.pid)})
            elif command == "REAP":
                code = child.wait(timeout=DEADLINE)
                reaped = True
                emit({"event": "REAPED", "pid": child.pid, "code": code})
            elif command == "CLOSE":
                child.stdin.write(b"CLOSE\n")
                child.stdin.flush()
                emit({"event": "CHILD", "child": json.loads(read_line(child.stdout.fileno()))})
            elif command == "QUIT":
                return
            else:
                raise RuntimeError("unknown supervisor control: " + command)
    finally:
        if not reaped:
            # Only this exact Popen child, never names or shared process groups.
            try:
                os.kill(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=DEADLINE)
        child.stdin.close()
        child.stdout.close()


def main():
    command = sys.argv[1]
    if command in ("set-inherited", "set-inherited-flock", "set-inherited-posix"):
        if command == "set-inherited-flock":
            fcntl.flock(3, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            record(3, fcntl.F_SETLK if command.endswith("posix") else fcntl.F_OFD_SETLK)
        emit({"status": "held", "pid": os.getpid(), **identity(3)})
        # Deliberately no F_UNLCK; process exit closes only this dup of fd 3.
    elif command == "query-inherited":
        emit(query(3))
    elif command == "query":
        fd = open_fixed(sys.argv[2])
        try:
            emit(query(fd))
        finally:
            os.close(fd)
    elif command.startswith("try-"):
        emit(try_lock(sys.argv[2], command[4:]))
    elif command == "supervise" or command == "supervise-deny-ofd":
        supervise(sys.argv[2:], sandbox=command.endswith("deny-ofd"))
    elif command == "namespace-capability":
        # Feature probe only: actual candidate row still uses a new guard open.
        if not os.path.isfile("/usr/bin/unshare"):
            emit({"status": "expected-skip", "reason": "/usr/bin/unshare unavailable"})
            return
        result = subprocess.run(["/usr/bin/unshare", "--user", "--map-root-user", "--pid", "--fork",
                                 "/usr/bin/python3", "-I", "-B", "-c",
                                 "import os; print(os.readlink('/proc/self/ns/pid'))"],
                                timeout=DEADLINE, capture_output=True, text=True, env=ENV)
        if result.returncode == 0:
            emit({"status": "supported", "pidNamespace": result.stdout.strip()})
        elif "Operation not permitted" in result.stderr or "Permission denied" in result.stderr:
            emit({"status": "expected-skip", "reason": result.stderr.strip()})
        else:
            raise RuntimeError("unexpected unshare failure: " + result.stderr)
    elif command == "sandbox-capability":
        # A separate child installs the filter so the supervisor/test is unaffected.
        result = subprocess.run(["/usr/bin/python3", "-I", "-B", __file__, "sandbox-probe", sys.argv[2]],
                                timeout=DEADLINE, capture_output=True, text=True, env=ENV)
        if result.returncode != 0:
            raise RuntimeError("unexpected sandbox probe failure: " + result.stderr)
        emit(json.loads(result.stdout))
    elif command == "sandbox-probe":
        try:
            deny_ofd()
        except OSError as error:
            if error.errno not in (errno.EPERM, errno.EACCES, errno.ENOSYS, errno.EINVAL):
                raise
            emit({"status": "expected-skip", "reason": str(error)})
            return
        emit(try_lock(sys.argv[2], "ofd"))
    else:
        raise RuntimeError("unknown probe command: " + command)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        emit({"event": "FIXTURE_ERROR", "error": str(error), "kind": type(error).__name__})
        sys.exit(1)
