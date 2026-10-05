"""Hold a run inode solely in an unreceived Unix SCM_RIGHTS message."""
import array
import json
import os
import socket
import sys

sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
try:
    fd = os.open(sys.argv[1], os.O_WRONLY | os.O_APPEND)
    identity = os.fstat(fd)
    try:
        sender.sendmsg([b"run"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])
    finally:
        os.close(fd)
    # No fd table entry in this process holds the file while the message waits.
    matching = []
    for name in os.listdir("/proc/self/fd"):
        try:
            stat = os.stat("/proc/self/fd/" + name)
            if (stat.st_dev, stat.st_ino) == (identity.st_dev, identity.st_ino):
                matching.append(name)
        except FileNotFoundError:
            pass
    print(json.dumps({"queued": True, "matching_fds": matching, "ino": identity.st_ino, "dev": identity.st_dev}), flush=True)
    if sys.stdin.readline().strip() != "receive":
        raise RuntimeError("parent did not authorize receive after retention")
    _, ancillary, _, _ = receiver.recvmsg(16, socket.CMSG_SPACE(array.array("i").itemsize))
    rights = array.array("i")
    for level, kind, data in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            rights.frombytes(data[:len(data) - len(data) % rights.itemsize])
    if len(rights) != 1:
        raise RuntimeError("expected exactly one descriptor")
    fd = rights[0]
    try:
        received = os.fstat(fd)
        os.write(fd, b"late SCM_RIGHTS write\n")
        os.fsync(fd)
        print(json.dumps({"received_same_inode": (received.st_dev, received.st_ino) == (identity.st_dev, identity.st_ino), "nlink": received.st_nlink}), flush=True)
    finally:
        os.close(fd)
finally:
    sender.close()
    receiver.close()
