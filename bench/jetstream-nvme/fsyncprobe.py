#!/usr/bin/env python3
"""Raw disk floor: append 200 B + fdatasync, N times, on the bench filesystem."""
import os, sys, time
path, n = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 3000
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
buf, lat = os.urandom(200), []
for _ in range(n):
    t = time.perf_counter(); os.write(fd, buf); os.fdatasync(fd); lat.append(time.perf_counter() - t)
os.close(fd)
lat.sort()
p = lambda q: lat[min(n - 1, int(q * n))] * 1000
print(f"append200+fdatasync n={n}: {n / sum(lat):.0f}/s p50={p(.5):.3f}ms p99={p(.99):.3f}ms p999={p(.999):.3f}ms")
