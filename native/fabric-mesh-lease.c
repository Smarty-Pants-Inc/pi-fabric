/* SPDX-License-Identifier: MIT
 * fabric-mesh-lease FILE...: is any OTHER open file description of each FILE alive? (smarty-dev#7936)
 * A write lease (F_SETLEASE F_WRLCK) is refused with EAGAIN while any other process (dumpable or not,
 * an fd or only an mmap) holds the inode open. It is released and closed at once, so no opener is
 * blocked for longer than the probe. Per FILE one stdout line: "free", "held", "absent" or "error <errno>",
 * then the path. Exit: 0 all free or absent, 3 any held, 2 any error. Static, without libc, as
 * fabric-landlock (same build).
 *
 * fabric-mesh-lease --watch PARENTPID CONTROLDIR FILE... (smarty-dev#8116): inotify watches on each FILE's directory
 * and on each existing FILE's inode go up BEFORE the same lease probe, so an open after the probe is an
 * event, whatever the opener's dumpability or locks. After the probe lines it prints "watching". Each file
 * created in CONTROLDIR asks for a report: per FILE "opens OPENS PROBED FILE": OPENS by name in its directory,
 * PROBED the helper's own probe open (0 or 1). The caller takes its final report BEFORE its own first open of any
 * FILE, so every other open is foreign: it refuses when OPENS exceeds PROBED. Closes are never netted, and nothing
 * is subtracted for the caller (pi-fabric#881): inotify merges an event with an identical one at the queue tail
 * (same wd, mask and name; the pid is not compared; only the queue TAIL, unread), so an own open could swallow a
 * foreign one. The probe therefore READS its own IN_OPEN from the queue (bounded, about 2 s; else "unwatched FILE",
 * fail closed) BEFORE it takes the lease: every later open is a new queue entry and cannot merge into the probe's.
 * A foreign open before that read holds the inode, so F_SETLEASE fails ("held"); one between F_UNLCK and close
 * is a new entry (two foreign opens may merge with each other, but one is enough to refuse). Test only:
 * FABRIC_MESH_LEASE_TEST_PAUSE_AFTER_UNLCK=DIR pauses once there; FABRIC_MESH_LEASE_TEST_IGNORE_PROBE_OPEN=1
 * makes the probe never see its own IN_OPEN (the fail-closed path). Both are inert unless set;
 * "alias FILE" (its inode opened through another name: a hard link), "moved FILE", then "overflow" (lost
 * events), "newdir" (a missing directory appeared) and "invalidated" (a watched directory or file inode was
 * deleted, moved or its watch dropped, so a replacement would go unwatched) when they apply, and "end N".
 * It dies with PARENTPID (PR_SET_PDEATHSIG, then a getppid check).
 */
#include <asm/unistd.h>
#include <linux/fcntl.h>
#include <linux/inotify.h>
#include <linux/prctl.h>

#if defined(__x86_64__)
__asm__(".global _start\n_start:\nxor %rbp,%rbp\nmov %rsp,%rdi\nand $-16,%rsp\ncall fabric_start\n");
static long call(long nr, long a, long b, long c) {
    long result;
    __asm__ volatile("syscall" : "=a"(result) : "a"(nr), "D"(a), "S"(b), "d"(c) : "rcx", "r11", "memory");
    return result;
}
#elif defined(__aarch64__)
__asm__(".global _start\n_start:\nmov x0,sp\nbl fabric_start\n");
static long call(long nr, long a, long b, long c) {
    register long x0 __asm__("x0") = a, x1 __asm__("x1") = b, x2 __asm__("x2") = c, x8 __asm__("x8") = nr;
    __asm__ volatile("svc 0" : "+r"(x0) : "r"(x1), "r"(x2), "r"(x8) : "memory");
    return x0;
}
#else
#error "fabric-mesh-lease supports Linux x86_64 and aarch64"
#endif
#define SYS(n, a, b, c) call(__NR_##n, (long)(a), (long)(b), (long)(c))
#define EAGAIN 11
#define ENOENT 2

static void out(const char *s) { long n = 0; while (s[n]) n++; SYS(write, 1, s, n); }
static void number(unsigned long value) {
    char digits[24]; unsigned int end = sizeof(digits), start = end;
    do { digits[--start] = '0' + value % 10; value /= 10; } while (value);
    SYS(write, 1, digits + start, end - start);
}

__attribute__((noreturn)) static void fail(const char *what, long error) {
    out("error "); number(-error); out(" "); out(what); out("\n");
    SYS(exit_group, 2, 0, 0);
    __builtin_unreachable();
}
static int same(const char *a, const char *b) { while (*a && *a == *b) { a++; b++; } return *a == *b; }
static long slash(const char *p) { long at = -1; for (long i = 0; p[i]; i++) if (p[i] == '/') at = i; return at; }

#define MAXF 8
#define SELF_EVENTS (IN_DELETE_SELF | IN_MOVE_SELF)
#define FILE_EVENTS (IN_OPEN | IN_CLOSE | SELF_EVENTS | IN_DONT_FOLLOW)
#define DIR_EVENTS (IN_OPEN | IN_CLOSE | IN_CREATE | IN_DELETE | IN_MOVE | SELF_EVENTS | IN_ONLYDIR | IN_DONT_FOLLOW)
static struct target { char *path; long dir, wd; unsigned long opens, closes, inode, seen; int gen, alias, moved, probed; } T[MAXF];
static long parent;
static int opened;
static char dirs[MAXF][4096];
static long D[MAXF], nfiles, ndirs, inotify;
static int overflow, newdir, invalidated;
static struct target *probing;
static int probeSeen, ignoreProbe;
static long controlWd = -1;
static unsigned long seq;
static const char *pauseDir;

/* An inode watch: its opens (any name) must not exceed the opens by name seen in its generation. */
static void watch(struct target *t) {
    t->wd = SYS(inotify_add_watch, inotify, t->path, FILE_EVENTS);
    if (t->wd < 0 && t->wd != -ENOENT) fail(t->path, t->wd);
    t->inode = t->seen = 0;
    t->gen = t->wd >= 0;
}
static void endgen(struct target *t) { if (t->gen && t->inode > t->seen) t->alias = 1; t->gen = 0; }

static void report(unsigned long seq) {
    for (long i = 0; i < nfiles; i++) {
        struct target *t = &T[i];
        if (t->gen && t->inode > t->seen) t->alias = 1;
        out("opens "); number(t->opens); out(" "); number(t->probed); out(" "); out(t->path); out("\n");
        if (t->alias) { out("alias "); out(t->path); out("\n"); }
        if (t->moved) { out("moved "); out(t->path); out("\n"); }
    }
    if (overflow) out("overflow\n");
    if (newdir) out("newdir\n");
    if (invalidated) out("invalidated\n");
    out("end "); number(seq); out("\n");
}

static void event(const struct inotify_event *e, long control, unsigned long *seq) {
    if (e->mask & IN_Q_OVERFLOW) overflow = 1;
    if (e->wd == control) { if ((e->mask & IN_CREATE) && e->len) report(++*seq); return; }
    // A watched directory or file inode that is deleted, moved or loses its watch: its replacement would
    // go unwatched, so every later count is void (smarty-dev#8116 review, fail closed).
    if (e->mask & (SELF_EVENTS | IN_IGNORED)) {
        for (long d = 0; d < ndirs; d++) if (D[d] == e->wd) invalidated = 1;
        for (long i = 0; i < nfiles; i++) if (T[i].wd == e->wd) invalidated = 1;
    }
    for (long d = 0; d < ndirs; d++) {
        if (D[d] != e->wd || !e->len) continue;
        // A missing watched directory's name that appears by create or rename, as a directory, a symlink or
        // anything else, is unwatched: refuse (pi-fabric#881 review round 2). A file name that appears by
        // rename is "moved" below, which refuses too.
        if (e->mask & (IN_CREATE | IN_MOVED_TO))
            for (long m = 0; m < ndirs; m++) if (D[m] < 0 && same(dirs[m] + slash(dirs[m]) + 1, e->name)) newdir = 1;
        if (e->mask & IN_ISDIR) continue;
        for (long i = 0; i < nfiles; i++) {
            struct target *t = &T[i];
            if (t->dir != d || !same(t->path + slash(t->path) + 1, e->name)) continue;
            if (e->mask & IN_OPEN) { t->opens++; if (t->gen) t->seen++; if (t == probing && !ignoreProbe) probeSeen = 1; }
            if (e->mask & IN_CLOSE) t->closes++;
            if (e->mask & IN_MOVE) t->moved = 1;
            if (e->mask & (IN_CREATE | IN_DELETE)) { endgen(t); if (e->mask & IN_CREATE) watch(t); }
        }
    }
    for (long i = 0; i < nfiles; i++) if (T[i].gen && T[i].wd == e->wd && (e->mask & IN_OPEN)) T[i].inode++;
}

/* One read of the inotify queue, every event processed; the bytes read, or a negative errno (EAGAIN when empty). */
static long drain(void) {
    static char buffer[4096] __attribute__((aligned(8)));
    long n = SYS(read, inotify, buffer, sizeof(buffer));
    for (long at = 0; at < n;) {
        const struct inotify_event *e = (const struct inotify_event *)(buffer + at);
        event(e, controlWd, &seq);
        at += sizeof(*e) + e->len;
    }
    return n;
}

static long probe(char *file, struct target *t);

__attribute__((noreturn)) static void watching(long argc, char **argv) {
    if (argc < 5 || argc - 4 > MAXF) fail("usage: --watch PARENTPID CONTROLDIR FILE...", -22);
    for (const char *p = argv[2]; *p; p++) {
        if (*p < '0' || *p > '9' || parent > 1L << 40) fail("PARENTPID", -22);
        parent = parent * 10 + (*p - '0');
    }
    long died = SYS(prctl, PR_SET_PDEATHSIG, 9, 0);
    if (died < 0) fail("prctl PR_SET_PDEATHSIG", died);
    /* The parent may have died before the prctl: then no signal comes. */
    if (SYS(getppid, 0, 0, 0) != parent) fail("parent gone", -3);
    inotify = SYS(inotify_init1, IN_CLOEXEC, 0, 0);
    if (inotify < 0) fail("inotify_init1", inotify);
    long control = SYS(inotify_add_watch, inotify, argv[3], IN_CREATE | IN_ONLYDIR | IN_DONT_FOLLOW);
    if (control < 0) fail(argv[3], control);
    for (long i = 4; i < argc; i++) {
        struct target *t = &T[nfiles++];
        t->path = argv[i];
        long cut = slash(t->path);
        if (cut <= 0 || cut >= 4096) fail(t->path, -22);
        for (t->dir = 0; t->dir < ndirs; t->dir++) {
            long k = 0;
            while (k < cut && dirs[t->dir][k] == t->path[k]) k++;
            if (k == cut && !dirs[t->dir][k]) break;
        }
        if (t->dir == ndirs) {
            for (long k = 0; k < cut; k++) dirs[ndirs][k] = t->path[k];
            D[ndirs] = SYS(inotify_add_watch, inotify, dirs[ndirs], DIR_EVENTS);
            if (D[ndirs] < 0 && D[ndirs] != -ENOENT) fail(dirs[ndirs], D[ndirs]);
            ndirs++;
        }
        watch(t);
    }
    controlWd = control;
    for (long i = 4; i < argc; i++) { opened = 0; probe(argv[i], &T[i - 4]); T[i - 4].probed = opened; }
    out("watching\n");
    for (;;) if (drain() <= 0) fail("read inotify", -5);
}

/* Wait (bounded, about 2 s) until the inotify queue has delivered the probe's own IN_OPEN for `t`: from then on any
 * other open is a NEW queue entry and cannot be merged into the probe's (pi-fabric#881). */
static int ownOpenRead(struct target *t) {
    probing = t; probeSeen = 0;
    SYS(fcntl, inotify, F_SETFL, O_NONBLOCK);
    for (long tries = 0; tries < 2000 && !probeSeen; tries++) {
        if (drain() < 0) { struct { long sec, nsec; } pause = { 0, 1000000 }; SYS(nanosleep, &pause, 0, 0); }
    }
    SYS(fcntl, inotify, F_SETFL, 0);
    probing = 0;
    return probeSeen;
}

/* Tests only (FABRIC_MESH_LEASE_TEST_PAUSE_AFTER_UNLCK=DIR, inert unless set): once, between F_UNLCK and close,
 * create DIR/paused and wait (at most 30 s) for DIR/go, so a test can open the file exactly there. */
static void testPause(void) {
    if (!pauseDir) return;
    static char file[4200];
    long n = 0;
    for (const char *s = pauseDir; *s && n < 4096; s++) file[n++] = *s;
    const char *names[2] = { "/paused", "/go" };
    for (int step = 0; step < 2; step++) {
        long end = n;
        for (const char *s = names[step]; *s; s++) file[end++] = *s;
        file[end] = 0;
        if (step == 0) { long made = SYS(openat, AT_FDCWD, file, O_CREAT | O_WRONLY | O_CLOEXEC); if (made >= 0) SYS(close, made, 0, 0); continue; }
        for (long tries = 0; tries < 6000; tries++) {
            long seen = SYS(openat, AT_FDCWD, file, O_RDONLY | O_CLOEXEC);
            if (seen >= 0) { SYS(close, seen, 0, 0); break; }
            struct { long sec, nsec; } pause = { 0, 5000000 };
            SYS(nanosleep, &pause, 0, 0);
        }
    }
    pauseDir = 0;
}

/* One lease probe line; the exit status contribution. `t` is set in --watch mode. */
static long probe(char *file, struct target *t) {
    long status = 0;
    long fd = SYS(openat, AT_FDCWD, file, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd == -ENOENT) { out("absent "); }
    else if (fd < 0) { out("error "); number(-fd); out(" "); status = 2; }
    else if (t && !ownOpenRead(t)) {
        /* The probe's own IN_OPEN never arrived: nothing can be counted exactly. Fail closed. */
        SYS(close, fd, 0, 0);
        out("unwatched "); status = 2;
    }
    else {
        opened = 1; /* this probe's own IN_OPEN, read from the queue above (watch mode) */
        long lease = SYS(fcntl, fd, F_SETLEASE, F_WRLCK);
        if (lease == 0) SYS(fcntl, fd, F_SETLEASE, F_UNLCK); /* release at once */
        if (t) testPause();
        SYS(close, fd, 0, 0);
        if (lease == 0) out("free ");
        else if (lease == -EAGAIN) { out("held "); status = 3; }
        else { out("error "); number(-lease); out(" "); status = 2; }
    }
    out(file); out("\n");
    return status;
}

__attribute__((noreturn, used)) void fabric_start(long *stack) {
    long argc = stack[0];
    char **argv = (char **)(stack + 1);
    for (char **env = argv + argc + 1; *env; env++) {
        const char *pause = "FABRIC_MESH_LEASE_TEST_PAUSE_AFTER_UNLCK=", *ignore = "FABRIC_MESH_LEASE_TEST_IGNORE_PROBE_OPEN=1";
        long k = 0;
        while (pause[k] && (*env)[k] == pause[k]) k++;
        if (!pause[k] && (*env)[k]) pauseDir = *env + k;
        if (same(*env, ignore)) ignoreProbe = 1;
    }
    if (argc >= 2 && same(argv[1], "--watch")) watching(argc, argv);
    int status = argc < 2 ? 2 : 0;
    for (long i = 1; i < argc; i++) {
        long result = probe(argv[i], 0);
        if (result == 2 || (result == 3 && status == 0)) status = (int)result;
    }
    SYS(exit_group, status, 0, 0);
    __builtin_unreachable();
}
