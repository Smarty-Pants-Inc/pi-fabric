/* SPDX-License-Identifier: MIT
 * fabric-mesh-lease FILE...: is any OTHER open file description of each FILE alive? (smarty-dev#7936)
 * A write lease (F_SETLEASE F_WRLCK) is refused with EAGAIN while any other process (dumpable or not,
 * an fd or only an mmap) holds the inode open. It is released and closed at once, so no opener is
 * blocked for longer than the probe. Per FILE one stdout line: "free", "held", "absent" or "error <errno>",
 * then the path. Exit: 0 all free or absent, 3 any held, 2 any error. Static, without libc, as
 * fabric-landlock (same build).
 *
 * fabric-mesh-lease --watch CONTROLDIR FILE... (smarty-dev#8116): inotify watches on each FILE's directory
 * and on each existing FILE's inode go up BEFORE the same lease probe, so an open after the probe is an
 * event, whatever the opener's dumpability or locks. After the probe lines it prints "watching". Each file
 * created in CONTROLDIR asks for a report: per FILE "net OPENS CLOSES FILE" (by name, in its directory),
 * "alias FILE" (its inode opened through another name: a hard link), "moved FILE", then "overflow" (lost
 * events) and "newdir" (a missing directory appeared) when they apply, and "end N". The caller subtracts
 * its own open fds. It dies with its parent (PR_SET_PDEATHSIG).
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
#define FILE_EVENTS (IN_OPEN | IN_CLOSE | IN_DONT_FOLLOW)
#define DIR_EVENTS (IN_OPEN | IN_CLOSE | IN_CREATE | IN_DELETE | IN_MOVE | IN_ONLYDIR | IN_DONT_FOLLOW)
static struct target { char *path; long dir, wd; unsigned long opens, closes, inode, seen; int gen, alias, moved; } T[MAXF];
static char dirs[MAXF][4096];
static long D[MAXF], nfiles, ndirs, inotify;
static int overflow, newdir;

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
        out("net "); number(t->opens); out(" "); number(t->closes); out(" "); out(t->path); out("\n");
        if (t->alias) { out("alias "); out(t->path); out("\n"); }
        if (t->moved) { out("moved "); out(t->path); out("\n"); }
    }
    if (overflow) out("overflow\n");
    if (newdir) out("newdir\n");
    out("end "); number(seq); out("\n");
}

static void event(const struct inotify_event *e, long control, unsigned long *seq) {
    if (e->mask & IN_Q_OVERFLOW) overflow = 1;
    if (e->wd == control) { if ((e->mask & IN_CREATE) && e->len) report(++*seq); return; }
    for (long d = 0; d < ndirs; d++) {
        if (D[d] != e->wd || !e->len) continue;
        if (e->mask & IN_ISDIR) {
            if (e->mask & (IN_CREATE | IN_MOVED_TO))
                for (long m = 0; m < ndirs; m++) if (D[m] < 0 && same(dirs[m] + slash(dirs[m]) + 1, e->name)) newdir = 1;
            continue;
        }
        for (long i = 0; i < nfiles; i++) {
            struct target *t = &T[i];
            if (t->dir != d || !same(t->path + slash(t->path) + 1, e->name)) continue;
            if (e->mask & IN_OPEN) { t->opens++; if (t->gen) t->seen++; }
            if (e->mask & IN_CLOSE) t->closes++;
            if (e->mask & IN_MOVE) t->moved = 1;
            if (e->mask & (IN_CREATE | IN_DELETE)) { endgen(t); if (e->mask & IN_CREATE) watch(t); }
        }
    }
    for (long i = 0; i < nfiles; i++) if (T[i].gen && T[i].wd == e->wd && (e->mask & IN_OPEN)) T[i].inode++;
}

static long probe(char *file);

__attribute__((noreturn)) static void watching(long argc, char **argv) {
    if (argc < 4 || argc - 3 > MAXF) fail("usage: --watch CONTROLDIR FILE...", -22);
    SYS(prctl, PR_SET_PDEATHSIG, 9, 0);
    inotify = SYS(inotify_init1, IN_CLOEXEC, 0, 0);
    if (inotify < 0) fail("inotify_init1", inotify);
    long control = SYS(inotify_add_watch, inotify, argv[2], IN_CREATE | IN_ONLYDIR | IN_DONT_FOLLOW);
    if (control < 0) fail(argv[2], control);
    for (long i = 3; i < argc; i++) {
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
    for (long i = 3; i < argc; i++) probe(argv[i]);
    out("watching\n");
    static char buffer[4096] __attribute__((aligned(8)));
    unsigned long seq = 0;
    for (;;) {
        long n = SYS(read, inotify, buffer, sizeof(buffer));
        if (n <= 0) fail("read inotify", n < 0 ? n : -5);
        for (long at = 0; at < n;) {
            const struct inotify_event *e = (const struct inotify_event *)(buffer + at);
            event(e, control, &seq);
            at += sizeof(*e) + e->len;
        }
    }
}

/* One lease probe line; the exit status contribution. */
static long probe(char *file) {
    long status = 0;
    long fd = SYS(openat, AT_FDCWD, file, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd == -ENOENT) { out("absent "); }
    else if (fd < 0) { out("error "); number(-fd); out(" "); status = 2; }
    else {
        long lease = SYS(fcntl, fd, F_SETLEASE, F_WRLCK);
        if (lease == 0) SYS(fcntl, fd, F_SETLEASE, F_UNLCK); /* release at once */
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
    if (argc >= 2 && same(argv[1], "--watch")) watching(argc, argv);
    int status = argc < 2 ? 2 : 0;
    for (long i = 1; i < argc; i++) {
        long result = probe(argv[i]);
        if (result == 2 || (result == 3 && status == 0)) status = (int)result;
    }
    SYS(exit_group, status, 0, 0);
    __builtin_unreachable();
}
