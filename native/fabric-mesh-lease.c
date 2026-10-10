/* SPDX-License-Identifier: MIT
 * fabric-mesh-lease FILE...: is any OTHER open file description of each FILE alive? (smarty-dev#7936)
 * A write lease (F_SETLEASE F_WRLCK) is refused with EAGAIN while any other process (dumpable or not,
 * an fd or only an mmap) holds the inode open. It is released and closed at once, so no opener is
 * blocked for longer than the probe. Per FILE one stdout line: "free", "held", "absent" or "error <errno>",
 * then the path. Exit: 0 all free or absent, 3 any held, 2 any error. Static, without libc, as
 * fabric-landlock (same build).
 */
#include <asm/unistd.h>
#include <linux/fcntl.h>

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

__attribute__((noreturn, used)) void fabric_start(long *stack) {
    long argc = stack[0];
    char **argv = (char **)(stack + 1);
    int status = argc < 2 ? 2 : 0;
    for (long i = 1; i < argc; i++) {
        long fd = SYS(openat, AT_FDCWD, argv[i], O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (fd == -ENOENT) { out("absent "); }
        else if (fd < 0) { out("error "); number(-fd); out(" "); status = 2; }
        else {
            long lease = SYS(fcntl, fd, F_SETLEASE, F_WRLCK);
            if (lease == 0) SYS(fcntl, fd, F_SETLEASE, F_UNLCK); /* release at once */
            SYS(close, fd, 0, 0);
            if (lease == 0) out("free ");
            else if (lease == -EAGAIN) { out("held "); if (status == 0) status = 3; }
            else { out("error "); number(-lease); out(" "); status = 2; }
        }
        out(argv[i]); out("\n");
    }
    SYS(exit_group, status, 0, 0);
    __builtin_unreachable();
}
