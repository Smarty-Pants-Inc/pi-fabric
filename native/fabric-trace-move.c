/* SPDX-License-Identifier: MIT
 * fabric-trace-move SOURCE DEST: one Linux renameat2(RENAME_NOREPLACE).
 * Stdout: "moved" + LF, exit 0; or "error " + positive errno + LF, exit 2.
 * Wrong argc reports EINVAL. Paths are passed unchanged to the kernel; a final
 * symlink is moved literally. No overwrite, copy, link/unlink, fallback, retry,
 * or post-move filesystem operation. Static, without libc, as fabric-mesh-lease.
 */
#include <asm/unistd.h>
#include <linux/errno.h>
#include <linux/fcntl.h>
#include <linux/fs.h>

#if defined(__x86_64__)
__asm__(".global _start\n_start:\nxor %rbp,%rbp\nmov %rsp,%rdi\nand $-16,%rsp\ncall fabric_start\n");
static long call(long nr, long a, long b, long c, long d, long e) {
    long result;
    register long r10 __asm__("r10") = d, r8 __asm__("r8") = e;
    __asm__ volatile("syscall" : "=a"(result)
        : "a"(nr), "D"(a), "S"(b), "d"(c), "r"(r10), "r"(r8)
        : "rcx", "r11", "memory");
    return result;
}
#elif defined(__aarch64__)
__asm__(".global _start\n_start:\nmov x0,sp\nbl fabric_start\n");
static long call(long nr, long a, long b, long c, long d, long e) {
    register long x0 __asm__("x0") = a, x1 __asm__("x1") = b, x2 __asm__("x2") = c,
        x3 __asm__("x3") = d, x4 __asm__("x4") = e, x8 __asm__("x8") = nr;
    __asm__ volatile("svc 0" : "+r"(x0)
        : "r"(x1), "r"(x2), "r"(x3), "r"(x4), "r"(x8) : "memory");
    return x0;
}
#else
#error "fabric-trace-move supports Linux x86_64 and aarch64"
#endif
#define SYS(n, a, b, c, d, e) call(__NR_##n, (long)(a), (long)(b), (long)(c), (long)(d), (long)(e))

__attribute__((noreturn)) static void fail(unsigned long error) {
    char line[32];
    unsigned int at = sizeof(line);
    line[--at] = '\n';
    do { line[--at] = '0' + error % 10; error /= 10; } while (error);
    line[--at] = ' '; line[--at] = 'r'; line[--at] = 'o';
    line[--at] = 'r'; line[--at] = 'r'; line[--at] = 'e';
    SYS(write, 1, line + at, sizeof(line) - at, 0, 0);
    SYS(exit_group, 2, 0, 0, 0, 0);
    __builtin_unreachable();
}

__attribute__((noreturn, used)) void fabric_start(long *stack) {
    long argc = stack[0];
    char **argv = (char **)(stack + 1);
    if (argc != 3) fail(EINVAL);
    long result = SYS(renameat2, AT_FDCWD, argv[1], AT_FDCWD, argv[2], RENAME_NOREPLACE);
    if (result < 0) fail((unsigned long)-result);
    SYS(write, 1, "moved\n", 6, 0, 0);
    SYS(exit_group, 0, 0, 0, 0, 0);
    __builtin_unreachable();
}
