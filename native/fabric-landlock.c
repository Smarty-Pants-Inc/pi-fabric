/* SPDX-License-Identifier: MIT
 * ponytail: landrun is a no-root static Go CLI candidate, but no pinned binary,
 * license or ABI-v4 contract is available in this offline lane. This small
 * freestanding adapter is static WITHOUT libc (no runtime/library dependency).
 * It confines BEFORE Bash, including BASH_ENV, and never retries unconfined.
 */
#include <asm/unistd.h>
#include <linux/fcntl.h>
#include <linux/landlock.h>
#include <linux/prctl.h>

#ifndef LANDLOCK_ACCESS_FS_REFER
#define LANDLOCK_ACCESS_FS_REFER (1ULL << 13)
#endif
#ifndef LANDLOCK_ACCESS_FS_TRUNCATE
#define LANDLOCK_ACCESS_FS_TRUNCATE (1ULL << 14)
#endif

/* Linux process entry and syscall ABI; no C runtime or dynamic loader. */
#if defined(__x86_64__)
__asm__(".global _start\n_start:\nxor %rbp,%rbp\nmov %rsp,%rdi\nand $-16,%rsp\ncall fabric_start\n");
static long call(long nr, long a, long b, long c, long d, long e) {
    register long r10 __asm__("r10") = d;
    register long r8 __asm__("r8") = e;
    register long r9 __asm__("r9") = 0;
    long result;
    __asm__ volatile("syscall" : "=a"(result) : "a"(nr), "D"(a), "S"(b),
        "d"(c), "r"(r10), "r"(r8), "r"(r9) : "rcx", "r11", "memory");
    return result;
}
#elif defined(__aarch64__)
__asm__(".global _start\n_start:\nmov x0,sp\nbl fabric_start\n");
static long call(long nr, long a, long b, long c, long d, long e) {
    register long x0 __asm__("x0") = a, x1 __asm__("x1") = b;
    register long x2 __asm__("x2") = c, x3 __asm__("x3") = d;
    register long x4 __asm__("x4") = e, x5 __asm__("x5") = 0;
    register long x8 __asm__("x8") = nr;
    __asm__ volatile("svc 0" : "+r"(x0) : "r"(x1), "r"(x2), "r"(x3),
        "r"(x4), "r"(x5), "r"(x8) : "memory");
    return x0;
}
#else
#error "fabric-landlock supports Linux x86_64 and aarch64; add a reviewed syscall/entry ABI for this architecture"
#endif
#define SYS(n, a, b, c, d, e) call(__NR_##n, (long)(a), (long)(b), (long)(c), (long)(d), (long)(e))

static void text(long fd, const char *s) {
    long length = 0;
    while (s[length]) length++;
    SYS(write, fd, s, length, 0, 0);
}
static void number(long fd, unsigned long value) {
    char digits[24];
    unsigned int end = sizeof(digits), start = end;
    do { digits[--start] = '0' + value % 10; value /= 10; } while (value);
    SYS(write, fd, digits + start, end - start, 0, 0);
}
static __attribute__((noreturn)) void fail(const char *operation, long error) {
    text(2, "fabric-landlock: "); text(2, operation); text(2, ": errno=");
    number(2, error < 0 ? (unsigned long)-error : (unsigned long)error);
    text(2, " (no unconfined retry)\n");
    SYS(exit_group, 125, 0, 0, 0, 0);
    __builtin_unreachable();
}
static char *value(char **env, const char *key) {
    for (; *env; env++) {
        unsigned int i = 0;
        while (key[i] && key[i] == (*env)[i]) i++;
        if (!key[i] && (*env)[i] == '=') return *env + i + 1;
    }
    return 0;
}
static int equal(const char *a, const char *b) {
    while (*a && *a == *b) { a++; b++; }
    return *a == *b;
}
static __attribute__((noreturn)) void execute(char *shell, char *command, char **env) {
    char *args[] = { shell, "-c", command, 0 };
    for (char *p = shell; *p; p++) {
        if (*p == '/') fail("exec shell", SYS(execve, shell, args, env, 0, 0));
    }
    /* Preserve execvp semantics for Pi's final PATH-based 'sh' fallback. */
    char *search = value(env, "PATH");
    if (!search) search = "/bin:/usr/bin";
    long error = -2;
    do {
        char candidate[4096];
        unsigned int length = 0;
        while (*search && *search != ':') {
            if (length < sizeof(candidate) - 1) candidate[length++] = *search;
            search++;
        }
        if (length && length < sizeof(candidate) - 1) candidate[length++] = '/';
        for (char *p = shell; *p && length < sizeof(candidate) - 1; p++) candidate[length++] = *p;
        candidate[length] = 0;
        error = SYS(execve, candidate, args, env, 0, 0);
    } while (*search && ++search);
    fail("exec shell on PATH", error);
}

__attribute__((used, noreturn)) void fabric_start(long *stack) {
    long argc = stack[0];
    char **argv = (char **)(stack + 1), **env = argv + argc + 1;
    long version = SYS(landlock_create_ruleset, 0, 0, LANDLOCK_CREATE_RULESET_VERSION, 0, 0);
    if (argc == 2 && equal(argv[1], "--abi")) {
        if (version < 0) fail("query ABI", version);
        number(1, (unsigned long)version); text(1, "\n");
        SYS(exit_group, 0, 0, 0, 0, 0); __builtin_unreachable();
    }
    if (argc != 3 || !equal(argv[1], "-c")) fail("expected -c COMMAND", -22);
    if (version < 4) fail("Linux Landlock ABI >=4 required", version < 0 ? version : -95);
    char *shell = value(env, "PI_FABRIC_LANDLOCK_SHELL");
    char *writes = value(env, "PI_FABRIC_LANDLOCK_WRITES");
    if (!shell || !*shell || !writes || !*writes) fail("missing trusted launch policy", -22);
    const __u64 handled = LANDLOCK_ACCESS_FS_WRITE_FILE |
        LANDLOCK_ACCESS_FS_REMOVE_DIR | LANDLOCK_ACCESS_FS_REMOVE_FILE |
        LANDLOCK_ACCESS_FS_MAKE_CHAR | LANDLOCK_ACCESS_FS_MAKE_DIR |
        LANDLOCK_ACCESS_FS_MAKE_REG | LANDLOCK_ACCESS_FS_MAKE_SOCK |
        LANDLOCK_ACCESS_FS_MAKE_FIFO | LANDLOCK_ACCESS_FS_MAKE_BLOCK |
        LANDLOCK_ACCESS_FS_MAKE_SYM | LANDLOCK_ACCESS_FS_REFER |
        LANDLOCK_ACCESS_FS_TRUNCATE;
    long ruleset = SYS(landlock_create_ruleset, &handled, sizeof(handled), 0, 0, 0);
    if (ruleset < 0) fail("create ruleset", ruleset);
    for (char *entry = writes; *entry;) {
        char *end = entry;
        while (*end && *end != '\n') end++;
        char delimiter = *end; *end = 0;
        if (*entry != '/') fail("non-absolute grant", -22);
        long parent = SYS(openat, AT_FDCWD, entry, O_PATH | O_CLOEXEC | O_DIRECTORY, 0, 0);
        int directory = parent >= 0;
        if (parent == -20) parent = SYS(openat, AT_FDCWD, entry, O_PATH | O_CLOEXEC, 0, 0);
        if (parent < 0) fail("open grant", parent);
        struct landlock_path_beneath_attr rule = { .parent_fd = (int)parent,
            .allowed_access = directory ? handled : LANDLOCK_ACCESS_FS_WRITE_FILE | LANDLOCK_ACCESS_FS_TRUNCATE };
        long result = SYS(landlock_add_rule, ruleset, LANDLOCK_RULE_PATH_BENEATH, &rule, 0, 0);
        if (result < 0) fail("add grant", result);
        SYS(close, parent, 0, 0, 0, 0);
        *end = delimiter;
        if (!delimiter) break;
        entry = end + 1;
    }
    long result = SYS(prctl, PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
    if (result < 0) fail("no_new_privs", result);
    result = SYS(landlock_restrict_self, ruleset, 0, 0, 0, 0);
    if (result < 0) fail("restrict self", result);
    SYS(close, ruleset, 0, 0, 0, 0);
    result = SYS(close_range, 3U, ~0U, 0, 0, 0);
    if (result < 0) fail("close inherited handles", result);
    char **kept = env;
    for (char **item = env; *item; item++) {
        char *one[] = { *item, 0 };
        if (!value(one, "PI_FABRIC_LANDLOCK_SHELL") && !value(one, "PI_FABRIC_LANDLOCK_WRITES")
            && !value(one, "PI_FABRIC_LANDLOCK_ESCAPE")) *kept++ = *item;
    }
    *kept = 0;
    execute(shell, argv[2], env);
}
