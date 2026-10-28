/* SPDX-License-Identifier: MIT
 * fabric-win-trace-move.exe SOURCE DEST: one non-replacing Windows rename.
 * Keep UTF-16 arguments intact, including extended-length path prefixes and case.
 * MoveFileExW with no flags fails if DEST exists; it cannot replace an entry or
 * fall back to a cross-volume copy. A source symbolic link moves as a link.
 * No existence probe, retry, copy, link/unlink, or post-move filesystem change.
 * stdout is exactly "moved" + LF (exit 0), or "error " + a positive decimal
 * GetLastError code + LF (exit 2). Invalid argument count is error 87.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdio.h>

static int fail(DWORD error) {
    char line[32];
    int length = snprintf(line, sizeof(line), "error %lu\n", (unsigned long)error);
    DWORD written;
    /* Bypass CRT text mode: the protocol requires one LF, never CRLF. If the
     * output channel fails, do not retry or attempt to reverse any move. */
    (void)WriteFile(GetStdHandle(STD_OUTPUT_HANDLE), line, (DWORD)length, &written, NULL);
    return 2;
}

int wmain(int argc, wchar_t **argv) {
    if (argc != 3) return fail(ERROR_INVALID_PARAMETER);
    /* No REPLACE_EXISTING, COPY_ALLOWED, DELAY_UNTIL_REBOOT, or other flags.
     * Pass paths directly to Windows; do not normalize or dereference them. */
    if (!MoveFileExW(argv[1], argv[2], 0)) return fail(GetLastError());
    DWORD written;
    (void)WriteFile(GetStdHandle(STD_OUTPUT_HANDLE), "moved\n", 6, &written, NULL);
    return 0;
}
