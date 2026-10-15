/* SPDX-License-Identifier: MIT
 * Hosted-Windows T8 EXTERNAL handle fixture, not a deleter. argv: path access-hex share-hex deadline-ms.
 * Real OPEN_EXISTING only; no writes, rename or disposition. stdin EOF is the normal last-close request.
 * An independent waitable-timer thread bounds even a blocked ReadFile; the parent also has an OS timeout.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

static HANDLE watchdogTimer;

static DWORD WINAPI watchdog(LPVOID unused) {
    (void)unused;
    if (WaitForSingleObject(watchdogTimer, INFINITE) == WAIT_OBJECT_0) TerminateProcess(GetCurrentProcess(), 124);
    return 0;
}

static int number(const wchar_t *text, int base, unsigned long long maximum, DWORD *value) {
    wchar_t *end;
    if (!*text || *text == L'-' || *text == L'+') return 0;
    for (const wchar_t *p = text; *p; p++) {
        if (!(*p >= L'0' && *p <= L'9') && !(base == 16 && *p >= L'a' && *p <= L'f')) return 0;
    }
    errno = 0;
    unsigned long long parsed = wcstoull(text, &end, base);
    if (errno || *end || parsed > maximum) return 0;
    *value = (DWORD)parsed;
    return 1;
}

static int output(const char *line, int length) {
    DWORD written = 0;
    return length > 0 && WriteFile(GetStdHandle(STD_OUTPUT_HANDLE), line, (DWORD)length, &written, NULL) && written == (DWORD)length;
}

static int failure(const char *phase, DWORD error) {
    fprintf(stderr, "external handle fixture: %s error=%lu\n", phase, (unsigned long)error);
    return 3;
}

int wmain(int argc, wchar_t **argv) {
    DWORD access, share, deadline;
    if (argc != 5 || !number(argv[2], 16, MAXDWORD, &access) || !number(argv[3], 16, 7, &share) ||
        !number(argv[4], 10, 18000, &deadline) || !deadline) return failure("usage", ERROR_INVALID_PARAMETER);

    watchdogTimer = CreateWaitableTimerW(NULL, TRUE, NULL);
    if (!watchdogTimer) return failure("timer", GetLastError());
    LARGE_INTEGER due;
    due.QuadPart = -(LONGLONG)deadline * 10000;
    if (!SetWaitableTimer(watchdogTimer, &due, 0, NULL, NULL, FALSE)) return failure("arm", GetLastError());
    HANDLE thread = CreateThread(NULL, 0, watchdog, NULL, 0, NULL);
    if (!thread) return failure("watchdog", GetLastError());
    /* The watchdog's thread HANDLE is not leaked or included in either census. Its thread remains process-owned. */
    if (!CloseHandle(thread)) return failure("thread-handle-close", GetLastError());
    DWORD baseline = 0, held = 0, closed = 0;
    if (!GetProcessHandleCount(GetCurrentProcess(), &baseline)) return failure("baseline-count", GetLastError());

    HANDLE file = CreateFileW(argv[1], access, share, NULL, OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (file == INVALID_HANDLE_VALUE) return failure("open", GetLastError());
    BY_HANDLE_FILE_INFORMATION info;
    if (!GetFileInformationByHandle(file, &info)) return failure("information", GetLastError());
    if (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) return failure("reparse-point", ERROR_REPARSE_TAG_INVALID);
    if (!GetProcessHandleCount(GetCurrentProcess(), &held)) return failure("held-count", GetLastError());
    if (held != baseline + 1) return failure("held-count-delta", ERROR_INVALID_DATA);
    char line[512];
    int length = snprintf(line, sizeof(line),
        "{\"event\":\"ready\",\"pid\":%lu,\"access\":%lu,\"share\":%lu,\"volume\":\"%lx\",\"index\":\"%llx\","
        "\"directory\":%s,\"baselineHandles\":%lu,\"heldHandles\":%lu}\n",
        (unsigned long)GetCurrentProcessId(), (unsigned long)access, (unsigned long)share,
        (unsigned long)info.dwVolumeSerialNumber, (unsigned long long)info.nFileIndexHigh << 32 | info.nFileIndexLow,
        info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ? "true" : "false", (unsigned long)baseline, (unsigned long)held);
    if (length <= 0 || length >= (int)sizeof(line) || !output(line, length)) return failure("ready-write", GetLastError());

    DWORD readError = ERROR_SUCCESS;
    for (;;) {
        char bytes[128];
        DWORD got = 0;
        if (!ReadFile(GetStdHandle(STD_INPUT_HANDLE), bytes, sizeof(bytes), &got, NULL)) {
            readError = GetLastError();
            if (readError == ERROR_BROKEN_PIPE) readError = ERROR_SUCCESS;
            break;
        }
        if (!got) break;
    }
    if (!CloseHandle(file)) return failure("file-close", GetLastError());
    if (!GetProcessHandleCount(GetCurrentProcess(), &closed)) return failure("closed-count", GetLastError());
    length = snprintf(line, sizeof(line),
        "{\"event\":\"closed\",\"pid\":%lu,\"baselineHandles\":%lu,\"heldHandles\":%lu,\"closedHandles\":%lu}\n",
        (unsigned long)GetCurrentProcessId(), (unsigned long)baseline, (unsigned long)held, (unsigned long)closed);
    if (length <= 0 || length >= (int)sizeof(line) || !output(line, length)) return failure("closed-write", GetLastError());
    if (readError) return failure("stdin", readError);
    if (closed != baseline) return failure("closed-count-delta", ERROR_INVALID_DATA);
    return 0;
}
