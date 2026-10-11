/* SPDX-License-Identifier: MIT
 * fabric-win-remove [--dry-run] KIND VOLUME INDEX PATH ... (smarty-dev#7858): verified removal on Windows.
 * Each entry is KIND ("f" file, "d" directory), the judged VOLUME serial and file INDEX (hex), and the PATH.
 * Entries come children first. Every entry is opened with FILE_FLAG_OPEN_REPARSE_POINT (a reparse point is
 * opened as itself, never followed) and FILE_FLAG_BACKUP_SEMANTICS, and ALL are verified on their handles
 * before any is deleted: not a reparse point, the judged kind, the judged volume serial and file index, and an
 * owner SID equal to this process token's user (or its default owner, the SID it stamps on what it creates).
 * Then each is deleted BY HANDLE (FileDispositionInfoEx with POSIX semantics, else FileDispositionInfo), so a
 * name swapped after the check is never touched. One stdout line: "ok N", or "refused I REASON" for entry I
 * (nothing deleted), or "failed I REASON" during deletion. Exit: 0 ok, 3 refused, 2 failed or usage.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <aclapi.h>
#include <stdio.h>
#include <wchar.h>

#define MAX_ENTRIES 4096
typedef struct { HANDLE handle; int directory; } Entry;
static Entry entries[MAX_ENTRIES];

static unsigned long long hex(const wchar_t *text, int *ok) {
    unsigned long long value = 0;
    *ok = *text != 0;
    for (; *text; text++) {
        int digit = *text >= L'0' && *text <= L'9' ? *text - L'0' : *text >= L'a' && *text <= L'f' ? *text - L'a' + 10 : -1;
        if (digit < 0 || value >> 60) { *ok = 0; return 0; }
        value = value << 4 | (unsigned)digit;
    }
    return value;
}

static PSID tokenSid(TOKEN_INFORMATION_CLASS kind, BYTE *buffer, DWORD size) {
    HANDLE token;
    DWORD length = 0;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) return NULL;
    BOOL got = GetTokenInformation(token, kind, buffer, size, &length);
    CloseHandle(token);
    if (!got) return NULL;
    return kind == TokenUser ? ((TOKEN_USER *)buffer)->User.Sid : ((TOKEN_OWNER *)buffer)->Owner;
}

/* Handles still open close at exit: an unverified entry is never marked for deletion. */
static int fail(const char *what, int index, const char *reason, int status) {
    printf("%s %d %s\n", what, index, reason);
    return status;
}

int wmain(int argc, wchar_t **argv) {
    int first = 1, dry = 0;
    if (argc > 1 && wcscmp(argv[1], L"--dry-run") == 0) { dry = 1; first = 2; }
    int count = (argc - first) / 4;
    if (count < 1 || count > MAX_ENTRIES || (argc - first) % 4 != 0) { printf("failed -1 usage\n"); return 2; }
    BYTE userBuffer[256], ownerBuffer[256];
    PSID user = tokenSid(TokenUser, userBuffer, sizeof(userBuffer));
    PSID owner = tokenSid(TokenOwner, ownerBuffer, sizeof(ownerBuffer));
    if (!user || !owner) { printf("failed -1 token\n"); return 2; }
    for (int i = 0; i < count; i++) {
        wchar_t **arg = argv + first + 4 * i;
        int okVolume, okIndex;
        unsigned long long volume = hex(arg[1], &okVolume), index = hex(arg[2], &okIndex);
        int directory = wcscmp(arg[0], L"d") == 0;
        if (!okVolume || !okIndex || (!directory && wcscmp(arg[0], L"f") != 0)) return fail("failed", i, "usage", 2);
        HANDLE handle = CreateFileW(arg[3], DELETE | READ_CONTROL | FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
        if (handle == INVALID_HANDLE_VALUE) return fail("refused", i, "open failed", 3);
        entries[i].handle = handle;
        entries[i].directory = directory;
        BY_HANDLE_FILE_INFORMATION info;
        if (!GetFileInformationByHandle(handle, &info)) return fail("refused", i, "no file information", 3);
        if (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) return fail("refused", i, "a reparse point", 3);
        if (!!(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != directory) return fail("refused", i, "type changed since judgment", 3);
        if (info.dwVolumeSerialNumber != volume || ((unsigned long long)info.nFileIndexHigh << 32 | info.nFileIndexLow) != index) {
            return fail("refused", i, "file id changed since judgment", 3);
        }
        PSID fileOwner = NULL;
        PSECURITY_DESCRIPTOR descriptor = NULL;
        if (GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &fileOwner, NULL, NULL, NULL, &descriptor) != ERROR_SUCCESS) {
            return fail("refused", i, "owner unreadable", 3);
        }
        BOOL ours = fileOwner && (EqualSid(fileOwner, user) || EqualSid(fileOwner, owner));
        LocalFree(descriptor);
        if (!ours) return fail("refused", i, "not owned by this user", 3);
    }
    if (dry) return fail("ok", count, "verified", 0);
    for (int i = 0; i < count; i++) {
        FILE_DISPOSITION_INFO_EX extended = { FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS };
        FILE_DISPOSITION_INFO plain = { TRUE };
        if (!SetFileInformationByHandle(entries[i].handle, FileDispositionInfoEx, &extended, sizeof(extended)) &&
            !SetFileInformationByHandle(entries[i].handle, FileDispositionInfo, &plain, sizeof(plain))) {
            return fail("failed", i, "delete failed", 2);
        }
        CloseHandle(entries[i].handle); /* the delete completes at close: before its parent directory */
        entries[i].handle = NULL;
    }
    printf("ok %d removed\n", count);
    return 0;
}
