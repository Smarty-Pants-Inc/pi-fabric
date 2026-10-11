/* SPDX-License-Identifier: MIT
 * fabric-win-remove [--dry-run] KIND VOLUME INDEX PATH ... (smarty-dev#7858): verified removal on Windows.
 * Each entry is KIND ("f" file, "d" directory), the judged VOLUME serial and file INDEX (hex), and the PATH.
 * Entries come children first; the LAST is the judged target and every other PATH lies below it. Every entry is
 * opened with FILE_FLAG_OPEN_REPARSE_POINT (a reparse point is opened as itself, never followed) and
 * FILE_FLAG_BACKUP_SEMANTICS, and ALL are verified on their handles before any is deleted: not a reparse point,
 * the judged kind, the judged volume serial and file index, an owner SID equal to this process token's user (or
 * its default owner, the SID it stamps on what it creates), and (pi-fabric#913 P1) bound to the judged tree: its
 * final path (GetFinalPathNameByHandle, every reparse point in it resolved) must be the target's final path plus
 * its own relative name, so an ancestor swapped for a junction after the walk is refused, never followed.
 * Then each is deleted BY HANDLE (FileDispositionInfoEx with POSIX semantics, else FileDispositionInfo), so a
 * name swapped after the check is never touched. One stdout line: "ok N", or "refused I REASON" for entry I
 * (nothing deleted), or "failed I REASON" during deletion. Exit: 0 ok, 3 refused, 2 failed or usage.
 *
 * fabric-win-remove --private VOLUME INDEX PATH (pi-fabric#913 P1): a private record (a retention approval or the
 * mesh state): the same open and id, owner and reparse checks, and no SID but ours, SYSTEM or Administrators may
 * write it (any allow ACE outside the read-only rights for another SID, an unknown ACE type or a null DACL refuses).
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <aclapi.h>
#include <stdio.h>
#include <wchar.h>

#define MAX_ENTRIES 4096
/* Read, traverse and synchronize only (the same set as src/storage/windows-temp-root.ts). */
#define READ_ONLY_RIGHTS 0xa01200a9u
typedef struct { HANDLE handle; int directory; } Entry;
static Entry entries[MAX_ENTRIES];
static PSID user, owner;

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

static int trusted(PSID sid) {
    return EqualSid(sid, user) || EqualSid(sid, owner) || IsWellKnownSid(sid, WinLocalSystemSid) || IsWellKnownSid(sid, WinBuiltinAdministratorsSid);
}

/* Open without following a reparse point and check kind, id and owner; NULL and *why set when refused. */
static HANDLE openJudged(const wchar_t *path, DWORD access, int directory, const wchar_t *volumeText, const wchar_t *indexText, const char **why) {
    int okVolume, okIndex;
    unsigned long long volume = hex(volumeText, &okVolume), index = hex(indexText, &okIndex);
    if (!okVolume || !okIndex) { *why = "usage"; return NULL; }
    HANDLE handle = CreateFileW(path, access, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
    if (handle == INVALID_HANDLE_VALUE) { *why = "open failed"; return NULL; }
    BY_HANDLE_FILE_INFORMATION info;
    PSID fileOwner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    *why = !GetFileInformationByHandle(handle, &info) ? "no file information"
        : info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT ? "a reparse point"
        : directory >= 0 && !!(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != directory ? "type changed since judgment"
        : info.dwVolumeSerialNumber != volume || ((unsigned long long)info.nFileIndexHigh << 32 | info.nFileIndexLow) != index
          ? "file id changed since judgment"
        : GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &fileOwner, NULL, NULL, NULL, &descriptor) != ERROR_SUCCESS
          ? "owner unreadable"
        : !fileOwner || !(EqualSid(fileOwner, user) || EqualSid(fileOwner, owner)) ? "not owned by this user" : NULL;
    if (descriptor) LocalFree(descriptor);
    if (*why) { CloseHandle(handle); return NULL; }
    return handle;
}

/* No SID but ours, SYSTEM or Administrators may write: NULL when private, else why not. */
static const char *privateDacl(HANDLE handle) {
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    if (GetSecurityInfo(handle, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, NULL, NULL, &dacl, NULL, &descriptor) != ERROR_SUCCESS) return "DACL unreadable";
    const char *why = dacl ? NULL : "a null DACL (everyone may write)";
    for (DWORD i = 0; !why && i < dacl->AceCount; i++) {
        ACE_HEADER *header;
        if (!GetAce(dacl, i, (void **)&header)) { why = "DACL unreadable"; break; }
        if (header->AceFlags & INHERIT_ONLY_ACE || header->AceType == ACCESS_DENIED_ACE_TYPE) continue;
        if (header->AceType != ACCESS_ALLOWED_ACE_TYPE) { why = "an unsupported ACE type"; break; }
        ACCESS_ALLOWED_ACE *ace = (ACCESS_ALLOWED_ACE *)header;
        if ((ace->Mask & ~READ_ONLY_RIGHTS) && !trusted((PSID)&ace->SidStart)) why = "writable by another SID";
    }
    LocalFree(descriptor);
    return why;
}

int wmain(int argc, wchar_t **argv) {
    BYTE userBuffer[256], ownerBuffer[256];
    user = tokenSid(TokenUser, userBuffer, sizeof(userBuffer));
    owner = tokenSid(TokenOwner, ownerBuffer, sizeof(ownerBuffer));
    if (!user || !owner) { printf("failed -1 token\n"); return 2; }
    const char *why = NULL;
    if (argc == 5 && wcscmp(argv[1], L"--private") == 0) {
        HANDLE handle = openJudged(argv[4], READ_CONTROL | FILE_READ_ATTRIBUTES, 0, argv[2], argv[3], &why);
        if (!handle) return fail("refused", 0, why, 3);
        why = privateDacl(handle);
        CloseHandle(handle);
        return why ? fail("refused", 0, why, 3) : fail("ok", 1, "private", 0);
    }
    int first = 1, dry = 0;
    if (argc > 1 && wcscmp(argv[1], L"--dry-run") == 0) { dry = 1; first = 2; }
    int count = (argc - first) / 4;
    if (count < 1 || count > MAX_ENTRIES || (argc - first) % 4 != 0) { printf("failed -1 usage\n"); return 2; }
    for (int i = 0; i < count; i++) {
        wchar_t **arg = argv + first + 4 * i;
        int directory = wcscmp(arg[0], L"d") == 0;
        if (!directory && wcscmp(arg[0], L"f") != 0) return fail("failed", i, "usage", 2);
        HANDLE handle = openJudged(arg[3], DELETE | READ_CONTROL | FILE_READ_ATTRIBUTES, directory, arg[1], arg[2], &why);
        if (!handle) return fail("refused", i, why, 3);
        entries[i].handle = handle;
        entries[i].directory = directory;
    }
    /* Bind every descendant to the judged tree: its final path is the target's final path plus its relative name. */
    static wchar_t top[32768], final[32768];
    const wchar_t *topPath = argv[first + 4 * (count - 1) + 3];
    size_t topLength = wcslen(topPath);
    DWORD length = GetFinalPathNameByHandleW(entries[count - 1].handle, top, 32768, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
    if (length == 0 || length >= 32768) return fail("refused", count - 1, "final path unreadable", 3);
    for (int i = 0; i < count - 1; i++) {
        const wchar_t *path = argv[first + 4 * i + 3];
        if (wcsncmp(path, topPath, topLength) != 0 || path[topLength] != L'\\') return fail("failed", i, "usage: not below the target", 2);
        DWORD got = GetFinalPathNameByHandleW(entries[i].handle, final, 32768, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
        if (got == 0 || got >= 32768) return fail("refused", i, "final path unreadable", 3);
        if (got != length + wcslen(path + topLength) || _wcsnicmp(final, top, length) != 0 || _wcsicmp(final + length, path + topLength) != 0) {
            return fail("refused", i, "outside the judged tree (a reparse point in its path)", 3);
        }
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
