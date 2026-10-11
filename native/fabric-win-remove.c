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
 * Every entry, and every ancestor of the target up to the volume root, is held WITHOUT FILE_SHARE_DELETE from
 * before the checks until exit (pi-fabric#913 round 2), so nothing in or above the tree can be renamed between
 * validation and the deletes. Then each is deleted BY HANDLE (FileDispositionInfoEx with POSIX semantics, else
 * FileDispositionInfo), so a name swapped after the check is never touched. An ancestor or entry already open by
 * someone with delete access cannot be held: the removal is refused (fail closed). One stdout line: "ok N", or "refused I REASON" for entry I
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
    /* No FILE_SHARE_DELETE (pi-fabric#913 round 2): while we hold it, nobody can rename or delete it. */
    HANDLE handle = CreateFileW(path, access, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
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

/* pi-fabric#913 round 2: every ancestor of the target, up to the volume root, held open WITHOUT FILE_SHARE_DELETE
 * (and checked: a real directory, not a reparse point) until exit, so no ancestor can be renamed or replaced
 * between validation and the deletes: the judged tree stays where it was judged. With every entry held the same
 * way, nothing in the tree can move either. */
static const char *holdAncestors(const wchar_t *target) {
    static wchar_t path[32768];
    size_t length = wcslen(target);
    if (length >= 32768) return "path too long";
    wcscpy(path, target);
    for (;;) {
        wchar_t *cut = wcsrchr(path, L'\\');
        if (!cut) return NULL;
        int root = cut > path && cut[-1] == L':';
        if (root) cut[1] = 0; else *cut = 0; /* "\\?\C:\" for the volume root */
        if (!*path || wcscmp(path, L"\\\\?") == 0) return NULL;
        HANDLE handle = CreateFileW(path, FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
        if (handle == INVALID_HANDLE_VALUE) return "an ancestor could not be held (in use with delete access, or gone)";
        BY_HANDLE_FILE_INFORMATION info;
        if (!GetFileInformationByHandle(handle, &info) || !(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) ||
            (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT)) return "an ancestor is not a real directory (a reparse point)";
        /* Held until the process exits. */
        if (root) return NULL;
    }
}

/* Tests only (FABRIC_WIN_REMOVE_TEST_PAUSE=DIR, inert unless set): after validation, create DIR\paused and wait
 * (at most 30 s) for DIR\go, so a test can try to move an entry or ancestor in exactly that window. */
static void testPause(void) {
    static wchar_t dir[4096], file[4200];
    DWORD got = GetEnvironmentVariableW(L"FABRIC_WIN_REMOVE_TEST_PAUSE", dir, 4096);
    if (got == 0 || got >= 4096) return;
    swprintf(file, 4200, L"%ls\\paused", dir);
    HANDLE marker = CreateFileW(file, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, 0, NULL);
    if (marker != INVALID_HANDLE_VALUE) CloseHandle(marker);
    swprintf(file, 4200, L"%ls\\go", dir);
    for (int tries = 0; tries < 6000 && GetFileAttributesW(file) == INVALID_FILE_ATTRIBUTES; tries++) Sleep(5);
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
    if ((why = holdAncestors(argv[first + 4 * (count - 1) + 3]))) return fail("refused", count - 1, why, 3);
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
    testPause();
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
