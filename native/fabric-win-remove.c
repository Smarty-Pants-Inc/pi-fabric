/* SPDX-License-Identifier: MIT
 * fabric-win-remove [--dry-run] KIND VOLUME INDEX MTIME SIZE PATH ... (smarty-dev#7858): verified removal on Windows.
 * Each entry is KIND ("f" file, "d" directory), the judged VOLUME serial and file INDEX, the judged last-write
 * time (a FILETIME) and SIZE (all hex), and the PATH. A FILE is opened without FILE_SHARE_WRITE too, so nobody can
 * write it while we hold it, and its last-write time and size must equal the judged ones (pi-fabric#913 round 4):
 * a file refreshed after the judgment, with the same id, is kept ("kept I modified since judgment", exit 4).
 * ponytail: a directory's own time is not compared; a child added to it after the walk makes its delete fail.
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
 * (nothing deleted), "kept I REASON" (nothing deleted), or "failed I REASON" during deletion. Exit: 0 ok, 3
 * refused, 4 kept, 2 failed or usage.
 *
 * fabric-win-remove --private VOLUME INDEX PATH (pi-fabric#913 P1): a private record (a retention approval or the
 * mesh state): the same open and id, owner and reparse checks, and no SID but ours, SYSTEM or Administrators may
 * write it (any allow ACE outside the read-only rights for another SID, an unknown ACE type or a null DACL refuses).
 *
 * smarty-dev#9569 (Commit A) mesh lock release, retire-before-dispose. Four more modes share the checks above:
 *
 * fabric-win-remove --capability-v1: prints "FMRCAP1 mesh-retire-v1 abi=1" (the versioned capability query).
 *
 * fabric-win-remove --cache-private-v1 P1 ... Pn (1 <= n <= 8): the retained helper cache chain, checked by the TRUSTED
 * packaged bootstrap (never by the cache executable itself). Every Pi is a direct child of P(i-1), opened as itself
 * (no reparse point followed): P1..P(n-1) real directories, Pn a regular file; each owned by this token's user (or
 * default owner) and privateDacl; every ancestor of P1 is held as a real directory with trusted ownership and no
 * untrusted replacement/ACL-takeover rights (create-only and inherit-only ACEs are harmless); the final path of Pn is
 * Pn itself. "ok private" exit 0, else "refused I REASON" exit 3.
 *
 * fabric-win-remove --cache-create-v1 P1 [P2]: trusted bootstrap only, before Node writes cache bytes. Create missing
 * direct-child directories with a protected, inheritable user/owner/SYSTEM/Administrators-only DACL. Existing entries
 * are checked, NEVER ACL-healed (even when empty). Hold ancestors and each component; verify owner, DACL, final path
 * and real-directory kind on readback. "ok created" exit 0, else "refused I REASON" exit 3.
 *
 * fabric-win-remove --cache-publish-v1 STAGING DEST: publish a closed and rehashed staging directory at DEST, a sibling,
 * with MoveFileExW and NO replace flag: an existing DEST is never replaced ("exists", exit 6). "ok published" exit 0.
 *
 * fabric-win-remove --mesh-retire-v1: one bounded framed request on stdin (the paths, the exact ids and the ORIGINAL
 * owner record bytes; format at meshRetire), then: W2 hold ancestors, open the lock directory L with DELETE and NO
 * delete sharing and verify its exact id, kind, SID, no reparse point and final path; W3 open L\owner without write
 * or delete sharing, verify its exact id, time, size and the full original bytes on the handle, bind it below L and
 * require the entry set of L (read on the held handle) to be exactly {owner}; close the owner handle (the directory
 * guard stays); W4 COMMIT: rename the HELD directory object to the sibling DEST (r.<64 hex>) with SetFileInformation-
 * ByHandle(FileRenameInfo), ReplaceIfExists FALSE; W5 only then, below the retired object, delete the reverified
 * owner by handle and the directory empty-only by handle. Repeating the SAME framed request first holds/verifies
 * DEST against original directory ids/kind/SID/final path, then resumes W5 (full original owner proof, or empty
 * original directory); it never opens canonical L in that cleanup branch. Zero owner ids/time denote empty-only
 * receipt rediscovery, with the full R and its length still required. W6 one terminal line and exit. A watchdog thread on a
 * waitable timer terminates the process (exit 124, no terminal line) at the request deadline even if a system call is
 * stuck or the parent died; the monotonic deadline is also checked before every mutating phase. Terminal lines:
 * "FMR1 released cleanup clean" | "FMR1 released cleanup pending-<why>" (exit 0), "FMR1 not-owner <phase> <why>"
 * (exit 5), "FMR1 refused <phase> <why>" (exit 3; nothing renamed or deleted). Any other output/exit pairing is an
 * UNKNOWN outcome for the caller. Tests only (inert unless both are set; the TypeScript caller strips them unless its
 * test hook sets them): FABRIC_WIN_MESH_TEST_CHECKPOINT=NAME and FABRIC_WIN_MESH_TEST_DIR=DIR pause at checkpoint NAME
 * (before-commit, after-commit, before-owner-dispose, after-owner, after-directory), writing DIR\NAME.paused (this pid)
 * and waiting for DIR\go, bounded by the watchdog. At before-owner-dispose ONLY, exact go bytes "expire-deadline\n"
 * can shorten the main-thread logical deadline to now; they never lengthen it or change the armed watchdog.
 * At any selected existing mesh checkpoint, exact "classic-disposition\n" or "trace-disposition\n" enables
 * process-local mesh-only disposition tracing (DIR\disposition.jsonl, at most 64 actual calls); classic also
 * selects the existing real classic fallback. Only at debris-verified, exact "classic-debris-after-I\n" (I one
 * of 0,1,2,3,4,5) also selects ONE existing debris-after-I pause, which waits on DIR\debris-after-I.go instead
 * of DIR\go (the first command cannot release it). No deadline/watchdog is extended or rearmed.
 * Empty go files and the generic verified remover are unchanged.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <aclapi.h>
#include <sddl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#define MAX_ENTRIES 4096
/* Read, traverse and synchronize only (the same set as src/storage/windows-temp-root.ts). */
#define READ_ONLY_RIGHTS (FILE_GENERIC_READ | FILE_GENERIC_EXECUTE)
typedef struct { HANDLE handle; int directory; } Entry;
static Entry entries[MAX_ENTRIES];
static PSID user, owner;
static const char MODIFIED[] = "modified since judgment";
#define FIELDS 6

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
/* With mtimeText and sizeText (a removal), a FILE is held without FILE_SHARE_WRITE and its last-write time and size
 * must be the judged ones: else *why is MODIFIED. */
static HANDLE openJudgedIds(const wchar_t *path, DWORD access, int directory, unsigned long long volume, unsigned long long index,
    int timed, unsigned long long mtime, unsigned long long size, const char **why);
static HANDLE openJudged(const wchar_t *path, DWORD access, int directory, const wchar_t *volumeText, const wchar_t *indexText,
    const wchar_t *mtimeText, const wchar_t *sizeText, const char **why) {
    int okVolume, okIndex, okTime = 1, okSize = 1;
    unsigned long long volume = hex(volumeText, &okVolume), index = hex(indexText, &okIndex);
    unsigned long long mtime = mtimeText ? hex(mtimeText, &okTime) : 0, size = sizeText ? hex(sizeText, &okSize) : 0;
    if (!okVolume || !okIndex || !okTime || !okSize) { *why = "usage"; return NULL; }
    return openJudgedIds(path, access, directory, volume, index, mtimeText != NULL, mtime, size, why);
}
static HANDLE openJudgedIds(const wchar_t *path, DWORD access, int directory, unsigned long long volume, unsigned long long index,
    int timed, unsigned long long mtime, unsigned long long size, const char **why) {
    int judgedFile = timed && directory == 0;
    /* No FILE_SHARE_DELETE (pi-fabric#913 round 2): while we hold it, nobody can rename or delete it; a judged
     * file not FILE_SHARE_WRITE either (round 4): nobody can write it after its time and size are checked. */
    HANDLE handle = CreateFileW(path, access, FILE_SHARE_READ | (judgedFile ? 0 : FILE_SHARE_WRITE), NULL, OPEN_EXISTING,
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
        : judgedFile && (((unsigned long long)info.ftLastWriteTime.dwHighDateTime << 32 | info.ftLastWriteTime.dwLowDateTime) != mtime ||
            ((unsigned long long)info.nFileSizeHigh << 32 | info.nFileSizeLow) != size) ? MODIFIED
        : GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &fileOwner, NULL, NULL, NULL, &descriptor) != ERROR_SUCCESS
          ? "owner unreadable"
        : !fileOwner || !(EqualSid(fileOwner, user) || EqualSid(fileOwner, owner)) ? "not owned by this user" : NULL;
    if (descriptor) LocalFree(descriptor);
    if (*why) { CloseHandle(handle); return NULL; }
    return handle;
}

/* TrustedInstaller owns normal Windows ancestors, but is NOT an additional writer allowed by --private or the
 * actual cache-chain privateDacl policy. Keep that policy's trusted set unchanged. */
static int ancestorTrusted(PSID sid) {
    static PSID installer;
    if (trusted(sid)) return 1;
    if (!installer && !ConvertStringSidToSidW(L"S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464", &installer)) return 0;
    return EqualSid(sid, installer);
}

static const char *daclDiagnostic(PSECURITY_DESCRIPTOR descriptor, const char *reason, long index, PSID trustee,
    DWORD raw, DWORD mapped, BYTE flags) {
    static char diagnostic[16384];
    LPSTR sid = NULL, sddl = NULL;
    ConvertSidToStringSidA(trustee, &sid);
    ConvertSecurityDescriptorToStringSecurityDescriptorA(descriptor, SDDL_REVISION_1, DACL_SECURITY_INFORMATION, &sddl, NULL);
    snprintf(diagnostic, sizeof(diagnostic), "%s; ace=%ld sid=%s mask=0x%08lx mapped=0x%08lx flags=0x%02x sddl=%s",
        reason, index, sid ? sid : "unreadable", (unsigned long)raw, (unsigned long)mapped,
        (unsigned)flags, sddl ? sddl : "unreadable");
    if (sid) LocalFree(sid);
    if (sddl) LocalFree(sddl);
    return diagnostic;
}

/* ancestor=0: strict private-object rights, unchanged for --private and the actual cache chain.
 * ancestor=1: SEC-1191-01 namespace replacement rights, ONLY for cache-mode ancestors. A parent DELETE_CHILD can
 * detach even a protected child; DELETE can detach the component itself; WRITE_DAC/WRITE_OWNER can enable either.
 * Owners can implicitly rewrite DACLs, so ownership must be trusted even without an allow ACE. Do NOT reject
 * ADD_FILE/ADD_SUBDIRECTORY alone: default C:\ grants Authenticated Users create-folder rights. */
static const char *checkedDacl(HANDLE handle, int ancestor) {
    PACL dacl = NULL;
    PSID fileOwner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD information = DACL_SECURITY_INFORMATION | (ancestor ? OWNER_SECURITY_INFORMATION : 0);
    if (GetSecurityInfo(handle, SE_FILE_OBJECT, information, ancestor ? &fileOwner : NULL, NULL, &dacl, NULL, &descriptor) != ERROR_SUCCESS) return "DACL unreadable";
    const char *why = dacl ? NULL : "a null DACL (everyone may write)";
    if (!why && ancestor && (!fileOwner || !ancestorTrusted(fileOwner))) {
        /* ace=-1 is implicit owner authority, not a stored ACE. */
        why = fileOwner ? daclDiagnostic(descriptor, "ancestor owned by another SID (implicit WRITE_DAC)", -1,
            fileOwner, WRITE_DAC, WRITE_DAC, 0) : "ancestor owner unreadable";
    }
    for (DWORD i = 0; !why && i < dacl->AceCount; i++) {
        ACE_HEADER *header;
        if (!GetAce(dacl, i, (void **)&header)) { why = "DACL unreadable"; break; }
        if (header->AceFlags & INHERIT_ONLY_ACE || header->AceType == ACCESS_DENIED_ACE_TYPE) continue;
        if (header->AceType != ACCESS_ALLOWED_ACE_TYPE) { why = "an unsupported ACE type"; break; }
        ACCESS_ALLOWED_ACE *ace = (ACCESS_ALLOWED_ACE *)header;
        GENERIC_MAPPING mapping = { FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_GENERIC_EXECUTE, FILE_ALL_ACCESS };
        DWORD rights = ace->Mask;
        MapGenericMask(&rights, &mapping);
        /* NTFS may already have expanded GW before readback. Match its full FILE mapping, not individual create
         * bits: generic write must refuse, but ADD_FILE or ADD_SUBDIRECTORY on an ancestor alone must allow. */
        int dangerous = ancestor
            ? (!!(rights & (DELETE | FILE_DELETE_CHILD | WRITE_DAC | WRITE_OWNER)) ||
                (rights & FILE_GENERIC_WRITE) == FILE_GENERIC_WRITE)
            : !!(rights & ~READ_ONLY_RIGHTS);
        if (dangerous && !(ancestor ? ancestorTrusted((PSID)&ace->SidStart) : trusted((PSID)&ace->SidStart))) {
            why = daclDiagnostic(descriptor, ancestor ? "ancestor replaceable by another SID" : "writable by another SID",
                (long)i, (PSID)&ace->SidStart, ace->Mask, rights, header->AceFlags);
        }
    }
    LocalFree(descriptor);
    return why;
}

static const char *privateDacl(HANDLE handle) { return checkedDacl(handle, 0); }

/* pi-fabric#913 round 2: every ancestor of the target, up to the volume root, held open WITHOUT FILE_SHARE_DELETE
 * (and checked: a real directory, not a reparse point) until exit, so no ancestor can be renamed or replaced
 * between validation and the deletes: the judged tree stays where it was judged. With every entry held the same
 * way, nothing in the tree can move either. */
static const char *holdAncestors(const wchar_t *target, int cache) {
    static wchar_t path[32768];
    size_t length = wcslen(target);
    if (length >= 32768) return "path too long";
    wcscpy(path, target);
    /* Cache UNC paths stop at the share root, not the non-filesystem server/UNC prefixes. Generic modes retain
     * their existing walk. A malformed or root-only cache target cannot skip the ancestor check. */
    size_t shareRoot = 0;
    if (cache && wcsncmp(path, L"\\\\?\\UNC\\", 8) == 0) {
        const wchar_t *server = wcschr(path + 8, L'\\');
        const wchar_t *share = server ? wcschr(server + 1, L'\\') : NULL;
        if (!server || server == path + 8 || !share || share == server + 1) return "invalid cache UNC ancestry";
        shareRoot = (size_t)(share - path);
    }
    for (;;) {
        wchar_t *cut = wcsrchr(path, L'\\');
        if (!cut) return NULL;
        int root = cut > path && cut[-1] == L':';
        int share = shareRoot && (size_t)(cut - path) == shareRoot;
        if (root) cut[1] = 0; else *cut = 0; /* "\\?\C:\" for the volume root */
        if (!*path || wcscmp(path, L"\\\\?") == 0) return NULL;
        HANDLE handle = CreateFileW(path, FILE_READ_ATTRIBUTES | (cache ? READ_CONTROL : 0), FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
        if (handle == INVALID_HANDLE_VALUE) return "an ancestor could not be held (in use with delete access, or gone)";
        BY_HANDLE_FILE_INFORMATION info;
        if (!GetFileInformationByHandle(handle, &info) || !(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) ||
            (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT)) return "an ancestor is not a real directory (a reparse point)";
        if (cache) {
            const char *why = checkedDacl(handle, 1);
            if (why) return why;
        }
        /* Held until the process exits. Cache policy checks the whole chain, including drive/share root. */
        if (root || share) return NULL;
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

/* ---- smarty-dev#9569 Commit A: retained cache checks and the mesh retire-before-dispose release ---- */

#define MESH_MAX_MESSAGE 65536
#define MESH_MAX_OWNER 4096
#define MESH_MAX_PATH 32767
#define MESH_FIELDS 10
#define MESH_MAX_DEADLINE_MS 30000u
#define MESH_BOOT_DEADLINE_MS 30000u
static HANDLE meshTimer;
static ULONGLONG meshDeadline;

/* The single terminal line, one WriteFile (no CRLF translation): a kill mid-write leaves a malformed line, UNKNOWN. */
static int meshOut(const char *outcome, const char *phase, const char *detail, int status) {
    char line[160];
    int length = snprintf(line, sizeof(line), "FMR1 %s %s %s\n", outcome, phase, detail);
    DWORD written = 0;
    if (length > 0 && length < (int)sizeof(line)) WriteFile(GetStdHandle(STD_OUTPUT_HANDLE), line, (DWORD)length, &written, NULL);
    return status;
}
static int refusedAt(const char *phase, const char *why) { return meshOut("refused", phase, why, 3); }

/* Watchdog: a thread blocked on a waitable timer (no polling). At the deadline it terminates this process whatever the
 * main thread is doing; it may cut a commit's acknowledgement, never extend a phase. */
static DWORD WINAPI meshWatchdog(LPVOID unused) {
    (void)unused;
    if (WaitForSingleObject(meshTimer, INFINITE) == WAIT_OBJECT_0) TerminateProcess(GetCurrentProcess(), 124);
    return 0;
}
static int meshArm(DWORD ms) {
    LARGE_INTEGER due;
    due.QuadPart = -(LONGLONG)ms * 10000;
    meshDeadline = GetTickCount64() + ms;
    return SetWaitableTimer(meshTimer, &due, 0, NULL, NULL, FALSE);
}
static int meshExpired(void) { return GetTickCount64() >= meshDeadline; }

/* Activated ONLY by exact go bytes at an explicitly selected existing checkpoint, never by a standalone env/verb. */
static int meshClassicDisposition;
static wchar_t meshDispositionTrace[4200];
static unsigned meshDispositionSequence;
static wchar_t meshClassicAfterCheckpoint[64]; /* test-only second pause; six exact commands below */

/* Tests only: see the header. Bounded by the watchdog (the request deadline). */
static void meshCheckpoint(const wchar_t *name) {
    static wchar_t want[64], dir[4096], file[4200];
    DWORD got = GetEnvironmentVariableW(L"FABRIC_WIN_MESH_TEST_CHECKPOINT", want, 64);
    if (got == 0 || got >= 64) return;
    int later = wcscmp(want, L"debris-verified") == 0 && meshClassicAfterCheckpoint[0] &&
        wcscmp(meshClassicAfterCheckpoint, name) == 0;
    if (wcscmp(want, name) != 0 && !later) return;
    got = GetEnvironmentVariableW(L"FABRIC_WIN_MESH_TEST_DIR", dir, 4096);
    if (got == 0 || got >= 4096) return;
    swprintf(file, 4200, L"%ls\\%ls.paused", dir, name);
    HANDLE marker = CreateFileW(file, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, 0, NULL);
    if (marker != INVALID_HANDLE_VALUE) {
        char pid[32];
        int length = snprintf(pid, sizeof(pid), "%lu\n", (unsigned long)GetCurrentProcessId());
        DWORD written = 0;
        if (length > 0) WriteFile(marker, pid, (DWORD)length, &written, NULL);
        CloseHandle(marker);
    }
    if (later) swprintf(file, 4200, L"%ls\\%ls.go", dir, name);
    else swprintf(file, 4200, L"%ls\\go", dir);
    while (GetFileAttributesW(file) == INVALID_FILE_ATTRIBUTES) Sleep(5);
    /* The existing explicit checkpoint gate alone authorizes this read. The watchdog is NEVER rearmed here. */
    HANDLE request = CreateFileW(file, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (request != INVALID_HANDLE_VALUE) {
        char command[32];
        DWORD length = 0;
        if (ReadFile(request, command, sizeof(command), &length, NULL)) {
            if (length == sizeof("expire-deadline\n") - 1 &&
                memcmp(command, "expire-deadline\n", length) == 0 && wcscmp(name, L"before-owner-dispose") == 0) {
                ULONGLONG now = GetTickCount64();
                if (now < meshDeadline) meshDeadline = now; /* only shorten, even if already expired */
            } else if ((length == sizeof("classic-disposition\n") - 1 && memcmp(command, "classic-disposition\n", length) == 0) ||
                (length == sizeof("trace-disposition\n") - 1 && memcmp(command, "trace-disposition\n", length) == 0)) {
                meshClassicDisposition = command[0] == 'c';
                swprintf(meshDispositionTrace, 4200, L"%ls\\disposition.jsonl", dir);
            } else if (wcscmp(name, L"debris-verified") == 0) {
                static const char *const commands[] = {
                    "classic-debris-after-0\n", "classic-debris-after-1\n", "classic-debris-after-2\n",
                    "classic-debris-after-3\n", "classic-debris-after-4\n", "classic-debris-after-5\n"
                };
                for (unsigned i = 0; i < 6; i++) {
                    if (length == sizeof("classic-debris-after-0\n") - 1 && memcmp(command, commands[i], length) == 0) {
                        meshClassicDisposition = 1;
                        swprintf(meshDispositionTrace, 4200, L"%ls\\disposition.jsonl", dir);
                        swprintf(meshClassicAfterCheckpoint, 64, L"debris-after-%u", i);
                        break;
                    }
                }
            }
        }
        CloseHandle(request);
    }
}

/* No paths in JSON. Missing/unwritable trace is a test failure, never manufactured evidence of a real call. */
static void meshTraceDisposition(const BY_HANDLE_FILE_INFORMATION *info, const char *api, BOOL ok, DWORD error) {
    if (meshDispositionSequence >= 64) return;
    unsigned sequence = ++meshDispositionSequence;
    char line[384];
    int length = snprintf(line, sizeof(line),
        "{\"pid\":%lu,\"sequence\":%u,\"api\":\"%s\",\"forced\":%s,\"ok\":%s,\"error\":%lu,"
        "\"volume\":\"%lx\",\"index\":\"%llx\",\"directory\":%s}\n",
        (unsigned long)GetCurrentProcessId(), sequence, api, meshClassicDisposition ? "true" : "false",
        ok ? "true" : "false", (unsigned long)(ok ? ERROR_SUCCESS : error), (unsigned long)info->dwVolumeSerialNumber,
        (unsigned long long)info->nFileIndexHigh << 32 | info->nFileIndexLow,
        info->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ? "true" : "false");
    HANDLE trace = CreateFileW(meshDispositionTrace, FILE_APPEND_DATA, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
        OPEN_ALWAYS, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (trace != INVALID_HANDLE_VALUE) {
        DWORD written = 0;
        if (length > 0 && length < (int)sizeof(line)) WriteFile(trace, line, (DWORD)length, &written, NULL);
        CloseHandle(trace);
    }
}

/* Delete by handle: POSIX semantics, else classic (completes at the last close). A directory fails when not empty.
 * This function serves mesh W/D only; the generic verified remover's inline disposal is deliberately untouched. */
static BOOL disposeHandle(HANDLE handle) {
    FILE_DISPOSITION_INFO_EX extended = { FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS };
    FILE_DISPOSITION_INFO plain = { TRUE };
    if (!meshDispositionTrace[0]) {
        return SetFileInformationByHandle(handle, FileDispositionInfoEx, &extended, sizeof(extended)) ||
            SetFileInformationByHandle(handle, FileDispositionInfo, &plain, sizeof(plain));
    }
    DWORD previous = GetLastError();
    BY_HANDLE_FILE_INFORMATION info;
    BOOL identified = GetFileInformationByHandle(handle, &info);
    SetLastError(previous);
    BOOL ok;
    DWORD error;
    if (!meshClassicDisposition) {
        ok = SetFileInformationByHandle(handle, FileDispositionInfoEx, &extended, sizeof(extended));
        error = GetLastError();
        if (identified) meshTraceDisposition(&info, "extended", ok, error);
        SetLastError(error);
        if (ok) return ok;
    }
    ok = SetFileInformationByHandle(handle, FileDispositionInfo, &plain, sizeof(plain));
    error = GetLastError();
    if (identified) meshTraceDisposition(&info, "classic", ok, error);
    SetLastError(error);
    return ok;
}

static const char *finalPath(HANDLE handle, wchar_t *buffer) {
    DWORD got = GetFinalPathNameByHandleW(handle, buffer, MESH_MAX_PATH + 1, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
    return got == 0 || got > MESH_MAX_PATH ? "final-path-unreadable" : NULL;
}

/* The entry set of a HELD directory, read on its handle (FILE_LIST_DIRECTORY): *owners counts a non-directory entry
 * named exactly "owner", *extra every other entry. NULL, or why it could not be read. */
static const char *heldEntries(HANDLE directory, int *owners, int *extra) {
    static ULONGLONG buffer[8192]; /* 64 KiB, 8-byte aligned */
    FILE_INFO_BY_HANDLE_CLASS kind = FileFullDirectoryRestartInfo;
    *owners = *extra = 0;
    for (;;) {
        if (!GetFileInformationByHandleEx(directory, kind, buffer, sizeof(buffer))) {
            return GetLastError() == ERROR_NO_MORE_FILES ? NULL : "entries-unreadable";
        }
        kind = FileFullDirectoryInfo;
        FILE_FULL_DIR_INFO *info = (FILE_FULL_DIR_INFO *)buffer;
        for (;;) {
            size_t length = info->FileNameLength / sizeof(WCHAR);
            int dot = (length == 1 && info->FileName[0] == L'.') || (length == 2 && info->FileName[0] == L'.' && info->FileName[1] == L'.');
            if (!dot) {
                if (length == 5 && wcsncmp(info->FileName, L"owner", 5) == 0 && !(info->FileAttributes & FILE_ATTRIBUTE_DIRECTORY)) (*owners)++;
                else (*extra)++;
            }
            if (!info->NextEntryOffset) break;
            info = (FILE_FULL_DIR_INFO *)((BYTE *)info + info->NextEntryOffset);
        }
    }
}

/* Read the whole held owner file (at most MESH_MAX_OWNER + 1 bytes): 1 equal, 0 different, -1 I/O error.
 * An unreadable record is never evidence of a different owner. */
static int sameRecord(HANDLE file, const unsigned char *record, DWORD recordLength) {
    static unsigned char bytes[MESH_MAX_OWNER + 1];
    DWORD total = 0;
    LARGE_INTEGER zero = { 0 };
    if (!SetFilePointerEx(file, zero, NULL, FILE_BEGIN)) return -1;
    for (;;) {
        DWORD got = 0;
        if (!ReadFile(file, bytes + total, (DWORD)sizeof(bytes) - total, &got, NULL)) return -1;
        if (got == 0) break;
        total += got;
        if (total >= sizeof(bytes)) return 0;
    }
    return total == recordLength && memcmp(bytes, record, recordLength) == 0;
}

static int namespaced(const wchar_t *path) { return wcsncmp(path, L"\\\\?\\", 4) == 0; }
/* The parent part's length (up to the last backslash), or 0. */
static size_t parentLength(const wchar_t *path) { const wchar_t *cut = wcsrchr(path, L'\\'); return cut ? (size_t)(cut - path) : 0; }

/* Cache creation accepts only literal absolute DOS/UNC paths, with no alternate stream, dot, short-name alias or
 * Win32 component normalization. finalPath on every held component rejects aliases that syntax cannot detect. */
static int cachePath(const wchar_t *path) {
    if (!namespaced(path)) return 0;
    const wchar_t *part;
    int unc = wcsncmp(path, L"\\\\?\\UNC\\", 8) == 0;
    if (unc) part = path + 8;
    else {
        if (!((path[4] >= L'A' && path[4] <= L'Z') || (path[4] >= L'a' && path[4] <= L'z')) ||
            path[5] != L':' || path[6] != L'\\') return 0;
        part = path + 7;
    }
    int components = 0;
    for (;;) {
        const wchar_t *end = wcschr(part, L'\\');
        size_t length = end ? (size_t)(end - part) : wcslen(part);
        if (!length || part[length - 1] == L'.' || part[length - 1] == L' ') return 0;
        for (size_t i = 0; i < length; i++) if (part[i] < 32 || wcschr(L"/:*?\"<>|", part[i])) return 0;
        components++;
        if (!end) break;
        part = end + 1;
    }
    return !unc || components >= 3; /* server, share, and a non-root directory */
}

static int cacheCreate(int argc, wchar_t **argv) {
    int count = argc - 2;
    if (count < 1 || count > 2) return fail("refused", 0, "usage: one or two cache directories", 3);
    for (int i = 0; i < count; i++) {
        const wchar_t *path = argv[2 + i];
        if (!cachePath(path)) return fail("refused", i, "not a literal absolute cache path", 3);
        if (i && (parentLength(path) != wcslen(argv[1 + i]) || wcsncmp(path, argv[1 + i], parentLength(path)) != 0))
            return fail("refused", i, "not a direct child of the previous entry", 3);
    }
    const char *why = holdAncestors(argv[2], 1);
    if (why) return fail("refused", 0, why, 3);
    BYTE systemSid[SECURITY_MAX_SID_SIZE], adminSid[SECURITY_MAX_SID_SIZE];
    DWORD systemSize = sizeof(systemSid), adminSize = sizeof(adminSid);
    if (!CreateWellKnownSid(WinLocalSystemSid, NULL, systemSid, &systemSize) ||
        !CreateWellKnownSid(WinBuiltinAdministratorsSid, NULL, adminSid, &adminSize)) return fail("refused", 0, "trusted SIDs unavailable", 3);
    PSID allowed[] = { user, owner, systemSid, adminSid };
    DWORD aclSize = sizeof(ACL);
    for (int i = 0; i < 4; i++) aclSize += sizeof(ACCESS_ALLOWED_ACE) - sizeof(DWORD) + GetLengthSid(allowed[i]);
    PACL acl = (PACL)calloc(1, aclSize);
    SECURITY_DESCRIPTOR sd;
    if (!acl || !InitializeAcl(acl, aclSize, ACL_REVISION)) return fail("refused", 0, "private ACL unavailable", 3);
    for (int i = 0; i < 4; i++) {
        if (!AddAccessAllowedAceEx(acl, ACL_REVISION, OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE, FILE_ALL_ACCESS, allowed[i]))
            return fail("refused", 0, "private ACE unavailable", 3);
    }
    if (!InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION) || !SetSecurityDescriptorOwner(&sd, user, FALSE) ||
        !SetSecurityDescriptorDacl(&sd, TRUE, acl, FALSE) || !SetSecurityDescriptorControl(&sd, SE_DACL_PROTECTED, SE_DACL_PROTECTED))
        return fail("refused", 0, "private descriptor unavailable", 3);
    SECURITY_ATTRIBUTES attributes = { sizeof(SECURITY_ATTRIBUTES), &sd, FALSE };
    for (int i = 0; i < count; i++) {
        const wchar_t *path = argv[2 + i];
        int created = CreateDirectoryW(path, &attributes);
        if (!created && GetLastError() != ERROR_ALREADY_EXISTS) return fail("refused", i, "private directory creation failed", 3);
        HANDLE handle = CreateFileW(path, READ_CONTROL | FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
        if (handle == INVALID_HANDLE_VALUE) return fail("refused", i, "directory could not be held", 3);
        BY_HANDLE_FILE_INFORMATION info;
        PSID fileOwner = NULL;
        PSECURITY_DESCRIPTOR descriptor = NULL;
        why = !GetFileInformationByHandle(handle, &info) ? "no file information"
            : !(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) || (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) ? "not a real directory"
            : GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &fileOwner, NULL, NULL, NULL, &descriptor) != ERROR_SUCCESS ? "security unreadable"
            : !fileOwner || !(EqualSid(fileOwner, user) || EqualSid(fileOwner, owner)) ? "not owned by this user" : NULL;
        if (!why && created) {
            SECURITY_DESCRIPTOR_CONTROL control;
            DWORD revision;
            if (!GetSecurityDescriptorControl(descriptor, &control, &revision) || !(control & SE_DACL_PROTECTED)) why = "new DACL is not protected";
        }
        if (descriptor) LocalFree(descriptor);
        /* Existing normal profiles may carry inherit-only CREATOR OWNER rules. Judge actual directory rights
         * here; the trusted bootstrap also checks the actual inherited staging directory and executable BEFORE
         * publication. Do not turn a harmless inherit-only template into a new blanket SID prohibition. */
        if (!why) why = privateDacl(handle);
        static wchar_t final[MESH_MAX_PATH + 1];
        if (!why) why = finalPath(handle, final);
        if (!why && _wcsicmp(final, path) != 0) why = "final path differs (a reparse point or alias in its path)";
        if (why) return fail("refused", i, why, 3);
        /* Held until exit, including while the next component is created and read back. */
    }
    free(acl);
    printf("ok created\n");
    return 0;
}

static int cachePrivate(int argc, wchar_t **argv) {
    int count = argc - 2;
    if (count < 1 || count > 8) { printf("failed -1 usage\n"); return 2; }
    const char *why = holdAncestors(argv[2], 1);
    if (why) return fail("refused", 0, why, 3);
    for (int i = 0; i < count; i++) {
        const wchar_t *path = argv[2 + i];
        if (!namespaced(path)) return fail("refused", i, "not an absolute namespaced path", 3);
        if (i > 0) {
            const wchar_t *up = argv[1 + i];
            size_t length = wcslen(up);
            if (parentLength(path) != length || wcsncmp(path, up, length) != 0) return fail("refused", i, "not a direct child of the previous entry", 3);
        }
        int last = i == count - 1;
        HANDLE handle = CreateFileW(path, READ_CONTROL | FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
        if (handle == INVALID_HANDLE_VALUE) return fail("refused", i, "open failed", 3);
        BY_HANDLE_FILE_INFORMATION info;
        PSID fileOwner = NULL;
        PSECURITY_DESCRIPTOR descriptor = NULL;
        why = !GetFileInformationByHandle(handle, &info) ? "no file information"
            : info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT ? "a reparse point"
            : !!(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == last ? (last ? "not a regular file" : "not a directory")
            : GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &fileOwner, NULL, NULL, NULL, &descriptor) != ERROR_SUCCESS
              ? "owner unreadable"
            : !fileOwner || !(EqualSid(fileOwner, user) || EqualSid(fileOwner, owner)) ? "not owned by this user" : NULL;
        if (descriptor) LocalFree(descriptor);
        if (!why) why = privateDacl(handle);
        if (!why && last) {
            static wchar_t final[MESH_MAX_PATH + 1];
            why = finalPath(handle, final);
            if (!why && _wcsicmp(final, path) != 0) why = "final path differs (a reparse point or alias in its path)";
        }
        /* Held until exit: no entry of the chain can be renamed while the later ones are checked. */
        if (why) return fail("refused", i, why, 3);
    }
    printf("ok private\n");
    return 0;
}

static int cachePublish(const wchar_t *staging, const wchar_t *dest) {
    size_t length = parentLength(dest);
    if (!namespaced(staging) || !namespaced(dest) || length == 0 || parentLength(staging) != length || wcsncmp(staging, dest, length) != 0) {
        printf("failed -1 usage\n"); return 2;
    }
    if (MoveFileExW(staging, dest, 0)) { printf("ok published\n"); return 0; }
    DWORD error = GetLastError();
    if (error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS || GetFileAttributesW(dest) != INVALID_FILE_ATTRIBUTES) { printf("exists 0 published\n"); return 6; }
    printf("refused 0 move failed\n");
    return 3;
}

/* Request (stdin, at most MESH_MAX_MESSAGE bytes): the 5 bytes "FMRQ1", then exactly MESH_FIELDS fields, each 8
 * lowercase hex digits of length and that many bytes, then nothing. Fields: 0 lock path, 1 retirement path (both UTF-8,
 * \\?\ absolute, same parent, the latter named r.<64 lowercase hex>), 2/3 directory volume/index, 4/5 owner volume/
 * index, 6 owner last-write FILETIME, 7 owner size (all lowercase hex, at most 16 digits), 8 the original owner bytes
 * (1..MESH_MAX_OWNER, equal to field 7), 9 deadline in decimal ms (1..MESH_MAX_DEADLINE_MS). Anything else refuses
 * at phase parse, before any open. */
static unsigned char meshMessage[MESH_MAX_MESSAGE + 1];
static int narrowHex(const unsigned char *text, DWORD length, unsigned long long *value) {
    if (length < 1 || length > 16) return 0;
    *value = 0;
    for (DWORD i = 0; i < length; i++) {
        int digit = text[i] >= '0' && text[i] <= '9' ? text[i] - '0' : text[i] >= 'a' && text[i] <= 'f' ? text[i] - 'a' + 10 : -1;
        if (digit < 0) return 0;
        *value = *value << 4 | (unsigned)digit;
    }
    return 1;
}
static wchar_t *widePath(const unsigned char *text, DWORD length) {
    if (length < 1 || length > MESH_MAX_PATH || memchr(text, 0, length)) return NULL;
    int wide = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (const char *)text, (int)length, NULL, 0);
    if (wide <= 0 || wide > MESH_MAX_PATH) return NULL;
    wchar_t *path = (wchar_t *)calloc((size_t)wide + 1, sizeof(wchar_t));
    if (!path || MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (const char *)text, (int)length, path, wide) != wide) { free(path); return NULL; }
    return namespaced(path) ? path : (free(path), NULL);
}

/* W5 shared by a fresh W4 commit and an exact same-receipt resume. directory is ALREADY held and verified
 * against the ORIGINAL directory ids/kind/SID; dest is its checked same-parent location. Never resolve L here.
 * Empty means no owner/file delete at all. An added/unjudged entry always survives empty-only disposition. */
static int meshCleanup(HANDLE directory, const wchar_t *dest, unsigned long long ownerVolume, unsigned long long ownerIndex,
    unsigned long long ownerTime, unsigned long long ownerSize, const unsigned char *record, DWORD recordLength) {
    static wchar_t top[MESH_MAX_PATH + 1], final[MESH_MAX_PATH + 1], ownerPath[MESH_MAX_PATH + 1];
    int owners, extra;
    if (finalPath(directory, top) || _wcsicmp(top, dest) != 0) return meshOut("released", "cleanup", "pending-location", 0);
    if (heldEntries(directory, &owners, &extra)) return meshOut("released", "cleanup", "pending-entries-unreadable", 0);
    /* Full original proof OR genuinely empty original directory, never arbitrary empty r.* by name. */
    if (extra != 0 || owners > 1) return meshOut("released", "cleanup", "pending-extra-entries", 0);
    if (meshExpired()) return meshOut("released", "cleanup", "pending-deadline", 0);
    /* Rediscovered empty receipts use zero owner ids/time (R and its length remain full in the unchanged frame).
     * If an owner arrived since JS judged empty, this request has NO file-delete authority, even if metadata fit. */
    if (owners && ownerVolume == 0 && ownerIndex == 0 && ownerTime == 0) return meshOut("released", "cleanup", "pending-owner-unverified", 0);
    if (owners == 1) {
        const char *why = NULL;
        swprintf(ownerPath, MESH_MAX_PATH + 1, L"%ls\\owner", dest);
        HANDLE ownerFile = openJudgedIds(ownerPath, DELETE | FILE_READ_DATA | READ_CONTROL | FILE_READ_ATTRIBUTES | SYNCHRONIZE, 0,
            ownerVolume, ownerIndex, 1, ownerTime, ownerSize, &why);
        if (!ownerFile) return meshOut("released", "cleanup", "pending-owner-unverified", 0);
        int recordState = sameRecord(ownerFile, record, recordLength);
        if (recordState < 0) return meshOut("released", "cleanup", "pending-record-unreadable", 0);
        if (recordState == 0 || finalPath(ownerFile, final) || _wcsnicmp(final, top, wcslen(top)) != 0 ||
            _wcsicmp(final + wcslen(top), L"\\owner") != 0) return meshOut("released", "cleanup", "pending-owner-unverified", 0);
        meshCheckpoint(L"before-owner-dispose");
        if (meshExpired()) return meshOut("released", "cleanup", "pending-deadline", 0);
        if (!disposeHandle(ownerFile)) return meshOut("released", "cleanup", "pending-owner-delete", 0);
        CloseHandle(ownerFile);
        meshCheckpoint(L"after-owner");
    }
    if (meshExpired()) return meshOut("released", "cleanup", "pending-deadline", 0);
    if (!disposeHandle(directory)) return meshOut("released", "cleanup", "pending-directory-not-empty", 0);
    CloseHandle(directory);
    meshCheckpoint(L"after-directory");
    return meshOut("released", "cleanup", "clean", 0);
}

/* C uses the same guarded ORIGINAL-R/provided-S transaction as W, but keeps the complete F forever.
 * Its framing is FMRC1; A's FMRQ1 request and ALL its output tuples remain unchanged. */
static int meshRetire(int recovery) {
    meshTimer = CreateWaitableTimerW(NULL, TRUE, NULL);
    if (!meshTimer || !meshArm(MESH_BOOT_DEADLINE_MS) || !CreateThread(NULL, 0, meshWatchdog, NULL, 0, NULL)) return refusedAt("parse", "watchdog-unavailable");
    /* Parse: bounded read, then the exact framing. */
    HANDLE in = GetStdHandle(STD_INPUT_HANDLE);
    DWORD total = 0;
    for (;;) {
        DWORD got = 0;
        if (!ReadFile(in, meshMessage + total, (DWORD)sizeof(meshMessage) - total, &got, NULL)) {
            if (GetLastError() == ERROR_BROKEN_PIPE) break;
            return refusedAt("parse", "stdin-unreadable");
        }
        if (got == 0) break;
        total += got;
        if (total > MESH_MAX_MESSAGE) return refusedAt("parse", "message-too-large");
    }
    if (total < 5 || memcmp(meshMessage, recovery ? "FMRC1" : "FMRQ1", 5) != 0) return refusedAt("parse", "bad-magic");
    const unsigned char *field[MESH_FIELDS];
    DWORD size[MESH_FIELDS];
    DWORD at = 5;
    for (int i = 0; i < MESH_FIELDS; i++) {
        unsigned long long length;
        if (total - at < 8 || !narrowHex(meshMessage + at, 8, &length)) return refusedAt("parse", "bad-field-length");
        at += 8;
        if (length > total - at) return refusedAt("parse", "truncated-field");
        field[i] = meshMessage + at;
        size[i] = (DWORD)length;
        at += (DWORD)length;
    }
    if (at != total) return refusedAt("parse", "trailing-bytes");
    wchar_t *lock = widePath(field[0], size[0]), *dest = widePath(field[1], size[1]);
    if (!lock || !dest) return refusedAt("parse", "bad-path");
    unsigned long long dirVolume, dirIndex, ownerVolume, ownerIndex, ownerTime, ownerSize;
    if (!narrowHex(field[2], size[2], &dirVolume) || !narrowHex(field[3], size[3], &dirIndex) || !narrowHex(field[4], size[4], &ownerVolume) ||
        !narrowHex(field[5], size[5], &ownerIndex) || !narrowHex(field[6], size[6], &ownerTime) || !narrowHex(field[7], size[7], &ownerSize)) {
        return refusedAt("parse", "bad-id");
    }
    if (size[8] < 1 || size[8] > MESH_MAX_OWNER || ownerSize != size[8] || memchr(field[8], 0, size[8])) return refusedAt("parse", "bad-record");
    unsigned long deadline = 0;
    if (size[9] < 1 || size[9] > 5) return refusedAt("parse", "bad-deadline");
    for (DWORD i = 0; i < size[9]; i++) {
        if (field[9][i] < '0' || field[9][i] > '9') return refusedAt("parse", "bad-deadline");
        deadline = deadline * 10 + (unsigned long)(field[9][i] - '0');
    }
    if (deadline < 1 || deadline > MESH_MAX_DEADLINE_MS) return refusedAt("parse", "bad-deadline");
    size_t lockLength = wcslen(lock), parent = parentLength(lock);
    const wchar_t *base = dest + parentLength(dest) + 1;
    int destName = wcslen(base) == 66 && base[0] == L'r' && base[1] == L'.';
    for (int i = 2; destName && i < 66; i++) destName = (base[i] >= L'0' && base[i] <= L'9') || (base[i] >= L'a' && base[i] <= L'f');
    const wchar_t *lockBase = lock + parent + 1;
    if (recovery) {
        size_t baseLength = wcslen(lockBase);
        destName = (wcscmp(lockBase, L".lock") == 0 || wcscmp(lockBase, L"custody.lock") == 0) &&
            wcslen(base) == baseLength + 6 + 64 && wcsncmp(base, lockBase, baseLength) == 0 &&
            wcsncmp(base + baseLength, L".dead.", 6) == 0;
        for (size_t i = baseLength + 6; destName && i < wcslen(base); i++)
            destName = (base[i] >= L'0' && base[i] <= L'9') || (base[i] >= L'a' && base[i] <= L'f');
    }
    if (parent < 4 || parentLength(dest) != parent || wcsncmp(lock, dest, parent) != 0 || !destName || !*lockBase ||
        wcscmp(lockBase, L".") == 0 || wcscmp(lockBase, L"..") == 0) return refusedAt("parse", "bad-placement");
    /* E3 caps logical mesh descendants, not namespace transport syntax or the independent cache path. */
    size_t lockPrefix = wcsncmp(lock, L"\\\\?\\UNC\\", 8) == 0 ? 6 : 4;
    size_t destPrefix = wcsncmp(dest, L"\\\\?\\UNC\\", 8) == 0 ? 6 : 4;
    if (lockLength - lockPrefix + 6 >= 240 || wcslen(dest) - destPrefix + 6 >= 240) return refusedAt("parse", "mesh-path-too-long");
    if (!meshArm((DWORD)deadline)) return refusedAt("parse", "watchdog-unavailable");

    /* W2: ancestors held, then L with DELETE (rename) and FILE_LIST_DIRECTORY, no delete sharing, held through W6. */
    const char *why = holdAncestors(lock, 0);
    if (why) return refusedAt("guard", "ancestor-not-held");
    /* A retry of the SAME framed request may find its original object already retired. Check T BEFORE L:
     * only confirmed absence permits the ordinary W2 path. Existing destinations are never replaced; a wrong
     * identity/kind/SID/reparse target refuses. A verified T resumes W5 without EVER opening canonical B. */
    HANDLE directory;
    /* Only W retirement T is resumable. C's destination is permanent F, never cleanup authority. */
    if (!recovery) {
        directory = openJudgedIds(dest, DELETE | FILE_LIST_DIRECTORY | READ_CONTROL | FILE_READ_ATTRIBUTES | SYNCHRONIZE, 1,
            dirVolume, dirIndex, 0, 0, 0, &why);
        if (directory) return meshCleanup(directory, dest, ownerVolume, ownerIndex, ownerTime, ownerSize, field[8], size[8]);
        DWORD destError = GetLastError();
        if (!why || strcmp(why, "open failed") != 0) return refusedAt("commit", "destination-exists");
        if (destError != ERROR_FILE_NOT_FOUND && destError != ERROR_PATH_NOT_FOUND) return refusedAt("guard", "directory-not-held");
    }
    directory = openJudgedIds(lock, DELETE | FILE_LIST_DIRECTORY | READ_CONTROL | FILE_READ_ATTRIBUTES | SYNCHRONIZE, 1,
        dirVolume, dirIndex, 0, 0, 0, &why);
    if (!directory) {
        /* "open failed" is set straight after CreateFileW, so its last error is still CreateFileW's. */
        DWORD error = GetLastError();
        if (why && strcmp(why, "open failed") == 0) {
            return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND ? meshOut("not-owner", "guard", "absent", 5)
                : refusedAt("guard", "directory-not-held");
        }
        /* Another directory now at L (a successor): never ours. Anything else is refused, not presumed foreign. */
        return why && strcmp(why, "file id changed since judgment") == 0 ? meshOut("not-owner", "guard", "directory-replaced", 5)
            : refusedAt("guard", "directory-unverified");
    }
    static wchar_t top[MESH_MAX_PATH + 1], final[MESH_MAX_PATH + 1], ownerPath[MESH_MAX_PATH + 1];
    if ((why = finalPath(directory, top)) || _wcsicmp(top, lock) != 0) return refusedAt("guard", "directory-location");

    /* W3: the owner, its exact ids and full ORIGINAL bytes on a handle that shares neither write nor delete. */
    if (lockLength + 6 > MESH_MAX_PATH) return refusedAt("owner", "path-too-long");
    swprintf(ownerPath, MESH_MAX_PATH + 1, L"%ls\\owner", lock);
    HANDLE ownerFile = openJudgedIds(ownerPath, FILE_READ_DATA | READ_CONTROL | FILE_READ_ATTRIBUTES | SYNCHRONIZE, 0,
        ownerVolume, ownerIndex, 1, ownerTime, ownerSize, &why);
    if (!ownerFile) {
        DWORD error = GetLastError();
        if (why && strcmp(why, "open failed") == 0) {
            return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND ? meshOut("not-owner", "owner", "absent", 5)
                : refusedAt("owner", "owner-not-held");
        }
        /* A changed owner id or time in the same directory is not a cooperating schedule: refused (retryable), never
         * claimed foreign. A different record is the only owner mismatch reported as not-owner. */
        return refusedAt("owner", why == MODIFIED ? "owner-modified" : "owner-unverified");
    }
    int recordState = sameRecord(ownerFile, field[8], size[8]);
    if (recordState < 0) return refusedAt("owner", "record-unreadable");
    if (recordState == 0) return meshOut("not-owner", "owner", "record-differs", 5);
    if ((why = finalPath(ownerFile, final)) || _wcsnicmp(final, top, wcslen(top)) != 0 || _wcsicmp(final + wcslen(top), L"\\owner") != 0) {
        return refusedAt("owner", "owner-outside-directory");
    }
    int owners, extra;
    if ((why = heldEntries(directory, &owners, &extra))) return refusedAt("owner", "entries-unreadable");
    if (owners != 1 || extra != 0) return refusedAt("owner", "unexpected-entries");
    /* Windows refuses to rename a directory with an open handle below it: the owner read handle closes, the directory
     * guard stays (every updated owner mutator needs a conflicting DELETE open of L; initializers use exclusive create). */
    CloseHandle(ownerFile);
    meshCheckpoint(L"before-commit");
    if (meshExpired()) return refusedAt("commit", "deadline");

    /* W4 COMMIT: the HELD object to DEST, never replacing. */
    size_t destLength = wcslen(dest);
    size_t renameSize = sizeof(FILE_RENAME_INFO) + (destLength + 1) * sizeof(WCHAR);
    FILE_RENAME_INFO *renameInfo = (FILE_RENAME_INFO *)calloc(1, renameSize);
    if (!renameInfo) return refusedAt("commit", "no-memory");
    /* calloc: ReplaceIfExists (and Flags) FALSE, RootDirectory NULL. */
    renameInfo->FileNameLength = (DWORD)(destLength * sizeof(WCHAR));
    memcpy(renameInfo->FileName, dest, destLength * sizeof(WCHAR));
    if (!SetFileInformationByHandle(directory, FileRenameInfo, renameInfo, (DWORD)renameSize)) {
        DWORD error = GetLastError();
        return refusedAt("commit", error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS ? "destination-exists"
            : error == ERROR_SHARING_VIOLATION || error == ERROR_ACCESS_DENIED ? "rename-denied" : "rename-failed");
    }
    free(renameInfo);
    meshCheckpoint(L"after-commit");
    /* C: F (including its original owner proof) is permanently retained. Never select W5, even on retry. */
    if (recovery) {
        static const char retained[] = "FMREC1 recovered commit retained\n";
        DWORD written = 0;
        WriteFile(GetStdHandle(STD_OUTPUT_HANDLE), retained, sizeof(retained) - 1, &written, NULL);
        return 0;
    }

    /* W5: identical held-object cleanup for fresh retirement and same-receipt resume. */
    return meshCleanup(directory, dest, ownerVolume, ownerIndex, ownerTime, ownerSize, field[8], size[8]);
}

/* ---- smarty-dev#9569 Commit B: D, independently judged leaves; NEVER detach canonical L ----
 * FMRD1 + fields: lock, deadline(decimal), count(decimal), then count * (path, kind, volume, index,
 * FILETIME, size, full record). Same 8-lowercase-hex length framing/caps as A; exactly 3+7*count fields.
 * Count <=64 includes L as the LAST directory; each parent follows ALL its descendants. All files carry their
 * own complete, caller-independently-judged bytes. Native enforces grammar/binding, not the caller's death policy.
 * Every handle/id/time/size/record AND every directory's initial entry set is checked BEFORE ANY disposal.
 * Added entries after that check are unselected and survive; empty-only disposition then fails. A killed partial
 * pass leaves independently judgeable records or empty directories, NEVER dependent raw payload.
 * FMREC1 refused PHASE DETAIL (3) means nothing disposed. FMREC1 partial delete DETAIL (2) may have disposed
 * selected handles; the wrapper reports recovery-outcome-unknown, never recovered or a pre-mutation refusal.
 */
#define DEBRIS_MAX_ENTRIES 64
#define DEBRIS_MAX_FIELDS (3 + 7 * DEBRIS_MAX_ENTRIES)
typedef struct {
    wchar_t *path;
    HANDLE handle;
    int directory;
    unsigned long long volume, index, time, size;
    const unsigned char *record;
    DWORD recordLength;
} DebrisEntry;
static DebrisEntry debris[DEBRIS_MAX_ENTRIES];

static int debrisOut(const char *outcome, const char *phase, const char *detail, int status) {
    char line[160];
    int length = snprintf(line, sizeof(line), "FMREC1 %s %s %s\n", outcome, phase, detail);
    DWORD written = 0;
    if (length > 0 && length < (int)sizeof(line)) WriteFile(GetStdHandle(STD_OUTPUT_HANDLE), line, (DWORD)length, &written, NULL);
    return status;
}
static int debrisRefuse(const char *phase, const char *why) { return debrisOut("refused", phase, why, 3); }
static int decimal(const unsigned char *text, DWORD length, DWORD max, DWORD *number) {
    if (length < 1 || length > 5) return 0;
    *number = 0;
    for (DWORD i = 0; i < length; i++) {
        if (text[i] < '0' || text[i] > '9') return 0;
        *number = *number * 10 + (DWORD)(text[i] - '0');
    }
    return *number >= 1 && *number <= max;
}
static int legalRetiredComponent(const wchar_t *name, size_t length) {
    if (length < 3 || name[0] != L'r' || name[1] != L'.' || name[length - 1] == L'.' || name[length - 1] == L' ') return 0;
    for (size_t i = 2; i < length; i++) if (name[i] < 32 || wcschr(L"\\/:*?\"<>|", name[i])) return 0;
    return 1;
}
static int releasedOwnerComponent(const wchar_t *name) {
    static const wchar_t prefix[] = L"owner.released.";
    if (wcslen(name) != 15 + 36 || wcsncmp(name, prefix, 15) != 0) return 0;
    for (int i = 0; i < 36; i++) {
        wchar_t c = name[15 + i];
        if (i == 8 || i == 13 || i == 18 || i == 23) { if (c != L'-') return 0; }
        else if (!((c >= L'0' && c <= L'9') || (c >= L'a' && c <= L'f') || (c >= L'A' && c <= L'F'))) return 0;
    }
    return 1;
}
/* Exact child-first inventory: no unknown positions, alias components, stream paths or implicit parents. */
static int debrisPlacement(const wchar_t *lock, DWORD count) {
    size_t topLength = wcslen(lock);
    const wchar_t *base = lock + parentLength(lock) + 1;
    if (wcscmp(base, L".lock") != 0 && wcscmp(base, L"custody.lock") != 0) return 0; /* F is NEVER collectible */
    if (wcscmp(debris[count - 1].path, lock) != 0 || !debris[count - 1].directory) return 0;
    for (DWORD i = 0; i < count - 1; i++) {
        DebrisEntry *entry = &debris[i];
        if (wcsncmp(entry->path, lock, topLength) != 0 || entry->path[topLength] != L'\\') return 0;
        const wchar_t *component = entry->path + topLength + 1, *cut;
        int depth = 1;
        while ((cut = wcschr(component, L'\\')) != NULL) {
            if (!legalRetiredComponent(component, (size_t)(cut - component))) return 0;
            component = cut + 1;
            if (++depth > 4) return 0;
        }
        if (entry->directory ? !legalRetiredComponent(component, wcslen(component)) :
            !(releasedOwnerComponent(component) || (depth > 1 && wcscmp(component, L"owner") == 0))) return 0;
        size_t parent = parentLength(entry->path);
        int found = 0;
        for (DWORD j = i + 1; j < count; j++) if (debris[j].directory && wcslen(debris[j].path) == parent &&
            wcsncmp(entry->path, debris[j].path, parent) == 0) found = 1;
        if (!found) return 0;
    }
    for (DWORD i = 0; i < count; i++) for (DWORD j = i + 1; j < count; j++)
        if (_wcsicmp(debris[i].path, debris[j].path) == 0) return 0;
    return 1;
}
/* Inspect each held directory without resolving it by name. Every initial child must be explicitly selected. */
static int debrisEntrySet(DWORD directory, DWORD count) {
    static ULONGLONG buffer[8192];
    FILE_INFO_BY_HANDLE_CLASS kind = FileFullDirectoryRestartInfo;
    DWORD observed = 0, expected = 0;
    const wchar_t *up = debris[directory].path;
    size_t length = wcslen(up);
    for (DWORD i = 0; i < count; i++) if (i != directory && parentLength(debris[i].path) == length &&
        wcsncmp(debris[i].path, up, length) == 0) expected++;
    for (;;) {
        if (!GetFileInformationByHandleEx(debris[directory].handle, kind, buffer, sizeof(buffer)))
            return GetLastError() == ERROR_NO_MORE_FILES && observed == expected;
        kind = FileFullDirectoryInfo;
        FILE_FULL_DIR_INFO *info = (FILE_FULL_DIR_INFO *)buffer;
        for (;;) {
            size_t chars = info->FileNameLength / sizeof(WCHAR);
            int dot = (chars == 1 && info->FileName[0] == L'.') || (chars == 2 && info->FileName[0] == L'.' && info->FileName[1] == L'.');
            if (!dot) {
                if (++observed > count || (info->FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT)) return 0;
                int found = 0;
                for (DWORD i = 0; i < count; i++) if (i != directory && parentLength(debris[i].path) == length &&
                    wcsncmp(debris[i].path, up, length) == 0 && wcslen(debris[i].path + length + 1) == chars &&
                    wcsncmp(debris[i].path + length + 1, info->FileName, chars) == 0) found = 1;
                if (!found) return 0;
            }
            if (!info->NextEntryOffset) break;
            info = (FILE_FULL_DIR_INFO *)((BYTE *)info + info->NextEntryOffset);
        }
    }
}
static int meshDebris(void) {
    meshTimer = CreateWaitableTimerW(NULL, TRUE, NULL);
    if (!meshTimer || !meshArm(MESH_BOOT_DEADLINE_MS) || !CreateThread(NULL, 0, meshWatchdog, NULL, 0, NULL))
        return debrisRefuse("parse", "watchdog-unavailable");
    DWORD total = 0;
    for (;;) {
        DWORD got = 0;
        if (!ReadFile(GetStdHandle(STD_INPUT_HANDLE), meshMessage + total, (DWORD)sizeof(meshMessage) - total, &got, NULL)) {
            if (GetLastError() == ERROR_BROKEN_PIPE) break;
            return debrisRefuse("parse", "stdin-unreadable");
        }
        if (!got) break;
        total += got;
        if (total > MESH_MAX_MESSAGE) return debrisRefuse("parse", "message-too-large");
    }
    if (total < 5 || memcmp(meshMessage, "FMRD1", 5) != 0) return debrisRefuse("parse", "bad-magic");
    const unsigned char *field[DEBRIS_MAX_FIELDS];
    DWORD size[DEBRIS_MAX_FIELDS], fields = 0, at = 5;
    while (at < total) {
        unsigned long long length;
        if (fields >= DEBRIS_MAX_FIELDS || total - at < 8 || !narrowHex(meshMessage + at, 8, &length))
            return debrisRefuse("parse", "bad-field-length");
        at += 8;
        if (length > total - at) return debrisRefuse("parse", "truncated-field");
        field[fields] = meshMessage + at;
        size[fields++] = (DWORD)length;
        at += (DWORD)length;
    }
    DWORD count, deadline;
    if (fields < 3 || !decimal(field[1], size[1], MESH_MAX_DEADLINE_MS, &deadline) ||
        !decimal(field[2], size[2], DEBRIS_MAX_ENTRIES, &count) || fields != 3 + 7 * count)
        return debrisRefuse("parse", "bad-fields");
    wchar_t *lock = widePath(field[0], size[0]);
    if (!lock) return debrisRefuse("parse", "bad-path");
    for (DWORD i = 0; i < count; i++) {
        DWORD f = 3 + 7 * i;
        DebrisEntry *entry = &debris[i];
        entry->path = widePath(field[f], size[f]);
        if (!entry->path) return debrisRefuse("parse", "bad-path");
        size_t prefix = wcsncmp(entry->path, L"\\\\?\\UNC\\", 8) == 0 ? 6 : 4;
        if (wcslen(entry->path) - prefix >= 240) return debrisRefuse("parse", "mesh-path-too-long");
        if (size[f + 1] != 1 || (field[f + 1][0] != 'd' && field[f + 1][0] != 'f')) return debrisRefuse("parse", "bad-kind");
        entry->directory = field[f + 1][0] == 'd';
        if (!narrowHex(field[f + 2], size[f + 2], &entry->volume) || !narrowHex(field[f + 3], size[f + 3], &entry->index) ||
            !narrowHex(field[f + 4], size[f + 4], &entry->time) || !narrowHex(field[f + 5], size[f + 5], &entry->size))
            return debrisRefuse("parse", "bad-id");
        entry->record = field[f + 6];
        entry->recordLength = size[f + 6];
        if (entry->directory ? entry->recordLength != 0 :
            (entry->recordLength < 1 || entry->recordLength > MESH_MAX_OWNER || entry->size != entry->recordLength || memchr(entry->record, 0, entry->recordLength)))
            return debrisRefuse("parse", "bad-record");
    }
    if (!debrisPlacement(lock, count)) return debrisRefuse("parse", "bad-placement");
    if (!meshArm(deadline)) return debrisRefuse("parse", "watchdog-unavailable");
    if (holdAncestors(lock, 0)) return debrisRefuse("guard", "ancestor-not-held");
    /* Guard canonical FIRST. Descendants then open under this exact pinned incarnation. */
    for (DWORD n = 0; n < count; n++) {
        DWORD i = n == 0 ? count - 1 : n - 1;
        DebrisEntry *entry = &debris[i];
        const char *why = NULL;
        entry->handle = openJudgedIds(entry->path, DELETE | READ_CONTROL | FILE_READ_ATTRIBUTES | SYNCHRONIZE |
            (entry->directory ? FILE_LIST_DIRECTORY : FILE_READ_DATA), entry->directory, entry->volume, entry->index, 1, entry->time, entry->size, &why);
        if (!entry->handle) return debrisRefuse("verify", "entry-unverified");
        static wchar_t final[MESH_MAX_PATH + 1];
        if (finalPath(entry->handle, final) || _wcsicmp(final, entry->path) != 0) return debrisRefuse("verify", "entry-location");
        /* Unlike generic removal, D also checks the supplied directory times/sizes BEFORE any mutation. */
        if (entry->directory) {
            BY_HANDLE_FILE_INFORMATION info;
            if (!GetFileInformationByHandle(entry->handle, &info) ||
                ((unsigned long long)info.ftLastWriteTime.dwHighDateTime << 32 | info.ftLastWriteTime.dwLowDateTime) != entry->time ||
                ((unsigned long long)info.nFileSizeHigh << 32 | info.nFileSizeLow) != entry->size)
                return debrisRefuse("verify", "directory-modified");
        } else if (sameRecord(entry->handle, entry->record, entry->recordLength) != 1) return debrisRefuse("verify", "record-unverified");
    }
    for (DWORD i = 0; i < count; i++) if (debris[i].directory && !debrisEntrySet(i, count)) return debrisRefuse("verify", "unexpected-entries");
    meshCheckpoint(L"debris-verified");
    if (meshExpired()) return debrisRefuse("verify", "deadline");
    for (DWORD i = 0; i < count; i++) {
        if (meshExpired()) return debrisOut("partial", "delete", "deadline", 2);
        if (!disposeHandle(debris[i].handle)) return debrisOut("partial", "delete", "entry-delete", 2);
        CloseHandle(debris[i].handle);
        debris[i].handle = NULL;
        wchar_t checkpoint[64];
        swprintf(checkpoint, 64, L"debris-after-%lu", (unsigned long)i);
        meshCheckpoint(checkpoint); /* hosted tests kill/join after EVERY real deletion, owner-first included */
    }
    return debrisOut("recovered", "cleanup", "clean", 0);
}

int wmain(int argc, wchar_t **argv) {
    BYTE userBuffer[256], ownerBuffer[256];
    user = tokenSid(TokenUser, userBuffer, sizeof(userBuffer));
    owner = tokenSid(TokenOwner, ownerBuffer, sizeof(ownerBuffer));
    if (!user || !owner) { printf("failed -1 token\n"); return 2; }
    const char *why = NULL;
    if (argc == 2 && wcscmp(argv[1], L"--mesh-retire-v1") == 0) return meshRetire(0);
    if (argc == 2 && wcscmp(argv[1], L"--mesh-recover-owner-v1") == 0) return meshRetire(1);
    if (argc == 2 && wcscmp(argv[1], L"--mesh-recover-debris-v1") == 0) return meshDebris();
    if (argc == 2 && wcscmp(argv[1], L"--capability-v1") == 0) { printf("FMRCAP1 mesh-retire-v1 abi=1\n"); return 0; }
    if (argc >= 2 && wcscmp(argv[1], L"--cache-create-v1") == 0) return cacheCreate(argc, argv);
    if (argc >= 2 && wcscmp(argv[1], L"--cache-private-v1") == 0) return cachePrivate(argc, argv);
    if (argc == 4 && wcscmp(argv[1], L"--cache-publish-v1") == 0) return cachePublish(argv[2], argv[3]);
    if (argc == 5 && wcscmp(argv[1], L"--private") == 0) {
        HANDLE handle = openJudged(argv[4], READ_CONTROL | FILE_READ_ATTRIBUTES, 0, argv[2], argv[3], NULL, NULL, &why);
        if (!handle) return fail("refused", 0, why, 3);
        why = privateDacl(handle);
        CloseHandle(handle);
        return why ? fail("refused", 0, why, 3) : fail("ok", 1, "private", 0);
    }
    int first = 1, dry = 0;
    if (argc > 1 && wcscmp(argv[1], L"--dry-run") == 0) { dry = 1; first = 2; }
    int count = (argc - first) / FIELDS;
    if (count < 1 || count > MAX_ENTRIES || (argc - first) % FIELDS != 0) { printf("failed -1 usage\n"); return 2; }
    if ((why = holdAncestors(argv[first + FIELDS * (count - 1) + 5], 0))) return fail("refused", count - 1, why, 3);
    for (int i = 0; i < count; i++) {
        wchar_t **arg = argv + first + FIELDS * i;
        int directory = wcscmp(arg[0], L"d") == 0;
        if (!directory && wcscmp(arg[0], L"f") != 0) return fail("failed", i, "usage", 2);
        HANDLE handle = openJudged(arg[5], DELETE | READ_CONTROL | FILE_READ_ATTRIBUTES, directory, arg[1], arg[2], arg[3], arg[4], &why);
        if (!handle) return why == MODIFIED ? fail("kept", i, why, 4) : fail("refused", i, why, 3);
        entries[i].handle = handle;
        entries[i].directory = directory;
    }
    /* Bind every descendant to the judged tree: its final path is the target's final path plus its relative name. */
    static wchar_t top[32768], final[32768];
    const wchar_t *topPath = argv[first + FIELDS * (count - 1) + 5];
    size_t topLength = wcslen(topPath);
    DWORD length = GetFinalPathNameByHandleW(entries[count - 1].handle, top, 32768, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
    if (length == 0 || length >= 32768) return fail("refused", count - 1, "final path unreadable", 3);
    for (int i = 0; i < count - 1; i++) {
        const wchar_t *path = argv[first + FIELDS * i + 5];
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
