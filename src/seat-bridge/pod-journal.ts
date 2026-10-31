import { SUFFIX_MAX } from "../seat-drain/journal-branch.js";

/** Fixed read-only argv for the pinned 0.112.0 image; no shell or in-image Fabric CLI. */
export const POD_PYTHON = "/usr/bin/python3";
export const POD_JOURNAL_SCRIPT = `# b5 harness handoff: exact resume exec argv; only FILE and SEALED are substituted.
import os, sys, stat, hashlib, base64, json
fd = None
try:
    sealed = int(sys.argv[2])
    if sealed <= 0:
        raise ValueError("sealed-size-invalid")
    fd = os.open(sys.argv[1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("journal-not-regular")
    if before.st_size < sealed:
        raise ValueError("journal-prefix-mismatch")
    if before.st_size - sealed > ${SUFFIX_MAX}:
        raise ValueError("journal-suffix-too-large")
    whole, prefix = hashlib.sha256(), hashlib.sha256()
    suffix, size = bytearray(), 0
    while True:
        chunk = os.read(fd, 1024 * 1024)
        if not chunk:
            break
        whole.update(chunk)
        cut = max(0, min(len(chunk), sealed - size))
        prefix.update(chunk[:cut])
        suffix.extend(chunk[cut:])
        size += len(chunk)
        if len(suffix) > ${SUFFIX_MAX}:
            raise ValueError("journal-suffix-too-large")
    after = os.fstat(fd)
    current = os.stat(sys.argv[1], follow_symlinks=False)
    if (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino) or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns) or size != after.st_size:
        raise ValueError("journal-changed-during-read")
    print(json.dumps({"size": size, "sha256": whole.hexdigest(), "prefixSha256": prefix.hexdigest(),
        "suffixBase64": base64.b64encode(suffix).decode("ascii"), "mtimeNs": str(after.st_mtime_ns)}))
except Exception as error:
    print(json.dumps({"error": str(error) if isinstance(error, ValueError) else "journal-unreadable"}))
    sys.exit(1)
finally:
    if fd is not None:
        os.close(fd)
`;

/** The image projects its own authenticated NATS participant into mesh/participants/<sha256(id)>.json.
 * mainProcess is produced by ownMainProcess(), not reconstructed from process.title's overwritten argv.
 */
export const POD_OWNER_SCRIPT = `# b5 harness handoff: exact publish exec argv; only MESH and SID are substituted.
import os, sys, stat, hashlib, json, socket
try:
    root = "session:" + sys.argv[2]
    key = hashlib.sha256(root.encode()).hexdigest()
    file = os.path.join(sys.argv[1], "participants", key + ".json")
    fd = os.open(file, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("pod-owner-not-regular")
        raw = os.read(fd, 65537)
        if len(raw) > 65536:
            raise ValueError("pod-owner-too-large")
    finally:
        os.close(fd)
    entry = json.loads(raw)
    value, marker = entry["value"], entry["natsProjection"]
    main = value["mainProcess"]
    pid = main["pid"]
    incarnation = value["ownerIncarnation"]
    if entry["format"] != 1 or entry["key"] != "topology/participants/" + key or value["kind"] != "root" or value["id"] != root or value["rootId"] != root or value["sessionId"] != sys.argv[2] or value["ownerIdentityId"] != root:
        raise ValueError("pod-owner-invalid")
    if not isinstance(incarnation, str) or not incarnation or len(incarnation) > 128 or marker["format"] != 1 or marker["incarnation"] != incarnation or marker["owner"] != value["ownerHostId"] + "#" + incarnation or marker["kvKey"] != marker["machine"] + ".p." + key[:32] or type(marker["revision"]) is not int or marker["revision"] < 1 or value.get("remoteHost") is not None:
        raise ValueError("pod-owner-invalid")
    if type(pid) is not int or pid < 1 or main["host"] != socket.gethostname():
        raise ValueError("pod-owner-process-mismatch")
    def process_identity():
        with open("/proc/%d/stat" % pid) as handle:
            text = handle.read()
        fields = text[text.rindex(")") + 2:].split()
        with open("/proc/%d/comm" % pid) as handle:
            comm = handle.read().strip()
        if not text.startswith(str(pid) + " (") or fields[0] in ("Z", "X") or comm != "pi" or fields[19] != main["startTime"]:
            raise ValueError("pod-owner-process-mismatch")
        return fields[19]
    start = process_identity()
    # A second identity read fences PID exit/reuse across this observation.
    if process_identity() != start:
        raise ValueError("pod-owner-process-mismatch")
    print(json.dumps({"rootId": root, "ownerHostId": value["ownerHostId"], "ownerIdentityId": value["ownerIdentityId"],
        "ownerIncarnation": incarnation, "machine": marker["machine"], "pid": pid, "processStartedAt": start,
        "projectionKey": marker["kvKey"], "projectionRevision": marker["revision"]}))
except Exception as error:
    print(json.dumps({"error": str(error) if isinstance(error, ValueError) else "pod-owner-unreadable"}))
    sys.exit(1)
`;

export const podJournalArgv = (file: string, sealedBytes: number): string[] =>
  [POD_PYTHON, "-I", "-c", POD_JOURNAL_SCRIPT, file, String(sealedBytes)];
export const podOwnerArgv = (mesh: string, sessionId: string): string[] =>
  [POD_PYTHON, "-I", "-c", POD_OWNER_SCRIPT, mesh, sessionId];
