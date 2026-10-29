#!/usr/bin/python3
"""Staged operator helper. No effects in --check. Never imported by the live link.
All four shell entrypoints share the SAME freeze check and fixed pin constants.
Initial migration rollback uses old state/config BEFORE seam installation only.
Release rollback changes only ExecStart's release path, never config or unit name.
"""
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import stat
import subprocess
import sys
import time

BASE = Path('/srv/scratch/paul/smarty-pi-tmp/pi-22f7c5b26c72/p.WBUV3x')
HERE = BASE / 'durable-switch'
HOME = Path('/home/paul')
OLD_CONFIG = BASE / 'nats-link-v2-aprime.json'
OLD_STATE = BASE / 'nats-link-state/aprime'
STATE = HOME / '.local/state/smarty/nats-link/aprime'
CONFIG = HOME / '.local/share/smarty-dev/nats-link/nats-link-v2-aprime.json'
FREEZE = HOME / '.local/state/smarty/nats-link/LINK-FREEZE'
UNIT = 'fv2-aprime-link-durable.service'
UNIT_FILE = HOME / '.config/systemd/user' / UNIT
INITIAL_UNIT = 'fv2-aprime-link11.service'
CURRENT_RELEASE = '4ec3b52d81a4a9afa34fb6f73b6297618a8449e6'
RELEASES = HOME / '.local/share/smarty-dev/fabric/releases'
NODE = HOME / '.local/share/node-v24.18.0-linux-x64/bin/node'
NODE_HASH = '41a74efb34cbde5c7632cdac0cf8bd1a14d0b8d73dc1e82755014d9a9ce70f5c'
PAUL = pwd.getpwnam('paul').pw_uid
ENV = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C', 'HOME': str(HOME),
       'XDG_RUNTIME_DIR': f'/run/user/{PAUL}',
       'DBUS_SESSION_BUS_ADDRESS': f'unix:path=/run/user/{PAUL}/bus',
       'PI_FABRIC_MESH_STATE_BACKEND': 'sqlite'}
EXCLUDED = {'link.lock'}  # Only this exact runtime lock; ledger custody files ARE copied/hashed.


class Refusal(Exception):
    pass


def refuse(message):
    raise Refusal(message)


def freeze():
    # Must precede argument validation, logging, mkdir, locks or any systemctl call.
    # lexists also refuses a dangling marker symlink. No override is provided.
    if os.path.lexists(FREEZE):
        print(f'REFUSE: LINK FREEZE is active: {FREEZE}', file=sys.stderr, flush=True)
        try:
            with open(FREEZE, 'r', errors='replace') as handle:
                for line in handle:
                    print(line, end='', file=sys.stderr, flush=True)
            print(file=sys.stderr, flush=True)
        except OSError:
            print('(marker contents unreadable; still frozen)', file=sys.stderr, flush=True)
        refuse('no link operation is permitted while LINK-FREEZE exists; no override')


def note(message):
    print(f'{dt.datetime.now(dt.timezone.utc).isoformat()} {message}', flush=True)


def digest(file):
    h = hashlib.sha256()
    with open(file, 'rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def chain(directory, missing=False):
    """Stricter than seam: NO sticky-dir exception, no symlinks, root/paul only."""
    directory = Path(directory)
    if not directory.is_absolute() or str(directory) != os.path.normpath(str(directory)):
        refuse(f'noncanonical path: {directory}')
    for part in reversed([directory, *directory.parents]):
        try:
            s = part.lstat()
        except FileNotFoundError:
            if missing:
                continue
            refuse(f'missing directory: {part}')
        if not stat.S_ISDIR(s.st_mode) or s.st_uid not in (0, PAUL) or s.st_mode & 0o022:
            refuse(f'unsafe ancestor {part}: mode={stat.S_IMODE(s.st_mode):04o} uid={s.st_uid}')


def file_safe(file, mode=None, paul=False, ancestors=True, single_link=True):
    file = Path(file)
    if ancestors:
        chain(file.parent)
    s = file.lstat()
    if not stat.S_ISREG(s.st_mode) or (single_link and s.st_nlink != 1) or s.st_uid not in ((PAUL,) if paul else (0, PAUL)) or s.st_mode & 0o022:
        refuse(f'unsafe file: {file} (requires regular, trusted owner, no group/other write; single-link={single_link})')
    if mode is not None and stat.S_IMODE(s.st_mode) != mode:
        refuse(f'wrong mode for {file}: expected {mode:04o}')
    return s


def mkdir_private(directory):
    freeze()
    chain(directory, missing=True)
    for part in reversed([Path(directory), *Path(directory).parents]):
        if not part.exists():
            freeze()
            part.mkdir(mode=0o700)
    chain(directory)


def write_new(file, data, mode=0o600):
    freeze()
    chain(file.parent)
    fd = os.open(file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'wb', closefd=False) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(fd)
    finally:
        os.close(fd)
    file_safe(file, mode, paul=True)
    sync_dir(file.parent)


def sync_dir(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def replace(file, data):
    freeze()
    chain(file.parent)
    if os.path.lexists(file):
        file_safe(file, paul=True)
    temp = file.with_name(f'.{file.name}.switch-{os.getpid()}-{time.time_ns()}')
    write_new(temp, data)
    freeze()
    os.replace(temp, file)
    sync_dir(file.parent)
    file_safe(file, 0o600, paul=True)


def run(args, timeout=30, effect=False):
    if effect:
        freeze()
    # Never print subprocess output on failure (may contain config/credential values).
    try:
        p = subprocess.run([str(a) for a in args], env=ENV, stdin=subprocess.DEVNULL,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        refuse(f'command timed out: {Path(str(args[0])).name}')
    if p.returncode != 0:
        refuse(f'command failed (exit {p.returncode}): {Path(str(args[0])).name}; raw output suppressed')
    return p.stdout


def show(unit, timeout=30):
    text = run(['/usr/bin/systemctl', '--user', 'show', unit,
                '--property=MainPID,ActiveState,SubState,FragmentPath,Transient,DropInPaths,LoadState'], timeout=timeout)
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line)


def proc(pid):
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
        fields = text[text.rindex(')') + 2:].split()
        return fields[19] if fields[0] != 'Z' else None
    except FileNotFoundError:
        return None


def command(pid):
    return Path(f'/proc/{pid}/cmdline').read_bytes().decode().rstrip('\0').split('\0')


def release_id(rel):
    if not re.fullmatch(r'[0-9a-f]{40}', rel):
        refuse('REL must be a full 40-hex release id, not current or a symlink')
    path = RELEASES / rel / 'bin/nats-link'
    file_safe(path)
    return path


def current_unit():
    pointer = BASE / 'link-current-unit'
    if pointer.exists():
        file_safe(pointer, paul=True)
        unit = pointer.read_text().strip()
    else:
        unit = INITIAL_UNIT
    if not unit.endswith('.service'):
        unit += '.service'
    if unit not in (INITIAL_UNIT, UNIT):
        refuse(f'unexpected link-current-unit: {unit}; re-inspect, do not guess')
    return unit


def running(unit, config, required=True, timeout=30):
    info = show(unit, timeout=timeout)
    pid = int(info.get('MainPID', '0'))
    if pid <= 1 or not proc(pid):
        if required:
            refuse(f'{unit} has no live MainPID')
        return info, None, None
    args = command(pid)
    if len(args) != 5 or args[0] != str(NODE) or args[2:] != ['start', '--config', str(config)]:
        refuse(f'{unit}: unexpected process argv; no action taken')
    relpath = Path(args[1])
    if relpath.parent.name != 'bin' or relpath.name != 'nats-link' or relpath.parent.parent.parent != RELEASES:
        refuse('current link is not a pinned release')
    rel = relpath.parent.parent.name
    release_id(rel)
    return info, pid, rel


def stop(unit):
    freeze()
    info = show(unit)
    pid = int(info.get('MainPID', '0'))
    start = proc(pid) if pid > 1 else None
    if info.get('LoadState') == 'not-found' and pid == 0:
        note(f'{unit} already absent/GCd; no process to stop')
        return
    note(f'stop {unit}; wait <=60s for MainPID={pid} start={start} to exit')
    deadline = time.monotonic() + 60
    run(['/usr/bin/systemctl', '--user', 'stop', '--no-block', unit], timeout=5, effect=True)
    # Event source is systemd stop; bounded /proc read-back is the safety net.
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        back = show(unit, timeout=min(5, remaining))
        gone = start is None or proc(pid) != start
        if gone and int(back.get('MainPID', '0')) == 0 and back.get('ActiveState') in ('inactive', 'failed'):
            note(f'stopped: original MainPID {pid} exited; manager inactive/failed')
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.25, remaining))
    refuse('MainPID/service did not stop within 60s; NO state copy or new start attempted')


def unit_bytes(rel, config):
    path = RELEASES / rel / 'bin/nats-link'
    return (f"[Unit]\nDescription=A' nats-link aprime-link-ryzen1 (fabric-v2, stable pin)\n\n"
            f'[Service]\nType=simple\nEnvironment=PI_FABRIC_MESH_STATE_BACKEND=sqlite\n'
            f'ExecStart={NODE} {path} start --config {config}\n'
            f'Restart=on-failure\nRestartSec=10s\nTimeoutStopSec=55s\nUMask=0077\n\n'
            f'[Install]\nWantedBy=default.target\n').encode()


def validate_unit(rel, config):
    file_safe(UNIT_FILE, 0o600, paul=True)
    if UNIT_FILE.read_bytes() != unit_bytes(rel, config):
        refuse('persistent unit differs from the staged template; refusing to overwrite unknown settings')
    info = show(UNIT)
    if info.get('Transient') != 'no' or info.get('FragmentPath') != str(UNIT_FILE) or info.get('DropInPaths'):
        refuse('stable unit must be persistent, load exactly this fragment, and have no drop-ins')


def configured_release():
    """Recover the release from our EXACT persistent fragment, even if MainPID=0."""
    file_safe(UNIT_FILE, 0o600, paul=True)
    pattern = (rb'^ExecStart=' + re.escape(str(NODE).encode()) + b' ' +
               re.escape(str(RELEASES).encode()) + rb'/([0-9a-f]{40})/bin/nats-link start --config ' +
               re.escape(str(CONFIG).encode()) + rb'$')
    found = re.findall(pattern, UNIT_FILE.read_bytes(), re.MULTILINE)
    if len(found) != 1:
        refuse('cannot identify release from the stable persistent unit')
    rel = found[0].decode()
    release_id(rel)
    validate_unit(rel, CONFIG)
    return rel


def durable_running_or_failed(allow_stopped=False):
    previous = configured_release()
    info, pid, actual = running(UNIT, CONFIG, required=not allow_stopped)
    if pid and actual != previous:
        refuse('running release differs from persistent fragment; re-inspect before swapping')
    if not pid:
        if info.get('ActiveState') not in ('inactive', 'failed'):
            refuse('stable unit is transitioning; do not race its existing job')
        state_not_busy(STATE)
    return pid, previous


def no_dropins():
    p = UNIT_FILE.with_name(UNIT_FILE.name + '.d')
    if os.path.lexists(p):
        refuse(f'drop-in directory exists: {p}; inspect instead of silently overriding')


def inventory(directory, hash_files=True):
    # State is private, plain directories/files only. Do not follow symlinks or copy devices.
    root = directory.lstat()
    if not stat.S_ISDIR(root.st_mode) or root.st_uid != PAUL or stat.S_IMODE(root.st_mode) != 0o700:
        refuse(f'state root must be paul-owned 0700 directory: {directory}')
    result, excluded = {}, []
    for parent, dirs, files in os.walk(directory, followlinks=False):
        for name in sorted(dirs + files):
            file = Path(parent) / name
            s = file.lstat()
            relative = file.relative_to(directory).as_posix()
            if s.st_uid != PAUL or s.st_mode & 0o077:
                refuse(f'non-private or foreign state entry: {relative}')
            if stat.S_ISDIR(s.st_mode):
                continue
            if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
                refuse(f'state entry must be a regular, single-link file: {relative}')
            if relative in EXCLUDED:
                excluded.append(relative)
            else:
                result[relative] = {'bytes': s.st_size, 'sha256': digest(file) if hash_files else 'NOT HASHED (--check/metadata only)'}
    return dict(sorted(result.items())), excluded


def state_not_busy(directory):
    lock = directory / 'link.lock'
    if not lock.exists():
        return
    value = json.loads(lock.read_text())
    pid = value.get('pid', 0)
    if not isinstance(pid, int) or pid < 2:
        refuse('malformed state lock')
    start = proc(pid)
    if start and (not value.get('startTime') or str(value['startTime']) == start):
        refuse(f'state lock still has a live holder: {directory} pid={pid}')


def old_config():
    file_safe(OLD_CONFIG, 0o600, paul=True)
    data = OLD_CONFIG.read_bytes()
    if len(data) > 1024 * 1024:
        refuse('old config exceeds the seam 1 MiB size cap')
    value = json.loads(data)
    if not isinstance(value, dict):
        refuse('old config must be a JSON object')
    if value.get('stateDir') != str(OLD_STATE):
        refuse('old config stateDir changed; no longer the requested migration input')
    return value


def durable_config_check():
    file_safe(CONFIG, 0o600, paul=True)
    if json.loads(CONFIG.read_bytes()).get('stateDir') != str(STATE):
        refuse('durable config stateDir differs from the permanent pin')
    inventory(STATE, hash_files=False)


def check_common():
    freeze()
    note(f'LINK-FREEZE absent: {FREEZE}; checked before effects, rechecked at every effect; no override')
    if os.geteuid() != PAUL:
        refuse('run as paul, not root or another account')
    # The supplied interpreter is a regular file with TWO hardlinks. The seam pins
    # its bytes, not its nlink; nlink=1 remains mandatory for config/unit/state files.
    file_safe(NODE, single_link=False)
    if digest(NODE) != NODE_HASH:
        refuse('pinned node interpreter sha256 changed; do not upgrade or replace it')
    for path in (STATE, CONFIG.parent, UNIT_FILE.parent):
        chain(path, missing=True)
        note(f'ancestor check PASS: {path} (missing parents would be created 0700; none created by --check)')
    no_dropins()
    note(f'fixed pins: unit={UNIT}; config={CONFIG}; interpreter={NODE}; sha256={NODE_HASH}')


def preflight(kind, rel, unit):
    check_common()
    release_id(rel)
    if unit != UNIT:
        refuse(f'unit name is permanently fixed: use {UNIT}; a new name would break the seam pin')
    if kind == 'switch':
        old_config()
        chain(OLD_STATE)
        entries, locks = inventory(OLD_STATE, hash_files=False)
        note(f'state files presently exist (NOT a stopped snapshot): {sorted(entries)}; excluded runtime lock present={locks}')
        if os.path.lexists(STATE):
            inventory(STATE, hash_files=False)
            if any(STATE.iterdir()):
                refuse('durable target exists and is not empty; never merge or overwrite state')
        if os.path.lexists(CONFIG) or os.path.lexists(UNIT_FILE):
            refuse('durable config or stable unit already exists; use release-swap, never rerun migration')
        if show(UNIT).get('LoadState') != 'not-found':
            refuse('stable name is already loaded (including a failed transient); choose no new name: re-inspect')
        cur = current_unit()
        if cur != INITIAL_UNIT:
            refuse('migration is one-shot from the inspected original unit only')
        _, pid, running_rel = running(cur, OLD_CONFIG)
        if running_rel != CURRENT_RELEASE:
            refuse('live release changed since preparation; re-inspect before switching')
        note(f'current={cur} MainPID={pid} release={running_rel}; stopped-state copy will be verified after stop')
    elif kind == 'reswitch':
        original = old_config()
        chain(OLD_STATE)
        inventory(OLD_STATE, hash_files=False)
        validate_unit(CURRENT_RELEASE, OLD_CONFIG)
        info, pid, running_rel = running(UNIT, OLD_CONFIG)
        if running_rel != CURRENT_RELEASE or info.get('ActiveState') != 'active' or info.get('SubState') != 'running':
            refuse('reswitch requires the stable unit running CURRENT_RELEASE on OLD_CONFIG')
        initial = show(INITIAL_UNIT)
        if int(initial.get('MainPID', '0')) != 0 or initial.get('ActiveState') not in ('inactive', 'failed'):
            refuse('reswitch requires the original unit to be stopped')
        file_safe(CONFIG, 0o600, paul=True)
        if len(CONFIG.read_bytes()) > 1024 * 1024:
            refuse('stale durable config exceeds the seam 1 MiB size cap')
        if not reswitch_json_equal(json.loads(CONFIG.read_bytes()), dict(original, stateDir=str(STATE))):
            refuse('stale durable config diff is not ONLY stateDir; refusing to archive unknown edits')
        chain(STATE)
        inventory(STATE, hash_files=False)
        state_not_busy(STATE)
        reswitch_config(original)
        note(f'reswitch input: {UNIT} MainPID={pid} release={running_rel}; original unit stopped; stale config/state will be archived, never deleted')
    elif kind == 'rollback':
        old_config()
        chain(OLD_STATE)
        inventory(OLD_STATE, hash_files=False)
        info, pid, current = running(INITIAL_UNIT, OLD_CONFIG, required=False)
        if pid and current != CURRENT_RELEASE:
            refuse('original unit now runs a different release; re-inspect')
        # Before seam pinning only. The durable copy is never copied back over old state.
        if os.path.lexists(UNIT_FILE):
            file_safe(UNIT_FILE, 0o600, paul=True)
            data = UNIT_FILE.read_bytes()
            if data not in (unit_bytes(rel, CONFIG), unit_bytes(rel, OLD_CONFIG)):
                refuse('migration rollback must use SAME release as the attempted switch; unknown unit refused')
            selected_config = CONFIG if data == unit_bytes(rel, CONFIG) else OLD_CONFIG
            info, pid, current = running(UNIT, selected_config, required=False)
            if pid and current != rel:
                refuse('rollback REL differs from running stable-unit release')
        elif show(UNIT).get('LoadState') != 'not-found':
            refuse('stable name is loaded without our persistent file; refusing to stop/overwrite it')
        note('migration rollback: OLD config/state, same stable unit; permitted BEFORE seam installation only')
    else:
        durable_config_check()
        pid, previous = durable_running_or_failed(allow_stopped=kind == 'release-rollback')
        note(f'release-only swap: MainPID={pid or 0}; previous={previous}; next={rel}; config bytes will not be edited')
    return True


def write_pointer():
    replace(BASE / 'link-current-unit', (UNIT + '\n').encode())


def start(rel, config, restarting=False):
    freeze()
    run(['/usr/bin/systemctl', '--user', 'daemon-reload'], effect=True)
    since = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f UTC')
    epoch = time.time()
    if restarting:
        old_pid = int(show(UNIT).get('MainPID', '0'))
        old_start = proc(old_pid) if old_pid > 1 else None
        deadline = time.monotonic() + 60
        run(['/usr/bin/systemctl', '--user', 'restart', '--no-block', UNIT], timeout=5, effect=True)
        while old_start and proc(old_pid) == old_start and time.monotonic() < deadline:
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        if old_start and proc(old_pid) == old_start:
            refuse('restart did not retire original MainPID within 60s')
        note(f'restart: original MainPID={old_pid} exited; same unit name retained')
    else:
        run(['/usr/bin/systemctl', '--user', 'start', UNIT], effect=True)
    # Recovery selects the stable unit directly even if the predecessor is GCd.
    # Publish link-current-unit only after fresh linked + matching process identity.
    note(f'started {UNIT}; release={rel}; config={config}')
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        info, pid, actual = running(UNIT, config, required=False, timeout=min(5, remaining))
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        journal = run(['/usr/bin/journalctl', '--user', '-u', UNIT, '--since', since,
                       '--no-pager', '-o', 'cat'], timeout=min(10, remaining))
        if pid and actual == rel and 'linked' in journal:
            note(f'linked (FRESH journal since {since}); MainPID={pid}; release={actual}')
            write_pointer()
            return epoch
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    refuse('no fresh linked event and matching MainPID within 90s')


def probe_pairs(epoch, rel):
    # Native Fabric probes are sent by the operator, NOT a new control plane in this helper.
    # Require 3 delivered + reverse ACK pairs from the existing link-probe log during this attempt.
    file = BASE / 'link-probe.jsonl'
    file_safe(file, paul=True)
    sent, ack = {}, {}
    for line in file.read_text().splitlines():
        try:
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            stamp = dt.datetime.fromisoformat(row.get('sentAt', row.get('ackSentAt', '')).replace('Z', '+00:00')).timestamp()
        except (ValueError, TypeError, AttributeError):
            continue
        if stamp < epoch or stamp > epoch + 900:
            continue
        n = row.get('n')
        if not isinstance(n, int):
            continue
        if row.get('ok') is True and row.get('class') == 'delivered' and row.get('release') in (rel, rel[:8]):
            sent[n] = row
        if row.get('ack') is True and row.get('dir') == 'v2->v1':
            ack[n] = row
    return sorted(set(sent) & set(ack))


def observe(epoch, rel, config):
    # Normal events: new journal 'linked' and native bidirectional probe ACK records.
    # Safety net: 30s read-only health sample, bounded to 15 min from new start.
    deadline = time.monotonic() + max(0, epoch + 900 - time.time())
    last_ok = False
    first = True
    while True:
        freeze()
        # Never begin another network/manager read at the deadline. Each read
        # shares the remaining original budget; a slow final status cannot add
        # another 20-50 seconds before automatic rollback.
        remaining = deadline - time.monotonic()
        if remaining > 0:
            try:
                _, pid, actual = running(UNIT, config, timeout=min(5, remaining))
                state = Path(json.loads(config.read_bytes())['stateDir'])
                lock = json.loads((state / 'link.lock').read_text())
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    refuse('observation budget exhausted during identity read')
                output = run([NODE, RELEASES / rel / 'bin/nats-link', 'status', '--config', config], timeout=min(20, remaining))
                status = json.loads(output)
                last_ok = (actual == rel and lock.get('pid') == pid and
                           str(lock.get('startTime')) == proc(pid) and
                           status.get('ok') is True and status.get('problems') == [])
            except (Refusal, OSError, ValueError, TypeError, KeyError, AttributeError):
                last_ok = False
        try:
            pairs = probe_pairs(epoch, rel)  # Final deadline read is local-only.
        except (Refusal, OSError, ValueError, TypeError):
            pairs = []
        note(f'{"FIRST PROBE read-back" if first else "health"}: status/lock={"PASS" if last_ok else "FAIL"}; new bidirectional probe pairs={pairs}')
        first = False
        if time.monotonic() >= deadline:
            if not last_ok or len(pairs) < 3:
                refuse('15-minute probe deadline failed (requires healthy status/lock plus >=3 NEW delivered/reverse-ACK probe pairs)')
            note('15-minute acceptance PASS; config path, unit name and node bytes are now permanent seam pins')
            return
        time.sleep(min(30, max(0, deadline - time.monotonic())))


def migration_rollback(rel):
    freeze()
    # May be reached before unit creation or before link-current-unit was updated.
    # Always stop the stable unit if loaded; then the old unit if it is still active.
    for unit in (UNIT, INITIAL_UNIT):
        info = show(unit)
        if int(info.get('MainPID', '0')) > 1 or info.get('ActiveState') not in ('inactive', 'failed', None):
            stop(unit)
    state_not_busy(OLD_STATE)
    state_not_busy(STATE) if STATE.exists() else None
    mkdir_private(UNIT_FILE.parent)
    if UNIT_FILE.exists():
        replace(UNIT_FILE, unit_bytes(rel, OLD_CONFIG))
    else:
        write_new(UNIT_FILE, unit_bytes(rel, OLD_CONFIG))
    start(rel, OLD_CONFIG)
    note('migration rollback complete: SAME release, OLD temp config/state, SAME stable unit; seam MUST NOT be installed/pinned yet')


def migrate(rel):
    stopped = False
    try:
        stop(INITIAL_UNIT)
        stopped = True
        freeze()
        state_not_busy(OLD_STATE)
        before, excluded = inventory(OLD_STATE)
        mkdir_private(STATE)
        if any(STATE.iterdir()):
            refuse('durable state became nonempty; refusing to merge')
        run(['/usr/bin/cp', '-a', str(OLD_STATE) + '/.', str(STATE) + '/'], timeout=120, effect=True)
        after, excluded_after = inventory(OLD_STATE)
        copied, copied_excluded = inventory(STATE)
        if before != after or before != copied:
            refuse('stopped-state file count/sha256 manifest mismatch')
        for parent, dirs, files in os.walk(STATE, topdown=False):
            for name in files:
                fd = os.open(Path(parent) / name, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            sync_dir(parent)
        note(f'manifest MATCH: files={len(before)}; excluded exact runtime lock={sorted(set(excluded + excluded_after + copied_excluded))}; all other lock files INCLUDED')
        note('files present (relative names / bytes / sha256): ' + json.dumps(copied, sort_keys=True))
        mkdir_private(CONFIG.parent)
        original = old_config()
        candidate = dict(original, stateDir=str(STATE))
        diff = [key for key in sorted(set(original) | set(candidate)) if original.get(key) != candidate.get(key)]
        if diff != ['stateDir']:
            refuse('config diff is not ONLY stateDir')
        note('key-level config diff: stateDir CHANGED; all other keys UNCHANGED; credential-ish values NEVER rendered')
        config_bytes = (json.dumps(candidate, indent=2) + '\n').encode()
        if len(config_bytes) > 1024 * 1024:
            refuse('durable config serialization would exceed the seam 1 MiB cap')
        write_new(CONFIG, config_bytes)
        if json.loads(CONFIG.read_bytes()) != candidate:
            refuse('durable config read-back mismatch')
        durable_config_check()
        mkdir_private(UNIT_FILE.parent)
        write_new(UNIT_FILE, unit_bytes(rel, CONFIG))
        epoch = start(rel, CONFIG)
        observe(epoch, rel, CONFIG)
    except (Refusal, OSError, ValueError) as error:
        note(f'migration failed: {type(error).__name__}; rollback only if initial stop completed')
        if stopped:
            migration_rollback(rel)
        raise


def reswitch_json_equal(left, right):
    """Type-exact JSON, ordered arrays; reject nonfinite numbers, distinguish 1/1.0/true."""
    return (json.dumps(left, sort_keys=True, separators=(',', ':'), allow_nan=False) ==
            json.dumps(right, sort_keys=True, separators=(',', ':'), allow_nan=False))


def reswitch_config(original):
    """The reviewed OLD_CONFIG ALREADY contains LINK-STANDIN; change ONLY stateDir."""
    allow = original.get('allow')
    if not isinstance(allow, dict) or not isinstance(allow.get('file'), list) or not isinstance(allow.get('nats'), list):
        refuse('reswitch requires existing allow.file and allow.nats lists')
    row = {"kind": "root", "id": "session:4070cfb8-03db-4b01-85e4-aee846e0f14c",
           "machine": "org-linkstandin", "send": ["followUp"], "receive": ["followUp", "cancel"],
           "note": "LINK-STANDIN seat-shaped stand-in pod (org-smarty-pants-inc/link-standin, epyc1); remove after 10-30 13:00Z sample"}
    nats = original.get('nats')
    if not isinstance(nats, dict) or not isinstance(nats.get('pathBMachines'), list):
        refuse('reswitch requires an existing nats.pathBMachines list')
    matches = [entry for entry in allow['nats'] if isinstance(entry, dict) and
               (entry.get('id') == row['id'] or entry.get('machine') == row['machine'])]
    if len(matches) != 1 or not reswitch_json_equal(matches[0], row):
        refuse('OLD_CONFIG must already contain exactly the expected LINK-STANDIN allow.nats row; no additions or repairs permitted')
    if (any(type(machine) is not str for machine in nats['pathBMachines']) or
            nats['pathBMachines'].count('org-linkstandin') != 1):
        refuse('OLD_CONFIG must already contain exactly one org-linkstandin in nats.pathBMachines')
    candidate = dict(original, stateDir=str(STATE))
    diff = [key for key in sorted(set(original) | set(candidate))
            if key not in original or key not in candidate or
            not reswitch_json_equal(original[key], candidate[key])]
    if diff != ['stateDir']:
        refuse('reswitch config diff must be exactly stateDir; allow and nats MUST remain unchanged')
    data = (json.dumps(candidate, indent=2, allow_nan=False) + '\n').encode()
    if len(data) > 1024 * 1024:
        refuse('durable config serialization would exceed the seam 1 MiB cap')
    return candidate, data


def archive_reswitch_stale():
    """Same-parent, atomic no-clobber renames; fsync each completed rename."""
    import ctypes
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    archives = [(path, path.with_name(f'{path.name}.stale-{stamp}')) for path in (CONFIG, STATE)]
    for source, target in archives:
        if os.path.lexists(target):
            refuse(f'stale archive already exists: {target}; never overwrite an archive')
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    for source, target in archives:
        freeze()
        chain(source.parent)
        # AT_FDCWD=-100, RENAME_NOREPLACE=1: even a racing collision cannot delete an archive.
        if rename(-100, os.fsencode(source), -100, os.fsencode(target), 1) != 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
        sync_dir(source.parent)
        note(f'archived stale path by rename: {source} -> {target}; parent fsynced')


def reswitch_restore_unit():
    """Restore ONLY the exact old fragment, before any state-custody refusal.

    Never edit OLD_CONFIG or OLD_STATE, and never delete partial output. Neither
    a late freeze nor busy state may strand the persistent unit on NEWREL/CONFIG.
    """
    chain(UNIT_FILE.parent)
    file_safe(UNIT_FILE, paul=True)
    data = unit_bytes(CURRENT_RELEASE, OLD_CONFIG)
    temp = UNIT_FILE.with_name(f'.{UNIT_FILE.name}.recover-{os.getpid()}-{time.time_ns()}')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb', closefd=False) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(fd)
    finally:
        os.close(fd)
    file_safe(temp, 0o600, paul=True)
    os.replace(temp, UNIT_FILE)
    sync_dir(UNIT_FILE.parent)
    file_safe(UNIT_FILE, 0o600, paul=True)
    if UNIT_FILE.read_bytes() != data:
        refuse('recovery fragment read-back mismatch; manual recovery required')
    note('reswitch old fragment restored exactly: CURRENT_RELEASE + OLD_CONFIG; service restart still requires free OLD_STATE')


def reswitch_rollback():
    # The repair brief authorizes restoration THEN refusal on a late freeze.
    # Scope that exception to this fixed rollback only, never forward work or a
    # CLI override. Legacy freeze/stop/start definitions remain byte-identical.
    # A real OLD_STATE holder is NEVER bypassed: restore the fragment, then refuse.
    global freeze
    previous_freeze = freeze

    def recovery_only():
        pass

    freeze = recovery_only
    try:
        try:
            stop(UNIT)
        finally:
            # A failed stop or busy OLD_STATE must not leave the persistent
            # fragment pinned to the attempted release/durable config.
            reswitch_restore_unit()
            # Publish the restored fragment to the manager even if custody (or
            # the preceding stop) prevents startup. Never leave a cached NEWREL.
            run(['/usr/bin/systemctl', '--user', 'daemon-reload'], effect=True)
        state_not_busy(OLD_STATE)
        start(CURRENT_RELEASE, OLD_CONFIG)
    finally:
        freeze = previous_freeze
    note('reswitch rollback complete: CURRENT_RELEASE, OLD_CONFIG/OLD_STATE, SAME stable unit; new durable config/state and stale archives retained')
    freeze()  # Retain a late marker and refuse, AFTER the exact old service is restored.


def reswitch(rel):
    import signal
    global note
    previous_note = note
    handlers = {}
    stopping = False
    recovering = False

    def best_effort_note(message):
        # Reswitch-only binding: preserve all legacy operation definitions/behavior.
        try:
            previous_note(message)
        except (OSError, ValueError):  # EPIPE, EBADF, closed stdout/stderr, etc.
            pass

    class ReswitchCancelled(BaseException):
        # A BaseException, so legacy observe()/probe handlers that catch Refusal/OSError can't swallow it.
        pass

    cancelled = []

    def interrupted(signum, frame):
        # Raise inside the transaction so stack unwinding closes in-flight fds.
        # Repeated graceful signals MUST NOT interrupt the one recovery attempt.
        if not recovering:
            cancelled.append(signum)  # latched: re-checked after observe() even if a raise were lost
            # Only observe's protected reads need to bypass its Refusal catches.
            while frame is not None and frame.f_code is not observe.__code__:
                frame = frame.f_back
            raise (ReswitchCancelled if frame is not None else Refusal)(f'reswitch interrupted by {signal.Signals(signum).name}; old-state recovery required')

    note = best_effort_note
    try:
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            handlers[signum] = signal.signal(signum, interrupted)
        original = old_config()
        candidate, config_bytes = reswitch_config(original)
        # Archive failures precede any stop: completed renames stay renamed, no rollback.
        archive_reswitch_stale()
        freeze()
        stopping = True  # A stop can take effect and then raise; recovery must cover that too.
        stop(UNIT)
        freeze()
        state_not_busy(OLD_STATE)
        before, excluded = inventory(OLD_STATE)
        mkdir_private(STATE)
        if any(STATE.iterdir()):
            refuse('durable state became nonempty; refusing to merge')
        run(['/usr/bin/cp', '-a', str(OLD_STATE) + '/.', str(STATE) + '/'], timeout=120, effect=True)
        after, excluded_after = inventory(OLD_STATE)
        copied, copied_excluded = inventory(STATE)
        if before != after or before != copied:
            refuse('stopped-state file count/sha256 manifest mismatch')
        for parent, dirs, files in os.walk(STATE, topdown=False):
            for name in files:
                fd = os.open(Path(parent) / name, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            sync_dir(parent)
        note(f'manifest MATCH: files={len(before)}; excluded exact runtime lock={sorted(set(excluded + excluded_after + copied_excluded))}; all other lock files INCLUDED')
        note('files present (relative names / bytes / sha256): ' + json.dumps(copied, sort_keys=True))
        if not reswitch_json_equal(old_config(), original):
            refuse('OLD_CONFIG changed during reswitch; refusing an unreviewed config diff')
        note("key-level config diff: ['stateDir']; allow.file/allow.nats/nats.pathBMachines and all other keys UNCHANGED; credential-ish values NEVER rendered")
        write_new(CONFIG, config_bytes)
        if not reswitch_json_equal(json.loads(CONFIG.read_bytes()), candidate):
            refuse('durable config read-back mismatch; reswitch permits ONLY stateDir')
        durable_config_check()
        replace(UNIT_FILE, unit_bytes(rel, CONFIG))
        epoch = start(rel, CONFIG)
        observe(epoch, rel, CONFIG)
        if cancelled:
            raise Refusal(f'reswitch interrupted by {signal.Signals(cancelled[-1]).name}; old-state recovery required')
    except BaseException as error:
        recovering = True
        if stopping:
            try:
                note(f'reswitch failed: {type(error).__name__}; restoring CURRENT_RELEASE on OLD_CONFIG')
            finally:
                reswitch_rollback()
        raise
    finally:
        recovering = True
        for signum, handler in handlers.items():
            signal.signal(signum, handler)
        note = previous_note


def swap(rel, allow_stopped=False):
    _, previous = durable_running_or_failed(allow_stopped=allow_stopped)
    before = UNIT_FILE.read_bytes()
    old_hash = digest(CONFIG)
    new = unit_bytes(rel, CONFIG)
    old_path = str(RELEASES / previous / 'bin/nats-link').encode()
    new_path = str(RELEASES / rel / 'bin/nats-link').encode()
    if before.count(old_path) != 1 or before.replace(old_path, new_path) != new:
        refuse('unit diff is not ONLY the release path')
    note(f'release rollback command: {HERE}/link-release-rollback.sh {previous}')
    changed = False
    try:
        replace(UNIT_FILE, new)
        changed = True
        if digest(CONFIG) != old_hash:
            refuse('config bytes changed during release swap; no config edit by this script')
        # daemon-reload + restart --no-block, then bound original MainPID exit to 60s.
        epoch = start(rel, CONFIG, restarting=True)
        observe(epoch, rel, CONFIG)
    except (Refusal, OSError, ValueError):
        if changed:
            freeze()
            note(f'release swap failed; automatic rollback to release={previous}; SAME unit/config/node')
            stop(UNIT)
            replace(UNIT_FILE, before)
            start(previous, CONFIG)
        raise


def main():
    freeze()  # Absolutely first operation, including --check and malformed invocations.
    args = sys.argv[1:]
    check = '--check' in args
    if check:
        args.remove('--check')
    if not args:
        refuse('missing operation')
    kind = args.pop(0)
    if kind not in ('switch', 'rollback', 'release-swap', 'release-rollback', 'reswitch'):
        refuse('unknown operation')
    wanted = 2 if kind in ('switch', 'rollback') else 1
    if len(args) != wanted:
        refuse('usage: [--check] REL [fv2-aprime-link.service]; no other flags/overrides')
    rel = args[0]
    unit = args[1] if wanted == 2 else UNIT
    preflight(kind, rel, unit)
    if check:
        note('CHECK ONLY: PASS; no files/directories/logs/locks written; no stop/start/restart/reload; no network probes')
        return
    # One operator at a time, across migration, swaps and both rollback entrypoints.
    # --check never creates or takes this lock. The persistent inode is never removed.
    mkdir_private(FREEZE.parent)
    lock_path = FREEZE.parent / '.link-operator.lock'
    freeze()
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        s = os.fstat(fd)
        if not stat.S_ISREG(s.st_mode) or s.st_uid != PAUL or s.st_nlink != 1 or stat.S_IMODE(s.st_mode) != 0o600:
            refuse('unsafe operator lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            refuse('another link operation is still running; never run overlapping switches/rollbacks')
        freeze()
        preflight(kind, rel, unit)  # Revalidate after acquiring operation custody.
        if kind == 'switch':
            migrate(rel)
        elif kind == 'rollback':
            migration_rollback(rel)
        elif kind == 'reswitch':
            reswitch(rel)
        else:
            swap(rel, allow_stopped=kind == 'release-rollback')
    finally:
        os.close(fd)


if __name__ == '__main__':
    try:
        main()
    except (Refusal, OSError, ValueError) as error:
        # No raw JSON values, paths from credential keys or command stderr.
        message = str(error) if isinstance(error, Refusal) else type(error).__name__
        print('REFUSE: ' + message, file=sys.stderr, flush=True)
        sys.exit(2)
