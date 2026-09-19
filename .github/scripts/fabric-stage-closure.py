"""Stage the three fixed public layers safely and describe every unpacked member.
Usage: python3 fabric-stage-closure.py EVIDENCE EMPTY_DEST MANIFEST
No install, build, pack, network or private source input.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import stat
import sys
import tarfile


def digest(stream):
    h = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
        h.update(chunk)
    return h.hexdigest()


def inventory(root):
    result = []
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            p = Path(directory) / name
            info = p.lstat()
            row = {'path': p.relative_to(root).as_posix(), 'mode': stat.S_IMODE(info.st_mode)}
            if p.is_symlink():
                assert p.resolve(strict=True).is_relative_to(root.resolve()), 'escaping link'
                row.update(type='symlink', target=os.readlink(p))
            elif p.is_dir():
                row.update(type='directory')
            elif p.is_file():
                with p.open('rb') as f:
                    row.update(type='file', size=info.st_size, sha256=digest(f))
            else:
                raise ValueError('Special member')
            result.append(row)
    return sorted(result, key=lambda row: row['path'])


def stage(evidence, destination):
    layers = [('fork-source.tar', 'pi-fabric/'), ('pi-fabric-0.92.25.tgz', 'package/'),
              ('node_modules.tar.gz', '')]
    plan, contents, total = {}, {}, 0
    archives = []
    try:
        for filename, prefix in layers:
            archive = tarfile.open(evidence / filename)
            archives.append(archive)
            seen = set()
            for member in archive:
                raw = member.name.rstrip('/')
                assert not raw.startswith('/') and '..' not in PurePosixPath(raw).parts
                assert raw == PurePosixPath(raw).as_posix(), 'noncanonical member'
                assert raw not in seen, 'duplicate archive member'
                seen.add(raw)
                if prefix:
                    if raw == prefix.rstrip('/'):
                        assert member.isdir()
                        continue
                    assert raw.startswith(prefix), 'unexpected layer prefix'
                    raw = raw[len(prefix):]
                else:
                    assert raw == 'node_modules' or raw.startswith('node_modules/')
                assert raw and '.git' not in PurePosixPath(raw).parts
                assert not member.mode & 0o7000, 'special mode'
                if member.isdir():
                    row = {'path': raw, 'type': 'directory', 'mode': 0o755}
                elif member.issym():
                    target = member.linkname
                    resolved = posixpath.normpath(posixpath.join(posixpath.dirname(raw), target))
                    assert target and not target.startswith('/')
                    assert resolved != '..' and not resolved.startswith('../'), 'escaping link'
                    row = {'path': raw, 'type': 'symlink', 'mode': 0o777, 'target': target}
                elif member.isfile():
                    total += member.size
                    assert total <= 2 * 1024**3, 'closure exceeds bound'
                    stream = archive.extractfile(member)
                    with stream:
                        sha = digest(stream)
                    row = {'path': raw, 'type': 'file', 'mode': 0o755 if member.mode & 0o111 else 0o644,
                           'size': member.size, 'sha256': sha}
                    contents[raw] = (archive, member)
                else:
                    raise ValueError('Hard link or special member is forbidden')
                if raw in plan:
                    assert plan[raw] == row, 'conflicting layers'
                plan[raw] = row
                assert len(plan) <= 200000, 'member count exceeds bound'
        for name in list(plan):
            for parent in PurePosixPath(name).parents:
                if str(parent) == '.':
                    continue
                row = {'path': str(parent), 'type': 'directory', 'mode': 0o755}
                assert str(parent) not in plan or plan[str(parent)] == row, 'non-directory ancestor'
                plan[str(parent)] = row
        assert len(plan) <= 200000, 'expanded member count exceeds bound'
        assert not destination.exists(), 'destination must be new'
        destination.mkdir(mode=0o700, parents=False)
        for name, row in sorted(plan.items(), key=lambda item: (len(PurePosixPath(item[0]).parts), item[0])):
            path = destination / name
            if row['type'] == 'directory':
                path.mkdir(mode=0o755)
                path.chmod(0o755)
            elif row['type'] == 'file':
                archive, member = contents[name]
                with archive.extractfile(member) as source, path.open('xb') as output:
                    for chunk in iter(lambda: source.read(1024 * 1024), b''):
                        output.write(chunk)
                path.chmod(row['mode'])
        for name, row in plan.items():
            if row['type'] == 'symlink':
                (destination / name).symlink_to(row['target'])
        expected = sorted(plan.values(), key=lambda row: row['path'])
        assert inventory(destination) == expected, 'staged bytes differ from declared layers'
        return expected
    finally:
        for archive in archives:
            archive.close()


if __name__ == '__main__':
    if sys.argv[1] == '--verify':
        root, manifest = map(Path, sys.argv[2:])
        assert inventory(root) == json.loads(manifest.read_text()), 'closure changed'
    else:
        evidence, destination, manifest = map(Path, sys.argv[1:])
        rows = stage(evidence, destination)
        manifest.write_text(json.dumps(rows, sort_keys=True, separators=(',', ':')) + '\n')
