"""Fixed issue161 operation. No command/ref input, install, rebuild or live state.
Destination fetch is a NEW CI-admitted operation, not reuse of Dev1's spent intent.
Reuse the received Fabric verifier/stager and Pi DATA-stage checks.
"""
import hashlib
import ctypes
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import resource
import signal
import stat
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

HERE = Path(__file__).resolve().parent
CAP = 128 * 1024**2
FABRIC_MANIFEST = '3876be84809fe45f50b14de1b5466871f8fe2a830aaa1eb264c8a00efe5bc12d'
PI_MANIFEST = '7610f9e84405e672158ff874f317ccad1435c211d875e1a98a6b6325611ede4d'
PI_ARCHIVE = '4702a03ec015a0134d2a5b57e983be3a9d4fbb8a8adfba9609ca6c8268803a03'
CLI = 'node/node_modules/@earendil-works/pi-coding-agent/dist/cli.js'
RELEASE_API = 'https://api.github.com/repos/Smarty-Pants-Inc/pi-fabric'
RELEASE_TAG = 'qualification-inputs-10718135458-10682730383'
RELEASE_COMMIT = '999936a251b97a6c057cf28f2665ed471941e85b'
# Bound to the sole publisher's authenticated published/tag/asset readback.
# Repository immutability is not required; every selected identity must match.
RELEASE_ID = 394152047
RELEASE_ASSET_IDS = {'artifact-10718135458.zip': 582338616, 'node-stage.tar.gz': 582338748}
ASSETS = (
    ('artifact-10718135458.zip', 119276501, '2501a67279b6675d68bf983e9952e7980e04a9f5109171a0b46cf17c6883ebb0'),
    ('node-stage.tar.gz', 38603868, PI_ARCHIVE),
)
TOOLS = {'node': '41a74efb34cbde5c7632cdac0cf8bd1a14d0b8d73dc1e82755014d9a9ce70f5c',
         'bun': '33d56b070be6a9e3da0ab013038b43d1645d0534ca811ecdba4472599117eb4b'}
REQUIRED = [
    'keeps the omitted full-history default on the actual native request path',
    'retains LARGE old threshold journal yet runs useful current inference, extensions:false/tools:[]',
    'retains LARGE old overflow journal yet runs useful current inference, extensions:false/tools:[]',
    'blocks native manual compaction before any summary request and retains the full journal',
    'two real activations retain full journals and current tool pairs, without Fabric tool enablement',
]


def sha(p):
    with p.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def save(p, value):
    with p.open('x') as f:
        json.dump(value, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    fd = os.open(p.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def deadline(*args):
    raise TimeoutError('fixed DATA deadline')


def receive(d):
    assert type(RELEASE_ID) is int and RELEASE_ID > 0, 'Release identity unbound'
    assert set(RELEASE_ASSET_IDS) == {row[0] for row in ASSETS}
    assert all(type(aid) is int and aid > 0 for aid in RELEASE_ASSET_IDS.values()), 'Asset identities unbound'
    assert len(set(RELEASE_ASSET_IDS.values())) == len(ASSETS)
    # Anonymous native GitHub transport only: no token, netrc or ambient proxy.
    opener = urllib.request.build_opener(NoRedirect, urllib.request.ProxyHandler({}))
    headers = {'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
    def metadata(path):
        with opener.open(urllib.request.Request(RELEASE_API + path, headers=headers), timeout=10) as response:
            raw = response.read(1024**2 + 1)
        assert len(raw) <= 1024**2
        return json.loads(raw)
    release = metadata(f'/releases/{RELEASE_ID}')
    ref = metadata('/git/ref/tags/' + RELEASE_TAG)
    assert release['id'] == RELEASE_ID and release['tag_name'] == RELEASE_TAG
    assert release['target_commitish'] == RELEASE_COMMIT and not release['draft'] and release['prerelease']
    assert ref['ref'] == 'refs/tags/' + RELEASE_TAG
    assert ref['object']['type'] == 'commit' and ref['object']['sha'] == RELEASE_COMMIT
    declared = {asset['name']: asset for asset in release['assets']}
    assert len(declared) == len(release['assets']) == len(ASSETS) and set(declared) == set(RELEASE_ASSET_IDS)
    for name, size, digest in ASSETS:
        asset = declared[name]
        assert asset['id'] == RELEASE_ASSET_IDS[name] and asset['state'] == 'uploaded'
        assert asset['size'] == size and asset['digest'] == 'sha256:' + digest
        assert asset['url'] == RELEASE_API + '/releases/assets/' + str(asset['id'])
        assert asset['browser_download_url'] == 'https://github.com/Smarty-Pants-Inc/pi-fabric/releases/download/' + RELEASE_TAG + '/' + name
    save(d / 'DESTINATION-PLACEMENT-INTENT.json', {
        'operation': 'NEW_DESTINATION_RELEASE_PLACEMENT_NOT_DEV1_RETRY', 'run': os.environ['GITHUB_RUN_ID'],
        'attempt': 1, 'releaseId': RELEASE_ID, 'tag': RELEASE_TAG, 'target': RELEASE_COMMIT,
        'assets': list(declared.values()), 'bodyGetsPerAsset': 1, 'retry': False,
        'sourceArtifactIds': [10718135458, 10682730383], 'piOperand': 'accepted inner node-stage.tar.gz, not outer diagnostic ZIP',
        'destination': str(d), 'closureEntryCap': 200000, 'expandedLayerBytesCap': 2 * 1024**3,
        'perBodyWallSeconds': 45, 'perBodyWireCap': CAP,
    })
    for name, size, digest in ASSETS:
        signal.alarm(45)
        started = time.monotonic()
        aid = RELEASE_ASSET_IDS[name]
        request = urllib.request.Request(RELEASE_API + f'/releases/assets/{aid}', headers={'Accept': 'application/octet-stream'})
        try:
            response = opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            assert error.code == 302
            location = error.headers['Location']
            target = urllib.parse.urlsplit(location)
            assert target.scheme == 'https' and target.port in (None, 443)
            assert not target.username and not target.password
            assert target.hostname in ('release-assets.githubusercontent.com', 'objects.githubusercontent.com')
            # At most this one redirect; fresh credential-free storage request.
            response = opener.open(urllib.request.Request(location), timeout=10)
        count = 0
        body = d / name
        with response, body.open('xb') as output:
            assert response.status == 200
            while chunk := response.read(1024**2):
                count += len(chunk)
                assert count <= min(CAP, size)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        assert count == size and sha(body) == digest
        signal.alarm(0)
        save(d / f'BODY-{aid}.json', {'asset': name, 'bytes': count, 'sha256': digest,
                                    'elapsedSeconds': time.monotonic() - started})


def stage_pi(d):
    archive = d / 'node-stage.tar.gz'
    assert archive.stat().st_size == 38603868 and sha(archive) == PI_ARCHIVE
    target = d / 'pi'
    with tarfile.open(archive, 'r:gz') as t:
        members = t.getmembers()
        assert len(members) == 15446
        assert all(m.offset_data + m.size <= 2 * 1024**3 for m in members)
        assert sum(m.size for m in members if m.isfile()) == 128085760
        seen = set()
        for m in members:
            name = str(PurePosixPath(m.name))
            assert not PurePosixPath(name).is_absolute() and '..' not in PurePosixPath(name).parts
            assert '\\' not in name and name not in seen and not m.mode & 0o7000
            assert m.isdir() or m.isfile() or m.issym()
            seen.add(name)
            tarfile.data_filter(m, str(target))
        target.mkdir(mode=0o700)
        t.extractall(target, filter='data')
        # data_filter strips directory group-write bits. Restore only verified
        # archive directory modes, before the complete original-mode check.
        for m in members:
            if m.isdir():
                q = target / str(PurePosixPath(m.name))
                assert not q.is_symlink() and q.is_dir()
                q.chmod(m.mode)
        manifest = []
        for m in members:
            name = str(PurePosixPath(m.name)); q = target / name; info = q.lstat()
            assert stat.S_IMODE(info.st_mode) == m.mode
            row = {'path': name, 'mode': oct(m.mode),
                   'type': 'directory' if m.isdir() else 'symlink' if m.issym() else 'file'}
            if m.isfile():
                assert stat.S_ISREG(info.st_mode) and info.st_size == m.size
                with t.extractfile(m) as f:
                    expected = hashlib.file_digest(f, 'sha256').hexdigest()
                assert sha(q) == expected
                row.update(bytes=m.size, sha256=expected)
            elif m.issym():
                assert q.is_symlink() and os.readlink(q) == m.linkname
                assert q.resolve(strict=True).is_relative_to(target.resolve())
                row['target'] = m.linkname
            else:
                assert q.is_dir() and not q.is_symlink()
            manifest.append(row)
        assert {str(q.relative_to(target)) for q in target.rglob('*')} == seen - {'.'}
    # Reproduce Light's accepted full-manifest serialization exactly.
    manifest_file = d / 'PI-MANIFEST.json'
    manifest_file.write_text(json.dumps(manifest, indent=2) + '\n')
    assert sha(manifest_file) == PI_MANIFEST
    assert sha(target / CLI) == '8189b66abc4f9f431dbb70941dcba690d76d040de1fbfff212886be35a53639d'


def stage(d):
    subprocess.run([sys.executable, '-I', '-B', str(HERE / 'fabric-fixed-artifact-verify.py'), str(d)],
                   check=True, timeout=120)
    inputs = d / 'inputs'
    inputs.mkdir(mode=0o700)
    with zipfile.ZipFile(d / 'artifact-10718135458.zip') as z:
        # The unchanged receiver has already verified the exact outer allowlist,
        # all checksums, bounded layers and complete confined closure manifest.
        for item in z.infolist():
            with z.open(item) as source, (inputs / item.filename).open('xb') as out:
                shutil.copyfileobj(source, out, 1024**2)
    assert sha(inputs / 'closure-manifest.json') == FABRIC_MANIFEST
    helper = HERE / 'fabric-stage-closure.py'
    assert sha(helper) == '980bbe767e5b8ce333b98da774fc49f02aea5ec9b3e4e71a03158fd11ba2f043'
    subprocess.run([sys.executable, '-I', '-B', str(helper), str(inputs), str(d / 'fabric'),
                    str(d / 'FABRIC-MANIFEST.json')], check=True, timeout=120)
    assert sha(d / 'FABRIC-MANIFEST.json') == FABRIC_MANIFEST
    stage_pi(d)
    save(d / 'PLACEMENT-VERIFIED.json', {'fabricManifest': FABRIC_MANIFEST,
         'piManifest': PI_MANIFEST, 'runtimeExecuted': False})


def verify_after(d):
    subprocess.run([sys.executable, '-I', '-B', str(HERE / 'fabric-stage-closure.py'), '--verify',
                    str(d / 'fabric'), str(d / 'FABRIC-MANIFEST.json')], check=True, timeout=20)
    rows = json.loads((d / 'PI-MANIFEST.json').read_text())
    root = d / 'pi'
    for row in rows:
        p = root / row['path']; info = p.lstat()
        assert stat.S_IMODE(info.st_mode) == int(row['mode'], 8)
        if row['type'] == 'file':
            assert stat.S_ISREG(info.st_mode) and info.st_size == row['bytes'] and sha(p) == row['sha256']
        elif row['type'] == 'symlink':
            assert p.is_symlink() and os.readlink(p) == row['target']
            assert p.resolve(strict=True).is_relative_to(root.resolve())
        else:
            assert p.is_dir() and not p.is_symlink()
    assert {str(p.relative_to(root)) for p in root.rglob('*')} == {r['path'] for r in rows} - {'.'}


def check_report(data):
    assert data['success'] and data['numPassedTests'] == 25 and data['numPendingTests'] == 1
    cases = [case for suite in data['testResults'] for case in suite['assertionResults']]
    assert len(cases) == 26 and sum(c['status'] == 'passed' for c in cases) == 25
    skipped = [c for c in cases if c['status'] != 'passed']
    # Vitest 4 reports a skipped assertion as "skipped" (numPendingTests is 1).
    assert len(skipped) == 1 and skipped[0]['status'] == 'skipped'
    assert skipped[0]['title'] == 'rejects an old native CLI that ignores the flag even when global compaction is already false'
    for name in REQUIRED:
        matches = [c for c in cases if c['title'] == name]
        assert len(matches) == 1 and matches[0]['status'] == 'passed'


def child_limits():
    # Bound each output/journal/cache file, including stdout/stderr, without a
    # monitor or extra runtime. CI still owns total disk/memory/CPU admission.
    resource.setrlimit(resource.RLIMIT_FSIZE, (8 * 1024**2, 8 * 1024**2))


def qualify(d):
    assert json.loads((d / 'PLACEMENT-VERIFIED.json').read_text())['fabricManifest'] == FABRIC_MANIFEST
    tools = {}
    for name, digest in TOOLS.items():
        found = shutil.which(name)
        assert found, 'Missing already-installed fixed tool: ' + name
        binary = Path(found).resolve()
        assert sha(binary) == digest, 'Destination tool differs: ' + name
        tools[name] = str(binary)
    state = d / 'state'
    state.mkdir(mode=0o700)
    for name in ('home', 'tmp', 'agent', 'output'):
        (state / name).mkdir(mode=0o700)
    env = {'PATH': ':'.join([str(Path(tools['node']).parent), str(Path(tools['bun']).parent), '/usr/bin', '/bin']),
           'HOME': str(state / 'home'), 'TMPDIR': str(state / 'tmp'), 'TMP': str(state / 'tmp'),
           'TEMP': str(state / 'tmp'), 'PI_CODING_AGENT_DIR': str(state / 'agent'),
           'CI': '1', 'PI_OFFLINE': '1',
           'PI_FABRIC_ACTIVATION_TEST_PI_BINARY': str(d / 'pi' / CLI),
           'PI_FABRIC_ACTIVATION_TEST_WORKER': str(d / 'fabric/dist/worker.js')}
    if os.environ.get('RUNNER_TRACKING_ID'):
        env['RUNNER_TRACKING_ID'] = os.environ['RUNNER_TRACKING_ID']
    # Version execution uses only byte-pinned tools and the scrubbed fixture env.
    versions = {name: subprocess.check_output([binary, '--version'], env=env, timeout=5, text=True).strip()
                for name, binary in tools.items()}
    assert versions == {'node': 'v24.18.0', 'bun': '1.4.0'}
    report = state / 'output/vitest.json'
    argv = [tools['bun'], 'run', 'node_modules/.bin/vitest', 'run', 'tests/worker-activation-window.test.ts',
            '--cache=false', '--reporter=verbose', '--reporter=json', '--outputFile=' + str(report)]
    save(d / 'NATIVE-INTENT.json', {'argv': argv, 'cwd': str(d / 'fabric'), 'environment': env,
         'fabricArtifact': 10718135458, 'piArtifact': 10682730383,
         'fabricManifest': FABRIC_MANIFEST, 'piManifest': PI_MANIFEST,
         'runtime': 'f7d71b57bfc9ec7ec76fc2e02f13642ec87033e3',
         'operands': {name: sha(d / 'fabric' / name) for name in ('tests/worker-activation-window.test.ts', 'dist/worker.js', 'dist/worker/activation-window.js')},
         'piCliSha256': sha(d / 'pi' / CLI),
         'toolPaths': tools, 'toolSha256': TOOLS, 'toolVersions': versions,
         'destination': {key: os.environ.get(key) for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB', 'GITHUB_SHA', 'RUNNER_NAME', 'RUNNER_OS', 'RUNNER_ARCH', 'ImageOS', 'ImageVersion')},
         'platform': list(os.uname()), 'wallSeconds': 180, 'killGraceSeconds': 5, 'retry': False})
    # ProcessTransport intentionally detaches workers. A Linux subreaper lets
    # this one operation receive their orphaned children; group exit alone is
    # not sufficient. GitHub's existing tracking tag remains for job cleanup.
    assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
    started = time.monotonic()
    with (d / 'native.stdout').open('xb') as out, (d / 'native.stderr').open('xb') as err:
        child = subprocess.Popen(argv, cwd=d / 'fabric', env=env, stdin=subprocess.DEVNULL,
                                 stdout=out, stderr=err, start_new_session=True, preexec_fn=child_limits)
        try:
            code = child.wait(timeout=180)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            time.sleep(5)
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
            save(d / 'NATIVE-RESULT.json', {'status': 'TIMEOUT_NO_RETRY', 'pid': child.pid, 'exit': child.returncode})
            raise TimeoutError('Native qualification deadline')
    # Exited leader alone does not prove child cleanup. Residual group members
    # force failure, even if all assertions passed; terminate only our own group.
    try:
        os.killpg(child.pid, 0)
    except ProcessLookupError:
        group_gone = True
    else:
        group_gone = False
        try:
            os.killpg(child.pid, signal.SIGTERM)
            time.sleep(5)
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    # Reap exited adopted workers, and refuse success for any surviving child.
    while True:
        try:
            if os.waitpid(-1, os.WNOHANG)[0] == 0:
                break
        except ChildProcessError:
            break
    children_file = Path(f'/proc/self/task/{os.getpid()}/children')
    adopted = [int(pid) for pid in children_file.read_text().split()]
    for pid in adopted:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    result = {'status': 'FAILED_OR_UNVERIFIED', 'pid': child.pid, 'exit': code,
              'originalProcessGroupGone': group_gone, 'residualAdoptedChildren': adopted,
              'elapsedSeconds': time.monotonic() - started}
    try:
        assert code == 0 and group_gone and not adopted
        assert not list((state / 'tmp').iterdir()), 'Fixture temp cleanup incomplete'
        verify_after(d)
        result['bothFullClosuresUnchangedAfter'] = True
        data = json.loads(report.read_text())
        check_report(data)
        result.update(status='NATIVE_FOUR_AND_DEFAULT_PASSED', requiredCases=REQUIRED,
                      passed=25, intentionalOldCliSkip=1)
    finally:
        save(d / 'NATIVE-RESULT.json', result)


def main():
    assert sys.platform == 'linux' and os.uname().machine == 'x86_64'
    assert os.environ['GITHUB_REPOSITORY'] == 'Smarty-Pants-Inc/pi-fabric'
    assert os.environ['GITHUB_REF'] == 'refs/heads/main' and os.environ['GITHUB_RUN_ATTEMPT'] == '1'
    os.umask(0o077)
    d = Path(os.environ['RUNNER_TEMP']) / 'fabric10718135458-native'
    action = sys.argv[1]
    assert action in ('receive', 'stage', 'qualify')
    if action == 'receive':
        d.mkdir(mode=0o700)
        assert shutil.disk_usage(d).free >= 8 * 1024**3
    signal.signal(signal.SIGALRM, deadline)
    {'receive': receive, 'stage': stage, 'qualify': qualify}[action](d)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Do not print token-bearing signed URLs in urllib exception messages.
        print('Fixed qualification STOP: ' + type(error).__name__, file=sys.stderr)
        sys.exit(1)
