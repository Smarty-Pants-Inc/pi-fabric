"""Light DATA/control tests only: no artifact fetch, extraction or native execution."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

spec = importlib.util.spec_from_file_location('entry', Path(__file__).with_name('fabric-fixed-native.py'))
entry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entry)


class Response(io.BytesIO):
    status = 200


class FixedEntryTests(unittest.TestCase):
    def report(self):
        cases = [{'title': n, 'status': 'passed'} for n in entry.REQUIRED]
        cases += [{'title': f'other source case {i}', 'status': 'passed'} for i in range(20)]
        cases += [{'title': 'rejects an old native CLI that ignores the flag even when global compaction is already false', 'status': 'pending'}]
        return {'success': True, 'numPassedTests': 25, 'numPendingTests': 1,
                'testResults': [{'assertionResults': cases}]}

    def test_exact_five_and_only_expected_skip(self):
        entry.check_report(self.report())

    def test_each_required_case_cannot_skip(self):
        for i in range(5):
            with self.subTest(case=i):
                data = self.report()
                data['testResults'][0]['assertionResults'][i]['status'] = 'pending'
                data['testResults'][0]['assertionResults'][-1]['status'] = 'passed'
                with self.assertRaises(AssertionError):
                    entry.check_report(data)

    def test_duplicate_required_case_rejected(self):
        data = self.report()
        data['testResults'][0]['assertionResults'][5]['title'] = entry.REQUIRED[0]
        with self.assertRaises(AssertionError):
            entry.check_report(data)

    def test_failed_report_rejected(self):
        data = self.report(); data['success'] = False
        with self.assertRaises(AssertionError):
            entry.check_report(data)

    def test_intent_is_exclusive(self):
        with tempfile.TemporaryDirectory() as root:
            p = Path(root) / 'intent.json'
            entry.save(p, {'one': True})
            with self.assertRaises(FileExistsError):
                entry.save(p, {'one': False})
            self.assertEqual(json.loads(p.read_text()), {'one': True})

    def receive(self, root, drift=None, storage_host='release-assets.githubusercontent.com', direct=False, bad_body=False):
        digest = hashlib.sha256(b'abc').hexdigest()
        assets = [('artifact-10718135458.zip', 3, digest), ('node-stage.tar.gz', 3, digest)]
        ids = {name: i for i, (name, _, _) in enumerate(assets, 1)}
        release = {'id': 123, 'tag_name': entry.RELEASE_TAG, 'target_commitish': entry.RELEASE_COMMIT,
                   'draft': False, 'prerelease': True, 'assets': []}
        ref = {'ref': 'refs/tags/' + entry.RELEASE_TAG, 'object': {'type': 'commit', 'sha': entry.RELEASE_COMMIT}}
        for name, size, sha in assets:
            release['assets'].append({'id': ids[name], 'name': name, 'state': 'uploaded', 'size': size,
                'digest': 'sha256:' + sha, 'url': entry.RELEASE_API + '/releases/assets/' + str(ids[name]),
                'browser_download_url': 'https://github.com/Smarty-Pants-Inc/pi-fabric/releases/download/' + entry.RELEASE_TAG + '/' + name})
        if drift == 'release-id': release['id'] = 456
        if drift == 'tag': release['tag_name'] = 'other'
        if drift == 'target': ref['object']['sha'] = 'wrong'
        if drift == 'draft': release['draft'] = True
        if drift == 'asset-id': release['assets'][1]['id'] = 99
        if drift == 'size': release['assets'][1]['size'] = 4
        if drift == 'digest': release['assets'][1]['digest'] = 'sha256:wrong'
        if drift == 'extra': release['assets'].append(dict(release['assets'][1]))
        if drift == 'asset-url': release['assets'][1]['url'] = 'https://attacker.invalid'
        calls = []
        testcase = self
        class Opener:
            def open(self, request, timeout):
                calls.append(request)
                testcase.assertFalse(request.has_header('Authorization'))
                url = request.full_url
                if url.endswith('/releases/123'):
                    return Response(json.dumps(release).encode())
                if '/git/ref/tags/' in url:
                    return Response(json.dumps(ref).encode())
                if '/releases/assets/' in url:
                    testcase.assertTrue((root / 'DESTINATION-PLACEMENT-INTENT.json').exists())
                    if not direct:
                        raise urllib.error.HTTPError(url, 302, 'redirect', {'Location': f'https://{storage_host}/signed'}, None)
                return Response(b'abd' if bad_body else b'abc')
        with patch.object(entry, 'ASSETS', assets), patch.object(entry, 'RELEASE_ID', 123), \
             patch.object(entry, 'RELEASE_ASSET_IDS', ids), patch.object(entry.signal, 'alarm'), \
             patch.object(entry.urllib.request, 'build_opener', return_value=Opener()), \
             patch.dict(os.environ, {'GH_TOKEN': 'unused-fake-token', 'GITHUB_RUN_ID': '1'}):
            entry.receive(root)
        return calls

    def test_once_anonymous_fetch_and_credential_free_redirect(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            self.assertEqual(len(self.receive(root)), 6)
            self.assertEqual((root / 'artifact-10718135458.zip').read_bytes(), b'abc')
            self.assertEqual((root / 'node-stage.tar.gz').read_bytes(), b'abc')
            with self.assertRaises(FileExistsError):
                self.receive(root)

    def test_direct_api_body_is_supported_without_second_get(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(len(self.receive(Path(root), direct=True)), 4)

    def test_all_identity_drift_stops_before_body_intent(self):
        for drift in ('release-id', 'tag', 'target', 'draft', 'asset-id', 'size', 'digest', 'extra', 'asset-url'):
            with self.subTest(drift=drift), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                with self.assertRaises(AssertionError): self.receive(root, drift=drift)
                self.assertEqual(list(root.iterdir()), [])

    def test_storage_allowlist_stops_without_body(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with self.assertRaises(AssertionError): self.receive(root, storage_host='attacker.invalid')
            self.assertTrue((root / 'DESTINATION-PLACEMENT-INTENT.json').exists())
            self.assertFalse(list(root.glob('*.zip')))

    def test_wire_cap_stops_without_success_receipt(self):
        with tempfile.TemporaryDirectory() as root, patch.object(entry, 'CAP', 2):
            root = Path(root)
            with self.assertRaises(AssertionError): self.receive(root)
            self.assertFalse(list(root.glob('BODY-*.json')))

    def test_body_digest_stops_without_success_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with self.assertRaises(AssertionError): self.receive(root, bad_body=True)
            self.assertFalse(list(root.glob('BODY-*.json')))
            self.assertFalse((root / 'node-stage.tar.gz').exists())

    def test_unbound_identity_has_no_network_or_intent(self):
        with tempfile.TemporaryDirectory() as root, patch.object(entry, 'RELEASE_ID', None), \
             patch.object(entry.urllib.request, 'build_opener') as opener:
            with self.assertRaises(AssertionError): entry.receive(Path(root))
            opener.assert_not_called()
            self.assertEqual(list(Path(root).iterdir()), [])


if __name__ == '__main__':
    unittest.main()
