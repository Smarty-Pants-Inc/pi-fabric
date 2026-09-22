"""Light DATA/control tests only: no artifact fetch, extraction or native execution."""
import copy
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

    def fake_transport(self, root, drift=False, storage_host='example.blob.core.windows.net'):
        body = b'abc'
        digest = hashlib.sha256(body).hexdigest()
        artifacts = [('pi-fabric', 1, 11, 'a', 3, digest), ('pi', 2, 22, 'b', 3, digest)]
        calls = []
        class Opener:
            def open(self, request, timeout):
                calls.append(request)
                url = request.full_url
                if url.endswith('/zip'):
                    selftest.assertTrue((root / 'DESTINATION-PLACEMENT-INTENT.json').exists())
                    raise urllib.error.HTTPError(url, 302, 'redirect', {'Location': f'https://{storage_host}/signed'}, None)
                if '/actions/artifacts/' in url:
                    index = 0 if url.endswith('/1') else 1
                    repo, aid, run, commit, size, sha = artifacts[index]
                    value = {'id': aid, 'expired': False, 'size_in_bytes': size, 'digest': 'sha256:' + sha,
                             'workflow_run': {'id': run, 'head_sha': 'wrong' if drift and index else commit}}
                    return Response(json.dumps(value).encode())
                selftest.assertFalse(request.has_header('Authorization'))
                return Response(body)
        selftest = self
        return artifacts, Opener(), calls

    def receive(self, root, **kwargs):
        artifacts, opener, calls = self.fake_transport(root, **kwargs)
        with patch.object(entry, 'ARTIFACTS', artifacts), patch.object(entry.signal, 'alarm'), \
             patch.object(entry.urllib.request, 'build_opener', return_value=opener), \
             patch.dict(os.environ, {'GH_TOKEN': 'fake-not-a-credential', 'GITHUB_RUN_ID': '1'}):
            entry.receive(root)
        return calls

    def test_once_fetch_and_credential_isolated_storage(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            calls = self.receive(root)
            self.assertEqual(len(calls), 6)  # two metadata, two API redirects, two bodies
            self.assertTrue(all(calls[i].has_header('Authorization') for i in (0, 1, 2, 4)))
            self.assertEqual((root / 'artifact-1.zip').read_bytes(), b'abc')
            self.assertEqual((root / 'artifact-2.zip').read_bytes(), b'abc')
            with self.assertRaises(FileExistsError):
                self.receive(root)

    def test_sibling_drift_stops_before_any_body_intent(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with self.assertRaises(AssertionError):
                self.receive(root, drift=True)
            self.assertEqual(list(root.iterdir()), [])

    def test_storage_allowlist_stops_without_body(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with self.assertRaises(AssertionError):
                self.receive(root, storage_host='attacker.invalid')
            self.assertTrue((root / 'DESTINATION-PLACEMENT-INTENT.json').exists())
            self.assertFalse(list(root.glob('artifact-*.zip')))

    def test_wire_cap_stops_without_success_receipt(self):
        with tempfile.TemporaryDirectory() as root, patch.object(entry, 'CAP', 2):
            root = Path(root)
            with self.assertRaises(AssertionError):
                self.receive(root)
            self.assertFalse(list(root.glob('BODY-*.json')))

    def test_missing_token_has_no_network_or_intent(self):
        with tempfile.TemporaryDirectory() as root, patch.dict(os.environ, {'GH_TOKEN': ''}), \
             patch.object(entry.urllib.request, 'build_opener') as opener:
            with self.assertRaises(AssertionError):
                entry.receive(Path(root))
            opener.assert_not_called()
            self.assertEqual(list(Path(root).iterdir()), [])


if __name__ == '__main__':
    unittest.main()
