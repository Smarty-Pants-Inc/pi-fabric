#!/usr/bin/env python3
"""Check the exact generated source carrier, host ACLs, bounds and hostd spec shape."""
import json
import os
from pathlib import Path
import re
import tempfile

from render import API, CORE, HOSTS, MAX_PAYLOAD, accounts, permissions, render, stream_configs

ROOT = Path(__file__).resolve().parent


def matches(pattern, subject):
    p, s = pattern.split('.'), subject.split('.')
    for i, token in enumerate(p):
        if token == '>':
            return i < len(s)  # NATS > matches one or more trailing tokens.
        if i >= len(s) or (token != '*' and token != s[i]):
            return False
    return len(p) == len(s)


def allowed(acl, direction, subject):
    return any(matches(p, subject) for p in acl[direction]['allow'])


def main():
    checks = {}
    with tempfile.TemporaryDirectory(prefix='u2-check-', dir=os.environ['TMPDIR']) as tmp:
        out = Path(tmp)
        render(out, Path('/home/paul/.local/state/smarty/nats/tls'),
               Path('/home/paul/.local/state/smarty/nats'))
        expected = sorted(p.name for p in out.iterdir())
        assert sorted(p.name for p in (ROOT / 'configs').iterdir()) == expected
        for name in expected:
            assert (out / name).read_bytes() == (ROOT / 'configs' / name).read_bytes(), name
        checks['deterministic_files'] = len(expected)
    for host in HOSTS:
        acl = permissions(host)
        assert all('>' != p for d in acl.values() for p in d['allow'])
        for s in (f'fabric.root.{host}.event', f'$KV.STATE_{host}.key',
                  f'{API}.$KV.STATE_{host}.key', f'{API}.STREAM.MSG.GET.KV_STATE_{host}',
                  f'$JS.API.STREAM.MSG.GET.KV_STATE_{host}',
                  f'{API}.CONSUMER.MSG.NEXT.ROOT_{host}.FABRIC',
                  f'$JS.ACK.ROOT_{host}.FABRIC.1',
                  f'$JS.ACK.fleet.hash.ROOT_{host}.FABRIC.1', f'_INBOX.{host}.1',
                  f'fabric.fleet.work.{host}.event', f'fabric.fleet.presence.{host}.up'):
            assert allowed(acl, 'publish', s), (host, 'expected allow', s)
        for s in (f'fabric.root.{host}.event', f'_INBOX.{host}.1',
                  f'fabric.hub.delivery.{host}.message', 'fabric.fleet.work.other.event'):
            assert allowed(acl, 'subscribe', s), (host, 'expected subscribe', s)
        for other in HOSTS:
            if other == host:
                continue
            for s in (f'fabric.root.{other}.event', f'$KV.STATE_{other}.key',
                      f'{API}.$KV.STATE_{other}.key', f'{API}.STREAM.MSG.GET.KV_STATE_{other}',
                      f'$JS.API.STREAM.MSG.GET.KV_STATE_{other}',
                      f'{API}.STREAM.INFO.ROOT_{other}',
                      f'{API}.CONSUMER.MSG.NEXT.ROOT_{other}.FABRIC', f'_INBOX.{other}.1',
                      f'fabric.fleet.work.{other}.event'):
                assert not allowed(acl, 'publish', s), (host, 'foreign publish', s)
            for s in (f'fabric.root.{other}.event', f'$KV.STATE_{other}.key',
                      f'fabric.hub.delivery.{other}.message', f'_INBOX.{other}.1'):
                assert not allowed(acl, 'subscribe', s), (host, 'foreign subscribe', s)
        for d in ('publish', 'subscribe'):
            for s in ('fabric.hub.control.takeover', '$SYS.REQ.SERVER.PING',
                      f'{API}.STREAM.CREATE.EVIL', f'{API}.STREAM.UPDATE.ROOT_{host}',
                      f'{API}.CONSUMER.CREATE.ROOT_{host}.EVIL',
                      f'{API}.CONSUMER.DURABLE.CREATE.ROOT_{host}.EVIL'):
                assert not allowed(acl, d, s), (host, d, s)
    checks['host_acl_matrix'] = len(HOSTS)
    core_accounts = json.loads(accounts())
    assert set(core_accounts) == {'FABRIC', 'SYS'}
    users = {u['user']: u['permissions'] for u in core_accounts['FABRIC']['users']}
    for h in HOSTS:
        assert users['fabric.' + h] == permissions(h)
        leaf = json.loads(accounts(h))
        assert 'jetstream' not in leaf['FABRIC']
        assert [u['user'] for u in leaf['FABRIC']['users']] == ['fabric.' + h]
        if h not in CORE:
            assert users['leaf.' + h] == permissions(h)
    assert allowed(users['fabric.hub'], 'publish', 'fabric.hub.control.test')
    assert not allowed(users['fabric.ops'], 'publish', 'fabric.hub.control.test')
    assert not allowed(users['fabric.ops'], 'subscribe', 'fabric.root.ryzen3.event')
    assert allowed(users['fabric.ops'], 'publish', f'{API}.STREAM.CREATE.ROOT_ryzen3')
    checks['privileged_accounts'] = 'hub-only traffic; ops API only; SYS separate'
    definitions = stream_configs()
    assert len(definitions) == 2 * len(HOSTS) + 1
    for s in definitions:
        assert s['storage'] == 'file' and s['num_replicas'] == 3
        assert s['max_msg_size'] == MAX_PAYLOAD == 262144
        assert s['max_bytes'] > 0 and s['max_consumers'] > 0
        assert s['discard'] == 'new'
        if s['name'].startswith('KV_'):
            assert s['max_age'] == 0 and s['max_msgs_per_subject'] == 10
        elif s['name'].startswith('ROOT_'):
            assert s['max_age'] > 0 and s['max_msgs'] > 0
    assert 102400 * 4 / 3 + 16384 < MAX_PAYLOAD
    for p in (ROOT / 'configs').glob('*.conf'):
        text = p.read_text()
        assert 'min_version: "1.3"' in text and 'verify_and_map: true' in text
        assert 'listen: "127.0.0.1:4222"' in text
        assert 'http: "127.0.0.1:8222"' in text
        assert 'no_auth_user' not in text and 'password' not in text
        if p.name.startswith('core-'):
            assert 'sync_interval: always' in text and 'store_dir:' in text
            assert 'route-ca.pem' in text and 'system_account: SYS' in text
        else:
            assert 'jetstream {' not in text and 'store_dir:' not in text
            assert text.count('tls://') == 3 and 'cluster {' not in text
    checks['storage_and_tls'] = 'R3/file/always/core-only; no leaf JetStream'
    for role in ('core', 'leaf'):
        spec = ROOT / 'hostd' / f'smarty-nats-{role}.service'
        text = spec.read_text()
        assert not re.search(r'^\[Install\]', text, re.M) and 'Restart=always' in text
        assert '\nRestartSec=5\n' in text and '\nTimeoutStopSec=40\n' in text
        directives = set(re.findall(r'^([A-Za-z]+)=', text, re.M))
        assert directives <= {'Description', 'Type', 'ExecStart', 'Restart', 'RestartSec', 'TimeoutStopSec'}
        child = json.loads((ROOT / 'hostd' / f'nats-{role}-child.json').read_text())
        assert child == {'id': 'nats-' + role, 'unit': f'docs/nats/hostd/smarty-nats-{role}.service'}
    checks['hostd_spec_shape'] = '2 numeric restart/stop-policy child specs; no standalone admission'
    for p in ROOT.rglob('*'):
        if p.is_file():
            assert p.suffix not in ('.pem', '.key', '.csr', '.crt', '.p12', '.pfx'), p
            assert not re.search(rb'-----BEGIN (?:[A-Z]+ )?PRIVATE KEY-----', p.read_bytes()), p
    # Local markdown links must point at real files; remote references are not fetched.
    for p in ROOT.glob('*.md'):
        for link in re.findall(r'\]\(([^)#]+)(?:#[^)]*)?\)', p.read_text()):
            if '://' not in link:
                assert (p.parent / link).exists(), (p.name, link)
    checks['source_safety'] = 'no TLS material; local document links exist'
    print(json.dumps({'status': 'PASS', 'checks': checks}, indent=2))


if __name__ == '__main__':
    main()
