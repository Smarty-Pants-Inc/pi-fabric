#!/usr/bin/env python3
"""Foreground local R3+2-leaf smoke; synthetic data and ephemeral test-only TLS.
Uses Python stdlib NATS protocol, OpenSSL and an already SHA256SUMS-verified server.
No fleet, credentials-store, GitHub API, install or background-service effects.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

from render import API, CORE, HOSTS, MAX_PAYLOAD, render


class SmokeCancelled(BaseException):
    """Cancellation must escape read-only readiness retry loops."""


def inbox_name(host):
    # Delayed replies from a discarded read-only connection must not reach a
    # fresh connection reusing sid=1. Keep the random token under the host ACL.
    return f'{host}.{uuid.uuid4().hex}'


class Nats:
    def __init__(self, port, tlsdir, identity, inbox, timeout=10):
        self.timeout = timeout
        self.inbox = inbox_name(inbox)
        self.sid = 0
        sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
        # NATS sends plaintext INFO before the TLS handshake (not a STARTTLS command).
        initial = b''
        while not initial.endswith(b'\r\n'):
            byte = sock.recv(1)
            if not byte:
                sock.close()
                raise RuntimeError('connection closed before INFO')
            initial += byte
            if len(initial) > 16384:
                raise RuntimeError('invalid initial INFO')
        assert initial.startswith(b'INFO '), initial
        ctx = ssl.create_default_context(cafile=str(tlsdir / 'transport-ca.pem'))
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.load_cert_chain(str(tlsdir / (identity + '.pem')), str(tlsdir / (identity + '.key')))
        self.sock = ctx.wrap_socket(sock, server_hostname='127.0.0.1')
        self.sock.settimeout(timeout)
        self.file = self.sock.makefile('rb')
        self.sock.sendall(b'CONNECT ' + json.dumps({'verbose': False, 'pedantic': True,
            'tls_required': True, 'name': 'u2-local-smoke', 'headers': True,
            'no_responders': True}).encode() + b'\r\nPING\r\n')
        self.until_pong()

    def read_line(self, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('NATS operation deadline exceeded')
        self.sock.settimeout(remaining)
        return self.file.readline()

    def until_pong(self):
        deadline = time.monotonic() + self.timeout
        while True:
            line = self.read_line(deadline)
            if line == b'PONG\r\n':
                return
            self.control(line)

    def control(self, line):
        if line == b'PING\r\n':
            self.sock.sendall(b'PONG\r\n')
        elif line.startswith(b'-ERR'):
            raise RuntimeError(line.decode().strip())
        elif not line:
            raise RuntimeError('NATS connection closed')
        elif not (line.startswith(b'INFO') or line.startswith(b'+OK')):
            raise RuntimeError('unexpected protocol line ' + repr(line))

    def message(self):
        # Keepalive traffic must not extend the total RPC deadline indefinitely.
        deadline = time.monotonic() + self.timeout
        while True:
            line = self.read_line(deadline)
            if line.startswith((b'MSG ', b'HMSG ')):
                parts = line.split()
                reply = ''
                if parts[0] == b'MSG':
                    if len(parts) == 5:
                        reply = parts[3].decode()
                    length = int(parts[-1])
                    body = self.file.read(length)
                else:
                    if len(parts) == 6:
                        reply = parts[3].decode()
                    headerlen, length = map(int, parts[-2:])
                    blob = self.file.read(length)
                    header, body = blob[:headerlen], blob[headerlen:]
                    if b'NATS/1.0 503' in header:
                        raise RuntimeError('no responders')
                assert self.file.read(2) == b'\r\n'
                return body, reply
            self.control(line)

    def publish(self, subject, body=b'', reply=''):
        prefix = f'PUB {subject} ' + (reply + ' ' if reply else '') + str(len(body))
        self.sock.sendall(prefix.encode() + b'\r\n' + body + b'\r\n')

    def subscribe_once(self, subject):
        self.sid += 1
        self.sock.sendall(f'SUB {subject} {self.sid}\r\nUNSUB {self.sid} 1\r\nPING\r\n'.encode())
        self.until_pong()

    def request(self, subject, body=b''):
        self.sid += 1
        reply = f'_INBOX.{self.inbox}.{self.sid}'
        self.sock.sendall(f'SUB {reply} {self.sid}\r\nUNSUB {self.sid} 1\r\n'.encode())
        self.publish(subject, body, reply)
        try:
            return self.message()
        except Exception as exc:
            raise RuntimeError(f'{subject}: {exc}') from exc

    def api(self, subject, body):
        result = json.loads(self.request(subject, json.dumps(body).encode())[0])
        if 'error' in result:
            raise RuntimeError(json.dumps(result['error']))
        return result

    def denied_publish(self, subject):
        try:
            self.publish(subject, b'{}')
            self.sock.sendall(b'PING\r\n')
            self.until_pong()
        except RuntimeError as exc:
            assert 'Permissions Violation' in str(exc), str(exc)
            return str(exc)
        raise AssertionError('unexpectedly allowed publish: ' + subject)

    def denied_subscribe(self, subject):
        try:
            self.sock.sendall(f'SUB {subject} 999\r\nPING\r\n'.encode())
            self.until_pong()
        except RuntimeError as exc:
            assert 'Permissions Violation' in str(exc), str(exc)
            return str(exc)
        raise AssertionError('unexpectedly allowed subscribe: ' + subject)

    def close(self):
        self.file.close()
        self.sock.close()


def health(port, endpoint='/healthz'):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{endpoint}', timeout=2) as response:
        return json.load(response)


def eventually(label, check, seconds=35):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (Exception,) as exc:
            last = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f'{label} did not converge: {last}')


def make_tls(tlsdir, logs):
    tlsdir.mkdir(mode=0o700)
    log = (logs / 'tls-fixture.log').open('w')

    def run(args):
        subprocess.run(['openssl'] + args, check=True, stdout=log, stderr=log)

    try:
        for ca in ('transport', 'route'):
            run(['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                 '-keyout', str(tlsdir / (ca + '-ca.key')), '-out', str(tlsdir / (ca + '-ca.pem')),
                 '-subj', '/CN=U2 disposable smoke ' + ca + ' CA',
                 '-addext', 'basicConstraints=critical,CA:TRUE',
                 '-addext', 'keyUsage=critical,keyCertSign,cRLSign'])
        identities = [('server-' + h, 'serverAuth', 'transport') for h in HOSTS]
        identities += [('route-' + h, 'serverAuth,clientAuth', 'route') for h in CORE]
        identities += [('fabric.' + h, 'clientAuth', 'transport') for h in HOSTS]
        identities += [('leaf-' + h, 'clientAuth', 'transport') for h in HOSTS if h not in CORE]
        identities += [(name, 'clientAuth', 'transport') for name in ('fabric.ops', 'fabric.sys', 'fabric.hub')]
        for name, usage, ca in identities:
            mapped = 'leaf.' + name[5:] if name.startswith('leaf-') else name
            run(['req', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(tlsdir / (name + '.key')),
                 '-out', str(tlsdir / (name + '.csr')), '-subj', '/CN=' + mapped])
            ext = tlsdir / (name + '.ext')
            ext.write_text('basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n'
                           'extendedKeyUsage=' + usage + '\nsubjectAltName=DNS:' + mapped + ',IP:127.0.0.1\n')
            run(['x509', '-req', '-in', str(tlsdir / (name + '.csr')), '-CA',
                 str(tlsdir / (ca + '-ca.pem')), '-CAkey', str(tlsdir / (ca + '-ca.key')),
                 '-CAcreateserial', '-out', str(tlsdir / (name + '.pem')), '-days', '1',
                 '-extfile', str(ext)])
        for file in tlsdir.glob('*.key'):
            file.chmod(0o600)
    finally:
        log.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--base-port', type=int, default=22000)
    a = p.parse_args()
    if not 1024 <= a.base_port <= 65224:
        p.error('--base-port must leave room for +311 within TCP port range')
    a.server = a.server.resolve(strict=True)
    version = subprocess.check_output([str(a.server), '-v'], text=True).strip()
    a.out.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix='u2-nats-', dir=os.environ['TMPDIR']))
    # No fleet listeners. Fail before launching anything if any offset port is in use.
    processes, handles, clients = {}, [], []
    result = {'hostname': socket.gethostname(), 'server_version': version,
              'checks': {}, 'status': 'FAIL'}
    started = time.monotonic()
    def note(name, evidence):
        result['checks'][name] = evidence
        print(name + ': ' + json.dumps(evidence), flush=True)
    def connect(host, leaf=True, identity=None, timeout=10):
        port = manifest['leaf_ports' if leaf else 'core_ports'][host]
        privileged_inbox = {'fabric.hub': 'hub', 'fabric.ops': 'ops', 'fabric.sys': 'sys'}
        c = Nats(port, tlsdir, identity or 'fabric.' + host,
                 privileged_inbox.get(identity, host), timeout=timeout)
        clients.append(c)
        return c
    def observe(host, subject, body, leaf=True, identity=None):
        # Retried readiness probes are read-only and use a fresh connection:
        # a timed-out socket.makefile must not be reused. Writes are never retried.
        c = connect(host, leaf=leaf, identity=identity, timeout=2)
        try:
            return c.api(subject, body)
        finally:
            c.close()
    try:
        parts = version.removeprefix('nats-server: v').split('.')
        assert len(parts) == 3 and all(s.isdigit() for s in parts), version
        assert tuple(map(int, parts)) >= (2, 14, 7), version
        # Ensure ordinary supervisor cancellation still reaps every owned server.
        def interrupted(signum, frame):
            raise SmokeCancelled(f'smoke cancelled by signal {signum}')
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        tlsdir = scratch / 'tls'
        make_tls(tlsdir, a.out)
        configdir = scratch / 'configs'
        render(configdir, tlsdir, scratch / 'store', True, a.base_port)
        manifest = json.loads((configdir / 'manifest.json').read_text())
        shutil.copytree(configdir, a.out / 'rendered-configs', dirs_exist_ok=True)
        ports = list(manifest['core_ports'].values()) + list(manifest['leaf_ports'].values())
        ports += list(manifest['monitor_ports'].values())
        ports += [a.base_port + 100 + i for i in range(3)] + [a.base_port + 200 + i for i in range(3)]
        for port in ports:
            with socket.socket() as probe:
                # Match the server's SO_REUSEADDR; a previous own run's TIME_WAIT
                # is not a live listener. No SO_REUSEPORT or process termination.
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
        productiondir = scratch / 'production-configs'
        render(productiondir, tlsdir, scratch / 'production-store')
        with (a.out / 'config-check.log').open('w') as checklog:
            for config in sorted(configdir.glob('*.conf')) + sorted(productiondir.glob('*.conf')):
                proc = subprocess.run([str(a.server), '-t', '-c', str(config)],
                                      stdout=checklog, stderr=checklog)
                assert proc.returncode == 0, f'config rejected: {config.name}'
        note('config_parse', {'smoke_configs': 5, 'production_configs': 9,
                             'sync_interval': 'always', 'tls': 'mandatory TLS1.3 mTLS'})
        for name in ['core-' + h for h in CORE] + ['leaf-ryzen3', 'leaf-ryzen5']:
            log = (a.out / (name + '.log')).open('w')
            handles.append(log)
            processes[name] = subprocess.Popen([str(a.server), '-D', '-c', str(configdir / (name + '.conf'))],
                                                stdout=log, stderr=log)
        eventually('all server health', lambda: all(
            health(port).get('status') == 'ok' for port in manifest['monitor_ports'].values()))
        def meta_leader():
            leaders = [health(manifest['monitor_ports']['core-' + h], '/jsz')['meta_cluster'].get('leader')
                       for h in CORE if processes['core-' + h].poll() is None]
            return leaders[0] if leaders and leaders[0] and len(set(leaders)) == 1 else None
        first_leader = eventually('initial meta leader', meta_leader)
        eventually('both leaves linked', lambda: all(health(
            manifest['monitor_ports']['leaf-' + h], '/leafz').get('leafnodes', 0) == 1
            for h in ('ryzen3', 'ryzen5')))
        note('startup', {'meta_leader': first_leader, 'servers': 5, 'leaf_links': 2})
        def metadata_info(leader, alive=3):
            info = health(manifest['monitor_ports'][leader], '/jsz')['meta_cluster']
            live_names = {name for name, proc in processes.items() if proc.poll() is None}
            assert info['leader'] == leader and leader in live_names, info
            assert info['cluster_size'] == 3 and len(info.get('replicas', [])) == 2, info
            assert len([r for r in info['replicas'] if r.get('current')
                        and r['name'] in live_names]) == alive - 1, info
            return info
        note('metadata_before', eventually('metadata followers current',
                                           lambda: metadata_info(first_leader)))
        admin = connect('ryzen1', leaf=False, identity='fabric.ops')
        definitions = json.loads((configdir / 'streams.json').read_text())
        selected = [s for s in definitions if s['name'] in
                    ('KV_STATE_ryzen3', 'KV_STATE_ryzen5', 'ROOT_ryzen3', 'ROOT_ryzen5', 'FLEET')]
        for config in selected:
            admin.api(f'{API}.STREAM.CREATE.' + config['name'], config)
        note('streams_created', [s['name'] for s in selected])
        def replica_info(client, stream, alive=3):
            info = client.api(f'{API}.STREAM.INFO.{stream}', {})
            replicas = info.get('cluster', {}).get('replicas', [])
            live_names = {name for name, proc in processes.items() if proc.poll() is None}
            current = [r for r in replicas if r.get('current') and r['name'] in live_names]
            assert info['config']['name'] == stream, info
            assert info['config']['num_replicas'] == 3, info
            assert info['cluster'].get('leader') in live_names, info
            assert len(current) >= alive - 1, info
            return {'leader': info['cluster']['leader'], 'configured_replicas': 3,
                    'current_followers': len(current), 'replicas': replicas}
        initial_replicas = eventually('R3 streams synchronized', lambda: {
            s['name']: replica_info(admin, s['name']) for s in selected})
        h3, h5 = connect('ryzen3'), connect('ryzen5')
        for h, client in (('ryzen3', h3), ('ryzen5', h5)):
            eventually('leaf API interest ' + h, lambda: client.api(
                f'{API}.STREAM.INFO.KV_STATE_{h}', {}))
        (a.out / 'leaf-interest.json').write_text(json.dumps({
            name: {'leafz': health(port, '/leafz'),
                   'subsz': health(port, '/subsz?subs=1')}
            for name, port in manifest['monitor_ports'].items()}, indent=2))
        value = b'S' * 102400
        event = b'E' * 65536
        def put_get(client, host, key, body):
            subject = f'$KV.STATE_{host}.{key}'
            # A disabled-JS leaf crosses a domain boundary. KV publishes must use
            # the domain-qualified alias; plain $KV is intentionally not extended.
            ack = json.loads(client.request(f'{API}.' + subject, body)[0])
            assert 'error' not in ack and ack['stream'] == 'KV_STATE_' + host, ack
            entry = client.api(f'{API}.STREAM.MSG.GET.KV_STATE_{host}', {'last_by_subj': subject})
            assert base64.b64decode(entry['message']['data']) == body, entry
            return {'bytes': len(body), 'sequence': ack['seq'], 'sha256': hashlib.sha256(body).hexdigest()}
        note('leaf_kv_roundtrip', {'ryzen3': put_get(h3, 'ryzen3', 'before', value),
                                  'ryzen5': put_get(h5, 'ryzen5', 'before', value)})
        ack = json.loads(h3.request('fabric.root.ryzen3.events.smoke', event)[0])
        assert ack['stream'] == 'ROOT_ryzen3', ack
        recovered = h3.api(f'{API}.STREAM.MSG.GET.ROOT_ryzen3', {'seq': ack['seq']})
        assert base64.b64decode(recovered['message']['data']) == event
        note('leaf_event_roundtrip', {'bytes': len(event), 'sequence': ack['seq']})
        # Centrally provision pull-only consumers; host users cannot choose an
        # arbitrary push deliver_subject and inject into another principal's inbox.
        consumer = admin.api(f'{API}.CONSUMER.DURABLE.CREATE.ROOT_ryzen3.SMOKE', {
            'stream_name': 'ROOT_ryzen3', 'config': {'durable_name': 'SMOKE', 'ack_policy': 'explicit',
                'deliver_policy': 'all', 'replay_policy': 'instant',
                'filter_subject': 'fabric.root.ryzen3.>', 'num_replicas': 3}})
        def consumer_ready(alive=3):
            info = observe('ryzen3', f'{API}.CONSUMER.INFO.ROOT_ryzen3.SMOKE', {})
            live_names = {name for name, proc in processes.items() if proc.poll() is None}
            assert info['name'] == 'SMOKE' and info['stream_name'] == 'ROOT_ryzen3', info
            assert info['config']['num_replicas'] == 3, info
            assert info['config']['ack_policy'] == 'explicit', info
            assert not info['config'].get('deliver_subject'), info
            assert info['config']['filter_subject'] == 'fabric.root.ryzen3.>', info
            cluster = info['cluster']
            assert cluster['leader'] in live_names, info
            current = [r for r in cluster.get('replicas', []) if r.get('current')
                       and r['name'] in live_names]
            assert len(current) == alive - 1, info
            return {'name': info['name'], 'stream_name': info['stream_name'],
                    'configured_replicas': info['config']['num_replicas'],
                    'leader': cluster['leader'], 'current_followers': len(current),
                    'ack_floor': info['ack_floor'], 'cluster': cluster}
        note('replicas_before', {'streams': initial_replicas, 'consumer': eventually(
            'consumer replicas', consumer_ready)})
        body, reply = h3.request(f'{API}.CONSUMER.MSG.NEXT.ROOT_ryzen3.SMOKE',
                                json.dumps({'batch': 1, 'expires': 5000000000}).encode())
        assert body == event and reply.startswith('$JS.ACK.'), reply
        h3.publish(reply, b'+ACK')
        h3.sock.sendall(b'PING\r\n')
        h3.until_pong()
        eventually('consumer ACK committed', lambda: h3.api(
            f'{API}.CONSUMER.INFO.ROOT_ryzen3.SMOKE', {})['num_ack_pending'] == 0)
        note('durable_consumer_ack', {'ack_subject': reply, 'pending': 0})
        denials = {}
        for subject in ('fabric.root.ryzen5.evil', '$KV.STATE_ryzen5.evil', f'{API}.$KV.STATE_ryzen5.evil', 'fabric.hub.control.evil',
                        f'{API}.STREAM.CREATE.EVIL', f'{API}.STREAM.MSG.GET.KV_STATE_ryzen5',
                        f'{API}.CONSUMER.CREATE.ROOT_ryzen3.EVIL',
                        f'{API}.CONSUMER.DURABLE.CREATE.ROOT_ryzen3.EVIL',
                        'fabric.fleet.work.ryzen5.evil'):
            c = connect('ryzen3')
            denials['pub:' + subject] = c.denied_publish(subject)
            c.close()
        for subject in ('fabric.root.ryzen5.>', '$KV.STATE_ryzen5.>', 'fabric.hub.control.>', '$SYS.>'):
            c = connect('ryzen3')
            denials['sub:' + subject] = c.denied_subscribe(subject)
            c.close()
        try:
            connect('ryzen3', identity='fabric.ryzen5')
            raise AssertionError('foreign-host certificate admitted at leaf')
        except (RuntimeError, ssl.SSLError, ConnectionError) as exc:
            denials['foreign_host_cert'] = type(exc).__name__
        note('acl_denials', denials)
        # Exercise the core's mapped leaf identity directly, independent of the
        # local leaf's ACL. A compromised/relaxed local leaf cannot widen it.
        core_denials = {}
        for subject in ('fabric.root.ryzen5.evil', f'{API}.$KV.STATE_ryzen5.evil',
                        'fabric.hub.control.evil', 'fabric.fleet.work.ryzen5.evil',
                        f'{API}.CONSUMER.DURABLE.CREATE.ROOT_ryzen3.EVIL'):
            c = Nats(manifest['core_ports']['ryzen1'], tlsdir, 'leaf-ryzen3', 'ryzen3')
            clients.append(c)
            core_denials[subject] = c.denied_publish(subject)
            c.close()
        c = Nats(manifest['core_ports']['ryzen1'], tlsdir, 'leaf-ryzen3', 'ryzen3')
        clients.append(c)
        own_info = c.api(f'{API}.STREAM.INFO.ROOT_ryzen3', {})
        assert own_info['config']['name'] == 'ROOT_ryzen3', own_info
        core_denials['subscribe:foreign-root'] = c.denied_subscribe('fabric.root.ryzen5.>')
        c.close()
        note('core_leaf_identity_acl', {'own_api': 'allowed', 'denials': core_denials})
        # Routes have a separate CA: ordinary application credentials are not
        # quorum-peer credentials, even if a local client can reach that port.
        ctx = ssl.create_default_context(cafile=str(tlsdir / 'route-ca.pem'))
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.load_cert_chain(str(tlsdir / 'fabric.ryzen3.pem'), str(tlsdir / 'fabric.ryzen3.key'))
        try:
            with socket.create_connection(('127.0.0.1', a.base_port + 100), timeout=5) as raw:
                with ctx.wrap_socket(raw, server_hostname='127.0.0.1') as route:
                    route.recv(16384)  # TLS 1.3 server alerts may follow client handshake completion.
            raise AssertionError('application certificate admitted on route listener')
        except ssl.SSLError as exc:
            assert 'ALERT_UNKNOWN_CA' in str(exc) or 'ALERT_BAD_CERTIFICATE' in str(exc), str(exc)
            note('route_trust_boundary', {'application_cert_rejected': True, 'reason': exc.reason})
        oversized = connect('ryzen3')
        try:
            # Declare an oversized body without transmitting it: the server
            # validates the PUB length before reading the body. Sending a full
            # oversized body races its immediate close/reset against sendall.
            oversized.sock.sendall(f'PUB fabric.root.ryzen3.oversized {MAX_PAYLOAD + 1}\r\n'.encode())
            oversized.until_pong()
            raise AssertionError('oversized payload admitted')
        except RuntimeError as exc:
            assert 'Maximum Payload Violation' in str(exc), str(exc)
            note('payload_limit', {'limit': MAX_PAYLOAD, 'rejected_declared_bytes': MAX_PAYLOAD + 1})
        finally:
            oversized.close()
        # Separate subscribers avoid mixing persistent replies with shared delivery.
        shared = connect('ryzen5')
        shared.subscribe_once('fabric.fleet.presence.ryzen3.smoke')
        def receiving_leaf_interest(host, subject):
            # Inspect the actual receiving link: producer-side JS wildcard
            # aggregation may hide an exact subject even when delivery is ready.
            return any(leaf.get('name') == 'leaf-' + host and
                       subject in leaf.get('subscriptions_list', [])
                       for core in CORE for leaf in health(
                           manifest['monitor_ports']['core-' + core],
                           '/leafz?subs=true').get('leafs', []))
        eventually('shared interest at receiving core link', lambda:
                   receiving_leaf_interest('ryzen5', 'fabric.fleet.presence.ryzen3.smoke'))
        shared_ack = json.loads(h3.request('fabric.fleet.presence.ryzen3.smoke', b'synthetic-presence')[0])
        assert shared_ack['stream'] == 'FLEET' and shared.message()[0] == b'synthetic-presence'
        note('shared_fleet_delivery', {'publisher': 'ryzen3', 'subscriber': 'ryzen5', 'stream': 'FLEET'})
        addressed = connect('ryzen3')
        addressed.subscribe_once('fabric.hub.delivery.ryzen3.smoke')
        eventually('addressed interest at receiving core link', lambda:
                   receiving_leaf_interest('ryzen3', 'fabric.hub.delivery.ryzen3.smoke'))
        hub = connect('ryzen1', leaf=False, identity='fabric.hub')
        hub_ack = json.loads(hub.request('fabric.hub.delivery.ryzen3.smoke', b'approved-hub-message')[0])
        # Hub uses its dedicated inbox; no generic response permission is needed.
        assert hub_ack['stream'] == 'FLEET', hub_ack
        assert addressed.message()[0] == b'approved-hub-message'
        note('hub_delivery', {'stream': hub_ack['stream'], 'leaf_received': 'ryzen3'})
        victim = processes[first_leader]
        victim.kill()
        victim.wait(timeout=10)
        assert victim.returncode == -signal.SIGKILL
        # A lost connection is an unknown-outcome error, never an implicit write retry.
        for c in clients:
            c.close()
        clients.clear()
        new_leader = eventually('meta leader failover', lambda: (
            leader if (leader := meta_leader()) and leader != first_leader else None), 45)
        live = [h for h in CORE if processes['core-' + h].poll() is None]
        survivor_host = new_leader.removeprefix('core-')
        # A metadata mutation, not just an open port, proves the surviving meta quorum.
        kvconfig = next(s for s in selected if s['name'] == 'KV_STATE_ryzen3')
        updated = dict(kvconfig, description='quorum mutation after SIGKILL')
        def surviving_streams_ready():
            c = connect(survivor_host, leaf=False, identity='fabric.ops', timeout=2)
            try:
                return {s['name']: replica_info(c, s['name'], 2) for s in selected}
            finally:
                c.close()
        surviving_replicas = eventually('data leaders and R2 quorum ready',
                                        surviving_streams_ready, 60)
        note('surviving_streams_ready', surviving_replicas)
        admin = connect(survivor_host, leaf=False, identity='fabric.ops')
        update_reply = admin.api(f'{API}.STREAM.UPDATE.KV_STATE_ryzen3', updated)
        assert update_reply['config']['description'] == updated['description'], update_reply
        for h in ('ryzen3', 'ryzen5'):
            eventually('leaf API recovered ' + h, lambda: observe(
                h, f'{API}.STREAM.INFO.KV_STATE_{h}', {}), 45)
        h3, h5 = connect('ryzen3'), connect('ryzen5')
        after = {'ryzen3': put_get(h3, 'ryzen3', 'after', value),
                 'ryzen5': put_get(h5, 'ryzen5', 'after', value)}
        # Surviving stream AND durable-consumer quorum must process another event.
        consumer_after = eventually('surviving consumer quorum', lambda: consumer_ready(2))
        root3_ack = json.loads(h3.request('fabric.root.ryzen3.events.after', event)[0])
        assert root3_ack['stream'] == 'ROOT_ryzen3', root3_ack
        body, reply = h3.request(f'{API}.CONSUMER.MSG.NEXT.ROOT_ryzen3.SMOKE',
                                json.dumps({'batch': 1, 'expires': 5000000000}).encode())
        assert body == event and reply.startswith('$JS.ACK.'), reply
        h3.publish(reply, b'+ACK')
        h3.sock.sendall(b'PING\r\n')
        h3.until_pong()
        eventually('post-loss consumer ACK committed', lambda: h3.api(
            f'{API}.CONSUMER.INFO.ROOT_ryzen3.SMOKE', {})['num_ack_pending'] == 0)
        ack = json.loads(h5.request('fabric.root.ryzen5.events.after', event)[0])
        assert ack['stream'] == 'ROOT_ryzen5'
        note('metadata_after', eventually('surviving metadata follower current',
                                          lambda: metadata_info(new_leader, 2)))
        note('quorum_after_sigkill', {'killed': first_leader, 'new_meta_leader': new_leader,
            'live_cores': live, 'metadata_update': 'acknowledged', 'kv': after,
            'event': {'bytes': len(event), 'sequence': ack['seq']},
            'streams': surviving_replicas, 'consumer': consumer_after,
            'consumer_after_ack': {'pending': 0, 'event_bytes': len(body)}})
        result['status'] = 'PASS'
    except (Exception, SmokeCancelled) as exc:
        result['error'] = str(exc)
        print('FAIL: ' + str(exc), file=sys.stderr, flush=True)
    finally:
        for c in clients:
            try:
                c.close()
            except Exception:
                pass
        for proc in processes.values():
            if proc.poll() is None:
                proc.terminate()
        for name, proc in processes.items():
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
            result.setdefault('process_exit', {})[name] = proc.returncode
        for handle in handles:
            handle.close()
        result['cleanup'] = {'all_children_reaped': all(p.poll() is not None for p in processes.values()),
                             'scratch_removed': False}
        shutil.rmtree(scratch)
        result['cleanup']['scratch_removed'] = not scratch.exists()
        result['elapsed_seconds'] = round(time.monotonic() - started, 2)
        (a.out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'cleanup': result['cleanup'],
                          'seconds': result['elapsed_seconds']}), flush=True)
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
