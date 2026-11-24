#!/usr/bin/env python3
"""Render secret-free NATS configs. No network, credentials, service or install effects."""
import argparse
import json
from pathlib import Path

HOSTS = ('ryzen1', 'ryzen2', 'ryzen3', 'ryzen4', 'ryzen5', 'epyc1', 'm4', 'i9', 'm5')
CORE = ('ryzen1', 'ryzen2', 'ryzen4')  # Proposed quorum peers; Ryzen 1 owns the control plane.
IPS = {'ryzen1': '100.78.221.70', 'ryzen2': '100.103.29.1', 'ryzen4': '100.105.145.68'}
DOMAIN = 'fleet'
MAX_PAYLOAD = 262144  # 256 KiB, incl. headers: >100 KiB state / >64 KiB event envelopes.
API = '$JS.fleet.API'


def q(value):
    return json.dumps(str(value))


def permissions(host):
    streams = (f'ROOT_{host}', f'KV_STATE_{host}')
    pub = [f'fabric.root.{host}.>', f'fabric.fleet.work.{host}.>',
           f'fabric.fleet.presence.{host}.>', f'fabric.fleet.request.{host}.>',
           f'$KV.STATE_{host}.>', f'{API}.$KV.STATE_{host}.>', f'_INBOX.{host}.>']
    sub = [f'fabric.root.{host}.>', 'fabric.fleet.work.>', 'fabric.fleet.presence.>',
           f'fabric.hub.delivery.{host}.>', f'$KV.STATE_{host}.>', f'_INBOX.{host}.>']
    for stream in streams:
        # Cores map domain-qualified subjects to the native API before ACL checks;
        # leaves forward the domain prefix. Both aliases remain exact-host scoped.
        for prefix in (API, '$JS.API'):
            pub += [f'{prefix}.STREAM.INFO.{stream}', f'{prefix}.STREAM.MSG.GET.{stream}',
                    f'{prefix}.CONSUMER.INFO.{stream}.>',
                    # Only ops provisions pull consumers: arbitrary deliver_subject
                    # in host-created push consumers bypasses cross-host subject ACLs.
                    f'{prefix}.CONSUMER.MSG.NEXT.{stream}.>']
        pub += [f'$JS.ACK.{stream}.>', f'$JS.ACK.fleet.*.{stream}.>']
    return {'publish': {'allow': pub}, 'subscribe': {'allow': sub}}


def user(name, host):
    # Mandatory verify_and_map mTLS maps the cert SAN to this user. No passwords.
    return {'user': name, 'permissions': permissions(host)}


def tls(certdir, name, route=False, mapping=False):
    ca = 'route-ca.pem' if route else 'transport-ca.pem'
    lines = [f'cert_file: {q(certdir / (name + ".pem"))}',
             f'key_file: {q(certdir / (name + ".key"))}',
             f'ca_file: {q(certdir / ca)}', 'verify: true', 'timeout: 5',
             'min_version: "1.3"']
    if mapping:
        lines.append('verify_and_map: true')
    return '{\n    ' + '\n    '.join(lines) + '\n  }'


def accounts(host=None):
    users = [user('fabric.' + h, h) for h in (HOSTS if host is None else (host,))]
    if host is None:
        # No leafnodes.authorization override: leaf mTLS falls through to these account
        # users so their publish/subscribe ACLs are enforced on the receiving core.
        users += [user('leaf.' + h, h) for h in HOSTS if h not in CORE]
        users += [{'user': 'fabric.ops', 'permissions': {
                      'publish': {'allow': [f'{API}.>', '$JS.API.>', '$JS.ACK.>', '_INBOX.ops.>']},
                      'subscribe': {'allow': ['_INBOX.ops.>']}}},
                  {'user': 'fabric.hub', 'permissions': {
                      'publish': {'allow': ['fabric.hub.>', 'fabric.fleet.>', '_INBOX.hub.>']},
                      'subscribe': {'allow': ['fabric.hub.>', 'fabric.fleet.>', '_INBOX.hub.>']}}}]
    account = {'users': users}
    if host is None:
        account['jetstream'] = {'max_memory': 268435456, 'max_store': 21474836480,
                                'max_streams': 64, 'max_consumers': 256}
    result = {'FABRIC': account}
    if host is None:
        result['SYS'] = {'users': [{'user': 'fabric.sys', 'permissions': {
            'publish': {'allow': ['$SYS.REQ.>']},
            'subscribe': {'allow': ['$SYS.>', '_INBOX.sys.>']}}}]}
    return json.dumps(result, indent=2)


def stream_configs():
    streams = []
    for h in HOSTS:
        streams += [{'name': 'ROOT_' + h, 'subjects': ['fabric.root.' + h + '.>'],
                     'retention': 'limits', 'storage': 'file', 'num_replicas': 3,
                     'max_bytes': 268435456, 'max_msg_size': MAX_PAYLOAD,
                     'max_age': 604800000000000, 'max_msgs': 100000,
                     'max_consumers': 16, 'discard': 'new',
                     'duplicate_window': 120000000000, 'allow_direct': False},
                    {'name': 'KV_STATE_' + h, 'subjects': ['$KV.STATE_' + h + '.>'],
                     'retention': 'limits', 'storage': 'file', 'num_replicas': 3,
                     'max_bytes': 134217728, 'max_msg_size': MAX_PAYLOAD,
                     'max_msgs_per_subject': 10, 'max_msgs': -1, 'max_age': 0,
                     'max_consumers': 16, 'discard': 'new',
                     'allow_rollup_hdrs': True, 'deny_delete': True,
                     'allow_direct': False}]
    streams += [{'name': 'FLEET', 'subjects': ['fabric.fleet.>', 'fabric.hub.delivery.>'],
                 'retention': 'limits', 'storage': 'file', 'num_replicas': 3,
                 'max_bytes': 1073741824, 'max_msg_size': MAX_PAYLOAD,
                 'max_age': 604800000000000, 'max_msgs': 500000,
                 'max_consumers': 64, 'discard': 'new', 'duplicate_window': 120000000000}]
    return streams


def render(out, certdir, store, smoke=False, base=22000):
    out.mkdir(parents=True, exist_ok=True)
    hosts = ('ryzen3', 'ryzen5') if smoke else tuple(h for h in HOSTS if h not in CORE)
    peer = {h: ('127.0.0.1' if smoke else IPS[h]) for h in CORE}
    client = {h: (base + i if smoke else 4222) for i, h in enumerate(CORE)}
    routes = {h: (base + 100 + i if smoke else 6222) for i, h in enumerate(CORE)}
    leaves = {h: (base + 200 + i if smoke else 7422) for i, h in enumerate(CORE)}
    for i, h in enumerate(CORE):
        route_urls = ['nats-route://' + peer[p] + ':' + str(routes[p]) for p in CORE if p != h]
        config = f'''# Rendered from docs/nats/render.py; secret-free; hostd child, not an install.
server_name: "core-{h}"
listen: "127.0.0.1:{client[h]}"
http: "127.0.0.1:{base + 300 + i if smoke else 8222}"
max_payload: {MAX_PAYLOAD}
max_pending: 8388608
max_connections: 2048
write_deadline: "5s"
ping_interval: "20s"
ping_max: 3
lame_duck_duration: "30s"
lame_duck_grace_period: "10s"
tls: {tls(certdir, 'server-' + h, mapping=True)}
jetstream {{
  domain: "fleet"
  store_dir: {q(store / h / 'jetstream')}
  max_memory_store: 536870912
  max_file_store: 32212254720
  sync_interval: always
}}
system_account: SYS
accounts: {accounts()}
cluster {{
  name: "fabric-r3"
  listen: "{peer[h]}:{routes[h]}"
  advertise: "{peer[h]}:{routes[h]}"
  routes: {json.dumps(route_urls)}
  tls: {tls(certdir, 'route-' + h, route=True)}
}}
leafnodes {{
  listen: "{peer[h]}:{leaves[h]}"
  advertise: "{peer[h]}:{leaves[h]}"
  tls: {tls(certdir, 'server-' + h, mapping=True)}
  # Authorization uses accounts.FABRIC.users with leaf.<host> mTLS identity + ACLs.
}}
'''
        (out / ('core-' + h + '.conf')).write_text(config)
    for i, h in enumerate(hosts):
        urls = ['tls://' + peer[p] + ':' + str(leaves[p]) for p in CORE]
        config = f'''# Leaf has NO JetStream: no accidental local R1 stream / divergent KV authority.
server_name: "leaf-{h}"
listen: "127.0.0.1:{base + 10 + i if smoke else 4222}"
http: "127.0.0.1:{base + 310 + i if smoke else 8222}"
max_payload: {MAX_PAYLOAD}
max_pending: 8388608
max_connections: 512
write_deadline: "5s"
ping_interval: "20s"
ping_max: 3
lame_duck_duration: "30s"
lame_duck_grace_period: "10s"
tls: {tls(certdir, 'server-' + h, mapping=True)}
accounts: {accounts(h)}
leafnodes {{
  reconnect: "2s"
  remotes: [{{
    urls: {json.dumps(urls)}
    account: "FABRIC"
    tls: {tls(certdir, 'leaf-' + h)}
  }}]
}}
'''
        (out / ('leaf-' + h + '.conf')).write_text(config)
    (out / 'streams.json').write_text(json.dumps(stream_configs(), indent=2) + '\n')
    (out / 'manifest.json').write_text(json.dumps({
        'mode': 'smoke' if smoke else 'production', 'cores': list(CORE), 'leaves': list(hosts),
        'max_payload': MAX_PAYLOAD, 'jetstream_domain': DOMAIN,
        'core_ports': client, 'leaf_ports': {h: (base + 10 + i if smoke else 4222)
                                         for i, h in enumerate(hosts)},
        'monitor_ports': {'core-' + h: (base + 300 + i if smoke else 8222)
                          for i, h in enumerate(CORE)} | {
            'leaf-' + h: (base + 310 + i if smoke else 8222) for i, h in enumerate(hosts)}
    }, indent=2) + '\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=('production', 'smoke'), default='production')
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--tls-dir', type=Path, default=Path('/home/paul/.local/state/smarty/nats/tls'))
    p.add_argument('--store-root', type=Path, default=Path('/home/paul/.local/state/smarty/nats'))
    p.add_argument('--base-port', type=int, default=22000)
    a = p.parse_args()
    if not 1024 <= a.base_port <= 65224:
        p.error('--base-port must leave room for +311 within TCP port range')
    render(a.out, a.tls_dir, a.store_root, a.mode == 'smoke', a.base_port)
    print(f'Rendered {a.mode} configs to {a.out}; no service changes.')


if __name__ == '__main__':
    main()
