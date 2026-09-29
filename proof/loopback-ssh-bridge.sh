#!/usr/bin/env bash
# smarty-dev#2045 lane C, step 1: the mesh bridge over REAL ssh on one host (loopback), with no global
# configuration change. Everything lives in one `mktemp -d`: a private sshd on a free high port
# (its own HostKey, AuthorizedKeysFile and forced command), the bridge's dedicated key, a private
# ssh_config and known_hosts. ~/.ssh and the system sshd are never read or written.
#
# usage: proof/loopback-ssh-bridge.sh BRIDGE_CHECKOUT [EVIDENCE_DIR]
#   BRIDGE_CHECKOUT  a built pi-fabric checkout with bin/mesh-bridge (pi-fabric#135)
#   The two Pis load this checkout's dist (must contain pi-fabric#132); run `bun run build` first.
#   EVIDENCE_DIR     copied there at the end (default: kept in the scratch dir)
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
bridge_checkout=$(cd "$1" && pwd)
evidence=${2:-}
bridge_bin=$bridge_checkout/bin/mesh-bridge
[[ -f $here/dist/index.js && -f $bridge_checkout/dist/mesh-bridge.js ]] || { echo "build both checkouts first" >&2; exit 2; }

T=$(mktemp -d -t mesh-bridge-loopback-XXXXXX)
chmod 700 "$T"
sshd_pid=
cleanup() { [[ -n $sshd_pid ]] && kill "$sshd_pid" 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p "$T/run/forge" "$T/bin"
node=$(command -v node)
forge_mesh=$T/run/forge/mesh

ssh-keygen -q -t ed25519 -N '' -f "$T/host_key" -C mesh-bridge-loopback-host
ssh-keygen -q -t ed25519 -N '' -f "$T/bridge_key" -C mesh-bridge@dev1
# The forced command: the key can run only the bridge agent on the forge mesh, pinned to peer dev1.
printf 'command="%s %s agent --mesh %s --peer dev1",restrict %s\n' "$node" "$bridge_bin" "$forge_mesh" "$(cat "$T/bridge_key.pub")" > "$T/authorized_keys"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
cat > "$T/sshd_config" <<EOF
Port $port
ListenAddress 127.0.0.1
HostKey $T/host_key
PidFile $T/sshd.pid
AuthorizedKeysFile $T/authorized_keys
AllowUsers $(id -un)
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
PermitRootLogin no
# The scratch dir is under /tmp (mode 1777), which StrictModes would reject; the dir itself is 0700.
StrictModes no
LogLevel VERBOSE
EOF
/usr/sbin/sshd -t -f "$T/sshd_config"
/usr/sbin/sshd -D -e -f "$T/sshd_config" 2> "$T/sshd.log" &
sshd_pid=$!

printf '[127.0.0.1]:%s %s\n' "$port" "$(cut -d' ' -f1,2 "$T/host_key.pub")" > "$T/known_hosts"
cat > "$T/ssh_config" <<EOF
Host forge-loop
  HostName 127.0.0.1
  Port $port
  User $(id -un)
  UserKnownHostsFile $T/known_hosts
  GlobalKnownHostsFile /dev/null
  StrictHostKeyChecking yes
EOF
# The bridge builds its own ssh argv (--ssh forge-loop --ssh-key ...); this shim only points it at the
# private ssh_config, as ~/.ssh/config would on a real host.
printf '#!/bin/sh\nexec /usr/bin/ssh -F %s "$@"\n' "$T/ssh_config" > "$T/bin/ssh"
chmod +x "$T/bin/ssh"
for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break; sleep 0.1; done

# The forced command wins: asking for another command still runs only the agent (it answers no shell).
{ echo "== forced command check: ssh forge-loop id (a shell would print the user id line)"
  timeout 5 "$T/bin/ssh" -T -o BatchMode=yes -o IdentitiesOnly=yes -i "$T/bridge_key" forge-loop id </dev/null 2>&1 || echo "exit=$?"
  grep 'Starting session' "$T/sshd.log" || true
} > "$T/run/forced-command-check.txt"
if grep -q '^uid=' "$T/run/forced-command-check.txt"; then echo "forced command NOT enforced" >&2; exit 1; fi

status=0
bun "$here/proof/loopback-ssh-bridge.mjs" "$T/run" "$here/dist/index.js" "$bridge_bin" forge-loop "$T/bridge_key" "$T/bin" \
  2>&1 | tee "$T/run/driver.log" || status=$?
status=${PIPESTATUS[0]:-$status}
cleanup; sshd_pid=
cp "$T/sshd.log" "$T/sshd_config" "$T/authorized_keys" "$T/ssh_config" "$T/run/"
if [[ -n $evidence ]]; then
  mkdir -p "$evidence"
  (cd "$T/run" && cp -r results.json driver.log bridge.log sshd.log sshd_config authorized_keys ssh_config forced-command-check.txt "$evidence/")
  for s in dev1 forge; do cp "$T/run/$s/rpc-stdout.jsonl" "$evidence/$s-rpc-stdout.jsonl"; cp "$T/run/$s/rpc-stderr.log" "$evidence/$s-rpc-stderr.log"; done
  echo "evidence: $evidence"
  rm -rf "$T"
else
  echo "scratch: $T (delete with: rm -rf $T)"
fi
exit "$status"
