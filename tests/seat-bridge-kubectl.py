#!/usr/bin/python3 -IB
"""A synthetic kubectl for the D3 resume-drain seam tests (smarty-dev#15310). No cluster is contacted.

State lives beside $KUBECONFIG as kube-state.json: {server, caB64, pod: {name, uid, meshRoot, running, terminated},
exec: {line, terminate}}. Every invocation's argv is appended to kube-calls.jsonl.
"""
import json
import os
import sys
import subprocess

directory = os.path.dirname(os.environ["KUBECONFIG"])
state_file = os.path.join(directory, "kube-state.json")
with open(state_file) as handle:
    state = json.load(handle)
args = sys.argv[1:]
with open(os.path.join(directory, "kube-calls.jsonl"), "a") as handle:
    handle.write(json.dumps({"argv": args, "env": sorted(os.environ)}) + "\n")
assert args[0] == "--kubeconfig" and args[2] == "--context", args
rest = args[4:]


def save():
    with open(state_file, "w") as handle:
        json.dump(state, handle)


if rest[:4] == ["config", "view", "--minify", "--raw"]:
    print(json.dumps({"clusters": [{"name": "c", "cluster": {"server": state["server"], "certificate-authority-data": state["caB64"]}}]}))
elif rest[0] == "-n" and rest[2:4] == ["get", "pod"]:
    pod = state["pod"]
    if pod is None:
        sys.exit(1)
    status = {"terminated": {"exitCode": pod["terminated"]}} if pod["terminated"] is not None else ({"running": {"startedAt": pod.get("startedAt", "2026-10-31T00:00:00Z")}} if pod["running"] else {"waiting": {}})
    print(json.dumps({"metadata": {"name": pod["name"], "uid": pod["uid"]},
                      "spec": {"nodeName": pod.get("nodeName", "synthetic-node"),
                               "volumes": [{"name": "state", "persistentVolumeClaim": {"claimName": "state"}}],
                               "containers": [{"name": "pi", "args": pod.get("args", []),
                                                "volumeMounts": [{"name": "state", "mountPath": "/var/lib/fabric"}],
                                                "env": [{"name": "PI_FABRIC_MESH_ROOT", "value": pod["meshRoot"]},
                                                        {"name": "PI_CODING_AGENT_SESSION_DIR", "value": "/var/lib/fabric/sessions"}] +
                                                ([{"name": "PI_FABRIC_MESH_NATS", "value": json.dumps({"machine": pod["natsMachine"],
                                                   "servers": ["tls://nats.synthetic.invalid:4222"]})}] if pod.get("natsMachine") else [])}] + pod.get("extraContainers", []),
                               "initContainers": pod.get("initContainers", []), "ephemeralContainers": pod.get("ephemeralContainers", [])},
                      "status": {"containerStatuses": [{"name": "pi", "state": status,
                          "restartCount": pod.get("restartCount", 0), "containerID": pod.get("containerID", "containerd://synthetic-pi")}]}}))
elif rest[0] == "-n" and rest[2] == "exec" and rest[7:9] == ["test", "-x"]:
    sys.exit(0 if state.get("cliPresent", True) else 1)
elif rest[0] == "-n" and rest[2] == "exec" and rest[7:10] == ["/usr/bin/python3", "-I", "-c"]:
    identity = state["identity"]
    if "exact resume exec argv" in rest[10] and state.get("journalFile") and not state.get("readOverride"):
        # Execute the EXACT production script, substituting only the fixture's mounted path.
        result = subprocess.run(rest[7:11] + [state["journalFile"], rest[12]], capture_output=True, text=True)
        sys.stdout.write(result.stdout)
        identity = dict(identity, code=result.returncode)
    else:
        print(state.get("ownerLine", identity["line"]) if "exact publish exec argv" in rest[10] else identity["line"])
    if identity.get("changeUid"):
        state["pod"]["uid"] = identity["changeUid"]
    if identity.get("restart"):
        state["pod"].update(identity["restart"])
    save()
    sys.exit(identity.get("code", 0))
elif rest[0] == "-n" and rest[2] == "exec" and "--identity" in rest:
    # Pinned 0.112.0 dist/cli/index.js knows mesh/decisions/nats/events, NOT session.
    if state.get("image0112"):
        sys.stderr.write('pi-fabric: unknown command "session"\nUsage: pi-fabric <command> [options]\n\nCommands:\n  mesh\n  decisions\n  nats\n  events\n\nRun `pi-fabric <command> --help` for command options.\n')
        sys.exit(2)
    identity = state["identity"]
    print(identity["line"])
    if identity.get("changeUid"):
        state["pod"]["uid"] = identity["changeUid"]
        save()
    sys.exit(identity.get("code", 0))
elif rest[0] == "-n" and rest[2] == "exec":
    print(state["exec"]["line"])
    if state["exec"].get("terminate"):
        state["pod"]["running"] = False
        state["pod"]["terminated"] = 0
        save()
    if state["exec"].get("code"):
        sys.exit(state["exec"]["code"])
else:
    sys.exit(2)
