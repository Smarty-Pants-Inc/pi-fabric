#!/usr/bin/python3 -IB
"""A synthetic kubectl for the D3 resume-drain seam tests (smarty-dev#15310). No cluster is contacted.

State lives beside $KUBECONFIG as kube-state.json: {server, caB64, pod: {name, uid, meshRoot, running, terminated},
exec: {line, terminate}}. Every invocation's argv is appended to kube-calls.jsonl.
"""
import json
import os
import sys

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
    status = {"terminated": {"exitCode": pod["terminated"]}} if pod["terminated"] is not None else ({"running": {}} if pod["running"] else {"waiting": {}})
    print(json.dumps({"metadata": {"name": pod["name"], "uid": pod["uid"]},
                      "spec": {"nodeName": pod.get("nodeName", "synthetic-node"),
                               "containers": [{"name": "pi", "env": [{"name": "PI_FABRIC_MESH_ROOT", "value": pod["meshRoot"]}]}]},
                      "status": {"containerStatuses": [{"name": "pi", "state": status}]}}))
elif rest[0] == "-n" and rest[2] == "exec" and rest[7:9] == ["test", "-x"]:
    sys.exit(0 if state.get("cliPresent", True) else 1)
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
