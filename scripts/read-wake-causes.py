#!/usr/bin/env python3
"""Read structured native Pi wake evidence; never infer authority from payload text.

Uses only Python stdlib. Counts producer custom_message.details.wakeCause and
actual custom pi-fabric.wake-cause.data separately (never double-counts them).
Raw-user ambiguous/unattributed diagnostics are counted separately, never as
Fabric wakes. They include human input and do not assert an originating party.
The optional >=2min idle view classifies native user requests as user, even if
text contains Fabric headers/JSON. It is diagnostic, not the fresh-turn counter.
"""
import argparse
import collections
import datetime
import json
import pathlib

CAUSES = frozenset(("steer", "followUp", "actor", "inbox", "host-event", "mesh"))


def timestamp_ms(value):
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
    return None


def cause_record(value):
    if not isinstance(value, dict) or value.get("cause") not in CAUSES:
        raise ValueError("invalid structured wake cause")
    sender = value.get("from")
    if not isinstance(sender, dict) or not all(isinstance(sender.get(k), str) for k in ("id", "name", "kind")):
        raise ValueError("invalid structured wake sender")
    if sender["kind"] not in ("main", "actor", "agent", "remote"):
        raise ValueError("invalid structured wake sender kind")
    for field in ("topic", "key"):
        if field in value and not isinstance(value[field], str):
            raise ValueError("invalid structured wake " + field)
    record = {"cause": value["cause"], "from": {k: sender[k] for k in ("id", "name", "kind")}}
    for field in ("topic", "key"):
        if field in value:
            record[field] = value[field]
    if "exact" in value:
        if not isinstance(value["exact"], bool):
            raise ValueError("invalid structured wake exactness")
        record["exact"] = value["exact"]
    return record


def diagnostic_record(value):
    if not isinstance(value, dict) or value.get("cause") not in ("ambiguous", "unattributed", "multiple"):
        raise ValueError("invalid wake diagnostic")
    record = {"cause": value["cause"]}
    if "exact" in value:
        if value["exact"] is not False:
            raise ValueError("diagnostic must not claim exact single origin")
        record["exact"] = False
    if value["cause"] == "multiple":
        causes = value.get("causes")
        if value.get("exact") is not False or not isinstance(causes, list) or len(causes) < 2:
            raise ValueError("multiple diagnostic requires inexact aggregate and admitted causes")
        record["causes"] = []
        for source in causes:
            if not isinstance(source, dict) or not isinstance(source.get("exact"), bool) or source.get("cause") == "multiple":
                raise ValueError("invalid admitted wake source")
            record["causes"].append(cause_record(source) if source.get("cause") in CAUSES else diagnostic_record(source))
    elif value["cause"] == "ambiguous":
        candidates = value.get("candidates")
        if value.get("basis") != "unconfirmed-raw-input-attempts" or not isinstance(candidates, list) or len(candidates) < 2:
            raise ValueError("ambiguous diagnostic requires unconfirmed attempt candidates")
        record["candidates"] = [cause_record(candidate) for candidate in candidates]
        record["basis"] = "unconfirmed-raw-input-attempts"
    return record


def read_session(filename, idle_ms):
    producers, wakes, diagnostics, idle_requests = [], [], [], []
    last_assistant = None
    with pathlib.Path(filename).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                at = timestamp_ms(entry.get("timestamp"))
                details = entry.get("details") or {}
                if entry.get("type") == "custom_message":
                    values = details.get("wakeCauses", [details["wakeCause"]] if "wakeCause" in details else [])
                    if not isinstance(values, list):
                        raise ValueError("invalid producer wake causes")
                    for value in values:
                        producers.append({"entryId": entry.get("id"), "at": at, "customType": entry.get("customType"),
                                          "record": cause_record(value)})
                if entry.get("type") == "custom" and entry.get("customType") == "pi-fabric.wake-cause":
                    wakes.append({"entryId": entry.get("id"), "at": at, "record": cause_record(entry.get("data"))})
                if entry.get("type") == "custom" and entry.get("customType") == "pi-fabric.wake-diagnostic":
                    diagnostics.append({"entryId": entry.get("id"), "at": at, "record": diagnostic_record(entry.get("data"))})
                message = entry.get("message") or {}
                if entry.get("type") == "message" and message.get("role") == "assistant":
                    last_assistant = at
                elif last_assistant is not None and at is not None and at - last_assistant >= idle_ms:
                    idle = at - last_assistant
                    if entry.get("type") == "message" and message.get("role") == "user":
                        # Native typed/RPC user stays user; no text/header inspection.
                        idle_requests.append({"entryId": entry.get("id"), "at": at, "idleMs": idle, "cause": "user"})
                    elif entry.get("type") == "custom_message":
                        idle_requests.append({"entryId": entry.get("id"), "at": at, "idleMs": idle,
                                              **(diagnostic_record({"cause": "multiple", "exact": False,
                                                                    "causes": [{**cause_record(value), "exact": True} for value in details["wakeCauses"]]})
                                                 if "wakeCauses" in details else cause_record(details["wakeCause"]) if "wakeCause" in details
                                                 else {"cause": "unattributed"})})
                    else:
                        continue
                    last_assistant = None  # One trigger per idle interval, like wake_triggers.py.
            except (ValueError, TypeError, AttributeError) as error:
                raise ValueError(f"{filename}:{line_number}: {error}") from error
    count = lambda rows: dict(sorted(collections.Counter(row["record"]["cause"] for row in rows).items()))
    return {"session": str(filename), "producerCount": len(producers), "producerCauseCounts": count(producers),
            "freshTurnCount": len(wakes), "freshTurnCauseCounts": count(wakes), "freshTurns": wakes,
            "producerMessages": producers, "rawUserDiagnosticCount": len(diagnostics),
            "rawUserDiagnosticCounts": count(diagnostics), "rawUserDiagnostics": diagnostics,
            "idleThresholdMs": idle_ms, "requestsAfterIdle": idle_requests,
            "requestsAfterIdleCounts": dict(sorted(collections.Counter(row["cause"] for row in idle_requests).items())),
            "freshTurnsWithoutMatchingProducer": [row for row in wakes if not any(row["record"] == item["record"] for item in producers)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sessions", nargs="+", type=pathlib.Path)
    parser.add_argument("--idle-ms", type=int, default=120000)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    if args.idle_ms < 0:
        parser.error("--idle-ms must be non-negative")
    output = json.dumps({"sessions": [read_session(file, args.idle_ms) for file in args.sessions]}, indent=2) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    print(output, end="")


if __name__ == "__main__":
    main()
