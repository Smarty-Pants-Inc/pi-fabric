#!/usr/bin/env python3
"""Read structured native Pi wake evidence; never infer authority from payload text.

Uses only Python stdlib. Counts producer custom_message.details.wakeCause and
actual custom pi-fabric.wake-cause.data separately (never double-counts them).
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
    return value


def read_session(filename, idle_ms):
    producers, wakes, idle_requests = [], [], []
    last_assistant = None
    with pathlib.Path(filename).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                at = timestamp_ms(entry.get("timestamp"))
                details = entry.get("details") or {}
                if entry.get("type") == "custom_message" and "wakeCause" in details:
                    producers.append({"entryId": entry.get("id"), "at": at, "customType": entry.get("customType"),
                                      "record": cause_record(details["wakeCause"])})
                if entry.get("type") == "custom" and entry.get("customType") == "pi-fabric.wake-cause":
                    wakes.append({"entryId": entry.get("id"), "at": at, "record": cause_record(entry.get("data"))})
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
                                              **(cause_record(details["wakeCause"]) if "wakeCause" in details
                                                 else {"cause": "unattributed"})})
                    else:
                        continue
                    last_assistant = None  # One trigger per idle interval, like wake_triggers.py.
            except (ValueError, TypeError, AttributeError) as error:
                raise ValueError(f"{filename}:{line_number}: {error}") from error
    count = lambda rows: dict(sorted(collections.Counter(row["record"]["cause"] for row in rows).items()))
    return {"session": str(filename), "producerCount": len(producers), "producerCauseCounts": count(producers),
            "freshTurnCount": len(wakes), "freshTurnCauseCounts": count(wakes), "freshTurns": wakes,
            "producerMessages": producers, "idleThresholdMs": idle_ms, "requestsAfterIdle": idle_requests,
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
