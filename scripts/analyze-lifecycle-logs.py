#!/usr/bin/env python3
"""Analyze Agentic API structured lifecycle logs.

This helper is intended for controlled one-request-at-a-time validation of the
streaming lifecycle introduced by the custom maintenance patches.

It can either read an existing log file/stdin or collect logs directly from a
Kubernetes pod with ``kubectl logs``. Text and JSON tracing output are both
accepted.

Because the current server does not attach one correlation ID to every
lifecycle log line, concurrent requests can interleave. The analyzer warns when
multiple request-level markers are detected; for deterministic validation,
issue one test request at a time and use a narrow --since window.

Exit codes:
  0: recognized lifecycle and invariants passed
  1: lifecycle invariant failure or expectation mismatch
  2: inconclusive / no lifecycle events / possibly still running
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
FIELD_RE_TEMPLATE = r"\b{key}=(?:\"([^\"]*)\"|([^\s]+))"

PHASES = {
    "upstream_http_send_started",
    "upstream_http_accepted",
    "upstream_http_transport_error",
    "upstream_http_rejected",
    "upstream_error_body_read_failed",
    "downstream_commit_ready",
    "downstream_commit_released",
    "downstream_sse_response_created",
    "upstream_stream_eof",
    "upstream_done_marker",
    "upstream_stream_read_error",
    "downstream_sse_error",
    "downstream_terminal_event",
    "request_error_response",
}

# The release acknowledgement is intentionally not included here. The worker
# logs downstream_commit_released after it wakes, while the caller can return
# the BoxStream and build the HTTP response immediately after sending release.
# Therefore released vs sse_response_created log ordering is scheduler-dependent.
NORMAL_ORDERED = [
    "upstream_http_send_started",
    "upstream_http_accepted",
    "downstream_commit_ready",
    "downstream_sse_response_created",
    "downstream_terminal_event",
]


@dataclass(frozen=True)
class Event:
    line_no: int
    phase: str
    raw: str
    status: str | None = None
    response_status: str | None = None
    error_type: str | None = None
    error_code: str | None = None


def json_fields(line: str) -> dict[str, object] | None:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def field(line: str, key: str, parsed_json: dict[str, object] | None = None) -> str | None:
    if parsed_json is not None and key in parsed_json:
        value = parsed_json[key]
        if value is None:
            return None
        return str(value)

    match = re.search(FIELD_RE_TEMPLATE.format(key=re.escape(key)), line)
    if not match:
        return None
    return match.group(1) if match.group(1) is not None else match.group(2)


def parse_events(lines: Iterable[str]) -> list[Event]:
    events: list[Event] = []
    for line_no, original in enumerate(lines, start=1):
        line = ANSI_RE.sub("", original.rstrip("\n"))
        parsed_json = json_fields(line)
        phase = field(line, "phase", parsed_json)
        if phase not in PHASES:
            continue
        events.append(
            Event(
                line_no=line_no,
                phase=phase,
                raw=line,
                status=field(line, "status", parsed_json),
                response_status=field(line, "response_status", parsed_json),
                error_type=field(line, "error_type", parsed_json),
                error_code=field(line, "error_code", parsed_json),
            )
        )
    return events


def first_index(events: list[Event], phase: str) -> int | None:
    for index, event in enumerate(events):
        if event.phase == phase:
            return index
    return None


def last_event(events: list[Event], phase: str) -> Event | None:
    for event in reversed(events):
        if event.phase == phase:
            return event
    return None


def ordered(events: list[Event], phases: list[str]) -> bool:
    indices: list[int] = []
    for phase in phases:
        index = first_index(events, phase)
        if index is None:
            return False
        indices.append(index)
    return indices == sorted(indices) and len(indices) == len(set(indices))


def collect_logs(args: argparse.Namespace) -> str:
    if args.file:
        return Path(args.file).read_text(encoding="utf-8", errors="replace")

    if args.pod:
        cmd = ["kubectl"]
        if args.namespace:
            cmd += ["-n", args.namespace]
        cmd += ["logs", args.pod, f"--since={args.since}", f"--tail={args.tail}"]
        if args.container:
            cmd += ["-c", args.container]
        if args.previous:
            cmd.append("--previous")
        try:
            proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
        except FileNotFoundError as exc:
            raise RuntimeError("kubectl was not found in PATH") from exc
        if proc.returncode != 0:
            raise RuntimeError(f"kubectl logs failed ({proc.returncode}): {proc.stderr.strip()}")
        return proc.stdout

    if sys.stdin.isatty():
        raise RuntimeError("provide --pod, --file, or pipe logs on stdin")
    return sys.stdin.read()


def validate(events: list[Event]) -> tuple[str, str, list[str], list[str]]:
    """Return (verdict, classification, failures, warnings)."""
    failures: list[str] = []
    warnings: list[str] = []

    if not events:
        return "WARN", "NO_LIFECYCLE_EVENTS", [], ["no known phase= lifecycle events were found"]

    counts = {phase: sum(event.phase == phase for event in events) for phase in PHASES}
    if counts["downstream_sse_response_created"] > 1 or counts["request_error_response"] > 1:
        warnings.append(
            "multiple request-level lifecycle markers were found; concurrent or multiple requests may be mixed"
        )

    ready = first_index(events, "downstream_commit_ready")
    released = first_index(events, "downstream_commit_released")
    sse_created = first_index(events, "downstream_sse_response_created")
    accepted = first_index(events, "upstream_http_accepted")
    request_error = first_index(events, "request_error_response")
    downstream_error = first_index(events, "downstream_sse_error")
    terminal = first_index(events, "downstream_terminal_event")

    if ready is not None and accepted is None:
        failures.append("downstream_commit_ready occurred without a prior upstream_http_accepted")
    if ready is not None and accepted is not None and accepted > ready:
        failures.append("downstream_commit_ready occurred before upstream_http_accepted")
    if released is not None and ready is None:
        failures.append("downstream_commit_released occurred without downstream_commit_ready")
    if released is not None and ready is not None and ready > released:
        failures.append("downstream_commit_released occurred before downstream_commit_ready")
    if sse_created is not None and ready is None:
        failures.append("HTTP 200 SSE was created without downstream_commit_ready")
    if sse_created is not None and ready is not None and ready > sse_created:
        failures.append("HTTP 200 SSE was created before downstream_commit_ready")
    if sse_created is not None and released is None:
        failures.append("HTTP 200 SSE was created but no downstream_commit_released was observed")
    if request_error is not None and sse_created is not None and sse_created < request_error:
        failures.append("request-level HTTP error occurred after downstream HTTP 200 SSE creation")
    if downstream_error is not None and sse_created is None:
        failures.append("post-commit SSE error was emitted without downstream SSE creation")
    if terminal is not None and downstream_error is not None:
        failures.append("both a downstream terminal event and downstream_sse_error were observed in one lifecycle window")

    terminal_event = last_event(events, "downstream_terminal_event")
    terminal_status = terminal_event.response_status if terminal_event else None

    if request_error is not None and sse_created is None:
        classification = "PRE_COMMIT_HTTP_ERROR"
        transport = first_index(events, "upstream_http_transport_error")
        rejected = first_index(events, "upstream_http_rejected")
        if transport is not None and transport > request_error:
            failures.append("upstream transport error was logged after request_error_response")
        if rejected is not None and rejected > request_error:
            failures.append("upstream HTTP rejection was logged after request_error_response")
    elif downstream_error is not None:
        classification = "POST_COMMIT_STREAM_ERROR"
    elif terminal is not None:
        if terminal_status == "completed":
            classification = "NORMAL_COMPLETION"
            if not ordered(events, NORMAL_ORDERED):
                failures.append("normal completion phases were missing or out of order")
        elif terminal_status == "failed":
            classification = "TERMINAL_RESPONSE_FAILED"
        elif terminal_status == "incomplete":
            classification = "TERMINAL_RESPONSE_INCOMPLETE"
        else:
            classification = "TERMINAL_RESPONSE_UNKNOWN"
            warnings.append(f"terminal response_status was {terminal_status!r}")
    else:
        classification = "INCOMPLETE_OR_STILL_RUNNING"
        warnings.append("no request_error_response, downstream_sse_error, or downstream_terminal_event was observed")

    if failures:
        verdict = "FAIL"
    elif classification == "INCOMPLETE_OR_STILL_RUNNING":
        verdict = "WARN"
    else:
        verdict = "PASS"

    return verdict, classification, failures, warnings


def expectation_matches(expect: str, classification: str) -> bool:
    if expect == "auto":
        return True
    mapping = {
        "normal": {"NORMAL_COMPLETION"},
        "precommit-error": {"PRE_COMMIT_HTTP_ERROR"},
        "postcommit-error": {"POST_COMMIT_STREAM_ERROR"},
        "terminal-failure": {"TERMINAL_RESPONSE_FAILED", "TERMINAL_RESPONSE_INCOMPLETE"},
    }
    return classification in mapping[expect]


def print_report(
    events: list[Event],
    verdict: str,
    classification: str,
    failures: list[str],
    warnings: list[str],
    expect: str,
    show_raw: bool,
) -> int:
    expectation_ok = expectation_matches(expect, classification)
    if not expectation_ok:
        verdict = "FAIL"
        failures = failures + [f"expected {expect!r}, observed {classification}"]

    print("=== Agentic API lifecycle validation ===")
    print(f"VERDICT       : {verdict}")
    print(f"CLASSIFICATION: {classification}")
    print(f"EVENTS        : {len(events)}")
    if expect != "auto":
        print(f"EXPECTATION   : {expect} ({'matched' if expectation_ok else 'MISMATCH'})")

    print("\nObserved lifecycle:")
    if not events:
        print("  (none)")
    else:
        for number, event in enumerate(events, start=1):
            details: list[str] = []
            if event.status:
                details.append(f"status={event.status}")
            if event.response_status:
                details.append(f"response_status={event.response_status}")
            if event.error_type:
                details.append(f"error_type={event.error_type}")
            if event.error_code:
                details.append(f"error_code={event.error_code}")
            suffix = f" [{' '.join(details)}]" if details else ""
            print(f"  {number:02d}. {event.phase}{suffix} (log line {event.line_no})")
            if show_raw:
                print(f"      {event.raw}")

    if failures:
        print("\nFailures:")
        for item in failures:
            print(f"  - {item}")
    if warnings:
        print("\nWarnings:")
        for item in warnings:
            print(f"  - {item}")

    if verdict == "PASS":
        return 0
    if verdict == "FAIL":
        return 1
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect and validate Agentic API phase= lifecycle logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Pull the last 5 minutes from one pod and classify automatically.
  python3 scripts/analyze-lifecycle-logs.py -n inference --pod agentic-api-abc123

  # Assert a normal Responses request.
  python3 scripts/analyze-lifecycle-logs.py -n inference --pod agentic-api-abc123 --since 2m --expect normal

  # Assert the pre-commit error contract.
  python3 scripts/analyze-lifecycle-logs.py -n inference --pod agentic-api-abc123 --since 2m --expect precommit-error

  # Assert a post-commit inference/stream failure.
  python3 scripts/analyze-lifecycle-logs.py -n inference --pod agentic-api-abc123 --since 2m --expect postcommit-error

  # Analyze previously captured logs.
  python3 scripts/analyze-lifecycle-logs.py --file /tmp/agentic.log

  # Or pipe logs directly.
  kubectl logs -n inference agentic-api-abc123 --since=2m | python3 scripts/analyze-lifecycle-logs.py
""",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--pod", help="pod name passed to kubectl logs")
    source.add_argument("--file", help="read logs from a local file")
    parser.add_argument("-n", "--namespace", help="Kubernetes namespace")
    parser.add_argument("-c", "--container", help="container name for kubectl logs")
    parser.add_argument("--since", default="5m", help="kubectl --since value (default: 5m)")
    parser.add_argument("--tail", type=int, default=2000, help="kubectl --tail value (default: 2000)")
    parser.add_argument("--previous", action="store_true", help="read previous container logs")
    parser.add_argument(
        "--expect",
        choices=["auto", "normal", "precommit-error", "postcommit-error", "terminal-failure"],
        default="auto",
        help="assert an expected lifecycle class (default: auto)",
    )
    parser.add_argument("--show-raw", action="store_true", help="print the raw log line for every lifecycle event")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        logs = collect_logs(args)
    except (OSError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    events = parse_events(logs.splitlines())
    verdict, classification, failures, warnings = validate(events)
    return print_report(events, verdict, classification, failures, warnings, args.expect, args.show_raw)


if __name__ == "__main__":
    raise SystemExit(main())
