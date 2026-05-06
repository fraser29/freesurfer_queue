#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple


QUEUE_STATES = ["queued", "running", "failed", "done", "timeout"]
SUCCESS_MARKERS = (
    "finished without error",
    "completed without error",
    "without error",
)
FAILURE_PATTERNS = {
    "missing_log": ("missing log",),
    "out_of_memory": ("out of memory", "killed", "oom"),
    "segfault": ("segmentation fault", "segfault"),
    "input_error": ("no such file", "file not found", "cannot open"),
    "permission_error": ("permission denied",),
    "license_error": ("license",),
    "unknown_failure": (),
}


def parse_env_file(env_path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def discover_queue_root(cli_queue_root: Optional[Path]) -> Path:
    if cli_queue_root is not None:
        return cli_queue_root
    env_path = Path(".env")
    if env_path.exists():
        env_values = parse_env_file(env_path)
        queue_root = env_values.get("QUEUE_ROOT")
        if queue_root:
            return Path(queue_root)
    return Path("fs_queue")


def list_jobs(state_dir: Path) -> List[Path]:
    if not state_dir.exists() or not state_dir.is_dir():
        return []
    return sorted([p for p in state_dir.iterdir() if p.is_dir()], key=lambda p: p.name)


def read_log_text(job_dir: Path) -> Optional[str]:
    logfile = job_dir / "recon.log"
    if not logfile.exists():
        return None
    try:
        with open(logfile, "r", encoding="utf-8", errors="ignore") as f:
            return f.read().lower()
    except OSError:
        return None


def classify_failure(log_text: Optional[str]) -> str:
    if not log_text:
        return "missing_log"
    for reason, patterns in FAILURE_PATTERNS.items():
        if reason == "unknown_failure":
            continue
        if any(pattern in log_text for pattern in patterns):
            return reason
    return "unknown_failure"


def has_success_marker(log_text: Optional[str]) -> bool:
    if not log_text:
        return False
    return any(marker in log_text for marker in SUCCESS_MARKERS)


def read_runtime_meta(job_dir: Path) -> Optional[dict]:
    runtime_path = job_dir / "runtime.json"
    if not runtime_path.exists():
        return None
    try:
        with open(runtime_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def summarize_overview(queue_root: Path) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for state in QUEUE_STATES:
        counts[state] = len(list_jobs(queue_root / state))
    return counts


def summarize_full(queue_root: Path) -> Tuple[Dict[str, int], Counter, List[str], List[str]]:
    overview = summarize_overview(queue_root)
    failure_reasons: Counter = Counter()
    failed_jobs = list_jobs(queue_root / "failed")
    for job_dir in failed_jobs:
        reason = classify_failure(read_log_text(job_dir))
        failure_reasons[reason] += 1

    done_without_marker: List[str] = []
    done_with_marker: List[str] = []
    for job_dir in list_jobs(queue_root / "done"):
        if has_success_marker(read_log_text(job_dir)):
            done_with_marker.append(job_dir.name)
        else:
            done_without_marker.append(job_dir.name)
    return overview, failure_reasons, done_with_marker, done_without_marker


def print_overview(queue_root: Path, counts: Dict[str, int]) -> None:
    print(f"Queue root: {queue_root}")
    print("Overview")
    for state in QUEUE_STATES:
        print(f"- {state}: {counts[state]}")


def print_full(
    queue_root: Path,
    counts: Dict[str, int],
    failure_reasons: Counter,
    done_with_marker: List[str],
    done_without_marker: List[str],
) -> None:
    print_overview(queue_root, counts)
    print("\nFailure reasons")
    if not failure_reasons:
        print("- none")
    else:
        for reason, count in failure_reasons.most_common():
            print(f"- {reason}: {count}")

    print("\nDone integrity check")
    print(f"- done jobs with success marker: {len(done_with_marker)}")
    print(f"- done jobs missing success marker: {len(done_without_marker)}")
    if done_without_marker:
        print("- done jobs missing marker list:")
        for job_name in done_without_marker:
            print(f"  - {job_name}")

    running_missing_runtime = []
    for job_dir in list_jobs(queue_root / "running"):
        if read_runtime_meta(job_dir) is None:
            running_missing_runtime.append(job_dir.name)
    print("\nRunning integrity check")
    print(f"- running jobs missing runtime.json: {len(running_missing_runtime)}")
    if running_missing_runtime:
        for job_name in running_missing_runtime:
            print(f"  - {job_name}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect FreeSurfer queue and summarize run outcomes."
    )
    parser.add_argument(
        "--mode",
        choices=["overview", "full"],
        default="overview",
        help="overview: counts by state; full: includes failure analysis and done validation",
    )
    parser.add_argument(
        "--queue-root",
        type=Path,
        default=None,
        help="Queue root directory (defaults to QUEUE_ROOT in .env, else fs_queue)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    queue_root = discover_queue_root(args.queue_root)
    if not queue_root.exists() or not queue_root.is_dir():
        raise SystemExit(f"Queue root does not exist or is not a directory: {queue_root}")

    if args.mode == "overview":
        counts = summarize_overview(queue_root)
        print_overview(queue_root, counts)
        return

    counts, failure_reasons, done_with_marker, done_without_marker = summarize_full(queue_root)
    print_full(queue_root, counts, failure_reasons, done_with_marker, done_without_marker)


if __name__ == "__main__":
    main()
