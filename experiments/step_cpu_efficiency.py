"""Fetch a small accounting window and calculate completed-step CPU efficiency."""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from getpass import getpass
import json
import os
from pathlib import Path
import time

import requests

BASE = "https://api.hpc.sjtu.edu.cn"
TZ = timezone(timedelta(hours=8))


def calculate_step(job_id, step):
    """Return a candidate and its exclusion reason; zero CPU is unverified data."""
    step_id = str(step["step"]["id"])
    if "COMPLETED" not in step["state"]:
        return None, "step_not_completed"
    if step_id.lower().rsplit(".", 1)[-1] == "extern":
        return None, "extern_step"

    cpu_allocations = [r["count"] for r in step["tres"]["allocated"] if r["type"] == "cpu"]
    if len(cpu_allocations) != 1 or cpu_allocations[0] <= 0:
        return None, "cpu_allocation_unavailable"
    cpus = cpu_allocations[0]
    elapsed = step["time"]["elapsed"]
    if elapsed <= 0:
        return None, "elapsed_not_positive"

    times = step["time"]
    if not all(key in times and "seconds" in times[key] and "microseconds" in times[key]
               for key in ("total", "user", "system")):
        return None, "cpu_time_missing"
    seconds = {
        key: Decimal(str(times[key]["seconds"]))
        + Decimal(str(times[key]["microseconds"])) / Decimal(1_000_000)
        for key in ("total", "user", "system")
    }
    if seconds["total"] <= 0:
        return None, "cpu_time_zero_or_invalid"
    capacity = Decimal(cpus) * Decimal(elapsed)
    difference = abs(seconds["total"] - seconds["user"] - seconds["system"])
    if difference > Decimal("0.001"):
        return None, "total_differs_from_user_plus_system"
    if seconds["total"] > capacity:
        return None, "cpu_time_exceeds_allocated_capacity"

    ratio = seconds["total"] / capacity
    return {
        "job_id": job_id,
        "step_id": step_id,
        "allocated_cpus": cpus,
        "elapsed_seconds": elapsed,
        "cpu_time_seconds": {key: str(value) for key, value in seconds.items()},
        "allocated_cpu_seconds": str(capacity),
        "efficiency_percent": str(ratio * 100),
        "average_busy_cpu_equivalents": str(seconds["total"] / Decimal(elapsed)),
        "checks": {
            "job_and_step_completed": True,
            "non_extern": True,
            "total_matches_user_plus_system_within_1ms": True,
            "cpu_time_not_above_capacity": True,
        },
        "source_fields": {
            "cpus": "jobs[].steps[].tres.allocated[type=cpu].count",
            "elapsed": "jobs[].steps[].time.elapsed",
            "cpu_time": "jobs[].steps[].time.total.seconds + microseconds / 1000000",
        },
    }, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=5)
    parser.add_argument("--proxy", default="http://127.0.0.1:7900")
    args = parser.parse_args()
    if args.minutes <= 0:
        parser.error("--minutes must be positive")
    username = os.environ.get("HPC_USERNAME") or input("JAccount username: ")
    password = os.environ.get("HPC_PASSWORD") or getpass("JAccount password: ")

    with requests.Session() as session:
        session.trust_env = False
        session.proxies.update({"http": args.proxy, "https": args.proxy})
        auth = session.post(BASE + "/token", json={
            "domain": "jaccount", "operator": username, "password": password,
        }, timeout=25)
        auth.raise_for_status()
        session.headers["Authorization"] = auth.text.strip()
        end = int(time.time())
        params = {"start_time": end - args.minutes * 60, "end_time": end}
        response = session.get(BASE + "/slurmdb/jobs", params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

    if data.get("errors"):
        raise RuntimeError("Slurm returned errors; no efficiency report generated")
    completed = [j for j in data["jobs"] if "COMPLETED" in j["state"]["current"]]
    candidates = []
    exclusions = Counter()
    for job in completed:
        for step in job["steps"]:
            candidate, reason = calculate_step(job["job_id"], step)
            if reason:
                exclusions[reason] += 1
            else:
                candidates.append(candidate)
    candidates.sort(key=lambda c: (c["job_id"], c["step_id"]))
    preferred = [c for c in candidates if c["elapsed_seconds"] >= 60]
    example = (preferred or candidates)[0] if candidates else None
    report = {
        "checked_at": datetime.now(TZ).isoformat(),
        "endpoint": BASE + "/slurmdb/jobs",
        "query": params,
        "http_status": response.status_code,
        "job_count": len(data["jobs"]),
        "completed_job_count": len(completed),
        "completed_job_step_count": sum(len(j["steps"]) for j in completed),
        "arithmetic_eligible_step_count": len(candidates),
        "excluded_step_counts": dict(exclusions),
        "warning_count": len(data.get("warnings", [])),
        "example_selection": "First by job_id then step_id, preferring elapsed >= 60 seconds",
        "example": example,
        "limitations": [
            "Step-level arithmetic only; accounting completeness still requires independent verification.",
            "Zero CPU time is excluded, not classified as zero utilization.",
            "No job-level aggregation or prediction of savings.",
            "CPU occupancy is not a measurement of useful computational work.",
            "No credentials, usernames, commands, paths, or raw job responses are saved.",
        ],
    }
    folder = Path(__file__).resolve().parent.parent / "reports"
    folder.mkdir(exist_ok=True)
    output = folder / (datetime.now(TZ).strftime("%Y-%m-%d_%H%M%S") + "_cpu_efficiency.json")
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("REPORT:", output)


if __name__ == "__main__":
    main()
