"""Read-only accounting sources and deterministic job statistics."""

from collections import Counter, deque
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sqlite3
from time import monotonic

from .job_cpu import MICROSECONDS, NO_VAL, allocated_cpu, analyze_job_cpu
from .query_time import SHANGHAI, shanghai_iso
from .slurm_jobs import SlurmJobsError


ANOMALIES = {"FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE"}
LIVE = {"RUNNING", "SUSPENDED", "COMPLETING"}
BASIS_LABEL = {"submission": "提交时间", "start": "开始时间", "end": "结束时间", "execution": "运行区间相交"}
TIME_FIELD = {"submission": "submission", "start": "start", "end": "end"}
MIB = 1024 ** 2
NO_TRES_VALUE = 2 ** 64 - 2
HPC_WINDOW_SECONDS = 3600
HPC_RETRY_WINDOW_SECONDS = 900
HPC_TIMEOUT_LIMIT = 3
HPC_READ_TIMEOUT = 25
HPC_QUERY_BUDGET = 120
RETRYABLE_HPC_ERRORS = {"hpc_timeout", "hpc_upstream_unavailable", "hpc_response_too_large", "hpc_incomplete_response"}


def epoch(value):
    return value if type(value) is int and 0 < value < NO_VAL else None


def safe_label(value):
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value) else None


def slurm_integer(value):
    if isinstance(value, dict):
        value = value.get("number") if value.get("set") and not value.get("infinite") else None
    return value if type(value) is int and 0 <= value < NO_VAL else None


def job_key(job):
    timing = job.get("time") or {}
    array = job.get("array") or {}
    het = job.get("het") or {}
    return (safe_label(job.get("cluster")), job.get("job_id"), epoch(timing.get("submission")),
            slurm_integer(array.get("job_id")), slurm_integer(array.get("task_id")),
            slurm_integer(het.get("job_id")), slurm_integer(het.get("job_offset", het.get("offset"))))


@dataclass(frozen=True)
class JobMemorySchema:
    """Operator-verified accounting field for a whole-job concurrent peak in bytes."""

    peak_field: str
    allocation_unit_bytes: int

    def __post_init__(self):
        if (not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*", self.peak_field)
                or self.peak_field.split(".")[0] in {"steps", "tres"}
                or self.allocation_unit_bytes not in {10 ** 6, MIB}):
            raise ValueError("请配置经核实的作业级峰值字段，以及 MB 的字节数（1000000 或 1048576）。")

    def peak(self, job):
        value = job
        for key in self.peak_field.split("."):
            value = value.get(key) if isinstance(value, dict) else None
        return value if type(value) is int and 0 < value < 2 ** 63 else None


FIXTURE_MEMORY = JobMemorySchema("fixture_job_peak_bytes", MIB)


def tres_count(entries, resource):
    values = [item.get("count") for item in entries or [] if isinstance(item, dict)
              and item.get("type") == resource]
    return values[0] if len(values) == 1 and type(values[0]) is int and 0 < values[0] < NO_TRES_VALUE else None


def step_usage(step):
    ident = step.get("step", {}).get("id")
    suffix = ident.rsplit(".", 1)[-1] if isinstance(ident, str) else ""
    safe_id = ident if re.fullmatch(r"[0-9]+\.(?:[0-9]+|batch|extern|interactive)", ident or "") else None
    total = (step.get("time") or {}).get("total") or {}
    seconds, micros = total.get("seconds"), total.get("microseconds")
    cpu = seconds + micros / MICROSECONDS if all(type(x) is int and x >= 0 for x in (seconds, micros)) else None
    requested = (step.get("tres") or {}).get("requested") or {}
    maximum = requested.get("max") or []
    rss = max((entry["count"] for entry in maximum if entry.get("type") == "mem"
               and type(entry.get("count")) is int and 0 < entry["count"] < NO_TRES_VALUE), default=None)
    states = step.get("state") or []
    pending = "RUNNING" in states
    return {"step_id": safe_id, "kind": suffix if suffix in {"batch", "extern", "interactive"} else "ordinary",
            "state": safe_label(states[0]) if states else None, "allocated_cpus": allocated_cpu(step),
            "cpu_seconds": None if pending else cpu, "cpu_accounting_pending": pending,
            "task_max_rss_bytes": rss, "step_memory_total_bytes": tres_count(requested.get("total"), "mem")}


def accounting_memory(job, steps, *, completed, memory_schema=None):
    tres = job.get("tres") or {}
    allocation = tres_count(tres.get("allocated"), "mem")
    requested = tres_count(tres.get("requested"), "mem")
    user_steps = [step for step in steps if step["kind"] != "extern"]
    seff_peak = max((step["step_memory_total_bytes"] for step in steps if step["step_memory_total_bytes"]),
                    default=None) if completed else None
    seff_complete = bool(seff_peak and user_steps and all(step["step_memory_total_bytes"] and
                         not step["cpu_accounting_pending"] for step in user_steps))
    peak = memory_schema.peak(job) if memory_schema and completed else None
    numerator = peak if memory_schema else seff_peak if seff_complete else None
    unit = memory_schema.allocation_unit_bytes if memory_schema else MIB
    requested_bytes = requested * unit if requested else None
    allocated_bytes = allocation * unit if allocation else None
    return {
        "requested_memory_mb": requested, "allocated_memory_mb": allocation,
        "requested_memory_bytes": requested_bytes, "memory_allocated_bytes": allocated_bytes,
        "memory_used_bytes": numerator,
        "job_memory_peak_bytes": peak, "seff_memory_bytes": seff_peak, "seff_memory_complete": seff_complete,
        "seff_memory_allocated_ratio": seff_peak / (allocation * MIB) if seff_complete and allocation else None,
        "memory_basis": "concurrent_peak" if memory_schema else "seff",
        "memory_ratio": numerator / requested_bytes if numerator and requested_bytes else None,
        "memory_allocated_ratio": numerator / allocated_bytes if numerator and allocated_bytes else None,
        "memory_slack_bytes": max(0, requested_bytes - numerator) if numerator and requested_bytes else None,
    }


def normalize_job(job, *, observed=None, suspect_boundary=False, memory_schema=None, keep_steps=False):
    timing, tres = job.get("time") or {}, job.get("tres") or {}
    states = (job.get("state") or {}).get("current") or []
    state = safe_label(states[0]) if states else None
    exit_record = job.get("exit_code") or {}
    cpu = analyze_job_cpu(job)
    steps = [step_usage(step) for step in (job.get("steps") or []) if isinstance(step, dict)]
    start, end, submit = (epoch(timing.get(field)) for field in ("start", "end", "submission"))
    suspended = timing.get("suspended")
    active = (end - start - suspended if start and end and type(suspended) is int
              and 0 <= suspended <= end - start and not suspect_boundary else None)
    completed = states == ["COMPLETED"] and not suspect_boundary
    cpu_valid = cpu["availability"] == "valid" and not suspect_boundary
    return {
        "key": job_key(job), "job_id": job.get("job_id"), "cluster": safe_label(job.get("cluster")),
        "partition": safe_label(job.get("partition")), "state": state, "submit": submit, "start": start, "end": end,
        "state_reason": safe_label((job.get("state") or {}).get("reason")) if keep_steps else None,
        "exit_code": slurm_integer(exit_record.get("return_code")) if keep_steps else None,
        "observed": observed, "boundary_uncertain": suspect_boundary,
        "active_seconds": active, "elapsed_seconds": end - start if start and end and end > start and not suspect_boundary else None,
        "suspended_seconds": suspended if type(suspended) is int and suspended >= 0 else None,
        "allocated_cpus": allocated_cpu(job), "requested_cpus": tres_count(tres.get("requested"), "cpu"),
        "allocated_nodes": tres_count(tres.get("allocated"), "node"),
        "task_max_rss_bytes": max((step["task_max_rss_bytes"] for step in steps if step["task_max_rss_bytes"]), default=None),
        **accounting_memory(job, steps, completed=completed, memory_schema=memory_schema),
        "cpu_ratio": cpu["utilization"] if cpu_valid else None,
        "cpu_reason": "window_boundary" if suspect_boundary else cpu["reason"],
        "cpu_availability": "inconsistent" if suspect_boundary else cpu["availability"],
        "cpu_seconds": cpu["used_cpu_microseconds"] / MICROSECONDS if cpu_valid else None,
        "cpu_capacity": cpu["capacity_cpu_seconds"] if cpu_valid else None,
        "cpu_unused_seconds": cpu["unused_capacity_cpu_microseconds"] / MICROSECONDS if cpu_valid else None,
        "step_count": len(steps), "steps": steps if keep_steps else None,
        "zero_cpu_steps": sum(step["cpu_seconds"] == 0 and step["kind"] != "extern" for step in steps),
        "unstarted_sentinel": timing.get("start") == NO_VAL,
    }


def matches_time(row, period, basis):
    if period is None:
        return True
    if basis == "execution":
        start, end = row["start"], row["end"]
        if end is None and row["state"] in LIVE:
            end = row["observed"]
        return bool(start and end and start < period.end and end > period.start)
    value = row[{"submission": "submit", "start": "start", "end": "end"}[basis]]
    return value is not None and period.start <= value < period.end


class SQLiteArchive:
    def __init__(self, path):
        self.path = Path(path).resolve()

    def connect(self):
        return sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)

    def coverage(self, period):
        with closing(self.connect()) as db:
            windows = db.execute("SELECT start_ts, end_ts FROM windows WHERE status='ok' ORDER BY start_ts").fetchall()
        intervals = []
        for start, end in windows:
            if intervals and start <= intervals[-1][1]:
                intervals[-1][1] = max(intervals[-1][1], end)
            else:
                intervals.append([start, end])
        if period:
            seconds = sum(max(0, min(end, period.end) - max(start, period.start)) for start, end in intervals)
        else:
            seconds = sum(end - start for start, end in intervals)
        return {"window_count": len(windows), "captured_seconds": seconds,
                "first_capture": shanghai_iso(intervals[0][0]) if intervals else None,
                "last_capture": shanghai_iso(intervals[-1][1]) if intervals else None,
                "requested_seconds": period.end - period.start if period else None,
                "covers_sampling_interval": bool(period and seconds == period.end - period.start),
                "archive_modified_at": shanghai_iso(self.path.stat().st_mtime), "complete": False}

    def event_bounds(self):
        aggregates = []
        for field in TIME_FIELD.values():
            path = f"json_extract(payload,'$.time.{field}')"
            timestamp = f"CASE WHEN {path} BETWEEN 1 AND {NO_VAL - 1} THEN {path} END"
            aggregates.extend((f"min({timestamp})", f"max({timestamp})"))
        with closing(self.connect()) as db:
            count, *bounds = db.execute("SELECT count(*), " + ", ".join(aggregates) + " FROM jobs").fetchone()
        events = {basis: {"earliest": shanghai_iso(earliest) if earliest else None,
                         "latest": shanghai_iso(latest) if latest else None}
                  for basis, earliest, latest in zip(TIME_FIELD, bounds[::2], bounds[1::2])}
        return {"archived_jobs": count, "event_bounds": events}

    def records(self, period=None, basis="execution", job_ids=(), partition=None):
        where, args = [], []
        if job_ids:
            placeholders = ",".join("?" for _ in job_ids)
            where.append(f"json_extract(payload, '$.job_id') IN ({placeholders})")
            args.extend(job_ids)
        if partition:
            where.append("json_extract(payload, '$.partition') = ? COLLATE NOCASE")
            args.append(partition)
        if period and basis in TIME_FIELD:
            field = TIME_FIELD[basis]
            where.append(f"json_extract(payload, '$.time.{field}') BETWEEN ? AND ?")
            args.extend((period.start, period.end - 1))
        if period and basis == "execution":
            where.append("json_extract(payload,'$.time.start') BETWEEN 1 AND ?")
            where.append("(json_extract(payload,'$.time.end') > ? OR last_window >= ?)")
            args.extend((period.end - 1, period.start, period.start))
        filters = " WHERE " + " AND ".join(where) if where else ""
        with closing(self.connect()) as db:
            for payload, last_window, observations in db.execute(
                "SELECT payload, last_window, observations FROM jobs" + filters, args
            ):
                yield json.loads(payload), last_window, observations


def accounting_snapshot(job, memory_schema=None):
    """Keep accounting fields used by metrics, identity, and drill-down."""
    fields = {"job_id", "cluster", "partition", "state", "array", "het", "flags", "restart_cnt",
              "time", "tres", "exit_code", "steps", "nodes", "node_list", "tres_alloc_str", "tres_req_str"}
    if memory_schema:
        fields.add(memory_schema.peak_field.split(".")[0])
    snapshot = {name: job[name] for name in fields & job.keys()}
    if isinstance(snapshot.get("steps"), list):
        steps = []
        for step in snapshot["steps"]:
            if not isinstance(step, dict):
                steps.append(None)
                continue
            tres = step.get("tres") or {}
            requested = tres.get("requested") or {}
            evidence_tres = {
                "allocated": tres.get("allocated"),
                "requested": {
                    "total": [entry for entry in requested.get("total") or [] if entry.get("type") in {"cpu", "mem"}],
                    "max": [entry for entry in requested.get("max") or [] if entry.get("type") == "mem"],
                },
            }
            steps.append({"step": {"id": (step.get("step") or {}).get("id")}, "state": step.get("state"),
                          "time": step.get("time"), "tres": evidence_tres})
        snapshot["steps"] = steps
    return snapshot


def remote_records(source, period, progress, *, resume=None, budget=HPC_QUERY_BUDGET, memory_schema=None):
    """Read bounded accounting windows and retain evidence and gaps for a later continuation."""
    previous_versions, previous_coverage = resume or ({}, {})
    versions = {key: [version | {"windows": list(version["windows"])} for version in history]
                for key, history in previous_versions.items()}
    timed_out, failed = [], []
    retry_windows = deque(previous_coverage.get("failed_windows", ()))
    completed_before = previous_coverage.get("windows_completed", 0)
    warnings = previous_coverage.get("warnings", 0)
    covered_seconds = previous_coverage.get("covered_seconds", 0)
    completed, consecutive_errors, point_retries = completed_before, 0, 0
    cursor = previous_coverage.get("next_start", period.start)
    window_size = previous_coverage.get("window_seconds", HPC_WINDOW_SECONDS)
    deadline = monotonic() + budget
    error = last_failure = None

    def update(message):
        total = completed + len(failed) + len(retry_windows) + (period.end - cursor + window_size - 1) // window_size
        progress(message, completed=completed, total=total, timed_out=len(timed_out))
        return total

    if resume:
        update(f"继续查询，复用 {completed_before} 个已成功的窗口。")
    while retry_windows or cursor < period.end:
        remaining = deadline - monotonic()
        if remaining <= 0:
            error = {"code": "hpc_query_budget", "message": "本轮查询已到时间上限，已保留成功窗口，可以继续查询剩余范围。"}
            break
        if retry_windows:
            start, end = retry_windows[0]
        else:
            start, end = cursor, min(cursor + window_size, period.end)
        update(f"正在查询 {shanghai_iso(start)[11:19]}–{shanghai_iso(end)[11:19]} 的 HPC 作业记账…")
        response = None
        try:
            response = source.read_accounting_window(start, end, min(HPC_READ_TIMEOUT, remaining))
        except SlurmJobsError as failure:
            if failure.code not in RETRYABLE_HPC_ERRORS:
                error = {"code": failure.code, "message": str(failure)}
                break
            if end - start > HPC_RETRY_WINDOW_SECONDS:
                window_size = HPC_RETRY_WINDOW_SECONDS
                update(str(failure) + " 改为每段最多 15 分钟继续。")
                continue
            if period.point and point_retries == 0 and failure.code != "hpc_response_too_large":
                point_retries += 1
                update("单时刻记账请求暂时失败，正在重试。")
                continue
            last_failure = {"code": failure.code, "message": str(failure)}
            if failure.code == "hpc_timeout":
                timed_out.append({"start": start, "end": end})
            failed.append((start, end))
            consecutive_errors += 1
        if retry_windows:
            retry_windows.popleft()
        else:
            cursor = end
        if response is None:
            if consecutive_errors >= HPC_TIMEOUT_LIMIT and (retry_windows or cursor < period.end):
                error = {"code": last_failure["code"], "message": f"HPC 连续 {HPC_TIMEOUT_LIMIT} 个短窗口读取失败，已暂停查询。"}
                break
            continue
        warnings += response.warning_count
        for record in response.jobs:
            if type(record.get("job_id")) is not int:
                continue
            job = accounting_snapshot(record, memory_schema)
            timing = job.get("time") or {}
            boundary = timing.get("start") in {start, end} or timing.get("end") in {start, end}
            key = job_key(job)
            history = versions.setdefault(key, [])
            if history and history[-1]["job"] == job and history[-1]["boundary"] == boundary:
                history[-1]["windows"].append(start)
            else:
                history.append({"job": job, "boundary": boundary, "windows": [start]})
            history[-1]["fetched_at"] = response.fetched_at
        completed += 1
        covered_seconds += end - start
        consecutive_errors = 0
    total = update("本轮窗口读取结束，正在整理已返回的记录。")
    return versions, {"windows_completed": completed, "windows_total": total, "warnings": warnings,
                      "covered_seconds": covered_seconds, "requested_seconds": period.end - period.start,
                      "windows_reused": completed_before, "timed_out_windows": timed_out,
                      "failed_windows": failed + list(retry_windows), "next_start": cursor, "window_seconds": window_size,
                      "error": error or last_failure, "complete": False}


def summarize(rows):
    count = len(rows)
    states = Counter(row["state"] or "缺失" for row in rows)
    cpu = [row for row in rows if row["cpu_ratio"] is not None]
    memory = [row for row in rows if row["memory_ratio"] is not None]
    peaks = [row["job_memory_peak_bytes"] for row in rows if row["job_memory_peak_bytes"] is not None]
    estimates = [row["seff_memory_bytes"] for row in rows if row["seff_memory_complete"]]
    requests = [row["requested_memory_mb"] for row in rows if row["requested_memory_mb"] is not None]
    allocations = [row["allocated_memory_mb"] for row in rows if row["allocated_memory_mb"] is not None]
    cpu_requests = [row["requested_cpus"] for row in rows if row["requested_cpus"] is not None]
    cpu_allocations = [row["allocated_cpus"] for row in rows if row["allocated_cpus"] is not None]
    return {
        "count": count, "states": [{"state": s, "count": n, "ratio": n / count}
                                   for s, n in sorted(states.items(), key=lambda item: (-item[1], item[0]))],
        "cpu_evaluated": len(cpu), "memory_evaluated": len(memory),
        "cpu_missing": count - len(cpu), "memory_missing": count - len(memory),
        "cpu_seconds": sum(row["cpu_seconds"] for row in cpu) if cpu else None,
        "cpu_capacity": sum(row["cpu_capacity"] for row in cpu) if cpu else None,
        "cpu_requested": sum(cpu_requests) if cpu_requests else None, "cpu_requested_samples": len(cpu_requests),
        "cpu_allocated": sum(cpu_allocations) if cpu_allocations else None, "cpu_allocated_samples": len(cpu_allocations),
        "memory_requested_mb": sum(requests) if requests else None, "memory_requested_samples": len(requests),
        "memory_allocated_mb": sum(allocations) if allocations else None, "memory_allocated_samples": len(allocations),
        "task_rss_samples": sum(row["task_max_rss_bytes"] is not None for row in rows),
        "job_memory_peak_samples": len(peaks), "peak_memory_bytes_total": sum(peaks) if peaks else None,
        "seff_memory_samples": len(estimates), "seff_memory_bytes_total": sum(estimates) if estimates else None,
        "anomaly_count": sum(row["state"] in ANOMALIES for row in rows),
    }


def data_quality(rows):
    steps_present = sum(row["step_count"] > 0 for row in rows)
    return {
        "steps_present": steps_present,
        "steps_ratio": steps_present / len(rows) if rows else None,
        "zero_recorded": sum(row["zero_cpu_steps"] > 0 for row in rows),
        "never_started": sum(row["unstarted_sentinel"] for row in rows),
        "accounting_pending": sum(row["state"] in LIVE | {"PENDING", "CONFIGURING"} for row in rows),
        "cpu_reasons": dict(Counter(row["cpu_reason"] for row in rows if row["cpu_reason"])),
        "missing_fields": {name: sum(row[name] is None for row in rows) for name in
                           ("submit", "start", "end", "state", "allocated_cpus", "requested_memory_mb",
                            "task_max_rss_bytes", "seff_memory_bytes", "job_memory_peak_bytes")},
    }


def daily_counts(rows, period, basis):
    day = datetime.fromtimestamp(period.start, SHANGHAI).replace(hour=0, minute=0, second=0, microsecond=0)
    while int(day.timestamp()) < period.end:
        tomorrow = day + timedelta(days=1)
        segment = type(period)(max(period.start, int(day.timestamp())), min(period.end, int(tomorrow.timestamp())))
        yield {"date": day.date().isoformat(), "count": sum(matches_time(row, segment, basis) for row in rows)}
        day = tomorrow
