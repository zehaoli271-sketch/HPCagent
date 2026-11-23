"""Historical job membership and CPU screening calculations."""

from collections import Counter
from math import isfinite

from .job_cpu import NO_VAL, allocated_cpu


WINDOW_SECONDS = 300
MIN_WINDOW_COVERAGE = 0.8
DEFAULT_CPU_THRESHOLD = 0.2


def _timestamp(value):
    return type(value) is int and 0 < value < NO_VAL


def find_jobs_running_at(records, at):
    """Use half-open run intervals and report uncertain records separately."""
    jobs = []
    uncertain = 0
    for job in records:
        timing = job.get("time") or {}
        start, end = timing.get("start"), timing.get("end")
        state = (job.get("state") or {}).get("current", [])
        if start == NO_VAL or (_timestamp(start) and start > at) or (_timestamp(end) and end <= at):
            continue
        if start in (None, 0) and "PENDING" in state:
            continue
        flags = job.get("flags")
        if (
            not _timestamp(start)
            or (not _timestamp(end) and not (end == 0 and state == ["RUNNING"]))
            or type(job.get("restart_cnt")) is not int or job["restart_cnt"] != 0
            or not isinstance(flags, list) or {"RESIZED", "RESIZING", "REQUEUED"}.intersection(flags)
            or timing.get("suspended") != 0 or "SUSPENDED" in state
            or type(job.get("job_id")) is not int or job["job_id"] <= 0
        ):
            uncertain += 1
            continue
        jobs.append(job)

    counts = Counter(job["job_id"] for job in jobs)
    unique = [job for job in jobs if counts[job["job_id"]] == 1]
    return unique, uncertain + len(jobs) - len(unique)


def get_job_cpu_usage_window(job, samples, start, end):
    """Measure the observed subinterval of a job-wide CPU-seconds counter."""
    if (job.get("het") or {}).get("job_id"):
        return {"utilization": None, "reason": "异构作业需要组件级监控与分配记录。"}
    cpus = allocated_cpu(job)
    if cpus is None:
        return {"utilization": None, "reason": "缺少可信的实际分配 CPU 数。"}
    if not samples:
        return {"utilization": None, "reason": "缺少唯一且可归属到该作业的 CPU 监控序列。"}
    points = []
    try:
        for timestamp, value in samples:
            timestamp, value = float(timestamp), float(value)
            if not isfinite(timestamp) or not isfinite(value) or value < 0:
                raise ValueError
            if start <= timestamp <= end:
                points.append((timestamp, value))
    except (ValueError, TypeError, OverflowError):
        return {"utilization": None, "reason": "CPU 监控样本无效。"}

    if len(points) < 2 or points[-1][0] - points[0][0] < (end - start) * MIN_WINDOW_COVERAGE:
        return {"utilization": None, "reason": "CPU 监控样本覆盖不足，无法评估该窗口。"}
    if points[0][0] < job["time"]["start"]:
        return {"utilization": None, "reason": "监控序列早于该次作业启动，归属待核实。"}
    if any(new_t <= old_t or new_v < old_v
           for (old_t, old_v), (new_t, new_v) in zip(points, points[1:])):
        return {"utilization": None, "reason": "监控样本时序异常或 CPU 计数器发生重置。"}

    duration = points[-1][0] - points[0][0]
    used = points[-1][1] - points[0][1]
    capacity = cpus * duration
    if used > capacity:
        return {"utilization": None, "reason": "窗口 CPU 用量超过分配容量，需要复核。"}
    return {
        "utilization": round(used / capacity, 8),
        "cpu_seconds": round(used, 3),
        "capacity_cpu_seconds": round(capacity, 3),
        "observed_start": points[0][0],
        "observed_end": points[-1][0],
        "reason": None,
    }


def screen_low_efficiency_jobs(rows, threshold, *, top_n=None):
    """Return a ranked subset or threshold candidates from available metrics."""
    scored = [row for row in rows if row["utilization"] is not None]
    eligible = scored if top_n is not None else [row for row in scored if row["utilization"] < threshold]
    eligible.sort(key=lambda row: (row["utilization"], int(row["job_id"])))
    candidates = eligible[:top_n]
    return {
        "threshold": threshold if top_n is None else None,
        "top_n": top_n,
        "evaluated_count": len(scored),
        "unavailable_count": len(rows) - len(scored),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "data_gaps": dict(Counter(row["reason"] for row in rows if row["utilization"] is None)),
    }
