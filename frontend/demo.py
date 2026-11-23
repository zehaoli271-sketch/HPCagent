"""Synthetic jobs for the credential-free web demo."""

from datetime import datetime, timedelta
import re

from core.query_time import SHANGHAI


def _job(job_id, start, end, used, partition="example-cpu"):
    elapsed = end - start
    allocation = [{"type": "cpu", "count": 8}]
    return {
        "job_id": job_id, "cluster": "synthetic", "partition": partition,
        "state": {"current": ["COMPLETED"]}, "flags": [], "restart_cnt": 0,
        "time": {"start": start, "end": end, "elapsed": elapsed, "suspended": 0},
        "tres": {"allocated": allocation},
        "steps": [{
            "step": {"id": f"{job_id}.batch"}, "state": ["COMPLETED"], "tres": {"allocated": allocation},
            "time": {"elapsed": elapsed, "total": {"seconds": used, "microseconds": 0}},
        }],
    }


def demo_jobs(at):
    jobs = [
        _job(10001, at - 1200, at + 600, 1800),
        _job(10002, at - 1200, at + 600, 9000),
        _job(10003, at - 1200, at + 600, 720, "example-batch"),
        _job(10004, at - 900, at, 1600),
        _job(10005, at + 100, at + 700, 3000),
    ]
    for job, used_gib in zip(jobs, (2, 6, 1, 3, 4)):
        job["tres"]["allocated"].append({"type": "mem", "count": 8192})
        job["tres"]["requested"] = [{"type": "cpu", "count": 8}, {"type": "mem", "count": 10240}]
        job["steps"][0]["tres"]["requested"] = {"total": [{"type": "mem", "count": used_gib * 1024 ** 3}]}
    return jobs


def demo_monitor(ids, start, end):
    used_per_second = {10001: 1.0, 10002: 5.2}
    return {job_id: [(start, "1000"), (end, str(1000 + used_per_second[job_id] * (end - start)))]
            for job_id in ids if job_id in used_per_second}


def demo_cgroup_response(params):
    query = params["query"]
    if "step" in params:
        step = params["step"]
        result = []
        for host in sorted(set(re.findall(r'hostname="(demo0[12])"', query))):
            labels = {"hostname": host, "cgroup": "/slurm/uid_1000/job_10006"}
            for resource in ("cpu", "memory"):
                values, times = [], []
                for evaluated in range(params["start"], params["end"] + 1, step):
                    stamp = evaluated - evaluated % 15
                    value = stamp - 1_700_000_000 if resource == "cpu" else 1024 ** 3
                    values.append([evaluated, str(value)])
                    times.append([evaluated, str(stamp)])
                result.extend((
                    {"metric": {**labels, "hpc_sample": resource}, "values": values},
                    {"metric": {**labels, "hpc_sample": f"{resource}_time"}, "values": times},
                ))
        return {"status": "success", "data": {"resultType": "matrix", "result": result}}
    hostname = re.search(r'hostname="(demo0[12])"', query)
    group = "/slurm/uid_1000/job_10006"
    at = params["time"]
    metric = query.split("{", 1)[0]
    labels = {"hostname": hostname[1], "cgroup": group, "jobid": "10006"} if hostname else {}
    if metric == "cgroup_info":
        return {"status": "success", "data": {"resultType": "vector",
                "result": [{"metric": labels, "value": [at, "1"]}] if labels else []}}
    values = [[stamp, str(stamp - 1_700_000_000 if metric == "cgroup_cpu_total_seconds" else 1024 ** 3)]
              for stamp in range(at - 300, at + 1, 15)]
    return {"status": "success", "data": {"resultType": "matrix",
            "result": [{"metric": labels, "values": values}] if labels else []}}


def chat_demo_jobs(now):
    today = datetime.fromtimestamp(now, SHANGHAI).replace(hour=0, minute=0, second=0, microsecond=0)
    jobs = []
    for days_ago in range(1, 16):
        day = today - timedelta(days=days_ago)
        for index in range(8):
            start = int((day + timedelta(hours=14, minutes=index * 10)).timestamp())
            elapsed = (index + 1) * 600
            fraction = (0.02, 0.03, 0.04, 0.05, 0.08, 0.7, 0, 0.2)[index]
            job = _job(20000 + days_ago * 10 + index, start, start + elapsed, int(8 * elapsed * fraction))
            job["time"]["submission"] = start - 300
            job["tres"]["requested"] = [{"type": "mem", "count": 8192}, {"type": "cpu", "count": 8}]
            job["tres"]["allocated"] += [{"type": "mem", "count": 8192}, {"type": "node", "count": 1}]
            job["fixture_job_peak_bytes"] = (index + 1) * 128 * 1024 ** 2
            if index == 7:
                job["state"]["current"] = ["OUT_OF_MEMORY"]
                del job["fixture_job_peak_bytes"]
            jobs.append(job)
    return jobs
