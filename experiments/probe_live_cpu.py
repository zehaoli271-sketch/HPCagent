"""Verify a real single-node job CPU counter without exporting credentials or Slurm records."""

import argparse
from collections import Counter
import json
from math import ceil, isfinite
import os
from pathlib import Path
import socket
from statistics import median
from threading import Event, Lock, Thread
from time import monotonic, time
from urllib.parse import urlsplit

from core.job_monitor import CPU_METRIC, PrometheusRateLimit, SAMPLE_MAX_AGE, read_job_cpu_counter, resolve_job_cgroup
from core.live_collection import LiveCollector, MIN_LIVE_CPU_SECONDS, utc_iso
from core.job_screening import WINDOW_SECONDS, get_job_cpu_usage_window
from core.slurm_jobs import SlurmJobsError
from frontend import server as web


PROBE_LOOKBACK = 2 * WINDOW_SECONDS


def read_allocations(source):
    collector = LiveCollector(source.running_catalog, source.prometheus_json)
    collector.refresh_catalog()
    if not collector.latest_catalog:
        error = collector.error or collector.catalog_error
        raise SlurmJobsError(error["code"], error["message"])
    return collector.latest_catalog[:2]


def probe(source, publish, stop):
    attempts = []
    current = {}

    def record(data):
        current.clear()
        current.update(data)
        publish(data)

    def wait_until(stamp):
        if stop.is_set() or stop.wait(max(0, stamp - time())):
            raise SlurmJobsError("stopped", "单作业实验已停止。")

    try:
        first_at, catalog = read_allocations(source)
        candidates = [job for job in catalog if job.stable and job.cpus and job.started
                      and first_at - job.started >= WINDOW_SECONDS and len(job.hosts) == 1]
        host_counts = Counter(job.hosts[0] for job in candidates)
        distinct_hosts = {}
        for job in sorted(candidates, key=lambda job: (-host_counts[job.hosts[0]], -job.started)):
            distinct_hosts.setdefault(job.hosts[0], job)
        jobs = list(distinct_hosts.values())[:6]
        if not jobs:
            raise SlurmJobsError("no_eligible_job", "清单中没有符合条件的单节点运行作业。")
        record({"status": "checking_cpu", "catalog_at": utc_iso(first_at), "candidate_nodes": len(jobs), "attempts": attempts})

        def request(path, params):
            for retry in range(4):
                wait_until(time())
                try:
                    return source.prometheus_json(path, params, timeout=15)
                except SlurmJobsError as error:
                    if error.code not in {"hpc_rate_limited", "monitor_budget"} or retry == 3:
                        raise
                    resume_at = time() + getattr(error, "retry_after", 60) + 1
                    publish({**current, "paused_by_gateway": True, "resume_at": utc_iso(resume_at)})
                    wait_until(resume_at)

        def read(job, path, start, end):
            points = read_job_cpu_counter(request, job.hosts[0], path, start, end)
            left, right = points[0], points[-1]
            if right[0] - left[0] < MIN_LIVE_CPU_SECONDS:
                raise SlurmJobsError("job_cpu_short_window", "CPU 计数样本覆盖不足一分钟。")
            cpu = get_job_cpu_usage_window(job.cpu_record(), points, left[0], right[0])
            if cpu["utilization"] is None:
                raise SlurmJobsError("job_cpu_invalid", cpu["reason"])
            return points, cpu

        selected = None
        for index, job in enumerate(jobs, start=1):
            at = int(time())
            started = monotonic()
            try:
                path = resolve_job_cgroup(request, job.job_id, job.hosts[0], at)
                points, cpu = read(job, path, max(job.started, at - PROBE_LOOKBACK), at)
                age = at - points[-1][0]
                attempts.append({"candidate": index, "status": "counter_found", "samples": len(points),
                                 "latest_sample_delay_seconds": round(age, 2),
                                 "sample_span_seconds": round(points[-1][0] - points[0][0], 2),
                                 "seconds": round(monotonic() - started, 2)})
                selected = job, path, points, cpu
                break
            except SlurmJobsError as error:
                attempts.append({"candidate": index, "status": error.code, "seconds": round(monotonic() - started, 2)})
                record({"status": "checking_cpu", "catalog_at": utc_iso(first_at), "attempts": attempts})
                if error.code in {"invalid_hpc_token", "permission_denied", "hpc_network_permission", "hpc_rate_limited", "stopped"}:
                    raise
        if selected is None:
            raise SlurmJobsError("no_valid_counter", "本次抽查的节点都未取得合格的 CPU 计数样本。")

        job, path, points, initial_cpu = selected
        last = points[-1][0]
        spacing = median(right[0] - left[0] for left, right in zip(points, points[1:]))
        first_expected = last + max(0, ceil((first_at - last) / spacing)) * spacing
        ready_at = max(first_at + 75, first_expected + spacing + 15)
        provisional = {"job_id": job.job_id, "allocated_cpus": job.cpus,
                       "observed_start": utc_iso(points[0][0]), "observed_end": utc_iso(last),
                       "cpu_seconds": initial_cpu["cpu_seconds"], "capacity_cpu_seconds": initial_cpu["capacity_cpu_seconds"],
                       "efficiency": initial_cpu["utilization"], "observed_sample_spacing_seconds": spacing}
        for check in range(4):
            record({"status": "confirming_allocation", "catalog_at": utc_iso(first_at), "attempts": attempts,
                    "early_measurement": provisional, "next_check_at": utc_iso(ready_at)})
            wait_until(ready_at)
            end = int(time())
            measured = monotonic()
            try:
                points, cpu = read(job, path, first_at, end)
            except SlurmJobsError as error:
                if error.code not in {"job_cpu_short_window", "job_cpu_no_points", "job_cpu_missing"} or check == 3:
                    raise
                ready_at = time() + max(30, min(120, spacing / 2))
            else:
                break
        if resolve_job_cgroup(request, job.job_id, job.hosts[0], int(time())) != path:
            raise SlurmJobsError("job_cgroup_changed", "复查时作业根 cgroup 发生变化。")
        duration = round(monotonic() - measured, 2)
        _, latest = read_allocations(source)
        if job not in latest:
            raise SlurmJobsError("allocation_changed", "复查时作业已结束或分配发生变化。")
        left, right = points[0], points[-1]
        result = {"status": "verified", "catalog_at": utc_iso(first_at), "attempts": attempts,
                 "job_id": job.job_id, "allocated_cpus": job.cpus, "samples": len(points),
                 "observed_start": utc_iso(left[0]), "observed_end": utc_iso(right[0]),
                 "counter_start": left[1], "counter_end": right[1], "cpu_seconds": cpu["cpu_seconds"],
                 "capacity_cpu_seconds": cpu["capacity_cpu_seconds"], "efficiency": cpu["utilization"],
                 "latest_sample_delay_seconds": round(end - right[0], 2),
                 "within_live_age_limit": end - right[0] <= SAMPLE_MAX_AGE,
                 "request_seconds": duration}
        record(result)

        selector = f'{CPU_METRIC}{{hostname={json.dumps(job.hosts[0])},cgroup={json.dumps(path)}}}'
        seconds = ceil(right[0] - left[0]) + 1
        timings = {}
        for mode, query in (("raw", f"{selector}[{seconds}s]"), ("rate", f"rate({selector}[{seconds}s])")):
            started = monotonic()
            try:
                response = request("/prometheus/query", {"query": query, "time": right[0]})
                data = response.get("data") or {}
                expected = "matrix" if mode == "raw" else "vector"
                series = [row for row in data.get("result", []) if row.get("metric", {}).get("cgroup") == path
                          and row.get("metric", {}).get("hostname") == job.hosts[0]]
                if response.get("status") != "success" or response.get("warnings") or data.get("resultType") != expected or len(series) != 1:
                    raise SlurmJobsError("job_monitor_invalid", "对照查询未返回唯一可信的作业序列。")
                row = series[0]
                timings[mode] = {"seconds": round(monotonic() - started, 2),
                                 "json_bytes": len(json.dumps(response).encode("utf-8"))}
                if mode == "rate":
                    value = float(row["value"][1])
                    if not isfinite(value) or value < 0:
                        raise ValueError("invalid rate")
                    timings[mode]["cpu_seconds_per_second"] = value
                else:
                    timings[mode]["points"] = len(row["values"])
            except SlurmJobsError as error:
                timings[mode] = {"error": error.code, "seconds": round(monotonic() - started, 2)}
            except (ValueError, KeyError, IndexError, TypeError, AttributeError):
                timings[mode] = {"error": "job_monitor_invalid", "seconds": round(monotonic() - started, 2)}
        publish({**result, "comparison": timings})
    except SlurmJobsError as error:
        publish({**current, "status": "failed", "code": error.code, "reason": str(error), "attempts": attempts})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8847)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--proxy", default=os.environ.get("HPC_PROXY"))
    args = parser.parse_args()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    if args.report.exists():
        parser.error("请选择新的报告路径。")

    def publish(data):
        temporary = args.report.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(args.report)

    connected = Lock()
    rate_limit = PrometheusRateLimit(30)

    class ProbeSource(web.DataSource):
        def __init__(self, token):
            super().__init__(None, None, args.proxy, token=token)
            self.prometheus_limit = rate_limit
            self.stop_probe = Event()
            self.worker_lock = Lock()
            self.worker = None
            self.result = None

        def validate_connection(self):
            payload = self.running_catalog()
            if not isinstance(payload.get("jobs"), list):
                raise SlurmJobsError("hpc_catalog_invalid", "Slurm 当前作业列表格式无效。")
            if not connected.acquire(blocking=False):
                raise SlurmJobsError("experiment_in_use", "单作业实验已经开始。")
            self.initial_catalog = payload

        def start_collection(self):
            with self.worker_lock:
                if self.stop_probe.is_set() or self.worker and self.worker.is_alive():
                    return
                self.result = None
                self.worker = Thread(target=probe, args=(self, self.record_result, self.stop_probe), daemon=True)
                self.worker.start()

        def record_result(self, result):
            publish(result)
            self.result = result

        def stop_collection(self):
            self.stop_probe.set()

        def fetch(self, view, *, refresh=False):
            if view != "running":
                return super().fetch(view, refresh=refresh)
            frame = {"view": view, "status": "warming_up", "jobs": [], "queried_at": None,
                     "source_updated_at": None, "source_kind": self.kind, "source": "Slurm + Prometheus"}
            result = self.result or {}
            if refresh and result.get("status") == "failed":
                self.start_collection()
            elif result.get("status") == "verified":
                delay = result["latest_sample_delay_seconds"]
                frame.update(status="partial", queried_at=result["observed_end"], source_updated_at=result["observed_end"],
                             jobs=[{"id": str(result["job_id"]), "state": "RUNNING", "partition": "单作业复测",
                                    "cpus": result["allocated_cpus"], "efficiency": result["efficiency"],
                                    "cpu_seconds": result["cpu_seconds"], "cpu_observed_start": result["observed_start"],
                                    "cpu_observed_end": result["observed_end"],
                                    "reason": f"取样时最新样本延迟 {delay:g} 秒。此效率对应所示的实测时间窗。"}])
            elif result.get("status") == "failed":
                frame.update(status="unavailable", error={"message": result["reason"]})
            return frame

    class ExclusiveProbeServer(web.HpcServer):
        allow_reuse_address = False

        def server_bind(self):
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            super().server_bind()

    class ProbeHandler(web.Handler):
        def do_GET(self):
            if urlsplit(self.path).path != "/":
                return super().do_GET()
            page = (web.ROOT / "index.html").read_text(encoding="utf-8")
            page = page.replace("<title>作业资源观察", "<title>单作业 CPU 复测").replace(
                'tabindex="-1">作业资源观察</h1>', 'tabindex="-1">单作业 CPU 复测</h1>', 1,
            ).replace("用一张图，了解集群中每个作业的 CPU 使用情况。", "连接 SJTU HPC 后自动验证一个运行作业的 CPU 计数与采样时间。", 1)
            body = page.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    web.SESSION_COOKIE = "hpc_single_cpu_session"
    web.HpcServer, web.Handler = ExclusiveProbeServer, ProbeHandler
    with web.create_server(web.DataSource(None, None, None), args.port, connection_factory=ProbeSource) as server:
        publish({"status": "waiting_for_connection"})
        print(f"在 http://localhost:{server.server_port}/ 连接 SJTU HPC 后开始单作业实验。", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
