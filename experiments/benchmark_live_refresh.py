"""Measure one pass over a fixed, authorized Slurm running catalog."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
from statistics import median
from threading import Lock, Thread
from time import monotonic

from core.job_monitor import CgroupMonitor, PrometheusRateLimit
from core.live_collection import LiveCollector
from core.slurm_jobs import SlurmJobsError
from frontend import server as web


class CountingMonitor(CgroupMonitor):
    def __init__(self):
        super().__init__()
        self.responded = {"cpu": set(), "memory": set()}

    def collect_live(self, request, jobs, start, end, *, node_offset=0, priority=None):
        samples, coverage = super().collect_live(request, jobs, start, end, node_offset=node_offset)
        for metric, hosts in coverage["visited_hosts"].items():
            self.responded[metric].update(hosts)
        return samples, coverage


class LiveSweep(LiveCollector):
    def __init__(self, catalog, prometheus, *, interval, rpm, publish):
        super().__init__(catalog, self._measure_request, interval=interval, rpm=rpm)
        self.request_prometheus = prometheus
        self.publish = publish
        self.monitor = CountingMonitor()
        self.catalog_max_age = float("inf")
        self.http_seconds = []
        self.request_errors = Counter()
        self.started_at = None
        self.catalog_seconds = None

    def _measure_request(self, path, params, *, timeout):
        started = monotonic()
        try:
            return self.request_prometheus(path, params, timeout=timeout)
        except SlurmJobsError as error:
            self.request_errors[error.code] += 1
            if error.code == "monitor_budget":
                started = None
            raise
        finally:
            if started is not None:
                self.http_seconds.append(monotonic() - started)

    def start(self):
        with self.lock:
            if self.worker is None and not self.stop_event.is_set():
                self.worker = Thread(target=self.run, name="hpc-live-benchmark", daemon=True)
                self.worker.start()

    def run(self):
        self.refresh_catalog()
        if not self.latest_catalog:
            self.publish({"status": "catalog_failed", "error": self.catalog_error or self.error})
            return
        _, allocations, job_count = self.latest_catalog
        nodes = {host for job in allocations if job.started for host in job.hosts}
        cpu_jobs, memory_jobs, both_jobs = set(), set(), set()
        best_both = rounds = 0
        round_errors = Counter()
        started = monotonic()

        def result(status, frame):
            cpu_hosts, memory_hosts = self.monitor.responded["cpu"], self.monitor.responded["memory"]
            latencies = self.http_seconds
            return {
                "status": status, "slurm_catalog_at": frame.get("catalog_queried_at"),
                "last_observation_at": frame.get("queried_at"), "catalog_seconds": self.catalog_seconds,
                "elapsed_seconds": round(monotonic() - (self.started_at or started), 2),
                "monitor_seconds": round(monotonic() - started, 2),
                "configured_interval_seconds": self.interval, "requests_per_round": self.requests_per_round,
                "rounds": rounds, "jobs_in_initial_catalog": job_count,
                "jobs_with_nodes_and_start": sum(bool(job.hosts and job.started) for job in allocations),
                "nodes": len(nodes), "nodes_attempted": min(self.node_offset, len(nodes)),
                "nodes_with_cpu_response": len(cpu_hosts), "nodes_with_memory_response": len(memory_hosts),
                "nodes_with_both_responses": len(cpu_hosts & memory_hosts),
                "jobs_with_valid_cpu_once": len(cpu_jobs), "jobs_with_valid_memory_once": len(memory_jobs),
                "jobs_with_both_valid_in_one_round": len(both_jobs), "max_jobs_with_both_in_one_frame": best_both,
                "requests": len(latencies), "request_median_seconds": round(median(latencies), 3) if latencies else None,
                "request_errors": dict(self.request_errors), "rounds_with_errors": dict(round_errors),
            }

        frame = self.snapshot()
        self.publish(result("scanning", frame))
        status = "scan_finished"
        while self.node_offset < len(nodes):
            round_start = monotonic()
            self.collect_once()
            frame = self.snapshot()
            rounds += 1
            coverage = frame["coverage"] or {}
            round_errors.update(coverage.get("errors", {}).keys())
            for row in frame["jobs"]:
                if row["efficiency"] is not None:
                    cpu_jobs.add(row["job_id"])
                if row["memory_used_bytes"] is not None:
                    memory_jobs.add(row["job_id"])
                if row["efficiency"] is not None and row["memory_used_bytes"] is not None:
                    both_jobs.add(row["job_id"])
            best_both = max(best_both, sum(row["efficiency"] is not None and row["memory_used_bytes"] is not None
                                          for row in frame["jobs"]))
            if self.stop_event.is_set() or "permission_denied" in round_errors or "hpc_network_permission" in round_errors:
                status = "stopped"
                break
            if self.node_offset >= len(nodes):
                break
            self.publish(result("scanning", frame))
            if self.stop_event.wait(max(1, self.interval - (monotonic() - round_start))):
                status = "stopped"
                break
        report = result(status, frame)
        if status == "scan_finished":
            # Re-read membership after the timed sweep. Duration above excludes this check.
            final_catalog = LiveCollector(self.catalog, self.request_prometheus)
            final_catalog.refresh_catalog()
            if final_catalog.latest_catalog:
                remaining = {job.job_id: job for job in final_catalog.latest_catalog[1]}
                unchanged = {job.job_id for job in allocations if remaining.get(job.job_id) == job}
                report["jobs_still_running_with_same_allocation"] = len(unchanged)
                report["unchanged_jobs_with_both_valid_in_one_round"] = len(unchanged & both_jobs)
            else:
                report["final_catalog_error"] = final_catalog.catalog_error or final_catalog.error
        self.publish(report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8771)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--proxy", default=os.environ.get("HPC_PROXY"))
    args = parser.parse_args()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    if args.report.exists():
        parser.error("请选择一个新的实验报告路径。")

    def publish(data):
        temporary = args.report.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(args.report)

    rate_limit = PrometheusRateLimit(30)
    single_run = Lock()

    class ExperimentSource(web.DataSource):
        def validate_connection(self):
            started = monotonic()
            super().validate_connection()
            if not single_run.acquire(blocking=False):
                raise SlurmJobsError("experiment_in_use", "本次实验已经开始。")
            self.collector.started_at = started
            self.collector.catalog_seconds = round(monotonic() - started, 2)

    def connection_factory(token):
        source = ExperimentSource(None, None, args.proxy, token=token)
        source.prometheus_limit = rate_limit
        source.collector = LiveSweep(source.running_catalog, source.prometheus_json, interval=30, rpm=30, publish=publish)
        return source

    web.SESSION_COOKIE = "hpc_benchmark_session"
    with web.create_server(web.DataSource(None, None, None), args.port, connection_factory=connection_factory) as server:
        publish({"status": "waiting_for_connection"})
        print(f"打开 http://localhost:{server.server_port}/ 并连接 SJTU HPC 开始实验。", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
