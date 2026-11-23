"""Session-scoped collection of Slurm allocations and Prometheus observations."""

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from threading import Condition, Event, Lock, Thread
from time import monotonic, time

from .job_monitor import SAMPLE_MAX_AGE, CgroupMonitor, combine_cpu_series, combine_memory_series, expand_hostlist, fresh_series
from .job_query import MIB, epoch, safe_label, slurm_integer, tres_count
from .job_screening import WINDOW_SECONDS, get_job_cpu_usage_window
from .slurm_jobs import SlurmJobsError


MIN_LIVE_CPU_SECONDS = 60
SWEEP_LOOKBACK = 2 * WINDOW_SECONDS + 15
SNAPSHOT_RETENTION = 20 * 60
AUTH_ERRORS = {"invalid_hpc_token", "permission_denied"}
UNSTABLE_FLAGS = {"RESIZED", "RESIZING", "REQUEUED"}


def utc_iso(stamp):
    return datetime.fromtimestamp(stamp, timezone.utc).isoformat() if stamp is not None else None


@dataclass(frozen=True)
class Allocation:
    job_id: int
    started: int | None
    submitted: int | None
    hosts: tuple
    cpus: int | None
    memory_bytes: int | None
    requested_cpus: int | None
    requested_memory_bytes: int | None
    partition: str | None
    cluster: str | None
    stable: bool

    def cpu_record(self):
        return {"time": {"start": self.started},
                "tres": {"allocated": [{"type": "cpu", "count": self.cpus}]}}


def memory_bytes(entries, encoded):
    count = tres_count(entries, "mem")
    if count:
        return count * MIB
    match = re.search(r"(?:^|,)mem=([0-9]+)([KMGT])(?:,|$)", encoded) if isinstance(encoded, str) else None
    return int(match[1]) * 1024 ** {"K": 1, "M": 2, "G": 3, "T": 4}[match[2]] if match else None


def allocation_from_job(job):
    timing = job.get("time") or {}
    node_list = job.get("nodes") or job.get("node_list")
    if isinstance(node_list, list) and all(isinstance(host, str) for host in node_list):
        node_list = ",".join(node_list)
    tres = job.get("tres") or {}
    allocated, requested = tres.get("allocated"), tres.get("requested")
    allocation_text, request_text = job.get("tres_alloc_str"), job.get("tres_req_str")
    cpu_allocation = re.search(r"(?:^|,)cpu=([1-9][0-9]*)(?:,|$)", allocation_text) if isinstance(allocation_text, str) else None
    cpu_request = re.search(r"(?:^|,)cpu=([1-9][0-9]*)(?:,|$)", request_text) if isinstance(request_text, str) else None
    flags = set(job.get("flags") or job.get("bitflags") or []) | set(job.get("job_state") or [])
    unstable = bool(flags & UNSTABLE_FLAGS or slurm_integer(job.get("restart_cnt")) or
                    (job.get("het") or {}).get("job_id") or slurm_integer(job.get("het_job_id")))
    return Allocation(
        job["job_id"], epoch(slurm_integer(job.get("start_time")) or timing.get("start")),
        epoch(slurm_integer(job.get("submit_time")) or timing.get("submission")), expand_hostlist(node_list),
        tres_count(allocated, "cpu") or (int(cpu_allocation[1]) if cpu_allocation else None) or slurm_integer(job.get("cpus")) or None,
        memory_bytes(allocated, allocation_text), tres_count(requested, "cpu") or (int(cpu_request[1]) if cpu_request else None),
        memory_bytes(requested, request_text), safe_label(job.get("partition")), safe_label(job.get("cluster")),
        not unstable,
    )


class LiveCollector:
    """Publish immutable snapshots from one authorized source and keep a short rolling history."""

    def __init__(self, catalog, prometheus, *, interval=30, rpm=None, clock=time, synthetic=False, full_pass=False):
        self.catalog, self.prometheus, self.interval, self.clock = catalog, prometheus, interval, clock
        self.requests_per_round = max(1, rpm * interval // 60) if rpm is not None else None
        self.synthetic = synthetic
        self.full_pass = full_pass
        self.monitor = CgroupMonitor()
        self.lock, self.stop_event, self.catalog_ready = Lock(), Event(), Event()
        self.changed = Condition(self.lock)
        self.revision = 0
        self.snapshots = deque(maxlen=1 + SNAPSHOT_RETENTION // interval)
        self.worker = None
        self.error = None
        self.catalog_error = None
        self.latest_catalog = None
        self.catalog_max_age = max(120, 3 * interval)
        self.stable_since = {}
        self.sample_cache = {"cpu": {}, "memory": {}}
        self.node_offset = 0
        self.priority = None
        self.monitor_retry_at = 0

    def start(self):
        with self.lock:
            if self.worker is None and not self.stop_event.is_set():
                catalog_worker = Thread(target=self._run, args=(self.refresh_catalog,), name="hpc-live-catalog", daemon=True)
                self.worker = Thread(target=self._run, args=(self.collect_once, self.catalog_ready, self.full_pass),
                                     name="hpc-live-collector", daemon=True)
                catalog_worker.start()
                self.worker.start()

    def stop(self):
        self.stop_event.set()
        self.catalog_ready.set()
        with self.changed:
            self.changed.notify_all()

    def wait_for_snapshot(self, after, timeout=20):
        with self.changed:
            self.changed.wait_for(lambda: self.revision != after or self.stop_event.is_set(), timeout=timeout)
            return self.revision

    def prioritize(self, job_id):
        with self.lock:
            self.priority = job_id

    def _run(self, task, ready=None, on_completion=False):
        if ready:
            ready.wait()
        while not self.stop_event.is_set():
            started = monotonic()
            published = task()
            if on_completion and published and published["coverage"]["nodes_total"] and not published["coverage"]["errors"]:
                self.stop_event.wait(1)
            else:
                self.stop_event.wait(max(1, self.interval - (monotonic() - started)))

    def _invalidate(self, issue):
        with self.lock:
            self.error = {"code": issue.code, "message": str(issue)}
            self.latest_catalog = None
            self.snapshots.clear()
        self.stop()

    def refresh_catalog(self):
        try:
            catalog = self.catalog()
            if not isinstance(catalog.get("jobs"), list):
                raise ValueError("invalid job catalog")
            if catalog.get("warnings"):
                raise SlurmJobsError("hpc_catalog_warning", "Slurm 当前作业列表带有警告，等待下一轮确认。")
            active = [job for job in catalog["jobs"] if type(job.get("job_id")) is int and job["job_id"] > 0
                      and "RUNNING" in (job.get("job_state") or [])]
            distinct = list(dict.fromkeys(allocation_from_job(job) for job in active))
            counts = Counter(job.job_id for job in distinct)
            jobs = [job for job in distinct if counts[job.job_id] == 1]
        except (SlurmJobsError, AttributeError, KeyError, TypeError, ValueError) as error:
            issue = error if isinstance(error, SlurmJobsError) else SlurmJobsError("hpc_catalog_invalid", "Slurm 当前作业列表格式无效。")
            if issue.code in AUTH_ERRORS:
                self._invalidate(issue)
            else:
                with self.lock:
                    self.catalog_error = {"code": issue.code, "message": str(issue)}
            return

        at = int(self.clock())
        with self.lock:
            if self.stop_event.is_set():
                return
            prior = self.stable_since if self.latest_catalog and at - self.latest_catalog[0] <= self.catalog_max_age else {}
            first_seen = at - WINDOW_SECONDS if self.synthetic else at
            self.stable_since = {job: prior.get(job, first_seen) for job in jobs if job.stable}
            self.latest_catalog = at, jobs, len(counts)
            self.catalog_error = None
            self.catalog_ready.set()

    def collect_once(self):
        with self.lock:
            catalog = self.latest_catalog
            priority, self.priority = self.priority, None
        if not catalog or self.stop_event.is_set():
            return
        catalog_at, jobs, _ = catalog
        at = int(self.clock())
        if at - catalog_at > self.catalog_max_age:
            return
        collection_started_at = at
        start = at - WINDOW_SECONDS
        eligible = {job.job_id: job.hosts for job in jobs if job.hosts and job.started}

        started = monotonic()
        deadline = started + self.interval
        requests = 0
        interrupted = Event()

        def request(path, params):
            nonlocal requests
            if self.full_pass:
                while not self.stop_event.is_set() and not interrupted.is_set():
                    try:
                        return self.prometheus(path, params, timeout=12)
                    except SlurmJobsError as error:
                        if error.code not in {"monitor_budget", "hpc_rate_limited"}:
                            if error.code in AUTH_ERRORS or error.code in {"hpc_network_permission", "hpc_proxy_unreachable"}:
                                interrupted.set()
                            raise
                        self.stop_event.wait(getattr(error, "retry_after", 60) + 0.1)
                raise SlurmJobsError("monitor_budget", "本轮采集提前结束。")
            left = deadline - monotonic()
            if self.stop_event.is_set() or left <= 0:
                raise SlurmJobsError("monitor_budget", "本轮监控读取已到时间上限，剩余节点将轮询。")
            if self.requests_per_round is not None and requests >= self.requests_per_round:
                raise SlurmJobsError("monitor_budget", "本轮监控请求已用完，剩余节点将轮询。")
            requests += 1
            return self.prometheus(path, params, timeout=min(left, 12))

        if at < self.monitor_retry_at:
            samples = {"cpu": {}, "memory": {}}
            coverage = {"nodes_total": len({node for hosts in eligible.values() for node in hosts}), "nodes_visited": 0,
                        "errors": {"permission_denied": "Prometheus 暂无可用权限，等待重新验证。"}}
        else:
            if self.full_pass:
                samples, coverage = self.monitor.collect_sweep(request, eligible, width=SWEEP_LOOKBACK,
                                                               clock=self.clock, priority=priority)
            else:
                samples, coverage = self.monitor.collect_live(request, eligible, start, at, node_offset=self.node_offset,
                                                              priority=priority)
            self.node_offset += coverage["nodes_attempted"]
            if "permission_denied" in coverage["errors"]:
                self.monitor_retry_at = at + 300
            if "invalid_hpc_token" in coverage["errors"]:
                self._invalidate(SlurmJobsError("invalid_hpc_token", coverage["errors"]["invalid_hpc_token"]))
                return
        sampled_allocations = {job.job_id: job for job in jobs}
        with self.lock:
            current = self.latest_catalog
            since = self.stable_since
        if not current or self.stop_event.is_set():
            return
        catalog_at, jobs, count = current
        at = int(self.clock())
        start = at - WINDOW_SECONDS
        visited = coverage.pop("visited_hosts", {})
        access_error = bool(AUTH_ERRORS & coverage["errors"].keys())
        for resource, by_job in samples.items():
            prior = self.sample_cache[resource]
            merged = {}
            for job in jobs:
                cached = {} if access_error else {
                    host: line for host, line in prior.get(job, {}).items()
                       if not self.full_pass and host not in visited.get(resource, ()) and fresh_series(line, at)
                }
                if sampled_allocations.get(job.job_id) == job:
                    cached.update(by_job.get(job.job_id, {}))
                by_job.pop(job.job_id, None)
                if cached:
                    merged[job] = cached
                    by_job[job.job_id] = cached
            self.sample_cache[resource] = merged
        rows = [self._row(job, samples, since.get(job), start, at) for job in jobs]
        missing_cpu = Counter(row["reason"] for row in rows if row["efficiency"] is None)
        snapshot = {
            "as_of": at, "queried_at": utc_iso(at),
            "window_start": utc_iso(collection_started_at - SWEEP_LOOKBACK if self.full_pass else start),
            "source": "Slurm + Prometheus",
            "collection_started_at": utc_iso(collection_started_at),
            "catalog_queried_at": utc_iso(catalog_at),
            "collection_seconds": round(monotonic() - started, 1),
            "status": "partial" if len(jobs) != count or missing_cpu or coverage["errors"] or any(
                row["memory_used_bytes"] is None for row in rows
            ) else "ready",
            "jobs": rows, "coverage": {"catalog_count": count, "ambiguous_jobs": count - len(jobs),
                                       "cpu_evaluated": len(rows) - sum(missing_cpu.values()),
                                       "cpu_gaps": dict(missing_cpu),
                                       "memory_evaluated": sum(row["memory_used_bytes"] is not None for row in rows), **coverage},
        }
        with self.changed:
            if not self.stop_event.is_set():
                self.revision += 1
                self.snapshots.append(snapshot)
                while self.snapshots and at - self.snapshots[0]["as_of"] > SNAPSHOT_RETENTION:
                    self.snapshots.popleft()
                self.error = None
                self.changed.notify_all()
                return snapshot

    def _row(self, job, samples, stable_at, start, at):
        cpu = {"utilization": None}
        memory = None
        reason = "等待作业级 CPU 样本覆盖所有分配节点。"
        if not job.hosts or not job.started:
            reason = "Slurm 缺少可信的分配节点或作业启动时间。"
        else:
            per_node = samples["cpu"].get(job.job_id, {})
            per_node_memory = samples["memory"].get(job.job_id, {})
            sample_age = WINDOW_SECONDS if self.full_pass else SAMPLE_MAX_AGE
            if self.full_pass:
                cpu_end = max((line[-1][0] for line in per_node.values()), default=at)
                memory_end = max((line[-1][0] for line in per_node_memory.values()), default=at)
                start = cpu_end - SWEEP_LOOKBACK
                memory_start = max(memory_end - SWEEP_LOOKBACK, job.started)
            else:
                cpu_end = memory_end = at
                memory_start = max(start, job.started)
            window_start = max(start, job.started)
            memory = combine_memory_series(job.hosts, per_node_memory, memory_start, memory_end, max_sample_age=sample_age)
            if not job.stable or not job.cpus:
                reason = "CPU 分配缺失，或作业有重启、异构或调整记录。"
            elif stable_at is None or cpu_end - max(window_start, stable_at) < MIN_LIVE_CPU_SECONDS:
                reason = "等待至少一分钟的稳定分配和 CPU 样本。"
            else:
                cpu_start = max(window_start, stable_at)
                if all(host in per_node for host in job.hosts):
                    series = combine_cpu_series(job.hosts, per_node, cpu_start, cpu_end,
                                                minimum_seconds=MIN_LIVE_CPU_SECONDS, max_sample_age=sample_age)
                    if series:
                        cpu = get_job_cpu_usage_window(job.cpu_record(), series, series[0][0], series[-1][0])
                        reason = cpu["reason"]
                    else:
                        reason = "CPU 样本尚未覆盖所有节点共同的一分钟，或样本已过期、计数异常。"
                else:
                    reason = "缺少部分分配节点的作业级 CPU 样本。"
        memory = memory or {}
        used_memory = memory.get("memory_used_bytes")
        return {
            "job_id": job.job_id, "id": str(job.job_id), "state": "RUNNING", "cluster": job.cluster,
            "partition": job.partition or "未知", "nodes": list(job.hosts), "cpus": job.cpus,
            "allocated_cpus": job.cpus, "allocated_nodes": len(job.hosts) if job.hosts else None,
            "active_seconds": at - job.started if job.started and at >= job.started else None,
            "allocated_memory_mb": job.memory_bytes / MIB if job.memory_bytes else None,
            "requested_cpus": job.requested_cpus, "requested_memory_bytes": job.requested_memory_bytes,
            "requested_memory_mb": job.requested_memory_bytes / MIB if job.requested_memory_bytes else None,
            "started_at": utc_iso(job.started), "efficiency": cpu["utilization"], "reason": reason,
            "cpu_seconds": cpu.get("cpu_seconds"), "cpu_capacity": cpu.get("capacity_cpu_seconds"),
            "cpu_observed_start": utc_iso(cpu.get("observed_start")), "cpu_observed_end": utc_iso(cpu.get("observed_end")),
            "memory_used_bytes": used_memory, "memory_allocated_bytes": job.memory_bytes,
            "memory_ratio": used_memory / job.memory_bytes if used_memory is not None and job.memory_bytes and job.stable else None,
            "memory_observed_at": utc_iso(memory.get("memory_observed_at")),
            "memory_max_skew_seconds": memory.get("memory_max_skew_seconds"),
        }

    def snapshot(self):
        with self.lock:
            latest = self.snapshots[-1] if self.snapshots else None
            error, catalog_error = self.error, self.catalog_error
            count = len(self.snapshots)
            catalog = self.latest_catalog
            metadata = {"collection_revision": self.revision,
                        "updates_on_completion": self.full_pass and not self.stop_event.is_set()}
        if not latest:
            return {"status": "unavailable" if error or catalog_error and not catalog else "warming_up",
                    "jobs": [], "coverage": None, "queried_at": None, "error": error, "catalog_error": catalog_error,
                    "snapshot_count": 0, **metadata}
        now = self.clock()
        stale = (not catalog or now - catalog[0] > self.catalog_max_age
                 or now - latest["as_of"] > max(60, 2 * self.interval,
                                                2 * latest["collection_seconds"] if self.full_pass else 0))
        status = "stale" if stale else "partial" if catalog_error else latest["status"]
        return {**latest, "status": status, "error": error, "catalog_error": catalog_error, "snapshot_count": count, **metadata}
