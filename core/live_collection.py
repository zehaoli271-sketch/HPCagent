"""Session-scoped collection of Slurm allocations and Prometheus observations."""

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from math import ceil
from threading import Condition, Event, Lock, Thread
from time import monotonic, time

from .job_monitor import (
    LIVE_QUERY_STEP, SAMPLE_MAX_AGE, CgroupMonitor, expand_hostlist, fresh_series, measure_cpu_series, measure_memory_series,
)
from .job_query import MIB, epoch, safe_label, slurm_integer, tres_count
from .job_screening import WINDOW_SECONDS, get_job_cpu_usage_window
from .slurm_jobs import SlurmJobsError


MIN_LIVE_CPU_SECONDS = 60
SWEEP_LOOKBACK = 2 * WINDOW_SECONDS + 15
# The captured SJTU job-counter series has a 300-second scrape interval.
DIRECT_LOOKBACK = 3 * WINDOW_SECONDS
DIRECT_MAX_SAMPLE_AGE = WINDOW_SECONDS + SAMPLE_MAX_AGE
SNAPSHOT_RETENTION = 20 * 60
AUTH_ERRORS = {"invalid_hpc_token", "permission_denied"}
UNSTABLE_FLAGS = {"RESIZED", "RESIZING", "REQUEUED"}


class CollectionConflict(Exception):
    pass


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
    allocation_since: int | None = None

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
    started = epoch(slurm_integer(job.get("start_time")) or timing.get("start"))
    resize = slurm_integer(job.get("resize_time"))
    allocation_since = max(started, resize) if started and resize is not None else None
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
        job["job_id"], started,
        epoch(slurm_integer(job.get("submit_time")) or timing.get("submission")), expand_hostlist(node_list),
        tres_count(allocated, "cpu") or (int(cpu_allocation[1]) if cpu_allocation else None) or slurm_integer(job.get("cpus")) or None,
        memory_bytes(allocated, allocation_text), tres_count(requested, "cpu") or (int(cpu_request[1]) if cpu_request else None),
        memory_bytes(requested, request_text), safe_label(job.get("partition")), safe_label(job.get("cluster")),
        not unstable, allocation_since,
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
        self.collection_cancelled = Event()
        self.background_tasks = set()
        self.foreground_queries = 0
        self.revision = 0
        self.snapshots = deque(maxlen=1 + SNAPSHOT_RETENTION // interval)
        self.worker = None
        self.error = None
        self.catalog_error = None
        self.latest_catalog = None
        self.initial_view = None
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
        with self.changed:
            self.stop_event.set()
            self.collection_cancelled.set()
            self.changed.notify_all()

    @property
    def collection_enabled(self):
        if not self.stop_event.is_set():
            return not self.collection_cancelled.is_set()
        return None

    def _interrupt_collection(self):
        self.collection_cancelled.set()
        self.catalog_ready.clear()
        self.changed.notify_all()

    def set_collection_enabled(self, enabled):
        with self.changed:
            current = self.collection_enabled
            if current is None or current == enabled:
                return current
            if enabled:
                self.collection_cancelled = Event()
                self.changed.notify_all()
            else:
                self._interrupt_collection()
            return enabled

    def begin_foreground_query(self, *, exclusive=False):
        with self.changed:
            if self.collection_enabled and (exclusive or any(
                cycle is self.collection_cancelled for cycle, _ in self.background_tasks
            )):
                raise CollectionConflict
            if exclusive and self.background_tasks:
                return False
            self.foreground_queries += 1
            return True

    def end_foreground_query(self):
        with self.changed:
            self.foreground_queries -= 1
            self.changed.notify_all()

    def wait_for_snapshot(self, after, timeout=20):
        with self.changed:
            self.changed.wait_for(lambda: self.revision != after or self.collection_cancelled.is_set(), timeout=timeout)
            return self.revision

    def prioritize(self, job_id):
        with self.lock:
            self.priority = job_id

    def _run(self, task, ready=None, on_completion=False):
        while True:
            with self.changed:
                self.changed.wait_for(lambda: self.stop_event.is_set() or (
                    not self.collection_cancelled.is_set() and not self.foreground_queries
                    and (ready is None or ready.is_set())
                ))
                if self.stop_event.is_set():
                    return
                cancelled = self.collection_cancelled
                work = (cancelled, task)
                self.background_tasks.add(work)
            started = monotonic()
            try:
                published = task(cancelled)
            finally:
                with self.changed:
                    self.background_tasks.discard(work)
            if on_completion and published and published["coverage"]["nodes_total"] and not published["coverage"]["errors"]:
                cancelled.wait(1)
            else:
                cancelled.wait(max(1, self.interval - (monotonic() - started)))

    def _invalidate(self, issue):
        with self.lock:
            self.error = {"code": issue.code, "message": str(issue)}
            self.latest_catalog = None
            self.initial_view = None
            self.snapshots.clear()
        self.stop()

    def seed_catalog(self, payload=None):
        """Publish the directory used before the first monitoring round."""
        self.refresh_catalog(self.stop_event, payload=payload)

    def sample_job(self, job_id, expected_start=None):
        if not self.begin_foreground_query(exclusive=not self.synthetic):
            return {"status": "waiting", "message": "正在等待先前发出的后台采集请求收尾。", "next_poll_seconds": 2}
        try:
            at = int(self.clock())
            with self.lock:
                catalog = self.latest_catalog
            if not catalog or at - catalog[0] >= self.catalog_max_age // 2 or all(
                job.job_id != job_id for job in catalog[1]
            ):
                self.seed_catalog()
            at = int(self.clock())
            with self.lock:
                catalog = self.latest_catalog
                error = self.error or self.catalog_error
                if not catalog or at - catalog[0] > self.catalog_max_age:
                    error = error or {"code": "hpc_catalog_unavailable", "message": "暂时无法核实运行作业及其分配节点。"}
                    raise SlurmJobsError(error["code"], error["message"])
                job = next((row for row in catalog[1] if row.job_id == job_id), None)
                stable_at = self.stable_since.get(job)
            if job is None or (expected_start is not None and job.started != expected_start):
                return {"status": "ended", "message": "Slurm 当前清单中已找不到这次作业运行。"}
            if not job.hosts or not job.started:
                return {"status": "unavailable", "message": "缺少可信的分配节点或启动时间，暂时无法监控。"}

            deadline = monotonic() + 20
            retry = LIVE_QUERY_STEP

            def request(path, params):
                nonlocal retry
                remaining = deadline - monotonic()
                if remaining <= 0 or self.stop_event.is_set():
                    raise SlurmJobsError("monitor_budget", "本次单作业采集已结束，请等待下一次采样。")
                try:
                    return self.prometheus(path, params, timeout=min(8, remaining))
                except SlurmJobsError as issue:
                    retry = max(retry, ceil(getattr(issue, "retry_after", 0)))
                    raise

            start = max(at - DIRECT_LOOKBACK, job.started)
            samples, coverage = CgroupMonitor().collect_live(
                request, {job_id: job.hosts}, start, at, only_job_id=job_id, raw=len(job.hosts) == 1,
            )
            if auth_error := next((key for key in coverage["errors"] if key in AUTH_ERRORS), None):
                raise SlurmJobsError(auth_error, coverage["errors"][auth_error])
            row = self._row(job, samples, stable_at, start, at, direct=True)
            notes = list(coverage["errors"].values())
            if error:
                notes.append(f"分配信息暂用上次 Slurm 清单。{error['message']}")
            return {"status": "sampling", "job": row, "queried_at": utc_iso(at), "next_poll_seconds": retry,
                    "max_sample_age_seconds": DIRECT_MAX_SAMPLE_AGE,
                    "notes": notes, "source": "合成测试数据" if self.synthetic else "Slurm + Prometheus"}
        finally:
            self.end_foreground_query()

    def refresh_catalog(self, cancelled=None, *, payload=None):
        cancelled = cancelled if cancelled is not None else self.collection_cancelled
        if cancelled.is_set():
            return
        try:
            catalog = self.catalog() if payload is None else payload
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
                    if not cancelled.is_set():
                        self.catalog_error = {"code": issue.code, "message": str(issue)}
            return

        at = int(self.clock())
        with self.changed:
            if cancelled.is_set():
                return
            prior = self.stable_since if self.latest_catalog and at - self.latest_catalog[0] <= self.catalog_max_age else {}
            first_seen = at - WINDOW_SECONDS if self.synthetic else at
            previous_ids = {job.job_id for job in self.latest_catalog[1]} if self.latest_catalog else set()
            self.stable_since = {
                job: prior.get(job, job.allocation_since
                               if job.job_id not in previous_ids and job.allocation_since and job.allocation_since <= at
                               else first_seen)
                for job in jobs if job.stable
            }
            self.latest_catalog = at, jobs, len(counts)
            if self.full_pass and self.initial_view is None and not self.snapshots:
                self.initial_view = {
                    "as_of": at, "queried_at": utc_iso(at), "source": "Slurm", "catalog_only": True,
                    "jobs": [self._row(job, None, None, at, at) for job in jobs],
                }
            self.catalog_error = None
            if cancelled is self.collection_cancelled:
                self.catalog_ready.set()
            self.changed.notify_all()

    def collect_once(self, cancelled=None):
        with self.lock:
            cancelled = cancelled if cancelled is not None else self.collection_cancelled
            if cancelled.is_set() or not self.catalog_ready.is_set() or not self.latest_catalog:
                return
            catalog = self.latest_catalog
            priority, self.priority = self.priority, None
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
                while not cancelled.is_set() and not interrupted.is_set():
                    try:
                        return self.prometheus(path, params, timeout=12)
                    except SlurmJobsError as error:
                        if error.code not in {"monitor_budget", "hpc_rate_limited"}:
                            if error.code in AUTH_ERRORS or error.code in {"hpc_network_permission", "hpc_proxy_unreachable"}:
                                interrupted.set()
                            raise
                        cancelled.wait(getattr(error, "retry_after", 60) + 0.1)
                raise SlurmJobsError("monitor_budget", "本轮采集提前结束。")
            left = deadline - monotonic()
            if cancelled.is_set() or left <= 0:
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
                                                               clock=self.clock, priority=priority, cancelled=cancelled)
            else:
                samples, coverage = self.monitor.collect_live(request, eligible, start, at, node_offset=self.node_offset,
                                                              priority=priority)
            if cancelled.is_set():
                return
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
        if not current or cancelled.is_set():
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
            if not cancelled.is_set():
                self.revision += 1
                self.snapshots.append(snapshot)
                self.initial_view = None
                while self.snapshots and at - self.snapshots[0]["as_of"] > SNAPSHOT_RETENTION:
                    self.snapshots.popleft()
                self.error = None
                self.changed.notify_all()
                return snapshot

    def _row(self, job, samples, stable_at, start, at, *, direct=False):
        full_sweep = self.full_pass and not direct
        cpu = {"utilization": None}
        memory = None
        memory_reason = "等待采集该作业各节点的内存样本。"
        reason = "等待作业级 CPU 样本覆盖所有分配节点。"
        if samples is None:
            reason = "已取得 Slurm 作业信息，CPU 和内存效率等待首轮监控数据。"
        elif not job.hosts or not job.started:
            reason = "Slurm 缺少可信的分配节点或作业启动时间。"
            memory_reason = reason
        else:
            per_node = samples["cpu"].get(job.job_id, {})
            per_node_memory = samples["memory"].get(job.job_id, {})
            sample_age = DIRECT_MAX_SAMPLE_AGE if direct else WINDOW_SECONDS if full_sweep else SAMPLE_MAX_AGE
            if full_sweep:
                cpu_end = max((line[-1][0] for line in per_node.values()), default=at)
                memory_end = max((line[-1][0] for line in per_node_memory.values()), default=at)
                start = cpu_end - SWEEP_LOOKBACK
                memory_start = max(memory_end - SWEEP_LOOKBACK, job.started)
            else:
                cpu_end = memory_end = at
                memory_start = max(start, job.started)
            window_start = max(start, job.started)
            memory_start = max(memory_start, job.allocation_since or job.started)
            memory, memory_reason = measure_memory_series(
                job.hosts, per_node_memory, memory_start, memory_end, max_sample_age=sample_age,
            )
            if not job.stable or not job.cpus:
                reason = "CPU 分配缺失，或作业有重启、异构或调整记录。"
            elif stable_at is None or cpu_end - max(window_start, stable_at) < MIN_LIVE_CPU_SECONDS:
                reason = "等待至少一分钟的稳定分配和 CPU 样本。"
            else:
                cpu_start = max(window_start, stable_at)
                if all(host in per_node for host in job.hosts):
                    series, reason = measure_cpu_series(
                        job.hosts, per_node, cpu_start, cpu_end, minimum_seconds=MIN_LIVE_CPU_SECONDS,
                        max_sample_age=sample_age, recent_seconds=WINDOW_SECONDS if direct else None,
                    )
                    if series:
                        cpu = get_job_cpu_usage_window(job.cpu_record(), series, series[0][0], series[-1][0])
                        reason = cpu["reason"]
                else:
                    reason = "缺少部分分配节点的作业级 CPU 样本。"
        memory = memory or {}
        used_memory = memory.get("memory_used_bytes")
        memory_ratio = None
        if used_memory is not None:
            if not job.memory_bytes:
                memory_reason = "Slurm 未提供可信的内存分配量。"
            elif not job.stable:
                memory_reason = "作业有重启、异构或分配调整记录，内存分配量待核实。"
            else:
                memory_ratio = used_memory / job.memory_bytes
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
            "memory_ratio": memory_ratio, "memory_reason": memory_reason,
            "memory_observed_at": utc_iso(memory.get("memory_observed_at")),
            "memory_max_skew_seconds": memory.get("memory_max_skew_seconds"),
        }

    def snapshot(self):
        with self.lock:
            latest = self.snapshots[-1] if self.snapshots else None
            error, catalog_error = self.error, self.catalog_error
            count = len(self.snapshots)
            catalog = self.latest_catalog
            initial = self.initial_view
            enabled = self.collection_enabled
            metadata = {"collection_revision": self.revision,
                        "collection_paused": enabled is False,
                        "updates_on_completion": self.full_pass and enabled is True}
        if not latest:
            status = "unavailable" if error or catalog_error and not catalog else "paused" if enabled is False else "warming_up"
            if initial and self.clock() - initial["as_of"] > self.catalog_max_age:
                status = "stale"
            return {
                    "jobs": [], "coverage": None, "queried_at": None, "error": error, "catalog_error": catalog_error,
                    "status": status, "snapshot_count": 0, **metadata, **(initial or {})}
        now = self.clock()
        stale = (not catalog or now - catalog[0] > self.catalog_max_age
                 or now - latest["as_of"] > max(60, 2 * self.interval,
                                                2 * latest["collection_seconds"] if self.full_pass else 0))
        status = "stale" if stale else "partial" if catalog_error else latest["status"]
        return {**latest, "status": status, "error": error, "catalog_error": catalog_error, "snapshot_count": count, **metadata}
