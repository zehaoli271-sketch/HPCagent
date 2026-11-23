"""Read job-root cgroup series from the SJTU Prometheus gateway."""

from bisect import bisect_right
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
from math import isfinite
import re
from threading import Lock
from time import monotonic

from .job_screening import MIN_WINDOW_COVERAGE
from .slurm_jobs import SlurmJobsError


CPU_METRIC = "cgroup_cpu_total_seconds"
MEMORY_METRIC = "cgroup_memory_used_bytes"
LIVE_BATCH_NODES = 8
LIVE_QUERY_STEP = 15
SAMPLE_MAX_AGE = 60
MEMORY_MAX_SKEW = 30
HOSTNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
ROOT_PREFIX = r"/slurm/uid_[0-9]+/job_"
ROOT_CGROUP = re.compile(ROOT_PREFIX + r"([1-9][0-9]*)")
ROOT_SELECTOR = ROOT_PREFIX + r"[1-9][0-9]*"
RESOURCE_BY_METRIC = {CPU_METRIC: "cpu", MEMORY_METRIC: "memory"}
STOP_CODES = {"invalid_hpc_token", "permission_denied", "hpc_rate_limited", "monitor_budget",
              "hpc_network_permission", "hpc_proxy_unreachable"}


class PrometheusRateLimit:
    """Share the gateway request budget across browser sessions and query modes."""

    def __init__(self, rpm, clock=monotonic):
        self.rpm, self.clock = rpm, clock
        self.calls, self.retry_at, self.lock = deque(), 0.0, Lock()
        self.request_spacing = 0.0

    def acquire(self):
        with self.lock:
            now = self.clock()
            while self.calls and now - self.calls[0] >= 60:
                self.calls.popleft()
            next_slot = self.calls[0] + 60 if len(self.calls) >= self.rpm else now
            paced_slot = self.calls[-1] + self.request_spacing if self.calls else now
            delay = max(next_slot, paced_slot, self.retry_at) - now
            if delay > 0:
                error = SlurmJobsError("monitor_budget", "已达到监控请求预算，等待下一次可用额度。")
                error.retry_after = delay
                raise error
            self.calls.append(now)

    def defer(self, delay=60):
        with self.lock:
            self.request_spacing = min(60, max(60 / self.rpm, self.request_spacing * 2))
            self.retry_at = max(self.retry_at, self.clock() + max(delay, self.request_spacing))


def expand_hostlist(expression):
    """Expand Slurm's comma-separated hosts and numeric bracket groups, retaining padding."""
    if not isinstance(expression, str) or not expression:
        return ()
    groups, depth, offset = [], 0, 0
    for index, char in enumerate(expression):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "," and depth == 0:
            groups.append(expression[offset:index])
            offset = index + 1
        if depth < 0 or depth > 1:
            return ()
    if depth:
        return ()
    groups.append(expression[offset:])
    hosts = {}
    for group in groups:
        names, offset = [""], 0
        for match in re.finditer(r"\[([^\[\]]+)\]", group):
            numbers = []
            for member in match[1].split(","):
                interval = member.split("-")
                if len(interval) > 2 or not all(re.fullmatch(r"[0-9]{1,9}", endpoint) for endpoint in interval):
                    return ()
                first, last = int(interval[0]), int(interval[-1])
                if not 0 <= last - first < 4096:
                    return ()
                width = len(interval[0]) if interval[0].startswith("0") else 1
                numbers.extend(str(value).zfill(width) for value in range(first, last + 1))
                if len(numbers) * len(names) > 4096:
                    return ()
            prefix = group[offset:match.start()]
            names = [name + prefix + number for name in names for number in numbers]
            offset = match.end()
        for name in names:
            name += group[offset:]
            if not HOSTNAME.fullmatch(name):
                return ()
            hosts[name] = None
        if len(hosts) > 4096:
            return ()
    return tuple(sorted(hosts))


def _prometheus_series(request, metric, hostname, end, *, suffix="", label=None, values=()):
    selector = f"hostname={json.dumps(hostname)}"
    if label:
        value = values[0] if len(values) == 1 else "|".join(re.escape(item) for item in values)
        op = "=" if len(values) == 1 else "=~"
        selector += f",{label}{op}{json.dumps(value)}"
    payload = request("/prometheus/query", {"query": f"{metric}{{{selector}}}{suffix}", "time": end})
    return [(tags, values) for tags, values in _series(payload, "matrix" if suffix else "vector")
            if tags.get("hostname") == hostname]


def _series(payload, expected):
    data = payload.get("data") if isinstance(payload, dict) else None
    if (not isinstance(data, dict) or payload.get("status") != "success" or payload.get("warnings")
            or data.get("resultType") != expected or not isinstance(data.get("result"), list)):
        raise SlurmJobsError("job_monitor_invalid", "Prometheus 未返回可信的作业监控序列。")
    return [(entry["metric"], entry.get("values")) for entry in data["result"]
            if isinstance(entry, dict) and isinstance(entry.get("metric"), dict)
            and all(isinstance(value, str) for value in entry["metric"].values())]


def resolve_job_cgroup(request, job_id, hostname, end):
    labels = _prometheus_series(request, "cgroup_info", hostname, end, label="jobid", values=(str(job_id),))
    paths = {tags["cgroup"] for tags, _ in labels
             if tags.get("jobid") == str(job_id)
             and (root := ROOT_CGROUP.fullmatch(tags.get("cgroup", ""))) and root[1] == str(job_id)}
    if len(paths) != 1:
        code = "job_cgroup_missing" if not paths else "job_cgroup_ambiguous"
        raise SlurmJobsError(code, "Prometheus 未找到唯一的作业根 cgroup。")
    return paths.pop()


def read_job_cpu_counter(request, hostname, path, start, end):
    """Read a job-root counter with its original scrape timestamps."""
    matches = [values for tags, values in _prometheus_series(
        request, CPU_METRIC, hostname, end, suffix=f"[{end - start + 1}s]", label="cgroup", values=(path,),
    ) if tags.get("cgroup") == path]
    if len(matches) != 1:
        code = "job_cpu_missing" if not matches else "job_cpu_ambiguous"
        raise SlurmJobsError(code, "Prometheus 未返回唯一的作业 CPU 计数序列。")
    points = _points(matches[0], start, end, counter=False)
    if not points:
        raise SlurmJobsError("job_cpu_no_points", "CPU 计数序列缺少有效的时间戳或数值。")
    if any(right[1] < left[1] for left, right in zip(points, points[1:])):
        raise SlurmJobsError("job_cpu_reset", "实测窗口内 CPU 计数器发生重置。")
    return points


def _live_selector(metric, hostname):
    return f'{metric}{{hostname={json.dumps(hostname)},cgroup=~"{ROOT_SELECTOR}"}}'


def _live_range_series(request, hosts, start, end):
    # query_range labels points with evaluation times. Read the scrape timestamps
    # alongside the values so repeated evaluations cannot pass as fresh samples.
    terms = []
    for metric, resource in RESOURCE_BY_METRIC.items():
        selectors = [_live_selector(metric, hostname) for hostname in hosts]
        values = " or ".join(selectors)
        timestamps = " or ".join(f"timestamp({selector})" for selector in selectors)
        terms.extend((f'label_replace(({values}),"hpc_sample","{resource}","hostname",".+")',
                      f'label_replace(({timestamps}),"hpc_sample","{resource}_time","hostname",".+")'))
    payload = request("/prometheus/query_range", {
        "query": " or ".join(terms), "start": start, "end": end, "step": LIVE_QUERY_STEP,
    })
    pairs = defaultdict(lambda: defaultdict(list))
    for tags, values in _series(payload, "matrix"):
        kind = tags.get("hpc_sample", "")
        resource = kind.removesuffix("_time")
        time_series = resource != kind
        if resource not in RESOURCE_BY_METRIC.values() or tags.get("hostname") not in hosts:
            continue
        identity = tuple(sorted((key, value) for key, value in tags.items()
                                if key not in {"__name__", "hpc_sample"}))
        pairs[resource, identity]["times" if time_series else "values"].append(values)
    for (resource, identity), paired in pairs.items():
        tags = dict(identity)
        if len(paired["values"]) != 1 or len(paired["times"]) != 1:
            # Keep the rejected identity visible to the duplicate-series check.
            yield resource, tags, ()
            continue
        stamps = dict(_points(paired["times"][0], start, end, counter=False))
        samples = {}
        for evaluated, value in _points(paired["values"][0], start, end, counter=False):
            stamp = stamps.get(evaluated)
            if stamp is not None and start <= stamp <= evaluated:
                if (samples and stamp < next(reversed(samples))) or samples.setdefault(stamp, value) != value:
                    samples.clear()
                    break
        yield resource, tags, sorted(samples.items())


def _points(raw, start, end, *, counter):
    points = []
    for entry in raw or []:
        try:
            raw_time, raw_value = entry
            stamp, value = float(raw_time), float(raw_value)
        except (ValueError, TypeError, OverflowError):
            return ()
        if not isfinite(stamp) or not isfinite(value) or value < 0:
            return ()
        if not start <= stamp <= end:
            continue
        if points and (stamp <= points[-1][0] or (counter and value < points[-1][1])):
            return ()
        points.append((stamp, value))
    return points


def _value_at(points, timestamp, *, interpolate=False):
    index = bisect_right(points, (timestamp, float("inf"))) - 1
    left, value = points[index]
    if interpolate and left < timestamp:
        right, following = points[index + 1]
        return value + (following - value) * (timestamp - left) / (right - left)
    return value


def combine_cpu_series(hosts, per_node, start, end, *, minimum_seconds=None, max_sample_age=SAMPLE_MAX_AGE):
    series = [_points(per_node.get(node), start, end, counter=True) for node in hosts]
    if not series or any(len(line) < 2 for line in series):
        return None
    left = max(line[0][0] for line in series)
    right = min(line[-1][0] for line in series)
    required = (end - start) * MIN_WINDOW_COVERAGE if minimum_seconds is None else minimum_seconds
    if right <= left or right - left < required or end - right > max_sample_age:
        return None
    used = sum(_value_at(line, right, interpolate=True) - _value_at(line, left, interpolate=True)
               for line in series)
    return [(left, 0.0), (right, used)]


def combine_memory_series(hosts, per_node, start, end, *, max_sample_age=SAMPLE_MAX_AGE):
    series = [_points(per_node.get(node), start, end, counter=False) for node in hosts]
    if not series or not all(series):
        return None
    at = min(line[-1][0] for line in series)
    if end - at > max_sample_age:
        return None
    aligned = [bisect_right(line, (at, float("inf"))) - 1 for line in series]
    if any(index < 0 or at - line[index][0] > MEMORY_MAX_SKEW for line, index in zip(series, aligned)):
        return None
    return {"memory_used_bytes": int(sum(line[index][1] for line, index in zip(series, aligned))),
            "memory_observed_at": at, "memory_max_skew_seconds": max(at - line[index][0] for line, index in zip(series, aligned))}


def fresh_series(raw, end):
    points = _points(raw, end - SAMPLE_MAX_AGE, end, counter=False)
    return bool(points)


class CgroupMonitor:
    """Map expected job IDs to job-root paths and query raw samples at one anchor."""

    def __init__(self):
        self.live_batch_nodes = LIVE_BATCH_NODES

    def collect_sweep(self, request, jobs, *, width, clock, priority=None):
        """Read each assigned node once, with the shared rate limiter on every request."""
        by_host = defaultdict(list)
        for job_id, hosts in jobs.items():
            for host in hosts:
                by_host[host].append(job_id)
        preferred = set(jobs.get(priority, ()))
        hosts = sorted(by_host, key=lambda host: (host not in preferred, host))
        groups = []
        for offset in range(0, len(hosts), LIVE_BATCH_NODES):
            by_job = defaultdict(list)
            for host in hosts[offset:offset + LIVE_BATCH_NODES]:
                for job_id in by_host[host]:
                    by_job[job_id].append(host)
            groups.append(by_job)

        def read(group):
            end = int(clock())
            return CgroupMonitor().collect_live(request, group, end - width, end)

        samples = {"cpu": defaultdict(dict), "memory": defaultdict(dict)}
        coverage = {"nodes_total": len(hosts), "nodes_visited": 0, "nodes_attempted": 0,
                    "errors": {}, "monitor_mode": "bounded_sweep"}
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="hpc-monitor") as workers:
            for chunk, result in workers.map(read, groups):
                coverage["nodes_visited"] += result["nodes_visited"]
                coverage["nodes_attempted"] += result["nodes_attempted"]
                coverage["errors"].update(result["errors"])
                for metric, metrics_by_job in chunk.items():
                    for job_id, lines in metrics_by_job.items():
                        samples[metric][job_id].update(lines)
        return samples, coverage

    def collect_live(self, request, jobs, start, end, *, node_offset=0, priority=None):
        by_node = defaultdict(set)
        for job_id, hosts in jobs.items():
            for host in hosts:
                by_node[host].add(job_id)
        nodes = sorted(by_node)
        if nodes:
            offset = node_offset % len(nodes)
            nodes = nodes[offset:] + nodes[:offset]
        if priority in jobs:
            preferred = set(jobs[priority])
            nodes.sort(key=lambda host: host not in preferred)

        samples = {"cpu": defaultdict(dict), "memory": defaultdict(dict)}
        visited = {"cpu": set(), "memory": set()}
        errors, index = {}, 0

        def store(series):
            matched = defaultdict(list)
            for resource, tags, points in series:
                host, path = tags.get("hostname"), tags.get("cgroup", "")
                root = ROOT_CGROUP.fullmatch(path)
                if root and int(root[1]) in by_node.get(host, ()):
                    matched[resource, host, int(root[1])].append(points)
            for (resource, host, job_id), lines in matched.items():
                if len(lines) == 1:
                    points = _points(lines[0], start, end, counter=False)
                    if points:
                        samples[resource][job_id][host] = points

        while index < len(nodes):
            if self.live_batch_nodes:
                batch = nodes[index:index + self.live_batch_nodes]
                try:
                    store(_live_range_series(request, batch, start, end))
                except SlurmJobsError as error:
                    if error.code == "prometheus_query_too_long" and self.live_batch_nodes > 1:
                        self.live_batch_nodes //= 2
                        continue
                    if error.code in {"prometheus_query_rejected", "prometheus_query_too_long"}:
                        self.live_batch_nodes = 0
                        continue
                    errors[error.code] = str(error)
                    if error.code in STOP_CODES:
                        break
                    if error.code in {"hpc_timeout", "hpc_response_too_large"}:
                        self.live_batch_nodes = max(1, self.live_batch_nodes // 2)
                else:
                    for hosts in visited.values():
                        hosts.update(batch)
                index += len(batch)
                continue

            hostname = nodes[index]
            stopped = False
            for metric, resource in RESOURCE_BY_METRIC.items():
                try:
                    payload = request("/prometheus/query", {
                        "query": f"{_live_selector(metric, hostname)}[{end - start + 1}s]", "time": end,
                    })
                    store((resource, tags, values) for tags, values in _series(payload, "matrix")
                          if tags.get("hostname") == hostname)
                except SlurmJobsError as error:
                    errors[error.code] = str(error)
                    if error.code in STOP_CODES:
                        stopped = True
                        break
                else:
                    visited[resource].add(hostname)
            if stopped:
                if hostname in visited["cpu"] or hostname in visited["memory"]:
                    index += 1
                break
            index += 1
        return samples, {
            "nodes_total": len(nodes), "nodes_visited": len(visited["cpu"] | visited["memory"]),
            "nodes_attempted": index, "visited_hosts": visited, "errors": errors,
            "monitor_mode": "batched_range" if self.live_batch_nodes else "node_range",
        }

    def collect(self, request, jobs, start, end, *, memory=True, node_offset=0, priority=None):
        by_node = defaultdict(set)
        for job_id, hosts in jobs.items():
            for host in hosts:
                by_node[host].add(job_id)
        nodes = sorted(by_node)
        if nodes:
            offset = node_offset % len(nodes)
            nodes = nodes[offset:] + nodes[:offset]
            if priority in jobs:
                wanted = set(jobs[priority])
                nodes.sort(key=lambda node: node not in wanted)
        samples = {"cpu": defaultdict(dict), "memory": defaultdict(dict)}
        errors, visited = {}, []
        duration = f"[{end - start + 1}s]"
        for hostname in nodes:
            try:
                ids = sorted(by_node[hostname])
                for offset in range(0, len(ids), 32):
                    batch = set(ids[offset:offset + 32])
                    labels = _prometheus_series(request, "cgroup_info", hostname, end, label="jobid",
                                                 values=tuple(str(job_id) for job_id in sorted(batch)))
                    matches = defaultdict(set)
                    for labels_row, _ in labels:
                        path = labels_row.get("cgroup") or ""
                        root = ROOT_CGROUP.fullmatch(path) if isinstance(path, str) else None
                        if root and labels_row.get("jobid") == root[1] and int(root[1]) in batch:
                            matches[int(root[1])].add(path)
                    paths = {next(iter(paths)): job_id for job_id, paths in matches.items() if len(paths) == 1}
                    if not paths:
                        continue
                    metrics = (("cpu", CPU_METRIC), ("memory", MEMORY_METRIC)) if memory else (("cpu", CPU_METRIC),)
                    for resource, metric in metrics:
                        found = defaultdict(list)
                        for tags, values in _prometheus_series(request, metric, hostname, end, suffix=duration,
                                                               label="cgroup", values=tuple(sorted(paths))):
                            path = tags.get("cgroup")
                            if isinstance(path, str) and path in paths:
                                found[paths[path]].append(values)
                        for job_id, series in found.items():
                            if len(series) == 1:
                                samples[resource][job_id][hostname] = series[0]
            except SlurmJobsError as error:
                errors[error.code] = str(error)
                if error.code in STOP_CODES:
                    break
            visited.append(hostname)
        return samples, {"nodes_total": len(nodes), "nodes_visited": len(visited),
                         "visited_hosts": visited, "errors": errors}


@dataclass(frozen=True)
class JobCpuMonitor:
    metric: str
    hostname: str
    job_label: str

    def __post_init__(self):
        if not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", self.metric):
            raise ValueError("作业 CPU 监控指标名无效")
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", self.job_label):
            raise ValueError("作业 CPU 监控标签名无效")
        if not self.hostname.strip() or self.job_label in {"job", "hostname", "__name__"}:
            raise ValueError("监控需要已核实的主机名和作业 ID 标签")

    def samples(self, get_json, ids, start, end):
        ids = sorted(set(ids))
        samples = {}
        for offset in range(0, len(ids), 50):
            batch = {str(job_id) for job_id in ids[offset:offset + 50]}
            id_pattern = "|".join(sorted(batch))
            query = (f'{self.metric}{{hostname={json.dumps(self.hostname)},'
                     f'{self.job_label}=~"{id_pattern}"}}[{end - start + 1}s]')
            payload = get_json("/prometheus/query", {"query": query, "time": end})
            data = payload.get("data") or {}
            if payload.get("status") != "success" or payload.get("warnings") or data.get("resultType") != "matrix":
                raise ValueError("Prometheus 未返回可用的作业 CPU 历史序列")
            for series in data["result"]:
                labels = series["metric"]
                job_id = labels.get(self.job_label)
                if labels.get("hostname") != self.hostname or job_id not in batch:
                    continue
                key = int(job_id)
                samples[key] = None if key in samples else series.get("values")
        return samples
