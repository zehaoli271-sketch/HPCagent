"""Local, read-only heatmap and Agent diagnosis service."""
import argparse
from datetime import datetime, timezone
from functools import partial
from getpass import getpass
from http.client import HTTPException
from http.cookies import CookieError, SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
from secrets import token_urlsafe
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from agent.chat import PAGE_SIZE, ChatState, ConversationRun, public_row
from agent.web_diagnostics import DEFAULT_SCREENING_MODE, SCREENING_MODES, DiagnosisService, error_response
from agent.zhiyuan import DEFAULT_MODEL, ZhiyuanModel
from core.efficiency import allocated_cpu, calculate_job_efficiency
from core.job_monitor import CPU_METRIC, JobCpuMonitor, PrometheusRateLimit, combine_cpu_series
from core.live_collection import CollectionConflict, LiveCollector, allocation_from_job
from core.job_query import JobMemorySchema, SQLiteArchive, accounting_memory, epoch, safe_label, slurm_integer, step_usage, tres_count
from core.job_screening import DEFAULT_CPU_THRESHOLD
from core.slurm_jobs import HPC_TOKEN_ENV, SlurmJobsError, parse_slurm_jobs, read_hpc_json
from frontend.demo import chat_demo_jobs, demo_cgroup_response, demo_jobs, demo_monitor

BASE = "https://api.hpc.sjtu.edu.cn"
ROOT = Path(__file__).resolve().parent
NO_VALUE = 4294967294
CONNECTION_PATH = "/api/v1/hpc-connection"
COLLECTION_PATH = "/api/v1/live-collection"
COLLECTION_CONFLICT = {"status": "collection_conflict", "message": "后台采集正在使用 HPC 接口。是否中断采集后继续查询？"}
CHAT_PATH = "/api/v1/chat"
JOB_DETAIL_PATH = "/api/v1/job-detail"
LIVE_JOB_PATH = "/api/v1/live-job"
CHAT_PAGE_PATH = "/api/v1/chat-page"
CHAT_ID = re.compile(r"[a-f0-9-]{32,36}")
SESSION_COOKIE = "hpc_session"
POLL_SECONDS = 10
RUNNING_CATALOG_TIMEOUT = 40


def number(value):
    if isinstance(value, dict):
        return value.get("number") if value.get("set") and not value.get("infinite") else None
    return value


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp and timestamp != NO_VALUE else None


def completed_summary(job, memory_schema=None):
    timing = job.get("time", {})
    metric = calculate_job_efficiency(job)
    steps = [step_usage(step) for step in job.get("steps") or [] if isinstance(step, dict)]
    memory = accounting_memory(job, steps, completed=job["state"]["current"] == ["COMPLETED"], memory_schema=memory_schema)
    return {"id": str(job["job_id"]), "state": "COMPLETED", "partition": job.get("partition") or "未知", "cpus": allocated_cpu(job),
            "requested_cpus": tres_count((job.get("tres") or {}).get("requested"), "cpu"),
            "started_at": iso(timing.get("start")), "ended_at": iso(timing.get("end")), "elapsed_seconds": timing.get("elapsed"),
            "memory_ratio": memory["memory_allocated_ratio"], "memory_basis": memory["memory_basis"],
            "memory_used_bytes": memory["memory_used_bytes"], "memory_allocated_bytes": memory["memory_allocated_bytes"],
            "requested_memory_bytes": memory["requested_memory_bytes"],
            "suspended_seconds": timing.get("suspended"), **metric}


def running_summary(job, now):
    allocation = allocation_from_job(job)
    return {"id": str(job["job_id"]), "state": "RUNNING", "partition": job.get("partition") or "未知",
            "cpus": allocation.cpus, "requested_cpus": allocation.requested_cpus, "started_at": iso(allocation.started),
            "active_seconds": now - allocation.started if allocation.started and allocation.started <= now else None,
            "nodes": allocation.hosts, "memory_allocated_bytes": allocation.memory_bytes,
            "requested_memory_bytes": allocation.requested_memory_bytes,
            "efficiency": None, "reason": "运行中的步骤尚未写入 TotalCPU。实时 CPU 用量需读取作业级监控。"}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class DataSource:
    def __init__(self, username, password, proxy, *, token=None, monitor=None, memory_schema=None):
        self.username, self.password, self.proxy = username, password, proxy
        self.token = token
        self.monitor = monitor
        self.memory_schema = memory_schema
        proxy_handler = ProxyHandler() if proxy is None else ProxyHandler({"https": proxy} if proxy else {})
        self.opener = build_opener(proxy_handler, NoRedirect())
        self.cache = {}
        self.collector = None
        self.initial_catalog = None
        self.prometheus_limit = None
        self.locks = {"running": threading.Lock(), "completed": threading.Lock()}
        self.auth_lock = threading.Lock()

    @property
    def kind(self):
        return "hpc_api" if self.token or self.username else "disconnected"

    @property
    def cpu_source(self):
        return self.monitor.metric if self.monitor else CPU_METRIC if self.collector else None

    def enable_collection(self, rate_limit, interval, *, full_pass=False):
        self.prometheus_limit = rate_limit
        synthetic = self.kind == "synthetic"
        self.collector = LiveCollector(self.running_catalog, self.prometheus_json, interval=interval,
                                       rpm=rate_limit.rpm, synthetic=synthetic, full_pass=full_pass)

    def running_catalog(self):
        payload, self.initial_catalog = self.initial_catalog, None
        return payload if payload is not None else self.get_json(
            "/slurm/jobs", timeout=RUNNING_CATALOG_TIMEOUT, read_window=True,
        )

    def validate_connection(self):
        if self.collector:
            payload = self.running_catalog()
            if not isinstance(payload.get("jobs"), list):
                raise SlurmJobsError("hpc_catalog_invalid", "Slurm 当前作业列表格式无效。")
            if self.collector.full_pass:
                self.collector.seed_catalog(payload)
            self.initial_catalog = payload
        else:
            self.fetch("running")

    def start_collection(self):
        if self.collector and self.kind != "disconnected":
            self.collector.start()

    def stop_collection(self):
        if self.collector:
            self.collector.stop()

    def live_snapshot(self, job_id=None):
        if not self.collector:
            return {"status": "unavailable", "jobs": [], "coverage": None, "queried_at": None,
                    "error": {"message": "请连接已启用实时采集的 SJTU HPC 数据源。"}}
        if job_id is not None:
            self.collector.prioritize(job_id)
        return self.collector.snapshot()

    def prometheus_json(self, path, params, *, timeout):
        if self.prometheus_limit:
            self.prometheus_limit.acquire()
        try:
            return self.get_json(path, params, timeout=timeout, read_window=True)
        except SlurmJobsError as error:
            if self.prometheus_limit and error.code == "hpc_rate_limited":
                self.prometheus_limit.defer(getattr(error, "retry_after", 60))
            raise

    def get_json(self, path, params=None, *, timeout=60, read_window=False):
        if self.kind == "disconnected":
            raise SlurmJobsError("missing_hpc_token", "请先连接 SJTU HPC。")
        token = None
        deadline = time.monotonic() + timeout
        try:
            with self.auth_lock:
                if not self.token:
                    auth = Request(BASE + "/token", data=json.dumps({
                        "domain": "jaccount", "operator": self.username, "password": self.password,
                    }).encode("utf-8"), headers={"Content-Type": "application/json"}, method="POST")
                    with self.opener.open(auth, timeout=min(25, timeout)) as response:
                        self.token = response.read().decode("utf-8").strip()
                token = self.token
            url = BASE + path + (f"?{urlencode(params)}" if params else "")
            authorization = token if token.lower().startswith("bearer ") else f"Bearer {token}"
            headers = {"Accept": "application/json"}
            if read_window:
                headers["Accept-Encoding"] = "gzip"
            request = Request(url, headers=headers, method="GET")
            request.add_unredirected_header("Authorization", authorization)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SlurmJobsError("hpc_timeout", "HPC 数据源响应超时。")
            with self.opener.open(request, timeout=remaining) as response:
                data = read_hpc_json(response, deadline=deadline) if read_window else json.load(response)
        except HTTPError as error:
            if path.startswith("/prometheus/") and error.code in (400, 414, 422):
                code = "prometheus_query_too_long" if error.code == 414 else "prometheus_query_rejected"
                raise SlurmJobsError(code, f"Prometheus 拒绝了监控查询（HTTP {error.code}）。") from error
            if error.code == 401:
                with self.auth_lock:
                    if self.username and self.token == token:
                        self.token = None
                raise SlurmJobsError("invalid_hpc_token", "HPC 登录已过期或令牌无效。") from error
            if error.code == 403:
                raise SlurmJobsError("permission_denied", "当前账号没有读取该数据的权限。") from error
            if error.code in (408, 504):
                raise SlurmJobsError("hpc_timeout", f"HPC 数据源响应超时（HTTP {error.code}）。") from error
            if error.code in (502, 503):
                raise SlurmJobsError("hpc_upstream_unavailable", f"HPC 暂时不可用（HTTP {error.code}）。") from error
            if error.code == 429:
                retry_after = (error.headers or {}).get("Retry-After", "")
                failure = SlurmJobsError("hpc_rate_limited", "HPC 限制了查询频率（HTTP 429），请稍后续查。")
                if retry_after.isdecimal() and len(retry_after) < 10:
                    failure.retry_after = max(1, int(retry_after))
                raise failure from error
            raise SlurmJobsError("hpc_query_error", f"HPC 数据源请求失败（HTTP {error.code}）。") from error
        except OSError as error:
            reason = error.reason if isinstance(error, URLError) else error
            if isinstance(reason, TimeoutError):
                raise SlurmJobsError("hpc_timeout", "HPC 数据源响应超时。") from error
            if isinstance(reason, PermissionError):
                raise SlurmJobsError(
                    "hpc_network_permission",
                    "本地服务的外网连接被权限限制拦截。请从普通 PowerShell 启动服务后重试。",
                ) from error
            if isinstance(reason, ConnectionRefusedError) and self.proxy:
                raise SlurmJobsError(
                    "hpc_proxy_unreachable", "无法连接已配置的 HPC 代理，请检查代理是否正在运行。",
                ) from error
            if isinstance(reason, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
                raise SlurmJobsError("hpc_incomplete_response", "连接 HPC 数据源时传输中断。") from error
            raise SlurmJobsError("hpc_query_error", "无法连接 HPC 数据源，请检查当前网络或代理。") from error
        except HTTPException as error:
            raise SlurmJobsError("hpc_incomplete_response", "HPC 数据源连接提前结束。") from error
        except ValueError as error:
            raise SlurmJobsError("hpc_query_error", "HPC 数据源响应格式异常。") from error
        if not isinstance(data, dict) or data.get("errors"):
            raise SlurmJobsError("hpc_query_error", "HPC 数据源返回错误。")
        return data

    def read_accounting_window(self, start, end, timeout):
        return parse_slurm_jobs(self.get_json("/slurmdb/jobs", {"start_time": start, "end_time": end},
                                              timeout=timeout, read_window=True))

    def jobs_at(self, at):
        return self.read_accounting_window(at, at + 1, 25)

    def job_by_id(self, job_id):
        return parse_slurm_jobs(self.get_json(f"/slurmdb/job/{job_id}"))

    def monitor_samples(self, job_ids, start, end):
        if self.monitor is None:
            raise SlurmJobsError("job_monitor_unconfigured", "当前未配置经验证的作业级 CPU 监控指标。")
        try:
            request = (lambda path, params: self.prometheus_json(path, params, timeout=8)) if self.prometheus_limit else self.get_json
            return self.monitor.samples(request, job_ids, start, end)
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise SlurmJobsError("job_monitor_invalid", "监控接口没有返回可信的作业 CPU 序列。") from error

    def monitor_jobs(self, records, start, end):
        if self.monitor or not self.collector or self.kind == "synthetic":
            return self.monitor_samples([row["job_id"] for row in records], start, end)
        allocations = [allocation_from_job(row) for row in records]
        jobs = {job.job_id: job.hosts for job in allocations if job.hosts and job.stable}
        deadline = time.monotonic() + 25

        def request(path, params):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SlurmJobsError("monitor_budget", "历史监控读取已到时间上限。")
            return self.prometheus_json(path, params, timeout=min(remaining, 8))

        data, coverage = self.collector.monitor.collect(request, jobs, start, end, memory=False)
        if coverage["errors"] and not data["cpu"]:
            code, message = next(iter(coverage["errors"].items()))
            raise SlurmJobsError(code, message)
        return {job_id: combine_cpu_series(hosts, data["cpu"].get(job_id, {}), start, end)
                for job_id, hosts in jobs.items()}

    def fetch(self, view, *, refresh=False):
        if self.kind == "disconnected":
            return {"view": view, "queried_at": None, "source_updated_at": None,
                    "window_start": None, "jobs": [], "source_kind": self.kind}
        if view == "running" and self.collector:
            snapshot = self.live_snapshot()
            return {**snapshot, "view": view, "source_kind": self.kind, "source_updated_at": snapshot["queried_at"]}
        with self.locks[view]:
            checked_at = time.monotonic()
            cached = self.cache.get(view)
            if not refresh and cached and checked_at - cached[0] < POLL_SECONDS:
                return cached[1]
            now = int(time.time())
            path = "/slurm/jobs" if view == "running" else "/slurmdb/jobs"
            params = None if view == "running" else {"start_time": now - 300, "end_time": now}
            data = self.get_json(path, params)
            if view == "running":
                jobs = [running_summary(job, now) for job in data["jobs"] if "RUNNING" in job.get("job_state", [])]
            else:
                jobs = [completed_summary(job, self.memory_schema) for job in data["jobs"] if "COMPLETED" in job.get("state", {}).get("current", []) and now - 300 <= job.get("time", {}).get("end", 0) <= now]
            jobs.sort(key=lambda job: job.get("ended_at") or job.get("started_at") or "", reverse=True)
            result = {"view": view, "queried_at": iso(now), "source_updated_at": iso(number(data.get("last_update"))),
                      "window_start": iso(now - 300) if view == "completed" else None, "jobs": jobs, "source_kind": self.kind}
            self.cache[view] = (checked_at, result)
            return result


class DemoDataSource(DataSource):
    kind = "synthetic"
    cpu_source = "合成作业 CPU 序列"

    def __init__(self):
        super().__init__(None, None, None)
        self.demo_started = int(time.time()) - 600

    def get_json(self, path, params=None, **_):
        now = int(time.time())
        if path == "/slurmdb/jobs":
            if params["end_time"] - params["start_time"] == 1:
                return {"jobs": demo_jobs(params["start_time"])}
            return {"jobs": demo_jobs(params["end_time"] - 720)}
        if path == "/slurm/jobs":
            if self.collector:
                return {"jobs": [{"job_id": 10006, "job_state": ["RUNNING"], "cpus": 8,
                                  "start_time": self.demo_started, "nodes": "demo[01-02]",
                                  "tres_alloc_str": "cpu=8,mem=8G", "tres_req_str": "cpu=8,mem=8G", "partition": "example-cpu"}]}
            return {"jobs": [{"job_id": 10006, "job_state": ["RUNNING"], "cpus": 8,
                              "start_time": now - 600, "partition": "example-cpu"}]}
        if path in {"/prometheus/query", "/prometheus/query_range"}:
            return demo_cgroup_response(params)
        return {"jobs": [job for job in demo_jobs(now - 1200) if path == f"/slurmdb/job/{job['job_id']}"]}

    def monitor_samples(self, job_ids, start, end):
        return demo_monitor(job_ids, start, end)

    def chat_jobs(self, now):
        return chat_demo_jobs(now)


class ForegroundSource:
    """Reserve the query's connection on its first uncached HPC read."""

    def __init__(self, source):
        self.source = source
        self.collector = source.collector if source.kind == "hpc_api" else None
        self.admitted = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.admitted:
            self.collector.end_foreground_query()

    def __getattr__(self, name):
        method = getattr(self.source, name)
        if self.collector and name in {"read_accounting_window", "jobs_at", "job_by_id", "monitor_jobs"}:
            return partial(self.read, method)
        return method

    def read(self, method, *args, **kwargs):
        if not self.admitted:
            self.collector.begin_foreground_query()
            self.admitted = True
        return method(*args, **kwargs)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self):
        if self.command in {"GET", "HEAD"} and not urlsplit(self.path).path.startswith("/api/"):
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def browser_connection(self):
        try:
            cookies = SimpleCookie(self.headers.get("Cookie", ""))
        except CookieError:
            return None, None
        cookie = cookies.get(SESSION_COOKIE)
        if cookie is None:
            return None, None
        with self.server.connection_lock:
            return cookie.value, self.server.connections.get(cookie.value)

    def source(self):
        _, connected = self.browser_connection()
        return connected or self.server.source

    def connection_status(self, connected=None):
        if connected is None:
            _, connected = self.browser_connection()
        source = connected or self.server.source
        return {"source_kind": source.kind, "connected": connected is not None,
                "collection_enabled": source.collector.collection_enabled if source.kind == "hpc_api" and source.collector else None}

    def same_origin(self):
        origin = self.headers.get("Origin")
        port = self.server.server_port
        return not origin or origin in {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}

    def connect_hpc(self):
        if not self.same_origin() or self.headers.get_content_type() != "application/json":
            return self.send_json(403, {"error": "请通过本地页面连接 SJTU HPC。"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 8192:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            token = payload.get("token") if isinstance(payload, dict) else None
            if not isinstance(token, str):
                raise ValueError
            token = token.strip()
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            if not 0 < len(token) <= 4096 or not token.isascii() or not token.isprintable() or " " in token:
                raise ValueError
        except (ValueError, UnicodeError):
            return self.send_json(400, {"error": "请输入有效的 SJTU HPC 访问令牌。"})
        if self.server.source.kind == "synthetic":
            return self.send_json(409, {"error": "当前是演示服务。请打开普通 PowerShell 启动的服务网址，再连接 SJTU HPC。"})

        source = self.server.connection_factory(token)
        try:
            source.validate_connection()
        except SlurmJobsError as error:
            statuses = {"invalid_hpc_token": 401, "permission_denied": 403, "hpc_timeout": 504}
            return self.send_json(statuses.get(error.code, 502), {"error": str(error)})
        except (ValueError, KeyError, TypeError, AttributeError):
            return self.send_json(502, {"error": "HPC 运行作业接口返回的数据结构异常。"})

        old, _ = self.browser_connection()
        session_id = token_urlsafe(32)
        with self.server.connection_lock:
            previous = self.server.connections.pop(old, None)
            self.server.connections[session_id] = source
        if previous:
            previous.stop_collection()
        source.start_collection()
        self.send_json(200, self.connection_status(source),
                       cookie=f"{SESSION_COOKIE}={session_id}; Path=/; HttpOnly; SameSite=Strict")

    def change_live_collection(self):
        if not self.same_origin() or self.headers.get_content_type() != "application/json":
            return self.send_json(403, {"error": "请通过本地页面更改后台采集设置。"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 128:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict) or type(payload.get("enabled")) is not bool:
                raise ValueError
        except (ValueError, UnicodeError):
            return self.send_json(400, {"error": "请选择开启或暂停后台全量采集。"})
        _, connected = self.browser_connection()
        source = connected or self.server.source
        enabled = (source.collector.set_collection_enabled(payload["enabled"])
                   if source.kind == "hpc_api" and source.collector else None)
        if enabled is None:
            return self.send_json(409, {"error": "请先连接可用的 SJTU HPC 数据源。"})
        return self.send_json(200, self.connection_status(connected))

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == CONNECTION_PATH:
            return self.send_json(200, self.connection_status())
        if url.path == "/api/v1/live-updates":
            return self.live_updates()
        if url.path != "/api/v1/job-heatmap":
            return super().do_GET()
        query = parse_qs(url.query)
        view = query.get("view", ["completed"])[0]
        if view not in ("completed", "running"):
            return self.send_json(400, {"error": "无效的视图"})
        try:
            data = self.source().fetch(view, refresh=query.get("refresh") == ["1"])
        except SlurmJobsError as error:
            statuses = {"invalid_hpc_token": 401, "permission_denied": 403, "hpc_timeout": 504}
            return self.send_json(statuses.get(error.code, 502), {"error": str(error)})
        except (ValueError, KeyError, TypeError, AttributeError):
            return self.send_json(502, {"error": "数据获取失败，请检查连接和登录状态后重试"})
        self.send_json(200, data)

    def live_updates(self):
        collector = self.source().collector
        if not collector or not collector.full_pass or not collector.collection_enabled:
            self.send_response(204)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.connection.settimeout(10)
        previous = -1
        try:
            while True:
                revision = collector.wait_for_snapshot(previous)
                if not collector.collection_enabled:
                    self.wfile.write(b"event: stopped\ndata: {}\n\n")
                    self.wfile.flush()
                    return
                event = f"data: {revision}\n\n" if revision != previous else ": waiting\n\n"
                self.wfile.write(event.encode("utf-8"))
                self.wfile.flush()
                previous = revision
        except OSError:
            return

    def do_POST(self):
        path = urlsplit(self.path).path
        if path == CONNECTION_PATH:
            return self.connect_hpc()
        if path == COLLECTION_PATH:
            return self.change_live_collection()
        if path == CHAT_PATH:
            return self.chat()
        if path == CHAT_PAGE_PATH:
            return self.chat_page()
        if path == LIVE_JOB_PATH:
            return self.live_job()
        if path == JOB_DETAIL_PATH:
            return self.chat_job_detail()
        if path == "/api/v1/diagnoses":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16_384:
                    raise ValueError
                payload = json.loads(self.rfile.read(length))
            except (ValueError, UnicodeError):
                return self.send_json(*error_response("INVALID_REQUEST", "请输入有效的诊断请求。"))
            try:
                source = self.source()
                narrator = self.server.narrator if source.kind == "hpc_api" else None
                with ForegroundSource(source) as foreground:
                    outcome = DiagnosisService(foreground, narrator=narrator, threshold=self.server.threshold).diagnose(payload)
            except CollectionConflict:
                return self.send_json(409, COLLECTION_CONFLICT)
            except (TypeError, ValueError, AttributeError, KeyError):
                outcome = error_response("UPSTREAM_ERROR", "作业或监控数据结构无法识别。")
            return self.send_json(*outcome)
        if path != "/api/v1/cpu-efficiency":
            return self.send_json(404, {"error": "接口不存在"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 1_000_000:
                raise ValueError("invalid length")
            job = json.loads(self.rfile.read(length))
            if not isinstance(job, dict):
                raise ValueError("invalid job")
            result = calculate_job_efficiency(job)
        except (ValueError, TypeError, KeyError, AttributeError):
            return self.send_json(400, {"error": "请输入有效的 Slurm 作业 JSON"})
        self.send_json(200, result)

    def live_job(self):
        if not self.same_origin() or self.headers.get_content_type() != "application/json":
            return self.send_json(403, {"message": "请通过看板读取实时作业数据。"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 256:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            job_id, started = payload["job_id"], payload.get("started_at")
            if (not isinstance(job_id, str) or not re.fullmatch(r"[1-9][0-9]{0,15}", job_id)
                    or started is not None and (type(started) is not int or not epoch(started))):
                raise ValueError
        except (KeyError, AttributeError, TypeError, ValueError):
            return self.send_json(400, {"message": "单作业监控请求无效。"})
        source = self.source()
        if source.kind == "disconnected" or not source.collector:
            return self.send_json(401, {"message": "请先连接 SJTU HPC。"})
        try:
            result = source.collector.sample_job(int(job_id), started)
        except CollectionConflict:
            return self.send_json(409, COLLECTION_CONFLICT)
        except SlurmJobsError as issue:
            statuses = {"invalid_hpc_token": 401, "permission_denied": 403, "hpc_timeout": 504}
            return self.send_json(statuses.get(issue.code, 502), {"message": str(issue)})
        return self.send_json(200, result)

    def chat_page(self):
        if not self.same_origin() or self.headers.get_content_type() != "application/json":
            return self.send_json(403, {"message": "请通过本地页面查看查询结果。"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 512:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            chat_id, page_id, page = payload["chat_id"], payload["page_id"], payload["page"]
            if (not isinstance(chat_id, str) or not CHAT_ID.fullmatch(chat_id)
                    or not isinstance(page_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{22}", page_id)
                    or type(page) is not int or page < 1):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            return self.send_json(400, {"message": "页码或查询结果标识无效。"})
        session, connected = self.browser_connection()
        with self.server.chat_lock:
            state = self.server.chat_states.get((session if connected else None, chat_id))
            saved = state.result_pages.get(page_id) if state else None
        if saved is None:
            return self.send_json(404, {"message": "这一轮结果已过期，请重新查询。"})
        rows, live = saved
        pages = (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE
        if page > pages:
            return self.send_json(400, {"message": "页码超出了本次结果的范围。"})
        offset = (page - 1) * PAGE_SIZE
        records = rows[offset:offset + PAGE_SIZE]
        if not live:
            records = [public_row(row) for row in records]
        return self.send_json(200, {"records": records, "page": page, "pages": pages, "offset": offset})

    def chat_job_detail(self):
        if not self.same_origin() or self.headers.get_content_type() != "application/json":
            return self.send_json(403, {"message": "请通过本地对话页面查看作业详情。"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError
            job_id, chat_id = payload.get("job_id"), payload.get("chat_id")
            job_ref, point_at = payload.get("job_ref"), payload.get("point_at")
            if (not isinstance(job_id, str) or not re.fullmatch(r"[1-9][0-9]{0,15}", job_id)
                    or not isinstance(chat_id, str) or not CHAT_ID.fullmatch(chat_id)
                    or (point_at is not None and not epoch(point_at))):
                raise ValueError
            job_id = int(job_id)
            if job_ref is not None:
                if (not isinstance(job_ref, list) or len(job_ref) != 7
                        or (job_ref[0] is not None and not safe_label(job_ref[0]))
                        or type(job_ref[1]) is not int or job_ref[1] != job_id
                        or any(value is not None and (type(value) is not int or slurm_integer(value) is None)
                               for value in job_ref[2:])):
                    raise ValueError
                job_ref = tuple(job_ref)
        except (ValueError, UnicodeError):
            return self.send_json(400, {"message": "作业详情请求无效。"})

        session, connected = self.browser_connection()
        with self.server.chat_lock:
            state = self.server.chat_states.get((session if connected else None, chat_id))
        try:
            with ForegroundSource(connected or self.server.source) as foreground:
                workflow = ConversationRun(state or ChatState(), foreground, self.server.archive,
                                           threshold=self.server.threshold, memory_schema=self.server.memory_schema)
                result = workflow.inspect_job(job_id, job_ref, point_at)
        except CollectionConflict:
            return self.send_json(409, COLLECTION_CONFLICT)
        except (ValueError, TypeError, AttributeError, KeyError):
            return self.send_json(502, {"message": "作业详情的数据结构无法读取。"})
        return self.send_json(200, result)

    def chat(self):
        if not self.same_origin() or self.headers.get_content_type() != "application/json":
            return self.send_json(403, {"message": "请通过本地页面发送对话。"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 16_384:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError
            question, chat_id = payload.get("question"), payload.get("chat_id")
            mode = payload.get("mode", DEFAULT_SCREENING_MODE)
            if (not isinstance(question, str) or not 0 < len(question.strip()) <= 1200
                    or not isinstance(chat_id, str) or not CHAT_ID.fullmatch(chat_id)
                    or mode not in SCREENING_MODES):
                raise ValueError
        except (ValueError, UnicodeError):
            return self.send_json(400, {"message": "请输入有效的对话消息，最多 1200 字。"})

        session, connected = self.browser_connection()
        source = connected or self.server.source
        key = (session if connected else None, chat_id)
        with self.server.chat_lock:
            states = self.server.chat_states
            state = states.pop(key, ChatState())
            states[key] = state
            if not state.lock.acquire(blocking=False):
                return self.send_json(409, {"message": "上一条查询仍在运行，请等待进度结束。"})
            if len(states) > 8:
                oldest_idle = next((old for old, entry in states.items() if not entry.lock.locked()), None)
                if oldest_idle:
                    del states[oldest_idle]
        try:
            self.close_connection = True
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "close")
            self.end_headers()

            def emit(event):
                self.wfile.write((json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"))
                self.wfile.flush()

            # Streaming is on a dedicated ThreadingHTTPServer worker. Demo uses local fixtures only.
            narrator = self.server.narrator if source.kind != "synthetic" else None
            try:
                with ForegroundSource(source) as foreground:
                    workflow = ConversationRun(state, foreground, self.server.archive, narrator,
                                               threshold=self.server.threshold, emit=emit, memory_schema=self.server.memory_schema)
                    result = workflow.run(question.strip(), mode)
            except CollectionConflict:
                result = COLLECTION_CONFLICT
            except (ValueError, TypeError, AttributeError, KeyError, SlurmJobsError):
                result = {"status": "needs_data", "message": "本次查询的作业数据结构无法读取，请缩小范围后重试。"}
            if result.get("pages", 1) > 1:
                page_id = token_urlsafe(16)
                with self.server.chat_lock:
                    if len(state.result_pages) >= 8:
                        del state.result_pages[next(iter(state.result_pages))]
                    state.result_pages[page_id] = (state.ordered, bool(result.get("live")))
                result["page_id"] = page_id
            emit({"type": "result", "data": result})
            workflow.narrate(result)
            emit({"type": "done"})
        except ConnectionError:
            pass
        finally:
            state.lock.release()

    def do_DELETE(self):
        if urlsplit(self.path).path != CONNECTION_PATH:
            return self.send_json(404, {"error": "接口不存在"})
        if not self.same_origin():
            return self.send_json(403, {"error": "请通过本地页面断开连接。"})
        session_id, _ = self.browser_connection()
        with self.server.connection_lock:
            source = self.server.connections.pop(session_id, None)
        if source:
            source.stop_collection()
        self.send_json(200, self.connection_status(),
                       cookie=f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0")

    def send_json(self, status, data, *, cookie=None):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        self.wfile.write(body)


class HpcServer(ThreadingHTTPServer):
    def server_close(self):
        self.source.stop_collection()
        with self.connection_lock:
            for source in self.connections.values():
                source.stop_collection()
        super().server_close()


def create_server(source, port, *, narrator=None, threshold=DEFAULT_CPU_THRESHOLD, connection_factory=None, archive=None,
                  memory_schema=None):
    server = HpcServer(("127.0.0.1", port), Handler)
    server.source = source
    server.narrator, server.threshold = narrator, threshold
    server.connections = {}
    server.connection_lock = threading.Lock()
    server.archive = archive
    server.memory_schema = memory_schema
    server.chat_states = {}
    server.chat_lock = threading.Lock()
    server.connection_factory = connection_factory or (
        lambda token: DataSource(None, None, source.proxy, token=token, monitor=source.monitor, memory_schema=memory_schema)
    )
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--proxy", default=os.environ.get("HPC_PROXY"), help="HPC 代理地址，默认使用系统设置")
    parser.add_argument("--demo", action="store_true", help="使用合成作业和监控样本")
    parser.add_argument("--archive", help="可选的只读作业 SQLite 归档路径，默认跳过")
    parser.add_argument("--collect-interval", type=int, default=30, help="后台采集间隔（秒）")
    parser.add_argument("--prometheus-rpm", type=int, default=30, help="本服务每分钟的 Prometheus 请求预算")
    parser.add_argument("--job-memory-peak-field", default=os.environ.get("HPC_JOB_MEMORY_PEAK_FIELD"),
                        help="经核实的作业级并发峰值字段路径，峰值以字节表示")
    parser.add_argument("--job-memory-unit-bytes", type=int, default=os.environ.get("HPC_JOB_MEMORY_UNIT_BYTES"),
                        help="经核实的 Slurm 作业级内存 MB 到字节的换算")
    parser.add_argument("--jaccount", action="store_true", help="在终端输入 JAccount 登录凭据")
    parser.add_argument("--cpu-threshold", type=float, default=DEFAULT_CPU_THRESHOLD,
                        help="CPU 候选阈值，使用 0 到 1 的比值")
    args = parser.parse_args()
    if not 0 < args.cpu_threshold <= 1:
        parser.error("--cpu-threshold 须大于 0 且不超过 1")
    if args.collect_interval < 10 or args.prometheus_rpm < 3:
        parser.error("采集间隔至少 10 秒，Prometheus 请求预算至少 3 次/分钟。")
    memory_schema = None
    if args.job_memory_peak_field or args.job_memory_unit_bytes is not None:
        if not args.job_memory_peak_field:
            parser.error("请为内存换算同时配置已核实的峰值字段。")
        try:
            memory_schema = JobMemorySchema(args.job_memory_peak_field, args.job_memory_unit_bytes)
        except ValueError as error:
            parser.error(str(error))
    monitor_config = [os.environ.get(name, "").strip() for name in (
        "SJTU_HPC_JOB_CPU_METRIC", "SJTU_HPC_JOB_CPU_HOSTNAME", "SJTU_HPC_JOB_CPU_ID_LABEL",
    )]
    if any(monitor_config) and not all(monitor_config):
        parser.error("作业监控配置需要指标、主机名和作业 ID 标签")
    try:
        monitor = JobCpuMonitor(*monitor_config) if all(monitor_config) else None
    except ValueError as error:
        parser.error(str(error))
    if args.demo:
        source = DemoDataSource()
    else:
        token = os.environ.get(HPC_TOKEN_ENV, "").strip()
        username = None if token else os.environ.get("HPC_USERNAME")
        password = None if token else os.environ.get("HPC_PASSWORD")
        if not token and args.jaccount:
            username = username or input("JAccount 用户名: ")
            password = password or getpass("JAccount 密码: ")
        if bool(username) != bool(password):
            parser.error("请同时配置 HPC_USERNAME 和 HPC_PASSWORD，或在网页输入访问令牌。")
        source = DataSource(username, password, args.proxy, token=token or None, monitor=monitor, memory_schema=memory_schema)
    api_key = os.environ.get("SJTU_MODEL_API_KEY", "").strip()
    narrator = ZhiyuanModel(api_key, os.environ.get("SJTU_MODEL_ID") or DEFAULT_MODEL) if api_key else None
    rate_limit = PrometheusRateLimit(args.prometheus_rpm)
    source.enable_collection(rate_limit, args.collect_interval, full_pass=not args.demo)
    def connection_factory(token):
        connected = DataSource(None, None, args.proxy, token=token, monitor=monitor, memory_schema=memory_schema)
        connected.enable_collection(rate_limit, args.collect_interval, full_pass=True)
        return connected

    archive = SQLiteArchive(args.archive) if args.archive and not args.demo else None
    with create_server(source, args.port, narrator=narrator, threshold=args.cpu_threshold,
                       connection_factory=connection_factory, archive=archive, memory_schema=memory_schema) as server:
        source.start_collection()
        print(f"打开 http://127.0.0.1:{server.server_port}/", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
