"""Local, read-only HPC heatmap service. Credentials stay in this process."""
import argparse
from datetime import datetime, timezone
from getpass import getpass
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.efficiency import allocated_cpu, calculate_job_efficiency

BASE = "https://api.hpc.sjtu.edu.cn"
ROOT = Path(__file__).resolve().parent
NO_VALUE = 4294967294


def number(value):
    if isinstance(value, dict):
        return value.get("number") if value.get("set") and not value.get("infinite") else None
    return value


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp and timestamp != NO_VALUE else None


def completed_summary(job):
    timing = job.get("time", {})
    metric = calculate_job_efficiency(job)
    return {"id": str(job["job_id"]), "state": "COMPLETED", "partition": job.get("partition") or "未知", "cpus": allocated_cpu(job),
            "started_at": iso(timing.get("start")), "ended_at": iso(timing.get("end")), "elapsed_seconds": timing.get("elapsed"),
            "suspended_seconds": timing.get("suspended"), **metric}


def running_summary(job):
    return {"id": str(job["job_id"]), "state": "RUNNING", "partition": job.get("partition") or "未知",
            "cpus": number(job.get("cpus")), "started_at": iso(number(job.get("start_time"))),
            "efficiency": None, "reason": "运行中的累计 CPU 时间尚不可可靠获取"}


class DataSource:
    def __init__(self, username, password, proxy):
        self.username, self.password, self.proxy = username, password, proxy
        self.token = None
        self.cache = {}
        self.locks = {"running": threading.Lock(), "completed": threading.Lock()}
        self.auth_lock = threading.Lock()

    def session(self):
        session = requests.Session()
        session.trust_env = False
        if self.proxy:
            session.proxies.update({"http": self.proxy, "https": self.proxy})
        return session

    def fetch(self, view):
        ttl = 10
        with self.locks[view]:
            cached = self.cache.get(view)
            if cached and time.monotonic() - cached[0] < ttl:
                return cached[1]
            with self.session() as session:
                with self.auth_lock:
                    if not self.token:
                        auth = session.post(BASE + "/token", json={"domain": "jaccount", "operator": self.username, "password": self.password}, timeout=(10, 25))
                        auth.raise_for_status()
                        self.token = auth.text.strip()
                    token = self.token
                now = int(time.time())
                path = "/slurm/jobs" if view == "running" else "/slurmdb/jobs"
                params = None if view == "running" else {"start_time": now - 300, "end_time": now}
                response = session.get(BASE + path, params=params, headers={"Authorization": token}, timeout=(10, 60))
                if response.status_code == 401:
                    with self.auth_lock:
                        self.token = None
                    raise RuntimeError("登录已过期，请重试")
                response.raise_for_status()
                data = response.json()
                if data.get("errors"):
                    raise RuntimeError("数据源返回错误")
                if view == "running":
                    jobs = [running_summary(job) for job in data["jobs"] if "RUNNING" in job.get("job_state", [])]
                else:
                    jobs = [completed_summary(job) for job in data["jobs"] if "COMPLETED" in job.get("state", {}).get("current", []) and now - 300 <= job.get("time", {}).get("end", 0) <= now]
                jobs.sort(key=lambda job: job.get("ended_at") or job.get("started_at") or "", reverse=True)
                result = {"view": view, "queried_at": iso(now), "source_updated_at": iso(number(data.get("last_update"))),
                          "window_start": iso(now - 300) if view == "completed" else None, "jobs": jobs}
                self.cache[view] = (time.monotonic(), result)
                return result


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path != "/api/v1/job-heatmap":
            return super().do_GET()
        view = parse_qs(url.query).get("view", ["completed"])[0]
        if view not in ("completed", "running"):
            return self.send_json(400, {"error": "无效的视图"})
        try:
            data = self.server.source.fetch(view)
        except (requests.RequestException, ValueError, KeyError, RuntimeError):
            return self.send_json(502, {"error": "数据获取失败，请检查连接和登录状态后重试"})
        self.send_json(200, data)

    def do_POST(self):
        if urlsplit(self.path).path != "/api/v1/cpu-efficiency":
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

    def send_json(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--proxy", default=os.environ.get("HPC_PROXY", "http://127.0.0.1:7900"))
    args = parser.parse_args()
    username = os.environ.get("HPC_USERNAME") or input("JAccount 用户名: ")
    password = os.environ.get("HPC_PASSWORD") or getpass("JAccount 密码: ")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.source = DataSource(username, password, args.proxy)
    print(f"打开 http://127.0.0.1:{args.port}/", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
