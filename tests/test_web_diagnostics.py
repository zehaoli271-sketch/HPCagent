from http.client import HTTPConnection
import gzip
from io import BytesIO
import json
from threading import Thread
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from agent.web_diagnostics import DiagnosisService
from core.job_monitor import JobCpuMonitor
from core.job_query import JobMemorySchema
from core.job_screening import find_jobs_running_at, get_job_cpu_usage_window
from core.slurm_jobs import SlurmJobsError, read_hpc_json
from frontend.demo import demo_jobs
from frontend.server import DataSource, DemoDataSource, create_server


AT = 1795338000
QUESTION = "帮我查看2026年11月22日17:00时刻的低效作业"
RANKING_QUESTION = "帮我查询2026年11月22日22:00最低效的三个作业"


class WebDiagnosisTest(unittest.TestCase):
    def test_demo_does_not_try_external_hpc_connection(self):
        factory = Mock()
        with create_server(DemoDataSource(), 0, connection_factory=factory) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            client = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
            try:
                client.request("POST", "/api/v1/hpc-connection", json.dumps({"token": "fixture-token"}),
                               {"Content-Type": "application/json"})
                reply = client.getresponse()
                self.assertEqual(reply.status, 409)
                self.assertIn("演示服务", json.load(reply)["error"])
                self.assertIsNone(reply.getheader("Set-Cookie"))
                factory.assert_not_called()
            finally:
                client.close()
                server.shutdown()
                worker.join(timeout=3)

    def test_connection_reports_permission_and_proxy_failures_separately(self):
        for error, proxy, expected in (
            (PermissionError(13, "socket permission denied"), None, "hpc_network_permission"),
            (ConnectionRefusedError(61, "proxy refused"), "http://127.0.0.1:10080", "hpc_proxy_unreachable"),
            (TimeoutError("upstream timed out"), None, "hpc_timeout"),
        ):
            with self.subTest(expected=expected):
                source = DataSource(None, None, proxy, token="synthetic-token")
                source.opener = Mock()
                source.opener.open.side_effect = URLError(error)
                with self.assertRaises(SlurmJobsError) as caught:
                    source.get_json("/slurm/jobs")
                self.assertEqual(caught.exception.code, expected)

        for status in (408, 504):
            with self.subTest(status=status):
                source = DataSource(None, None, None, token="synthetic-token")
                source.opener = Mock()
                source.opener.open.side_effect = HTTPError("https://example.invalid", status, "Timeout", {}, None)
                with self.assertRaises(SlurmJobsError) as caught:
                    source.get_json("/slurmdb/jobs")
                self.assertEqual(caught.exception.code, "hpc_timeout")

    def test_live_poll_refreshes_after_ten_seconds_and_manual_refresh_bypasses_cache(self):
        source = DataSource(None, None, None, token="synthetic-token")
        source.get_json = Mock(side_effect=[
            {"jobs": [{"job_id": job_id, "job_state": ["RUNNING"]}]} for job_id in (1, 2, 3)
        ])
        with patch("frontend.server.time.monotonic") as tick:
            tick.return_value = 100
            self.assertEqual(source.fetch("running")["jobs"][0]["id"], "1")
            tick.return_value = 105
            self.assertEqual(source.fetch("running")["jobs"][0]["id"], "1")
            tick.return_value = 110
            self.assertEqual(source.fetch("running")["jobs"][0]["id"], "2")
            self.assertEqual(source.fetch("running", refresh=True)["jobs"][0]["id"], "3")
        self.assertEqual(source.get_json.call_count, 3)

    def test_completed_heatmap_reports_cores_and_memory_against_allocation(self):
        job = demo_jobs(AT)[3]
        source = DataSource(None, None, None, token="synthetic-token")
        source.get_json = Mock(return_value={"jobs": [job]})
        with patch("frontend.server.time.time", return_value=AT):
            row = source.fetch("completed")["jobs"][0]
            self.assertEqual((row["cpus"], row["requested_cpus"]), (8, 8))
            self.assertEqual(row["memory_ratio"], 0.375)
            self.assertEqual(row["memory_basis"], "seff")

            job["steps"][0]["tres"].pop("requested")
            row = source.fetch("completed", refresh=True)["jobs"][0]
            self.assertIsNone(row["memory_ratio"])
            self.assertEqual(row["memory_allocated_bytes"], 8 * 1024 ** 3)

            source.memory_schema = JobMemorySchema("confirmed_peak", 10 ** 6)
            job["confirmed_peak"] = 2_048_000_000
            row = source.fetch("completed", refresh=True)["jobs"][0]
            self.assertEqual(row["memory_ratio"], 0.25)
            self.assertEqual(row["memory_basis"], "concurrent_peak")

    def test_connection_switches_heatmap_and_diagnostics_for_only_that_browser(self):
        disconnected = DataSource(None, None, None)
        gateway = Mock()

        def upstream(request, *, timeout):
            if request.get_header("Authorization") != "Bearer synthetic-token":
                raise HTTPError(request.full_url, 401, "Unauthorized", {}, None)
            if request.full_url.endswith("/slurm/jobs"):
                return BytesIO(b'{"jobs":[{"job_id":10001,"job_state":["RUNNING"]}]}')
            if request.full_url.endswith("/slurmdb/job/10001"):
                return BytesIO(json.dumps({"jobs": demo_jobs(AT)[:1]}).encode())
            self.fail(f"Unexpected request to {request.full_url}")

        gateway.open.side_effect = upstream

        def connect(token):
            source = DataSource(None, None, None, token=token)
            source.opener = gateway
            return source

        with create_server(disconnected, 0, connection_factory=connect) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            browser = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
            cookie = None
            try:
                def request(method, path, payload=None, *, send_cookie=True):
                    headers = {"Content-Type": "application/json"}
                    if send_cookie and cookie:
                        headers["Cookie"] = cookie
                    body = json.dumps(payload) if payload is not None else None
                    browser.request(method, path, body, headers=headers)
                    reply = browser.getresponse()
                    return reply.status, json.load(reply), reply.getheader("Set-Cookie")

                path = "/api/v1/hpc-connection"
                _, initial, _ = request("GET", "/api/v1/job-heatmap?view=running")
                self.assertEqual(initial["source_kind"], "disconnected")
                self.assertEqual(initial["jobs"], [])
                gateway.open.assert_not_called()
                status, connected, set_cookie = request("POST", path, {"token": "Bearer synthetic-token"})
                self.assertEqual((status, connected["source_kind"]), (200, "hpc_api"))
                self.assertIn("HttpOnly", set_cookie)
                self.assertNotIn("synthetic-token", set_cookie)
                cookie = set_cookie.split(";", 1)[0]
                _, heatmap, _ = request("GET", "/api/v1/job-heatmap?view=running")
                self.assertEqual(heatmap["jobs"][0]["id"], "10001")
                _, other, _ = request("GET", path, send_cookie=False)
                self.assertFalse(other["connected"])
                status, job, _ = request("POST", "/api/v1/diagnoses", {"question": "分析作业 10001"})
                self.assertEqual((status, job["data_source"]["kind"]), (200, "hpc_api"))

                status, _, error_cookie = request("POST", path, {"token": "synthetic-invalid"})
                self.assertEqual(status, 401)
                self.assertIsNone(error_cookie)
                self.assertTrue(request("GET", path)[1]["connected"])
                status, disconnected_response, expire = request("DELETE", path)
                self.assertEqual((status, disconnected_response["source_kind"]), (200, "disconnected"))
                self.assertIn("Max-Age=0", expire)
                self.assertFalse(request("GET", path)[1]["connected"])
            finally:
                browser.close()
                server.shutdown()
                worker.join(timeout=3)

    def test_running_at_respects_both_time_boundaries_and_uncertain_history(self):
        jobs = demo_jobs(AT)
        jobs[2]["time"]["suspended"] = 20
        jobs[4]["time"]["start"] = AT

        selected, uncertain = find_jobs_running_at(jobs, AT)

        self.assertEqual([job["job_id"] for job in selected], [10001, 10002, 10005])
        self.assertEqual(uncertain, 1)

    def test_counter_measurement_rejects_short_coverage_and_reset(self):
        job = demo_jobs(AT)[0]
        start = AT - 300
        short = get_job_cpu_usage_window(job, [(start, 100), (start + 60, 200)], start, AT)
        reset = get_job_cpu_usage_window(job, [(start, 100), (start + 120, 50), (AT, 300)], start, AT)
        valid = get_job_cpu_usage_window(job, [(start + 15, 100), (AT - 15, 370)], start, AT)

        self.assertIsNone(short["utilization"])
        self.assertIsNone(reset["utilization"])
        self.assertEqual(valid["utilization"], 0.125)
        self.assertEqual(valid["observed_start"], start + 15)
        self.assertEqual(valid["observed_end"], AT - 15)

    def test_monitor_queries_one_configured_series_per_authorized_job(self):
        monitor = JobCpuMonitor("example_job_cpu_seconds_total", "example-exporter", "job_id")
        calls = []

        def response(path, params):
            calls.append((path, params))
            return {"status": "success", "data": {"resultType": "matrix", "result": [
                {"metric": {"job_id": "10001", "hostname": "example-exporter"}, "values": [[AT, "30"]]},
                {"metric": {"job_id": "10002", "hostname": "example-exporter"}, "values": [[AT, "35"]]},
                {"metric": {"job_id": "10002", "hostname": "example-exporter"}, "values": [[AT, "40"]]},
                {"metric": {"job_id": "99999", "hostname": "example-exporter"}, "values": [[AT, "50"]]},
            ]}}

        samples = monitor.samples(response, [10001, 10002], AT - 300, AT)

        self.assertEqual(samples, {10001: [[AT, "30"]], 10002: None})
        self.assertEqual(calls, [("/prometheus/query", {"time": AT, "query":
            'example_job_cpu_seconds_total{hostname="example-exporter",job_id=~"10001|10002"}[301s]'})])

    def test_live_missing_monitor_reports_unavailable_instead_of_using_lifetime(self):
        class LiveRecords(DataSource):
            def get_json(self, path, params=None, **_):
                return {"jobs": demo_jobs(params["start_time"])}

        service = DiagnosisService(LiveRecords(None, None, None, token="synthetic-token"))

        status, data = service.diagnose({"question": RANKING_QUESTION, "mode": "window"})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "insufficient_data")
        self.assertEqual(data["screening"]["evaluated_count"], 0)
        self.assertEqual(data["screening"]["unavailable_count"], 3)
        self.assertEqual(data["screening"]["coverage"], "not_verified")
        self.assertEqual(data["screening"]["top_n"], 3)
        self.assertIsNone(data["screening"]["threshold"])
        self.assertIn("无法给出该时刻的排名", data["message"])

    def test_point_uses_same_bounded_reader_and_reports_accounting_failure(self):
        source = DataSource(None, None, None, token="synthetic-token")
        source.read_accounting_window = Mock(side_effect=SlurmJobsError("hpc_timeout", "模拟记账超时"))
        status, result = DiagnosisService(source).diagnose({"question": QUESTION})
        self.assertEqual(status, 504)
        self.assertEqual(source.read_accounting_window.call_count, 2)
        self.assertEqual(result["history_coverage"]["windows_total"], 1)
        self.assertEqual(result["history_coverage"]["windows_completed"], 0)
        self.assertIsNone(result["screening"])

    def test_historical_transport_requests_and_decodes_gzip_or_plain_json(self):
        source = DataSource(None, None, None, token="synthetic-token")
        raw = json.dumps({"jobs": demo_jobs(AT)[:1]}).encode()
        for encoding, body in (("gzip", gzip.compress(raw)), ("identity", raw)):
            with self.subTest(encoding=encoding):
                response = BytesIO(body)
                response.headers = {"Content-Encoding": encoding}
                source.opener.open = Mock(return_value=response)
                result = source.read_accounting_window(AT, AT + 1, 25)
                self.assertEqual(result.jobs[0]["job_id"], 10001)
                request = source.opener.open.call_args.args[0]
                self.assertEqual(request.get_header("Accept-encoding"), "gzip")
                self.assertGreater(source.opener.open.call_args.kwargs["timeout"], 0)
        response = BytesIO(gzip.compress(raw)[:-5])
        response.headers = {"Content-Encoding": "gzip"}
        with self.assertRaises(SlurmJobsError) as error:
            read_hpc_json(response, deadline=float("inf"))
        self.assertEqual(error.exception.code, "hpc_incomplete_response")

    def test_lowest_three_ranks_without_a_threshold_and_preserves_mode(self):
        service = DiagnosisService(DemoDataSource())
        _, window = service.diagnose({"question": RANKING_QUESTION.replace("22:00", "22:00前五分钟")})
        _, lifetime = service.diagnose({"question": RANKING_QUESTION})

        self.assertEqual(window["status"], "insufficient_data")
        self.assertEqual(window["screening"]["mode"], "window")
        self.assertEqual(window["screening"]["at"], "2026-11-22T22:00:00+08:00")
        self.assertEqual(window["screening"]["top_n"], 3)
        self.assertEqual([job["job_id"] for job in window["screening"]["candidates"]], ["10001", "10002"])
        self.assertEqual(lifetime["status"], "complete")
        ranking = lifetime["screening"]
        self.assertIsNone(ranking["threshold"])
        self.assertEqual([job["job_id"] for job in ranking["candidates"]], ["10003", "10001", "10002"])
        self.assertGreater(ranking["candidates"][-1]["utilization"], 0.20)

    def test_lowest_arabic_count_can_limit_the_result(self):
        service = DiagnosisService(DemoDataSource())
        _, data = service.diagnose({"question": "查看2026-11-22 22:00 CPU 利用率最低的 1 个作业", "mode": "window"})

        self.assertEqual(data["status"], "complete")
        self.assertEqual(data["screening"]["top_n"], 1)
        self.assertEqual([job["job_id"] for job in data["screening"]["candidates"]], ["10001"])

    def test_demo_http_screening_modes_and_single_job_use_same_application(self):
        with create_server(DemoDataSource(), 0) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
            try:
                connection.request("GET", "/")
                response = connection.getresponse()
                self.assertIn(b'id="diagnosis-form"', response.read())
                self.assertEqual(response.status, 200)

                def diagnose(payload):
                    connection.request("POST", "/api/v1/diagnoses", json.dumps(payload),
                                       {"Content-Type": "application/json"})
                    reply = connection.getresponse()
                    self.assertEqual(reply.status, 200)
                    return json.load(reply)

                result = diagnose({"question": QUESTION})
                self.assertEqual(result["data_source"]["kind"], "synthetic")
                lifetime = result["screening"]
                self.assertEqual(lifetime["mode"], "lifetime")
                self.assertIsNone(lifetime["window_start"])
                self.assertEqual(lifetime["candidate_count"], 2)
                screen = diagnose({"question": QUESTION, "mode": "window"})["screening"]
                self.assertEqual(screen["at"], "2026-11-22T17:00:00+08:00")
                self.assertEqual(screen["window_start"], "2026-11-22T16:55:00+08:00")
                self.assertEqual(screen["running_count"], 3)
                self.assertEqual(screen["unavailable_count"], 1)
                self.assertEqual([(row["job_id"], row["utilization"]) for row in screen["candidates"]],
                                 [("10001", 0.125)])
                job = diagnose({"question": "分析作业 10001", "job_id": "10001"})["job"]
                self.assertEqual(job["cpu_efficiency"]["value"], 0.125)
                clarification = diagnose({"question": "分析作业 10002", "job_id": "10001"})
                self.assertEqual(clarification["status"], "needs_clarification")
            finally:
                connection.close()
                server.shutdown()
                worker.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
