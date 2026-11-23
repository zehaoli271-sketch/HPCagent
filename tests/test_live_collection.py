from contextlib import closing
from datetime import datetime, timezone
from http.client import HTTPConnection
import json
import re
from threading import Event, Thread
import unittest
from unittest.mock import Mock

from agent.chat import ChatState, ConversationRun
from core.job_monitor import CgroupMonitor, PrometheusRateLimit, combine_cpu_series, combine_memory_series, expand_hostlist
from core.live_collection import LiveCollector, allocation_from_job
from core.slurm_jobs import SlurmJobsError
from frontend.demo import demo_cgroup_response
from frontend.server import DataSource, DemoDataSource, create_server


STAMP = 1795400000
PATH = "/slurm/uid_1000/job_10006"
JOB_NODE_PAIR = ("demo01", "demo02")
JOB = {"job_id": 10006, "job_state": ["RUNNING"], "cpus": {"set": True, "number": 8},
       "start_time": {"set": True, "number": STAMP - 1000}, "tres_alloc_str": "cpu=8,mem=8G",
       "nodes": "demo[01-02]", "partition": "example-cpu"}


class LiveCollectionTest(unittest.TestCase):
    def test_hostlist_supports_expansion_and_rejects_invalid_input(self):
        self.assertEqual(expand_hostlist("demo[01-02,05],rack[1-2]n[003-004],cas100"),
                         ("cas100", "demo01", "demo02", "demo05", "rack1n003", "rack1n004", "rack2n003", "rack2n004"))
        for bad in ("demo[02-01]", "demo[1,,2]", 'node1"}', "a[1]}", "a[0-999999]", "a[", "a[]"):
            self.assertEqual(expand_hostlist(bad), ())

    def test_query_labels_select_matching_job_roots_and_concrete_nodes(self):
        calls = []

        def prometheus(path, params):
            calls.append((path, params))
            hostname = "demo01" if 'hostname="demo01"' in params["query"] else "demo02"
            label = {"hostname": hostname, "cgroup": PATH, "jobid": "10006"}
            if params["query"].startswith("cgroup_info"):
                self.assertIn('jobid="10006"', params["query"])
                return {"status": "success", "data": {"resultType": "vector", "result": [
                    {"metric": {**label, "cgroup": PATH + "/step_0"}}, {"metric": label},
                    {"metric": {**label, "hostname": "unexpected"}},
                ]}}
            self.assertIn(f'cgroup="{PATH}"', params["query"])
            return {"status": "success", "data": {"resultType": "matrix", "result": [
                {"metric": label, "values": [[STAMP - 300, "1000"], [STAMP, "1300"]]},
                {"metric": {**label, "cgroup": PATH + "/step_0"}, "values": [[STAMP, "2500"]]},
            ]}}

        frame, coverage = CgroupMonitor().collect(prometheus, {10006: ("demo01", "demo02")}, STAMP - 300, STAMP)
        self.assertEqual(len(calls), 6)
        self.assertEqual(coverage["nodes_visited"], 2)
        self.assertEqual(combine_cpu_series(JOB_NODE_PAIR, frame["cpu"][10006], STAMP - 300, STAMP)[-1], (STAMP, 600))
        self.assertEqual(combine_memory_series(JOB_NODE_PAIR, frame["memory"][10006], STAMP - 300, STAMP)["memory_used_bytes"], 2600)

    def test_missing_nodes_reset_and_stale_samples_leave_metrics_unavailable(self):
        start, end = STAMP - 300, STAMP
        samples = {"demo01": [(start, 100), (end, 120)], "demo02": [(start, 200), (end, 230)]}
        self.assertIsNotNone(combine_cpu_series(JOB_NODE_PAIR, samples, start, end))
        self.assertIsNone(combine_cpu_series(JOB_NODE_PAIR, {"demo01": samples["demo01"]}, start, end))
        self.assertIsNone(combine_cpu_series(JOB_NODE_PAIR, {**samples, "demo02": [(start, 200), (end, 50)]}, start, end))
        self.assertIsNone(combine_memory_series(JOB_NODE_PAIR, {**samples, "demo02": [(end - 90, 200)]}, start, end))

    def test_worker_calculates_snapshot_updates_deduplicates_and_invalidates_stale_auth(self):
        time = Mock(return_value=STAMP)
        running = dict(JOB)
        catalog = Mock(side_effect=lambda: {"jobs": [running, {**running, "user_name": "ignored"}]})
        source = DemoDataSource()
        collector = LiveCollector(catalog, source.get_json, clock=time)
        collector.refresh_catalog()
        collector.collect_once()
        initial = collector.snapshot()
        self.assertEqual(initial["coverage"]["catalog_count"], 1)
        self.assertIsNone(initial["jobs"][0]["efficiency"])
        self.assertEqual(initial["jobs"][0]["memory_used_bytes"], 2 * 1024 ** 3)
        time.return_value += 90
        collector.refresh_catalog()
        collector.collect_once()
        next_frame = collector.snapshot()
        self.assertEqual(next_frame["snapshot_count"], 2)
        self.assertAlmostEqual(next_frame["jobs"][0]["efficiency"], 0.25)
        self.assertAlmostEqual(next_frame["jobs"][0]["memory_ratio"], 0.25)
        running["cpus"] = 16
        running["tres_alloc_str"] = "cpu=16,mem=8G"
        time.return_value += 30
        collector.refresh_catalog()
        collector.collect_once()
        self.assertIsNone(collector.snapshot()["jobs"][0]["efficiency"])
        catalog.side_effect = SlurmJobsError("hpc_timeout", "模拟 Slurm 超时")
        collector.refresh_catalog()
        collector.collect_once()
        self.assertEqual(collector.snapshot()["status"], "partial")
        self.assertEqual(collector.snapshot()["catalog_error"]["code"], "hpc_timeout")
        time.return_value += 121
        collector.collect_once()
        self.assertEqual(collector.snapshot()["status"], "stale")
        catalog.side_effect = SlurmJobsError("invalid_hpc_token", "模拟权限令牌失效")
        collector.refresh_catalog()
        self.assertEqual(collector.snapshot()["jobs"], [])
        self.assertTrue(collector.stop_event.is_set())

    def test_live_batches_evaluate_3017_jobs_even_when_slurm_refresh_times_out(self):
        now = STAMP - STAMP % 15
        clock = Mock(return_value=now)
        jobs = [{**JOB, "job_id": 10000 + index, "nodes": f"node{index % 64:02}", "cpus": None} for index in range(3017)]
        self.assertEqual(allocation_from_job(jobs[0]).cpus, 8)
        catalog = Mock(side_effect=[{"jobs": jobs}, SlurmJobsError("hpc_timeout", "模拟 Slurm 超时")])
        calls = []

        def prometheus(path, params, *, timeout):
            self.assertEqual(path, "/prometheus/query_range")
            self.assertEqual(params["step"], 15)
            hosts = set(re.findall(r'hostname="(node[0-9]+)"', params["query"]))
            self.assertLessEqual(len(hosts), 8)
            self.assertIn("timestamp(cgroup_cpu_total_seconds{", params["query"])
            self.assertIn("timestamp(cgroup_memory_used_bytes{", params["query"])
            calls.append(hosts)
            end = params["end"]
            result = []
            for job in jobs:
                host = job["nodes"]
                if host not in hosts:
                    continue
                labels = {"hostname": host, "cgroup": f"/slurm/uid_1000/job_{job['job_id']}"}
                for resource, values in (("cpu", ("100", "220")), ("memory", ("8", "8"))):
                    result.extend((
                        {"metric": {**labels, "hpc_sample": resource},
                         "values": [[end - 60, values[0]], [end, values[1]]]},
                        {"metric": {**labels, "hpc_sample": resource + "_time"},
                         "values": [[end - 60, str(end - 60)], [end, str(end)]]},
                    ))
            return {"status": "success", "data": {"resultType": "matrix", "result": result}}

        collector = LiveCollector(catalog, prometheus, clock=clock, rpm=30)
        collector.refresh_catalog()
        collector.collect_once()
        self.assertEqual(collector.snapshot()["coverage"]["cpu_evaluated"], 0)
        clock.return_value = now + 60
        collector.refresh_catalog()
        collector.collect_once()
        frame = collector.snapshot()
        self.assertEqual(frame["coverage"]["catalog_count"], 3017)
        self.assertEqual(frame["coverage"]["nodes_visited"], 64)
        self.assertEqual(frame["coverage"]["cpu_evaluated"], 3017)
        self.assertEqual(frame["coverage"]["memory_evaluated"], 3017)
        self.assertEqual(frame["jobs"][0]["efficiency"], 0.25)
        self.assertEqual(frame["catalog_error"]["code"], "hpc_timeout")
        self.assertEqual(len(calls), 16)

    def test_live_ranges_use_scrape_timestamps_and_reject_ambiguous_root(self):
        def prometheus(_, params):
            payload = demo_cgroup_response(params)
            for row in list(payload["data"]["result"]):
                host, kind = row["metric"]["hostname"], row["metric"]["hpc_sample"]
                if (host, kind) == ("demo01", "cpu_time"):
                    row["values"] = [[evaluated, str(STAMP - 310)] for evaluated, _ in row["values"]]
                if (host, kind) == ("demo02", "memory"):
                    payload["data"]["result"].append({**row, "metric": {**row["metric"], "instance": "duplicate"}})
            return payload

        frames, coverage = CgroupMonitor().collect_live(prometheus, {10006: JOB_NODE_PAIR}, STAMP - 300, STAMP)
        self.assertEqual(coverage["nodes_visited"], 2)
        self.assertEqual(set(frames["cpu"][10006]), {"demo02"})
        self.assertEqual(set(frames["memory"][10006]), {"demo01"})

    def test_in_flight_catalog_refresh_does_not_block_new_metrics(self):
        occupied, finish = Event(), Event()
        clock = Mock(return_value=STAMP)

        def slow_catalog():
            occupied.set()
            finish.wait(3)
            return {"jobs": [{**JOB, "tres_alloc_str": "cpu=16,mem=8G"}]}

        catalog = Mock(return_value={"jobs": [JOB]})
        collector = LiveCollector(catalog, DemoDataSource().get_json, clock=clock)
        collector.refresh_catalog()
        clock.return_value += 90
        catalog.side_effect = slow_catalog
        refresh = Thread(target=collector.refresh_catalog)
        refresh.start()
        try:
            self.assertTrue(occupied.wait(1))
            collector.collect_once()
            self.assertTrue(refresh.is_alive())
            self.assertEqual(collector.snapshot()["jobs"][0]["efficiency"], 0.25)
        finally:
            finish.set()
            refresh.join(3)
        collector.collect_once()
        self.assertEqual(collector.snapshot()["jobs"][0]["cpus"], 16)
        self.assertIsNone(collector.snapshot()["jobs"][0]["efficiency"])

    def test_live_ranges_fall_back_on_gateway_rejection_with_cpu_before_memory(self):
        calls = []

        def prometheus(path, params):
            calls.append((path, params["query"]))
            if path == "/prometheus/query_range":
                raise SlurmJobsError("prometheus_query_rejected", "模拟网关拒绝批量查询")
            if params["query"].startswith("cgroup_memory_used_bytes"):
                raise SlurmJobsError("hpc_timeout", "模拟内存查询超时")
            return demo_cgroup_response(params)

        frames, coverage = CgroupMonitor().collect_live(prometheus, {10006: JOB_NODE_PAIR}, STAMP - 300, STAMP)
        self.assertEqual(coverage["monitor_mode"], "node_range")
        self.assertEqual(coverage["nodes_visited"], 2)
        self.assertEqual(coverage["nodes_attempted"], 2)
        self.assertIn("hpc_timeout", coverage["errors"])
        self.assertEqual(len(calls), 5)
        self.assertTrue(calls[1][1].startswith("cgroup_cpu_total_seconds"))
        self.assertEqual(set(frames["cpu"][10006]), set(JOB_NODE_PAIR))
        self.assertFalse(frames["memory"])

    def test_respects_shared_rate_budget_and_stops_on_429(self):
        clock = Mock(return_value=100)
        limit = PrometheusRateLimit(3, clock=clock)
        for _ in range(3):
            limit.acquire()
        with self.assertRaises(SlurmJobsError):
            limit.acquire()
        limit.defer(90)
        clock.return_value = 161
        with self.assertRaises(SlurmJobsError):
            limit.acquire()
        clock.return_value = 190
        limit.acquire()
        reader = Mock(side_effect=SlurmJobsError("hpc_rate_limited", "模拟 429"))
        _, coverage = CgroupMonitor().collect(reader, {10006: JOB_NODE_PAIR}, STAMP - 300, STAMP)
        reader.assert_called_once()
        self.assertIn("hpc_rate_limited", coverage["errors"])

    def test_heatmap_and_chat_use_same_snapshot_without_requerying_upstream(self):
        source = DemoDataSource()
        source.demo_started = STAMP - 1000
        source.enable_collection(PrometheusRateLimit(30), 30)
        source.collector.clock = lambda: STAMP
        source.get_json = Mock(wraps=source.get_json)
        source.collector.refresh_catalog()
        source.collector.collect_once()
        count = source.get_json.call_count
        with create_server(source, 0) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=5)) as browser:
                    browser.request("GET", "/api/v1/job-heatmap?view=running")
                    response = browser.getresponse()
                    dashboard = json.loads(response.read())
                    self.assertEqual(response.status, 200)
                    browser.request("POST", "/api/v1/chat", json.dumps({
                        "chat_id": "a" * 32, "question": "查询当前 CPU 利用率低于30%的作业",
                    }), {"Content-Type": "application/json"})
                    response = browser.getresponse()
                    events = [json.loads(line) for line in response.read().splitlines()]
                    report = next(event["data"] for event in events if event["type"] == "result")
                    self.assertTrue(report["live"])
                    self.assertEqual(report["records"], dashboard["jobs"])
                    self.assertEqual(report["fetched_at"], dashboard["queried_at"])
                    self.assertEqual(source.get_json.call_count, count)
            finally:
                server.shutdown()
                worker.join()
        self.assertTrue(source.collector.stop_event.is_set())

    def test_running_dashboard_receives_new_revision_after_all_batches_finish(self):
        blocked, release = Event(), Event()
        source = DemoDataSource()
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        collector = source.collector
        collector.clock = lambda: STAMP
        jobs = [JOB, {**JOB, "job_id": 10007, "nodes": "extra[01-08]"}]
        collector.catalog = lambda: {"jobs": jobs}
        limited = 4

        def read(path, params, *, timeout):
            nonlocal limited
            if 'hostname="extra08"' in params["query"]:
                if limited:
                    limited -= 1
                    error = SlurmJobsError("hpc_rate_limited", "模拟网关限流")
                    error.retry_after = 0
                    raise error
                blocked.set()
                if not release.wait(5):
                    raise AssertionError("test did not release the second batch")
            return demo_cgroup_response(params)

        collector.prometheus = read
        collector.refresh_catalog()
        with create_server(source, 0) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            sweep = Thread(target=collector.collect_once, daemon=True)
            try:
                with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=6)) as browser:
                    browser.request("GET", "/api/v1/live-updates")
                    response = browser.getresponse()
                    self.assertEqual(response.readline(), b"data: 0\n")
                    self.assertEqual(response.readline(), b"\n")
                    sweep.start()
                    self.assertTrue(blocked.wait(3))
                    self.assertEqual(collector.snapshot()["status"], "warming_up")
                    self.assertEqual(collector.snapshot()["collection_revision"], 0)
                    release.set()
                    self.assertEqual(response.readline(), b"data: 1\n")
                    snapshot = collector.snapshot()
                    self.assertEqual(snapshot["collection_revision"], 1)
                    self.assertEqual(snapshot["coverage"]["nodes_attempted"], 10)
                    self.assertEqual(snapshot["coverage"]["nodes_visited"], 10)
                    self.assertEqual(limited, 0)
                    self.assertEqual(snapshot["jobs"][0]["efficiency"], 0.25)
                    self.assertIsNone(snapshot["jobs"][1]["efficiency"])
                    self.assertGreaterEqual(snapshot["collection_seconds"], 0)
            finally:
                release.set()
                collector.stop()
                if sweep.ident is not None:
                    sweep.join(6)
                server.shutdown()
                worker.join(6)

    def test_full_pass_keeps_older_measurement_in_detail_and_excludes_it_from_current_screening(self):
        source = DemoDataSource()
        source.demo_started = STAMP - 1000
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        source.collector.clock = lambda: STAMP
        source.collector.refresh_catalog()
        source.collector.collect_once()
        reporter = ConversationRun(ChatState(), source, None, now=datetime.fromtimestamp(STAMP + 600, timezone.utc))
        report = reporter.run("查询当前 CPU 利用率低于30%的作业")
        self.assertEqual(report["records"], [])
        self.assertTrue(any("已超过 5 分钟" in note for note in report["notes"]))
        detail = ConversationRun(ChatState(), source, None).run("查询当前作业 ID 为10006 的 CPU 效率")
        self.assertEqual(detail["live_detail"]["efficiency"], 0.25)

    def test_live_query_keeps_high_usage_when_requesting_specific_job(self):
        source = DemoDataSource()
        source.demo_started = STAMP - 1000
        source.enable_collection(PrometheusRateLimit(30), 30)
        source.collector.clock = lambda: STAMP
        source.collector.refresh_catalog()
        source.collector.collect_once()
        report = ConversationRun(ChatState(), source, None).run("查询当前作业 ID 为10006 的 CPU 效率")
        self.assertEqual(report["live_detail"]["job_id"], 10006)
        self.assertEqual(report["live_detail"]["efficiency"], 0.25)

    def test_browser_connection_starts_and_stops_a_session_collector(self):
        demo = DemoDataSource()
        published = Event()
        limit = PrometheusRateLimit(30)
        connected = DataSource(None, None, None, token="fixture-token")

        def request(path, params=None, **kwargs):
            if path == "/slurm/jobs":
                return {"jobs": [JOB]}
            return demo.get_json(path, params, **kwargs)

        connected.get_json = Mock(side_effect=request)
        connected.enable_collection(limit, 30)
        connected.collector.clock = lambda: STAMP
        original_collect = connected.collector.collect_once

        def notify():
            original_collect()
            published.set()

        connected.collector.collect_once = notify
        with create_server(DataSource(None, None, None), 0, connection_factory=lambda _: connected) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=5)) as browser:
                    browser.request("POST", "/api/v1/hpc-connection", json.dumps({"token": "fixture-token"}),
                                    {"Content-Type": "application/json"})
                    response = browser.getresponse()
                    self.assertEqual(response.status, 200)
                    cookie = response.getheader("Set-Cookie").split(";", 1)[0]
                    self.assertNotIn("fixture-token", cookie)
                    response.read()
                    self.assertTrue(published.wait(3))

                    browser.request("GET", "/api/v1/job-heatmap?view=running")
                    self.assertEqual(json.loads(browser.getresponse().read())["jobs"], [])
                    browser.request("GET", "/api/v1/job-heatmap?view=running", headers={"Cookie": cookie})
                    live = json.loads(browser.getresponse().read())
                    self.assertEqual(live["jobs"][0]["memory_used_bytes"], 2 * 1024 ** 3)
                    self.assertEqual(connected.get_json.call_args_list[0].args[0], "/slurm/jobs")
                    self.assertEqual(sum(call.args[0] == "/slurm/jobs" for call in connected.get_json.call_args_list), 1)
                    browser.request("DELETE", "/api/v1/hpc-connection", headers={"Cookie": cookie})
                    self.assertEqual(browser.getresponse().status, 200)
                    self.assertTrue(connected.collector.stop_event.is_set())
            finally:
                server.shutdown()
                worker.join()
