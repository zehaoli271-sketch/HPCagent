from contextlib import closing
from datetime import datetime, timezone
from http.client import HTTPConnection
import json
import re
from threading import Event, Thread
import unittest
from unittest.mock import Mock

from agent.chat import ChatState, ConversationRun
from core.job_monitor import (
    CgroupMonitor, PrometheusRateLimit, combine_cpu_series, combine_memory_series, expand_hostlist,
    measure_cpu_series, measure_memory_series,
)
from core.live_collection import CollectionConflict, LiveCollector, allocation_from_job
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

    def test_four_node_memory_alignment_accepts_samples_on_either_side_of_earliest_latest_sample(self):
        nodes = ("demo01", "demo02", "demo03", "demo04")
        samples = {
            node: [(STAMP - 300 + offset, 80), (STAMP - 80 + offset, 100 + index)]
            for index, (node, offset) in enumerate(zip(nodes, (0, 7, 17, 24)))
        }
        result, reason = measure_memory_series(nodes, samples, STAMP - 900, STAMP, max_sample_age=360)
        self.assertIsNone(reason)
        self.assertEqual(result["memory_used_bytes"], 406)
        self.assertEqual(result["memory_observed_at"], STAMP - 56)
        self.assertEqual(result["memory_max_skew_seconds"], 24)

    def test_memory_alignment_can_use_prior_complete_sample_group_while_it_is_fresh(self):
        samples = {"demo01": [(STAMP - 320, 100), (STAMP - 20, 300)], "demo02": [(STAMP - 310, 200)]}
        result, reason = measure_memory_series(JOB_NODE_PAIR, samples, STAMP - 900, STAMP, max_sample_age=360)
        self.assertIsNone(reason)
        self.assertEqual(result["memory_used_bytes"], 300)
        self.assertEqual(result["memory_observed_at"], STAMP - 310)

    def test_missing_memory_sample_outcomes_are_reported_separately(self):
        cases = [
            ({"demo01": [(STAMP - 20, 100)]}, "1 个返回了有效内存样本"),
            ({"demo01": [(STAMP - 20, 100)], "demo02": [(STAMP - 400, 200)]}, "样本已过期"),
            ({"demo01": [(STAMP - 20, 100)], "demo02": [(STAMP - 130, 200)]}, "内存样本相差 110 秒"),
        ]
        for samples, expected in cases:
            with self.subTest(reason=expected):
                measured, reason = measure_memory_series(JOB_NODE_PAIR, samples, STAMP - 900, STAMP, max_sample_age=360)
                self.assertIsNone(measured)
                self.assertIn(expected, reason)

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
                    jobs.append({"job_id": 10008, "job_state": ["RUNNING"]})
                    collector.refresh_catalog()
                    initial = collector.snapshot()
                    self.assertEqual(initial["status"], "warming_up")
                    self.assertEqual(initial["collection_revision"], 0)
                    self.assertTrue(initial["catalog_only"])
                    self.assertEqual([row["id"] for row in initial["jobs"]], ["10006", "10007"])
                    self.assertTrue(all(row["efficiency"] is None and row["memory_ratio"] is None for row in initial["jobs"]))
                    release.set()
                    self.assertEqual(response.readline(), b"data: 1\n")
                    snapshot = collector.snapshot()
                    self.assertEqual(snapshot["collection_revision"], 1)
                    self.assertFalse(snapshot.get("catalog_only", False))
                    self.assertEqual([row["id"] for row in snapshot["jobs"]], ["10006", "10007", "10008"])
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

    def test_pause_interrupts_waiting_sweep_and_preserves_last_published_snapshot(self):
        waiting = Event()
        source = DemoDataSource()
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        collector = source.collector
        collector.clock = lambda: STAMP
        collector.catalog = lambda: {"jobs": [JOB]}
        collector.prometheus = lambda _path, params, **_: demo_cgroup_response(params)
        collector.refresh_catalog()
        previous = collector.collect_once()

        def rate_limited(*_args, **_kwargs):
            waiting.set()
            error = SlurmJobsError("monitor_budget", "模拟等待监控额度")
            error.retry_after = 120
            raise error

        normal = collector.prometheus
        collector.prometheus = Mock(side_effect=rate_limited)
        sweep = Thread(target=collector.collect_once, daemon=True)
        try:
            sweep.start()
            self.assertTrue(waiting.wait(3))
            self.assertFalse(collector.set_collection_enabled(False))
            paused = collector.snapshot()
            self.assertTrue(paused["collection_paused"])
            self.assertFalse(paused["updates_on_completion"])
            self.assertEqual(paused["jobs"], previous["jobs"])
            self.assertTrue(collector.set_collection_enabled(True))
            sweep.join(3)
            self.assertFalse(sweep.is_alive())
            self.assertEqual(collector.snapshot()["collection_revision"], 1)
            collector.prometheus.assert_called_once()
            self.assertIsNone(collector.collect_once())
            collector.prometheus = normal
            collector.refresh_catalog()
            self.assertIsNotNone(collector.collect_once())
            self.assertEqual(collector.snapshot()["collection_revision"], 2)
        finally:
            collector.stop()
            sweep.join(3)

    def test_live_query_keeps_high_usage_when_requesting_specific_job(self):
        source = DemoDataSource()
        source.demo_started = STAMP - 1000
        source.enable_collection(PrometheusRateLimit(30), 30)
        source.collector.clock = lambda: STAMP
        source.collector.refresh_catalog()
        source.collector.collect_once()
        report = ConversationRun(ChatState(), source, None).run("查询10006的当前效率")
        self.assertEqual(report["live_detail"]["job_id"], 10006)
        self.assertEqual(report["live_detail"]["efficiency"], 0.25)
        self.assertEqual(report["monitor_job"]["job_id"], 10006)

    def test_single_job_reads_only_its_nodes_and_measures_new_sample_timestamps(self):
        now = STAMP - 90
        source = DataSource(None, None, None, token="fixture-token")
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        collector = source.collector
        collector.clock = lambda: now
        collector.catalog = Mock(return_value={"jobs": [JOB, {**JOB, "job_id": 10007, "nodes": "other01"}]})
        collector.refresh_catalog()
        collector.prometheus = Mock(side_effect=lambda _path, params, **_: demo_cgroup_response(params))
        now = STAMP
        with self.assertRaises(CollectionConflict):
            collector.sample_job(10006)
        collector.prometheus.assert_not_called()
        collector.set_collection_enabled(False)

        first = collector.sample_job(10006)
        self.assertEqual(first["job"]["efficiency"], 0.25)
        self.assertEqual(first["job"]["memory_ratio"], 0.25)
        self.assertEqual(first["next_poll_seconds"], 15)
        query = collector.prometheus.call_args.args[1]["query"]
        self.assertIn('/slurm/uid_[0-9]+/job_10006', query)
        self.assertIn('hostname="demo01"', query)
        self.assertIn('hostname="demo02"', query)
        self.assertNotIn("other01", query)
        self.assertEqual(collector.snapshot()["collection_revision"], 0)

        now += 15
        second = collector.sample_job(10006, JOB["start_time"]["number"])
        self.assertGreater(second["job"]["cpu_observed_end"], first["job"]["cpu_observed_end"])
        self.assertGreater(second["job"]["memory_observed_at"], first["job"]["memory_observed_at"])
        self.assertEqual(collector.sample_job(10006, JOB["start_time"]["number"] + 1)["status"], "ended")

    def test_single_node_accepts_fractional_samples_five_minutes_apart(self):
        source = DataSource(None, None, None, token="fixture-token")
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        collector = source.collector
        collector.clock = lambda: STAMP
        collector.catalog = lambda: {"jobs": [
            {**JOB, "nodes": "demo01", "tres_alloc_str": "cpu=4,mem=8G", "resize_time": 0},
        ]}
        collector.refresh_catalog()
        collector.set_collection_enabled(False)
        first, last = STAMP - 433.666, STAMP - 133.666

        def read(path, params, *, timeout):
            self.assertEqual(path, "/prometheus/query")
            self.assertTrue(params["query"].endswith("[901s]"))
            self.assertIn('hostname="demo01"', params["query"])
            self.assertIn('job_10006', params["query"])
            values = ((1000, 1300) if params["query"].startswith("cgroup_cpu_total_seconds")
                      else (2 * 1024 ** 3, 2 * 1024 ** 3))
            return {"status": "success", "data": {"resultType": "matrix", "result": [
                {"metric": {"hostname": "demo01", "cgroup": PATH}, "values": list(zip((first, last), values))},
            ]}}

        collector.prometheus = Mock(side_effect=read)
        sample = collector.sample_job(10006)
        self.assertEqual(sample["job"]["efficiency"], 0.25)
        self.assertEqual(sample["job"]["memory_ratio"], 0.25)
        self.assertEqual(sample["job"]["cpu_seconds"], 300)
        self.assertEqual(datetime.fromisoformat(sample["job"]["cpu_observed_end"]).timestamp(), last)
        self.assertEqual(sample["max_sample_age_seconds"], 360)
        self.assertEqual(collector.prometheus.call_count, 2)

    def test_cpu_sample_failures_distinguish_missing_points_stale_and_counter_reset(self):
        cases = [
            ([(STAMP - 150, 3)], "两个 CPU 采样点"),
            ([(STAMP - 60, 3), (STAMP - 30, 4)], "共同 CPU 采样区间过短"),
            ([(STAMP - 700, 3), (STAMP - 400, 4)], "样本已过期"),
            ([(STAMP - 500, 4), (STAMP - 200, 3)], "累计计数发生回退"),
        ]
        for points, expected in cases:
            with self.subTest(reason=expected):
                measured, reason = measure_cpu_series(("demo01",), {"demo01": points}, STAMP - 900, STAMP,
                                                      minimum_seconds=60, max_sample_age=360, recent_seconds=300)
                self.assertIsNone(measured)
                self.assertIn(expected, reason)

    def test_pager_reads_original_result_after_a_later_question_without_upstream_calls(self):
        source = DemoDataSource()
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        source.collector.catalog = lambda: {"jobs": [{**JOB, "job_id": 10006 + index} for index in range(41)]}
        source.collector.clock = lambda: STAMP
        source.collector.refresh_catalog()
        source.get_json = Mock(side_effect=AssertionError("pagination must not query upstream"))
        with create_server(source, 0) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=5)) as browser:
                    def post(path, body):
                        browser.request("POST", path, json.dumps(body), {"Content-Type": "application/json"})
                        response = browser.getresponse()
                        return response.status, response.read()

                    chat = {"chat_id": "a" * 32, "question": "查询当前所有正在运行的作业"}
                    status, data = post("/api/v1/chat", chat)
                    self.assertEqual(status, 200)
                    result = next(event["data"] for event in map(json.loads, data.splitlines()) if event["type"] == "result")
                    self.assertEqual(result["pages"], 3)
                    self.assertEqual(result["records"][0]["job_id"], 10006)
                    post("/api/v1/chat", {**chat, "question": "查询10006的当前效率"})
                    page = {"chat_id": chat["chat_id"], "page_id": result["page_id"], "page": 2}
                    status, data = post("/api/v1/chat-page", page)
                    self.assertEqual(status, 200)
                    self.assertEqual(json.loads(data)["offset"], 20)
                    self.assertEqual(json.loads(data)["records"][0]["job_id"], 10026)
                    self.assertEqual(post("/api/v1/chat-page", {**page, "chat_id": "b" * 32})[0], 404)
            finally:
                server.shutdown()
                worker.join()

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

        def notify(cancelled):
            original_collect(cancelled)
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

    def test_default_collection_reports_conflicts_and_interruption_allows_query(self):
        demo = DemoDataSource()
        connected = DataSource(None, None, None, token="fixture-token")
        connected.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        connected.collector.clock = lambda: STAMP
        jobs = [JOB, {"job_id": 10008, "job_state": ["RUNNING"]}, {"job_id": 10009, "job_state": ["PENDING"]}]
        entered, release = Event(), Event()

        def read(path, params=None, **opts):
            if path == "/slurm/jobs":
                return {"jobs": jobs}
            if path.startswith("/prometheus/"):
                entered.set()
                if not release.wait(8):
                    raise AssertionError("test did not release the active collection")
            return demo.get_json(path, params, **opts)

        connected.get_json = Mock(side_effect=read)
        with create_server(DataSource(None, None, None), 0, connection_factory=lambda _: connected) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=5)) as browser:
                    def request(method, path, body=None, headers=None):
                        browser.request(method, path, json.dumps(body) if body is not None else None, headers or {})
                        response = browser.getresponse()
                        return response, response.read()

                    headers = {"Content-Type": "application/json"}
                    response, data = request("POST", "/api/v1/hpc-connection", {"token": "fixture-token"}, headers)
                    self.assertEqual(response.status, 200)
                    self.assertTrue(json.loads(data)["collection_enabled"])
                    self.assertTrue(entered.wait(3))
                    authenticated = {**headers, "Cookie": response.getheader("Set-Cookie").split(";", 1)[0]}
                    toggle = "/api/v1/live-collection"
                    heatmap = "/api/v1/job-heatmap?view=running"
                    self.assertEqual(request("POST", toggle, {"enabled": True}, headers)[0].status, 409)
                    self.assertEqual(request("POST", toggle, {"enabled": "yes"}, authenticated)[0].status, 400)
                    self.assertEqual(request("POST", toggle, {"enabled": True}, {
                        **authenticated, "Origin": "https://example.com",
                    })[0].status, 403)
                    initial = json.loads(request("GET", heatmap, headers=authenticated)[1])
                    self.assertEqual(initial["status"], "warming_up")
                    self.assertTrue(initial["catalog_only"])
                    self.assertEqual([row["id"] for row in initial["jobs"]], ["10006", "10008"])
                    self.assertIsNone(initial["jobs"][0]["efficiency"])
                    self.assertIsNone(initial["jobs"][0]["memory_ratio"])
                    response, data = request("POST", "/api/v1/chat", {
                        "chat_id": "a" * 32, "question": "查询当前作业 ID 为 10006 的 CPU 效率",
                    }, authenticated)
                    self.assertEqual(response.status, 200)
                    report = next(event["data"] for event in map(json.loads, data.splitlines()) if event["type"] == "result")
                    self.assertEqual(report["source"], "Slurm")
                    self.assertEqual(report["live_detail"]["job_id"], 10006)
                    self.assertIsNone(report["live_detail"]["efficiency"])
                    response, data = request("POST", "/api/v1/chat", {
                        "chat_id": "a" * 32, "question": "查询当前 CPU 利用率低于 30% 的作业",
                    }, authenticated)
                    self.assertEqual(response.status, 200)
                    report = next(event["data"] for event in map(json.loads, data.splitlines()) if event["type"] == "result")
                    self.assertEqual(report["records"], [])
                    question = {"chat_id": "a" * 32, "question": "分析作业 10001"}
                    response, data = request("POST", "/api/v1/chat", question, authenticated)
                    self.assertEqual(response.status, 200)
                    report = next(event["data"] for event in map(json.loads, data.splitlines()) if event["type"] == "result")
                    self.assertEqual(report["status"], "collection_conflict")
                    response, data = request("POST", "/api/v1/job-detail", {
                        "chat_id": "a" * 32, "job_id": "10001",
                    }, authenticated)
                    self.assertEqual(response.status, 409)
                    self.assertEqual(json.loads(data)["status"], "collection_conflict")
                    self.assertTrue(connected.collector.collection_enabled)
                    self.assertFalse(any(call.args[0].startswith("/slurmdb/") for call in connected.get_json.call_args_list))
                    live_job = {"job_id": "10006", "started_at": JOB["start_time"]["number"]}
                    self.assertEqual(request("POST", "/api/v1/live-job", live_job, authenticated)[0].status, 409)

                    response, data = request("POST", toggle, {"enabled": False}, authenticated)
                    self.assertEqual(response.status, 200)
                    self.assertIsNone(response.getheader("Set-Cookie"))
                    self.assertEqual(json.loads(data), {"source_kind": "hpc_api", "connected": True,
                                                        "collection_enabled": False})
                    response, data = request("POST", "/api/v1/live-job", live_job, authenticated)
                    self.assertEqual(response.status, 200)
                    self.assertEqual(json.loads(data)["status"], "waiting")
                    self.assertEqual(request("GET", "/api/v1/job-heatmap?view=completed", headers=authenticated)[0].status, 200)
                    response, data = request("POST", "/api/v1/chat", question, authenticated)
                    self.assertEqual(response.status, 200)
                    report = next(event["data"] for event in map(json.loads, data.splitlines()) if event["type"] == "result")
                    self.assertEqual(report["details"][0]["job_id"], 10001)
                    self.assertEqual([call.args[0] for call in connected.get_json.call_args_list
                                      if call.args[0].startswith("/slurmdb/")], ["/slurmdb/jobs", "/slurmdb/job/10001"])
                    self.assertEqual(connected.collector.snapshot()["collection_revision"], 0)

                    release.set()
                    response, data = request("POST", toggle, {"enabled": True}, authenticated)
                    self.assertEqual(response.status, 200)
                    self.assertTrue(json.loads(data)["collection_enabled"])
                    self.assertEqual(connected.collector.wait_for_snapshot(0, timeout=3), 1)
                    response, data = request("POST", toggle, {"enabled": False}, authenticated)
                    self.assertEqual(response.status, 200)
                    self.assertEqual(json.loads(data), {"source_kind": "hpc_api", "connected": True,
                                                        "collection_enabled": False})
                    self.assertIsNone(response.getheader("Set-Cookie"))
                    paused = json.loads(request("GET", heatmap, headers=authenticated)[1])
                    self.assertTrue(paused["collection_paused"])
                    self.assertEqual(paused["jobs"][0]["job_id"], JOB["job_id"])
                    self.assertEqual(request("GET", "/api/v1/live-updates", headers=authenticated)[0].status, 204)
            finally:
                release.set()
                server.shutdown()
                worker.join()

    def test_startup_token_shows_initial_directory_while_collection_runs(self):
        source = DataSource(None, None, None, token="fixture-token")
        entered, release = Event(), Event()

        def read(path, params=None, **_):
            if path == "/slurm/jobs":
                return {"jobs": [JOB]}
            entered.set()
            if not release.wait(5):
                raise AssertionError("test did not release the active collection")
            return demo_cgroup_response(params)

        source.get_json = Mock(side_effect=read)
        source.enable_collection(PrometheusRateLimit(30), 30, full_pass=True)
        collector = source.collector
        collector.clock = lambda: STAMP
        source.start_collection()
        try:
            self.assertTrue(entered.wait(3))
            snapshot = collector.snapshot()
            self.assertTrue(snapshot["catalog_only"])
            self.assertFalse(snapshot["collection_paused"])
            self.assertEqual(snapshot["jobs"][0]["job_id"], JOB["job_id"])
            self.assertEqual(source.get_json.call_args_list[0].args[0], "/slurm/jobs")
        finally:
            source.stop_collection()
            release.set()
