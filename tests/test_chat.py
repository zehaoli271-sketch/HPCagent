"""Acceptance scenarios use explicitly synthetic jobs and isolated SQLite fixtures."""

from contextlib import closing
from datetime import datetime, timedelta
from http.client import HTTPConnection
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from threading import Thread
import unittest
from unittest.mock import patch

from agent.chat import ChatState, ConversationRun
from agent.chat_intent import parse_intent
from agent.zhiyuan import ZhiyuanModel
from core.job_query import MIB, NO_TRES_VALUE, JobMemorySchema, SQLiteArchive, accounting_snapshot, job_key, normalize_job, remote_records
from core.query_time import SHANGHAI, TimeInputError, TimeRange, parse_time_scope
from core.slurm_jobs import SlurmJobsError, parse_slurm_jobs
from frontend.demo import chat_demo_jobs
from frontend.server import DataSource, DemoDataSource, create_server


NOW = datetime(2026, 11, 23, 13, 0, tzinfo=SHANGHAI)


class ChatAcceptance(unittest.TestCase):
    def setUp(self):
        self.state = ChatState()
        self.source = DemoDataSource()

    def run_chat(self, prompt, archive=None):
        return ConversationRun(self.state, self.source, archive, now=NOW).run(prompt)

    def test_01_yesterday(self):
        data = self.run_chat("查询昨天的所有作业")
        self.assertEqual(data["source"], "合成测试数据")
        self.assertEqual(data["aggregate"]["count"], 8)
        self.assertEqual(data["scope"]["start_time"], "2026-11-22T00:00:00+08:00")

    def test_02_submission_last_24h(self):
        data = self.run_chat("查询最近24小时提交的作业")
        self.assertEqual(data["scope"]["time_basis"], "submission")
        self.assertEqual(data["scope"]["start_time"], "2026-11-22T13:00:00+08:00")
        self.assertEqual(data["aggregate"]["count"], 8)

    def test_03_exact_interval(self):
        data = self.run_chat("查询11月15日14:00至20:00的作业")
        self.assertEqual(data["scope"]["end_time"], "2026-11-15T20:00:00+08:00")
        self.assertEqual(data["aggregate"]["count"], 8)

    def test_04_completed_state(self):
        data = self.run_chat("查询所有COMPLETED作业")
        self.assertEqual(data["aggregate"]["count"], 15 * 7)
        self.assertEqual(data["aggregate"]["states"], [{"state": "COMPLETED", "count": 105, "ratio": 1.0}])
        self.assertEqual(len(data["records"]), 20)
        second = self.run_chat("下一页")
        self.assertEqual(second["page"], 2)
        self.assertTrue({row["job_id"] for row in second["records"]}.isdisjoint(row["job_id"] for row in data["records"]))

    def test_05_job_and_step_detail(self):
        data = self.run_chat("查询Job ID为20011的作业")
        detail, = data["details"]
        self.assertEqual(detail["job_id"], 20011)
        self.assertAlmostEqual(detail["cpu_ratio"], 0.03)
        self.assertEqual(detail["steps"][0]["step_id"], "20011.batch")

    def test_06_daily_counts(self):
        data = self.run_chat("统计过去7天每天的作业数量")
        self.assertEqual(data["scope"]["time_basis"], "submission")
        self.assertEqual(len(data["days"]), 7)
        self.assertEqual(sum(day["count"] for day in data["days"]), data["aggregate"]["count"])
        self.assertTrue(all(day["count"] in {0, 8} for day in data["days"]))

    def test_07_cpu_threshold(self):
        data = self.run_chat("查询昨天CPU效率低于10%的作业")
        self.assertEqual(data["screening_summary"]["candidates"], 5)
        self.assertEqual(data["scope"]["threshold"], 0.1)
        self.assertGreater(data["screening_summary"]["unused_cpu_seconds"], 0)

    def test_08_memory_request(self):
        data = self.run_chat("找出内存申请过量的作业")
        self.assertGreater(data["screening_summary"]["candidates"], 0)
        self.assertEqual(data["aggregate"]["memory_evaluated"], 105)
        self.assertEqual(data["aggregate"]["memory_missing"], 15)

    def test_09_deepest_five(self):
        data = self.run_chat("分析最严重的5条低效作业")
        self.assertEqual(data["tools"], ["query_jobs", "detect_inefficient_jobs", "get_job_detail"])
        self.assertEqual(len(data["details"]), 5)
        self.assertTrue(all(detail["facts"] and detail["suggestions"] for detail in data["details"]))

    def test_10_multi_turn(self):
        initial = self.run_chat("查询昨天所有已完成的作业")
        self.assertEqual(initial["scope"]["time_basis"], "end")
        second = self.run_chat("其中CPU利用率低于10%的有多少？")
        self.assertEqual(second["screening_summary"]["candidates"], 5)
        data = self.run_chat("这些作业中运行时间最长的是哪条？")
        row, = data["records"]
        self.assertEqual(row["job_id"], 20014)
        self.assertEqual(self.run_chat("详细分析第一条")["details"][0]["job_id"], 20014)
        changed = self.run_chat("改查前天")
        self.assertEqual(changed["scope"]["states"], ("COMPLETED",))
        self.assertEqual(changed["scope"]["threshold"], 0.1)
        self.assertEqual(changed["scope"]["start_time"], "2026-11-21T00:00:00+08:00")

    def test_11_why_inefficient(self):
        self.run_chat("找出CPU利用率低于10%的作业")
        self.run_chat("这些作业中运行时间最长的是哪条？")
        result = self.run_chat("为什么这条作业被判定为低效？")
        row, = result["details"]
        self.assertTrue(any("CPU 秒" in line for line in row["facts"]))
        self.assertTrue(any("10.00%" in line for line in row["facts"]))

    def test_12_sqlite_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            make_archive(path)
            result = self.run_chat("本地数据库最早能查询到什么时候？", SQLiteArchive(path))
            self.assertEqual(result["coverage"]["window_count"], 1)
            self.assertEqual(result["coverage"]["first_capture"], "2026-11-22T00:00:00+08:00")
            self.assertFalse(result["coverage"]["complete"])
            self.assertEqual(result["coverage"]["event_bounds"]["submission"]["earliest"], "2026-11-22T13:55:00+08:00")

    def test_13_cpu_quality(self):
        result = self.run_chat("为什么有些作业无法计算CPU利用率？")
        self.assertEqual(result["quality"]["zero_recorded"], 15)
        self.assertEqual(result["aggregate"]["cpu_missing"], 30)
        self.assertIn("步骤 CPU 时间均报告为零，统计完整性待核实", result["quality"]["cpu_reasons"])

    def test_14_period_comparison(self):
        result = self.run_chat("查询最近7天并与前7天对比")
        self.assertEqual(result["scope"]["start_time"], result["comparison"]["scope"]["end_time"])
        self.assertEqual(result["aggregate"]["count"], 56)
        self.assertEqual(result["comparison"]["aggregate"]["count"], 56)

    def test_15_empty_period(self):
        result = self.run_chat("查询2025年11月15日的作业")
        self.assertEqual(result["aggregate"]["count"], 0)
        self.assertIn("本次查询的数据范围", result["message"])


def make_archive(path):
    start = int(datetime(2026, 11, 22, tzinfo=SHANGHAI).timestamp())
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE windows (start_ts INTEGER, end_ts INTEGER, status TEXT)")
        db.execute("CREATE TABLE jobs (payload TEXT, last_window INTEGER, observations INTEGER)")
        db.execute("INSERT INTO windows VALUES (?,?,?)", (start, start + 60, "ok"))
        db.executemany("INSERT INTO jobs VALUES (?,?,?)", ((json.dumps(job), start, 1)
            for job in chat_demo_jobs(int(NOW.timestamp()))[:8]))


class ChatIntegration(unittest.TestCase):
    def test_seff_memory_uses_largest_step_total_and_preserves_concurrent_peak(self):
        job = chat_demo_jobs(int(NOW.timestamp()))[0]
        # Two steps with different task totals. Their sum is neither the seff result nor a concurrent peak.
        batch = job["steps"][0]
        batch["tres"] = {"requested": {"total": [{"type": "mem", "count": 384 * MIB}],
                                       "max": [{"type": "mem", "count": 256 * MIB}]}}
        ordinary = {**batch, "step": {"id": f"{job['job_id']}.0"},
                    "tres": {"requested": {"total": [{"type": "mem", "count": 512 * MIB}],
                                           "max": [{"type": "mem", "count": 128 * MIB}]}}}
        job["steps"].append(ordinary)
        row = normalize_job(accounting_snapshot(job), keep_steps=True)
        self.assertEqual((row["task_max_rss_bytes"], row["seff_memory_bytes"]), (256 * MIB, 512 * MIB))
        self.assertEqual(row["memory_ratio"], 512 / 8192)
        self.assertEqual(row["seff_memory_allocated_ratio"], 512 / 8192)
        self.assertIsNone(row["job_memory_peak_bytes"])
        real_peak = normalize_job(job, memory_schema=JobMemorySchema("fixture_job_peak_bytes", MIB))
        self.assertEqual(real_peak["job_memory_peak_bytes"], 128 * MIB)
        self.assertEqual(real_peak["seff_memory_bytes"], row["seff_memory_bytes"])
        self.assertEqual(real_peak["memory_basis"], "concurrent_peak")

        ordinary["tres"]["requested"]["total"][0]["count"] = NO_TRES_VALUE
        unknown = normalize_job(job)
        self.assertEqual(unknown["seff_memory_bytes"], 384 * MIB)
        self.assertFalse(unknown["seff_memory_complete"])
        self.assertIsNone(unknown["memory_ratio"])

    def test_running_step_totalcpu_is_pending(self):
        job = chat_demo_jobs(int(NOW.timestamp()))[6]
        job["state"]["current"] = ["RUNNING"]
        job["steps"][0]["state"] = ["RUNNING"]
        row = normalize_job(job, keep_steps=True)
        self.assertTrue(row["steps"][0]["cpu_accounting_pending"])
        self.assertIsNone(row["steps"][0]["cpu_seconds"])
        self.assertEqual(row["zero_cpu_steps"], 0)

    def test_historical_completion_cutoff_uses_end_events_and_excludes_running_jobs(self):
        prompt = "帮我查看2026年10月22日17:00时刻已完成的低效作业"
        intent = parse_intent(prompt, now=NOW)
        self.assertEqual((intent.operation, intent.basis, intent.states), ("screen", "end", ("COMPLETED",)))
        self.assertEqual(intent.scope()["scope_note"], "completion_cutoff")
        self.assertEqual(intent.scope()["start_time"], "2026-10-22T00:00:00+08:00")
        self.assertEqual(intent.scope()["end_time"], "2026-10-22T17:00:01+08:00")
        self.assertFalse(intent.period.point)
        earlier, later = chat_demo_jobs(intent.period.start + 86400)[:2]
        after = dict(later, time={**later["time"], "end": intent.period.end + 600})
        class Remote:
            kind = "hpc_api"
            def read_accounting_window(self, start, end, timeout):
                self.first = getattr(self, "first", None) or (start, end)
                return parse_slurm_jobs({"jobs": [earlier, after]})
        source = Remote()
        result = ConversationRun(ChatState(), source, None, now=NOW).run(prompt)
        self.assertEqual(source.first[0], intent.period.start)
        self.assertEqual(result["tools"], ["query_jobs", "detect_inefficient_jobs"])
        self.assertEqual(result["aggregate"]["count"], 1)
        exact = parse_intent("查看10月22日17:00完成的低效作业", now=NOW)
        self.assertEqual((exact.operation, exact.basis, exact.period.end - exact.period.start), ("screen", "end", 1))
        running = parse_intent("查看10月22日17:00正在运行的低效作业", now=NOW)
        self.assertEqual((running.operation, running.basis), ("point", "execution"))

    def test_point_cpu_screen_defaults_to_accounting_and_uses_monitor_on_request(self):
        state = ChatState()
        source = DemoDataSource()
        with patch.object(source, "monitor_samples", wraps=source.monitor_samples) as monitor:
            question = "帮我查看2026年11月21日17:00时刻的低效作业"
            report = ConversationRun(state, source, None, now=NOW).run(question, "auto")
            self.assertEqual(report["screening"]["mode"], "lifetime")
            self.assertEqual(report["screening"]["cpu_source"], "Slurm TotalCPU")
            monitor.assert_not_called()

            followup = ConversationRun(state, source, None, now=NOW).run("再看前 5 分钟")
            window = followup["screening"]
            self.assertEqual(window["mode"], "window")
            self.assertEqual(window["window_start"], "2026-11-21T16:55:00+08:00")
            self.assertEqual(window["at"], report["screening"]["at"])
            monitor.assert_called_once()

            explicit = ConversationRun(state, source, None, now=NOW).run(
                question.replace("17:00时刻", "17:00前五分钟"), "lifetime"
            )
            self.assertEqual(explicit["screening"]["mode"], "lifetime")
            self.assertEqual(monitor.call_count, 1)

    def test_time_shanghai_cross_date_and_errors(self):
        same_day = parse_time_scope("11月15日下午2点到晚上8点", NOW)
        self.assertEqual(same_day.as_dict()["start_time"], "2026-11-15T14:00:00+08:00")
        cross_day = parse_time_scope("11月15日22:00至02:30", NOW)
        self.assertEqual(cross_day.as_dict()["end_time"], "2026-11-16T02:30:00+08:00")
        self.assertEqual(parse_time_scope("2025年12月31日到2026年1月1日", NOW).end
                         - parse_time_scope("2025年12月31日到2026年1月1日", NOW).start, 172800)
        for text in ("2026年2月30日", "2027年1月1日", "2026年11月15日16:00至16:00", "11月15日14:00至"):
            with self.subTest(text=text), self.assertRaises(TimeInputError):
                parse_time_scope(text, NOW)
        with self.assertRaises(TimeInputError):
            parse_intent("详细分析第一条", now=NOW)
        self.assertEqual(parse_intent("这周CPU超过4核、内存小于10GB的作业", now=NOW).filters[1].value, 10240)

    def test_archive_readonly_missing_memory_and_partial_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            make_archive(path)
            source = DataSource(None, None, None)
            archive = SQLiteArchive(path)
            with closing(archive.connect()) as db, self.assertRaises(sqlite3.OperationalError):
                db.execute("DELETE FROM jobs")
            result = ConversationRun(ChatState(), source, archive, now=NOW).run("昨天内存申请过量的作业")
            self.assertEqual(result["status"], "needs_data")
            self.assertEqual(result["aggregate"]["count"], 8)
            self.assertEqual(result["aggregate"]["memory_evaluated"], 0)
            self.assertFalse(result["coverage"]["complete"])
            self.assertTrue(any("同一时刻" in note for note in result["notes"]))

    def test_remote_windows_keep_versions_and_stop_on_401_403_and_bad_request(self):
        period = TimeRange(1795300000, 1795315000)
        fixture, *_ = chat_demo_jobs(int(NOW.timestamp()))
        fixture["array"] = {"job_id": 20000, "task_id": {"set": True, "infinite": False, "number": 0}}
        self.assertEqual(job_key(fixture)[4], 0)
        for code in ("invalid_hpc_token", "permission_denied", "hpc_query_error"):
            with self.subTest(code=code):
                class Remote:
                    def __init__(self):
                        self.calls = []

                    def read_accounting_window(self, start, end, timeout):
                        self.calls.append((start, end))
                        if len(self.calls) == 3:
                            raise SlurmJobsError(code, "合成故障")
                        first = dict(fixture, state={"current": ["RUNNING"]}, steps=[])
                        return parse_slurm_jobs({"jobs": [first if len(self.calls) == 1 else fixture]})

                remote = Remote()
                events = []
                versions, coverage = remote_records(remote, period, lambda text, **fields: events.append(fields))
                history, = versions.values()
                self.assertEqual([version["job"]["state"]["current"][0] for version in history], ["RUNNING", "COMPLETED"])
                self.assertEqual(coverage["windows_completed"], 2)
                self.assertEqual(coverage["error"]["code"], code)
                self.assertEqual(len(remote.calls), 3)
                self.assertTrue(all(end - start <= 3600 for start, end in remote.calls))
                self.assertEqual(events[-1], {"completed": 2, "total": 5, "timed_out": 0})

    def test_hpc_timeout_shortens_windows_and_preserves_partial_candidates(self):
        job = chat_demo_jobs(int(NOW.timestamp()))[1]
        start = int(datetime(2026, 11, 22, tzinfo=SHANGHAI).timestamp())

        class Remote:
            kind = "hpc_api"

            def __init__(self):
                self.windows = []
                self.intermittent = True

            def read_accounting_window(self, left, right, timeout):
                self.windows.append((left, right))
                if right - left > 900 or (self.intermittent and left == start + 900):
                    raise SlurmJobsError("hpc_timeout", "模拟超时")
                return parse_slurm_jobs({"jobs": [job] if left <= job["time"]["start"] < right else []})

        source, state = Remote(), ChatState()
        events = []
        result = ConversationRun(state, source, None, now=NOW, emit=events.append).run(
            "查询昨天CPU效率低于10%的作业"
        )
        self.assertEqual(source.windows[:4], [(start, start + 3600), (start, start + 900),
                                               (start + 900, start + 1800), (start + 1800, start + 2700)])
        self.assertEqual(source.windows[-1][1], start + 86400)
        self.assertEqual(result["status"], "partial_data")
        self.assertEqual(result["screening_summary"]["candidates"], 1)
        coverage = result["coverage"]
        self.assertEqual(coverage["windows_completed"], 95)
        self.assertEqual(coverage["covered_seconds"], 85500)
        self.assertEqual(coverage["requested_seconds"], 86400)
        self.assertEqual(coverage["timed_out_windows"], [{"start": start + 900, "end": start + 1800}])
        self.assertTrue(any("15 分钟" in event["message"] for event in events if event["type"] == "progress"))
        source.intermittent = False
        source.windows.clear()
        resumed = ConversationRun(state, source, None, now=NOW).run("继续查询")
        self.assertEqual(source.windows, [(start + 900, start + 1800)])
        self.assertEqual(resumed["coverage"]["windows_reused"], 95)
        self.assertEqual(resumed["coverage"]["covered_seconds"], 86400)
        self.assertEqual(resumed["screening_summary"]["candidates"], 1)

    def test_hpc_consecutive_timeouts_stop_and_empty_snapshot_can_be_retried(self):
        class Remote:
            kind = "hpc_api"

            def __init__(self):
                self.calls = 0
                self.available = False

            def read_accounting_window(self, start, end, timeout):
                self.calls += 1
                if not self.available:
                    raise SlurmJobsError("hpc_timeout", "模拟超时")
                return parse_slurm_jobs({"jobs": []})

        source, state = Remote(), ChatState()
        prompt = "查询昨天CPU效率低于10%的作业"
        failed = ConversationRun(state, source, None, now=NOW).run(prompt)
        self.assertEqual(source.calls, 4)
        self.assertEqual(failed["status"], "needs_data")
        self.assertNotIn("aggregate", failed)
        self.assertNotIn("screening_summary", failed)
        self.assertEqual(failed["coverage"]["windows_completed"], 0)
        self.assertEqual(len(failed["coverage"]["timed_out_windows"]), 3)
        self.assertIn("已暂停查询", failed["coverage"]["error"]["message"])
        source.available = True
        recovered = ConversationRun(state, source, None, now=NOW).run(prompt)
        self.assertEqual(source.calls, 100)
        self.assertEqual(recovered["aggregate"]["count"], 0)
        self.assertEqual(recovered["coverage"]["windows_completed"], 96)

    def test_historical_read_budget_preserves_next_window_for_resume(self):
        class Remote:
            calls = []
            def read_accounting_window(self, start, end, timeout):
                self.calls.append((start, end, timeout))
                return parse_slurm_jobs({"jobs": []})

        period = TimeRange(1795300000, 1795307200)
        source = Remote()
        emit = lambda *_args, **_fields: None
        with patch("core.job_query.monotonic", side_effect=[0, 0, 20]):
            first = remote_records(source, period, emit, budget=10)
        self.assertEqual(source.calls, [(period.start, period.start + 3600, 10)])
        self.assertEqual(first[1]["error"]["code"], "hpc_query_budget")
        _, continued = remote_records(source, period, emit, resume=first)
        self.assertEqual([(left, right) for left, right, _ in source.calls],
                         [(period.start, period.start + 3600), (period.start + 3600, period.end)])
        self.assertEqual(continued["windows_reused"], 1)

    def test_configured_job_memory_and_same_partition_comparison(self):
        with self.assertRaises(ValueError):
            JobMemorySchema("steps.memory_peak", 1024 ** 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            make_archive(path)
            source = DataSource(None, None, None)
            archive = SQLiteArchive(path)
            schema = JobMemorySchema("fixture_job_peak_bytes", 1024 ** 2)
            state = ChatState()
            def ask(text):
                return ConversationRun(state, source, archive, now=NOW, memory_schema=schema).run(text)
            result = ask("昨天内存申请过量的作业")
            self.assertEqual(result["aggregate"]["memory_evaluated"], 7)
            self.assertGreater(result["screening_summary"]["memory_slack_bytes"], 0)
            details = ask("对比作业 20011 和 20012")["details"]
            self.assertEqual([job["job_id"] for job in details], [20011, 20012])
            ask("查询作业 20011")
            self.assertEqual(ask("对比同类作业")["peer_reference"]["job_id"], 20011)

    @patch("agent.zhiyuan.urlopen")
    def test_streaming_excludes_internal_reasoning(self, open_model):
        open_model.return_value = io.BytesIO(
            b'data: {"choices":[{"delta":{"reasoning_content":"private"}}]}\n\n'
            b'data: {"choices":[{"delta":{"content":"only reply"}}]}\n\n'
            b'data: [DONE]\n\n'
        )
        model = ZhiyuanModel("test-only")
        self.assertEqual(list(model.stream_reply({"count": 0})), ["only reply"])
        self.assertIs(json.loads(open_model.call_args.args[0].data)["stream"], True)

    def test_ndjson_api_session_isolation(self):
        with create_server(DemoDataSource(), 0) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                def post(prompt, chat_id, **options):
                    with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=5)) as client:
                        client.request("POST", "/api/v1/chat", json.dumps({"question": prompt, "chat_id": chat_id, **options}),
                                       {"Content-Type": "application/json"})
                        response = client.getresponse()
                        self.assertEqual(response.status, 200)
                        self.assertIn("application/x-ndjson", response.getheader("Content-Type"))
                        return [json.loads(line) for line in response.read().splitlines()]
                events = post("查询昨天作业", "a" * 32)
                self.assertEqual(events[0]["type"], "progress")
                self.assertEqual(events[-1]["type"], "done")
                self.assertEqual(events[-2]["data"]["aggregate"]["count"], 8)
                self.assertEqual(post("详细分析第一条", "a" * 32)[-2]["data"]["status"], "complete")
                self.assertEqual(post("详细分析第一条", "b" * 32)[-2]["data"]["status"], "needs_clarification")
                question = "查询2026年11月21日17:00时刻的低效作业"
                self.assertEqual(post(question, "c" * 32, mode="auto")[-2]["data"]["screening"]["mode"], "lifetime")
            finally:
                server.shutdown()
                worker.join()

    def test_click_details_reuses_job_identity_and_leaves_chat_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.sqlite"
            make_archive(path)
            with closing(sqlite3.connect(path)) as db, db:
                payload, observed, observations = db.execute("SELECT * FROM jobs LIMIT 1").fetchone()
                duplicate = json.loads(payload)
                duplicate["cluster"] = "another-cluster"
                db.execute("INSERT INTO jobs VALUES (?,?,?)", (json.dumps(duplicate), observed, observations))
            source = DataSource(None, None, None)
            with create_server(source, 0, archive=SQLiteArchive(path)) as server:
                worker = Thread(target=server.serve_forever, daemon=True)
                worker.start()
                try:
                    with closing(HTTPConnection("127.0.0.1", server.server_port, timeout=5)) as client:
                        chat_id = "abc1" * 8
                        def post(path, payload):
                            client.request("POST", path, json.dumps({"chat_id": chat_id, **payload}),
                                           {"Content-Type": "application/json"})
                            response = client.getresponse()
                            return response.status, response.read()

                        code, body = post("/api/v1/chat", {"question": "查询2026年11月22日的作业"})
                        self.assertEqual(code, 200)
                        records = json.loads(body.splitlines()[-2])["data"]["records"]
                        row = next(row for row in records if row["cluster"] == "another-cluster")
                        key = (None, chat_id)
                        conversation = server.chat_states[key]
                        ordered = conversation.ordered
                        code, body = post("/api/v1/job-detail", {"job_id": str(row["job_id"]), "job_ref": row["job_ref"]})
                        self.assertEqual(code, 200)
                        detail, = json.loads(body)["details"]
                        self.assertEqual(detail["cluster"], "another-cluster")
                        self.assertEqual(detail["steps"][0]["step_id"], f"{row['job_id']}.batch")
                        self.assertIs(conversation.ordered, ordered)
                        self.assertIsNone(conversation.selected)
                        code, _ = post("/api/v1/job-detail", {"job_id": "101", "job_ref": row["job_ref"]})
                        self.assertEqual(code, 400)
                finally:
                    server.shutdown()
                    worker.join()

    def test_open_point_result_fetches_identical_demo_record(self):
        src = DemoDataSource()
        state = ChatState()
        question = "查看2026年11月21日17:00时刻的低效作业"
        point = ConversationRun(state, src, None, now=NOW).run(question)["screening"]
        row = point["candidates"][0]
        at = int(datetime.fromisoformat(point["at"]).timestamp())
        result = ConversationRun(state, src, None, now=NOW).inspect_job(int(row["job_id"]), row["job_ref"], at)
        self.assertEqual(result["details"][0]["cpu_ratio"], row["utilization"])
        self.assertTrue(result["details"][0]["steps"])
