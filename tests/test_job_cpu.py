from copy import deepcopy
from io import BytesIO
import json
import unittest
from unittest.mock import patch

from agent.runtime import Agent, Reply, ToolCall
from agent.slurm_tools import job_cpu_tool
from core.efficiency import allocated_cpu, calculate_job_efficiency
from core.job_cpu import analyze_job_cpu


def step(suffix, seconds, micros=0):
    return {
        "step": {"id": f"42.{suffix}"},
        "state": ["COMPLETED"],
        "time": {"total": {"seconds": seconds, "microseconds": micros}},
    }


def completed_job():
    return {
        "job_id": 42,
        "state": {"current": ["COMPLETED"]},
        "restart_cnt": 0,
        "flags": [],
        "time": {"start": 100, "end": 110, "elapsed": 10, "suspended": 0},
        "tres": {"allocated": [{"type": "cpu", "count": 4}]},
        "steps": [step("batch", 1, 1_300_000), step("extern", 500), step("0", 7, 700_000)],
    }


def response(jobs, warnings=()):
    return BytesIO(json.dumps({"jobs": jobs, "warnings": warnings, "errors": []}).encode("utf-8"))


class JobCpuTest(unittest.TestCase):
    def test_heatmap_adapter_uses_shared_suspension_calculation(self):
        job = completed_job()
        job["time"].update(elapsed=8, suspended=2)

        self.assertEqual(allocated_cpu(job), 4)
        self.assertEqual(calculate_job_efficiency(job), {
            "efficiency": 0.3125, "cpu_seconds": 10.0, "reason": None,
        })

    def test_heatmap_adapter_converts_invalid_metric_to_existing_fields(self):
        job = completed_job() | {"restart_cnt": 1}

        self.assertEqual(calculate_job_efficiency(job), {
            "efficiency": None, "cpu_seconds": None, "reason": "作业曾重启或调整分配，需要核实资源容量",
        })

    def test_whole_job_sums_work_and_batch_with_microsecond_carry(self):
        cpu = analyze_job_cpu(completed_job())

        self.assertEqual(cpu["availability"], "valid")
        self.assertEqual(cpu["included_step_ids"], ["42.batch", "42.0"])
        self.assertEqual(cpu["used_cpu_microseconds"], 10_000_000)
        self.assertEqual(cpu["capacity_cpu_seconds"], 40)
        self.assertEqual(cpu["utilization"], 0.25)

    def test_elapsed_contract_subtracts_suspension_once(self):
        job = completed_job()
        job["time"]["suspended"] = 2
        for elapsed in (10, 8):
            with self.subTest(elapsed=elapsed):
                job["time"]["elapsed"] = elapsed
                cpu = analyze_job_cpu(job)
                self.assertEqual(cpu["active_seconds"], 8)
                self.assertEqual(cpu["utilization"], 0.3125)
        job["time"]["elapsed"] = 9
        self.assertEqual(analyze_job_cpu(job)["reason"], "job_time_contract_mismatch")

    def test_outside_calculation_scope_has_no_utilization(self):
        cases = (
            ({"state": {"current": ["RUNNING"]}}, "outside_v1_completed_scope"),
            ({"time": {"start": 4294967294}}, "never_started"),
            ({"restart_cnt": 1}, "restart_or_resize"),
            ({"flags": ["RESIZED"]}, "restart_or_resize"),
            ({"het": {"job_id": 42}}, "heterogeneous_job"),
            ({"steps": []}, "steps_unavailable"),
            ({"steps": [step("batch", 2), step("batch", 2)]}, "duplicate_step_id"),
            ({"steps": [step("batch", 2, None)]}, "step_cpu_time_unavailable"),
            ({"steps": [step("batch", 41)]}, "cpu_exceeds_capacity"),
        )
        for changes, reason in cases:
            with self.subTest(reason=reason):
                cpu = analyze_job_cpu(completed_job() | changes)
                self.assertEqual(cpu["reason"], reason)
                self.assertIsNone(cpu["utilization"])

    def test_zero_cpu_conflict_identifies_the_step(self):
        work = step("0", 0)
        work["tres"] = {"requested": {"total": [{"type": "cpu", "count": 1000}]}}
        job = completed_job()
        job["steps"][2] = work

        cpu = analyze_job_cpu(job)

        self.assertEqual(cpu["reason"], "zero_total_cpu_with_positive_tres")
        self.assertEqual(cpu["positive_cpu_tres_step_ids"], ["42.0"])
        self.assertIsNone(cpu["utilization"])

    @patch("core.slurm_jobs.urlopen")
    def test_agent_tool_uses_single_job_endpoint_and_allowlisted_evidence(self, urlopen):
        job = completed_job() | {"user": "synthetic-user", "working_directory": "/private"}
        urlopen.return_value = response([job])

        class TestModel:
            def next_action(self, context):
                if context.exchanges:
                    result = context.exchanges[0].result
                    return Reply("answered", f"CPU 利用率为 {result.data['cpu']['utilization']:.0%}")
                return ToolCall("cpu-1", "analyze_job_cpu", {"job_id": 42})

        turn = Agent(TestModel(), [job_cpu_tool("synthetic-token")]).run("作业 42 的 CPU 利用率")

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://api.hpc.sjtu.edu.cn/slurmdb/job/42")
        self.assertEqual(request.get_header("Authorization"), "Bearer synthetic-token")
        self.assertEqual(turn.reply.text, "CPU 利用率为 25%")
        result = turn.exchanges[0].result
        self.assertNotIn("synthetic-user", repr(result))
        self.assertNotIn("/private", repr(result))
        self.assertEqual(result.evidence.query_scope, {"lookup": "job_id", "job_id": 42})

    @patch("core.slurm_jobs.urlopen")
    def test_ambiguous_job_can_be_resolved_by_cluster_and_start(self, urlopen):
        first = completed_job() | {"cluster": "synthetic-a"}
        second = deepcopy(first)
        second["time"].update(start=200, end=210)
        third = deepcopy(second) | {"cluster": "synthetic-b"}
        urlopen.side_effect = [response([first, second, third]), response([first, second, third])]
        tool = job_cpu_tool("synthetic-token")

        self.assertEqual(tool.handler({"job_id": 42}).code, "ambiguous_job_id")
        result = tool.handler({"job_id": 42, "cluster": "synthetic-a", "start_time": 200})
        self.assertEqual(result.data["job"]["time_unix_seconds"]["start"], 200)
        self.assertEqual(result.data["cpu"]["utilization"], 0.25)

    @patch("core.slurm_jobs.urlopen")
    def test_gateway_warning_does_not_become_a_valid_utilization(self, urlopen):
        urlopen.return_value = response([completed_job()], [{"description": "private"}])

        result = job_cpu_tool("synthetic-token").handler({"job_id": 42})

        self.assertEqual(result.data["cpu"]["reason"], "hpc_response_warnings")
        self.assertIsNone(result.data["cpu"]["utilization"])
        self.assertNotIn("private", repr(result))


if __name__ == "__main__":
    unittest.main()
