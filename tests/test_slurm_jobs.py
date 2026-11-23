from datetime import datetime
from io import BytesIO
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from agent.slurm_tools import slurm_jobs_tool
from core.slurm_jobs import fetch_slurm_jobs


WINDOW = {"start": "2026-11-22T08:00:00+08:00", "end": "2026-11-22T00:01:00Z", "time_basis": "usage"}


def response(payload):
    return BytesIO(json.dumps(payload).encode("utf-8"))


class SlurmJobsTest(unittest.TestCase):
    @patch("core.slurm_jobs.urlopen")
    def test_usage_window_with_explicit_offsets_and_small_model_preview(self, urlopen):
        jobs = [
            {
                "job_id": job_id,
                "state": {"current": ["COMPLETED"]},
                "time": {"submission": 1795305500, "start": 1795305692, "end": 1795305700},
                "user": "synthetic-user",
                "working_directory": "/private",
                "steps": [{"step": {"id": "synthetic-step"}}],
            }
            for job_id in range(1, 22)
        ]
        urlopen.return_value = response({"jobs": jobs, "errors": [], "warnings": [{"description": "private"}]})

        result = slurm_jobs_tool("synthetic-token").handler(WINDOW)

        request = urlopen.call_args.args[0]
        url = urlsplit(request.full_url)
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer synthetic-token")
        self.assertEqual(url.netloc, "api.hpc.sjtu.edu.cn")
        self.assertEqual(url.path, "/slurmdb/jobs")
        self.assertEqual(parse_qs(url.query), {"start_time": ["1795305600"], "end_time": ["1795305660"]})
        self.assertEqual(result.evidence.query_scope, {
            "start_time": 1795305600, "end_time": 1795305660, "time_basis": "usage",
        })
        self.assertIsNotNone(datetime.fromisoformat(result.evidence.fetched_at).tzinfo)
        self.assertEqual(result.data["returned_count"], 21)
        self.assertEqual(result.data["coverage"], "not_verified")
        self.assertEqual(result.data["warning_count"], 1)
        self.assertTrue(result.data["preview_truncated"])
        self.assertEqual(len(result.data["jobs"]), 20)
        self.assertEqual(result.data["jobs"][0], {
            "job_id": 1,
            "states": ["COMPLETED"],
            "time_unix_seconds": {
                "submission": 1795305500, "eligible": None, "start": 1795305692, "end": 1795305700,
            },
        })

    @patch("core.slurm_jobs.urlopen")
    def test_backend_keeps_step_fields_for_later_analysis(self, urlopen):
        jobs = [{"steps": [{"time": {"total": {"seconds": 10}}}]}]
        urlopen.return_value = response({"jobs": jobs, "errors": [], "warnings": []})

        self.assertEqual(fetch_slurm_jobs("synthetic-token", 1795305600, 1795305660).jobs, jobs)

    @patch("core.slurm_jobs.urlopen")
    def test_unsupported_basis_and_invalid_windows_do_not_make_requests(self, urlopen):
        tool = slurm_jobs_tool("synthetic-token")
        cases = (
            ({"time_basis": "submission"}, "unsupported_time_basis"),
            ({"start": "2026-11-22T08:00:00"}, "invalid_window"),
            ({"end": "2026-11-22T07:59:00+08:00"}, "invalid_window"),
            ({"disable_truncate_usage_time": True}, "invalid_arguments"),
        )
        for changes, code in cases:
            with self.subTest(changes=changes):
                self.assertEqual(tool.handler(WINDOW | changes).code, code)
        urlopen.assert_not_called()

    @patch("core.slurm_jobs.urlopen")
    def test_empty_success_is_distinct_from_gateway_error(self, urlopen):
        urlopen.side_effect = [
            response({"jobs": [], "errors": [], "warnings": []}),
            response({"jobs": [], "errors": [{"description": "internal details"}]}),
        ]
        tool = slurm_jobs_tool("synthetic-token")

        self.assertEqual(tool.handler(WINDOW).data["returned_count"], 0)
        failure = tool.handler(WINDOW)
        self.assertEqual(failure.code, "hpc_query_error")
        self.assertNotIn("internal details", failure.message)

    @patch("core.slurm_jobs.urlopen")
    def test_unauthorized_and_forbidden_are_distinct(self, urlopen):
        tool = slurm_jobs_tool("synthetic-token")
        for status, code in ((401, "invalid_hpc_token"), (403, "permission_denied")):
            with self.subTest(status=status):
                urlopen.side_effect = HTTPError("https://api.hpc.sjtu.edu.cn/slurmdb/jobs", status, "", {}, None)
                self.assertEqual(tool.handler(WINDOW).code, code)


if __name__ == "__main__":
    unittest.main()
