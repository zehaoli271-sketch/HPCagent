from io import BytesIO
import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from experiments.probe_slurm_job_time import probe


class TimeProbeTest(unittest.TestCase):
    @patch("core.slurm_jobs.urlopen")
    def test_comparison_runs_one_parameter_per_request(self, urlopen):
        start, end = 1794110400, 1794110460
        full = {
            "cluster": "synthetic",
            "job_id": 1,
            "time": {"submission": start - 120, "start": start - 60, "end": end + 60},
        }
        clipped = {**full, "time": {**full["time"], "start": start, "end": end}}
        urlopen.side_effect = iter(
            BytesIO(json.dumps({"jobs": jobs}).encode("utf-8"))
            for jobs in ([full], [], [full], [clipped])
        )

        report = probe("synthetic-token", start, end)

        queries = [parse_qs(urlsplit(call.args[0].full_url).query) for call in urlopen.call_args_list]
        common = {"start_time": [str(start)], "end_time": [str(end)]}
        self.assertEqual(queries, [
            common,
            common | {"usage_time_as_submit_time": ["true"]},
            common | {"disable_truncate_usage_time": ["true"]},
            common | {"disable_truncate_usage_time": ["false"]},
        ])
        self.assertEqual(report["variants"]["submit_true"]["vs_baseline"]["only_left"], 1)
        self.assertEqual(report["baseline"]["window_cases"]["spans_window"], 1)
        no_trunc_comparison = report["no_truncate_false_vs_true"]
        self.assertEqual(no_trunc_comparison["common"], 1)
        self.assertEqual(no_trunc_comparison["shared_identities_with_changed_job_time"], 1)


if __name__ == "__main__":
    unittest.main()
