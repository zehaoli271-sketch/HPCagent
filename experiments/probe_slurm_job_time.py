"""Compare four authorized job-accounting requests without exporting job records."""

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import os

from core.slurm_jobs import HPC_TOKEN_ENV, SlurmJobsError, SlurmJobsResponse, fetch_slurm_jobs, unix_seconds


VARIANTS = {
    "submit_true": {"usage_time_as_submit_time": True},
    "no_truncate_true": {"disable_truncate_usage_time": True},
    "no_truncate_false": {"disable_truncate_usage_time": False},
}


@dataclass
class Snapshot:
    identities: Counter
    job_times: dict
    step_times: dict
    metadata: dict

    @classmethod
    def from_response(
        cls, response: SlurmJobsResponse, start: int, end: int, job_id: int | None
    ) -> "Snapshot":
        identities = Counter()
        job_times = defaultdict(Counter)
        step_times = defaultdict(Counter)
        window_cases = Counter()
        for job in response.jobs:
            if job_id is not None and job.get("job_id") != job_id:
                continue
            times = job.get("time") or {}
            array = job.get("array") or {}
            submitted, started, ended = (times.get(field) for field in ("submission", "start", "end"))
            window_cases["submitted_in_window"] += type(submitted) is int and start <= submitted < end
            window_cases["start_at_window_start"] += started == start
            window_cases["end_at_window_end"] += ended == end
            window_cases["spans_window"] += (
                type(started) is int and type(ended) is int and 0 < started < start < end < ended < 4294967294
            )
            identity = json.dumps(
                [job.get("cluster"), job.get("job_id"), times.get("submission"), array.get("task_id")],
                sort_keys=True,
            )
            identities[identity] += 1
            job_times[identity][json.dumps([times.get(key) for key in ("start", "end", "elapsed")])] += 1
            for step in job.get("steps") or []:
                step_time = step.get("time") or {}
                step_times[identity][json.dumps([
                    (step.get("step") or {}).get("id"),
                    *[step_time.get(key) for key in ("start", "end", "elapsed")],
                ], sort_keys=True)] += 1
        return cls(identities, job_times, step_times, {
            "returned_count": len(response.jobs),
            "compared_count": sum(identities.values()),
            "warning_count": response.warning_count,
            "fetched_at": response.fetched_at,
            "window_cases": dict(window_cases),
        })

    def compare(self, other: "Snapshot") -> dict[str, int]:
        common = self.identities.keys() & other.identities.keys()
        return {
            "only_left": sum((self.identities - other.identities).values()),
            "only_right": sum((other.identities - self.identities).values()),
            "common": sum((self.identities & other.identities).values()),
            "shared_identities_with_changed_job_time": sum(
                self.job_times[key] != other.job_times[key] for key in common
            ),
            "shared_identities_with_changed_step_records": sum(
                self.step_times.get(key) != other.step_times.get(key) for key in common
            ),
        }


def probe(token: str, start: int, end: int, job_id: int | None = None) -> dict:
    baseline = Snapshot.from_response(fetch_slurm_jobs(token, start, end), start, end, job_id)
    report = {"window": {"start_time": start, "end_time": end}, "baseline": baseline.metadata, "variants": {}}
    truncation = {}
    for name, params in VARIANTS.items():
        try:
            variant = Snapshot.from_response(fetch_slurm_jobs(token, start, end, **params), start, end, job_id)
        except SlurmJobsError as error:
            report["variants"][name] = {"error_code": error.code, "message": str(error)}
            continue
        report["variants"][name] = {**variant.metadata, "vs_baseline": baseline.compare(variant)}
        if "disable_truncate_usage_time" in params:
            truncation[params["disable_truncate_usage_time"]] = variant
    if True in truncation and False in truncation:
        report["no_truncate_false_vs_true"] = truncation[False].compare(truncation[True])
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="对比交大 Slurm 作业查询的时间参数")
    parser.add_argument("--start", required=True, help="带时区的 ISO 8601 开始时间")
    parser.add_argument("--end", required=True, help="带时区的 ISO 8601 结束时间")
    parser.add_argument("--job-id", type=int, help="只在本地比较指定作业 ID")
    args = parser.parse_args()
    token = os.environ.get(HPC_TOKEN_ENV, "").strip()
    if not token:
        parser.error(f"请先在 {HPC_TOKEN_ENV} 中配置有权限的 HPC 访问令牌")
    try:
        start, end = unix_seconds(args.start), unix_seconds(args.end)
    except (ValueError, OverflowError, OSError):
        parser.error("请使用带时区且精确到秒的 ISO 8601 时间")
    try:
        print(json.dumps(probe(token, start, end, args.job_id), ensure_ascii=False, indent=2))
    except SlurmJobsError as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
