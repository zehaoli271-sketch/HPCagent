"""Expose a small, allowlisted job query result to the model."""

from core.job_cpu import FORMULA_VERSION, analyze_job_cpu
from core.slurm_jobs import JOB_URL, JOBS_URL, SlurmJobsError, fetch_slurm_job, fetch_slurm_jobs, unix_seconds

from .runtime import Tool, ToolEvidence, ToolFailure, ToolSpec, ToolSuccess


PREVIEW_LIMIT = 20
TIMESTAMP_FIELDS = ("submission", "eligible", "start", "end")
QUERY_SLURM_JOBS = ToolSpec(
    name="query_slurm_jobs",
    description=(
        "根据交大 Slurm 作业记账的 usage 窗口查询作业。"
        "仅用于用户同意按该口径查询的场景，无法精确按提交、开始、结束或实际运行区间检索。"
        "返回本次响应中的作业数和最多 20 条预览。"
    ),
    parameters={
        "type": "object",
        "properties": {
            "start": {"type": "string", "description": "带时区的 ISO 8601 开始时间"},
            "end": {"type": "string", "description": "带时区的 ISO 8601 结束时间"},
            "time_basis": {"type": "string", "enum": ["usage"]},
        },
        "required": ["start", "end", "time_basis"],
        "additionalProperties": False,
    },
)
ANALYZE_JOB_CPU = ToolSpec(
    name="analyze_job_cpu",
    description=(
        "按数字作业 ID 查询单条记账详情，并计算正常完成作业的整作业 CPU 利用率。"
        "结果包含累计 CPU 微秒、分配容量、纳入的步骤和计算依据，或数据不足的原因。"
    ),
    parameters={
        "type": "object",
        "properties": {
            "job_id": {"type": "integer", "minimum": 1},
            "cluster": {"type": "string", "minLength": 1, "description": "同 ID 对应多个集群时用于区分"},
            "start_time": {
                "type": "integer", "minimum": 1, "description": "同 ID 对应多次运行时使用已知的开始时间（Unix 秒）",
            },
        },
        "required": ["job_id"],
        "additionalProperties": False,
    },
)


def job_preview(job: dict) -> dict:
    time = job.get("time")
    state = job.get("state")
    current_state = state.get("current") if isinstance(state, dict) else state
    times = time if isinstance(time, dict) else {}
    return {
        "job_id": job.get("job_id") if type(job.get("job_id")) is int else None,
        "states": [item for item in current_state if isinstance(item, str)] if isinstance(current_state, list) else [],
        "time_unix_seconds": {
            field: times[field] if type(times.get(field)) is int and 0 < times[field] < 4294967294 else None
            for field in TIMESTAMP_FIELDS
        },
    }


def slurm_jobs_tool(token: str) -> Tool:
    def query(arguments: dict[str, object]) -> ToolSuccess | ToolFailure:
        if arguments.keys() != {"start", "end", "time_basis"}:
            return ToolFailure("invalid_arguments", "请提供开始时间、结束时间和查询口径。")
        if arguments["time_basis"] != "usage":
            return ToolFailure("unsupported_time_basis", "当前可查询平台的记账 usage 窗口。")
        try:
            start, end = unix_seconds(arguments["start"]), unix_seconds(arguments["end"])
        except (ValueError, OverflowError, OSError):
            return ToolFailure("invalid_window", "请使用带时区且精确到秒的 ISO 8601 时间。")
        try:
            result = fetch_slurm_jobs(token, start, end)
        except SlurmJobsError as error:
            return ToolFailure(error.code, str(error))

        preview = [job_preview(job) for job in result.jobs[:PREVIEW_LIMIT]]
        return ToolSuccess(
            data={
                "returned_count": len(result.jobs),
                "warning_count": result.warning_count,
                "coverage": "not_verified",
                "jobs": preview,
                "preview_truncated": len(preview) < len(result.jobs),
            },
            evidence=ToolEvidence(
                source=JOBS_URL,
                query_scope={"time_basis": "usage", "start_time": start, "end_time": end},
                fetched_at=result.fetched_at,
            ),
        )

    return Tool(QUERY_SLURM_JOBS, query)


def job_cpu_tool(token: str = "", *, fetch_detail=None) -> Tool:
    def analyze(arguments: dict[str, object]) -> ToolSuccess | ToolFailure:
        if arguments.keys() - {"job_id", "cluster", "start_time"}:
            return ToolFailure("invalid_arguments", "请提供作业 ID，可用集群和开始时间区分多个记录。")
        job_id = arguments.get("job_id")
        cluster = arguments.get("cluster")
        start_time = arguments.get("start_time")
        if type(job_id) is not int or job_id <= 0:
            return ToolFailure("invalid_job_id", "请提供正整数作业 ID。")
        if "cluster" in arguments and (not isinstance(cluster, str) or not cluster.strip()):
            return ToolFailure("invalid_arguments", "集群名称须为非空字符串。")
        if "start_time" in arguments and (type(start_time) is not int or start_time <= 0):
            return ToolFailure("invalid_arguments", "开始时间须使用正整数 Unix 秒。")
        try:
            response = fetch_detail(job_id) if fetch_detail else fetch_slurm_job(token, job_id)
        except SlurmJobsError as error:
            return ToolFailure(error.code, str(error))

        matches = [
            (job, job_preview(job)) for job in response.jobs
            if job.get("job_id") == job_id and (cluster is None or job.get("cluster") == cluster)
        ]
        if start_time is not None:
            matches = [(record, job) for record, job in matches if job["time_unix_seconds"]["start"] == start_time]
        if not matches:
            return ToolFailure("job_not_found", "本次接口响应中没有与所给条件匹配的作业记录。")
        if len(matches) > 1:
            return ToolFailure("ambiguous_job_id", "这个作业 ID 对应多条记录，请补充集群或开始时间。")

        record, job = matches[0]
        if response.warning_count:
            cpu = {"availability": "pending_definition", "reason": "hpc_response_warnings",
                   "utilization": None, "formula_version": FORMULA_VERSION}
        else:
            cpu = analyze_job_cpu(record)
        return ToolSuccess(
            data={"job": job, "cpu": cpu, "warning_count": response.warning_count},
            evidence=ToolEvidence(
                source=JOB_URL.format(job_id=job_id),
                query_scope={"lookup": "job_id", **arguments},
                fetched_at=response.fetched_at,
            ),
        )

    return Tool(ANALYZE_JOB_CPU, analyze)
