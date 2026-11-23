"""Read job accounting records from the authorized SJTU HPC endpoint."""

from dataclasses import dataclass
from datetime import datetime, timezone
from http.client import HTTPException
import json
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import zlib


JOBS_URL = "https://api.hpc.sjtu.edu.cn/slurmdb/jobs"
JOB_URL = "https://api.hpc.sjtu.edu.cn/slurmdb/job/{job_id}"
HPC_TOKEN_ENV = "SJTU_HPC_API_TOKEN"
ACCOUNTING_BODY_LIMIT = 96 * 1024 * 1024


class SlurmJobsError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SlurmJobsResponse:
    """Backend records and transport metadata for one accounting request."""

    jobs: list[dict]
    warning_count: int
    fetched_at: str


def read_hpc_json(response, *, deadline):
    """Read a bounded body and decode HTTP gzip when the gateway offers it."""
    headers = getattr(response, "headers", {})
    encoding = headers.get("Content-Encoding", "").strip().lower()
    if encoding not in ("", "identity", "gzip"):
        raise SlurmJobsError("invalid_hpc_response", "HPC 数据源返回了无法读取的压缩格式。")
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS) if encoding == "gzip" else None
    body = bytearray()
    received = 0
    try:
        while True:
            if monotonic() >= deadline:
                raise SlurmJobsError("hpc_timeout", "读取 HPC 响应数据超时。")
            chunk = response.read1(64 * 1024)
            if not chunk:
                break
            received += len(chunk)
            if decoder:
                chunk = decoder.decompress(chunk, ACCOUNTING_BODY_LIMIT - len(body) + 1)
            body.extend(chunk)
            if len(body) > ACCOUNTING_BODY_LIMIT:
                raise SlurmJobsError("hpc_response_too_large", "单个窗口的 HPC 响应超出本地读取上限。")
        content_length = headers.get("Content-Length", "")
        if (content_length.isdecimal() and received != int(content_length)) or (decoder and not decoder.eof):
            raise SlurmJobsError("hpc_incomplete_response", "HPC 响应传输提前结束。")
        if decoder and decoder.unused_data:
            raise ValueError("trailing gzip data")
        return json.loads(body)
    except HTTPException as error:
        raise SlurmJobsError("hpc_incomplete_response", "读取 HPC 响应时连接提前结束。") from error
    except (zlib.error, ValueError, UnicodeError) as error:
        raise SlurmJobsError("invalid_hpc_response", "HPC 数据源返回的响应格式无效或传输不完整。") from error


def unix_seconds(value: object) -> int:
    if not isinstance(value, str):
        raise ValueError
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or instant.microsecond:
        raise ValueError
    return int(instant.timestamp())


def fetch_slurm_jobs(
    token: str,
    start_time: int,
    end_time: int,
    *,
    usage_time_as_submit_time: bool | None = None,
    disable_truncate_usage_time: bool | None = None,
) -> SlurmJobsResponse:
    """Fetch a usage window. The optional upstream flags are for gateway experiments."""
    if type(start_time) is not int or type(end_time) is not int or not 0 < start_time < end_time:
        raise SlurmJobsError("invalid_window", "查询窗口须使用有效的 Unix 秒，并且结束时间晚于开始时间。")
    query: dict[str, int | str] = {"start_time": start_time, "end_time": end_time}
    for name, value in (
        ("usage_time_as_submit_time", usage_time_as_submit_time),
        ("disable_truncate_usage_time", disable_truncate_usage_time),
    ):
        if value is not None:
            query[name] = str(value).lower()
    return _get_jobs(token, f"{JOBS_URL}?{urlencode(query)}")


def fetch_slurm_job(token: str, job_id: int) -> SlurmJobsResponse:
    """Fetch accounting records for a numeric job ID without a usage window."""
    if type(job_id) is not int or job_id <= 0:
        raise SlurmJobsError("invalid_job_id", "请提供正整数作业 ID。")
    return _get_jobs(token, JOB_URL.format(job_id=job_id))


def _get_jobs(token: str, url: str) -> SlurmJobsResponse:
    if not token.strip():
        raise SlurmJobsError("missing_hpc_token", "请先连接有作业查询权限的 SJTU HPC 账号。")
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    request.add_unredirected_header("Authorization", f"Bearer {token}")
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.load(response)
    except HTTPError as error:
        if error.code == 401:
            raise SlurmJobsError("invalid_hpc_token", "平台拒绝了访问令牌，请重新连接 SJTU HPC。") from error
        if error.code == 403:
            raise SlurmJobsError("permission_denied", "当前账号没有该作业查询权限。") from error
        raise SlurmJobsError("hpc_http_error", f"平台作业查询失败（HTTP {error.code}）。") from error
    except (URLError, TimeoutError) as error:
        raise SlurmJobsError("hpc_connection_error", "连接 SJTU HPC 作业接口失败。") from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise SlurmJobsError("invalid_hpc_response", "平台未返回有效的 JSON 作业数据。") from error

    return parse_slurm_jobs(payload)


def parse_slurm_jobs(payload: object) -> SlurmJobsResponse:
    if not isinstance(payload, dict):
        raise SlurmJobsError("invalid_hpc_response", "平台作业响应结构不符合约定。")
    if payload.get("errors"):
        raise SlurmJobsError("hpc_query_error", "平台报告作业查询失败。")
    jobs = payload.get("jobs")
    warnings = payload.get("warnings", [])
    if not isinstance(jobs, list) or not all(isinstance(job, dict) for job in jobs) or not isinstance(warnings, list):
        raise SlurmJobsError("invalid_hpc_response", "平台作业响应结构不符合约定。")
    return SlurmJobsResponse(jobs, len(warnings), datetime.now(timezone.utc).isoformat())
