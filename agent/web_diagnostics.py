"""Request-scoped Agent workflows for the web diagnosis endpoint."""

from datetime import datetime, timedelta, timezone
from http import HTTPStatus
import json
import re
from uuid import uuid4

from core.efficiency import REASON_MESSAGES
from core.job_cpu import MICROSECONDS, allocated_cpu, analyze_job_cpu
from core.job_query import job_key, remote_records
from core.query_time import TimeRange
from core.job_screening import (
    DEFAULT_CPU_THRESHOLD,
    WINDOW_SECONDS,
    find_jobs_running_at,
    get_job_cpu_usage_window,
    screen_low_efficiency_jobs,
)
from core.slurm_jobs import SlurmJobsError

from .runtime import Agent, ModelContext, Reply, Tool, ToolCall, ToolEvidence, ToolFailure, ToolSpec, ToolSuccess
from .slurm_tools import job_cpu_tool
from .zhiyuan import ZhiyuanError


SHANGHAI = timezone(timedelta(hours=8))
DATE_TIME = re.compile(
    r"([0-9]{4})[-年]([0-9]{1,2})[-月]([0-9]{1,2})日?[T\s]*"
    r"([0-9]{1,2}):([0-9]{2})(?::([0-9]{2}))?(Z|[+-][0-9]{2}:[0-9]{2})?"
)
JOB_ID = re.compile(r"(?:作业\s*(?:ID\s*)?|\bjob\s*(?:id\s*)?)[#：:\s]*([0-9]+)(?![0-9_+])", re.I)
RANK_COUNT = r"[0-9]+|[一二两三四五六七八九]?十[一二三四五六七八九]?|[一二两三四五六七八九]"
LOWEST = r"(?:利用率最低|效率最低|最低效)"
RANKING = re.compile(
    rf"{LOWEST}\s*的?\s*前?\s*(?P<after>{RANK_COUNT})\s*(?:个|条)"
    rf"|前\s*(?P<before>{RANK_COUNT})\s*(?:个|条)?\s*(?:CPU\s*)?{LOWEST}",
    re.I,
)
CHINESE_DIGITS = {**dict(zip("一二三四五六七八九", range(1, 10))), "两": 2}
DEFAULT_SCREENING_MODE = "auto"
SCREENING_MODES = (DEFAULT_SCREENING_MODE, "lifetime", "window")
FIVE_MINUTE_WINDOW = re.compile(r"前\s*(?:的\s*)?[5５五]\s*分钟|(?<![0-9０-９])[5５五]\s*分钟(?:窗口|监控)")
LIFETIME_REQUEST = re.compile(r"整段(?:运行|复盘)|整个运行(?:期间|过程)")
ERROR_HTTP = {
    "INVALID_REQUEST": HTTPStatus.BAD_REQUEST,
    "JOB_NOT_FOUND": HTTPStatus.NOT_FOUND,
    "UPSTREAM_ERROR": HTTPStatus.BAD_GATEWAY,
    "UPSTREAM_TIMEOUT": HTTPStatus.GATEWAY_TIMEOUT,
}


def iso(seconds):
    return datetime.fromtimestamp(seconds, SHANGHAI).isoformat()


def envelope(status, message, **fields):
    return {
        "schema_version": "1.0",
        "request_id": uuid4().hex,
        "status": status,
        "message": message,
        "data_source": None,
        "job": None,
        "diagnosis": None,
        "error": None,
        "screening": None,
        **fields,
    }


def error_response(code, message):
    return ERROR_HTTP[code], envelope("error", message, error={"code": code, "message": message})


def report(facts=(), gaps=(), recommendations=()):
    return {"facts": list(facts), "hypotheses": [], "data_gaps": list(gaps),
            "recommendations": list(recommendations)}


def parse_timestamp(match):
    year, month, day, hour, minute, second = (int(part or 0) for part in match.groups()[:6])
    offset = match[7]
    if offset:
        suffix = "+00:00" if offset == "Z" else offset
        instant = datetime.fromisoformat(f"{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{suffix}")
    else:
        instant = datetime(year, month, day, hour, minute, second, tzinfo=SHANGHAI)
    return int(instant.timestamp())


def parse_top_n(question):
    match = RANKING.search(question)
    if not match:
        return None
    count = match["after"] or match["before"]
    if count.isdecimal():
        return int(count)
    if "十" in count:
        tens, ones = count.split("十")
        return 10 * CHINESE_DIGITS.get(tens, 1) + CHINESE_DIGITS.get(ones, 0)
    return CHINESE_DIGITS[count]


def requested_screening_mode(question):
    """Identify an explicitly requested CPU time scope in a chat message."""
    if LIFETIME_REQUEST.search(question):
        return "lifetime"
    if FIVE_MINUTE_WINDOW.search(question):
        return "window"
    return None


class ToolPlan:
    """Use a fixed read-only plan once the user's request has an exact scope."""

    def __init__(self, calls):
        self.calls = calls

    def next_action(self, context):
        if context.exchanges and isinstance(context.exchanges[-1].result, ToolFailure):
            return Reply("needs_data", context.exchanges[-1].result.message)
        index = len(context.exchanges)
        if index == len(self.calls):
            return Reply("answered", "诊断流程已完成。")
        name, arguments = self.calls[index]
        return ToolCall(f"web-{index + 1}", name, arguments)


class ScreeningRun:
    def __init__(self, source, at, mode, threshold, top_n, progress):
        self.source, self.at, self.mode, self.threshold = source, at, mode, threshold
        self.top_n = top_n
        self.start = at - WINDOW_SECONDS
        self.progress = progress
        self.history_coverage = None

    def run(self, question):
        handlers = (self.find_jobs_running_at, self.get_job_cpu_usage_window, self.screen_low_efficiency_jobs)
        tools = [
            Tool(ToolSpec(fn.__name__, fn.__doc__, {"type": "object", "properties": {}}), fn)
            for fn in handlers
        ]
        agent = Agent(ToolPlan([(tool.spec.name, {}) for tool in tools]), tools)
        return agent.run(question).exchanges[-1].result

    def find_jobs_running_at(self, _):
        """按记账记录查找指定时刻在运行的作业。"""
        try:
            if self.source.kind == "hpc_api":
                versions, self.history_coverage = remote_records(
                    self.source, TimeRange(self.at, self.at + 1, point=True), self.progress
                )
                if not self.history_coverage["windows_completed"]:
                    error = self.history_coverage["error"]
                    return ToolFailure(error["code"], error["message"])
                records = [history[-1]["job"] for history in versions.values()]
                fetched_at = max((history[-1]["fetched_at"] for history in versions.values()),
                                 default=datetime.now(timezone.utc).isoformat())
                self.warnings = self.history_coverage["warnings"]
            else:
                response = self.source.jobs_at(self.at)
                records, fetched_at, self.warnings = response.jobs, response.fetched_at, response.warning_count
        except SlurmJobsError as error:
            return ToolFailure(error.code, str(error))
        self.jobs, self.uncertain = find_jobs_running_at(records, self.at)
        self.evidence = ToolEvidence(
            source=self.source.kind,
            query_scope={"at": iso(self.at), "mode": self.mode},
            fetched_at=fetched_at,
        )
        return ToolSuccess({"running_count": len(self.jobs), "uncertain_count": self.uncertain}, self.evidence)

    def get_job_cpu_usage_window(self, _):
        """按所选口径读取五分钟监控或计算整段记账用量。"""
        problem = None
        samples = {}
        if self.warnings:
            problem = "作业记账查询带有上游警告，数据范围待核实。"
        elif self.mode == "window" and self.jobs:
            try:
                samples = self.source.monitor_jobs(self.jobs, self.start, self.at)
            except SlurmJobsError as error:
                problem = str(error)
        self.rows = []
        for job in self.jobs:
            if problem:
                metric = {"utilization": None, "reason": problem}
            elif self.mode == "lifetime":
                metric = self.lifetime(job)
            else:
                metric = get_job_cpu_usage_window(job, samples.get(job["job_id"]), self.start, self.at)
            self.rows.append({"job_id": str(job["job_id"]), "job_ref": job_key(job),
                              "partition": job.get("partition"), **metric})
        return ToolSuccess({"records": len(self.rows)}, self.evidence)

    def lifetime(self, job):
        timing = job["time"]
        if self.source.kind == "hpc_api" and (timing["start"] == self.at or timing["end"] == self.at + 1):
            return {"utilization": None, "reason": "作业时间贴近查询边界，整段时间待核实。"}
        metric = analyze_job_cpu(job)
        if metric["availability"] != "valid":
            return {"utilization": None, "reason": REASON_MESSAGES.get(metric["reason"], "作业记账待核实。")}
        return {"utilization": metric["utilization"], "cpu_seconds": metric["used_cpu_microseconds"] / MICROSECONDS,
                "capacity_cpu_seconds": metric["capacity_cpu_seconds"], "reason": None}

    def screen_low_efficiency_jobs(self, _):
        """按请求输出最低排名或低于 CPU 阈值的作业。"""
        result = screen_low_efficiency_jobs(self.rows, self.threshold, top_n=self.top_n)
        result.update(
            mode=self.mode, at=iso(self.at), window_start=iso(self.start) if self.mode == "window" else None,
            running_count=len(self.jobs), uncertain_count=self.uncertain,
            coverage="synthetic" if self.source.kind == "synthetic" else "not_verified",
            cpu_source=self.source.cpu_source if self.mode == "window" else "Slurm TotalCPU",
        )
        return ToolSuccess(result, self.evidence)


def _step_summary(step):
    ident = (step.get("step") or {}).get("id", "")
    suffix = str(ident).rsplit(".", 1)[-1]
    timing = step.get("time") or {}
    total = timing.get("total") or {}
    seconds, micros = total.get("seconds"), total.get("microseconds")
    used = seconds + micros / MICROSECONDS if all(type(n) is int and n >= 0 for n in (seconds, micros)) else None
    states = step.get("state") or []
    pending = "RUNNING" in states
    max_tres = ((step.get("tres") or {}).get("requested") or {}).get("max") or []
    max_rss = max((entry["count"] for entry in max_tres
                   if entry.get("type") == "mem" and type(entry.get("count")) is int and entry["count"] > 0),
                  default=None)
    return {
        "step_id": str(ident), "kind": suffix if suffix in {"batch", "extern"} else
        ("ordinary" if suffix.isdecimal() else "other"),
        "state": next(iter(states), None), "allocated_cpus": allocated_cpu(step),
        "elapsed_seconds": timing.get("elapsed"), "cpu_time_seconds": None if pending else used,
        "cpu_accounting_pending": pending, "max_rss_bytes": max_rss,
    }


class DiagnosisService:
    def __init__(self, source, narrator=None, threshold=DEFAULT_CPU_THRESHOLD):
        self.source, self.narrator, self.threshold = source, narrator, threshold

    def diagnose(self, payload):
        question = payload.get("question") if isinstance(payload, dict) else None
        if not isinstance(question, str) or not question.strip() or len(question) > 300:
            return error_response("INVALID_REQUEST", "请输入 1 至 300 字的诊断问题。")
        if re.search(r"内存|显存|\bGPU\b", question, re.I):
            return HTTPStatus.OK, envelope("needs_clarification", "当前支持 CPU 诊断。请确认你要查看 CPU 使用情况。")
        selected = payload.get("job_id")
        if selected is not None and (not isinstance(selected, str) or not re.fullmatch(r"[0-9]{1,16}", selected)
                                     or int(selected) == 0):
            return error_response("INVALID_REQUEST", "作业 ID 须为正整数形式的字符串。")
        ids = {int(match[1]) for match in JOB_ID.finditer(question)}
        if selected is not None:
            ids.add(int(selected))
        times = list(DATE_TIME.finditer(question))
        if len(ids) > 1 or len(times) > 1 or (ids and times):
            return HTTPStatus.OK, envelope("needs_clarification", "请选择一个作业 ID 或一个查询时刻。")
        if ids and min(ids) > 0:
            status, result = self.single(question, next(iter(ids)))
        elif times:
            if re.search(r"已(?:经)?完成|完成的作业|已结束|截至|截止", question):
                return HTTPStatus.OK, envelope("needs_clarification",
                                               "请使用作业对话，按结束时间查询已完成作业。")
            try:
                at = parse_timestamp(times[0])
                if not 300 < at <= datetime.now(timezone.utc).timestamp():
                    raise ValueError
            except (ValueError, OverflowError):
                return HTTPStatus.OK, envelope("needs_clarification", "请提供一个有效的历史日期和时刻。")
            mode = payload.get("mode", DEFAULT_SCREENING_MODE)
            if mode not in SCREENING_MODES:
                return error_response("INVALID_REQUEST", "请选择自动、整段运行复盘或时刻前 5 分钟。")
            top_n = parse_top_n(question)
            if top_n == 0:
                return HTTPStatus.OK, envelope("needs_clarification", "请输入需要排名的作业数量。")
            status, result = self.screen(question, at, mode, top_n)
        else:
            return HTTPStatus.OK, envelope("needs_clarification", "请提供一个作业 ID，或输入年、月、日和具体时刻。")
        if self.narrator and result["diagnosis"] and result["status"] == "complete":
            self.explain(result)
        return status, result

    def data_source(self, evidence):
        return {"kind": self.source.kind, "queried_at": evidence.fetched_at,
                "source_updated_at": evidence.source_updated_at}

    def screen(self, question, at, mode=DEFAULT_SCREENING_MODE, top_n=None, *, progress=None):
        if mode == DEFAULT_SCREENING_MODE:
            mode = requested_screening_mode(question) or "lifetime"
        workflow = ScreeningRun(self.source, at, mode, self.threshold, top_n, progress or (lambda *_args, **_fields: None))
        result = workflow.run(question)
        if isinstance(result, ToolFailure):
            timeouts = {"hpc_timeout", "hpc_query_budget"}
            status, data = error_response("UPSTREAM_TIMEOUT" if result.code in timeouts else "UPSTREAM_ERROR", result.message)
            if workflow.history_coverage:
                data.update(
                    history_coverage=workflow.history_coverage,
                    data_source={"kind": self.source.kind, "queried_at": datetime.now(timezone.utc).isoformat()},
                    diagnosis=report(gaps=[f"未取得 {iso(at)} 的作业记账记录。"]),
                )
            return status, data
        data = result.data
        evaluated = data["evaluated_count"]
        facts = [f"核对到 {data['running_count']} 条运行记录，{evaluated} 条可评估。"]
        if mode == "lifetime":
            facts.append("按指定时刻选出在运行的作业，再用可靠的完成记账计算各作业整段平均 CPU 利用率。")
        gaps = [f"{reason}（{count} 条）" for reason, count in data["data_gaps"].items()]
        if data["uncertain_count"]:
            gaps.append(f"{data['uncertain_count']} 条记录无法确认该时刻的运行状态。")
        if data["coverage"] == "not_verified":
            gaps.append("筛查范围限当前接口返回的记录，覆盖度仍待核对。")
        if top_n is not None:
            verdict = "complete" if evaluated >= top_n else "insufficient_data"
            if evaluated:
                facts.append(f"按 CPU 利用率从低到高列出 {data['candidate_count']} 条可评估作业。")
                message = ("已列出可评估作业中的最低排名。"
                           if verdict == "complete" else f"只有 {evaluated} 条作业可排名，已列出全部可评估作业。")
            elif mode == "window" and not data["cpu_source"] and data["running_count"]:
                message = "缺少作业级 CPU 历史监控，无法给出该时刻的排名。"
            else:
                message = "当前查询口径下没有可用于排名的 CPU 数据。"
        else:
            verdict = "complete" if evaluated else "insufficient_data"
            if evaluated:
                facts.append(f"按低于 {data['threshold']:.2%} 的阈值筛出 {data['candidate_count']} 条复核候选。")
            if mode == "lifetime":
                message = ("已按整段平均 CPU 利用率筛查可评估作业。"
                           if evaluated else "当时在运行的作业暂无足够记账证据用于整段复盘。")
            else:
                message = ("已完成可评估作业的 CPU 窗口筛查。"
                           if evaluated else "该时刻前 5 分钟暂无足够监控数据完成 CPU 筛查。")
        return HTTPStatus.OK, envelope(verdict, message, screening=data,
                                       data_source=self.data_source(result.evidence), diagnosis=report(facts, gaps))

    def single(self, question, job_id):
        response = None

        def fetch_detail(selected_id):
            nonlocal response
            response = self.source.job_by_id(selected_id)
            return response

        tool = job_cpu_tool(fetch_detail=fetch_detail)
        agent = Agent(ToolPlan([(tool.spec.name, {"job_id": job_id})]), [tool])
        result = agent.run(question).exchanges[0].result
        if isinstance(result, ToolFailure):
            if result.code == "ambiguous_job_id":
                return HTTPStatus.OK, envelope("needs_clarification", "同号作业有多条记录，请先确认目标作业。")
            codes = {"job_not_found": "JOB_NOT_FOUND", "hpc_timeout": "UPSTREAM_TIMEOUT"}
            return error_response(codes.get(result.code, "UPSTREAM_ERROR"), result.message)
        record = next(job for job in response.jobs if job.get("job_id") == job_id)
        metric = result.data["cpu"]
        value = metric["utilization"]
        reason = REASON_MESSAGES.get(metric["reason"], "作业统计或数据源警告待核实。") if value is None else None
        quality = "unavailable" if value is None else ("verified" if self.source.kind == "synthetic" else "unverified")
        timing = record.get("time") or {}
        job = {
            "job_id": str(job_id), "state": next(iter(result.data["job"]["states"]), None),
            "partition": record.get("partition"), "allocated_cpus": allocated_cpu(record),
            "elapsed_seconds": timing.get("elapsed"), "suspended_seconds": timing.get("suspended"),
            "cpu_efficiency": {"value": value, "quality": quality, "reason": reason},
            "steps": [_step_summary(step) for step in record.get("steps") or [] if isinstance(step, dict)],
        }
        facts = []
        if value is not None:
            used = metric["used_cpu_microseconds"] / MICROSECONDS
            capacity = metric["capacity_cpu_seconds"]
            facts = [f"有效运行 {metric['active_seconds']} 秒，累计使用 {used:g} CPU 秒。",
                     f"分配容量为 {capacity:g} CPU 秒，整段平均 CPU 利用率为 {value:.2%}。"]
        return HTTPStatus.OK, envelope(
            "complete" if value is not None else "insufficient_data",
            "已完成单作业 CPU 分析。" if value is not None else "这条作业暂缺可信的 CPU 利用率。",
            job=job, data_source=self.data_source(result.evidence),
            diagnosis=report(facts, [reason] if reason else []),
        )

    def explain(self, result):
        context = ModelContext(
            instructions="用中文用一小段话解释给定诊断事实。保持原有口径和结论，勿补造原因或承诺缩容效果。",
            question=json.dumps(result["diagnosis"], ensure_ascii=False),
            tools=(), history=(), exchanges=(),
        )
        try:
            reply = self.narrator.next_action(context)
        except ZhiyuanError:
            return
        if isinstance(reply, Reply) and reply.text.strip():
            result["explanation"] = reply.text
