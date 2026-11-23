"""Tool orchestration for the existing web conversation."""

from dataclasses import dataclass, field, replace
from datetime import datetime
import re
import sqlite3
from threading import Lock

from core.efficiency import REASON_MESSAGES
from core.job_query import (
    ANOMALIES, FIXTURE_MEMORY, daily_counts, data_quality, job_key, matches_time, normalize_job, remote_records, summarize,
)
from core.job_screening import WINDOW_SECONDS
from core.query_time import SHANGHAI, TimeInputError, shanghai_iso
from core.slurm_jobs import SlurmJobsError
from .chat_intent import Intent, parse_intent
from .runtime import Agent, Reply, Tool, ToolCall, ToolEvidence, ToolFailure, ToolSpec, ToolSuccess
from .web_diagnostics import DEFAULT_SCREENING_MODE, DiagnosisService
from .zhiyuan import ZhiyuanError


PAGE_SIZE = 20
SOURCE = {"sqlite": "本地 SQLite 抽样归档", "hpc_api": "交大 HPC API", "synthetic": "合成测试数据", "unavailable": "数据源暂不可用"}
REASONS = {**REASON_MESSAGES, "window_boundary": "时间贴近 API 请求边界，整段记账待核对。"}


@dataclass
class Dataset:
    kind: str
    rows: list
    coverage: dict
    fetched_at: str
    notes: list = field(default_factory=list)
    versions: dict = field(default_factory=dict, repr=False)

    @property
    def query_unavailable(self):
        return self.kind == "unavailable" or (self.kind == "hpc_api" and not self.coverage["windows_completed"])

    @property
    def partial(self):
        return self.kind == "hpc_api" and self.coverage["windows_completed"] < self.coverage["windows_total"]


@dataclass
class ChatState:
    lock: Lock = field(default_factory=Lock)
    intent: object = None
    data: Dataset | None = None
    ordered: list = field(default_factory=list)
    selected: dict | None = None
    latest_point: dict | None = None
    live_frame: dict | None = None


def sorted_rows(rows, intent):
    if intent.order in {"cpu", "memory"}:
        key = intent.order + "_ratio"
        return sorted(rows, key=lambda row: (row[key] is None, row[key] or 0, row["job_id"]))
    if intent.order == "longest":
        return sorted(rows, key=lambda row: (row["active_seconds"] is None, -(row["active_seconds"] or 0), row["job_id"]))
    key = {"submission": "submit", "end": "end"}.get(intent.basis, "start")
    return sorted(rows, key=lambda row: (-(row[key] or 0), row["job_id"]))


def public_row(row):
    hidden = {"key", "unstarted_sentinel", "zero_cpu_steps"}
    output = {key: value for key, value in row.items() if key not in hidden and value is not None}
    output["job_ref"] = row["key"]
    output["cpu_reason_text"] = REASONS.get(row["cpu_reason"], "整段 CPU 用量待核实。") if row["cpu_ratio"] is None else None
    for key in ("submit", "start", "end", "observed"):
        if key in output:
            output[key] = shanghai_iso(output[key])
    return output


def select_rows(data, intent):
    return [row for row in data.rows if (not intent.states or row["state"] in intent.states)
            and all(rule.matches(row) for rule in intent.filters)]


class ConversationRun:
    def __init__(self, state, source, archive, model=None, *, threshold=0.2, now=None, emit=None, memory_schema=None):
        self.state, self.source, self.archive, self.model, self.threshold = state, source, archive, model, threshold
        self.now = now or datetime.now(SHANGHAI)
        self.emit = emit or (lambda event: None)
        self.memory_schema = FIXTURE_MEMORY if source.kind == "synthetic" else memory_schema
        self.data = self.intent = self.result = None
        self.rows = []
        self.calls = []

    def progress(self, message, **fields):
        self.emit({"type": "progress", "message": message, **fields})

    def run(self, question, mode=DEFAULT_SCREENING_MODE):
        self.progress("正在理解查询需求和解析时间…")
        try:
            self.intent = parse_intent(question, self.state.intent, now=self.now, default_threshold=self.threshold)
            if self.intent.operation == "unknown" and self.model:
                self.intent = self.model_intent(question)
        except TimeInputError as error:
            return {"status": "needs_clarification", "message": str(error)}
        kind = self.intent.operation
        if kind == "unknown":
            return {"status": "needs_clarification",
                    "message": "请提供作业 ID、时间范围或筛选条件。例如“昨天完成的作业有多少？”"}
        if kind == "point" or (self.state.latest_point and kind == "detail" and not self.intent.job_ids
                               and self.state.intent and self.state.intent.operation == "point"):
            return self.point(question, mode)
        primary = {"coverage": "get_data_coverage", "detail": "get_job_detail", "live": "get_live_jobs"}.get(kind, "query_jobs")
        handlers = {
            "get_data_coverage": (self.coverage_tool, "正在核对归档覆盖范围…"),
            "query_jobs": (self.query_tool, "正在选择数据源并查询作业…"),
            "aggregate_jobs": (self.analysis_tool, "正在汇总作业和资源用量…"),
            "detect_inefficient_jobs": (self.analysis_tool, "正在计算资源利用率和筛选候选…"),
            "analyze_data_quality": (self.analysis_tool, "正在核查字段缺失和统计状态…"),
            "compare_jobs": (self.analysis_tool, "正在对比作业或两个时间范围…"),
            "get_job_detail": (self.detail_tool, "正在查询作业详情和步骤记录…"),
            "get_live_jobs": (self.live_tool, "正在读取后台采集的实时作业快照…"),
        }
        query_tool = [Tool(ToolSpec(name, status, {"type": "object", "properties": {}}), self.wrap(name, fn, status))
                      for name, (fn, status) in handlers.items()]
        agent = Agent(self, query_tool, max_tool_calls=3)
        self.first_tool = primary
        turn = agent.run(question)
        if self.result is None:
            return {"status": "needs_data", "message": turn.reply.text}
        self.result["tools"] = self.calls
        return self.result

    def live_tool(self):
        intent, previous = self.intent, self.state.intent
        priority = intent.job_ids[0] if len(intent.job_ids) == 1 else None
        current = self.source.live_snapshot(priority)
        continued = (previous and previous.operation == "live" and self.state.live_frame and not intent.latest
                     and (intent.page != previous.page or intent.ordinal is not None))
        frame = self.state.live_frame if continued and current["status"] in {"ready", "partial"} else current
        continued = frame is self.state.live_frame and continued
        status = frame["status"]
        rows = frame["jobs"]
        notes = []
        if frame.get("error"):
            notes.append(frame["error"]["message"])
        if frame.get("catalog_error"):
            notes.append(f"Slurm 作业列表刷新失败。 {frame['catalog_error']['message']}")
        coverage = frame["coverage"] or {}
        notes.extend(coverage.get("errors", {}).values())
        if intent.metric == "cpu" and not coverage.get("cpu_evaluated"):
            notes.extend(f"{reason}（{count} 条）" for reason, count in coverage.get("cpu_gaps", {}).items())
        if coverage.get("ambiguous_jobs"):
            notes.append(f"{coverage['ambiguous_jobs']} 个作业 ID 有多种分配记录，暂未纳入筛查。")
        if status in {"warming_up", "unavailable", "stale"}:
            message = {"warming_up": "后台正在采集第一批实时样本。",
                       "unavailable": "当前没有可用的实时作业快照。",
                       "stale": "当前采集已过期，以下为最近一次记录。"}[status]
        else:
            message = ""
        if intent.job_ids:
            rows = [row for row in rows if row["job_id"] in intent.job_ids]
            if not rows and status in {"ready", "partial"}:
                notes.append("该作业暂无运行记录。请核对作业 ID，或按已完成作业查询。")
        if intent.partition:
            rows = [row for row in rows if row["partition"].casefold() == intent.partition.casefold()]
        if intent.filters:
            rows = [row for row in rows if all(rule.matches(row) for rule in intent.filters)]
        metric = "memory_ratio" if intent.metric == "memory" else "efficiency"
        metric_time = "memory_observed_at" if metric == "memory_ratio" else "cpu_observed_end"
        measured = [row for row in rows if row[metric] is not None]
        if frame.get("updates_on_completion") and intent.live_filter:
            current_measurements = [row for row in measured if row.get(metric_time)
                                    and (self.now - datetime.fromisoformat(row[metric_time])).total_seconds() <= WINDOW_SECONDS]
            expired = len(measured) - len(current_measurements)
            if expired:
                notes.append(f"{expired} 条作业的最新观测已超过 {WINDOW_SECONDS // 60} 分钟，留作详情参考。")
            measured = current_measurements
        evaluated = len(measured)
        total = len(rows)
        if continued:
            rows = self.state.ordered
        elif intent.live_filter and intent.metric and status in {"ready", "partial"}:
            rows = [row for row in measured if intent.rank_only or row[metric] < intent.threshold]
            rows = sorted(rows, key=lambda row: (row[metric], row["job_id"]))
        else:
            rows = sorted(rows, key=lambda row: row["job_id"])
        if intent.top is not None:
            rows = rows[:intent.top]
        pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = min(intent.page, pages)
        offset = (page - 1) * PAGE_SIZE
        if not message:
            if intent.metric:
                label = "内存" if intent.metric == "memory" else "CPU"
                message = f"{total} 条匹配作业中，{evaluated} 条可评估{label}，本次列出 {len(rows)} 条。"
                if not evaluated:
                    notes.append("该指标暂不可筛查，缺数作业等待采样或覆盖更多分配节点。")
            else:
                message = f"本次快照有 {total} 条匹配的运行作业。"
        if intent.live_filter and intent.metric and not intent.rank_only and status in {"ready", "partial"}:
            notes.append(f"复核条件为观测用量低于分配容量的 {intent.threshold:.2%}。")
            if rows:
                if intent.metric == "memory":
                    notes.append("可对照多个采样窗口的内存波动及任务规模，再评估分配是否过大。")
                else:
                    notes.append("可先核对线程数、并行度和 I/O 等待，并观察低利用率是否持续。")
        selected = None
        if intent.ordinal is not None:
            if 1 <= intent.ordinal <= len(rows):
                selected = rows[intent.ordinal - 1]
            else:
                notes.append("上一轮排序中没有该序号。")
        elif priority and len(rows) == 1:
            selected = rows[0]
        if selected and status in {"ready", "partial"}:
            advice = []
            if selected["efficiency"] is not None and selected["efficiency"] < intent.threshold:
                advice.append("检查作业的线程数、并行度和 I/O 等待，确认低利用率持续多久。")
            if selected["memory_ratio"] is not None and selected["memory_ratio"] < intent.threshold:
                advice.append("对照峰值阶段和多次运行的内存用量，再评估是否减少申请。")
            selected = {**selected, "advice": advice}
        self.state.intent, self.state.live_frame = replace(intent, page=page), frame
        self.state.ordered, self.state.data, self.state.latest_point, self.state.selected = rows, None, None, None
        self.result = {
            "status": "complete" if status in {"ready", "partial"} else "needs_data", "message": message,
            "source": SOURCE["synthetic"] if self.source.kind == "synthetic" else "Slurm + Prometheus",
            "fetched_at": frame["queried_at"], "notes": notes, "live": True, "live_status": status,
            "coverage": coverage, "records": rows[offset:offset + PAGE_SIZE], "offset": offset,
            "page": page, "pages": pages, "live_detail": selected, "snapshot_count": frame.get("snapshot_count", 0),
        }
        return ToolSuccess({"matching": total}, ToolEvidence(self.source.kind, intent.scope(), frame["queried_at"] or self.now.isoformat()))

    def model_intent(self, question):
        """For unfamiliar phrasing, use one native tool to propose an ordinary query."""
        inferred = self.intent

        def accept(arguments):
            nonlocal inferred
            rewrite = arguments.get("query")
            if isinstance(rewrite, str) and 0 < len(rewrite) <= 300:
                try:
                    proposal = parse_intent(rewrite, self.state.intent, now=self.now, default_threshold=self.threshold)
                except TimeInputError:
                    pass
                else:
                    inferred = proposal
            return ToolSuccess({"accepted": inferred.operation != "unknown"}, self.evidence())

        spec = ToolSpec("resolve_readonly_query", "把用户的表达规范成明确的作业查询。保留原时间和条件，缺少必要条件时请提问。",
                        {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]})
        sanitized = re.sub(r"(?i)\bBearer\s+\S+|(?:[A-Za-z]:[\\/]|/(?:home|scratch|mnt)/)[^\s，。]+", "[已隐藏]", question)
        sanitized = re.sub(r"(?i)(?:令牌|密码|token|api[ _-]?key|账号|account|用户名)\s*[:：=]?\s*[A-Za-z0-9_.-]+",
                           "[已隐藏]", sanitized)
        try:
            Agent(self.model, [Tool(spec, accept)], max_tool_calls=1).run(sanitized)
        except ZhiyuanError:
            self.emit({"type": "notice", "message": "致远一号暂不可用，可以换一种表达后重试。"})
        if inferred.operation != "unknown":
            self.emit({"type": "notice", "message": "已根据对话解析查询条件，请核对报告中的时间和筛选口径。"})
        return inferred

    def next_action(self, context):
        """Choose the next tool from the prior result and current goal."""
        if not context.exchanges:
            name = self.first_tool
        else:
            last = context.exchanges[-1]
            if isinstance(last.result, ToolFailure):
                return Reply("needs_data", last.result.message)
            if last.call.name == "query_jobs":
                name = {"quality": "analyze_data_quality", "screen": "detect_inefficient_jobs",
                        "compare": "compare_jobs", "peers": "compare_jobs"}.get(self.intent.operation, "aggregate_jobs")
            elif (last.call.name == "detect_inefficient_jobs" and self.intent.depth
                  and last.result.data.get("candidates", 0)):
                name = "get_job_detail"
            else:
                return Reply("answered", "查询已完成。")
        return ToolCall(f"chat-{len(context.exchanges) + 1}", name, {})

    def evidence(self):
        return ToolEvidence(self.data.kind if self.data else "sqlite", self.intent.scope(),
                            self.data.fetched_at if self.data else self.now.isoformat())

    def wrap(self, name, handler, status):
        def execute(_):
            self.calls.append(name)
            self.progress(status)
            return handler()
        return execute

    def coverage_tool(self):
        try:
            coverage = self.archive.coverage(self.intent.period) if self.archive else {}
            if coverage.get("window_count"):
                coverage.update(self.archive.event_bounds())
        except (OSError, sqlite3.Error):
            return ToolFailure("archive_unavailable", "本地 SQLite 归档暂不可读，请检查文件路径和结构。")
        if not coverage.get("window_count"):
            return ToolFailure("archive_missing", "当前服务没有可用的本地采集窗口清单。")
        basis = "start" if self.intent.basis == "execution" else self.intent.basis
        first_event = coverage["event_bounds"][basis]["earliest"]
        message = f"本地最早的成功采集窗口始于 {coverage['first_capture']}。"
        if first_event:
            message += f"归档的{self.intent.scope()['time_basis_label']}可追溯到 {first_event}。" if basis != "start" else (
                f"归档中最早的作业开始时间为 {first_event}。")
        self.result = {"status": "complete", "message": message,
                       "source": SOURCE["sqlite"], "scope": self.intent.scope(), "coverage": coverage,
                       "fetched_at": self.now.isoformat(),
                       "notes": ["最早的采集窗口和最早的作业事件是不同口径。采集间隔存在缺口，作业覆盖完整性尚未验证。"]}
        return ToolSuccess({"capture_windows": coverage["window_count"]}, self.evidence())

    def load(self, intent, *, resume=None):
        period = intent.period
        if self.source.kind == "synthetic":
            stamp = int(self.now.timestamp())
            raw = self.source.chat_jobs(stamp)
            versions = {job_key(job): [{"job": job, "windows": [stamp], "boundary": False}] for job in raw}
            coverage = {"complete": True, "fixture": True}
            kind, notes = "synthetic", ["本结果使用合成测试数据。"]
            rows = (normalize_job(job, observed=stamp, memory_schema=self.memory_schema) for job in raw)
        else:
            versions, coverage, notes = {}, {}, []
            if self.archive:
                try:
                    coverage = self.archive.coverage(period)
                except (sqlite3.Error, OSError):
                    notes.append("本地 SQLite 归档暂不可读。")
            remote = self.source.kind == "hpc_api" and period and (intent.latest or not coverage.get("covers_sampling_interval"))
            if remote:
                kind = "hpc_api"
                versions, coverage = remote_records(self.source, period, self.progress, resume=resume,
                                                     memory_schema=self.memory_schema)
                if coverage["windows_completed"]:
                    notes.append("按报告中的时间口径匹配已返回的 HPC 记录。作业覆盖和时间截断行为待验证。")
                if coverage["windows_reused"]:
                    notes.append(f"本轮复用 {coverage['windows_reused']} 个已经成功的查询窗口。")
                if skipped := coverage["timed_out_windows"]:
                    first = skipped[0]
                    notes.append(f"{len(skipped)} 个短窗口超时。首个缺失区间为 "
                                 f"{shanghai_iso(first['start'])} 至 {shanghai_iso(first['end'])}。")
                if coverage["error"]:
                    notes.append(coverage["error"]["message"])
                if coverage["warnings"]:
                    notes.append(f"上游返回 {coverage['warnings']} 条警告，结果覆盖程度待核实。")
                rows = (normalize_job(history[-1]["job"], observed=int(datetime.fromisoformat(history[-1]["fetched_at"]).timestamp()),
                                      suspect_boundary=history[-1]["boundary"], memory_schema=self.memory_schema)
                        for history in versions.values())
            elif self.archive and coverage:
                kind = "sqlite"
                notes.append("本地归档采用抽样采集，查询结果仅覆盖文件中的保留快照。")
                if period and not coverage.get("covers_sampling_interval"):
                    notes.append("所选时间段存在采集空档，本次计数仅适用于已收录的作业。")
                rows = (normalize_job(job, observed=observed, memory_schema=self.memory_schema) for job, observed, _ in
                        self.archive.records(period, intent.basis, partition=intent.partition))
            else:
                kind, rows = "unavailable", ()
                notes.append("请配置本地归档、连接有权限的 HPC 数据源，或缩小时间范围。实时范围查询需要明确起止时间。")
        selected = [row for row in rows if type(row["job_id"]) is int and matches_time(row, period, intent.basis)
                    and (not intent.partition or (row["partition"] or "").casefold() == intent.partition.casefold())]
        if uncertain := sum(row["boundary_uncertain"] for row in selected):
            notes.append(f"{uncertain} 条记录的时间贴近请求边界，已排除其整段 CPU 利用率。")
        if kind == "hpc_api":
            coverage.update(observed_versions=sum(len(history) for history in versions.values()),
                            returned_jobs=len(versions), scope_matches=len(selected))
        return Dataset(kind, selected, coverage, datetime.now(SHANGHAI).isoformat(), notes, versions)

    def query_tool(self):
        intent = self.intent
        old = self.state.intent
        cached = self.state.data
        same_scope = cached and not intent.latest and not intent.scope_changed(old)
        continuation = same_scope and cached.partial and (intent.resume or cached.query_unavailable)
        if same_scope and not continuation and not cached.query_unavailable:
            self.data = cached
        else:
            try:
                resume = (cached.versions, cached.coverage) if continuation else None
                self.data = self.load(intent, resume=resume)
            except (sqlite3.Error, OSError):
                return ToolFailure("archive_unavailable", "本地归档读取失败，请检查 SQLite 文件或连接 HPC 后重试。")
        self.rows = select_rows(self.data, intent)
        return ToolSuccess({"matches": len(self.rows)}, self.evidence())

    def analysis_tool(self):
        intent, data, rows = self.intent, self.data, self.rows
        notes = list(data.notes)
        if data.query_unavailable:
            self.state.intent, self.state.data = intent, data
            self.state.ordered, self.state.selected, self.state.latest_point = [], None, None
            self.result = {"status": "needs_data", "message": "本次查询未取得可用数据。",
                           "scope": intent.scope(), "source": SOURCE[data.kind], "fetched_at": data.fetched_at,
                           "coverage": data.coverage, "notes": notes}
            return ToolSuccess({"candidates": 0}, self.evidence())
        aggregate = summarize(rows)
        extras = {}
        total = len(rows)
        is_screen = intent.operation == "screen"
        if is_screen:
            field = intent.metric + "_ratio"
            metric_rows = [row for row in rows if row[field] is not None
                           and (intent.rank_only or row[field] < intent.threshold)]
            rows = sorted_rows(metric_rows, intent)
            extras["screening_summary"] = {
                "metric": intent.metric, "threshold": None if intent.rank_only else intent.threshold, "candidates": len(rows),
                "unused_cpu_seconds": sum(row["cpu_unused_seconds"] for row in rows) if rows and intent.metric == "cpu" else None,
                "memory_slack_bytes": sum(row["memory_slack_bytes"] for row in rows) if rows and intent.metric == "memory" else None,
            }
            if intent.metric == "memory":
                basis = "concurrent_peak" if self.memory_schema else "seff"
                extras["screening_summary"]["memory_basis"] = basis
                if basis == "seff":
                    notes.append("seff 按步骤中的任务峰值之和取最大值。它是记账估算，同一时刻的作业并发总峰值需用监控核对。")
                    if not aggregate["memory_evaluated"]:
                        notes.append("没有同时具备足够步骤内存记账和有效申请量的样本。")
                elif not aggregate["memory_evaluated"]:
                    notes.append("缺少已验证的同一时刻作业级内存峰值或有效申请量。")
            elif intent.metric == "cpu":
                notes.append("CPU 利用率使用已完成作业整段 TotalCPU 有效步骤之和与分配容量。未使用容量为估算，供复核。")
        elif intent.operation == "anomaly":
            rows = [row for row in rows if row["state"] in ANOMALIES]
            notes.append("异常按 Slurm 状态筛选。可结合退出状态、日志及节点事件复核。")
        elif intent.operation == "quality":
            extras["quality"] = data_quality(rows)
            extras["quality"]["cpu_reasons"] = {REASONS.get(key, key): n for key, n in extras["quality"]["cpu_reasons"].items()}
            notes.append("步骤记录为 0 表示源字段观察值。证据不足时，无法确认真实 CPU 消耗为零。")
        elif intent.operation == "trend":
            if intent.period is None:
                return ToolFailure("missing_interval", "请给出要按天统计的日期范围。")
            extras["days"] = list(daily_counts(rows, intent.period, intent.basis))
            if intent.basis == "execution":
                notes.append("按运行区间逐日计算，跨天作业可能出现在多个日期。")
        elif intent.operation == "compare":
            baseline = replace(intent, period=intent.compare_period)
            error = data.coverage.get("error") or {}
            if error.get("code") in {"invalid_hpc_token", "permission_denied"}:
                notes.append("权限检查失败，已停止后续窗口和对照范围的查询。")
            else:
                try:
                    other = self.load(baseline)
                except (sqlite3.Error, OSError):
                    return ToolFailure("archive_unavailable", "无法读取对照时间段，请检查本地归档。")
                baseline_stats = None if other.query_unavailable else summarize(select_rows(other, baseline))
                differences = {field: aggregate[field] - baseline_stats[field]
                               for field in ("count", "cpu_seconds", "cpu_capacity", "memory_requested_mb")
                               if baseline_stats and not data.partial and not other.partial
                               and aggregate[field] is not None and baseline_stats[field] is not None}
                extras["comparison"] = {"scope": baseline.scope(), "source": SOURCE[other.kind], "fetched_at": other.fetched_at,
                                        "aggregate": baseline_stats, "coverage": other.coverage, "notes": other.notes,
                                        "differences": differences}
        elif intent.operation == "peers":
            if self.state.selected and self.state.selected["partition"]:
                rows = [row for row in rows if row["partition"] == self.state.selected["partition"]]
                notes.append(f"同类对照按分区 {self.state.selected['partition']} 匹配。")
                aggregate = summarize(rows)
                total = len(rows)
                extras["peer_reference"] = public_row(self.state.selected)
            elif not intent.partition:
                return ToolFailure("missing_partition", "请先选择一个作业，或说明用于对照的分区。")
        if intent.depth and intent.top is None:
            intent = self.intent = replace(intent, top=5)
        if not is_screen:
            rows = sorted_rows(rows, intent)
        if intent.top is not None:
            rows = rows[:intent.top]
        count = len(rows)
        pages = max(1, (count + PAGE_SIZE - 1) // PAGE_SIZE)
        page = min(intent.page, pages)
        offset = (page - 1) * PAGE_SIZE
        self.state.intent, self.state.data, self.state.ordered = replace(intent, page=page), data, rows
        self.state.selected = rows[0] if intent.top == 1 and rows else None
        self.state.latest_point = None
        metric_count = aggregate["cpu_evaluated" if intent.metric == "cpu" else "memory_evaluated"]
        if is_screen and not metric_count and total:
            message = f"读取到 {total} 条匹配记录，当前指标缺少可评估数据。"
        elif is_screen:
            message = f"在 {metric_count} 条指标可用的作业中，找到 {extras['screening_summary']['candidates']} 条候选。"
        elif not total:
            message = "在本次查询的数据范围内，暂未找到匹配记录。"
        else:
            message = f"本次读取到 {total} 条符合时间和资源条件的作业。"
        status = "needs_data" if is_screen and total and not metric_count else "complete"
        if data.partial:
            message = "仅完成部分时间窗口。" + message
            if status == "complete":
                status = "partial_data"
        self.result = {"status": status,
                       "message": message, "scope": intent.scope(), "source": SOURCE[data.kind], "fetched_at": data.fetched_at,
                       "coverage": data.coverage, "notes": notes, "aggregate": aggregate, "page": page, "pages": pages,
                       "offset": offset, "records": [public_row(row) for row in rows[offset:offset + PAGE_SIZE]], **extras}
        return ToolSuccess({"candidates": count}, self.evidence())

    def inspect_job(self, job_id, job_ref=None, point_at=None):
        """Open a result's details without altering the ongoing conversation."""
        previous = self.state
        data = previous.data
        if data and job_ref and job_ref not in data.versions and not any(row["key"] == job_ref for row in data.rows):
            data = None
        self.state = ChatState(intent=previous.intent, data=data)
        self.intent = Intent(operation="detail", job_ids=(job_id,), threshold=self.threshold)
        self.detail_tool(job_ref=job_ref, point_at=point_at)
        return self.result

    def raw_details(self, job_id, wanted=None, archived_matches=None, *, point_at=None):
        data = self.state.data
        if data and not self.intent.latest and data.versions:
            records = [(key, history) for key, history in data.versions.items() if key[1] == job_id
                       and (wanted is None or key == wanted["key"])]
            if records:
                output = []
                for _, history in records:
                    latest = history[-1]
                    row = normalize_job(latest["job"], observed=latest["windows"][-1], suspect_boundary=latest["boundary"],
                                        memory_schema=self.memory_schema, keep_steps=True)
                    row["observations"] = sum(len(version["windows"]) for version in history)
                    row["version_history"] = [{"state": safe["state"], "cpu_ratio": safe["cpu_ratio"],
                                               "recorded_at": version.get("fetched_at"),
                                               "observed_windows": [shanghai_iso(window) for window in version["windows"]]}
                                              for version in history for safe in (normalize_job(version["job"],
                                                                                               suspect_boundary=version["boundary"]),)]
                    output.append((row, data.kind))
                return output
        if self.source.kind == "synthetic":
            fixtures = (self.source.jobs_at(point_at).jobs if point_at is not None else
                        self.source.chat_jobs(int(self.now.timestamp())))
            return [(normalize_job(job, memory_schema=self.memory_schema, keep_steps=True), "synthetic")
                    for job in fixtures if job["job_id"] == job_id and (wanted is None or job_key(job) == wanted["key"])]
        if self.archive and not self.intent.latest and (wanted is None or not data or data.kind == "sqlite"):
            archived = []
            try:
                candidates = (archived_matches.get(job_id, []) if archived_matches is not None else
                              self.archive.records(job_ids=(job_id,)))
                for job, observed, observations in candidates:
                    if wanted is None or job_key(job) == wanted["key"]:
                        row = normalize_job(job, observed=observed, memory_schema=self.memory_schema, keep_steps=True)
                        row["observations"] = observations
                        archived.append((row, "sqlite"))
            except (sqlite3.Error, OSError):
                if self.source.kind != "hpc_api":
                    raise
            if archived:
                return archived
        if self.source.kind == "hpc_api":
            if wanted is not None and data and data.kind == "sqlite" and not self.intent.latest:
                return []
            response = self.source.job_by_id(job_id)
            return [(normalize_job(job, suspect_boundary=bool(response.warning_count), memory_schema=self.memory_schema, keep_steps=True), "hpc_api")
                    for job in response.jobs if job.get("job_id") == job_id and (wanted is None or job_key(job) == wanted["key"])]
        return []

    def detail_tool(self, *, job_ref=None, point_at=None):
        intent = self.intent
        picked = []
        if self.result and intent.depth:
            picked = self.state.ordered[:intent.top]
        elif intent.ordinal:
            if not 0 < intent.ordinal <= len(self.state.ordered):
                return ToolFailure("missing_reference", "当前排序结果中没有这条记录。")
            picked = [self.state.ordered[intent.ordinal - 1]]
        elif not intent.job_ids:
            if self.state.selected is None:
                return ToolFailure("missing_reference", "请提供要分析的作业 ID，或指出上一轮结果中的第几条。")
            picked = [self.state.selected]
        lookups = [(row["job_id"], row) for row in picked] or [(job_id, None) for job_id in intent.job_ids]
        if job_ref is not None:
            lookups = [(intent.job_ids[0], {"key": job_ref})]
        details, kinds, notes = [], set(), []
        archived_matches = None
        if self.archive and len(lookups) > 1 and not intent.latest and (not picked or self.state.data.kind == "sqlite"):
            try:
                archived_matches = {}
                for job, observed, observations in self.archive.records(job_ids=tuple(job_id for job_id, _ in lookups)):
                    archived_matches.setdefault(job["job_id"], []).append((job, observed, observations))
            except (sqlite3.Error, OSError):
                notes.append("本地归档批量读取失败。")
        for job_id, wanted in lookups:
            try:
                found = self.raw_details(job_id, wanted, archived_matches, point_at=point_at)
            except SlurmJobsError as error:
                notes.append(str(error))
                if error.code in {"invalid_hpc_token", "permission_denied"}:
                    break
                continue
            except (sqlite3.Error, OSError):
                notes.append("本地归档读取失败，请检查 SQLite 文件。")
                break
            if len(found) != 1:
                reason = "同号作业有多条记录，请先按时间或分区筛选后选择结果中的具体条目。" if found else "数据源中暂未找到该作业。"
                notes.append(f"作业 {job_id}：{reason}")
                continue
            row, kind = found[0]
            kinds.add(SOURCE[kind])
            known_threshold = self.state.intent.threshold if self.state.intent else intent.threshold
            threshold = known_threshold if self.result is None else intent.threshold
            detail = public_row(row)
            facts, hypotheses, suggestions = [], [], []
            if row["cpu_ratio"] is not None:
                facts.append(f"已用 {row['cpu_seconds']:g} CPU 秒，分配容量 {row['cpu_capacity']:g} CPU 秒。")
                if row["cpu_ratio"] < threshold:
                    facts.append(f"整段平均 CPU 利用率 {row['cpu_ratio']:.2%}，低于 {threshold:.2%}，可作为复核候选。")
                    hypotheses.append("并行度不足、等待 I/O 或运行阶段负载不均均有可能，需通过监控或日志核实。")
                    suggestions.append("核对线程数、并行度及 I/O 等待；可在相同输入下对照一次较少 CPU 核数的运行。")
                else:
                    facts.append(f"整段平均 CPU 利用率 {row['cpu_ratio']:.2%}，达到 {threshold:.2%} 的筛查阈值。")
            else:
                facts.append(detail["cpu_reason_text"])
            if row["memory_ratio"] is None:
                if row["memory_basis"] == "seff":
                    facts.append("步骤内存记账或申请量不足，暂无法用 seff 口径筛查。")
                else:
                    facts.append("缺少可用于计算作业内存利用率的作业级并发峰值与申请量。")
            else:
                metric = "seff 内存估算" if row["memory_basis"] == "seff" else "作业级并发内存峰值"
                facts.append(f"{metric}为申请量的 {row['memory_ratio']:.2%}。")
                if row["seff_memory_allocated_ratio"] is not None:
                    facts.append(f"seff 内存效率（估算值 ÷ 分配量）为 {row['seff_memory_allocated_ratio']:.2%}。")
                if row["memory_ratio"] < threshold:
                    suggestions.append("查看多次同类运行的作业内存用量和波动，预留安全余量后再测试较小申请量。")
            if row["state"] in ANOMALIES:
                facts.append(f"Slurm 记录的异常状态为 {row['state']}。")
                suggestion = {
                    "OUT_OF_MEMORY": "核对作业级内存上限、节点间内存分布和 OOM 日志。",
                    "TIMEOUT": "对照申请时限与各步骤进度，核对超时前是否卡在某一阶段。",
                }.get(row["state"], "结合 Slurm 退出状态、作业日志和节点事件复核异常。")
                suggestions.append(suggestion)
            details.append({**detail, "facts": facts, "hypotheses": hypotheses, "suggestions": suggestions})
            if len(lookups) == 1:
                self.state.selected = row
        if self.result:
            self.result["details"] = details
            self.result["notes"].extend(notes)
        else:
            if kinds == {SOURCE["synthetic"]}:
                notes.append("本结果使用合成测试数据。")
            if "本地 SQLite 抽样归档" in kinds:
                notes.append("详情来自归档中的保留快照。观测次数来自采集元数据，快照是否最新待核实。")
            self.result = {"status": "complete" if details else "needs_data",
                           "message": ("已查询作业资源与步骤。" if details else "暂时无法提供作业详情。"),
                           "details": details, "notes": notes, "source": "、".join(sorted(kinds)) or "当前数据源",
                           "fetched_at": self.state.data.fetched_at if picked and self.state.data else self.now.isoformat()}
        return ToolSuccess({"details": len(details)}, self.evidence())

    def point(self, question, mode):
        intent = self.intent
        if intent.operation != "point":
            if intent.ordinal and 0 < intent.ordinal <= len(self.state.ordered):
                candidate = self.state.ordered[intent.ordinal - 1]
                self.progress("正在查询该作业的整段记账和步骤…")
                self.intent = replace(intent, ordinal=None, job_ids=(int(candidate["job_id"]),))
                self.detail_tool(job_ref=candidate["job_ref"], point_at=intent.period.start)
                self.result["message"] = f"上一轮排序第 {intent.ordinal} 条是作业 {candidate['job_id']}。"
                self.result["point_evidence"] = self.state.latest_point["screening"] | {"candidates": [candidate]}
                return self.result
            return {"status": "needs_data", "message": "请提供上一轮筛查结果中的条目序号或作业 ID。"}
        self.progress("正在查找该时刻运行的作业并核对 CPU 证据…")
        diagnosis = DiagnosisService(self.source, threshold=intent.threshold)
        _, result = diagnosis.screen(question, intent.period.start, mode, intent.top if intent.rank_only else None,
                                     progress=self.progress)
        self.state.intent, self.state.latest_point = intent, result if result.get("screening") else None
        self.state.data, self.state.selected = None, None
        self.state.ordered = (result.get("screening") or {}).get("candidates", [])
        return result

    def narrate(self, result):
        if not self.model or result["status"] != "complete":
            return
        # Only send bounded, aggregate evidence or internally produced detail facts.
        evidence = {key: result[key] for key in ("message", "aggregate", "screening_summary", "notes") if key in result}
        evidence["job_findings"] = [{"facts": row["facts"], "suggestions": row["suggestions"]}
                                    for row in result.get("details", [])[:5]]
        self.progress("正在生成解读…")
        try:
            for content in self.model.stream_reply(evidence):
                self.emit({"type": "delta", "text": content})
        except ZhiyuanError as error:
            self.emit({"type": "notice", "message": str(error)})
