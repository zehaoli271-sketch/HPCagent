"""Interpret conversational scope while preserving explicitly inherited filters."""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import operator
import re

from core.query_time import COUNT, SHANGHAI, TimeInputError, TimeRange, number, parse_time_scope, resolve_event_period
from .web_diagnostics import parse_top_n, requested_screening_mode


STATES = {
    "COMPLETED": r"COMPLETED|已(?:经)?完成|成功完成|正常完成|(?:天|内|后)完成的",
    "PENDING": r"PENDING|排队|等待调度",
    "RUNNING": r"\bRUNNING\b|(?:正在|当前|还在)运行|运行中",
    "FAILED": r"FAILED|失败",
    "TIMEOUT": r"TIMEOUT|超时",
    "OUT_OF_MEMORY": r"OUT_OF_MEMORY|\bOOM\b|内存不足",
    "CANCELLED": r"CANCELLED|已取消|取消的",
    "SUSPENDED": r"SUSPENDED|已挂起",
    "NODE_FAIL": r"NODE_FAIL|节点故障",
}
ID = re.compile(r"(?:job[\s_-]*id|作业\s*(?:ID|号)?)(?:\s*(?:为|是|=|：|:|#))?\s*([1-9][0-9]{0,15})(?![0-9]|\s*[年年月日:点]|-)", re.I)
BARE_ID = re.compile(r"(?:^|[、,，和与及\s])([1-9][0-9]{0,15})(?![0-9年月日点:%％.\-])(?:$|[、,，和与及\s])")
RESOURCE = re.compile(
    rf"(CPU(?:核数)?|内存(?:申请量|分配量)?|(?:实际)?运行(?:时间|时长)?|节点(?:数)?)\s*"
    rf"(至少|最多|不低于|不超过|大于等于|小于等于|低于|小于|少于|高于|大于|超过|多于|等于|>=|<=|>|<|=)?\s*"
    rf"({COUNT})\s*(核|[KMGT](?:i)?B|小时|分钟|秒|个节点|台)", re.I
)
OPS = {"至少": ">=", "不低于": ">=", "大于等于": ">=", "最多": "<=", "不超过": "<=", "小于等于": "<=",
       "低于": "<", "小于": "<", "少于": "<", "高于": ">", "大于": ">", "超过": ">", "多于": ">", "等于": "="}
COMPARE = {"=": operator.eq, ">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le}
REFERENCE = r"这些|其中|刚才|上一轮|这条|那条|第.+[个条]|接着|继续|下一页|上一页"
ORDER = {"cpu": "CPU 利用率从低到高", "memory": "内存申请利用率从低到高",
         "longest": "有效运行时间从长到短", "recent": "按所选时间口径从近到远"}


@dataclass(frozen=True)
class Filter:
    field: str
    op: str
    value: float

    def matches(self, row):
        value = row.get(self.field)
        return value is not None and COMPARE[self.op](value, self.value)


@dataclass(frozen=True)
class Intent:
    operation: str = "query"
    period: TimeRange | None = None
    basis: str = "execution"
    states: tuple = ()
    filters: tuple = ()
    partition: str | None = None
    metric: str | None = None
    threshold: float = 0.2
    rank_only: bool = False
    order: str = "recent"
    top: int | None = None
    depth: bool = False
    page: int = 1
    latest: bool = False
    resume: bool = False
    live_filter: bool = False
    scope_note: str | None = None
    job_ids: tuple = ()
    ordinal: int | None = None
    compare_period: TimeRange | None = None

    def scope(self):
        from core.job_query import BASIS_LABEL

        return {**(self.period.as_dict() if self.period else {"timezone": "Asia/Shanghai"}),
                "time_basis": self.basis, "time_basis_label": BASIS_LABEL[self.basis],
                "states": self.states, "resource_filters": [vars(item) for item in self.filters],
                "partition": self.partition, "threshold": self.threshold if self.metric and not self.rank_only else None,
                "metric": self.metric, "rank_only": self.rank_only, "sort": ORDER[self.order],
                "latest": self.latest, "interval_rule": "[start,end)", "scope_note": self.scope_note}

    def scope_changed(self, other):
        return other is None or (self.period, self.basis, self.partition) != (other.period, other.basis, other.partition)


def parse_intent(question, previous=None, *, now=None, default_threshold=0.2):
    question = question.strip()
    comparison = re.search(r"(?:并)?(?:和|与|对比|相比|比较|对照)\s*((?:最近|过去|前|上|本|今|昨|[0-9]{1,4}[年月\-]).+)", question)
    primary = question[:comparison.start()] if comparison else question
    period = parse_time_scope(primary, now)
    baseline = parse_time_scope(comparison[1], now) if comparison else None
    if period and baseline and re.match(rf"前\s*{COUNT}\s*(?:天|周|小时|星期)", comparison[1]):
        width = baseline.end - baseline.start
        baseline = TimeRange(period.start - width, period.start)
    if comparison and period and baseline is None:
        raise TimeInputError("请指定用于对比的第二个时间范围。")
    if re.search(r"\b\d{1,2}[:：]\d{2}\b", question) and period is None:
        raise TimeInputError("请补充这个时刻对应的日期。")
    if period is None and re.search(r"最近几天|前几天|前段时间|上个月|月底|月初|前阵子", question):
        raise TimeInputError("请补充明确的日期范围，例如“最近 7 天”或“11 月 10 日到 15 日”。")

    ids = tuple(dict.fromkeys(int(match[1]) for match in ID.finditer(question)))
    if ids and re.search(r"对比|比较", question) and period is None:
        # Only accept unmarked numbers in an explicitly identified ID comparison.
        ids = tuple(dict.fromkeys((*ids, *(int(value) for value in BARE_ID.findall(question + " ")))))
    if question.isdecimal() and 0 < int(question) < 10 ** 16:
        ids = (int(question),)
    ordinal = re.search(rf"第\s*({COUNT})\s*(?:条|个)", question)
    ordinal = int(number(ordinal[1])) if ordinal else None
    page = re.search(rf"第\s*({COUNT})\s*页", question)
    page_number = int(number(page[1])) if page else None
    if page_number is None and "下一页" in question:
        page_number = (previous.page if previous else 0) + 1
    if page_number is None and "上一页" in question:
        page_number = max(1, (previous.page if previous else 1) - 1)
    reset = bool(re.search(r"新查询|从头|清空|重置", question))
    reference = bool(re.search(REFERENCE, question))
    if reset:
        previous = None
    if previous is None and (ordinal is not None or page_number is not None or (reference and not period and not ids)):
        raise TimeInputError("当前对话没有可引用的查询结果，请先查询一批作业。")
    intent = previous or Intent(threshold=default_threshold)
    intent = replace(intent, job_ids=ids, ordinal=ordinal, compare_period=baseline,
                     latest=bool(re.search(r"最新|重新查|刷新", question)),
                     resume=bool(re.search(r"续查|补查|重试|再查一次|继续查询", question)))
    daily = bool(re.search(r"每天|逐日|按天|按日|趋势|每日", question))
    if re.search(r"提交", primary):
        basis = "submission"
    elif re.search(r"开始|启动", primary):
        basis = "start"
    elif re.search(r"结束|完成|结算", primary):
        basis = "end"
    elif re.search(r"运行过|正在运行|在运行|运行区间|时刻", primary):
        basis = "execution"
    elif daily and previous is None:
        basis = "submission"
    else:
        basis = intent.basis
    if period is not None:
        period, scope_note = resolve_event_period(primary, period, basis)
        intent = replace(intent, period=period, scope_note=scope_note)
    intent = replace(intent, basis=basis)
    states = tuple(state for state, pattern in STATES.items() if re.search(pattern, question, re.I))
    if re.search(r"取消状态筛选|任意状态|不限状态", question):
        intent = replace(intent, states=())
    elif states:
        intent = replace(intent, states=states)
    elif basis == "end" and re.search(r"完成的(?:低效)?作业", primary):
        intent = replace(intent, states=("COMPLETED",))
    if re.search(r"取消资源筛选|不限资源", question):
        intent = replace(intent, filters=(), partition=None)
    filters = {item.field: item for item in intent.filters}
    for name, op, amount, unit in RESOURCE.findall(question):
        unit = unit.lower()
        if name.upper().startswith("CPU") and unit == "核":
            field, multiplier = "allocated_cpus", 1
        elif name.startswith("内存") and unit in {"kb", "mb", "gb", "tb", "kib", "mib", "gib", "tib"}:
            field = "allocated_memory_mb" if "分配" in name else "requested_memory_mb"
            multiplier = 1024 ** {"k": -1, "m": 0, "g": 1, "t": 2}[unit[0]]
        elif unit in {"小时", "分钟", "秒"} and "运行" in name:
            field, multiplier = "active_seconds", {"小时": 3600, "分钟": 60, "秒": 1}[unit]
        elif name.startswith("节点") and unit in {"个节点", "台"}:
            field, multiplier = "allocated_nodes", 1
        else:
            continue
        filters[field] = Filter(field, OPS.get(op, op or "="), number(amount) * multiplier)
    partition = re.search(r"(?:分区\s*(?:为|=|是)?\s*([A-Za-z0-9_.-]+)|([A-Za-z0-9_.-]+)\s*分区)", question)
    intent = replace(intent, filters=tuple(filters.values()), partition=(partition[1] or partition[2]) if partition else intent.partition)
    top = parse_top_n(question)
    generic_top = re.search(rf"(?:最严重的?|前)\s*({COUNT})\s*(?:条|个)", question)
    if generic_top and top is None:
        top = int(number(generic_top[1]))
    if top is not None and not 1 <= top <= 100:
        raise TimeInputError("请选择 1 到 100 条排名结果。")
    if page_number is not None and page_number < 1:
        raise TimeInputError("页码从第 1 页开始。")
    percent = re.search(rf"(?:低于|小于|低过|阈值(?:\s*(?:为|调到|改为|设为|设成|是|=))?|改成|<)\s*({COUNT})\s*[%％]", question)
    threshold = number(percent[1]) / 100 if percent else intent.threshold
    if not 0 < threshold <= 1:
        raise TimeInputError("请输入大于 0%、且不超过 100% 的利用率阈值。")
    metric = intent.metric
    if re.search(r"内存.*(?:过量|冗余|低效|利用率|浪费)|(?:过量|冗余|低效|浪费).*内存", question):
        metric = "memory"
    elif re.search(r"CPU.*(?:利用率|效率|浪费|低效)|最低效|低效|最严重|申请.*实际|不匹配", question, re.I):
        metric = "cpu"
    if re.search(r"取消低效筛选|全部作业|所有作业", question) and not re.search(r"低效|利用率|浪费", question):
        metric = None
    requested_mode = requested_screening_mode(question)
    rank_only = bool(re.search(r"最低效|利用率最低|效率最低|最低的", question)) if metric and not percent else False
    if not rank_only and not percent and metric == intent.metric and (reference or page_number or period or requested_mode):
        rank_only = intent.rank_only
    order = ("longest" if re.search(r"(?:运行|时长).*(?:排序|最长)|最长.*(?:运行|时长)", question)
             else metric if metric and (metric != intent.metric or top is not None or (percent and not intent.metric)) else intent.order)
    forced_longest = bool(re.search(r"最长.*哪|最长的(?:是|哪)", question))
    explain = bool(re.search(r"为什么.*(?:这条|那条).*低效|这条.*为什么|判定.*低效|判为低效", question))
    point_period = period or (intent.period if requested_mode and intent.operation == "point" else None)
    point_screen = bool(point_period and point_period.point and basis == "execution" and
                        (metric == "cpu" and re.search(r"低效|最低|利用率|效率", question)
                         or requested_mode and (metric == "cpu" or re.search(r"CPU", question, re.I))))
    live = not period and "COMPLETED" not in states and bool(
        re.search(r"当前|现在|实时|正在运行|运行中", question)
        or (previous and previous.operation == "live" and not states and
            (reference or page_number or ordinal is not None or percent or intent.latest or explain))
    )
    if live:
        operation = "live"
        screen_live = bool(percent or rank_only or re.search(r"低效|冗余|过量|浪费|最严重", question)
                           or (previous and previous.operation == "live" and previous.live_filter
                               and (reference or page_number or ordinal is not None)))
        intent = replace(intent, period=None, basis="execution", scope_note=None, states=("RUNNING",), live_filter=screen_live)
    elif ids or ordinal is not None or explain or re.search(r"这条作业|那条作业", question):
        operation = "detail"
    elif re.search(r"(?:数据库|数据|采集).*(?:覆盖|最早|完整|更新)|什么时候.*(?:数据库|能查)", question) and not reference:
        operation = "coverage"
    elif re.search(r"数据质量|缺失|缺少|覆盖率|无法计算|特殊值|零值|尚未生成", question) and not point_screen:
        operation = "quality"
    elif period and baseline:
        operation = "compare"
    elif point_screen:
        operation = "point"
        intent = replace(intent, period=point_period)
        metric = "cpu"
    elif re.search(r"同类|同分区.*对比|同行", question):
        operation = "peers"
    elif re.search(r"异常|OOM|OUT_OF_MEMORY|TIMEOUT|失败原因", question, re.I) and not re.search(r"查询.*作业", question):
        operation = "anomaly"
    elif daily:
        operation = "trend"
    elif metric:
        operation = "screen"
    elif re.search(r"统计|多少|数量|分布|占比|实际使用|使用情况|总量", question):
        operation = "summary"
    elif period is not None or states or filters or partition or page_number is not None or reference or reset or intent.latest or intent.resume:
        operation = "query"
    else:
        operation = "unknown"
    if intent.latest and intent.period is None and operation in {"query", "summary", "screen", "anomaly", "quality", "peers"}:
        end = int((now or datetime.now(SHANGHAI)).timestamp())
        intent = replace(intent, period=TimeRange(end - 3600, end))
    if daily and period and baseline is None and (calendar_days := re.search(rf"过去\s*({COUNT})\s*天", question)):
        whole_days = number(calendar_days[1])
        if isinstance(whole_days, int):
            end = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI).replace(hour=0, minute=0, second=0, microsecond=0)
            intent = replace(intent, period=TimeRange(int((end - timedelta(days=whole_days)).timestamp()), int(end.timestamp())))
    return replace(intent, operation=operation, metric=metric, threshold=threshold, rank_only=rank_only,
                   top=1 if forced_longest else top if top is not None else intent.top
                   if page_number or period or percent or requested_mode else None,
                   depth=bool(re.search(r"分析.*(?:最严重|前|最低)|(?:最严重|最低).*分析", question)),
                   order=order, page=page_number or 1)
