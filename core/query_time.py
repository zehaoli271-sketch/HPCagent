"""Parse conversation time scopes as Shanghai wall time and half-open UTC intervals."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


try:
    SHANGHAI = ZoneInfo("Asia/Shanghai")
except ZoneInfoNotFoundError:
    SHANGHAI = timezone(timedelta(hours=8), "Asia/Shanghai")

CHINESE_DIGITS = {**dict(zip("零一二三四五六七八九", range(10))), "两": 2, "〇": 0}
COUNT = r"(?:[0-9]+(?:\.[0-9]+)?|[一二两三四五六七八九]?十[一二三四五六七八九]?|[零〇一二两三四五六七八九])"
DAY = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}|(?:[0-9]{4}\s*年\s*)?[0-9]{1,2}\s*月\s*[0-9]{1,2}\s*[日号]|今天|昨天|前天")
CLOCK = re.compile(
    rf"\s*T?\s*(凌晨|早上|上午|中午|下午|傍晚|晚上)?\s*({COUNT})"
    rf"(?:(?:[:：]([0-9]{{2}})(?:[:：]([0-9]{{2}}))?)|(?:[点时]\s*({COUNT})?\s*分?))"
    r"\s*(Z|[+-][0-9]{2}:[0-9]{2})?"
)
RANGE = re.compile(r"\s*(?:到|至|~|～|—|–)\s*")
ROLLING = re.compile(rf"(?:(?:最近|过去|近)\s*({COUNT})?|前\s*({COUNT}))\s*(小时|天|周|星期)")


class TimeInputError(ValueError):
    """The user supplied a time expression that needs correction."""


def number(text):
    if not text:
        return 1
    if text.isascii():
        return float(text) if "." in text else int(text)
    if "十" in text:
        tens, ones = text.split("十")
        return 10 * CHINESE_DIGITS.get(tens, 1) + CHINESE_DIGITS.get(ones, 0)
    return CHINESE_DIGITS[text]


@dataclass(frozen=True)
class TimeRange:
    start: int
    end: int
    point: bool = False

    def as_dict(self):
        return {"start_time": shanghai_iso(self.start), "end_time": shanghai_iso(self.end),
                "timezone": "Asia/Shanghai", "point": self.point}


def shanghai_iso(timestamp):
    return datetime.fromtimestamp(timestamp, SHANGHAI).isoformat()


def resolve_event_period(question, period, basis):
    """Convert a completed-by cutoff into a bounded range of end events."""
    if period is None or not period.point or basis != "end":
        return period, None
    if not re.search(r"已(?:经)?完成|截至|截止|之前(?:完成|已结束)|前完成", question):
        return period, None
    midnight = datetime.fromtimestamp(period.start, SHANGHAI).replace(hour=0, minute=0, second=0, microsecond=0)
    before = re.search(r"之前(?:完成|已结束)|前完成", question)
    end = period.start if before else period.end
    if int(midnight.timestamp()) >= end:
        raise TimeInputError("请补充要查询的起始日期，默认范围从所选日期的零点开始。")
    return TimeRange(int(midnight.timestamp()), end), "completion_before" if before else "completion_cutoff"


def _day(text, now):
    relative = {"今天": 0, "昨天": 1, "前天": 2}
    if text in relative:
        return now.date() - timedelta(days=relative[text])
    parts = [int(part) for part in re.findall(r"[0-9]+", text)]
    return date(*(parts if len(parts) == 3 else [now.year, *parts]))


def _on_day(day, clock):
    if clock is None:
        return datetime.combine(day, time(), SHANGHAI)
    period, hour, colon_min, second, named_min, offset = clock.groups()
    hour = number(hour)
    if not isinstance(hour, int):
        raise ValueError
    if period in {"下午", "傍晚", "晚上"} and hour < 12:
        hour += 12
    elif period in {"凌晨", "早上", "上午"} and hour == 12:
        hour = 0
    elif period == "中午" and hour < 6:
        hour += 12
    minute = number(colon_min or named_min) if colon_min or named_min else 0
    value = datetime.combine(day, time(hour, minute, int(second or 0)), SHANGHAI)
    if offset:
        suffix = "+00:00" if offset == "Z" else offset
        value = datetime.fromisoformat(value.replace(tzinfo=None).isoformat() + suffix)
    return value


def parse_time_scope(question, now=None):
    """Return a requested interval or a single historical evaluation second."""
    now = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI).replace(microsecond=0)
    midnight = datetime.combine(now.date(), time(), SHANGHAI)
    try:
        if match := ROLLING.search(question):
            amount = number(match[1] or match[2])
            hours = amount * {"小时": 1, "天": 24, "周": 168, "星期": 168}[match[3]]
            start, end = now - timedelta(hours=hours), now
        elif re.search(r"本周|这周|上周", question):
            monday = midnight - timedelta(days=now.weekday())
            start, end = (monday - timedelta(days=7), monday) if "上周" in question else (monday, now)
        elif match := DAY.search(question):
            day = _day(match[0], now)
            tail = question[match.end():]
            left_clock = CLOCK.match(tail)
            if left_clock:
                tail = tail[left_clock.end():]
            start = _on_day(day, left_clock)
            separator = RANGE.match(tail)
            if separator:
                tail = tail[separator.end():]
                right_day = DAY.match(tail)
                day_of_month = re.match(r"([0-9]{1,2})\s*[日号]", tail)
                if right_day:
                    next_day = _day(right_day[0], now)
                elif day_of_month:
                    next_day = day.replace(day=int(day_of_month[1]))
                    right_day = day_of_month
                else:
                    next_day = day
                right_clock = CLOCK.match(tail[right_day.end():] if right_day else tail)
                if not (right_day or right_clock):
                    raise TimeInputError("请补充结束日期或时刻。")
                end = _on_day(next_day, right_clock)
                if right_clock and not right_day and end < start:
                    end += timedelta(days=1)
                elif right_day and not right_clock:
                    end += timedelta(days=1)
                if not right_clock and next_day == now.date():
                    end = min(end, now)
            elif left_clock:
                if start > now:
                    raise TimeInputError("请输入已经过去的查询时刻。")
                at = int(start.timestamp())
                return TimeRange(at, at + 1, point=True)
            else:
                end = start + timedelta(days=1)
                if day == now.date():
                    end = min(end, now)
        else:
            return None
    except (TypeError, ValueError, OverflowError) as error:
        if isinstance(error, TimeInputError):
            raise
        raise TimeInputError("日期或时刻无效，请提供明确的年月日与时间。") from error
    if end > now:
        raise TimeInputError("查询结束时间尚未到达，请指定已经过去的范围。")
    if start >= end:
        raise TimeInputError("查询结束时间须晚于开始时间。")
    return TimeRange(int(start.timestamp()), int(end.timestamp()))
