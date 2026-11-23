"""Calculate whole-job CPU utilization from a lifetime accounting record."""


NO_VAL = 0xFFFFFFFE
MICROSECONDS = 1_000_000
FORMULA_VERSION = "job-cpu-v1"


def _field(record, *path):
    for part in path:
        if not isinstance(record, dict):
            return None
        record = record.get(part)
    return record


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _cpu_count(entries):
    if not isinstance(entries, list):
        return None
    counts = [entry.get("count") for entry in entries
              if isinstance(entry, dict) and entry.get("type") == "cpu"]
    if len(counts) == 1 and _integer(counts[0], 1):
        return counts[0]
    return None


def allocated_cpu(job: dict) -> int | None:
    return _cpu_count(_field(job, "tres", "allocated"))


def _step_cpu_us(step):
    seconds = _field(step, "time", "total", "seconds")
    micros = _field(step, "time", "total", "microseconds")
    if _integer(seconds) and _integer(micros):
        return seconds * MICROSECONDS + micros
    return None


def _elapsed_shape(start, end, elapsed, suspended):
    if type(start) is int and start == NO_VAL:
        return "never_started"
    if not all(_integer(value) for value in (start, end, elapsed, suspended)):
        return "missing_or_invalid"
    if not 0 < start < end < NO_VAL or suspended > end - start:
        return "invalid_bounds"
    if elapsed == end - start:
        return "includes_suspended" if suspended else "no_suspension"
    if suspended and elapsed == end - start - suspended:
        return "appears_already_net_of_suspension"
    return "inconsistent"


def _unavailable(availability, reason, **evidence):
    return {"availability": availability, "reason": reason, "utilization": None, **evidence}


def analyze_job_cpu(job: dict) -> dict:
    """Apply the vetted offline CPU rules to a complete job and its recorded steps."""
    start, end, elapsed, suspended = (_field(job, "time", key)
                                      for key in ("start", "end", "elapsed", "suspended"))
    shape = _elapsed_shape(start, end, elapsed, suspended)
    evidence = {"formula_version": FORMULA_VERSION, "elapsed_shape": shape}
    if shape == "never_started":
        return _unavailable("special_value", "never_started", **evidence)
    if _field(job, "state", "current") != ["COMPLETED"]:
        return _unavailable("not_applicable", "outside_v1_completed_scope", **evidence)

    restarts = job.get("restart_cnt")
    if not _integer(restarts):
        return _unavailable("missing", "restart_count_unavailable", **evidence)
    flags = job.get("flags")
    if not isinstance(flags, list) or not all(isinstance(flag, str) for flag in flags):
        return _unavailable("missing", "job_flags_unavailable", **evidence)
    if restarts or {"RESIZING", "RESIZED", "REQUEUED"}.intersection(flags):
        return _unavailable("pending_definition", "restart_or_resize", **evidence)
    if _integer(_field(job, "het", "job_id"), 1):
        return _unavailable("not_applicable", "heterogeneous_job", **evidence)
    if shape == "missing_or_invalid":
        return _unavailable("missing", "job_time_fields_unavailable", **evidence)
    if shape in {"invalid_bounds", "inconsistent"}:
        return _unavailable("inconsistent", "job_time_contract_mismatch", **evidence)

    # The reported elapsed value can already exclude suspension.
    active = end - start - suspended
    if active <= 0:
        return _unavailable("inconsistent", "nonpositive_active_seconds", **evidence)
    cpus = allocated_cpu(job)
    if cpus is None:
        return _unavailable("missing", "allocated_cpu_unavailable", **evidence)
    capacity = cpus * active
    evidence.update(allocated_cpu=cpus, active_seconds=active, capacity_cpu_seconds=capacity)

    steps = job.get("steps")
    if not isinstance(steps, list) or not steps:
        return _unavailable("missing", "steps_unavailable", **evidence)
    step_ids = [_field(step, "step", "id") for step in steps]
    if any(not isinstance(step_id, str) or "." not in step_id for step_id in step_ids):
        return _unavailable("inconsistent", "step_id_unrecognized", **evidence)
    if len(step_ids) != len(set(step_ids)):
        return _unavailable("inconsistent", "duplicate_step_id", **evidence)
    included = {step_id: step for step_id, step in zip(step_ids, steps)
                if step_id.rsplit(".", 1)[1] != "extern"}
    if not included:
        return _unavailable("missing", "no_user_steps", **evidence)
    if any(_field(step, "state") != ["COMPLETED"] for step in included.values()):
        return _unavailable("inconsistent", "included_step_not_completed", **evidence)
    values = {step_id: _step_cpu_us(step) for step_id, step in included.items()}
    if None in values.values():
        return _unavailable("missing", "step_cpu_time_unavailable", **evidence)

    used = sum(values.values())
    zero_step_ids = [step_id for step_id, value in values.items() if value == 0]
    if zero_step_ids:
        conflicts = [step_id for step_id in zero_step_ids
                     if _cpu_count(_field(included[step_id], "tres", "requested", "total")) is not None]
        if conflicts:
            return _unavailable("inconsistent", "zero_total_cpu_with_positive_tres",
                                zero_step_ids=zero_step_ids, positive_cpu_tres_step_ids=conflicts, **evidence)
        reason = "all_user_step_cpu_zero" if used == 0 else "some_user_step_cpu_zero"
        return _unavailable("observed_zero", reason, zero_step_ids=zero_step_ids, **evidence)

    capacity_us = capacity * MICROSECONDS
    if used > capacity_us:
        return _unavailable("inconsistent", "cpu_exceeds_capacity", used_cpu_microseconds=used,
                            excess_cpu_microseconds=used - capacity_us, **evidence)
    return {
        "availability": "valid",
        "reason": None,
        "used_cpu_microseconds": used,
        "utilization": round(used / capacity_us, 8),
        "unused_capacity_cpu_microseconds": capacity_us - used,
        "included_step_ids": list(included),
        **evidence,
    }
