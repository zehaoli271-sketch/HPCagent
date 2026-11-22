"""作业级 CPU 效率计算。输入为 Slurm accounting 作业记录。"""
NO_VALUE = 4294967294


def allocated_cpu(job):
    values = [item.get("count") for item in job.get("tres", {}).get("allocated", []) if item.get("type") == "cpu"]
    return values[0] if len(values) == 1 else None


def calculate_job_efficiency(job):
    timing = job.get("time", {})
    cpus = allocated_cpu(job)
    elapsed = timing.get("elapsed")
    suspended = timing.get("suspended")
    steps = [step for step in job.get("steps", []) if str(step.get("step", {}).get("id", "")).rsplit(".", 1)[-1] != "extern"]
    reason = None
    cpu_seconds = 0.0
    if "COMPLETED" not in job.get("state", {}).get("current", []):
        reason = "作业尚未正常完成"
    elif not cpus or elapsed is None or suspended is None or elapsed <= suspended or not timing.get("start") or timing.get("start") == NO_VALUE:
        reason = "分配或运行时间不完整"
    elif job.get("restart_cnt", 0) or timing.get("suspended", 0):
        reason = "存在挂起或重启，资源分配需要核实"
    elif not steps:
        reason = "缺少步骤统计"
    for step in steps:
        if "COMPLETED" not in step.get("state", []):
            reason = "步骤未正常完成，统计完整性待核实"
        cpu = step.get("time", {}).get("total", {})
        if "seconds" not in cpu or "microseconds" not in cpu:
            reason = "步骤 CPU 时间缺失"
            continue
        seconds = cpu["seconds"] + cpu["microseconds"] / 1_000_000
        if seconds <= 0:
            reason = "步骤 CPU 时间为零，统计完整性待核实"
        cpu_seconds += seconds
    efficiency = None if reason else cpu_seconds / (cpus * (elapsed - suspended))
    if efficiency is not None and not 0 <= efficiency <= 1:
        efficiency, reason = None, "统计超出分配容量，需要核实"
    return {"efficiency": efficiency, "cpu_seconds": round(cpu_seconds, 3) if not reason else None, "reason": reason}
