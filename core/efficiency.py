"""Adapt the shared CPU calculation to the heatmap's response fields."""

from .job_cpu import MICROSECONDS, allocated_cpu as allocated_cpu, analyze_job_cpu


REASON_MESSAGES = {
    "never_started": "作业尚未开始",
    "outside_v1_completed_scope": "作业尚未正常完成",
    "restart_count_unavailable": "缺少作业重启次数，资源分配待核实",
    "job_flags_unavailable": "缺少作业标记，资源分配待核实",
    "restart_or_resize": "作业曾重启或调整分配，需要核实资源容量",
    "heterogeneous_job": "异构作业需合并各组件的记账数据",
    "job_time_fields_unavailable": "缺少作业运行时间字段",
    "job_time_contract_mismatch": "作业起止时间、运行时长或挂起时间不一致",
    "nonpositive_active_seconds": "没有可用的有效运行时间",
    "allocated_cpu_unavailable": "缺少可信的实际分配 CPU 数",
    "steps_unavailable": "缺少步骤统计",
    "step_id_unrecognized": "步骤 ID 无法识别，统计待核实",
    "duplicate_step_id": "存在重复的步骤 ID，统计待核实",
    "no_user_steps": "没有可用于统计的作业步骤",
    "included_step_not_completed": "步骤未正常完成，统计完整性待核实",
    "step_cpu_time_unavailable": "步骤 CPU 时间缺失或无效",
    "all_user_step_cpu_zero": "步骤 CPU 时间均报告为零，统计完整性待核实",
    "some_user_step_cpu_zero": "部分步骤 CPU 时间报告为零，统计完整性待核实",
    "zero_total_cpu_with_positive_tres": "步骤 TotalCPU 和 CPU TRES 记账冲突，需要核实",
    "cpu_exceeds_capacity": "CPU 用量超过分配容量，需要核实",
}


def calculate_job_efficiency(job: dict) -> dict:
    metric = analyze_job_cpu(job)
    if metric["availability"] != "valid":
        return {
            "efficiency": None,
            "cpu_seconds": None,
            "reason": REASON_MESSAGES.get(metric["reason"], "作业记账数据待核实"),
        }
    return {
        "efficiency": metric["utilization"],
        "cpu_seconds": round(metric["used_cpu_microseconds"] / MICROSECONDS, 3),
        "reason": None,
    }
