"""Run the review agent with Zhiyuan-1 or an offline demonstration model."""

import argparse
import os

from core.slurm_jobs import HPC_TOKEN_ENV

from .runtime import Agent, ModelContext, Reply
from .slurm_tools import job_cpu_tool, slurm_jobs_tool
from .zhiyuan import DEFAULT_MODEL, ZhiyuanError, ZhiyuanModel


class DemoModel:
    def next_action(self, context: ModelContext) -> Reply:
        return Reply("needs_data", "智能体框架已运行。离线演示模式下，具体作业数据待查询。")


def main() -> None:
    parser = argparse.ArgumentParser(description="运行 HPC Guardian 智能体框架")
    parser.add_argument("--demo", action="store_true", help="使用离线演示模型")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="致远一号模型 ID")
    parser.add_argument("question", help="要复核的作业问题")
    args = parser.parse_args()
    if not args.question.strip():
        parser.error("请输入作业复核问题")

    tools = ()
    if args.demo:
        model = DemoModel()
    else:
        api_key = os.environ.get("SJTU_MODEL_API_KEY", "").strip()
        if not api_key:
            parser.error("请在环境变量 SJTU_MODEL_API_KEY 中设置致远一号模型密钥，或使用 --demo")
        model = ZhiyuanModel(api_key, args.model)
        hpc_token = os.environ.get(HPC_TOKEN_ENV, "").strip()
        if hpc_token:
            tools = (slurm_jobs_tool(hpc_token), job_cpu_tool(hpc_token))

    try:
        turn = Agent(model, tools).run(args.question)
    except ZhiyuanError as error:
        parser.exit(1, f"{error}\n")
    print(turn.reply.text)


if __name__ == "__main__":
    main()
