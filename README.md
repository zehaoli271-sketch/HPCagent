# HPC Guardian

上海交通大学超算低效作业识别项目：读取授权的 Slurm 数据，检查数据质量，计算资源指标，协助调查异常作业并提出可验证的建议。

项目处于原型阶段。泽浩负责可视化，队友负责智能体接入；技术栈及联调接口由团队共同决定。

## 目录

| 目录 | 用途 |
| --- | --- |
| `frontend/` | 可视化界面 |
| `agent/` | 智能体与工具调用 |
| `core/` | 共享数据模型和确定性计算 |
| `tests/` | 测试与核对案例 |
| `docs/` | 数据口径和接口约定 |
| `experiments/` | 标明限制的实验原型 |

目录仅为预留，不代表功能已经实现。联调前请共同完善 `docs/接口约定.md`。

## 协作

从最新 `main` 创建功能分支，例如 `feature/visualization`、`feature/agent-integration`，完成小功能后提交、推送并发起 Pull Request，请队友检查。提交前用 `git status` 和 `git diff --cached` 检查内容。

不要提交密码、令牌、原始作业数据、个人配置或未获授权共享的结果；脱敏不自动代表允许公开。

## 原型

`experiments/step_cpu_efficiency.py` 是早期步骤级试算脚本，不是正式的作业效率计算器：它尚未扣除挂起时间，也没有实现作业级步骤汇总，不能用于正式诊断。运行需要 Python 和 `requests`，使用交互输入或 `HPC_USERNAME`、`HPC_PASSWORD` 环境变量提供凭据，不要将凭据写进文件。

```powershell
python -m pip install requests
python experiments/step_cpu_efficiency.py
```

脚本默认使用 `http://127.0.0.1:7900`，可通过 `--proxy` 覆盖；输出 `reports/` 已被忽略。接口仅用于授权的只读查询。
