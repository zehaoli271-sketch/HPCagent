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

智能体框架位于 `agent/`。联调前请共同完善 [接口约定](docs/接口约定.md)。

## 运行智能体框架

### 启动作业对话

在仓库根目录运行下列命令，然后打开浏览器访问 `http://127.0.0.1:8765/`。演示模式使用合成作业和合成监控序列，无需凭据或第三方依赖。

```powershell
python -m frontend.server --demo
```

使用顶部的「展示」和「对话」切换热图与作业诊断。也可以从展示页输入「查询昨天所有已完成的作业」并发送。接着可以问「其中 CPU 利用率低于 10% 的有多少」「按运行时间排序」和「详细分析第一条」，或直接点击结果中的作业 ID 查看详情。切换页面会保留当前对话。输入「新查询」可清空后端的筛选条件。演示数据按服务当天生成，使用作业 ID 20011 可以试查详情。

看板的「正在运行」和「最近完成」都支持切换 CPU 与内存效率、选择排序字段和方向。点开方格可查看分配和申请的 CPU 核数。页面默认自动刷新，关闭后可通过「刷新数据」读取最新快照。

以下命令启动正式看板，默认连接已授权的 Slurm 和 Prometheus。

```powershell
python -m frontend.server
```

需要核对已有 SQLite 归档时，用 `--archive` 启用只读查询。

```powershell
python -m frontend.server --archive "D:/path/to/jobs.sqlite"
```

如需使用授权的远程作业数据，在网页右上角点击「连接 SJTU HPC」，输入有读取权限的访问令牌。也可在启动前设置环境变量 `SJTU_HPC_API_TOKEN`。后台默认每 30 秒刷新运行作业目录，正式服务在每轮监控采集完成后通知首页更新，热图和对话共用内存快照。监控查询默认限制为本地服务总计 30 次/分钟，可用 `--collect-interval` 和 `--prometheus-rpm` 配置目录间隔和监控预算。首页展示该轮实际耗时、节点覆盖和数据缺口。改用新版代码时需要重新启动网页服务，网页输入的令牌保存在该服务进程的内存中，重启后需重新连接。

可以问「查询当前 CPU 利用率低于 30% 的作业」或「查询当前作业 ID 为 10006 的 CPU 和内存」。实时 CPU 效率至少需要一分钟稳定分配及完整节点样本。历史时间范围继续按需读取 Slurm 记账。 [实时采集接口](docs/接口约定.md#实时采集与快照)

设置 `SJTU_MODEL_API_KEY` 和可选的 `SJTU_MODEL_ID` 可启用致远一号的意图补充解析和流式解读。对话会先给出工具报告，解读失败时仍可查看报告。请在后端环境中配置密钥。

给出一个历史时刻筛查低效作业时，默认按该时刻选出正在运行的作业，使用可靠的完成记账计算整段平均 CPU 利用率。要查该时刻附近的用量，可以在问题中写「时刻前 5 分钟」，或在网页选择「时刻前 5 分钟」。CPU 和内存候选共享默认阈值 20%，可用 `--cpu-threshold` 调整，也可在对话里指定百分比。

已完成作业默认用 seff 内存估算筛查，可在对话中查「内存申请过量的已完成作业」。详情列出 seff 估算、并发内存峰值和各步骤单任务 MaxRSS。部署方确认作业级并发峰值字段及 MB 换算后，可配置 `HPC_JOB_MEMORY_PEAK_FIELD` 和 `HPC_JOB_MEMORY_UNIT_BYTES` 以采用该口径筛查。 [对话接口、数据口径及监控配置](docs/接口约定.md#作业对话)

### 验证网页对话

```powershell
python -m unittest discover -s tests -v
node --check frontend/app.js
node --check frontend/diagnoses.js
```

`tests/test_chat.py` 覆盖指定的 15 个对话场景。另有只读 SQLite、跨日期、HPC 窗口合并与错误、对话隔离和模型 SSE 的集成测试。验收数据采用标明来源的合成样本，远程请求使用模拟响应。

单作业 CPU 计数差分已用真实接口核验。整轮发布和限流后续查有合成测试覆盖。真实环境的整轮采集耗时和全量覆盖仍待复测，运行中作业详情使用最近一次已发布的采集结果。

### 命令行智能体

在仓库根目录用 Python 3.10 或以上版本运行。默认使用致远一号模型服务的 `deepseek-chat`，可以用 `--model` 指定账号可用的模型 ID。模型密钥通过 `SJTU_MODEL_API_KEY` 在后端进程或本地终端配置。校外联调按学校文档使用 VPN。[致远一号 API 使用文档](https://apps.hpc.sjtu.edu.cn/posts/claw-guide-sjtu-api)

```powershell
$env:SJTU_MODEL_API_KEY = [System.Net.NetworkCredential]::new('', (Read-Host '致远一号模型密钥' -AsSecureString)).Password
python -m agent "复核一个作业的 CPU 效率需要哪些数据？"
```

`--demo` 使用离线演示模型，无需安装依赖或提供密钥。正常运行时，设置 `SJTU_HPC_API_TOKEN` 可启用两个只读工具。`query_slurm_jobs` 查询平台的 usage 窗口。`analyze_job_cpu` 按数字作业 ID 取详情并计算整作业 CPU 利用率。具体口径和验证状态见 [接口约定](docs/接口约定.md#slurm-作业查询)。

```powershell
python -m agent --demo "查一下这个作业为什么值得优先复核"
python -m unittest discover -s tests -v
```

入口是 `agent.runtime.Agent.run()`。致远一号适配器实现 `Model.next_action()`，查询能力可通过 `Tool` 注册。测试用合成数据验证调用流程，并模拟致远一号的 HTTP 请求与响应。

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
