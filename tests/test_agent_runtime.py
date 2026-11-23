import unittest

from agent.runtime import Agent, Reply, Tool, ToolCall, ToolEvidence, ToolFailure, ToolSpec, ToolSuccess


class ScriptedModel:
    def __init__(self, *actions):
        self.actions = iter(actions)
        self.contexts = []

    def next_action(self, context):
        self.contexts.append(context)
        return next(self.actions)


class AgentTest(unittest.TestCase):
    def test_empty_registry_and_final_reply(self):
        reply = Reply("needs_data", "作业查询工具待接入")
        model = ScriptedModel(reply)

        turn = Agent(model).run("查一下作业 123")

        self.assertEqual(turn.reply, reply)
        self.assertEqual(turn.exchanges, ())
        self.assertEqual(model.contexts[0].tools, ())

    def test_tool_result_can_drive_a_second_call_and_supply_followup_context(self):
        candidate_call = ToolCall("call-1", "candidates", {"limit": 1})
        detail_call = ToolCall("call-2", "job_evidence", {"job_id": "synthetic-1"})
        conclusion = Reply("answered", "已找到一条候选作业及其复核证据")
        followup = Reply("needs_data", "缩核后的耗时需要对照数据")
        model = ScriptedModel(candidate_call, detail_call, conclusion, followup)
        candidate_result = ToolSuccess(
            {"job_ids": ["synthetic-1"]},
            ToolEvidence("synthetic", {"limit": 1}, "2026-11-22T00:00:00Z"),
        )
        detail_result = ToolSuccess(
            {"allocated_cpus": 64},
            ToolEvidence("synthetic", {"job_id": "synthetic-1"}, "2026-11-22T00:00:00Z"),
        )
        calls = []

        def candidates(arguments):
            calls.append(arguments)
            return candidate_result

        def job_evidence(arguments):
            calls.append(arguments)
            return detail_result

        agent = Agent(model, (
            Tool(ToolSpec("candidates", "返回候选作业", {"type": "object"}), candidates),
            Tool(ToolSpec("job_evidence", "返回单条作业证据", {"type": "object"}), job_evidence),
        ))
        turn = agent.run("查优先级最高的作业，并解释入选依据")
        next_turn = agent.run("这条作业能缩核吗？", history=(turn,))

        self.assertEqual(calls, [{"limit": 1}, {"job_id": "synthetic-1"}])
        self.assertEqual([spec.name for spec in model.contexts[0].tools], ["candidates", "job_evidence"])
        self.assertEqual(model.contexts[0].exchanges, ())
        self.assertEqual(model.contexts[1].exchanges[0].result, candidate_result)
        self.assertEqual(model.contexts[2].exchanges[1].result, detail_result)
        self.assertEqual(model.contexts[3].history, (turn,))
        self.assertEqual(turn.reply, conclusion)
        self.assertEqual(next_turn.reply, followup)

    def test_unregistered_tool_is_returned_as_a_failure(self):
        model = ScriptedModel(ToolCall("call-1", "unregistered", {}), Reply("needs_data", "无法查询"))

        turn = Agent(model).run("查作业")

        self.assertEqual(turn.exchanges[0].result.code, "tool_unavailable")
        self.assertEqual(model.contexts[1].exchanges, turn.exchanges)
        self.assertEqual(turn.reply.status, "needs_data")

    def test_tool_limit_stops_additional_dispatch(self):
        model = ScriptedModel(ToolCall("call-1", "jobs", {}), ToolCall("call-2", "jobs", {}))
        calls = []

        def jobs(arguments):
            calls.append(arguments)
            return ToolFailure("permission_denied", "当前账号没有该查询权限")

        agent = Agent(model, (Tool(ToolSpec("jobs", "查询作业", {}), jobs),), max_tool_calls=1)

        turn = agent.run("查作业")

        self.assertEqual(calls, [{}])
        self.assertEqual(turn.exchanges[0].result.code, "permission_denied")
        self.assertEqual(turn.reply.status, "tool_limit")

    def test_blank_question_is_rejected_before_calling_model(self):
        model = ScriptedModel()

        with self.assertRaises(ValueError):
            Agent(model).run("  ")
        self.assertEqual(model.contexts, [])


if __name__ == "__main__":
    unittest.main()
