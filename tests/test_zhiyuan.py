from io import BytesIO
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from agent.runtime import Agent, Tool, ToolEvidence, ToolSpec, ToolSuccess
from agent.zhiyuan import ZhiyuanError, ZhiyuanModel


def response(message):
    return BytesIO(json.dumps({"choices": [{"message": message}]}).encode("utf-8"))


class ZhiyuanModelTest(unittest.TestCase):
    @patch("agent.zhiyuan.urlopen")
    def test_question_without_tools_uses_sjtu_endpoint(self, urlopen):
        urlopen.return_value = response({"content": "[needs_data]\n作业信息待查询。", "tool_calls": None})

        turn = Agent(ZhiyuanModel("synthetic-key")).run("查一下作业 123")

        request = urlopen.call_args.args[0]
        payload = json.loads(request.data)
        self.assertEqual(request.full_url, "https://models.sjtu.edu.cn/api/v1/chat/completions")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer synthetic-key")
        self.assertEqual(payload["model"], "deepseek-chat")
        self.assertEqual(payload["messages"][-1], {"role": "user", "content": "查一下作业 123"})
        self.assertNotIn("tools", payload)
        self.assertNotIn("synthetic-key", request.data.decode("utf-8"))
        self.assertEqual(turn.reply.status, "needs_data")
        self.assertEqual(turn.reply.text, "作业信息待查询。")

    @patch("agent.zhiyuan.urlopen")
    def test_tool_protocol_round_trip(self, urlopen):
        urlopen.side_effect = [
            response({"tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "job_evidence", "arguments": '{"job_id":"synthetic-1"}'},
            }]}),
            response({"content": "[answered]\n已核对作业分配的核数。"}),
        ]

        def job_evidence(arguments):
            return ToolSuccess(
                {"allocated_cpus": 64},
                ToolEvidence("synthetic", arguments, "2026-11-22T00:00:00Z"),
            )

        tool = Tool(ToolSpec("job_evidence", "返回单条作业证据", {"type": "object"}), job_evidence)
        turn = Agent(ZhiyuanModel("synthetic-key", "qwen"), (tool,)).run("核对作业的分配核数")

        first, second = (json.loads(call.args[0].data) for call in urlopen.call_args_list)
        self.assertEqual(first["model"], "qwen")
        self.assertEqual(first["tools"][0]["function"]["name"], "job_evidence")
        self.assertEqual(second["messages"][-2]["tool_calls"][0]["id"], "call-1")
        result = second["messages"][-1]
        self.assertEqual(result["role"], "tool")
        self.assertEqual(result["tool_call_id"], "call-1")
        self.assertEqual(json.loads(result["content"])["data"], {"allocated_cpus": 64})
        self.assertEqual(turn.reply.status, "answered")

    @patch("agent.zhiyuan.urlopen")
    def test_http_error_message_excludes_response_text(self, urlopen):
        urlopen.side_effect = HTTPError(
            "https://models.sjtu.edu.cn/api/v1/chat/completions",
            401,
            "synthetic-server-message",
            {},
            None,
        )

        with self.assertRaises(ZhiyuanError) as raised:
            Agent(ZhiyuanModel("synthetic-key")).run("查作业")

        self.assertIn("HTTP 401", str(raised.exception))
        self.assertNotIn("synthetic-server-message", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
