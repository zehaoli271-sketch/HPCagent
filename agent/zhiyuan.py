"""Chat Completions adapter for SJTU's Zhiyuan-1 model service."""

from dataclasses import asdict
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .runtime import ModelContext, Reply, ToolCall, ToolExchange


CHAT_URL = "https://models.sjtu.edu.cn/api/v1/chat/completions"
DEFAULT_MODEL = "deepseek-chat"

REPLY_FORMAT = """每轮最多请求一个工具。没有可用作业工具时，说明当前可以解释分析方法，
具体作业的信息仍待查询。直接答复时，第一行只写一个状态标记：
[answered] 表示完成，[needs_clarification] 表示需澄清，[needs_data] 表示需要证据。
第二行起写面向用户的答复。
"""


class ZhiyuanError(RuntimeError):
    """A request or response error from the model service."""


def exchange_messages(exchange: ToolExchange) -> list[dict[str, object]]:
    call = exchange.call
    return [
        {"role": "assistant", "content": None, "tool_calls": [{
            "id": call.call_id,
            "type": "function",
            "function": {
                "name": call.name,
                "arguments": json.dumps(call.arguments, ensure_ascii=False),
            },
        }]},
        {
            "role": "tool",
            "tool_call_id": call.call_id,
            "content": json.dumps(asdict(exchange.result), ensure_ascii=False),
        },
    ]


class ZhiyuanModel:
    def __init__(self, api_key: str, model_id: str = DEFAULT_MODEL):
        self._api_key = api_key
        self.model_id = model_id

    def next_action(self, context: ModelContext) -> ToolCall | Reply:
        messages = [{"role": "system", "content": context.instructions + "\n" + REPLY_FORMAT}]
        for turn in context.history:
            messages.append({"role": "user", "content": turn.question})
            for exchange in turn.exchanges:
                messages.extend(exchange_messages(exchange))
            messages.append({"role": "assistant", "content": turn.reply.text})
        messages.append({"role": "user", "content": context.question})
        for exchange in context.exchanges:
            messages.extend(exchange_messages(exchange))

        payload = {"model": self.model_id, "messages": messages, "stream": False}
        if context.tools:
            payload["tools"] = [{"type": "function", "function": asdict(spec)} for spec in context.tools]
            payload["parallel_tool_calls"] = False

        try:
            with urlopen(self._request(payload), timeout=60) as response:
                message = json.load(response)["choices"][0]["message"]
            calls = message.get("tool_calls") or []
            if calls:
                call, = calls
                arguments = json.loads(call["function"]["arguments"])
                if not isinstance(arguments, dict):
                    raise ValueError("Tool arguments must be an object")
                return ToolCall(call["id"], call["function"]["name"], arguments)
            text = message["content"].strip()
            if not text:
                raise ValueError("Empty model reply")
        except HTTPError as error:
            raise ZhiyuanError(f"致远一号模型请求失败（HTTP {error.code}）。") from error
        except (URLError, TimeoutError) as error:
            raise ZhiyuanError("连接致远一号模型服务失败，请检查校园网或 VPN。") from error
        except (AttributeError, IndexError, KeyError, TypeError, ValueError) as error:
            raise ZhiyuanError("致远一号返回了无法解析的模型答复。") from error

        label, _, answer = text.partition("\n")
        if answer.strip():
            if label == "[answered]":
                return Reply("answered", answer.strip())
            if label == "[needs_clarification]":
                return Reply("needs_clarification", answer.strip())
            if label == "[needs_data]":
                return Reply("needs_data", answer.strip())
        return Reply("answered" if context.exchanges else "needs_data", text)

    def _request(self, payload):
        request = Request(CHAT_URL, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                          headers={"Content-Type": "application/json"}, method="POST")
        request.add_unredirected_header("Authorization", f"Bearer {self._api_key}")
        return request

    def stream_reply(self, evidence):
        messages = [
            {"role": "system", "content": "用中文简短解读这份工具报告。只使用给定事实，解释缺失证据。"
                                         "如果提出建议，请注明需要人工验证。不要输出内部推理。"},
            {"role": "user", "content": json.dumps(evidence, ensure_ascii=False)},
        ]
        payload = {"model": self.model_id, "messages": messages, "stream": True}
        try:
            with urlopen(self._request(payload), timeout=60) as response:
                for raw in response:
                    line = raw.decode("utf-8").strip()
                    if not line.startswith("data:"):
                        continue
                    body = line[5:].strip()
                    if body == "[DONE]":
                        return
                    event = json.loads(body)
                    if event.get("error"):
                        raise ZhiyuanError("致远一号解读中断，工具结果已保留。")
                    choices = event.get("choices") or []
                    if choices:
                        text = choices[0].get("delta", {}).get("content")
                        if isinstance(text, str) and text:
                            yield text
        except HTTPError as error:
            raise ZhiyuanError(f"致远一号解读失败（HTTP {error.code}），工具结果已保留。") from error
        except (OSError, AttributeError, KeyError, TypeError, ValueError) as error:
            raise ZhiyuanError("致远一号解读中断，工具结果已保留。") from error
