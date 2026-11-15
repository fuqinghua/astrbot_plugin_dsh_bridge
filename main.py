"""把 AstrBot 的消息转发给本机 DSH 智能体（DSH Go）。

链路：平台消息 -> AstrBot -> 本插件 -> http://127.0.0.1:8787/chat
      -> dsh headless（带手机操控 MCP）-> 回复
"""
import asyncio
import json

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register


@register("astrbot_plugin_dsh_bridge", "dsh", "把消息转发给本机 DSH 智能体", "1.0.0")
class DshBridge(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config or {}
        self.base = str(self.config.get("bridge_url", "http://127.0.0.1:8787")).rstrip("/")
        self.token = str(self.config.get("token", "")).strip()
        self.mode = str(self.config.get("reply_mode", "at_or_private")).strip()
        self.prefix = str(self.config.get("prefix", "dsh "))
        self.timeout = int(self.config.get("timeout", 300) or 300)
        self.show_progress = bool(self.config.get("show_progress", True))
        self.reset_command = str(self.config.get("reset_command", "/dsh reset")).strip()
        self._session = None

    async def _client(self):
        if self._session is None or self._session.closed:
            # trust_env=False：不吃 HTTP_PROXY 等环境变量，否则本机 127.0.0.1 会被塞进代理
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                trust_env=False,
            )
        return self._session

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _should_handle(self, event: AstrMessageEvent, text: str) -> bool:
        if self.mode == "all":
            return True
        if self.mode == "prefix":
            return text.startswith(self.prefix)
        # at_or_private：私聊，或被 @ / 唤醒
        try:
            if event.is_private_chat():
                return True
        except Exception:
            pass
        return bool(getattr(event, "is_at_or_wake_command", False))

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        text = (event.message_str or "").strip()
        if not text:
            return
        if text == self.reset_command:
            await self._reset(event)
            return
        if not self._should_handle(event, text):
            return
        if self.mode == "prefix":
            text = text[len(self.prefix):].strip()
            if not text:
                return

        key = f"{event.get_platform_id()}:{event.get_session_id()}"
        if self.show_progress:
            yield event.plain_result("收到，正在让手机那边处理…")

        payload = {"session": key, "text": text, "timeoutMs": self.timeout * 1000}
        try:
            client = await self._client()
            async with client.post(f"{self.base}/chat", json=payload, headers=self._headers()) as resp:
                raw = await resp.text()
                try:
                    data = json.loads(raw)
                except Exception:
                    data = {"ok": False, "error": raw[:300]}
                if resp.status != 200 or not data.get("ok"):
                    err = data.get("error") or f"HTTP {resp.status}"
                    detail = data.get("detail") or ""
                    logger.warning(f"[dsh_bridge] failed: {err} {detail}")
                    yield event.plain_result(f"DSH 那边出错了：{err}"[:500])
                    return
                reply = (data.get("reply") or "").strip() or "（DSH 没有返回内容）"
                yield event.plain_result(reply)
        except asyncio.TimeoutError:
            yield event.plain_result("等太久了，这次先放弃。可以让我做个更小的动作再试。")
        except Exception as e:  # noqa: BLE001
            logger.error(f"[dsh_bridge] request error: {e}")
            yield event.plain_result(f"连不上 DSH 桥（{self.base}）：{type(e).__name__}")

    async def _reset(self, event: AstrMessageEvent):
        key = f"{event.get_platform_id()}:{event.get_session_id()}"
        try:
            client = await self._client()
            async with client.post(f"{self.base}/reset", json={"session": key}, headers=self._headers()) as resp:
                await resp.text()
            yield event.plain_result("这个对话的上下文已清空。")
        except Exception as e:  # noqa: BLE001
            yield event.plain_result(f"清空失败：{type(e).__name__}")

    async def terminate(self):
        if self._session and not self._session.closed:
            await self._session.close()
