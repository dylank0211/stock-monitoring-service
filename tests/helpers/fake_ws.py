import asyncio
import json
from contextlib import asynccontextmanager

class FakeWS:
    """
    Minimal websocket stub compatible with `websockets.client.connect` context manager.
    Script behavior by pushing JSON-serializable messages into `inbound`.
    Captures outbound `send()` payloads in `outbound`.
    """
    def __init__(self, scripted=None, delay=0.0):
        self.inbound = asyncio.Queue()
        self.outbound = []
        self._closed = False
        self.delay = delay
        self._ctx_entered = False
        if scripted:
            # preload inbound messages
            for m in scripted:
                self.inbound.put_nowait(json.dumps(m))

    async def __aenter__(self):
        self._ctx_entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._closed = True

    async def send(self, data: str):
        # record what client sends
        self.outbound.append(json.loads(data))

    async def close(self):
        self._closed = True
        try:
            # push a sentinel to unblock any awaiting recv()
            await self.inbound.put(json.dumps({"__closed__": True}))
        except Exception:
            pass

    async def recv(self) -> str:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self._closed and self.inbound.empty():
            # if closed and nothing queued, behave like a closed socket
            raise asyncio.CancelledError()
        data = await self.inbound.get()
        # if we got the sentinel after close, raise to break stream loop
        try:
            obj = json.loads(data)
            if isinstance(obj, dict) and obj.get("__closed__"):
                raise asyncio.CancelledError()
        except Exception:
            pass
        return data
