"""Reuse the project's mature Lichtblick publisher behind the RC2 TCP owner."""
import asyncio
import importlib.util
from pathlib import Path


class LightbrickAdapter:
    def __init__(self, controller, port):
        path = Path(__file__).parent / "dashboard_backend" / "bridge.py"
        spec = importlib.util.spec_from_file_location("linggo_dashboard", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.publisher = module.FoxglovePublisher(port)
        self.queue = asyncio.Queue(maxsize=32)
        self.loop = asyncio.get_running_loop()
        self.controller = controller
        self.publisher.set_downlink(self.command)
        # Add raw interaction channels while retaining the existing dashboard.
        from foxglove import Channel
        self.extra = {m: Channel("/"+m, message_encoding="json") for m in ("voice", "cart")}
        controller.telemetry_callback = self.offer

    def offer(self, frame):
        if self.queue.full():
            self.queue.get_nowait()
        self.queue.put_nowait(frame)

    def command(self, cmd):
        self.loop.call_soon_threadsafe(lambda: asyncio.create_task(self.controller.send_interaction(cmd)))

    async def run(self):
        while True:
            frame = await self.queue.get()
            await asyncio.to_thread(self.publish, frame)

    def publish(self, frame):
        m = frame.get("m")
        if m in self.extra:
            self.extra[m].log({k: v for k, v in frame.items() if k != "m"})
        else:
            self.publisher.on_frame(m, frame)

    def close(self):
        self.controller.telemetry_callback = None
        self.publisher.set_downlink(None)
        self.publisher.close()
