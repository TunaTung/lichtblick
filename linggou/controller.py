"""RC2 integrated remote: independent command stream and real wheel diagnostics."""
import argparse
import asyncio
import contextlib
import json
import socket
import time
from http import HTTPStatus
from pathlib import Path
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from business_protocol import allowed_interaction

ROOT = Path(__file__).resolve().parent
ACTIONS = {"stop": "STOP", "forward": "FWD", "backward": "BACK",
           "strafe_left": "LEFT", "strafe_right": "RIGHT", "spin_left": "CCW", "spin_right": "CW"}
PERIOD = 0.050
UI_LEASE = 0.250


class Controller:
    def __init__(self):
        self.writer = self.owner = None
        self.armed = False
        self.nonce = None
        self.generation = 0
        self.action = self.reported = "STOP"
        self.ui_tick = self.report_tick = self.hello_tick = 0.0
        self.frames = self.timeout_count = self.ui_timeouts = 0
        self.max_gap_ms = self.max_ui_gap_ms = 0.0
        self.send_lock = asyncio.Lock()
        self.wifi = None
        self.wheels = [None] * 4
        self.command_age_ms = self.command_rx = self.max_command_gap_ms = 0
        self.stop_reason = 0
        self.speed = self.reported_speed = "normal"
        self.speed_supported = False
        self.interaction = {}
        self.telemetry_callback = None

    def online(self):
        return self.writer is not None and not self.writer.is_closing() and self.armed

    def snapshot(self):
        age = (time.monotonic() - self.report_tick) * 1000 if self.report_tick else None
        return {"generation": self.generation, "online": self.online(), "connected": self.writer is not None,
                "fresh": age is not None and age < 300, "desired": self.action, "reported": self.reported,
                "report_age_ms": round(age) if age is not None else None, "frames": self.frames,
                "max_gap_ms": round(self.max_gap_ms, 1), "mcu_timeouts": self.timeout_count,
                "ui_timeouts": self.ui_timeouts, "max_ui_gap_ms": round(self.max_ui_gap_ms, 1), "wifi": self.wifi,
                "command_age_ms": self.command_age_ms, "command_rx": self.command_rx,
                "max_command_gap_ms": self.max_command_gap_ms, "stop_reason": self.stop_reason,
                "selected_speed": self.speed, "reported_speed": self.reported_speed,
                "speed_supported": self.speed_supported,
                "interaction": {m: dict(data=v[0], age_ms=round((time.monotonic()-v[1])*1000))
                                for m, v in self.interaction.items()},
                "wheels": [dict(w, age_ms=round((time.monotonic()-w["received"])*1000)) if w else None
                           for w in self.wheels]}

    def reset(self):
        self.armed = False
        self.nonce = None
        self.action = self.reported = "STOP"
        self.ui_tick = self.hello_tick = 0.0
        self.speed = self.reported_speed = "normal"
        self.speed_supported = False
        self.generation += 1

    def motion_frame(self):
        suffix = ",FAST" if self.speed == "fast" else ""
        return f"$RC2,START,{self.action}{suffix}"

    async def write(self, line):
        async with self.send_lock:
            writer = self.writer
            if writer is None or writer.is_closing():
                return
            # Never build a FIFO of old movement states behind a slow TCP link.
            if writer.transport.get_write_buffer_size() > 128:
                writer.close()
                return
            try:
                writer.write((line + "\n").encode("ascii"))
                await asyncio.wait_for(writer.drain(), 0.030)
            except (OSError, asyncio.TimeoutError):
                writer.close()

    async def stop(self):
        self.action = "STOP"
        self.ui_tick = 0.0
        await self.write("$RC2,STOP")

    async def car(self, reader, writer):
        if self.writer is not None:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            return
        self.writer = writer
        self.reset()
        self.frames = self.timeout_count = 0
        self.max_gap_ms = self.report_tick = 0.0
        self.wifi = None
        self.wheels = [None] * 4
        self.interaction = {}
        sock = writer.get_extra_info("socket")
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        print("RC2 TCP connected:", writer.get_extra_info("peername"), flush=True)
        try:
            await self.stop()
            self.hello_tick = time.monotonic()
            await self.write("$RC2,HELLO")
            while True:
                raw = await asyncio.wait_for(reader.readline(), 2.0)
                if not raw:
                    break
                if raw.startswith(b"{"):
                    try:
                        frame = json.loads(raw)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if isinstance(frame, dict) and frame.get("m") in {
                            "info", "st", "bat", "uw", "fw", "ch", "la", "vi", "imu", "pose",
                            "wg", "dg", "mode", "voice", "cart", "us", "echo", "pong"}:
                        self.interaction[frame["m"]] = (frame, time.monotonic())
                        if self.telemetry_callback:
                            self.telemetry_callback(frame)
                    continue
                try:
                    parts = raw.decode("ascii").strip().split(",")
                except UnicodeDecodeError:
                    continue
                if len(parts) < 3 or parts[0] != "$RC2":
                    continue
                if len(parts) in (9, 10) and parts[1] == "WIFI":
                    try:
                        values = list(map(int, parts[2:]))
                    except ValueError:
                        continue
                    if all(0 <= v <= 0xFFFFFFFF for v in values) and values[0] <= 1 and values[1] <= 1:
                        self.wifi = dict(zip(("sleep_disabled", "nodelay_enabled", "drops", "rx_errors",
                                              "rx_overflows", "silence_resets", "send_errors", "telemetry_drops"), values))
                    continue
                if len(parts) == 6 and parts[1] == "WHEEL":
                    try:
                        index, target, actual, pwm = map(int, parts[2:])
                    except ValueError:
                        continue
                    if 0 <= index < 4 and abs(target) <= 1700 and abs(actual) <= 100000 and abs(pwm) <= 11999:
                        self.wheels[index] = {"target_rpm": target/10, "actual_rpm": actual/10,
                                              "pwm": pwm, "received": time.monotonic()}
                    continue
                if len(parts) == 3 and parts[1] in ("READY", "ARMED", "LOCKED"):
                    if not parts[2].isascii() or not parts[2].isdigit():
                        continue
                    nonce = int(parts[2])
                    if not 0 <= nonce <= 0xFFFFFFFF:
                        continue
                    if parts[1] == "READY" and nonce:
                        self.reset()
                        self.nonce = nonce
                        self.hello_tick = time.monotonic()
                        await self.write(f"$RC2,ARM,{nonce}")
                    elif parts[1] == "ARMED" and self.nonce == nonce and nonce:
                        self.armed = True
                        self.action = "STOP"
                        self.generation += 1
                    elif parts[1] == "LOCKED":
                        self.reset()
                    continue
                if len(parts) not in (6, 10, 11) or parts[1] != "STATE" or parts[4] not in ACTIONS.values():
                    continue
                if len(parts) == 11 and parts[10] not in ("NORMAL", "FAST"):
                    continue
                try:
                    nonce, armed, timeouts = int(parts[2]), int(parts[3]), int(parts[5])
                except ValueError:
                    continue
                if not 0 <= nonce <= 0xFFFFFFFF or armed not in (0, 1) or not 0 <= timeouts <= 0xFFFFFFFF:
                    continue
                if len(parts) >= 10:
                    try:
                        age, reason, count, gap = map(int, parts[6:10])
                    except ValueError:
                        continue
                    if not all(0 <= v <= 0xFFFFFFFF for v in (age, count, gap)) or not 0 <= reason <= 6:
                        continue
                    self.command_age_ms, self.stop_reason = age, reason
                    self.command_rx, self.max_command_gap_ms = count, gap
                self.speed_supported = len(parts) == 11
                self.reported_speed = parts[10].lower() if self.speed_supported else "normal"
                now = time.monotonic()
                if self.report_tick:
                    self.max_gap_ms = max(self.max_gap_ms, (now - self.report_tick) * 1000)
                self.report_tick = now
                self.frames += 1
                self.reported, self.timeout_count = parts[4], timeouts
                if self.armed and (not armed or nonce != self.nonce):
                    self.reset()
        except (OSError, ValueError, asyncio.TimeoutError):
            pass
        finally:
            self.reset()
            self.writer = None
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            print("RC2 TCP disconnected; require a new press.", flush=True)

    async def browser(self, ws):
        if self.owner is not None:
            await ws.close(code=1008, reason="Another control page is open")
            return
        self.owner = ws
        self.speed = "normal"
        self.generation += 1
        await self.stop()
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    await self.stop()
                    continue
                if not isinstance(msg, dict) or not isinstance(msg.get("cmd"), str):
                    await self.stop()
                    continue
                if msg["cmd"] == "set_speed":
                    await self.stop()
                    if (msg.get("speed") in ("normal", "fast") and self.speed_supported and
                            msg.get("generation") == self.generation and self.online()):
                        self.speed = msg["speed"]
                        self.generation += 1  # Old queued holds cannot restart after a gear change.
                    continue
                if msg["cmd"] not in ACTIONS:
                    await self.stop()
                    continue
                action = ACTIONS[msg["cmd"]]
                if action == "STOP":
                    await self.stop()
                elif msg.get("generation") == self.generation and self.online():
                    now = time.monotonic()
                    if self.ui_tick:
                        self.max_ui_gap_ms = max(self.max_ui_gap_ms, (now - self.ui_tick) * 1000)
                    self.ui_tick = now
                    changed = self.action != action
                    self.action = action
                    if changed:
                        await self.write(self.motion_frame())
                else:
                    await self.stop()
        except ConnectionClosed:
            pass
        finally:
            await self.stop()
            self.speed = "normal"
            self.generation += 1
            self.owner = None

    async def control_loop(self):
        while True:
            if self.online():
                if self.action != "STOP" and (not self.owner or time.monotonic() - self.ui_tick >= UI_LEASE):
                    self.ui_timeouts += 1
                    await self.stop()
                # Control output runs independently of car reports and browser status publishing.
                await self.write("$RC2,STOP" if self.action == "STOP" else self.motion_frame())
            elif self.writer is not None and time.monotonic() - self.hello_tick >= 1.2:
                self.hello_tick = time.monotonic()
                await self.write("$RC2,HELLO")
            await asyncio.sleep(PERIOD)

    async def publish_loop(self):
        while True:
            await asyncio.sleep(0.100)
            if self.owner:
                try:
                    await asyncio.wait_for(self.owner.send(json.dumps(self.snapshot())), 0.050)
                except (ConnectionClosed, asyncio.TimeoutError):
                    await self.stop()

    async def send_interaction(self, cmd):
        if not allowed_interaction(cmd) or self.writer is None:
            return
        if cmd in {"$q", "$q-", "$w", "$w-"}:
            await self.stop()
            self.generation += 1
        await self.write(cmd)


def http_request(connection, request, controller):
    if request.path == "/ws" and request.headers.get("Upgrade", "").lower() == "websocket":
        return None
    if request.path == "/status":
        response = connection.respond(HTTPStatus.OK, json.dumps(controller.snapshot()))
        response.headers["Content-Type"] = "application/json"
    else:
        paths = {"/": ("index.html", "text/html; charset=utf-8"), "/control.js": ("control.js", "text/javascript; charset=utf-8")}
        if request.path not in paths:
            return connection.respond(HTTPStatus.NOT_FOUND, "Not found\n")
        name, mime = paths[request.path]
        response = connection.respond(HTTPStatus.OK, (ROOT / "web" / name).read_text(encoding="utf-8"))
        response.headers["Content-Type"] = mime
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Linggou-Remote"] = "rc2"
    return response


async def run(args):
    c = Controller()
    adapter = None
    if getattr(args, "lightbrick", False):
        from lightbrick_adapter import LightbrickAdapter
        adapter = LightbrickAdapter(c, args.ws_port)
    server = await asyncio.start_server(c.car, "0.0.0.0", args.car_port, limit=1024)
    async with server, serve(c.browser, "127.0.0.1", args.web_port,
                            process_request=lambda a, b: http_request(a, b, c),
                            compression=None, ping_interval=None, max_size=256, max_queue=4,
                            origins=[f"http://127.0.0.1:{args.web_port}", f"http://localhost:{args.web_port}"]):
        print(f"RC2 staging: http://127.0.0.1:{args.web_port}/; car TCP {args.car_port}", flush=True)
        try:
            tasks = [c.control_loop(), c.publish_loop()]
            if adapter:
                tasks.append(adapter.run())
            await asyncio.gather(*tasks)
        finally:
            await c.stop()
            if adapter:
                adapter.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--web-port", type=int, default=8088)
    p.add_argument("--car-port", type=int, default=8090)
    p.add_argument("--lightbrick", action="store_true", help="Publish telemetry through the existing Foxglove SDK")
    p.add_argument("--ws-port", type=int, default=8765)
    try:
        asyncio.run(run(p.parse_args()))
    except KeyboardInterrupt:
        pass
