# -*- coding: utf-8 -*-
"""Foxglove WS 协议探针 / 演示前一键自检: 冒充 Lichtblick 连上 bridge, 验证通道与消息流。

用法:
  venv/Scripts/python.exe probe.py [--port 8765] [--secs 8]

自检分级:
  核心通道(必须流): /pose /imu /follow /uwb /health /lidar_meta /chassis
  视觉通道(应流):   /scene /tf /rosout /power /us /stats /load
  事件通道(允许静默): /mode /vi /prox
健康断言: wifi=1, lp>0, power.low=0, /health.wage<3s(核心数据活), /health.alarm_state=0(无报警)
结论: 全过 → "★ 演示就绪"; 有缺 → 逐条列出
"""
import argparse
import asyncio
import json
import struct
import time

import websockets

CORE = {'/pose', '/imu', '/follow', '/uwb', '/health', '/lidar_meta', '/chassis'}
VISUAL = {'/scene', '/tf', '/power', '/us', '/stats'}
EVENT = {'/mode', '/vi', '/prox', '/load', '/rosout'}   # 事件帧: wg/识别/日志仅在事件时发


async def run(port, secs):
    stats = {}          # cid -> [topic, encoding, count, first_payload_desc]
    async with websockets.connect(
            f'ws://127.0.0.1:{port}',
            subprotocols=['foxglove.ws.protocol', 'foxglove.sdk.v1'],
            max_size=None) as ws:
        t_end = time.time() + secs
        while time.time() < t_end:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=max(0.1, t_end - time.time()))
            except (asyncio.TimeoutError, TimeoutError):
                break
            if isinstance(msg, str):
                data = json.loads(msg)
                op = data.get('op')
                if op == 'serverInfo':
                    print(f"serverInfo: {data.get('name')}")
                elif op == 'status':
                    print('status:', data.get('message'))
                elif op == 'advertise':
                    new_subs = []
                    for ch in data.get('channels', []):
                        cid = ch['id']
                        stats[cid] = [ch['topic'], ch.get('encoding', ''), 0, None]
                        new_subs.append({'id': cid, 'channelId': cid})
                    if new_subs:
                        await ws.send(json.dumps({'op': 'subscribe', 'subscriptions': new_subs}))
            else:
                op = msg[0]
                # 实测 SDK 服务端 messageData: [op=1][u32 订阅号][u64 logTime ns][payload]
                if op == 1:
                    sid = struct.unpack_from('<I', msg, 1)[0]
                    payload = msg[13:]
                    ent = stats.get(sid)
                    if ent:
                        ent[2] += 1
                        if ent[3] is None:
                            if ent[1] == 'json':
                                try:
                                    ent[3] = json.loads(payload.decode('utf-8', 'replace'))
                                except Exception:
                                    ent[3] = f'<decode失败 {len(payload)}B>'
                            else:
                                ent[3] = f'<{ent[1]} {len(payload)}B>'

    by_topic = {t: (n, first) for t, e, n, first in stats.values()}
    print('\n=== 演示前自检 ===')
    fail = []

    print('[核心通道]')
    for t in sorted(CORE):
        n, first = by_topic.get(t, (0, None))
        mark = 'OK' if n > 0 else '!!'
        print(f'  [{mark}] {t:<14} 收 {n:>4} 条')
        if n == 0:
            fail.append(f'核心通道 {t} 无数据 (车端未连? bridge 未起?)')
    print('[视觉通道]')
    for t in sorted(VISUAL):
        n, first = by_topic.get(t, (0, None))
        mark = 'OK' if n > 0 else '!!'
        print(f'  [{mark}] {t:<14} 收 {n:>4} 条')
        if n == 0 and t in ('/scene', '/tf'):
            fail.append(f'视觉通道 {t} 无数据 (3D 主视觉会缺内容)')
    print('[事件通道 - 允许静默]')
    for t in sorted(EVENT):
        n, _ = by_topic.get(t, (0, None))
        print(f'  [--] {t:<14} 收 {n:>4} 条')

    print('[健康断言]')
    h = by_topic.get('/health', (0, None))[1]
    if isinstance(h, dict):
        if h.get('wifi') != 1:
            fail.append(f"/health.wifi={h.get('wifi')} (车端 WiFi 断开)")
        if (h.get('lp') or 0) <= 0:
            fail.append(f"/health.lp={h.get('lp')} (主循环停转?)")
        w = h.get('wage')
        if w is not None and w >= 3:
            fail.append(f"/health.wage={w}s (核心数据断流/滞后)")
        if h.get('alarm_state') not in (0, None):
            fail.append(f"/health.alarm_state={h.get('alarm_state')} "
                        f"first={h.get('first')} (有报警待处理)")
        print(f"  wifi={h.get('wifi')} rc={h.get('rc')} dp={h.get('dp')} lp={h.get('lp')} "
              f"lpmax={h.get('lpmax')} wage={h.get('wage')}s alarm={h.get('alarm_state')}")
    else:
        print('  (无 /health 样本)')
    p = by_topic.get('/power', (0, None))[1]
    if isinstance(p, dict):
        if p.get('low'):
            fail.append(f"/power.low=1 (电池低电 {p.get('v')}V)")
        print(f"  电池 {p.get('v')}V low={p.get('low')}")

    if fail:
        print('\n!! 未就绪:')
        for f in fail:
            print('  -', f)
    else:
        print('\n★ 演示就绪: 全链路 PASS (核心通道在流, 3D 视觉链完整, 无健康异常)')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--port', type=int, default=8765)
    ap.add_argument('--secs', type=float, default=8.0)
    a = ap.parse_args()
    asyncio.run(run(a.port, a.secs))


if __name__ == '__main__':
    main()
