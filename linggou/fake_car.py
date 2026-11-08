# -*- coding: utf-8 -*-
"""假车端: 模拟 ESP-01S 透传行为, 无车联调 bridge.py / 上位机。

行为与真固件一致(见《上位机交接文档.md》):
  - 连上后先发 info 帧
  - 按周期发 14 种上行帧 (uw/fw 200ms, ch/la/imu 500ms, pose/us 1s, bat 2s, st 5s, dg 10s)
  - 收到下行行 → 回 echo 帧 {"m":"echo","rx":"..."}
  - --reconnect-every N: 模拟电池供电 ESP 掉电重启(验证 dp/rc 健康计数)

动态演示模式(2026-09-05, 供 3D 运动反映验收/答辩彩排):
  - 车沿 8 字轨迹连续行驶(x/y/yaw/vx 按运动学一致变化, 上电原点出发)
  - IMU roll/pitch 低幅摆动(模拟推车颠簸)
  - us 每 24s 一轮避障事件: 10~13s 右侧近障(12cm), 17~19s 后方近障(9cm)
  - ch 四轮 RPM 按差速联动(右轮+ 左轮-), PWM 成比例
  - UWB 目标固定在世界系 (2.6, 1.6), 基站系坐标按车姿换算 → bridge 修正后
    红球应钉在世界系固定点不动(强验证坐标系换算)
  - la 36 点按"矩形房间墙"几何求交(车转/移动时点云呈现固定环境的轮廓)

用法:
  python fake_car.py                        # 连 127.0.0.1:8080, 持续发帧
  python fake_car.py --port 8080 --speed 2  # 2 倍速发帧
  python fake_car.py --cmd "$w"             # 开跑 3 秒后从下行通道注入一条 $w(经 send_cmd.txt)
"""
import argparse
import json
import math
import os
import random
import socket
import sys
import threading
import time

CMD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'send_cmd.txt')

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

# --- 车体/场景参数(与固件 motor.h 一致的量级) ---
WHEEL_CIRC = 0.213628   # 轮周长 m
WHEEL_R = 0.085         # 轮半径 m (size 0.17/2)
TRACK_HALF = 0.29       # 半轮距 m (差速换算)

# 模拟货架矩形 (cx, cy, w, h) m — 放在 8 字两环内部空洞与左右外侧, 雷达可扫到不挡路
_SHELVES = [
    (1.1, 1.1, 0.5, 0.4),      # 右环内部
    (-1.1, 1.1, 0.5, 0.4),     # 左环内部
    (2.75, 1.1, 0.4, 1.2),     # 右侧靠墙
    (-2.75, 1.1, 0.4, 1.2),    # 左侧靠墙
]


def _ray_rect(ox, oy, dx, dy, xmin, xmax, ymin, ymax):
    """射线-AABB 最近交距 (m); 起点在盒内返回出口距离, 无交返回 None"""
    tmin, tmax = 0.0, float('inf')
    for o, d, lo, hi in ((ox, dx, xmin, xmax), (oy, dy, ymin, ymax)):
        if abs(d) < 1e-9:
            if o < lo or o > hi:
                return None
        else:
            t1, t2 = (lo - o) / d, (hi - o) / d
            if t1 > t2:
                t1, t2 = t2, t1
            tmin, tmax = max(tmin, t1), min(tmax, t2)
            if tmin > tmax:
                return None
    if tmax < 1e-4:
        return None
    return tmin if tmin > 1e-4 else tmax


def frame(m, **kw):
    obj = {'m': m}
    obj.update(kw)
    return json.dumps(obj, separators=(',', ':'))


def _wrap_deg(d):
    """角度规约到 (-180, 180]"""
    return (d + 180.0) % 360.0 - 180.0


def _motion(t):
    """8 字轨迹运动学。返回 dict(x, y, yaw_deg, v, wz)"""
    T = 36.0                      # 8 字周期 s
    w = 2.0 * math.pi / T
    x = 2.2 * math.sin(w * t)
    y = 1.1 + 1.1 * math.sin(2.0 * w * t)        # 中心 (0, 1.1), y∈[0, 2.2]
    dx = 2.2 * w * math.cos(w * t)
    dy = 2.2 * w * math.cos(2.0 * w * t)
    v = math.hypot(dx, dy)                       # 车体前向速度 m/s
    yaw = math.degrees(math.atan2(dy, dx))       # 车头朝向(度)
    eps = 0.05
    yaw2 = math.degrees(math.atan2(2.2 * w * math.cos(2.0 * w * (t + eps)),
                                   2.2 * w * math.cos(w * (t + eps))))
    wz = math.radians(_wrap_deg(yaw2 - yaw)) / eps   # 角速度 rad/s(带回绕处理)
    return dict(x=x, y=y, yaw=yaw, v=v, wz=max(min(wz, 1.2), -1.2))


def _uwb_target(t):
    """UWB: 目标固定在世界系 (2.6, 1.6); 输出车载基站系坐标(x=右, y=前, m)"""
    m = _motion(t)
    yaw = math.radians(m['yaw'])
    rel_x, rel_y = 2.6 - m['x'], 1.6 - m['y']
    fwd = rel_x * math.cos(yaw) + rel_y * math.sin(yaw)      # 前向分量
    right = rel_x * math.sin(yaw) - rel_y * math.cos(yaw)    # 右向分量
    return fwd, right, m


def _us(t):
    """三路超声(右/左/后, cm): 每 24s 一轮避障事件"""
    m = t % 24.0
    if 10.0 <= m < 13.0:
        return 12, 150, 200      # 右近障
    if 17.0 <= m < 19.0:
        return 95, 150, 9        # 后近障
    return 95, 150, 200


def _lidar(t):
    """36 点模拟: 房间墙 x∈[-3,3] y∈[-1,3] + 三个货架矩形, 射线取最近交点;
    叠加角度抖动(±0.9°)、距离噪声(±2.5cm)、5% 丢点、2% 多径野值 —
    拟真但比真雷达干净(真车噪声/丢点更大更频繁), 演示观感好"""
    m = _motion(t)
    base = math.radians(m['yaw'])
    out = []
    for i in range(36):
        if random.random() < 0.05:
            out.append(-1)                        # 偶发丢点
            continue
        ang = base + math.radians(i * 10) + random.gauss(0, 0.015)
        cx, cy = math.cos(ang), math.sin(ang)
        best = _ray_rect(m['x'], m['y'], cx, cy, -3.0, 3.0, -1.0, 3.0) or 12.0
        for scx, scy, sw, sh in _SHELVES:
            d = _ray_rect(m['x'], m['y'], cx, cy,
                          scx - sw / 2, scx + sw / 2, scy - sh / 2, scy + sh / 2)
            if d and d < best:
                best = d
        if best >= 12.0:
            out.append(-1)                        # 打不到任何东西
            continue
        d = best + random.gauss(0, 0.025)
        if random.random() < 0.02:
            d += random.uniform(0.15, 0.4)        # 多径野值
        out.append(int(max(d, 0.02) * 100))       # m -> cm
    return out


def _ch(t):
    """底盘期望/反馈: 四轮 RPM 按差速联动(v 基础 + wz*50 差速), PWM 成比例"""
    m = _motion(t)
    base = m['v'] / 0.213628 * 60.0              # 轮 RPM = v/周长*60
    diff = m['wz'] * 50.5                        # 左右轮 RPM 差(半轮距/轮径换算近似)
    r = [int(base + diff), int(base - diff), int(base + diff), int(base - diff)]
    p = [int(x * 21) for x in r]                 # PWM ≈ RPM*21 (±11999 内)
    return dict(ex=[round(m['v'], 3), 0.0, round(m['wz'], 3)],
                fb=[round(m['v'] * 0.97, 3), 0.0, round(m['wz'] * 0.95, 3)], r=r, p=p)


def make_frames(t, fault=False):
    """按 t(秒) 生成全部帧(动态演示数据, 字段名与固件一致)。
    fault=True: 故障注入剧本(验收报警/续航/称重 UI) —
      t>=15s 雷达 err=3 → 首出报警"雷达报错"; t>=25s 电池 low=1 + 1.2V/min 放电
      (续航估计可视); t>=35s wg 放上 450g 物品(称重状态带可视)。"""
    m = _motion(t)
    fwd, right, _ = _uwb_target(t)
    c = _ch(t)
    d1, d2, d3 = _us(t)
    la_err = 3 if (fault and t >= 15.0) else 0
    bat_v = 11.85 - 0.4 * math.sin(2 * math.pi * t / 60)
    if fault:
        bat_v -= 0.02 * t               # 1.2 V/min 放电, ~40s 后续航估计开始出数
    bat_low = 1 if (fault and t >= 25.0) else 0
    frames = {
        'uw': frame('uw', d=round(math.hypot(fwd, right), 3),
                    a=round(math.degrees(math.atan2(right, fwd)), 1),
                    x=round(right, 3), y=round(fwd, 3),
                    ok=1, fc=int(t * 5), err=0, gl=0),
        'fw': frame('fw', st=3, tv=1, d=round(0.65 + 0.08 * math.sin(2 * math.pi * t / 13), 3),
                    a=round(math.degrees(math.atan2(right, fwd)), 1),
                    vx=round(m['v'], 3), vy=0.0, wz=round(m['wz'], 3)),
        'ch': frame('ch', **c),
        'la': frame('la', s=2, fc=int(t * 2), err=la_err, d=_lidar(t)),
        'imu': frame('imu', r=round(2.5 * math.sin(2 * math.pi * t / 5.0), 2),
                     p=round(1.8 * math.sin(2 * math.pi * t / 3.7), 2),
                     y=round(m['yaw'], 2), gx=0.1, gy=0.2,
                     gz=round(math.degrees(m['wz']), 2),
                     ax=0.01, ay=0.02, az=0.99, t=29.5),
        'pose': frame('pose', x=round(m['x'], 3), y=round(m['y'], 3),
                      yaw=round(m['yaw'], 2), td=round(0.4 * t, 2),
                      sl=round(0.02 + 0.05 * abs(math.sin(2 * math.pi * t / 17)), 3),
                      vx=round(m['v'], 3)),
        'us': frame('us', d1=d1, d2=d2, d3=d3),
        'bat': frame('bat', v=round(bat_v, 2), low=bat_low),
        'st': frame('st', wifi=1, up=int(t * 1000), rc=2, hb=int(t), dp=0, sm=6,
                    lp=998, lpmax=1420),
        'dg': frame('dg', c=[1, 0, 2, 1, 1, 0, 2, 1, 3, 0, 5, 0, 2, 1, 456, 12, 3, 1, 0, 8, 41, 0]),
    }
    if fault and t >= 35.0:
        kg = min(0.45, (t - 35.0) * 0.05)
        frames['wg'] = frame('wg', kg=round(kg, 3), raw=int(kg * 1000), en=1)
    return frames


_PERIODS = {'uw': 0.2, 'fw': 0.2, 'ch': 0.5, 'la': 0.5, 'imu': 0.5,
            'pose': 1.0, 'us': 1.0, 'bat': 2.0, 'st': 5.0, 'dg': 10.0}


class FakeCar:
    def __init__(self, host, port, speed=1.0, reconnect_every=0, fault=False):
        self.host, self.port = host, port
        self.speed = speed
        self.reconnect_every = reconnect_every
        self.fault = fault

    def run(self):
        n = 0
        while True:
            try:
                self.session(n)
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                print(f'[fake-car] 连接断开: {e}, 3s 后重连', flush=True)
            time.sleep(3)
            n += 1

    def session(self, session_no):
        sock = socket.create_connection((self.host, self.port), timeout=5)
        sock.settimeout(None)   # 5s 超时仅限建连; 否则 _reader 空闲 5s 抛 timeout 静默死亡
        print(f'[fake-car] 已连接 {self.host}:{self.port} (session {session_no})', flush=True)
        stop = threading.Event()
        threading.Thread(target=self._reader, args=(sock, stop), daemon=True).start()

        t0 = time.time()
        last_sent = {}
        periods = dict(_PERIODS)
        if self.fault:
            periods['wg'] = 0.5           # 故障剧本里 wg 才登场(0.5s 一帧)
        # info 帧: 每次连上(含重连)都发
        sock.sendall((frame('info', fw='901w1', up=int(time.time() * 1000) % 10_000_000,
                            rst=0x00000001) + '\n').encode())
        try:
            while True:
                t = (time.time() - t0) * self.speed
                if self.reconnect_every and t >= self.reconnect_every:
                    print('[fake-car] 模拟 ESP 掉电重启(主动断开)', flush=True)
                    return
                batch = []
                frames = make_frames(t, self.fault)
                for name, period in periods.items():
                    if t - last_sent.get(name, -1e9) >= period:
                        last_sent[name] = t
                        f = frames.get(name)
                        if f:                     # wg 等条件帧未到登场时间时缺席
                            batch.append(f)
                if batch:
                    sock.sendall(('\n'.join(batch) + '\n').encode())
                time.sleep(0.02 / self.speed)
        finally:
            stop.set()
            sock.close()

    def _reader(self, sock, stop):
        """收下行 → 回 echo 帧(模拟固件行为)"""
        buf = b''
        try:
            while not stop.is_set():
                data = sock.recv(1024)
                if not data:
                    break
                buf += data
                while b'\n' in buf:
                    line, buf = buf.split(b'\n', 1)
                    text = line.decode('utf-8', 'replace').strip()
                    if text:
                        print(f'[fake-car] 收到下行: {text!r} → 回 echo', flush=True)
                        sock.sendall((frame('echo', rx=text) + '\n').encode())
        except OSError:
            pass


def main():
    ap = argparse.ArgumentParser(description='假车端(模拟 ESP-01S 透传)')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=8080)
    ap.add_argument('--speed', type=float, default=1.0, help='发帧时间倍速')
    ap.add_argument('--reconnect-every', type=float, default=0, help='每 N 秒模拟掉电重连(0=不模拟)')
    ap.add_argument('--fault', action='store_true',
                    help='故障注入剧本: 15s 雷达报错/25s 低电+放电/35s 放物品(验收报警·续航·称重 UI)')
    ap.add_argument('--cmd', metavar='CMD', help='启动 3 秒后向 send_cmd.txt 注入一条下行指令(测回环)')
    args = ap.parse_args()

    if args.cmd:
        def inject():
            time.sleep(3)
            with open(CMD_FILE, 'w', encoding='utf-8') as f:
                f.write(args.cmd + '\n')
            print(f'[fake-car] 已注入下行测试: {args.cmd}', flush=True)
        threading.Thread(target=inject, daemon=True).start()

    FakeCar(args.host, args.port, args.speed, args.reconnect_every, args.fault).run()


if __name__ == '__main__':
    main()
