# -*- coding: utf-8 -*-
"""灵购购物车上位机桥（Lichtblick / Foxglove WebSocket 版）

数据流:
  车端(STM32+ESP-01S) --TCP 8080--> bridge.py --Foxglove WS--> Lichtblick 桌面端
  下行: send_cmd.txt 写一行 / 终端输入 --> bridge.py --> TCP 下发给车

运行模式:
  真车:   python bridge.py                          # TCP 监听 8080 等车连接
  重放:   python bridge.py --replay ../wifi_rebuild/server_log.txt [--speed 10]
  自测:   python bridge.py --selftest --replay ...  # 只跑数据层, 不启可视化发布

帧格式见《上位机交接文档.md》: 一行一个 JSON, 首字段 "m" 为帧名, 共 14 种。
"""
import argparse
import atexit
import json
import math
import os
import re
import socket
import struct
import sys
import threading
import time
from collections import Counter, deque
from business_protocol import allowed_interaction

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TCP_PORT = 8080   # 车端固件写死连接此端口, 勿改
DEFAULT_WS_PORT = 8765    # Lichtblick 默认 Foxglove WebSocket 端口
CMD_FILE = os.path.join(BASE_DIR, 'send_cmd.txt')   # 下行注入文件(与 wifi_rebuild 同机制)
BRIDGE_LOG = os.path.join(BASE_DIR, 'bridge_log.txt')

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

_log_lock = threading.Lock()
_log_fp = None


def log(msg):
    line = f'[{time.strftime("%H:%M:%S")}] {msg}'
    global _log_fp
    with _log_lock:
        print(line, flush=True)
        if _log_fp is None:
            try:
                _log_fp = open(BRIDGE_LOG, 'a', encoding='utf-8')
            except OSError:
                _log_fp = False
        if _log_fp:
            try:
                _log_fp.write(line + '\n')
                _log_fp.flush()
            except OSError:
                pass


# --------------------------------------------------------------------------
# 数据管线: 行 -> JSON -> 按 m 分发 -> 状态缓存 + 发布
# --------------------------------------------------------------------------
class Pipeline:
    """线程安全。所有数据源(TCP/重放)共用同一条管线。"""

    def __init__(self, publisher=None):
        self.publisher = publisher or DebugPublisher()
        self.lock = threading.Lock()
        self.state = {}       # m -> 最新帧对象(发布层组合消息用)
        self.counts = Counter()
        self.bad = 0

    def feed_line(self, raw, src='car'):
        """raw: bytes 或 str, 一行 JSON(可不带换行)"""
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8', 'replace')
        line = raw.strip()
        if not line:
            return
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            # 非法行: AT 残留/hello/半行, 按交接文档约定直接丢弃
            with self.lock:
                self.bad += 1
            return
        if not isinstance(obj, dict) or not isinstance(obj.get('m'), str):
            with self.lock:
                self.bad += 1
            return
        m = obj['m']
        with self.lock:
            self.counts[m] += 1
            self.state[m] = obj
        # 全帧原始行入档 (链路判决/调试用; ~18 行/s, 10 分钟约 1MB)
        log(f'[DATA] {line}')
        self.publisher.on_frame(m, obj)

    def summary(self):
        with self.lock:
            return dict(self.counts), self.bad, dict(self.state)


# --------------------------------------------------------------------------
# 发布层
# --------------------------------------------------------------------------
class OccupancyMapper:
    """雷达射线投射占用栅格(世界系)。costmap 语义: 0=空闲 100=占用 255=未知。
    log-odds 累积: 射线穿过格 -0.35(变空), 命中格 +0.9(变占); 每分钟衰减 5% 防
    旧图僵化; clear() 供 info 帧(车重启)清图。8m×8m @ 5cm = 160×160 格。"""

    def __init__(self, size_m=8.0, res=0.05):
        self.res = res
        self.n = int(size_m / res)
        self.half = size_m / 2.0
        self.lo = [0.0] * (self.n * self.n)
        self.dirty = False
        self._last_decay = time.time()

    def clear(self):
        self.lo = [0.0] * (self.n * self.n)
        self.dirty = True

    def _mark(self, x, y, delta):
        i = int((x + self.half) / self.res)
        j = int((y + self.half) / self.res)
        if 0 <= i < self.n and 0 <= j < self.n:
            v = self.lo[j * self.n + i] + delta
            self.lo[j * self.n + i] = 4.0 if v > 4.0 else (-4.0 if v < -4.0 else v)

    def update(self, px, py, yaw_deg, ranges):
        """ranges: [(angle_deg 车体系, dist_m, valid)]; 世界角 = yaw + 车体系角。
        命中距离超出栅格覆盖(对角)视为野值/无回波: 只标记穿过, 不标占用 —
        否则墙外"从未探测"格子会被野值染成占用红。"""
        now = time.time()
        if now - self._last_decay > 60:
            self._last_decay = now
            self.lo = [v * 0.95 for v in self.lo]
        step = self.res * 0.9
        dmax = min(self.half * 1.4, 12.0)
        for ang_deg, dist, valid in ranges:
            if not valid:
                continue
            ang = math.radians(yaw_deg + ang_deg)
            dx, dy = math.cos(ang), math.sin(ang)
            d = 0.2
            while d < dmax - 0.05 and d < dist - 0.05:
                self._mark(px + dx * d, py + dy * d, -0.6)
                d += step
            if dist <= dmax:
                # 命中权重低: 真墙多圈反复命中才累积过阈值, 单次野值(+0.4)永不显形
                self._mark(px + dx * dist, py + dy * dist, 0.45)
        self.dirty = True

    def render_blocks(self):
        """occupied 格 -> 世界坐标中心点列表 (供 SceneUpdate cubes 渲染)。
        free/unknown 不发: 地面网格即背景, 未探测天然隐形 — 零撞色"""
        half, res = self.half, self.res
        out = []
        for j in range(self.n):
            base = j * self.n
            for i in range(self.n):
                if self.lo[base + i] > 0.5:
                    out.append((-half + (i + 0.5) * res, -half + (j + 0.5) * res))
        return out


class DebugPublisher:
    """自测用: 统计各帧速率, 定期打印。不依赖任何第三方库。"""

    def __init__(self, interval=3.0):
        self.interval = interval
        self._lock = threading.Lock()
        self._since = Counter()
        self._stop = False
        self._samples = {}          # m -> 首个样例
        threading.Thread(target=self._loop, daemon=True).start()

    def on_frame(self, m, obj):
        with self._lock:
            self._since[m] += 1
            self._samples.setdefault(m, obj)

    def _loop(self):
        while not self._stop:
            time.sleep(self.interval)
            with self._lock:
                if self._since:
                    log('[发布] ' + '  '.join(f'{k}:{v}' for k, v in sorted(self._since.items())))
                    self._since = Counter()

    def close(self):
        self._stop = True


class FoxglovePublisher:
    """发布层: 把 14 种帧打包成 Lichtblick 可直接渲染的标准消息。

    渲染全部交给 Lichtblick 内置面板(3D/Plot/Gauge/RosOut/Table), 本层只做数据打包:
      /lidar   PointCloud(protobuf)  雷达 36 点(la), cm->m, x/y/z+distance 四字段
              (distance 供面板按距离着色; 不用 LaserScan 通道——其单分量 position 会触发
               three.js computeBoundingSphere NaN, 每帧报错且网格被剔除, Lichtblick v1.28.1 实测)
      /scene   SceneUpdate(protobuf) 购物车模型(方块+四轮) + 头顶文字标签 + 行驶轨迹线 + UWB 目标
      /tf      FrameTransforms       world->cart: 平移=pose.x/y, 旋转=IMU r/p/y
      /rosout  Log(protobuf)         info/mode/echo 文本日志(RosOut 面板直接显示)
      JSON 标量通道(Plot/Gauge/Table 面板用):
      /chassis /follow /uwb /imu /power /health /diag /us /pose /mode /vi /lidar_meta
    """

    # 车体模型(购物车简模, cart 坐标系, 车头朝 +X, 米)
    _BODY = dict(pos=(0.05, 0.0, 0.38), size=(0.72, 0.5, 0.4), color=(0.20, 0.50, 0.95, 0.92))
    _HANDLE = dict(pos=(-0.36, 0.0, 0.64), size=(0.06, 0.46, 0.06), color=(0.65, 0.68, 0.72, 1.0))
    _WHEEL = dict(size=(0.17, 0.17, 0.06), color=(0.12, 0.12, 0.14, 1.0),
                  xy=((0.26, 0.29), (0.26, -0.29), (-0.26, 0.29), (-0.26, -0.29)))
    _TARGET = dict(size=(0.30, 0.30, 0.30), color=(0.95, 0.25, 0.25, 0.9))
    # 超声波 3 路方位(度, 车体系: 0=前 +90=左 -90=右 180=后)。
    # 固件 obstacle_avoidance.c:14-17 — 传感器1(d1)→车右侧, 2(d2)→车左侧, 3(d3)→车后方
    _US_ANGLES = (-90.0, 90.0, 180.0)

    def __init__(self, ws_port=DEFAULT_WS_PORT):
        import foxglove
        from foxglove import Capability, Channel, ServerListener
        from foxglove.channels import GridChannel, PointCloudChannel, SceneUpdateChannel
        from foxglove.messages import (
            ArrowPrimitive, Color, CubePrimitive, CylinderPrimitive, FrameTransform,
            FrameTransforms, Grid, LinePrimitive, LinePrimitiveLineType, Log, LogLevel,
            PackedElementField, PackedElementFieldNumericType, Point3, PointCloud, Pose,
            Quaternion, SceneEntity, SceneEntityDeletion, SceneEntityDeletionType,
            SceneUpdate, SpherePrimitive, TextPrimitive, Timestamp, Vector2, Vector3)
        try:
            from foxglove.messages import TriangleListPrimitive  # 超声 FOV 扇形用
        except ImportError:
            TriangleListPrimitive = None
        try:
            from foxglove.messages import Duration as _Duration  # 易逝实体寿命
        except ImportError:
            _Duration = None

        self._fx = foxglove
        self._M = dict(Log=Log, Color=Color, CubePrimitive=CubePrimitive,
                       CylinderPrimitive=CylinderPrimitive, FrameTransform=FrameTransform,
                       FrameTransforms=FrameTransforms, PointCloud=PointCloud, Pose=Pose,
                       Quaternion=Quaternion, SceneEntity=SceneEntity, SceneUpdate=SceneUpdate,
                       SpherePrimitive=SpherePrimitive, Timestamp=Timestamp, Vector3=Vector3,
                       Vector2=Vector2, Grid=Grid,
                       LogLevel=LogLevel,
                       LinePrimitive=LinePrimitive, TextPrimitive=TextPrimitive,
                       Point3=Point3, LineType=LinePrimitiveLineType,
                       Arrow=ArrowPrimitive,
                       SceneEntityDeletion=SceneEntityDeletion,
                       DelType=SceneEntityDeletionType,
                       PEF=PackedElementField, FT=PackedElementFieldNumericType,
                       Tri=TriangleListPrimitive, Duration=_Duration)
        self._q_id = Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        self._q_wheel = Quaternion(x=0.7071068, y=0.0, z=0.0, w=0.7071068)  # 圆柱轴转成 Y(轮轴)
        self._q_flip = Quaternion(x=0.0, y=0.0, z=1.0, w=0.0)  # 绕 Z 转 180°(倒车箭头)

        pub = self

        class _UiListener(ServerListener):
            """Lichtblick Publish 面板的客户端发布 → 白名单转下行"""

            _ALLOWED = ('$q', '$q-', '$w', '$w-', '$wt',
                        '$i01', '$i02', '$i03')   # $iNN=模拟识别; $wt=称重切换(桥翻译)

            def on_client_advertise(self, client, channel):
                cid = getattr(channel, 'id', channel)
                topic = getattr(channel, 'topic', '?')
                with pub._lock:
                    pub._client_chans[cid] = topic
                log(f'[UI] 客户端宣告通道 {topic} (id={cid})')

            def on_client_unadvertise(self, *a):
                try:
                    with pub._lock:
                        pub._client_chans.pop(a[1], None)
                except Exception:
                    pass

            def on_message_data(self, client, channel_id, data):
                try:
                    with pub._lock:
                        topic = pub._client_chans.get(channel_id)
                    if topic != '/cmd':
                        return
                    cmd = str(json.loads(bytes(data).decode('utf-8', 'replace'))
                              .get('cmd', '')).strip()
                    if cmd == 'ack':
                        # ACK 确认报警: 桥端状态机消化, 不下发车端(ISA-18.2 操作员响应)
                        with pub._lock:
                            pub._alarm['acked'] = True
                        log('[UI] 报警已确认 (ACK)')
                        return
                    if cmd in self._ALLOWED or allowed_interaction(cmd):
                        # $wt = 称重切换: 按桥端缓存的 en 状态翻译成 $w / $w-
                        if cmd == '$wt':
                            cmd = '$w-' if pub._hud.get('_wg_en') else '$w'
                            log(f'[UI] 称重切换 -> {cmd}')
                        log(f'[UI] 面板下发 {cmd}')
                        if pub._downlink:
                            pub._downlink(cmd)
                        else:
                            log('[UI] (无下行通路——重放模式下按钮无效)')
                    else:
                        log(f'[UI] 拒绝非白名单指令 {cmd!r}')
                except Exception as e:
                    log(f'!! [UI] 处理失败: {e}')

        # ★共享状态必须先于 start_server 初始化: SDK 绑定端口即接受连接,
        #   自动重连的客户端 advertise 可能落在状态就绪前(2026-09-06 审查 P2)
        self.pc_ch = PointCloudChannel('/lidar')
        self.scene_ch = SceneUpdateChannel('/scene')
        self.json_ch = {}
        for m, topic in [('ch', '/chassis'), ('fw', '/follow'), ('uw', '/uwb'),
                         ('imu', '/imu'), ('bat', '/power'), ('st', '/health'),
                         ('dg', '/diag'), ('us', '/us'), ('pose', '/pose'),
                         ('mode', '/mode'), ('vi', '/vi'), ('la', '/lidar_meta'),
                         ('stats', '/stats'), ('wg', '/load')]:
            self.json_ch[m] = Channel(topic, message_encoding='json')
        self.json_ch['prox'] = Channel('/prox')   # 障碍接近距离(cm), 面板状态灯用

        self._lock = threading.Lock()
        self._last_pose = (0.0, 0.0)   # pose.x/y, 未接电池时恒 0
        self._last_imu = (0.0, 0.0, 0.0)  # roll, pitch, yaw (deg)
        self._traj = deque(maxlen=240)    # 行驶轨迹(世界系), pose 帧驱动, 最多 4 分钟
        self._wheel_ang = [0.0, 0.0, 0.0, 0.0]  # 四轮累计转角(rad), 轮动画积分
        self._last_loop_t = time.time()
        self._mapper = OccupancyMapper()        # 实时占用栅格(雷达射线投射)
        self._hud = dict(mode='--', speed=0.0, volt=0.0, odo=0.0)  # 3D 购物车头顶文字标签
        self._client_chans = {}           # 客户端发布通道 id -> topic (界面遥控)
        self._downlink = None             # 下行发送函数(由 main 注入 bus.dispatch)
        self._last_frame = {}             # 事件帧缓存(晚加入状态同步用)
        self._last_recv = {}              # 各帧最近到达时刻(数据龄/断流检测)
        self._alarm = {'first': None, 'first_ts': None, 'acked': True}  # ISA-18.2 首出寄存器
        self._volt_hist = deque(maxlen=90)  # (t, V) 2s 一点, 180s 滑窗算放电斜率
        self._hud_tick = 0
        self._sweep_ang = 0.0             # 扫描臂当前角度
        self._handlers = {
            'la': self._on_la, 'imu': self._on_imu, 'pose': self._on_pose, 'uw': self._on_uw,
            'ch': self._on_ch, 'fw': self._on_fw,
            'bat': self._on_bat, 'st': self._on_st,
            'dg': lambda o: self._json('dg', o), 'us': self._on_us,
            'vi': self._on_vi, 'wg': self._on_wg,
            'mode': self._on_mode, 'echo': self._on_echo, 'info': self._on_info,
        }

        self.server = foxglove.start_server(
            name='linggou-bridge', port=ws_port,
            capabilities=[Capability.ClientPublish], server_listener=_UiListener())
        atexit.register(self.close)
        log(f'Foxglove WS 服务端 ws://localhost:{self.server.port} (Lichtblick 连这里)')
        log('界面遥控: Publish 面板 topic=/cmd 载荷 {"cmd":"$q|$q-|$w|$w-"}')

        threading.Thread(target=self._sweep_loop, daemon=True).start()
        threading.Thread(target=self._wheels_loop, daemon=True).start()
        threading.Thread(target=self._map_loop, daemon=True).start()
        threading.Thread(target=self._health_loop, daemon=True).start()
        threading.Thread(target=self._resync_loop, daemon=True).start()

    # -- 入口(线程安全: TCP 线程/重放线程都可能调) --
    def on_frame(self, m, obj):
        h = self._handlers.get(m)
        if h is None:
            return
        try:
            with self._lock:
                self._last_frame[m] = obj
                self._last_recv[m] = time.time()
                h(obj)
        except Exception as e:
            log(f'!! 发布 {m} 失败: {e}')

    def set_downlink(self, fn):
        """注入下行发送函数(main 里接 bus.dispatch), 供界面遥控按钮调用"""
        self._downlink = fn

    def close(self):
        try:
            self.server.stop()
        except Exception:
            pass

    # -- 工具 --
    def _now(self):
        ns = time.time_ns()
        return self._M['Timestamp'](sec=ns // 1_000_000_000, nsec=ns % 1_000_000_000)

    def _life(self, sec):
        """易逝实体寿命: 高频动画实体(sweep/us_fov/uwb_cone/cart_model)带 lifetime,
        线程崩溃/桥重启后渲染端自动过期清除 — 不再留僵尸帧(2026-09-05 黄线事故)。
        SDK 无 Duration 时返回 None(常驻, 行为回退旧版)。"""
        D = self._M.get('Duration')
        return None if D is None else D(sec=int(sec), nsec=int((sec % 1.0) * 1e9))

    def _v3(self, t):
        return self._M['Vector3'](x=t[0], y=t[1], z=t[2])

    def _col(self, t):
        return self._M['Color'](r=t[0], g=t[1], b=t[2], a=t[3])

    def _json(self, key, obj):
        self.json_ch[key].log({k: v for k, v in obj.items() if k != 'm'})

    def _log_text(self, text, level):
        self._fx.log('/rosout', self._M['Log'](timestamp=self._now(), level=level,
                                               message=text, name='linggou'))

    def _publish_tf(self):
        roll, pitch, yaw = (math.radians(a) for a in self._last_imu)
        cr, sr = math.cos(roll / 2), math.sin(roll / 2)
        cp, sp = math.cos(pitch / 2), math.sin(pitch / 2)
        cy, sy = math.cos(yaw / 2), math.sin(yaw / 2)
        q = self._M['Quaternion'](x=sr * cp * cy - cr * sp * sy, y=cr * sp * cy + sr * cp * sy,
                                  z=cr * cp * sy - sr * sp * cy, w=cr * cp * cy + sr * sp * sy)
        x, y = self._last_pose
        self._fx.log('/tf', self._M['FrameTransforms'](transforms=[self._M['FrameTransform'](
            timestamp=self._now(), parent_frame_id='world', child_frame_id='cart',
            translation=self._v3((x, y, 0.0)), rotation=q)]))

    # -- 各帧处理 --
    # OpenMV 二维码编号 -> 商品 (同步自屏工程 main.lua g_scan_items / 旧固件 openmv.c,
    # STM32 本体不存表, 上位机持有副本做展示富化)
    ITEMS = {
        1: ('苹果', '3元/斤'), 2: ('香蕉', '4.5元/斤'), 3: ('牛奶', '8元/盒'),
        4: ('鸡蛋', '6.8元/斤'), 5: ('大米', '3.2元/斤'), 6: ('食盐', '2.5元/袋'),
        7: ('生抽', '9.9元/瓶'), 8: ('纸巾', '12.9元/提'), 9: ('洗衣液', '25元/桶'),
        10: ('面包', '7.5元/个'), 11: ('可乐', '3元/罐'),
    }

    def _on_vi(self, obj):
        i = int(obj.get('i') or 0)
        name, price = self.ITEMS.get(i, ('?', '?'))
        self._hud['item'] = f"{i:02d} {name}" if i else '--'
        self._json('vi', dict(obj, name=name, price=price))
        previous = self._hud.get('_vi_seq')
        seq = obj.get('seq')
        if i and (seq is None or seq != previous):
            self._log_text(f'识别: {i:02d} {name} {price}', self._M['LogLevel'].Info)
        self._hud['_vi_seq'] = seq

    def _on_wg(self, obj):
        # wg 是事件帧: en 变化立即 1 帧; en=1 时 >5g 变化或 1s 心跳; en=0 后静默
        # en=0 时 kg 冻结在最后值而非 0 — txt 必须按 en 分态, 否则取下后灯显旧值
        en = int(obj.get('en') or 0)
        valid = obj.get('valid') == 1
        reading = obj.get('kg')
        numeric = isinstance(reading, (int, float)) and not isinstance(reading, bool) and math.isfinite(reading)
        kg = reading if numeric else 0.0
        g = kg * 1000.0
        usable = en and valid and numeric
        obj['state'] = 2 if usable else (1 if en else 0)
        obj['txt'] = f'{g:.0f} g' if usable else ('等待有效称重' if en else '待机')
        if not usable:
            obj['kg'] = None  # a stale/invalid sample must not drive the gauge
            obj['valid'] = 0
        # 派生: 称重状态(状态时间带用) — 1s 心跳内克数变化 <10g 判稳定(死区语义)
        obj['ws'] = '有效采样' if usable else ('等待有效称重' if en else '待机')
        self._hud['_wg_prev'] = g
        self._json('wg', obj)
        prev = self._hud.get('_wg', 0.0)
        self._hud['_wg'] = g
        self._hud['_wg_en'] = en   # 供 $wt 称重切换按钮判态
        self._hud['_wg_ts'] = time.time()   # watchdog: en=1 静默超时判待机
        if usable and prev < 5.0 <= g:
            self._log_text(f'放上物品: {obj["txt"]}', self._M['LogLevel'].Info)
        elif usable and prev >= 5.0 > g:
            self._log_text('取下物品', self._M['LogLevel'].Info)

    def _wg_watchdog(self):
        """en=1 时 wg 心跳 1s 一帧; 若 >5s 静默 = 边沿帧丢失/采样已停,
           桥端合成一帧 en=0 待机 — 灯回待机、$wt 判态自愈, 不再卡旧状态"""
        with self._lock:
            if not self._hud.get('_wg_en'):
                return
            if time.time() - self._hud.get('_wg_ts', 0) < 5.0:
                return
            self._hud['_wg_en'] = 0
        obj = {'m': 'wg', 'kg': None, 'valid': 0, 'state': 0,
               'raw': 0, 'en': 0, 'txt': '待机', 'ws': '数据超时', 'stale': True}
        self._json('wg', obj)
        log('[wg] en=1 静默超时, 合成待机帧 (下行丢失兜底)')

    def _on_st(self, obj):
        # st 只入缓存(on_frame 已记), /health 由 _health_loop 2Hz 合成发布 —
        # 活性灯自身不能依赖被监测的数据流(st 停发时 /health 必须还活着)
        pass

    def _health_loop(self):
        """2Hz 合成 /health: st 原始字段 + 数据龄 + ISA-18.2 首出报警状态机 + 续航估计。"""
        while True:
            time.sleep(0.5)
            try:
                obj = self._health_tick()
                if obj is not None:
                    self.json_ch['st'].log(obj)
            except Exception as e:
                log(f'!! health 失败: {e}')

    def _health_tick(self):
        now = time.time()
        with self._lock:
            st = dict(self._last_frame.get('st') or {})
            # 数据龄: 快周期话题(pose/imu/fw/uw/ch/la ≤1s)取最旧者; 从未到过按断流计
            ages = {}
            for m in ('pose', 'imu', 'fw', 'uw', 'ch', 'la', 'us', 'bat', 'st'):
                t0 = self._last_recv.get(m)
                ages[m] = None if t0 is None else round(now - t0, 2)
            fast = [ages[m] for m in ('pose', 'imu', 'fw', 'uw', 'ch', 'la')]
            wage = 99.0 if (not fast or any(a is None for a in fast)) else round(max(fast), 2)
            # 报警状态机: 源 = 雷达报错 / 电池低电 / 链路断(真故障, 不含正常避障近障)
            sources = []
            if (self._last_frame.get('la') or {}).get('err') or 0:
                sources.append('雷达报错')
            if (self._last_frame.get('bat') or {}).get('low') == 1:
                sources.append('电池低电')
            if st.get('wifi', 1) == 0:            # wifi=0 = 链路断(缺键按正常;旧写法 or 1 把 0 吞掉)
                sources.append('链路断开')
            al = self._alarm
            if sources and al['first'] is None:
                al['first'] = sources[0]
                al['first_ts'] = now
                al['acked'] = False
                log(f'[报警] 首出: {al["first"]} (当前活跃: {", ".join(sources)})')
                self._log_text(f'首出报警: {al["first"]}', self._M['LogLevel'].Warning)
            elif not sources and al['acked']:
                al['first'] = None
            alarm_state = 0 if al['first'] is None else (1 if not al['acked'] else 2)
            # 续航估计: 电压 100s+ 滑窗线性斜率, 斜率<0 才发布; 分钟向下取整(保守)
            rng = None
            vh = self._volt_hist
            if len(vh) >= 50:
                (t1, v1), (t0, v0) = vh[-1], vh[0]
                span = t1 - t0
                if span >= 100:
                    slope = (v1 - v0) / span          # V/s
                    if slope < -1e-6 and v1 > 10.5:
                        rng = max(1, int((v1 - 10.5) / -slope // 60))
            obj = {k: v for k, v in st.items() if k != 'm'}
            obj.update(wage=wage, ages=ages, alarm_state=alarm_state,
                       first=al['first'],
                       first_age=round(now - al['first_ts'], 1) if al['first_ts'] else None,
                       acked=al['acked'], alarms=sources, range_min=rng)
            return obj

    def _publish_stats(self):
        """关键数字表 (/stats): 1Hz 随 pose 刷新, 供 Table 面板直读"""
        f = self._last_frame.get('fw') or {}
        h = self._hud
        st = self._last_frame.get('st') or {}
        rows = [
            {'指标': '电压',   '值': f"{h.get('volt') or 0:.1f} V"},
            {'指标': '速度',   '值': f"{h.get('speed') or 0:.2f} m/s"},
            {'指标': '跟距',   '值': f"{f.get('d') or 0:.2f} m"},
            {'指标': '里程',   '值': f"{h.get('odo') or 0:.1f} m"},
            {'指标': '最近识别', '值': h.get('item', '--')},
            {'指标': '主循环',  '值': f"{st.get('lp', '--')} Hz"},
            {'指标': '心跳',    '值': str(st.get('hb', '--'))},
        ]
        self.json_ch['stats'].log({'row': rows})

    def _on_la(self, obj):
        pts = []
        ranges = []                     # 同步喂占用栅格建图(含无效点)
        for i, dcm in enumerate(obj.get('d') or []):
            valid = dcm is not None and dcm >= 0
            ranges.append((i * 10.0, (dcm if valid else 0) / 100.0, valid))
            if not valid:               # 无效点(-1)剔除
                continue
            ang = math.radians(i * 10)
            d = dcm / 100.0                 # cm -> m
            pts.append((d * math.cos(ang), d * math.sin(ang), 0.0, d))
        px, py = self._last_pose
        self._mapper.update(px, py, self._last_imu[2], ranges)
        data = b''.join(struct.pack('<ffff', *p) for p in pts)
        M, V3 = self._M['PointCloud'], self._M['Vector3']
        self.pc_ch.log(M(timestamp=self._now(), frame_id='cart',
                         pose=self._M['Pose'](position=V3(x=0.0, y=0.0, z=0.24),
                                              orientation=self._q_id),
                         point_stride=16,
                         fields=[self._M['PEF'](name='x', offset=0, type=self._M['FT'].Float32),
                                 self._M['PEF'](name='y', offset=4, type=self._M['FT'].Float32),
                                 self._M['PEF'](name='z', offset=8, type=self._M['FT'].Float32),
                                 self._M['PEF'](name='distance', offset=12,
                                               type=self._M['FT'].Float32)],
                         data=data))
        self._json('la', {'s': obj.get('s'), 'fc': obj.get('fc'), 'err': obj.get('err')})

    def _on_imu(self, obj):
        self._last_imu = (obj.get('r') or 0.0, obj.get('p') or 0.0, obj.get('y') or 0.0)
        self._json('imu', obj)
        self._publish_tf()
        self._scene_cart()               # 2Hz 保活, 姿态动画实际由 /tf 驱动

    _FW_NAMES = {0: 'IDLE', 1: 'STOPPED', 2: 'HOLDING', 3: 'APPROACHING',
                 4: 'LOST', 5: 'SEARCHING'}

    def _on_fw(self, obj):
        obj['e'] = round((obj.get('d') or 0.0) - 0.6, 3)   # 派生: 跟随误差(目标 0.6m)
        # 派生: 跟随裕度 %(F1 delta 语言) — 误差距 0.9m 停跟阈值还剩多少
        obj['m'] = round(max(0.0, min(100.0, (0.9 - abs(obj['e'])) / 0.9 * 100.0)), 1)
        self._json('fw', obj)
        st = obj.get('st')
        if st is not None and st != self._hud.get('_fw_st'):
            self._hud['_fw_st'] = st
            d = obj.get('d') or 0.0
            self._log_text(f"跟随: {self._FW_NAMES.get(st, st)}"
                           + (f" {d:.2f} m" if st in (2, 3) else ""),
                           self._M['LogLevel'].Info)

    def _on_pose(self, obj):
        prev = self._traj[-1] if self._traj else None
        self._last_pose = (obj.get('x') or 0.0, obj.get('y') or 0.0)
        self._hud['speed'] = obj.get('vx') or 0.0
        self._hud['odo'] = obj.get('td') or 0.0
        if prev is None or self._last_pose != prev:
            # pose 1Hz 偏粗, 短弧会折线化: 线性插 3 个中间点(>1.5m 跳变视为瞬移不插)
            if prev is not None and (abs(self._last_pose[0] - prev[0])
                                     + abs(self._last_pose[1] - prev[1])) < 1.5:
                for k in (1, 2, 3):
                    f = k / 4.0
                    self._traj.append((prev[0] + (self._last_pose[0] - prev[0]) * f,
                                       prev[1] + (self._last_pose[1] - prev[1]) * f))
            self._traj.append(self._last_pose)
        self._json('pose', obj)
        self._publish_tf()
        self._publish_stats()

    def _on_uw(self, obj):
        self._json('uw', obj)
        M = self._M
        ok = bool(obj.get('ok'))
        bx, by = obj.get('x') or 0.0, obj.get('y') or 0.0   # 基站系: x=右, y=前 (m)
        d = obj.get('d') or 0.0
        px, py = self._last_pose
        if ok and (abs(bx) + abs(by)) > 1e-6:
            # 基站系(车载, x=右/y=前) → 世界系: 按 pose.yaw 旋转, 固件
            # uwb_follow.c:81-92 tag_to_global 同款换算 — 否则车一转弯目标球就飘
            yaw = math.radians(self._last_imu[2])
            tx = px + by * math.cos(yaw) + bx * math.sin(yaw)
            ty = py + by * math.sin(yaw) - bx * math.cos(yaw)
            # 目标球 + 车→目标连线 + 距离文字(单实体, 一次删除可整体清掉)
            ent = M['SceneEntity'](
                timestamp=self._now(), frame_id='world', id='uwb_target', frame_locked=True,
                spheres=[M['SpherePrimitive'](
                    pose=M['Pose'](position=self._v3((tx, ty, 0.6)), orientation=self._q_id),
                    size=self._v3(self._TARGET['size']), color=self._col(self._TARGET['color']))],
                lines=[M['LinePrimitive'](
                    type=M['LineType'].LineStrip,
                    pose=M['Pose'](position=self._v3((0.0, 0.0, 0.3)), orientation=self._q_id),
                    thickness=0.03, scale_invariant=False,
                    points=[M['Point3'](x=px, y=py, z=0.0), M['Point3'](x=tx, y=ty, z=0.0)],
                    color=self._col((0.95, 0.30, 0.25, 0.85)))],
                texts=[M['TextPrimitive'](
                    pose=M['Pose'](position=self._v3((tx, ty, 0.95)), orientation=self._q_id),
                    billboard=True, font_size=0.2,
                    text=f"目标 {d:.2f} m",
                    color=self._col((1.0, 0.45, 0.40, 1.0)))])
            self.scene_ch.log(M['SceneUpdate'](entities=[ent]))
            self._uwb_cone(obj)
        else:
            # ok 掉线/无坐标: 清理残留实体(修 v2 的幽灵红球问题)
            # type 必须用 MatchingId(只删指定 id); All=清空整个场景, 会导致车模型闪烁
            self.scene_ch.log(M['SceneUpdate'](deletions=[
                M['SceneEntityDeletion'](timestamp=self._now(), id='uwb_target',
                                         type=M['DelType'].MatchingId),
                M['SceneEntityDeletion'](timestamp=self._now(), id='uwb_cone',
                                         type=M['DelType'].MatchingId)]))

    def _uwb_cone(self, obj):
        """UWB 精度锥(紫色立体圆锥, 与超声平面扇形明显区分): 锥尖在车、张口套住
        目标球, 口径随实测距离 d 变宽 — '距离越远、角度不确定度越大'的直读隐喻。
        几何全来自实测 uw 帧(距离 d/方位 a), 半张角 10° 是可视化参数非仿真数据。"""
        M = self._M
        d = obj.get('d') or 0.0
        a = obj.get('a') or 0.0
        L = min(d, 5.0)
        if L < 0.2:
            return
        theta = math.radians(-a)              # uw.a 右正 → 车体系(左=+Y)取负
        mouth = 0.35 * L                      # 半张角 ≈10° → 口径 2·L·tan10°
        cone = M['CylinderPrimitive'](
            pose=M['Pose'](
                position=self._v3((L / 2 * math.cos(theta), L / 2 * math.sin(theta), 0.45)),
                orientation=self._cone_quat(theta)),
            size=self._v3((mouth, mouth, L)),
            bottom_scale=0.12, top_scale=1.0,
            color=self._col((0.55, 0.35, 0.95, 0.16)))
        self.scene_ch.log(M['SceneUpdate'](entities=[M['SceneEntity'](
            timestamp=self._now(), frame_id='cart', id='uwb_cone', frame_locked=True,
            lifetime=self._life(1.5),
            cylinders=[cone])]))

    def _on_ch(self, obj):
        self._hud['_ch_ts'] = time.time()   # 轮动画断流判据(>2s 停转)
        self._json('ch', obj)

    def _on_us(self, obj):
        self._json('us', obj)
        ds = [obj.get('d1'), obj.get('d2'), obj.get('d3')]
        valid = [d for d in ds if isinstance(d, (int, float)) and d > 0]
        near = min(valid) if valid else 999
        self._json('prox', {'d': near})
        was = self._hud.get('_near', 999)
        if (near < 20) != (was < 20):        # 近障状态沿: 只在进出时播报一次
            self._hud['_near'] = near
            self._log_text(f"近障 {near:.0f} cm!" if near < 20 else "障碍解除",
                           self._M['LogLevel'].Warning if near < 20 else self._M['LogLevel'].Info)
        M = self._M
        spheres = []
        for i, dcm in enumerate(ds):
            if not (isinstance(dcm, (int, float)) and dcm > 0):
                continue                      # 0 = 无回波, 不画
            d = min(dcm / 100.0, 4.0)         # cm -> m, 封顶 4m
            ang = math.radians(self._US_ANGLES[i])
            if dcm <= 20:
                col = (0.95, 0.20, 0.20, 0.95)   # 近障红
            elif dcm <= 80:
                col = (0.98, 0.80, 0.20, 0.85)   # 中距黄
            else:
                col = (0.25, 0.85, 0.35, 0.75)   # 远绿
            spheres.append(M['SpherePrimitive'](
                pose=M['Pose'](position=self._v3((d * math.cos(ang), d * math.sin(ang), 0.35)),
                               orientation=self._q_id),
                size=self._v3((0.12, 0.12, 0.12)), color=self._col(col)))
        if spheres:
            self.scene_ch.log(M['SceneUpdate'](entities=[M['SceneEntity'](
                timestamp=self._now(), frame_id='cart', id='us_obstacles', frame_locked=True,
                spheres=spheres)]))
        else:
            self.scene_ch.log(M['SceneUpdate'](deletions=[M['SceneEntityDeletion'](
                timestamp=self._now(), id='us_obstacles', type=M['DelType'].MatchingId)]))
        self._us_fov(ds)

    # 超声 FOV 扇形(逐顶点渐变三角面): 有回波→按距着色到实测距离(与球同色语义),
    # 无回波→灰淡扇示意量程 1m。传感器"看得到在哪、多宽" — 评委可读的传感器几何。
    _US_ORIGIN = ((0.15, -0.20), (0.15, 0.20), (-0.35, 0.0))  # 右/左/后传感器车体系位置

    def _us_fov(self, ds):
        M = self._M
        if M.get('Tri') is None:
            return
        tris = [t for t in (self._us_sector(i, d) for i, d in enumerate(ds))
                if t is not None]
        if not tris:
            return
        self.scene_ch.log(M['SceneUpdate'](entities=[M['SceneEntity'](
            timestamp=self._now(), frame_id='cart', id='us_fov', frame_locked=True,
            lifetime=self._life(1.5),
            triangles=tris)]))

    def _us_sector(self, i, dcm):
        M = self._M
        ang = math.radians(self._US_ANGLES[i])
        half = math.radians(22)            # 单侧半张角 ≈22°(HC-SR04 量级)
        if isinstance(dcm, (int, float)) and dcm > 0:
            d = min(dcm / 100.0, 4.0)
            if dcm <= 20:
                base = (0.95, 0.20, 0.20)      # 近障红(与球一致)
            elif dcm <= 80:
                base = (0.98, 0.80, 0.20)      # 中距黄
            else:
                base = (0.25, 0.85, 0.35)      # 远绿
        else:
            d, base = 1.0, (0.55, 0.60, 0.66)  # 无回波: 灰淡扇
        ox, oy = self._US_ORIGIN[i]
        n = 7
        arc = [(ox + d * math.cos(ang - half + 2 * half * k / n),
                oy + d * math.sin(ang - half + 2 * half * k / n)) for k in range(n + 1)]
        pts, cols = [], []
        for k in range(n):
            pts.extend(((ox, oy), arc[k], arc[k + 1]))
            cols.extend(((base[0], base[1], base[2], 0.30),   # 顶点浓 → 弧端淡(波束渐隐)
                         (base[0], base[1], base[2], 0.05),
                         (base[0], base[1], base[2], 0.05)))
        P, C = M['Point3'], M['Color']
        return M['Tri'](
            pose=M['Pose'](position=self._v3((0.0, 0.0, 0.035)), orientation=self._q_id),
            points=[P(x=x, y=y, z=0.0) for x, y in pts],
            colors=[C(r=r, g=g, b=b, a=a) for r, g, b, a in cols])

    def _sweep_loop(self):
        """雷达扫掠观感: 20Hz 旋转"余辉楔面+细亮刃"贴地显示(经典雷达屏)。
        亮刃=当前方位细线; 余辉=其后 ~120° 楔面逐顶点渐隐。替代旧纯黄粗线(塑料感)。"""
        while True:
            time.sleep(0.05)
            try:
                with self._lock:
                    self._sweep_ang = (self._sweep_ang + 0.10) % (2 * math.pi)
                    ang = self._sweep_ang
                    M = self._M
                    R, N, TRAIL = 5.5, 12, 2.1
                    arc = [(R * math.cos(ang - TRAIL * k / N),
                            R * math.sin(ang - TRAIL * k / N)) for k in range(N + 1)]
                    pts, cols = [], []
                    for k in range(N):
                        a_k = 0.32 * (1.0 - k / N)          # 亮刃侧浓 → 拖尾端透明
                        a_k1 = 0.32 * (1.0 - (k + 1) / N)
                        pts.extend(((0.0, 0.0), arc[k], arc[k + 1]))
                        cols.extend(((1.0, 0.72, 0.15, a_k),
                                     (1.0, 0.72, 0.15, a_k),
                                     (1.0, 0.72, 0.15, a_k1)))
                    P, C = M['Point3'], M['Color']
                    wedge = M['Tri'](
                        pose=M['Pose'](position=self._v3((0.0, 0.0, 0.05)),
                                       orientation=self._q_id),
                        points=[P(x=x, y=y, z=0.0) for x, y in pts],
                        colors=[C(r=r, g=g, b=b, a=a) for r, g, b, a in cols])
                    blade = M['LinePrimitive'](
                        type=M['LineType'].LineStrip,
                        pose=M['Pose'](position=self._v3((0.0, 0.0, 0.06)),
                                       orientation=self._q_id),
                        thickness=0.035, scale_invariant=False,
                        points=[P(x=0.0, y=0.0, z=0.0),
                                P(x=R * math.cos(ang), y=R * math.sin(ang), z=0.0)],
                        color=self._col((1.0, 0.86, 0.45, 0.95)))
                    glow = M['LinePrimitive'](
                        type=M['LineType'].LineStrip,
                        pose=M['Pose'](position=self._v3((0.0, 0.0, 0.055)),
                                       orientation=self._q_id),
                        thickness=0.12, scale_invariant=False,
                        points=[P(x=0.0, y=0.0, z=0.0),
                                P(x=R * math.cos(ang), y=R * math.sin(ang), z=0.0)],
                        color=self._col((1.0, 0.72, 0.15, 0.22)))
                    self.scene_ch.log(M['SceneUpdate'](entities=[M['SceneEntity'](
                        timestamp=self._now(), frame_id='cart', id='sweep',
                        frame_locked=True, lifetime=self._life(0.4),
                        triangles=[wedge],
                        lines=[glow, blade])]))
            except Exception as e:
                log(f'!! sweep 失败: {e}')
                time.sleep(1.0)

    def _map_loop(self):
        """1Hz 把 occupied 格子发成贴地灰白薄片(SceneUpdate 实体 map_blocks)。
        不用 foxglove.Grid — 其 v1.28.1 colormap 色带与超声/点云撞色;
        只发障碍块: free 露出网格地面, unknown 天然隐形。无新块不重发。"""
        M = self._M
        while True:
            time.sleep(1.0)
            try:
                with self._lock:
                    if not self._mapper.dirty:
                        continue
                    self._mapper.dirty = False
                    blocks = self._mapper.render_blocks()
                if not blocks:
                    self.scene_ch.log(M['SceneUpdate'](deletions=[M['SceneEntityDeletion'](
                        timestamp=self._now(), id='map_blocks', type=M['DelType'].MatchingId)]))
                    continue
                cubes = [M['CubePrimitive'](
                    pose=M['Pose'](position=self._v3((x, y, 0.012)), orientation=self._q_id),
                    size=self._v3((0.05, 0.05, 0.024)),
                    color=self._col((0.70, 0.74, 0.80, 0.62))) for x, y in blocks]
                self.scene_ch.log(M['SceneUpdate'](entities=[M['SceneEntity'](
                    timestamp=self._now(), frame_id='world', id='map_blocks', frame_locked=True,
                    cubes=cubes)]))
            except Exception as e:
                log(f'!! map 失败: {e}')
                time.sleep(1.0)

    def _resync_loop(self):
        """每 5s 重发事件型帧(mode/vi)的缓存值, 晚加入的观看者立刻看到当前状态"""
        while True:
            time.sleep(5.0)
            try:
                with self._lock:
                    for m in ('mode', 'vi'):
                        obj = self._last_frame.get(m)
                        if obj:
                            self._json(m, obj)
            except Exception:
                pass
            try:
                self._wg_watchdog()
            except Exception:
                pass

    def _on_mode(self, obj):
        self._hud['mode'] = {1: 'FOLLOW', 2: 'SCAN'}.get(obj.get('v'), f"mode{obj.get('v')}")
        self._json('mode', obj)
        name = {1: '跟随 FOLLOW', 2: '扫描 SCAN'}.get(obj.get('v'), f"未知({obj.get('v')})")
        self._log_text(f'模式切换 -> {name}', self._M['LogLevel'].Info)

    def _on_echo(self, obj):
        rx = obj.get('rx', '')
        self._log_text(f'车端回声: {rx}', self._M['LogLevel'].Info)

    def _on_info(self, obj):
        self._traj.clear()               # 车端重启 = 新轨迹
        self._mapper.clear()             # 车端重启 = 清图重建
        self._log_text(f"车端上线 fw={obj.get('fw')} rst={obj.get('rst') or 0:#x}",
                       self._M['LogLevel'].Info)

    def _on_bat(self, obj):
        self._hud['volt'] = obj.get('v') or 0.0
        self._json('bat', obj)
        self._volt_hist.append((time.time(), self._hud['volt']))

    def _scene_cart(self):
        """HUD 文字 + 轨迹线(2Hz 保活); 车模型/轮动画由 _wheels_loop 20Hz 发"""
        M = self._M
        pose_t0 = self._last_recv.get('pose')
        stale = pose_t0 is None or (time.time() - pose_t0) > 2.5
        hud_txt = (f"{self._hud['mode']}  {self._hud['speed']:.2f} m/s  "
                   f"{self._hud['volt']:.1f} V  |  {self._hud['odo']:.1f} m")
        hud_col = (0.55, 0.95, 1.0, 1.0)
        if stale:
            hud_txt += '  |  数据断流!'
            hud_col = (1.0, 0.55, 0.20, 1.0)
        ents = [M['SceneEntity'](
            timestamp=self._now(), frame_id='cart', id='hud', frame_locked=True,
            texts=[M['TextPrimitive'](
                pose=M['Pose'](position=self._v3((0.0, 0.0, 0.95)), orientation=self._q_id),
                billboard=True, font_size=0.22,
                text=hud_txt,
                color=self._col(hud_col))])]
        if len(self._traj) >= 2:
            pts = list(self._traj)
            # 轨迹渐隐: LINE_STRIP 无法逐点 alpha, 两实体分层 — 全程淡青 + 最近 60 点高亮
            ents.append(M['SceneEntity'](
                timestamp=self._now(), frame_id='world', id='traj', frame_locked=True,
                lines=[M['LinePrimitive'](
                    type=M['LineType'].LineStrip,
                    pose=M['Pose'](position=self._v3((0.0, 0.0, 0.05)), orientation=self._q_id),
                    thickness=0.03, scale_invariant=False,
                    points=[M['Point3'](x=x, y=y, z=0.0) for x, y in pts],
                    color=self._col((0.10, 0.90, 1.0, 0.30)))]))
            if len(pts) > 8:
                ents.append(M['SceneEntity'](
                    timestamp=self._now(), frame_id='world', id='traj_hot', frame_locked=True,
                    lines=[M['LinePrimitive'](
                        type=M['LineType'].LineStrip,
                        pose=M['Pose'](position=self._v3((0.0, 0.0, 0.06)), orientation=self._q_id),
                        thickness=0.045, scale_invariant=False,
                        points=[M['Point3'](x=x, y=y, z=0.0) for x, y in pts[-60:]],
                        color=self._col((0.10, 0.90, 1.0, 0.95)))]))
        self.scene_ch.log(M['SceneUpdate'](entities=ents))

    # -- 四轮动画/朝向箭头 --
    def _cone_quat(self, theta):
        """圆锥姿态: 本地 +Z 轴指向车体系方位 theta(rad, atan2(y,x))。
        = q_z(θ) ⊗ q_y(90°) — q_y(90°) 把圆柱轴 Z 摆平到 +X, q_z 再转到方位角。
        PyO3 属性不可读, 全程元组运算(q1=q_z(θ), q2=q_y(90°): (0, k, 0, k))。"""
        s, c = math.sin(theta / 2.0), math.cos(theta / 2.0)
        k = 0.7071068
        return self._M['Quaternion'](x=-s * k, y=c * k, z=s * k, w=c * k)

    def _wheel_quat(self, a):
        """轮子姿态四元数 = 绕 Y 自转 a(rad) ⊗ 轴摆正(绕 X 90°)。
        foxglove PyO3 消息对象属性不可读(.x 报错), 全程元组运算, 仅构造时关键字传参。"""
        sx, sy, sz, sw = 0.0, math.sin(a / 2.0), 0.0, math.cos(a / 2.0)  # q_y(a)
        bx, by, bz, bw = 0.7071068, 0.0, 0.0, 0.7071068                  # q_wheel 常量
        Q = self._M['Quaternion']
        return Q(x=sw * bx + sx * bw + sy * bz - sz * by,
                 y=sw * by - sx * bz + sy * bw + sz * bx,
                 z=sw * bz + sx * by - sy * bx + sz * bw,
                 w=sw * bw - sx * bx - sy * by - sz * bz)

    def _wheels_loop(self):
        """20Hz 重发 cart_model: 四轮按 ch.r RPM 差速自转 + 车头速度箭头。
        (麦轮无转向舵机 — '转弯'的正确表达 = 朝向随 /tf yaw 变化 + 四轮差速可辨)"""
        # 固件 ch.r 顺序 RF,LF,RR,LR → 几何位(车头+X 左+Y): 右前(0.26,-0.29) 左前(0.26,0.29)
        # 右后(-0.26,-0.29) 左后(-0.26,0.29)
        geo = [(0.26, -0.29), (0.26, 0.29), (-0.26, -0.29), (-0.26, 0.29)]
        M = self._M
        while True:
            time.sleep(0.05)
            try:
                now = time.time()
                dt = min(now - self._last_loop_t, 0.2)
                self._last_loop_t = now
                with self._lock:
                    ch = self._last_frame.get('ch') or {}
                    rs = ch.get('r') or [0, 0, 0, 0]
                    if now - self._hud.get('_ch_ts', 0) > 2.0:
                        rs = [0, 0, 0, 0]           # ch 断流 2s → 轮子停转
                    # pose 断流 >2.5s → 整车变半透明灰("冻结的界面最危险, 因为它看着正常")
                    pose_t0 = self._last_recv.get('pose')
                    stale = pose_t0 is None or (now - pose_t0) > 2.5
                    body_col = (0.55, 0.58, 0.62, 0.35) if stale else self._BODY['color']
                    wheel_col = (0.40, 0.42, 0.45, 0.40) if stale else self._WHEEL['color']
                    wheels = []
                    for i, (rpm, (wx, wy)) in enumerate(zip(rs, geo)):
                        v_ms = (rpm or 0.0) * 0.213628 / 60.0     # RPM -> 轮面线速度 m/s
                        self._wheel_ang[i] = ((self._wheel_ang[i] + v_ms / 0.085 * dt)
                                              % (2 * math.pi))
                        q = self._wheel_quat(self._wheel_ang[i])
                        wheels.append(M['CylinderPrimitive'](
                            pose=M['Pose'](position=self._v3((wx, wy, 0.085)), orientation=q),
                            size=self._v3(self._WHEEL['size']),
                            color=self._col(wheel_col)))
                    vx = self._hud.get('speed') or 0.0
                    # ArrowPrimitive 字段(实测 schema): pose/shaft_length/shaft_diameter/
                    # head_length/head_diameter/color — 无 length 字段
                    arrow = M['Arrow'](
                        pose=M['Pose'](position=self._v3((0.45, 0.0, 0.5)),
                                       orientation=self._q_id if vx >= 0 else self._q_flip),
                        shaft_length=0.16 + min(abs(vx) * 1.2, 0.9),
                        shaft_diameter=0.04, head_length=0.14, head_diameter=0.12,
                        color=self._col((0.15, 0.95, 0.55, 0.9)))
                    ent = M['SceneEntity'](
                        timestamp=self._now(), frame_id='cart', id='cart_model',
                        frame_locked=True, lifetime=self._life(0.5),
                        cubes=[M['CubePrimitive'](
                                   pose=M['Pose'](position=self._v3(self._BODY['pos']),
                                                  orientation=self._q_id),
                                   size=self._v3(self._BODY['size']),
                                   color=self._col(body_col)),
                               M['CubePrimitive'](
                                   pose=M['Pose'](position=self._v3(self._HANDLE['pos']),
                                                  orientation=self._q_id),
                                   size=self._v3(self._HANDLE['size']),
                                   color=self._col((0.55, 0.58, 0.62, 0.35) if stale
                                                   else self._HANDLE['color']))],
                        cylinders=wheels, arrows=[] if stale else [arrow])
                    self.scene_ch.log(M['SceneUpdate'](entities=[ent]))
                self._hud_tick += 1
                if self._hud_tick % 10 == 0:      # 2Hz 保活 HUD(断流时也要翻 STALE 牌)
                    with self._lock:
                        self._scene_cart()
            except Exception as e:
                log(f'!! wheels 失败: {e}')
                time.sleep(1.0)


def make_publisher(args):
    if args.selftest:
        return DebugPublisher()
    try:
        return FoxglovePublisher(args.ws_port)
    except NotImplementedError:
        log('!! Foxglove 发布层尚未实现, 降级为 DebugPublisher (--selftest 语义)')
        return DebugPublisher()


# --------------------------------------------------------------------------
# 数据源 1: 真车 TCP (车端 ESP-01S 主动连接)
# --------------------------------------------------------------------------
class TcpSource:
    """监听 0.0.0.0:port, 接收 ESP 透传数据; 下行广播给所有客户端。
    收帧拆行逻辑与 wifi_rebuild/tcp_test_server.py 一致(recv -> buf 按 \\n 拆)。"""

    def __init__(self, port, pipeline, downlink_bus):
        self.port = port
        self.pipeline = pipeline
        self.downlink_bus = downlink_bus
        self._clients = set()
        self._lock = threading.Lock()
        self._tx_lock = threading.Lock()

    # -- 上行 --
    def serve_forever(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(('0.0.0.0', self.port))
        except OSError as e:
            log(f'!! 端口 {self.port} 绑定失败: {e} (残留进程占用? taskkill /F /IM python.exe)')
            sys.exit(1)
        srv.listen(5)
        log(f'TCP 监听 0.0.0.0:{self.port}, 等待车端连接...')
        self.downlink_bus.set_sender(self._send_to_all)
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def _handle(self, conn, addr):
        log(f'[CONNECT] {addr[0]}:{addr[1]}')
        conn.settimeout(30)   # st 帧 ≤5s 必到; 电池掉电半开连接 30s 静默后清场, 防 _clients 滞留
        with self._lock:
            self._clients.add(conn)
        heartbeat_stop = threading.Event()

        def heartbeat():
            while not heartbeat_stop.is_set():
                try:
                    with self._tx_lock:
                        conn.sendall(b'$ping\n')
                except OSError:
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                heartbeat_stop.wait(5.0)

        threading.Thread(target=heartbeat, daemon=True).start()
        try:
            buf = b''
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while b'\n' in buf:
                    line, buf = buf.split(b'\n', 1)
                    self.pipeline.feed_line(line)
                if len(buf) > 65536:          # 防恶意/异常膨胀
                    log(f'[RAW] {addr[0]}: 缓冲异常, 丢弃 {len(buf)}B')
                    buf = b''
        except (ConnectionResetError, OSError):
            pass
        finally:
            heartbeat_stop.set()
            with self._lock:
                self._clients.discard(conn)
            try:
                conn.close()
            except OSError:
                pass
            log(f'[CLOSE] {addr[0]}:{addr[1]}')

    # -- 下行 --
    def _send_to_all(self, text):
        data = (text.strip() + '\n').encode('utf-8')
        with self._lock:
            targets = list(self._clients)
        ok = 0
        for c in targets:
            try:
                with self._tx_lock:
                    c.sendall(data)
                ok += 1
            except OSError:
                pass
        log(f'[TX] {text!r} -> {ok} client(s)')


class DownlinkBus:
    """下行双通道: send_cmd.txt 轮询(0.3s, 读后删) + 终端输入。"""

    def __init__(self):
        self._sender = None

    def set_sender(self, fn):
        self._sender = fn

    def dispatch(self, text):
        if self._sender and text:
            self._sender(text)

    def start_cmdfile_poller(self):
        def poll():
            while True:
                try:
                    if os.path.isfile(CMD_FILE):
                        with open(CMD_FILE, 'r', encoding='utf-8') as f:
                            line = f.readline().strip()
                        try:
                            os.remove(CMD_FILE)
                        except OSError:
                            pass
                        if line:
                            self.dispatch(line)
                except OSError:
                    pass
                time.sleep(0.3)
        threading.Thread(target=poll, daemon=True).start()

    def start_console(self):
        def console():
            try:
                while True:
                    line = input().strip()
                    if line:
                        self.dispatch(line)
            except (EOFError, OSError):
                return
        threading.Thread(target=console, daemon=True).start()


# --------------------------------------------------------------------------
# 数据源 2: 日志重放 (server_log.txt, 答辩前无车开发/演示)
# --------------------------------------------------------------------------
_RE_DATA = re.compile(r'^\[\d{2}:\d{2}:\d{2}\]\s*\[DATA\]\s*[\d.]+:\s*(.*)$')
_RE_TIME = re.compile(r'^\[(\d{2}):(\d{2}):(\d{2})\]')


def _hms_to_sec(s):
    h, m, sec = s.split(':')
    return int(h) * 3600 + int(m) * 60 + int(sec)


class ReplaySource:
    """解析 tcp_test_server 的历史日志, 按时间轴加速重放真实帧。

    日志行格式: '[HH:MM:SS] [DATA] 10.236.22.113: {"m":"bat",...}'
    非 [DATA] 行(连接/关闭日志)与非法 JSON(AT 残留等)自动跳过。
    """

    def __init__(self, path, pipeline, speed=10.0, max_gap=5.0, start=None, loop=False, limit=0):
        self.path = path
        self.pipeline = pipeline
        self.speed = max(speed, 0.1)
        self.max_gap = max_gap
        self.start_sec = _hms_to_sec(start) if start else None
        self.loop = loop
        self.limit = limit

    def _parse(self):
        """返回 [(sec, line), ...]"""
        items = []
        with open(self.path, 'r', encoding='utf-8', errors='replace') as f:
            for ln in f:
                m = _RE_DATA.match(ln.strip())
                if not m:
                    continue
                payload = m.group(1).strip()
                t = _RE_TIME.match(ln.strip())
                sec = _hms_to_sec(t.group(0).strip('[]')) if t else 0
                if self.start_sec is not None and sec < self.start_sec:
                    continue
                items.append((sec, payload))
        return items

    def run(self):
        items = self._parse()
        if not items:
            log(f'!! 日志 {self.path} 里没有可重放的 [DATA] 帧')
            return
        log(f'重放 {self.path}: 共 {len(items)} 帧, speed={self.speed}x, max_gap={self.max_gap}s')
        fed = 0
        while True:
            prev = None
            for sec, payload in items:
                if prev is not None:
                    gap = sec - prev
                    if gap > 0:                       # 同秒内的帧直接连发
                        wait = min(gap / self.speed, self.max_gap)
                        time.sleep(wait)
                prev = sec
                self.pipeline.feed_line(payload, src='replay')
                fed += 1
                if self.limit and fed >= self.limit:
                    log(f'--limit {self.limit} 已到, 停止重放')
                    return
            if not self.loop:
                log('重放完毕(日志末尾)。--loop 可循环播放')
                return
            log('循环重放...')
            time.sleep(1.0)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description='灵购购物车上位机桥')
    ap.add_argument('--replay', metavar='LOG', help='重放 tcp_test_server 日志(无车开发/演示)')
    ap.add_argument('--speed', type=float, default=10.0, help='重放加速倍数(默认 10)')
    ap.add_argument('--max-gap', type=float, default=5.0, help='重放时间空洞封顶秒数(默认 5)')
    ap.add_argument('--start', metavar='HH:MM:SS', help='重放起始时刻')
    ap.add_argument('--loop', action='store_true', help='重放循环播放(答辩演示用)')
    ap.add_argument('--limit', type=int, default=0, help='只喂 N 帧后停止(测试用)')
    ap.add_argument('--tcp-port', type=int, default=DEFAULT_TCP_PORT, help='车端 TCP 端口(默认 8080)')
    ap.add_argument('--ws-port', type=int, default=DEFAULT_WS_PORT, help='Foxglove WS 端口(默认 8765)')
    ap.add_argument('--selftest', action='store_true', help='只跑数据层(DebugPublisher), 验证收帧')
    args = ap.parse_args()

    pipeline = Pipeline(make_publisher(args))
    bus = DownlinkBus()
    if hasattr(pipeline.publisher, 'set_downlink'):
        pipeline.publisher.set_downlink(bus.dispatch)   # 界面遥控按钮 → 下行

    if args.replay:
        log(f'重放模式: {args.replay} (下行通道关闭)')
        ReplaySource(args.replay, pipeline, args.speed, args.max_gap,
                     args.start, args.loop, args.limit).run()
        time.sleep(1.0)
    else:
        tcp = TcpSource(args.tcp_port, pipeline, bus)
        bus.start_cmdfile_poller()
        bus.start_console()
        tcp.serve_forever()

    counts, bad, _ = pipeline.summary()
    total = sum(counts.values())
    log(f'管线统计: 总帧 {total}, 坏行 {bad}, 分布 {dict(sorted(counts.items()))}')


if __name__ == '__main__':
    main()
