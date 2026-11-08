# 灵购智能购物车 · 上位机接入层

本目录是大创「灵购」项目基于 [Lichtblick](https://github.com/lichtblick-suite/lichtblick) 的上位机业务代码（最新基线：`integration_remote_voice_custom_20261029`，2026-10-29）。

上游仓库提供可视化框架；本目录负责车端协议接入、主题发布、布局与遥控网页。

## 数据流

```
车端 STM32 + ESP-01S
        │ TCP 8090（controller 持有）
        ▼
controller.py ──lightbrick_adapter──► Foxglove WS :8765 ──► Lichtblick
        │
        └── HTTP :8088 ──► web/ 遥控页
```

也可直接运行 `dashboard_backend/bridge.py`（独立 TCP 8080 桥，适合旧链路 / 日志重放 / fake_car）。

## 文件

| 路径 | 说明 |
|---|---|
| `controller.py` | RC2 遥控 + 业务指令 TCP 服务，可选挂载 Lichtblick |
| `lightbrick_adapter.py` | 异步队列适配，复用 `dashboard_backend/bridge.py` 发布器 |
| `business_protocol.py` | 下行白名单（含 `$PRICE` / `$SIM,WEIGHT`） |
| `protocol.json` | RC2 与业务扩展协议说明 |
| `dashboard_backend/bridge.py` | 帧解析 → Foxglove 主题发布 |
| `dashboard_backend/layout.json` | Lichtblick 布局（演示 / 系统双 Tab） |
| `web/` | 本地遥控网页 |
| `fake_car.py` / `probe.py` | 无车联调与通道探针 |
| `start.ps1` | 一键启动（controller + Lichtblick 适配） |
| `docs/` | 帧契约与侧可读说明 |

## 运行

```powershell
# 依赖（建议 venv）
python -m venv venv
.\venv\Scripts\pip install foxglove-sdk websockets

# 推荐：最新集成入口（遥控网页 + Lichtblick）
.\start.ps1

# 或仅跑旧独立桥（TCP 8080）
.\venv\Scripts\python.exe dashboard_backend\bridge.py
```

Lichtblick 连接：Open connection → **Foxglove WebSocket** → `ws://localhost:8765`，导入 `dashboard_backend/layout.json`。

遥控网页：`http://127.0.0.1:8088/`

## 业务扩展（custom 20261029）

- `$PRICE,NN,CENTS`：改 RAM 单价并重算账单（不写 flash）
- `$SIM,WEIGHT,GRAMS`：待称重商品手动确认入账（非伪造 HX711）

详见 `protocol.json` 与 `docs/`。

## 说明

- 不含车端固件、WiFi 凭据与私有网络配置。
- 本 fork 在官方 Lichtblick 之上叠加 `linggou/`；框架本身用法见仓库根 README。
