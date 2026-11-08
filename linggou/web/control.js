"use strict";
const buttons = Array.from(document.querySelectorAll("[data-cmd]"));
const statusText = document.getElementById("status");
const details = document.getElementById("details");
const diagnostic = document.getElementById("diagnostic");
const wheelTable = document.getElementById("wheels");
const speedSelect = document.getElementById("speed");
const speedInfo = document.getElementById("speed-info");
const reasons = ["启动待命", "松开停车", "指令超时停车", "链路中断停车", "主循环超时停车", "等待新按键", "运行中"];
const wheelNames = ["右前", "左前", "右后", "左后"];
const names = {STOP:"停车", FWD:"前进", BACK:"后退", LEFT:"左平移", RIGHT:"右平移", CCW:"原地左转（逆时针）", CW:"原地右转（顺时针）"};
let ws = null, held = "stop", activeButton = null, pointer = null;
let ready = false, generation = -1;
let speedSupported = false;

function send() {
    if (ws && ws.readyState === WebSocket.OPEN) {
        // A slow connection must not accumulate old movement commands.
        if (ws.bufferedAmount > 1024) { ws.close(); return; }
        ws.send(JSON.stringify({cmd: held, generation}));
    }
}
function stop() {
    held = "stop";
    if (activeButton) activeButton.classList.remove("active");
    activeButton = null; pointer = null;
    speedSelect.disabled = !ready || !speedSupported;
    send();
}
speedSelect.addEventListener("change", () => {
    const value = speedSelect.value;
    stop();
    if (ready && speedSupported && ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({cmd:"set_speed", speed:value, generation}));
    }
});
for (const button of buttons) {
    button.addEventListener("pointerdown", event => {
        if (!ready || event.button !== 0 || pointer !== null) return;
        event.preventDefault();
        button.setPointerCapture(event.pointerId);
        pointer = event.pointerId; activeButton = button;
        held = button.dataset.cmd;
        speedSelect.disabled = true;
        button.classList.add("active");
        send();
    });
    button.addEventListener("pointerup", event => { if (event.pointerId === pointer) stop(); });
    button.addEventListener("pointercancel", stop);
    button.addEventListener("lostpointercapture", stop);
    button.addEventListener("contextmenu", event => event.preventDefault());
}
document.getElementById("stop").addEventListener("pointerdown", stop);
window.addEventListener("pointerup", event => { if (event.pointerId === pointer) stop(); });
window.addEventListener("blur", stop);
window.addEventListener("pagehide", stop);
document.addEventListener("visibilitychange", () => { if (document.hidden) stop(); });

function connect() {
    ready = false; stop();
    ws = new WebSocket(`ws://${location.host}/ws`);
    ws.onmessage = event => {
        const state = JSON.parse(event.data);
        if (generation !== state.generation || !state.online) stop();
        generation = state.generation;
        ready = state.online;
        speedSupported = state.speed_supported === true;
        speedSelect.value = state.selected_speed === "fast" ? "fast" : "normal";
        speedSelect.disabled = !ready || !speedSupported || held !== "stop";
        speedInfo.textContent = speedSelect.value === "fast"
            ? "高速档：前后/平移 0.30m/s，原地转 0.525rad/s。"
            : "标准档：前后/平移 0.20m/s，原地转 0.35rad/s。";
        if (ready && !speedSupported) speedInfo.textContent += " 当前主板未启用换档。";
        for (const button of buttons) button.disabled = !ready;
        statusText.textContent = ready ? (state.fresh ? "小车已就绪 · 按住按钮开始" : "状态回传延迟") : (state.connected ? "小车已连接 · 正在确认协议" : "控制器已就绪 · 等待小车连接");
        details.textContent = ready ? `主板目标动作：${names[state.reported]} ｜ 回传距今 ${state.report_age_ms ?? "—"}ms` : "等待 RC2 主板确认";
        diagnostic.textContent = `主板：${reasons[state.stop_reason] ?? "等待诊断"} ｜ 指令年龄 ${state.command_age_ms ?? "—"}ms ｜ 有效帧 ${state.command_rx ?? 0} ｜ 超时 ${state.mcu_timeouts ?? 0} 次`;
        wheelTable.innerHTML = (state.wheels ?? [null,null,null,null]).map((w,i) => {
            const fresh = w && w.age_ms < 800;
            return `<tr><td>${wheelNames[i]}</td><td>${fresh ? w.target_rpm.toFixed(1) : "—"}</td><td>${fresh ? w.actual_rpm.toFixed(1) : "—"}</td><td>${fresh ? (100*w.pwm/11999).toFixed(0) : "—"}</td></tr>`;
        }).join("");
    };
    ws.onclose = event => {
        ready = false; held = "stop";
        speedSelect.value = "normal"; speedSelect.disabled = true;
        if (activeButton) activeButton.classList.remove("active");
        activeButton = null; pointer = null;
        for (const button of buttons) button.disabled = true;
        details.textContent = "小车会在指令超时后撤销输出";
        statusText.textContent = event.code === 1008 ? "另一个遥控页面正在使用，请关闭多余页面" : "控制器连接中断 · 正在重连";
        if (event.code !== 1008) setTimeout(connect, 1000);
    };
    ws.onerror = () => ws.close();
}
setInterval(() => { if (ready && held !== "stop") send(); }, 50);
connect();
