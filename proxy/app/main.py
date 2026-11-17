"""FastAPI 应用：代理配置、连接统计、维护模式、上传试传、网页。"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .engine import MAINT_MAX_GRACE, MAINT_MIN_GRACE, DelayProxy, ProxyConfig

STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_CHUNK = 256 * 1024


def _env_config() -> ProxyConfig:
    return ProxyConfig(
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("LISTEN_PORT", "7000")),
        target_host=os.getenv("TARGET_HOST", "127.0.0.1"),
        target_port=int(os.getenv("TARGET_PORT", "9000")),
        up_delay_ms=float(os.getenv("UP_DELAY_MS", "300")),
        down_delay_ms=float(os.getenv("DOWN_DELAY_MS", "300")),
    )


proxy = DelayProxy(_env_config())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await proxy.apply_config(proxy.cfg)  # 启动监听器
    yield
    await proxy.shutdown()


app = FastAPI(title="TCP Delay Proxy", lifespan=lifespan)


class ConfigIn(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = Field(ge=1, le=65535)
    target_host: str = Field(min_length=1)
    target_port: int = Field(ge=1, le=65535)
    up_delay_ms: float = Field(ge=0, le=3_600_000)
    down_delay_ms: float = Field(ge=0, le=3_600_000)


class MaintenanceIn(BaseModel):
    grace_s: float = Field(ge=MAINT_MIN_GRACE, le=MAINT_MAX_GRACE)


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def get_config():
    snap = proxy.snapshot()
    return {k: snap[k] for k in ("config", "listening", "listen_error", "accepting")}


@app.post("/api/config")
async def set_config(cfg: ConfigIn):
    # 维护期间允许改配置，但引擎会保持停收（accepting 不由本接口控制）
    await proxy.apply_config(ProxyConfig(**cfg.model_dump()))
    snap = proxy.snapshot()
    return {"ok": snap["listening"],
            **{k: snap[k] for k in ("config", "listening", "listen_error", "accepting")}}


@app.get("/api/connections")
async def connections():
    return proxy.snapshot()


@app.post("/api/history/clear")
async def clear_history():
    proxy.history.clear()
    return {"ok": True}


@app.post("/api/maintenance/start")
async def maintenance_start(body: MaintenanceIn):
    """进入维护：停收新 TCP 连接与新试传，等待在途连接排空。"""
    try:
        await proxy.start_maintenance(body.grace_s)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))
    return {"ok": True, "maintenance": proxy.snapshot()["maintenance"]}


@app.post("/api/maintenance/cancel")
async def maintenance_cancel():
    """期限未到取消维护并恢复接入；已有连接按原配置继续传输。"""
    m = await proxy.cancel_maintenance()
    if m is None:
        raise HTTPException(409, "no maintenance in progress")
    return {"ok": True, "maintenance": proxy.snapshot()["maintenance"]}


@app.post("/api/maintenance/resume")
async def maintenance_resume():
    """排空（或取消）后恢复接入。"""
    try:
        await proxy.resume_access()
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))
    snap = proxy.snapshot()
    return {"ok": True, "accepting": snap["accepting"],
            "maintenance": snap["maintenance"]}


async def _find_upload_conn(peer_port: int, tries: int = 30) -> int | None:
    """按本端临时端口把本次试传关联到代理连接 ID。"""
    suffix = f":{peer_port}"
    for _ in range(tries):
        for c in proxy.snapshot()["active"]:
            if c["client"].endswith(suffix):
                return c["id"]
        await asyncio.sleep(0.1)
    return None


async def _conn_final_state(conn_id: int, tries: int = 30) -> str | None:
    """连接的终态（closed/aborted/error）。读到 EOF 后复核用：
    维护中止对客户端表现为被动关闭的干净 EOF，必须以连接终态为准，
    不能把该 EOF 当作传输成功。"""
    for _ in range(tries):
        snap = proxy.snapshot()
        for c in snap["history"]:
            if c["id"] == conn_id:
                return c["state"]
        if any(c["id"] == conn_id for c in snap["active"]):
            await asyncio.sleep(0.1)  # 仍在清理，等它落账
            continue
        return None  # 活动与历史都没有：无法判定
    return None


@app.post("/api/test-upload")
async def test_upload(file: UploadFile):
    """把上传的文件经本代理转发到目标：先读欢迎行，发完文件后半关闭，
    再读回执直到 EOF。整条路径都经过延迟代理，等同真实设备链路。
    维护中拒绝新试传；在途试传关联到代理连接，被维护中止时返回失败。"""
    snap = proxy.snapshot()
    if not snap["accepting"]:
        m = proxy.maintenance
        if m is not None:
            m.refused_uploads += 1
        raise HTTPException(503, "maintenance in progress: new uploads refused")
    if not snap["listening"]:
        raise HTTPException(503, f"proxy listener not running: {snap['listen_error']}")
    cfg = proxy.cfg
    delay_s = (cfg.up_delay_ms + cfg.down_delay_ms) / 1000.0
    result: dict = {"file_name": file.filename, "bytes_sent": 0, "ok": False,
                    "conn_id": None}
    writer = None
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", cfg.listen_port), timeout=15)
        sock = writer.get_extra_info("sockname")  # 本端地址：代理看到的 client 端口
        if sock:
            result["conn_id"] = await _find_upload_conn(sock[1])
        # 目标（示例接收端）先发的欢迎行；自定义目标可能不发，探测失败不阻塞上传
        try:
            welcome = await asyncio.wait_for(reader.readline(),
                                             timeout=max(5.0, delay_s + 5))
            result["welcome"] = welcome.decode("utf-8", "replace").strip()
        except Exception:
            result["welcome"] = None
        while True:
            chunk = await file.read(UPLOAD_CHUNK)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
            result["bytes_sent"] += len(chunk)
        # 半关闭：目标读到 EOF 后才返回回执
        if writer.can_write_eof():
            writer.write_eof()
        else:
            writer.close()
        # 读回执直到 EOF；期间若本轮维护中止了本连接，立即以失败收场
        # （上行 EOF 不算成功：必须拿到完整回执）
        conn_id = result["conn_id"]
        deadline = time.monotonic() + max(120.0, 2 * delay_s + 60)
        parts: list[bytes] = []
        while True:
            if conn_id is not None:
                mobj = proxy.maintenance
                if mobj is not None:
                    hit = next((r for r in mobj.results if r.id == conn_id), None)
                    if hit is not None and hit.outcome == "aborted":
                        result["error"] = (f"maintenance aborted connection "
                                           f"#{conn_id}: {hit.detail}")
                        result["maintenance_outcome"] = "aborted"
                        return result
            try:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=0.5)
            except asyncio.TimeoutError:
                if time.monotonic() > deadline:
                    raise TimeoutError("timed out waiting for receipt")
                continue
            if not chunk:
                break  # 目标侧 EOF：回执读完
            parts.append(chunk)
        # EOF 复核：维护中止对客户端也是干净 EOF，须以连接终态为准，
        # 不能把中止造成的 EOF 当作传输成功
        if conn_id is not None:
            final = await _conn_final_state(conn_id)
            if final == "aborted":
                result["error"] = (f"maintenance aborted connection #{conn_id} "
                                   f"(EOF on abort is not a successful receipt)")
                result["maintenance_outcome"] = "aborted"
                return result
        result["receipt"] = b"".join(parts).decode("utf-8", "replace").strip()
        result["ok"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["duration_ms"] = round((time.monotonic() - t0) * 1000, 1)
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        await file.close()
    return result
