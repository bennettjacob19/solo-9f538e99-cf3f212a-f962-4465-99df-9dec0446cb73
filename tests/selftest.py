#!/usr/bin/env python3
"""端到端自测：验证延迟代理的关键语义。

前置（见 README）：
  * 示例接收端监听 127.0.0.1:19000
  * 代理网页/API 在 127.0.0.1:18000，代理 TCP 监听 127.0.0.1:17000

运行：python3 tests/selftest.py
可用环境变量覆盖：WEB_URL / PROXY_HOST / PROXY_PORT / TARGET
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import struct
import sys
import time

import httpx

WEB = os.environ.get("WEB_URL", "http://127.0.0.1:18000")
PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "17000"))
TARGET_HOST, TARGET_PORT = os.environ.get("TARGET", "127.0.0.1:19000").rsplit(":", 1)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


async def set_cfg(client: httpx.AsyncClient, *, up: float, down: float):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
        "up_delay_ms": up, "down_delay_ms": down})
    r.raise_for_status()
    d = r.json()
    assert d["listening"], d
    return d


async def upload(client: httpx.AsyncClient, payload: bytes, name="blob.bin"):
    r = await client.post("/api/test-upload", files={"file": (name, payload)})
    r.raise_for_status()
    return r.json()


async def find_conn(client: httpx.AsyncClient, up_bytes: int, retries: int = 30):
    """按上行字节数精确定位一条连接（先历史后活动）。"""
    for _ in range(retries):
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if c["up"]["bytes"] == up_bytes:
                return c
        await asyncio.sleep(0.2)
    return None


async def main():
    async with httpx.AsyncClient(base_url=WEB, timeout=httpx.Timeout(300.0, connect=10.0)) as client:
        # 0. 服务起来且在监听
        d = (await client.get("/api/config")).json()
        check("web api up, proxy listening", d.get("listening") is True, str(d))

        await set_cfg(client, up=300, down=300)

        # 1. 上传 1 MiB：回执须带正确字节数与 SHA-256（隐含验证：EOF 在全部
        #    待发字节之后才转发，否则接收端统计不到完整大小）
        payload = os.urandom(1024 * 1024)
        sha = hashlib.sha256(payload).hexdigest()
        t0 = time.monotonic()
        res = await upload(client, payload)
        dt = time.monotonic() - t0
        check("upload ok", res.get("ok") is True, str(res)[:300])
        check("welcome line received", bool(res.get("welcome")), str(res.get("welcome")))
        receipt = res.get("receipt", "")
        check("receipt byte count matches", f"bytes={len(payload)}" in receipt, receipt)
        check("receipt sha256 matches", sha in receipt, receipt)
        # 流水化：1 MiB = 64 个 16KiB 块，若按块累加等待需 >= 64*0.3s ≈ 19s
        check("delay pipelined (not per-chunk)", dt < 8.0, f"{dt:.2f}s for 1 MiB @300ms")

        # 2. 统计：双向字节数、64 KiB 缓冲上限
        conn = await find_conn(client, len(payload))
        check("connection stats recorded", conn is not None)
        if conn:
            check("up bytes counted", conn["up"]["bytes"] == len(payload), str(conn["up"]))
            check("down bytes counted", conn["down"]["bytes"] > 0, str(conn["down"]))
            check("up buffer peak <= 64 KiB", conn["up"]["peak"] <= 65536, str(conn["up"]["peak"]))
            check("up buffer peak reaches 64 KiB", conn["up"]["peak"] == 65536,
                  str(conn["up"]["peak"]))
            check("receipt captured in stats", sha in conn.get("receipt", ""),
                  conn.get("receipt", "")[-80:])

        # 3. 每条连接沿用建立时的配置：连接中途改配置不影响它
        await set_cfg(client, up=1500, down=1500)
        reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        await asyncio.wait_for(reader.readline(), 30)  # 欢迎行
        data = b"snapshot-test-" + b"x" * 200
        writer.write(data)
        await writer.drain()
        await asyncio.sleep(0.3)  # 让数据进入延迟线
        await set_cfg(client, up=0, down=0)  # 连接中途改配置
        writer.write_eof()
        receipt_b = await asyncio.wait_for(reader.read(), 60)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        check("snapshot conn receipt ok", f"bytes={len(data)}".encode() in receipt_b,
              receipt_b.decode(errors="replace"))
        conn2 = await find_conn(client, len(data))
        check("connection kept establish-time config",
              conn2 is not None and conn2["up_delay_ms"] == 1500
              and conn2["down_delay_ms"] == 1500,
              f"up={conn2 and conn2['up_delay_ms']} down={conn2 and conn2['down_delay_ms']}")

        # 4. 并发上传互不影响
        await set_cfg(client, up=200, down=200)
        payloads = [os.urandom(256 * 1024) for _ in range(3)]
        results = await asyncio.gather(*(upload(client, p, f"c{i}.bin")
                                         for i, p in enumerate(payloads)))
        ok = all(r.get("ok") and hashlib.sha256(p).hexdigest() in r.get("receipt", "")
                 for r, p in zip(results, payloads))
        check("3 concurrent uploads, receipts correct", ok,
              str([r.get("receipt") for r in results]))

        # 5. 异常断连（RST）后释放队列与连接
        before = {c["id"] for c in (await client.get("/api/connections")).json()["history"]}

        def rst():
            s = socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=5)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.sendall(b"reset-me")
            s.close()  # 发 RST（在途数据可能被丢弃，故不按字节数识别该连接）
        await asyncio.to_thread(rst)
        rst_conn = None
        for _ in range(30):
            d = (await client.get("/api/connections")).json()
            for c in d["history"]:
                if c["id"] not in before:
                    rst_conn = c
                    break
            if rst_conn is not None:
                break
            await asyncio.sleep(0.2)
        check("reset connection moved to history", rst_conn is not None)
        if rst_conn is not None:
            active_ids = {c["id"] for c in (await client.get("/api/connections")).json()["active"]}
            check("reset connection released from active set", rst_conn["id"] not in active_ids,
                  f"state={rst_conn['state']} error={rst_conn['error']}")

        await set_cfg(client, up=300, down=300)  # 恢复默认

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
