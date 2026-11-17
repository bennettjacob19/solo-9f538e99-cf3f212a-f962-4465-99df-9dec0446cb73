#!/usr/bin/env python3
"""端到端自测：验证延迟代理的关键语义与维护模式。

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


async def wait_maint_state(client: httpx.AsyncClient, state: str, timeout: float = 20.0):
    """轮询直到本轮维护进入指定状态。"""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        d = (await client.get("/api/connections")).json()
        m = d.get("maintenance")
        if m and m["state"] == state:
            return m
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

        # 6. 维护：正常排空（在途连接完成后 drained，逐条结果 normal）
        await set_cfg(client, up=800, down=800)
        rdr6, wtr6 = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        await asyncio.wait_for(rdr6.readline(), 30)  # 欢迎行
        payload6 = b"maint-drain-" + b"y" * 200
        wtr6.write(payload6)
        await wtr6.drain()
        await asyncio.sleep(0.2)
        r = await client.post("/api/maintenance/start", json={"grace_s": 30})
        check("maintenance start ok", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
        d = (await client.get("/api/connections")).json()
        m = d["maintenance"]
        check("maintenance draining, accepting off",
              d["accepting"] is False and m is not None
              and m["state"] == "draining" and m["remaining"] >= 1,
              f"accepting={d['accepting']} remaining={m and m['remaining']}")

        # 6a. 新 TCP 连接被拒绝（对端读到干净 EOF，收不到欢迎行）
        refused_r, refused_w = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        got = b"?"
        try:
            refused_w.write(b"should-be-refused")
            await refused_w.drain()
            got = await asyncio.wait_for(refused_r.read(100), 5)
        except ConnectionError:
            got = b""
        finally:
            refused_w.close()
        check("new TCP conn refused during maintenance", got == b"", repr(got))

        # 6b. 改配置不能绕过停收
        await set_cfg(client, up=800, down=800)
        d = (await client.get("/api/connections")).json()
        check("config change cannot bypass stop-accepting", d["accepting"] is False)

        # 6c. 新试传被拒绝（503）
        r = await client.post("/api/test-upload", files={"file": ("x.bin", b"zzz")})
        check("new upload refused during maintenance", r.status_code == 503, str(r.status_code))

        # 6d. 在途连接完成（EOF -> 回执），维护报告排空且该连接结果 normal
        wtr6.write_eof()
        receipt6 = await asyncio.wait_for(rdr6.read(), 60)
        wtr6.close()
        try:
            await wtr6.wait_closed()
        except Exception:
            pass
        check("in-flight conn receipt ok during drain",
              f"bytes={len(payload6)}".encode() in receipt6, receipt6.decode(errors="replace"))
        m = await wait_maint_state(client, "drained")
        check("maintenance drained after in-flight conn cleanup", m is not None, str(m)[:200])
        if m:
            res = [x for x in m["results"] if x["up_bytes"] == len(payload6)]
            check("drained conn recorded as normal (saw EOF)",
                  len(res) == 1 and res[0]["outcome"] == "normal" and res[0]["saw_eof"],
                  str(m["results"]))
        d = (await client.get("/api/connections")).json()
        check("still not accepting after drain (manual resume)", d["accepting"] is False)
        r = await client.post("/api/maintenance/resume")
        d = (await client.get("/api/connections")).json()
        check("resume restores accepting", r.status_code == 200 and d["accepting"] is True)

        # 7. 维护超时：中止剩余连接；在途试传（已发上行 EOF）关联连接并失败
        await set_cfg(client, up=2000, down=2000)
        payload7 = os.urandom(40 * 1024)  # < 64 KiB：快速发完并半关闭，数据滞留延迟线
        up_task = asyncio.create_task(upload(client, payload7, "maint-abort.bin"))
        up_conn = None
        for _ in range(50):
            d = (await client.get("/api/connections")).json()
            cands = [c for c in d["active"]
                     if c["up"]["bytes"] >= len(payload7) and c["saw_eof"]]
            if cands:
                up_conn = cands[0]
                break
            await asyncio.sleep(0.1)
        check("in-flight upload conn found (EOF seen, data in flight)",
              up_conn is not None, str(up_conn and up_conn["id"]))
        r = await client.post("/api/maintenance/start", json={"grace_s": 1})
        check("maintenance start (1s grace)", r.status_code == 200)
        res7 = await up_task  # 上传须失败：维护中止，不能把已发 EOF 当成功
        check("in-flight upload failed by maintenance abort",
              res7.get("ok") is False and "maintenance" in str(res7.get("error", "")),
              str(res7)[:300])
        check("upload result linked to its proxy connection",
              res7.get("conn_id") is not None and up_conn is not None
              and res7["conn_id"] == up_conn["id"],
              f"conn_id={res7.get('conn_id')} expect={up_conn and up_conn['id']}")
        m = await wait_maint_state(client, "drained")
        check("maintenance drained after abort cleanup", m is not None)
        if m and up_conn:
            res = [x for x in m["results"] if x["id"] == up_conn["id"]]
            check("aborted conn recorded as maintenance-abort (EOF not success)",
                  len(res) == 1 and res[0]["outcome"] == "aborted" and res[0]["saw_eof"],
                  str(m["results"]))
        await client.post("/api/maintenance/resume")

        # 8. 取消维护：恢复接入，原连接继续；旧期限与迟到结果不影响下一轮
        await set_cfg(client, up=1000, down=1000)
        rdr8, wtr8 = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        await asyncio.wait_for(rdr8.readline(), 30)
        payload8 = b"cancel-continue-" + b"z" * 100
        wtr8.write(payload8)
        await wtr8.drain()
        await asyncio.sleep(0.2)
        await client.post("/api/maintenance/start", json={"grace_s": 2})
        await asyncio.sleep(0.3)
        r = await client.post("/api/maintenance/cancel")
        check("cancel maintenance ok", r.status_code == 200)
        d = (await client.get("/api/connections")).json()
        check("accepting restored after cancel",
              d["accepting"] is True and d["maintenance"] is not None
              and d["maintenance"]["state"] == "cancelled",
              f"accepting={d['accepting']} state={d['maintenance'] and d['maintenance']['state']}")
        epoch_cancelled = d["maintenance"]["epoch"]
        await asyncio.sleep(2.2)  # 越过旧期限：旧定时器不得再中止连接
        d = (await client.get("/api/connections")).json()
        still = [c for c in d["active"] if c["up"]["bytes"] == len(payload8)]
        check("old deadline does not abort conn after cancel", len(still) == 1,
              f"active matching={len(still)}")
        wtr8.write_eof()  # 原连接按原配置继续：补 EOF，拿到回执
        receipt8 = await asyncio.wait_for(rdr8.read(), 60)
        wtr8.close()
        try:
            await wtr8.wait_closed()
        except Exception:
            pass
        check("cancelled-round conn completes normally",
              f"bytes={len(payload8)}".encode() in receipt8, receipt8.decode(errors="replace"))
        await asyncio.sleep(0.5)  # 等连接清理（其迟到结果应被旧轮次丢弃）
        r = await client.post("/api/maintenance/start", json={"grace_s": 30})
        d = (await client.get("/api/connections")).json()
        m = d["maintenance"]
        check("new round epoch increments, late results do not leak in",
              m["epoch"] == epoch_cancelled + 1 and m["results"] == [],
              f"epoch={m['epoch']} results={m['results']}")
        m = await wait_maint_state(client, "drained", timeout=5)
        check("empty maintenance drains immediately",
              m is not None and m["remaining"] == 0, str(m)[:200])
        await client.post("/api/maintenance/resume")
        res8 = await upload(client, b"post-maintenance", "after.bin")
        check("upload works after resume", res8.get("ok") is True, str(res8)[:200])

        await set_cfg(client, up=300, down=300)  # 恢复默认

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
