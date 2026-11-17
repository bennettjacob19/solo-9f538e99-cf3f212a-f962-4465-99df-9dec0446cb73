"""TCP 延迟代理引擎（纯 asyncio，无第三方依赖）。

实现的语义：
* 延迟从代理读到字节时起算：每个数据块获得各自的投递时刻
  (read_time + delay)，按 FIFO 顺序写出，连续数据流水通过延迟线，
  不会按读取块数累加等待。
* 每个方向的应用缓冲（已读未写）上限 64 KiB；缓冲满时只暂停该方向的
  读取，反向及本机其他连接照常传输。
* 连接建立时快照配置，整条连接沿用，之后改配置不影响它。
* 收到单向 EOF 后，先把该方向队列中待发字节全部写出，再转发半关闭
  (write_eof)；反向方向继续传输直到结束。
* 断连（正常结束或对端 RST）时释放队列并关闭两侧连接。
* 维护模式：进入维护即停收新 TCP 连接（运行配置 accepting=False，
  改配置接口无法重新打开）；已有连接按原配置继续传输，双向传输与
  资源清理全部完成才算排空（上行 EOF 不算排空）；宽限期（1~60 秒）
  到期中止剩余连接并释放其队列，清理完成后才报告排空，逐条区分
  正常结束与维护中断。维护按轮次（epoch）隔离：上一轮的期限与
  迟到结果不会中断连接，也不会覆盖新一轮的结果。
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from collections import deque
from dataclasses import dataclass, field

BUFFER_LIMIT = 64 * 1024   # 每方向应用缓冲上限
READ_CHUNK = 16 * 1024     # 单次 read() 最大字节数
TAIL_KEEP = 4096           # 下行数据尾部保留长度（回执预览）
HISTORY_MAX = 100          # 历史连接保留条数
CONNECT_TIMEOUT = 15.0     # 连接目标的超时
MAINT_MIN_GRACE = 1.0      # 维护宽限期下限（秒）
MAINT_MAX_GRACE = 60.0     # 维护宽限期上限（秒）


@dataclass
class ProxyConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 7000
    target_host: str = "127.0.0.1"
    target_port: int = 9000
    up_delay_ms: float = 300.0    # 客户端 -> 目标
    down_delay_ms: float = 300.0  # 目标 -> 客户端
    accepting: bool = True        # 是否接收新 TCP 连接（维护期间由引擎强制关闭）


@dataclass
class DirectionStats:
    bytes: int = 0    # 已读累计字节
    peak: int = 0     # 应用缓冲峰值
    queued: int = 0   # 当前缓冲字节


@dataclass
class ConnInfo:
    id: int
    client: str
    cfg: ProxyConfig  # 建立时的配置快照
    started_at: float = field(default_factory=time.time)
    ended_at: float | None = None
    state: str = "active"  # active | closed | aborted | error
    error: str | None = None
    saw_eof: bool = False  # 是否已见客户端上行 EOF（半关闭请求）
    up: DirectionStats = field(default_factory=DirectionStats)    # 客户端 -> 目标
    down: DirectionStats = field(default_factory=DirectionStats)  # 目标 -> 客户端
    downlink_tail: bytes = b""  # 下行数据尾部（回执预览）


@dataclass
class MaintConnResult:
    """维护轮次中一条连接的终态记录。"""
    id: int
    client: str
    outcome: str          # normal | aborted | error
    detail: str | None
    up_bytes: int
    down_bytes: int
    saw_eof: bool
    ended_at: float


class MaintenanceState:
    """一轮维护的状态。epoch 单调递增；只有当前轮次的定时器与结果有效。"""

    def __init__(self, epoch: int, grace_s: float):
        self.epoch = epoch
        self.grace_s = grace_s
        self.started_at = time.time()
        self.deadline = self.started_at + grace_s
        self._deadline_mono = time.monotonic() + grace_s
        self.state = "draining"      # draining | drained | cancelled
        self.ended_at: float | None = None
        self.initial = 0             # 进入维护时被跟踪的连接数
        self.refused_tcp = 0         # 维护期间被拒绝的新 TCP 连接数
        self.refused_uploads = 0     # 维护期间被拒绝的新试传数
        self.results: list[MaintConnResult] = []
        self._done: set[int] = set()
        self._event = asyncio.Event()

    def note_tracked(self, cid: int):
        if self.state == "draining":
            self.initial += 1

    def note_done(self, res: MaintConnResult):
        """连接清理完成后上报；非 draining 状态（已取消/已收口）的迟到结果丢弃。"""
        if self.state != "draining" or res.id in self._done:
            return
        self._done.add(res.id)
        self.results.append(res)
        self.maybe_drained()

    def maybe_drained(self):
        """全部被跟踪连接清理完成 -> 排空（含进入时本就没有连接的情况）。"""
        if self.state == "draining" and len(self._done) >= self.initial:
            self._finish("drained")

    def _finish(self, state: str):
        if self.state != "draining":
            return
        self.state = state
        self.ended_at = time.time()
        self._event.set()

    def cancel(self):
        self._finish("cancelled")

    def timeout_drained(self):
        """宽限期满且中止清理完成后收口为 drained。"""
        self._finish("drained")

    async def wait(self):
        await self._event.wait()

    @property
    def remaining(self) -> int:
        return max(0, self.initial - len(self._done))

    def snapshot(self) -> dict:
        return {
            "epoch": self.epoch,
            "state": self.state,
            "grace_s": self.grace_s,
            "started_at": self.started_at,
            "deadline": self.deadline,
            "remaining_s": (max(0.0, self._deadline_mono - time.monotonic())
                            if self.state == "draining" else 0.0),
            "ended_at": self.ended_at,
            "initial": self.initial,
            "remaining": self.remaining,
            "refused_tcp": self.refused_tcp,
            "refused_uploads": self.refused_uploads,
            "results": [dataclasses.asdict(r) for r in self.results],
        }


class DirectionPump:
    """单向延迟管道：从 reader 读，延迟后写入 writer。"""

    def __init__(self, name, reader, writer, delay_s, stats, on_data=None, on_eof=None):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.delay = delay_s
        self.stats = stats
        self.on_data = on_data  # 写出后的回调（用于抓回执）
        self.on_eof = on_eof    # 读到 EOF 时的回调（仅标记，不算排空）
        self._queue: deque[tuple[float, bytes | None]] = deque()  # None = EOF 哨兵
        self._queued = 0
        self._space = asyncio.Event()   # 缓冲未满
        self._space.set()
        self._items = asyncio.Event()   # 队列非空

    async def run(self):
        rx = asyncio.create_task(self._read_loop(), name=f"pump-{self.name}-rx")
        tx = asyncio.create_task(self._write_loop(), name=f"pump-{self.name}-tx")
        try:
            await asyncio.gather(rx, tx)
        finally:
            for t in (rx, tx):
                t.cancel()
            await asyncio.gather(rx, tx, return_exceptions=True)
            # 断连/结束：释放队列
            self._queue.clear()
            self._queued = 0
            self.stats.queued = 0

    async def _read_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            await self._space.wait()  # 缓冲满则暂停该向读取（反向不受影响）
            max_read = min(READ_CHUNK, BUFFER_LIMIT - self._queued)
            chunk = await self.reader.read(max_read)
            ready_at = loop.time() + self.delay  # 延迟自读到字节时起算
            if chunk:
                self._queue.append((ready_at, chunk))
                self._queued += len(chunk)
                self.stats.bytes += len(chunk)
                self.stats.queued = self._queued
                if self._queued > self.stats.peak:
                    self.stats.peak = self._queued
                if self._queued >= BUFFER_LIMIT:
                    self._space.clear()
                self._items.set()
            else:
                # EOF 哨兵入队：半关闭只在该向待发字节全部写出后转发
                self._queue.append((ready_at, None))
                self._items.set()
                if self.on_eof is not None:
                    self.on_eof()
                return

    async def _write_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            while not self._queue:
                self._items.clear()
                await self._items.wait()
            ready_at, chunk = self._queue[0]
            wait = ready_at - loop.time()
            if wait > 0:
                # 只等到队首块到期；后续块各自带投递时刻，流水发送
                await asyncio.sleep(wait)
            self._queue.popleft()
            if chunk is None:
                # 该向待发字节已全部写出 -> 转发半关闭
                try:
                    if self.writer.can_write_eof():
                        self.writer.write_eof()
                        await self.writer.drain()
                    else:
                        self.writer.close()
                except (ConnectionError, OSError, RuntimeError):
                    pass
                return
            self.writer.write(chunk)
            await self.writer.drain()
            self._queued -= len(chunk)
            self.stats.queued = self._queued
            if self.on_data is not None:
                self.on_data(chunk)
            if self._queued < BUFFER_LIMIT:
                self._space.set()


class Connection:
    """一条代理连接：客户端 <-> 两个 DirectionPump <-> 目标。"""

    def __init__(self, client_reader, client_writer, cfg: ProxyConfig, conn_id: int):
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.target_reader = None
        self.target_writer = None
        peer = client_writer.get_extra_info("peername")
        client = f"{peer[0]}:{peer[1]}" if peer else "?"
        self.info = ConnInfo(id=conn_id, client=client, cfg=dataclasses.replace(cfg))
        self._abort = False
        self._abort_reason = "maintenance timeout"
        self._task: asyncio.Task | None = None

    def _capture_downlink(self, data: bytes):
        tail = self.info.downlink_tail + data
        self.info.downlink_tail = tail[-TAIL_KEEP:]

    def _note_eof(self):
        # 仅记录“客户端已半关闭”；排空以整条连接清理完成为准
        self.info.saw_eof = True

    def abort(self, reason: str = "maintenance timeout"):
        """维护中止：取消连接任务，走正常清理路径（释放队列、关闭两侧）。"""
        if self.info.ended_at is not None:
            return  # 已结束，迟到中止无效
        self._abort = True
        self._abort_reason = reason
        t = self._task
        if t is not None:
            t.cancel()

    @property
    def end_reason(self) -> str:
        """任务结束后的归类：aborted | error | normal。"""
        if self._abort:
            return "aborted"
        if self.info.state == "error":
            return "error"
        return "normal"

    async def run(self):
        self._task = asyncio.current_task()
        try:
            await self._run()
        finally:
            # 兜底：即使清理途中被取消，也保证终态与结束时间落账
            info = self.info
            if self._abort:
                info.state = "aborted"
                if not info.error:
                    info.error = self._abort_reason
            elif info.state == "active":
                info.state = "closed"
            if info.ended_at is None:
                info.ended_at = time.time()

    async def _run(self):
        info = self.info
        cfg = info.cfg
        if self._abort:
            # 中止到达时任务尚未启动：直接走清理
            self._close(self.client_writer)
            await self._wait_closed(self.client_writer)
            return
        try:
            self.target_reader, self.target_writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.target_host, cfg.target_port),
                timeout=CONNECT_TIMEOUT,
            )
        except Exception as exc:
            info.state = "error"
            info.error = f"connect target {cfg.target_host}:{cfg.target_port} failed: {exc}"
            self._close(self.client_writer)
            await self._wait_closed(self.client_writer)
            info.ended_at = time.time()
            return

        up = DirectionPump("up", self.client_reader, self.target_writer,
                           cfg.up_delay_ms / 1000.0, info.up,
                           on_eof=self._note_eof)
        down = DirectionPump("down", self.target_reader, self.client_writer,
                             cfg.down_delay_ms / 1000.0, info.down,
                             on_data=self._capture_downlink)
        tasks = [asyncio.create_task(up.run()), asyncio.create_task(down.run())]
        try:
            await asyncio.gather(*tasks)
        except Exception as exc:
            info.state = "error"
            info.error = f"{type(exc).__name__}: {exc}"
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # 断连：关闭两侧连接（队列已在 pump 内释放）
            self._close(self.client_writer)
            self._close(self.target_writer)
            await self._wait_closed(self.client_writer)
            await self._wait_closed(self.target_writer)
            if self._abort:
                info.state = "aborted"
                if not info.error:
                    info.error = self._abort_reason
            elif info.state == "active":
                info.state = "closed"
            info.ended_at = time.time()

    @staticmethod
    def _close(writer):
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

    @staticmethod
    async def _wait_closed(writer):
        if writer is not None:
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception:
                pass


class DelayProxy:
    """监听 TCP 端口，为每条连接建立到目标的延迟管道。"""

    def __init__(self, cfg: ProxyConfig):
        self.cfg = cfg
        self.server: asyncio.Server | None = None
        self.listen_error: str | None = None
        self.active: dict[int, Connection] = {}
        self.history: deque[ConnInfo] = deque(maxlen=HISTORY_MAX)
        self.maintenance: MaintenanceState | None = None
        self._next_id = 1
        self._epoch = 0
        self._accept_closed = False   # 维护强制停收；改配置无法重新打开
        self._tasks: set[asyncio.Task] = set()
        self._maint_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    @property
    def listening(self) -> bool:
        return self.server is not None and self.server.is_serving()

    async def apply_config(self, new_cfg: ProxyConfig):
        """应用新配置；仅影响之后建立的连接。监听地址/端口变化时重启监听器。
        accepting 由维护流程控制，这里不允许借改配置绕过停收。"""
        async with self._lock:
            restart = (
                self.server is None
                or new_cfg.listen_host != self.cfg.listen_host
                or new_cfg.listen_port != self.cfg.listen_port
            )
            self.cfg = dataclasses.replace(new_cfg)
            if self._accept_closed:
                self.cfg.accepting = False
            if not restart:
                return
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()
                self.server = None
            self.listen_error = None
            try:
                self.server = await asyncio.start_server(
                    self._on_client, new_cfg.listen_host, new_cfg.listen_port)
            except Exception as exc:
                self.listen_error = str(exc)

    # ---- 维护模式 ----

    async def start_maintenance(self, grace_s: float) -> MaintenanceState:
        """进入维护：停收新连接，跟踪现有连接直到排空或宽限期满。"""
        if not (MAINT_MIN_GRACE <= grace_s <= MAINT_MAX_GRACE):
            raise ValueError(
                f"grace_s must be within {MAINT_MIN_GRACE:g}~{MAINT_MAX_GRACE:g}s")
        async with self._lock:
            if self.maintenance is not None and self.maintenance.state == "draining":
                raise RuntimeError("maintenance already in progress")
            # 新一轮 epoch：旧轮次的期限与结果自此失效
            self._epoch += 1
            m = MaintenanceState(self._epoch, float(grace_s))
            self.maintenance = m
            self._accept_closed = True
            self.cfg.accepting = False
            for cid in self.active:
                m.note_tracked(cid)
            m.maybe_drained()  # 进入时本就没有在管连接 -> 立即排空
            self._maint_task = asyncio.create_task(
                self._watchdog(m), name=f"maint-{m.epoch}")
            return m

    async def cancel_maintenance(self) -> MaintenanceState | None:
        """期限未到取消维护：恢复接入，已有连接按原配置继续传输。"""
        async with self._lock:
            m = self.maintenance
            if m is None or m.state != "draining":
                return None
            m.cancel()
            self._accept_closed = False
            self.cfg.accepting = True
            return m

    async def resume_access(self) -> bool:
        """排空（或取消）后恢复接入。"""
        async with self._lock:
            m = self.maintenance
            if m is not None and m.state == "draining":
                raise RuntimeError("maintenance still draining; cancel it first")
            self._accept_closed = False
            self.cfg.accepting = True
            return True

    async def _watchdog(self, m: MaintenanceState):
        """宽限期看门狗：期满中止剩余连接，待其清理完成后收口本轮。"""
        try:
            await asyncio.wait_for(
                m.wait(), timeout=max(0.0, m.deadline - time.time()))
        except asyncio.TimeoutError:
            pass
        # 只认当前轮次：已取消/已被新一轮替换的旧期限不得再中断连接
        if m.state != "draining" or self.maintenance is not m:
            return
        for conn in list(self.active.values()):
            conn.abort(f"maintenance timeout after {m.grace_s:g}s")
        # 等被中止连接完成清理（释放队列、关闭两侧），才报告排空
        tasks = [t for t in list(self._tasks) if not t.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        m.timeout_drained()

    # ---- 连接生命周期 ----

    def _on_client(self, reader, writer):
        if not self.cfg.accepting:
            # 维护中：拒绝新 TCP 连接
            m = self.maintenance
            if m is not None:
                m.refused_tcp += 1
            try:
                writer.close()
            except Exception:
                pass
            return
        cfg = dataclasses.replace(self.cfg)  # 建立时快照配置
        conn = Connection(reader, writer, cfg, self._next_id)
        self._next_id += 1
        cid = conn.info.id
        self.active[cid] = conn
        task = asyncio.create_task(conn.run(), name=f"conn-{cid}")
        self._tasks.add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self._finish(cid)))

    def _finish(self, cid: int):
        conn = self.active.pop(cid, None)
        if conn is None:
            return
        self.history.appendleft(conn.info)
        m = self.maintenance
        if m is not None:
            outcome = conn.end_reason
            if outcome == "aborted":
                detail = conn.info.error or "maintenance abort"
            elif outcome == "error":
                detail = conn.info.error
            else:
                detail = ("clean close after client EOF" if conn.info.saw_eof
                          else "clean close (no client EOF)")
            m.note_done(MaintConnResult(
                id=conn.info.id, client=conn.info.client, outcome=outcome,
                detail=detail, up_bytes=conn.info.up.bytes,
                down_bytes=conn.info.down.bytes, saw_eof=conn.info.saw_eof,
                ended_at=conn.info.ended_at or time.time()))

    async def shutdown(self):
        if self._maint_task is not None:
            self._maint_task.cancel()
            await asyncio.gather(self._maint_task, return_exceptions=True)
            self._maint_task = None
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def snapshot(self) -> dict:
        def conn_dict(info: ConnInfo) -> dict:
            return {
                "id": info.id,
                "client": info.client,
                "state": info.state,
                "error": info.error,
                "saw_eof": info.saw_eof,
                "target": f"{info.cfg.target_host}:{info.cfg.target_port}",
                "up_delay_ms": info.cfg.up_delay_ms,
                "down_delay_ms": info.cfg.down_delay_ms,
                "started_at": info.started_at,
                "ended_at": info.ended_at,
                "up": dataclasses.asdict(info.up),
                "down": dataclasses.asdict(info.down),
                "receipt": info.downlink_tail.decode("utf-8", "replace"),
            }

        return {
            "listening": self.listening,
            "listen_error": self.listen_error,
            "accepting": self.cfg.accepting,
            "config": dataclasses.asdict(self.cfg),
            "maintenance": self.maintenance.snapshot() if self.maintenance else None,
            "active": [conn_dict(c.info) for c in self.active.values()],
            "history": [conn_dict(i) for i in self.history],
        }
