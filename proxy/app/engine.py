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


@dataclass
class ProxyConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 7000
    target_host: str = "127.0.0.1"
    target_port: int = 9000
    up_delay_ms: float = 300.0    # 客户端 -> 目标
    down_delay_ms: float = 300.0  # 目标 -> 客户端


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
    state: str = "active"  # active | closed | error
    error: str | None = None
    up: DirectionStats = field(default_factory=DirectionStats)    # 客户端 -> 目标
    down: DirectionStats = field(default_factory=DirectionStats)  # 目标 -> 客户端
    downlink_tail: bytes = b""  # 下行数据尾部（回执预览）


class DirectionPump:
    """单向延迟管道：从 reader 读，延迟后写入 writer。"""

    def __init__(self, name, reader, writer, delay_s, stats, on_data=None):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.delay = delay_s
        self.stats = stats
        self.on_data = on_data  # 写出后的回调（用于抓回执）
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

    def _capture_downlink(self, data: bytes):
        tail = self.info.downlink_tail + data
        self.info.downlink_tail = tail[-TAIL_KEEP:]

    async def run(self):
        info = self.info
        cfg = info.cfg
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
                           cfg.up_delay_ms / 1000.0, info.up)
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
            if info.state == "active":
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
        self._next_id = 1
        self._tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()

    @property
    def listening(self) -> bool:
        return self.server is not None and self.server.is_serving()

    async def apply_config(self, new_cfg: ProxyConfig):
        """应用新配置；仅影响之后建立的连接。监听地址/端口变化时重启监听器。"""
        async with self._lock:
            restart = (
                self.server is None
                or new_cfg.listen_host != self.cfg.listen_host
                or new_cfg.listen_port != self.cfg.listen_port
            )
            self.cfg = dataclasses.replace(new_cfg)
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

    def _on_client(self, reader, writer):
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
        if conn is not None:
            self.history.appendleft(conn.info)

    async def shutdown(self):
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
            "config": dataclasses.asdict(self.cfg),
            "active": [conn_dict(c.info) for c in self.active.values()],
            "history": [conn_dict(i) for i in self.history],
        }
