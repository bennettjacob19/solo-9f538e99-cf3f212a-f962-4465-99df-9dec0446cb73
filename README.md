# TCP 延迟代理（慢链路“上传结束收不到回执”复现工具）

带网页的 TCP 延迟代理：可配置目标地址与上下行延迟，网页上直接上传文件试传，
实时显示每条连接的回执、双向字节数与缓冲峰值。用于在本地复现慢链路下
“上传结束却收不到回执”的问题。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

* 网页 / API：http://localhost:8000
* TCP 延迟代理监听：`localhost:7000`（可直接对接设备或客户端）
* 示例接收端：容器网络内 `receiver:9000`（先发欢迎行，读到 EOF 后回
  `RECEIPT bytes=<n> sha256=<hex>`）

网页上可修改监听地址/端口、目标地址、上下行延迟（毫秒），保存后对**之后建立
的连接**生效；已建立的连接沿用建立时的配置。修改监听地址/端口会重启监听器。

## 复现“慢链路收不到回执”

1. 把上行/下行延迟调大（例如各 2000 ms）；
2. 在“上传试传”里选一个文件开始试传；
3. 在“活动连接”里观察：上行缓冲被 64 KiB 上限顶住、EOF 等待队列冲刷后才
   半关闭、回执（下行尾部）在延迟后才到达——上传“结束”与回执到达之间的
   时间差即慢链路效应。

## 关键语义（实现于 `proxy/app/engine.py`）

* **延迟起算**：每个数据块的投递时刻 = 代理读到它的时刻 + 延迟；队列按
  FIFO 投递，连续数据流水通过延迟线，不按读取块数累加等待。
* **64 KiB 应用缓冲**：每方向“已读未写”字节数上限 64 KiB，满则暂停该向
  读取（反向与别的连接不受影响）。
* **半关闭**：收到单向 EOF 后，先把该向待发字节全部写出，再 `write_eof`
  转发半关闭；反向继续传输直到结束。
* **断连释放**：正常结束或对端 RST 都会取消泵任务、清空队列、关闭两侧
  连接，并把连接移入历史表。
* **配置快照**：每条连接在 accept 时快照配置，整条连接沿用。

## 本地开发（不用 Docker）

```bash
pip install -r proxy/requirements.txt
PORT=19000 python3 receiver/server.py &
cd proxy && LISTEN_PORT=17000 TARGET_HOST=127.0.0.1 TARGET_PORT=19000 \
  UP_DELAY_MS=300 DOWN_DELAY_MS=300 \
  uvicorn app.main:app --host 127.0.0.1 --port 18000
```

## 自测

```bash
python3 tests/selftest.py   # 需按上面方式起好接收端与代理（端口见脚本头注释）
```

覆盖：回执字节数/SHA-256 校验、延迟流水化（非按块累加）、64 KiB 缓冲上限
与峰值、EOF 先冲刷再半关闭、连接级配置快照、并发上传、RST 断连释放。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/config` | 当前配置与监听状态 |
| POST | `/api/config` | 更新配置（仅影响新连接） |
| GET | `/api/connections` | 活动+历史连接：双向字节数、缓冲当前/峰值、回执尾部 |
| POST | `/api/test-upload` |  multipart 上传文件，经代理试传并返回回执 |
| POST | `/api/history/clear` | 清空历史连接 |

## 目录结构

```
docker-compose.yml      # proxy + receiver 一键启动
proxy/
  Dockerfile
  requirements.txt
  app/
    main.py             # FastAPI：配置/统计/试传/网页
    engine.py           # 延迟代理引擎（asyncio，无第三方依赖）
    static/index.html   # 网页
receiver/
  Dockerfile
  server.py             # 示例接收端：欢迎行 + EOF 后回执(字节数+SHA-256)
tests/selftest.py       # 端到端自测
```
