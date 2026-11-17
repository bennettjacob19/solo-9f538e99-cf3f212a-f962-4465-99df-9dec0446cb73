"""本地示例接收端（模拟设备/平台侧服务）：

1. 连接建立后立即发送一行欢迎语；
2. 持续读取直到对端 EOF（半关闭）；
3. 返回收到的字节数和 SHA-256 作为回执，然后关闭连接。
"""
import asyncio
import hashlib
import os

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "9000"))
WELCOME = b"WELCOME tcp-delay-demo receiver; send data then half-close (EOF) to get the receipt\n"


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    print(f"[receiver] connection from {peer}", flush=True)
    try:
        writer.write(WELCOME)
        await writer.drain()
        h = hashlib.sha256()
        total = 0
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break  # 对端 EOF
            h.update(chunk)
            total += len(chunk)
        receipt = f"RECEIPT bytes={total} sha256={h.hexdigest()}\n"
        writer.write(receipt.encode())
        await writer.drain()
        print(f"[receiver] {peer} -> {total} bytes, receipt sent", flush=True)
    except (ConnectionError, OSError) as exc:
        print(f"[receiver] {peer} error: {exc}", flush=True)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(handle, HOST, PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"[receiver] listening on {addrs}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
