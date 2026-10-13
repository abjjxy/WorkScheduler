import asyncio
import copy
import json
import struct
import uuid
import zlib
from logger import app_logger
from config import settings

MSG_HEARTBEAT = 0x01
MSG_HEARTBEAT_ACK = 0x02
MSG_ORDER_DELIVER = 0x03
MSG_ORDER_RESULT = 0x04
MSG_WORKER_OFFLINE = 0x05

HEADER_SIZE = 24


def pack_message(msg_type: int, body: dict) -> tuple[bytes, str]:
    payload = copy.copy(body)
    payload["msg_type"] = msg_type
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    msg_id = uuid.uuid4()
    crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF

    header = struct.pack(">I16sI", len(payload_bytes), msg_id.bytes, crc)
    return header + payload_bytes, msg_id.hex


async def unpack_message(reader: asyncio.StreamReader) -> tuple[int, dict, str] | None:
    header_data = await reader.readexactly(HEADER_SIZE)
    if not header_data:
        return None

    total_len, msg_id_bytes, crc_recv = struct.unpack(">I16sI", header_data)

    body_data = await reader.readexactly(total_len)

    crc_calc = zlib.crc32(body_data) & 0xFFFFFFFF
    if crc_calc != crc_recv:
        app_logger.warning("报文 CRC32 校验失败 msg_id=%s expected=%08x actual=%08x",
                           uuid.UUID(bytes=msg_id_bytes).hex, crc_recv, crc_calc)
        return None

    body = json.loads(body_data.decode("utf-8"))
    msg_type = body.pop("msg_type", 0)
    return msg_type, body, uuid.UUID(bytes=msg_id_bytes).hex


class WorkerTCPClient:
    def __init__(self, worker_id: str,
                 host=settings.SCHEDULER_CLIENT_HOST,
                 port=settings.SCHEDULER_CLIENT_PORT):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.running = False
        self._seen_msg_ids: set = set()
        self._dedup_dirty = False

    def _is_dup_local(self, msg_id: str) -> bool:
        if msg_id in self._seen_msg_ids:
            return True
        self._seen_msg_ids.add(msg_id)
        self._dedup_dirty = True
        return False

    async def _dedup_gc(self):
        """每分钟清理一次 msg_id 集合（防止内存泄漏）"""
        while self.running:
            await asyncio.sleep(60)
            if self._dedup_dirty and len(self._seen_msg_ids) > 1000:
                half = len(self._seen_msg_ids) // 2
                self._seen_msg_ids = set(list(self._seen_msg_ids)[half:])
                self._dedup_dirty = False

    async def connect(self) -> bool:
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            self.running = True
            app_logger.info("✅ Worker[%s] 成功连接调度中心 %s:%s", self.worker_id, self.host, self.port)
            return True
        except Exception as e:
            app_logger.warning("❌ Worker[%s] 连接失败: %s", self.worker_id, e)
            self.running = False
            return False

    async def heartbeat_task(self):
        while self.running:
            try:
                beat_msg, beat_id = pack_message(MSG_HEARTBEAT, {"worker_id": self.worker_id})
                self.writer.write(beat_msg)
                await self.writer.drain()
                app_logger.debug("[%s] 💓 Worker[%s] 发送心跳", beat_id[:8], self.worker_id)
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL)
            except Exception as e:
                app_logger.error("💔 Worker[%s] 心跳发送异常: %s", self.worker_id, e)
                self.running = False
                break

    async def message_recv_task(self):
        while self.running:
            try:
                unpack_res = await unpack_message(self.reader)
                if unpack_res is None:
                    break
                msg_type, body, msg_id = unpack_res

                if self._is_dup_local(msg_id):
                    app_logger.warning("[%s] Worker[%s] 收到重复消息，丢弃", msg_id[:8], self.worker_id)
                    continue

                if msg_type == MSG_HEARTBEAT_ACK:
                    continue

                elif msg_type == MSG_ORDER_DELIVER:
                    order_id = body["order_id"]
                    order_content = body["content"]
                    app_logger.info("[%s] 📥 Worker[%s] 收到工单 order_id=%s, content=%s",
                                    msg_id[:8], self.worker_id, order_id, order_content)

                    await asyncio.sleep(3)

                    result_msg, result_id = pack_message(
                        MSG_ORDER_RESULT,
                        {
                            "worker_id": self.worker_id,
                            "order_id": order_id,
                            "result": "success",
                            "msg": "工单执行完成"
                        }
                    )
                    self.writer.write(result_msg)
                    await self.writer.drain()
                    app_logger.info("[%s] 📤 Worker[%s] 工单%s处理完成，上报结果 → [%s]",
                                    msg_id[:8], self.worker_id, order_id, result_id[:8])

                else:
                    app_logger.warning("[%s] Worker[%s] 收到未知消息类型 %s",
                                       msg_id[:8], self.worker_id, msg_type)

            except asyncio.IncompleteReadError:
                app_logger.info("🔌 Worker[%s] 服务端关闭连接", self.worker_id)
                self.running = False
                break
            except Exception as e:
                app_logger.error("⚠️ Worker[%s] 接收消息异常: %s", self.worker_id, e)
                self.running = False
                break

    async def run(self):
        RECONNECT_DELAY = 3.0
        while True:
            connected = await self.connect()
            if connected:
                heartbeat_coro = asyncio.create_task(self.heartbeat_task())
                recv_coro = asyncio.create_task(self.message_recv_task())
                dedup_gc_coro = asyncio.create_task(self._dedup_gc())
                await asyncio.gather(heartbeat_coro, recv_coro, dedup_gc_coro)

                if self.writer:
                    self.writer.close()
                    await self.writer.wait_closed()
                app_logger.warning("Worker[%s] 连接断开，%.1fs 后重连", self.worker_id, RECONNECT_DELAY)

            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    import sys
    worker_id = sys.argv[1] if len(sys.argv) > 1 else settings.WORKER_NAME
    client = WorkerTCPClient(worker_id=worker_id)
    asyncio.run(client.run())