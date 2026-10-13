import asyncio
import copy
import json
import struct
import time
import uuid
import zlib
from typing import Dict, Optional, List
import aiomysql
import redis.asyncio as aioredis
from logger import app_logger
from config import settings
from rate_limiter import TokenBucketLimiter

"""
应用代码(FastAPI/aiohttp)
        ↓
HTTP/ASGI协议解析（uvicorn/aiohttp）
        ↓
asyncio事件循环（异步socket） / http.server（同步socket）
        ↓
Python socket API
        ↓
OS内核 TCP协议栈
        ↓
网卡
"""

MSG_HEARTBEAT = 0x01
MSG_HEARTBEAT_ACK = 0x02
MSG_ORDER_DELIVER = 0x03
MSG_ORDER_RESULT = 0x04
MSG_WORKER_OFFLINE = 0x05

#TCP 消息头设计：[4字节消息长度][16字节msg_id][4字节crc32校验]  = 24 字节
HEADER_SIZE = struct.calcsize(">I16sI")

worker_connections: Dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter, float]] = {}

mysql_pool: Optional[aiomysql.Pool] = None
redis_client: Optional[aioredis.Redis] = None


def pack_message(msg_type: int, body: dict) -> tuple[bytes, str]:
    payload = copy.copy(body)
    payload["msg_type"] = msg_type
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    msg_id = uuid.uuid4()
    '''
v1	暴露 MAC 地址；时钟回拨可能冲突
v3/v5	需要预先知道要哈希的 namespace + name；同一个 name 永远同一个值 → 不能每次生成新 ID
v4	✅ 纯随机，无外部依赖，不可预测
    '''
    crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF

#struct是 Python 内置库，用来按 C 语言二进制结构打包字节
#把 Python 对象按指定格式打包成固定长度的二进制
#`>` ：**大端序（网络字节序）**，网络传输统一用大端，高位字节放前面
#`I` ：unsigned int，**4 字节无符号整数**
    header = struct.pack(">I16sI", len(payload_bytes), msg_id.bytes, crc)
    return header + payload_bytes, msg_id.hex


async def unpack_message(reader: asyncio.StreamReader) -> Optional[tuple[int, dict, str]]:
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

# -------------------------- 数据库操作 --------------------------
async def init_db():
    global mysql_pool
    mysql_pool = await aiomysql.create_pool(**settings.mysql_config, autocommit=True)
    app_logger.info("MySQL 连接池初始化完成 (autocommit=True)")

async def db_get_wait_orders() -> List[dict]:
    """获取所有待派发工单"""
    async with mysql_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            #DictCursor 会自动将查询结果由元组格式转换为字典格式
            await cur.execute(
                "SELECT id,content,retry_count FROM work_order WHERE status=0 ORDER BY create_time ASC"
            )
            wait_orders = await cur.fetchall()
            return wait_orders
async def db_update_order_status(order_id: int, status: int, worker_id=None):
    """更新工单状态"""
    async with mysql_pool.acquire() as conn:
        async with conn.cursor() as cur:
            if worker_id:
                await cur.execute(
                    "UPDATE work_order SET status=%s,worker_id=%s WHERE id=%s",
                    (status, worker_id, order_id)
                )
            else:
                await cur.execute(
                    "UPDATE work_order SET status=%s WHERE id=%s",
                    (status, order_id)
                )
# -------------------------- Redis工人在线管理 & 分布式锁 --------------------------
#为实现跨实例共享工人在线状态，使用Redis集合存储
async def init_redis():
    global redis_client
    redis_client = await aioredis.from_url(settings.redis_url)
    app_logger.info("Redis 连接初始化完成")

async def redis_add_online_worker(worker_id: str):
    await redis_client.sadd("online_workers", worker_id)

async def redis_remove_online_worker(worker_id: str):
    await redis_client.srem("online_workers", worker_id)

async def redis_get_online_workers() -> list:
    members = await redis_client.smembers("online_workers")
    return [m.decode() for m in members]

async def try_order_lock(order_id: int) -> bool:
    """工单分布式锁，防止重复派发"""
    key = f"order_lock:{order_id}"
    return await redis_client.set(key, "locked", ex=30, nx=True)

async def release_order_lock(order_id: int):
    await redis_client.delete(f"order_lock:{order_id}")

async def is_duplicate_msg(msg_id: str) -> bool:
    """消息去重：返回 True 表示重复（已处理过），False 表示新消息"""
    key = f"msg_seen:{msg_id}"
    return await redis_client.set(key, "1", nx=True, ex=settings.MSG_DEDUP_TTL) is None
#返回True或者None

# 单个Worker连接处理协程
async def handle_worker_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    worker_id = None
    try:
        while True:
            unpack_res = await unpack_message(reader)
            if unpack_res is None:
                break
            msg_type, body, msg_id = unpack_res
            worker_id = body.get("worker_id")

            if await is_duplicate_msg(msg_id):
                app_logger.warning("[%s] 收到重复消息，丢弃", msg_id[:8])
                continue

            if msg_type == MSG_HEARTBEAT:
                now = time.time()
                worker_connections[worker_id] = (reader, writer, now)
                await redis_add_online_worker(worker_id)
                ack_msg, ack_id = pack_message(MSG_HEARTBEAT_ACK, {"worker_id": worker_id})
                writer.write(ack_msg)
                await writer.drain()
                app_logger.debug("[%s] 心跳 worker %s 在线 → ACK[%s]", msg_id[:8], worker_id, ack_id[:8])

            elif msg_type == MSG_ORDER_RESULT:
                order_id = body["order_id"]
                result = body["result"]
                app_logger.info("[%s] 工单完成 order=%s worker=%s result=%s",
                                msg_id[:8], order_id, worker_id, result)
                await db_update_order_status(order_id, 2, worker_id)
                await release_order_lock(order_id)
            else:
                app_logger.warning("[%s] 收到未知消息类型: %s", msg_id[:8], msg_type)

    except asyncio.IncompleteReadError:
        app_logger.info("worker %s 连接中断", worker_id)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        app_logger.error("worker %s 异常: %s", worker_id, e, exc_info=True)
    finally:
        remove_online = False
        if worker_id and worker_connections.get(worker_id, (None, None, None))[1] is writer:
            del worker_connections[worker_id]
            remove_online = True
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        if worker_id and remove_online and redis_client:
            try:
                await redis_remove_online_worker(worker_id)
            except Exception:
                pass
        app_logger.info("worker %s 连接已关闭", worker_id)

# 心跳检测后台协程：定时遍历，踢掉超时worker
async def heartbeat_check_task():
    HEARTBEAT_TIMEOUT = 12.0
    while True:
        await asyncio.sleep(2.0)
        now = time.time()
        offline_workers = []
        for wid, (r, w, last_ts) in worker_connections.items():
            if now - last_ts > HEARTBEAT_TIMEOUT:
                offline_workers.append(wid)
        for wid in offline_workers:
            if wid in worker_connections:
                del worker_connections[wid]
                await redis_remove_online_worker(wid)
                app_logger.warning("心跳超时 worker %s 标记离线，回收工单重派", wid)
                async with mysql_pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE work_order SET status=0,retry_count=retry_count+1 WHERE worker_id=%s AND status=1",
                            (wid,)
                        )

# 工单派发后台协程
async def order_dispatch_task():
    """轮询派发工单：空闲工人 + 分布式锁防重派"""
    while True:
        try:
            await asyncio.sleep(1)
            wait_orders = await db_get_wait_orders()
            if not wait_orders:
                continue

            online_workers = [
                wid for wid in await redis_get_online_workers()
                if wid in worker_connections
            ]
            if not online_workers:
                continue

            # 极简负载均衡：轮询派发
            for idx, order in enumerate(wait_orders):
                oid = order["id"]
                content = order["content"]
                rid = online_workers[idx % len(online_workers)]

                # 分布式锁防止重复派发
                if not await try_order_lock(oid):
                    continue

                # 拿到工人连接
                if rid not in worker_connections:
                    await release_order_lock(oid)
                    continue

                r, w, t = worker_connections[rid]
                # 下发工单
                deliver_msg, deliver_id = pack_message(MSG_ORDER_DELIVER, {
                    "order_id": oid,
                    "content": content
                })
                w.write(deliver_msg)
                await w.drain()

                await db_update_order_status(oid, 1, rid)
                app_logger.info("[%s] 📤 派发工单 %s 给工人 %s", deliver_id[:8], oid, rid)
        except Exception as e:
            app_logger.error("工单派发任务异常: %s", e, exc_info=True)
            await asyncio.sleep(1)


# 启动TCP服务
async def start_tcp_server():
    global mysql_pool, redis_client
    await init_db()
    await init_redis()
    app_logger.info("MySQL、Redis 连接初始化完成")

    conn_limiter = TokenBucketLimiter(redis_client)
    app_logger.info("TCP 令牌桶限流器已初始化: capacity=%d refill=%d/s (enabled=%s)",
                    settings.TCP_RATE_BUCKET_CAPACITY, settings.TCP_RATE_REFILL_RATE, settings.RATE_LIMIT_ENABLED)

    async def handle_with_limit(reader, writer):
        allowed = await conn_limiter.allow("tcp:connect")
        if not allowed:
            app_logger.warning("TCP 连接触发限流，拒绝新连接")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return
        await handle_worker_connection(reader, writer)

    server = await asyncio.start_server(
        handle_with_limit,
        settings.SCHEDULER_HOST, settings.SCHEDULER_PORT
    )
    addr = server.sockets[0].getsockname()
    app_logger.info("调度中心 TCP 服务启动，监听 %s", addr)

    hb_task = asyncio.create_task(heartbeat_check_task())
    dispatch_task = asyncio.create_task(order_dispatch_task())
    app_logger.info("后台任务已启动: heartbeat_check + order_dispatch")

    try:
        async with server:
            await server.serve_forever()
    finally:
        app_logger.info("调度中心开始关闭...")

        hb_task.cancel()
        dispatch_task.cancel()

        all_handlers = list(worker_connections.values())
        for _, w, _ in all_handlers:
            w.close()

        for t in (hb_task, dispatch_task):
            try:
                await t
            except asyncio.CancelledError:
                pass

        worker_connections.clear()

        await asyncio.sleep(0.2)

        if redis_client:
            try:
                await redis_client.aclose()
                redis_client = None
                app_logger.info("Redis 连接已关闭")
            except Exception as e:
                app_logger.warning("Redis 关闭异常: %s", e)

        if mysql_pool:
            mysql_pool.close()
            await mysql_pool.wait_closed()
            mysql_pool = None
            app_logger.info("MySQL 连接池已关闭")

if __name__ == "__main__":
    asyncio.run(start_tcp_server())