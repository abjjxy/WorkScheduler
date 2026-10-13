import asyncio
import socket
import sys
import time

import aiomysql
import redis.asyncio as aioredis
import aiohttp
from aiohttp import web

from config import settings
from scheduler_tcp import start_tcp_server
from http_api import create_app
from worker_client import WorkerTCPClient

TEST_HTTP_URL = f"http://{settings.HTTP_HOST}:{settings.HTTP_PORT}"


async def _cleanup_db():
    pool = await aiomysql.create_pool(**settings.mysql_config, autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM work_order")
            await cur.execute("ALTER TABLE work_order AUTO_INCREMENT = 1")
    pool.close()
    await pool.wait_closed()
    print("  🧹 MySQL 已清空")


async def _cleanup_redis():
    r = aioredis.from_url(settings.redis_url)
    await r.flushdb()
    await r.aclose()
    print("  🧹 Redis 已清空")


async def cleanup_all():
    await _cleanup_db()
    await _cleanup_redis()


async def _wait_port(host: str, port: int, timeout: float = 6.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket()
        try:
            s.settimeout(0.5)
            s.connect((host, port))
            s.close()
            return True
        except (ConnectionRefusedError, OSError, socket.timeout):
            try:
                s.close()
            except Exception:
                pass
            await asyncio.sleep(0.2)
    return False


async def run_tests():
    print("\n" + "=" * 60)
    print("  🧪 开始测试用例")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:

        # -------- 用例 1：提交 3 个工单 --------
        print("\n📝 [用例1] 批量提交 3 个工单")
        order_ids = []
        contents = ["修空调", "换灯泡", "修水管"]
        for content in contents:
            async with session.post(
                f"{TEST_HTTP_URL}/api/work_order",
                json={"content": content},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                assert resp.status == 200, f"HTTP {resp.status}: {await resp.text()}"
                data = await resp.json()
            assert data["code"] == 0, f"业务错误: {data}"
            oid = data["data"]["order_id"]
            order_ids.append(oid)
            print(f"  ✅ 工单 order_id={oid} content={content}")
        assert len(order_ids) == 3

        # -------- 用例 2：查询初始状态 --------
        print("\n🔍 [用例2] 查询初始状态（应该都是待处理）")
        for oid in order_ids:
            async with session.get(f"{TEST_HTTP_URL}/api/work_order/{oid}",
                                   timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
            assert data["code"] == 0
            assert data["data"]["status"] == 0, f"预期 status=0 实际 {data['data']['status']}"
            print(f"  ✅ order_id={oid} status=0 待处理")

        # -------- 用例 3：监控接口初始状态 --------
        print("\n📊 [用例3] 监控接口初始状态")
        async with session.get(f"{TEST_HTTP_URL}/monitor/stat",
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            assert resp.status == 200
            stat = await resp.json()
        assert stat["online_worker"] >= 1, "在线 worker 应至少有 1 个"
        assert stat["pending_work_order"] == 3
        print(f"  ✅ 在线 worker={stat['online_worker']} 待处理工单={stat['pending_work_order']}")

        # -------- 用例 4：等待 Worker 处理 --------
        print("\n⏳ [用例4] 等待 Worker 处理工单（约 10 秒）...")
        await asyncio.sleep(10)

        # -------- 用例 5：验证工单全部完成 --------
        print("\n✅ [用例5] 验证所有工单已完成")
        all_done = True
        for oid in order_ids:
            async with session.get(f"{TEST_HTTP_URL}/api/work_order/{oid}",
                                   timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
            row = data["data"]
            if row["status"] == 2:
                print(f"  ✅ order_id={oid} status=2(完成) worker={row.get('worker_id')}")
            else:
                print(f"  ❌ order_id={oid} status={row['status']} 未完成")
                all_done = False
        assert all_done, "存在未完成的工单"

        # -------- 用例 6：监控接口终态 --------
        print("\n📊 [用例6] 监控接口终态")
        async with session.get(f"{TEST_HTTP_URL}/monitor/stat",
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            stat = await resp.json()
        assert stat["pending_work_order"] == 0
        print(f"  ✅ 在线 worker={stat['online_worker']} 待处理工单={stat['pending_work_order']}")

        # -------- 用例 7：参数校验 --------
        print("\n⚠️  [用例7] 参数校验")
        async with session.post(f"{TEST_HTTP_URL}/api/work_order", json={},
                                timeout=aiohttp.ClientTimeout(total=5)) as resp:
            assert resp.status == 400
        print("  ✅ 空 content → 400")

        async with session.get(f"{TEST_HTTP_URL}/api/work_order/abc",
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            assert resp.status == 400
        print("  ✅ 非数字 order_id → 400")

        async with session.get(f"{TEST_HTTP_URL}/api/work_order/999999",
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            assert resp.status == 404
        print("  ✅ 不存在的 order_id → 404")

    # -------- 用例 8：Redis 状态验证 --------
    print("\n🔑 [用例8] 直接验证 Redis 在线工人列表")
    r = aioredis.from_url(settings.redis_url)
    online = await r.scard("online_workers")
    await r.aclose()
    assert online >= 1, f"Redis online_workers 为空"
    print(f"  ✅ Redis online_workers 在线数={online}")

    print("\n" + "=" * 60)
    print("  🎉 全部 8 个用例通过！")
    print("=" * 60)



async def main():
    print("=" * 60)
    print("  🚀 端到端集成测试 (一键启动 → 测试 → 复原)")
    print("=" * 60)

    scheduler_task = None
    http_runner = None
    worker_task = None

    try:

        print("\n🔄 清理初始状态...")
        await cleanup_all()

        print("\n🚀 启动调度中心 TCP 服务...")
        scheduler_task = asyncio.create_task(start_tcp_server())
        ok = await _wait_port(settings.SCHEDULER_HOST, settings.SCHEDULER_PORT)
        assert ok, f"TCP 调度中心启动失败，端口 {settings.SCHEDULER_PORT} 未就绪"
        print(f"  ✅ TCP 调度中心 {settings.SCHEDULER_HOST}:{settings.SCHEDULER_PORT} 已就绪")

        print("🚀 启动 HTTP API 服务...")
        http_runner = web.AppRunner(create_app())
        await http_runner.setup()
        site = web.TCPSite(http_runner, settings.HTTP_HOST, settings.HTTP_PORT)
        await site.start()
        ok = await _wait_port(settings.HTTP_HOST, settings.HTTP_PORT)
        assert ok, f"HTTP API 启动失败，端口 {settings.HTTP_PORT} 未就绪"
        print(f"  ✅ HTTP API {settings.HTTP_HOST}:{settings.HTTP_PORT} 已就绪")

        print("🚀 启动 Worker 客户端...")
        worker_task = asyncio.create_task(WorkerTCPClient("test-worker").run())
        await asyncio.sleep(1.5)
        print("  ✅ Worker 已连接调度中心")

        await run_tests()

    finally:
        print("\n" + "=" * 60)
        print("🧹 开始停止服务并复原...")

        # 阶段1：所有已知顶层 task 先发 cancel 信号
        for t in (worker_task, scheduler_task):
            if t and not t.done():
                t.cancel()

        # 阶段2：先关 HTTP（触发 on_shutdown 释放连接池）
        if http_runner:
            try:
                await http_runner.cleanup()
                print("  ⏹  HTTP API 已停止")
            except Exception as e:
                print(f"  ⚠️  HTTP 关闭警告: {e}")

        # 阶段3：扫出所有孤儿 task 一起 cancel
        current = asyncio.current_task()
        orphan = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
        for t in orphan:
            t.cancel()
        if orphan:
            print(f"  ⏹  额外 cancel {len(orphan)} 个后台 task")

        # 阶段4：等所有 task 真正退出
        for t in list(asyncio.all_tasks()):
            if t is not asyncio.current_task() and not t.done():
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

        print("  ✅ 所有后台 task 已安全退出")

        await asyncio.sleep(0.1)

        # 阶段5：最后清数据
        try:
            await cleanup_all()
        except Exception as e:
            print(f"  ⚠️  cleanup_all 警告: {e}")

        print("\n✅ 复原完成。数据库已清空，Redis 已 FLUSHDB。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹  用户中断，正在清理...")
        sys.exit(0)