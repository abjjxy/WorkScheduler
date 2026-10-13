# http_api.py
# from aiohttp import web
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse

from pydantic import BaseModel, Field
#BaseModel：Pydantic 数据模型基类

import aiomysql
import redis.asyncio as aioredis
# import asyncio
from datetime import datetime
from logger import app_logger
from config import settings
from rate_limiter import SlidingWindowLimiter
from contextlib import asynccontextmanager

# 创建工单的请求体模型
class WorkOrderCreate(BaseModel):
    content: str = Field(..., description="工单内容，不能为空")
#Field(..., ...) 中 ... 表示必填字段。

# 通用响应模型
class ApiResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: dict | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动：初始化资源
    mysql_pool = await aiomysql.create_pool(**settings.mysql_config, autocommit=True)
    redis_client = aioredis.from_url(settings.redis_url)
    #aiohttp 用 app["key"] 存共享数据，FastAPI 用 app.state.key。
    app.state.mysql_pool = mysql_pool
    app.state.redis = redis_client
    app.state.rate_limiter = SlidingWindowLimiter(redis_client)
    app_logger.info("FastAPI 服务启动成功，MySQL/Redis 已初始化")
    
    yield  # 服务运行中...
    
    # 关闭：释放资源
    if mysql_pool:
        mysql_pool.close()
        await mysql_pool.wait_closed()
    if redis_client:
        await redis_client.aclose()
    app_logger.info("FastAPI 服务已关闭")

app = FastAPI(lifespan=lifespan, title="工单调度系统 API")


#用 FastAPI 的 Depends 优雅地把连接池注入到 handler
async def get_mysql_pool(request: Request):
    return request.app.state.mysql_pool

async def get_redis(request: Request):
    return request.app.state.redis


@app.post("/api/work_order", response_model=ApiResponse)
async def create_work_order(
    body: WorkOrderCreate,                                    # 自动解析+校验 JSON
    mysql_pool = Depends(get_mysql_pool)
):
    try:
        content = body.content  # Pydantic 已保证非空
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO work_order (content, status, retry_count) VALUES (%s, 0, 0)",
                    (content,)
                )
                order_id = cur.lastrowid
        app_logger.info("创建工单成功 order_id=%s", order_id)
        return ApiResponse(code=0, msg="工单创建成功", data={"order_id": order_id})
    except Exception as e:
        app_logger.error("创建工单异常: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"创建失败:{str(e)}")


@app.get("/api/work_order/{order_id}", response_model=ApiResponse)
async def get_work_order(
    order_id: int,                                           # 路径参数自动转 int
    mysql_pool = Depends(get_mysql_pool)
):
    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM work_order WHERE id = %s", (order_id,))
                row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="工单不存在")
        
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        
        return ApiResponse(code=0, msg="success", data=row)
    except HTTPException:
        raise
    except Exception as e:
        app_logger.error("查询工单异常: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"查询异常:{str(e)}")


@app.get("/monitor/stat")
async def get_monitor_stat(
    redis = Depends(get_redis),
    mysql_pool = Depends(get_mysql_pool)
):
    try:
        online_worker_num = await redis.scard("online_workers")
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM work_order WHERE status=0")
                res = await cur.fetchone()
                pending_order_count = res[0]
        return {
            "online_worker": online_worker_num,
            "pending_work_order": pending_order_count
        }
    except Exception as e:
        app_logger.error("监控查询异常: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"监控查询失败:{str(e)}")



@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    limiter = request.app.state.rate_limiter
    if limiter:
        resource = f"{request.method}:{request.url.path}"
        allowed = await limiter.allow(resource)
        if not allowed:
            app_logger.warning("限流拒绝 %s %s", request.method, request.url.path)
            return JSONResponse(
                {"code": 429, "msg": "请求过于频繁，请稍后再试"},
                status_code=429
            )
    response = await call_next(request)
    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.HTTP_HOST, port=settings.HTTP_PORT)
   















# async def on_startup(app):
#     mysql_pool = await aiomysql.create_pool(**settings.mysql_config, autocommit=True)
#     redis_client = aioredis.from_url(settings.redis_url)
#     app["mysql_pool"] = mysql_pool
#     app["redis"] = redis_client
#     app["rate_limiter"] = SlidingWindowLimiter(redis_client)
#     app_logger.info("HTTP 服务启动成功，MySQL/Redis 连接池已初始化，限流 enabled=%s", settings.RATE_LIMIT_ENABLED)

# async def on_shutdown(app):
#     mysql_pool = app.get("mysql_pool")
#     redis_client = app.get("redis")
#     if mysql_pool:
#         mysql_pool.close()
#         await mysql_pool.wait_closed()
#     if redis_client:
#         await redis_client.aclose()
#     app_logger.info("HTTP 服务已关闭，资源释放完成")

# async def create_work_order(request: web.Request):
#     try:
#         data = await request.json()
#         content = data.get("content")
#         if not content:
#             app_logger.warning("POST /api/work_order 参数缺失: content 为空")
#             return web.json_response({"code": 400, "msg": "工单内容content不能为空"}, status=400)

#         async with request.app["mysql_pool"].acquire() as conn:
#             async with conn.cursor() as cur:
#                 sql = """
#                 INSERT INTO work_order (content, status, retry_count)
#                 VALUES (%s, 0, 0)
#                 """
#                 await cur.execute(sql, (content,))
#                 await conn.commit()
#                 order_id = cur.lastrowid
#         app_logger.info("POST /api/work_order 创建成功 order_id=%s content=%s", order_id, content)
#         return web.json_response({
#             "code": 0,
#             "msg": "工单创建成功",
#             "data": {"order_id": order_id}
#         })
#     except Exception as e:
#         app_logger.error("POST /api/work_order 异常: %s", e, exc_info=True)
#         return web.json_response({"code": 500, "msg": f"创建失败:{str(e)}"}, status=500)

# async def get_work_order(request: web.Request):
#     order_id = request.match_info.get("order_id")
#     if not order_id or not order_id.isdigit():
#         app_logger.warning("GET /api/work_order/%s 参数非法: order_id 必须为数字", order_id)
#         return web.json_response({"code": 400, "msg": "order_id必须为数字"}, status=400)
#     order_id = int(order_id)

#     try:
#         async with request.app["mysql_pool"].acquire() as conn:
#             async with conn.cursor(aiomysql.DictCursor) as cur:
#                 sql = "SELECT * FROM work_order WHERE id = %s"
#                 await cur.execute(sql, (order_id,))
#                 row = await cur.fetchone()
#         if not row:
#             app_logger.info("GET /api/work_order/%s 工单不存在", order_id)
#             return web.json_response({"code": 404, "msg": "工单不存在"}, status=404)
#         for k, v in row.items():
#             if isinstance(v, datetime):
#                 row[k] = v.strftime("%Y-%m-%d %H:%M:%S")
#         app_logger.info("GET /api/work_order/%s 查询成功", order_id)
#         return web.json_response({"code": 0, "msg": "success", "data": row})
#     except Exception as e:
#         app_logger.error("GET /api/work_order/%s 异常: %s", order_id, e, exc_info=True)
#         return web.json_response({"code": 500, "msg": f"查询异常:{str(e)}"}, status=500)

# async def get_monitor_stat(request):
#     try:
#         redis = request.app["redis"]
#         mysql_pool = request.app["mysql_pool"]

#         online_worker_num = await redis.scard("online_workers")

#         async with mysql_pool.acquire() as conn:
#             async with conn.cursor() as cur:
#                 await cur.execute("SELECT COUNT(*) FROM work_order WHERE status=0")
#                 res = await cur.fetchone()
#                 pending_order_count = res[0]

#         app_logger.info("GET /monitor/stat online=%d pending=%d",
#                         online_worker_num, pending_order_count)
#         return web.json_response({
#             "online_worker": online_worker_num,
#             "pending_work_order": pending_order_count
#         })
#     except Exception as e:
#         app_logger.error("GET /monitor/stat 异常: %s", e, exc_info=True)
#         return web.json_response({"code": 500, "msg": f"监控查询失败:{str(e)}"}, status=500)
    
# async def rate_limit_middleware(app, handler):
#     async def middleware(request: web.Request):
#         limiter = request.app.get("rate_limiter")
#         if limiter:
#             resource = f"{request.method}:{request.path}"
#             allowed = await limiter.allow(resource)
#             if not allowed:
#                 app_logger.warning("限流拒绝 %s %s", request.method, request.path)
#                 return web.json_response(
#                     {"code": 429, "msg": "请求过于频繁，请稍后再试"},
#                     status=429
#                 )
#         return await handler(request)
#     return middleware


# def create_app():
#     app = web.Application(middlewares=[rate_limit_middleware])
#     app.add_routes([
#         web.post("/api/work_order", create_work_order),
#         web.get("/api/work_order/{order_id}", get_work_order),
#         web.get("/monitor/stat", get_monitor_stat)
#     ])
#     app.on_startup.append(on_startup)
#     app.on_shutdown.append(on_shutdown)
#     return app

# if __name__ == "__main__":
#     app = create_app()
#     web.run_app(app, host=settings.HTTP_HOST, port=settings.HTTP_PORT)