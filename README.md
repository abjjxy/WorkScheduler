# 工单调度系统 (Work Scheduler)

基于 `asyncio` 原生异步 TCP 与 `FastAPI` 构建的分布式工单调度系统，主要用于实现任务的分发、状态管理、失败重试和高并发限流。调度中心通过自定义二进制协议与 Worker 节点通信，负责完成工单的派发、心跳检测、消息去重和故障恢复等关键能力。

---

## 项目概述

本项目聚焦于构建一个高可用、高并发的任务调度平台，适用于后台任务处理、分布式执行、消息分发和系统异步处理等场景。系统整体由三个核心部分组成：

1. HTTP 接入层：对外暴露工单创建、查询与监控接口
2. 调度中心：负责工单派发、状态更新、Worker 管理和重试机制
3. Worker 节点：负责接收并执行具体业务任务，并上报处理结果

项目实现了从工单创建到任务执行完成的闭环处理，并具备稳定的容错和扩展能力，适合学习分布式任务调度、异步网络编程与高并发系统设计。

---

## 技术栈

| 类别 | 技术选型 |
|------|----------|
| 异步框架 | `asyncio`（TCP 服务端）、`FastAPI`（HTTP 接入层） |
| 数据库 | MySQL (`aiomysql`) |
| 中间件 | Redis (`redis.asyncio`) |
| 序列化 | `struct` 二进制打包 / `json` |
| 校验 | `CRC32` |
| 限流 | 滑动窗口 + 令牌桶 |
| 配置管理 | `.env` |
| 日志 | 自定义日志模块 + 轮转日志 |

---

## 架构设计

```text
┌────────────┐    HTTP/JSON    ┌────────────────────┐
│   Client    │ ─────────────▶ │   FastAPI API      │
└────────────┘                └────────────────────┘
                                     │
                                     ▼
                              ┌────────────────────┐
                              │       MySQL        │
                              │   工单持久化存储    │
                              └────────────────────┘
                                     ▲
                                     │
                                     │ 轮询待派发工单
                                     │
┌────────────┐   自定义二进制协议 ┌────────────────────┐
│ Worker 1   │ ◀──────────────▶ │  Scheduler Center  │ ◀──── Redis
│ Worker 2   │ ◀──────────────▶ │  (asyncio)         │      在线Worker/
│ Worker N   │ ◀──────────────▶ │                    │      锁/限流/去重
└────────────┘                └────────────────────┘
```

### 核心流程

1. 客户端通过 HTTP 接口创建工单
2. 工单写入 MySQL，状态初始化为待派发（status = 0）
3. 调度中心后台协程轮询待处理工单
4. 从 Redis 在线 Worker 集合中选择可用节点并派发任务
5. Worker 执行完成后上报结果，更新工单状态为已完成（status = 2）
6. 若 Worker 心跳超时或任务失败，则回收在途工单并重试调度

---

## 项目结构

```text
.
├── scheduler_tcp.py      # 调度中心 TCP 服务端（核心）
├── worker_client.py      # Worker 客户端（断线重连 + 本地去重）
├── http_api.py           # FastAPI HTTP 接入层
├── rate_limiter.py       # 分布式限流器（滑动窗口 + 令牌桶）
├── config.py             # 全局配置（读取 .env）
├── logger.py             # 日志模块（控制台 + 轮转日志）
├── test.py               # 测试代码
├── requirements.txt      # 依赖列表
├── init.sql             # 数据库初始化脚本
├── .env                 # 本地环境变量
├── logs/                # 日志目录
└── README.md            # 项目说明文档
```

---

## 核心功能模块

### 1. 自定义 TCP 二进制协议

使用轻量级二进制协议实现调度中心和 Worker 的通信，报文结构如下：

```text
[4字节 length][16字节 msg_id][4字节 CRC32][消息体 JSON]
```

| 字段 | 长度 | 说明 |
|------|------|------|
| length | 4 字节 | 消息体字节长度 |
| msg_id | 16 字节 | UUID v4，消息唯一标识 |
| crc32 | 4 字节 | 消息体 CRC32 校验值 |
| body | N 字节 | JSON 编码的消息体 |

消息类型如下：

| 类型 | 值 | 方向 | 说明 |
|------|----|------|------|
| MSG_HEARTBEAT | 0x01 | Worker → Server | 心跳消息 |
| MSG_HEARTBEAT_ACK | 0x02 | Server → Worker | 心跳响应 |
| MSG_ORDER_DELIVER | 0x03 | Server → Worker | 派发工单 |
| MSG_ORDER_RESULT | 0x04 | Worker → Server | 上报处理结果 |

---

### 2. 消息去重

为了避免网络抖动或重试机制导致的重复投递，系统设计了消息去重策略：

- 服务端使用 Redis 的 `SET NX EX` 记录消息唯一标识 `msg_id`
- Worker 端使用本地 `set` 维护已处理消息，防止重复消费
- TTL 可配置，避免去重记录无限增长

这保证了在异常重试环境下不会重复执行同一条消息。

---

### 3. 分布式锁

在工单派发前，调度中心通过 Redis 分布式锁控制同一工单的唯一派发：

```bash
SET order_lock:{id} locked NX EX 30
```

该机制可防止同一工单被多个 Worker 同时处理，避免任务冲突和脏数据问题。

---

### 4. 心跳检测与故障恢复

系统采用心跳机制维持 Worker 在线状态：

- Worker 每 3 秒发送一次心跳
- 调度中心记录每个 Worker 的最后心跳时间
- 后台任务定期检查超时节点
- 超时 12 秒未收到心跳，则判定 Worker 离线
- 将其在途工单回收并重置为待派发状态，触发重试

这样可以在节点失联、网络抖动和进程异常退出场景中保证任务可恢复。

---

### 5. 限流机制

为保证系统在高并发场景下稳定运行，引入了分布式限流设计：

| 算法 | 用途 | 数据结构 | 原子性 |
|------|------|----------|--------|
| 滑动窗口 | HTTP 接口限流 | ZSet | Lua 脚本 |
| 令牌桶 | TCP 连接限流 | Hash | Lua 脚本 |

两种限流策略均通过 Redis Lua 脚本原子执行，避免并发竞争，提升系统稳定性。

---

## 安装与运行

### 环境要求

- Python 3.10+
- MySQL 8.0+
- Redis 6.0+

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 初始化数据库

```sql
CREATE DATABASE work_scheduler DEFAULT CHARSET utf8mb4;

CREATE TABLE work_order (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    content     TEXT NOT NULL,
    status      TINYINT NOT NULL DEFAULT 0 COMMENT '0待派发 1处理中 2已完成',
    worker_id   VARCHAR(64) DEFAULT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

也可以直接执行项目中的初始化脚本：

```bash
mysql -u root -p < init.sql
```

### 3. 配置环境变量

复制示例配置：

```bash
cp .env.example .env
```

如果当前目录中已有 `.env`，可直接编辑并填充相应参数。

### 4. 启动服务

```bash
# 启动调度中心
python scheduler_tcp.py

# 启动 HTTP 接入层
python http_api.py

# 启动 Worker 节点
python worker_client.py worker-1
python worker_client.py worker-2
```

---

## 配置说明

`.env` 中的核心配置项如下：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| SCHEDULER_HOST | 0.0.0.0 | 调度中心监听地址 |
| SCHEDULER_PORT | 8888 | 调度中心监听端口 |
| WORKER_NAME | worker-1 | Worker 默认名称 |
| HEARTBEAT_INTERVAL | 3 | 心跳间隔（秒） |
| HTTP_HOST | 0.0.0.0 | HTTP 服务监听地址 |
| HTTP_PORT | 8080 | HTTP 服务监听端口 |
| MYSQL_HOST/PORT/USER/PASSWORD/DB | - | MySQL 连接配置 |
| REDIS_HOST/PORT/DB | 127.0.0.1/6379/0 | Redis 连接配置 |
| RATE_LIMIT_ENABLED | true | 是否开启限流 |
| HTTP_RATE_WINDOW | 10 | HTTP 滑动窗口大小（秒） |
| HTTP_RATE_MAX_PER_WINDOW | 100 | 单窗口最大请求数 |
| TCP_RATE_BUCKET_CAPACITY | 50 | TCP 令牌桶容量 |
| TCP_RATE_REFILL_RATE | 10 | TCP 令牌补充速率（个/秒） |
| MSG_DEDUP_TTL | 3600 | 消息去重 TTL（秒） |

---

## HTTP API 接口

### 1. 创建工单

```http
POST /api/work_order
Content-Type: application/json
```

请求体示例：

```json
{
  "content": "处理用户反馈"
}
```

响应示例：

```json
{
  "code": 0,
  "msg": "工单创建成功",
  "data": {
    "order_id": 123
  }
}
```

### 2. 查询工单

```http
GET /api/work_order/{order_id}
```

响应示例：

```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "id": 123,
    "content": "处理用户反馈",
    "status": 2,
    "worker_id": "worker-1",
    "retry_count": 1
  }
}
```

### 3. 监控统计

```http
GET /monitor/stat
```

响应示例：

```json
{
  "online_worker": 3,
  "pending_work_order": 12
}
```

### 4. 限流响应

当接口触发限流保护时，返回：

```json
{
  "code": 429,
  "msg": "请求过于频繁，请稍后再试"
}
```

---

## 设计亮点

- 采用 `asyncio` 构建高并发调度中心，提升任务处理吞吐能力
- 使用自定义二进制协议减少网络传输开销，提高消息处理效率
- 结合 Redis 分布式锁、消息去重与心跳检测，增强系统稳定性和容错能力
- 实现滑动窗口 + 令牌桶双重限流机制，保障高并发场景下服务可用性
- 提供 HTTP 管理接口和监控接口，便于接入、排障和运维管理

---

## 未来优化方向

- 支持工单优先级和延迟任务调度
- 引入更完整的任务重试策略和退避机制
- 扩展多调度中心高可用部署方案
- 引入监控告警平台，如 Prometheus + Grafana
- 支持 MySQL 分库分表，提升大规模任务处理能力

---

## 结语

本项目实现了一个具备分布式特性的工单调度平台，覆盖任务接入、执行调度、状态管理和故障恢复等核心能力，适合用于学习异步网络编程、分布式任务调度和高并发系统设计。该系统具有较强的工程扩展性，可作为后续业务系统接入和调度平台开发的基础框架。
