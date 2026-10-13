import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # TCP调度中心
    SCHEDULER_HOST = os.getenv("SCHEDULER_HOST", "0.0.0.0")
    SCHEDULER_PORT = int(os.getenv("SCHEDULER_PORT", 8888))

    # Worker客户端
    WORKER_NAME = os.getenv("WORKER_NAME", "worker-1")
    SCHEDULER_CLIENT_HOST = os.getenv("SCHEDULER_CLIENT_HOST", "127.0.0.1")
    SCHEDULER_CLIENT_PORT = int(os.getenv("SCHEDULER_CLIENT_PORT", 8888))
    HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3))

    # HTTP服务
    HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
    HTTP_PORT = int(os.getenv("HTTP_PORT", 8080))

    # MySQL
    MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
    MYSQL_USER = os.getenv("MYSQL_USER", "smithsoda")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "123456")
    MYSQL_DB = os.getenv("MYSQL_DB", "work_scheduler")

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))

    # 日志
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # 限流
    RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() in ("true", "1", "yes")

    # HTTP 滑动窗口限流（每个接口独立窗口）
    HTTP_RATE_WINDOW = int(os.getenv("HTTP_RATE_WINDOW", "10"))
    HTTP_RATE_MAX_PER_WINDOW = int(os.getenv("HTTP_RATE_MAX_PER_WINDOW", "100"))

    # TCP 令牌桶限流（Worker 连接）
    TCP_RATE_BUCKET_CAPACITY = int(os.getenv("TCP_RATE_BUCKET_CAPACITY", "50"))
    TCP_RATE_REFILL_RATE = int(os.getenv("TCP_RATE_REFILL_RATE", "10"))

    # 消息去重：msg_id 在 Redis 中保留时间（秒）
    MSG_DEDUP_TTL = int(os.getenv("MSG_DEDUP_TTL", "3600"))

    # 重试次数
    MAX_RETRY_COUNTS = int(os.getenv("MAX_RETRY_COUNTS", "3"))

    @property
    def mysql_config(self) -> dict:
        return {
            "host": self.MYSQL_HOST,
            "port": self.MYSQL_PORT,
            "user": self.MYSQL_USER,
            "password": self.MYSQL_PASSWORD,
            "db": self.MYSQL_DB,
            "charset": "utf8mb4",
        }

    @property
    def redis_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Config()